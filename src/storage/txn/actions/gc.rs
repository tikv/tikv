use crate::storage::mvcc::{
    GcInfo, MvccTxn, Result as MvccResult, GC_DELETE_VERSIONS_HISTOGRAM, MAX_TXN_WRITE_SIZE,
    MVCC_VERSIONS_HISTOGRAM,
};
use crate::storage::Snapshot;
use txn_types::{Key, TimeStamp, WriteType};

pub fn gc<S: Snapshot>(
    txn: &mut MvccTxn<S>,
    key: Key,
    safe_point: TimeStamp,
) -> MvccResult<GcInfo> {
    let mut remove_older = false;
    let mut ts = TimeStamp::max();
    let mut found_versions = 0;
    let mut deleted_versions = 0;
    let mut latest_delete = None;
    let mut is_completed = true;
    while let Some((commit, write)) = txn.reader.seek_write(&key, ts)? {
        ts = commit.prev();
        found_versions += 1;

        if txn.write_size >= MAX_TXN_WRITE_SIZE {
            // Cannot remove latest delete when we haven't iterate all versions.
            latest_delete = None;
            is_completed = false;
            break;
        }

        if remove_older {
            txn.delete_write(key.clone(), commit);
            if write.write_type == WriteType::Put && write.short_value.is_none() {
                txn.delete_value(key.clone(), write.start_ts);
            }
            deleted_versions += 1;
            continue;
        }

        if commit > safe_point {
            continue;
        }

        // Set `remove_older` after we find the latest value.
        match write.write_type {
            WriteType::Put | WriteType::Delete => {
                remove_older = true;
            }
            WriteType::Rollback | WriteType::Lock => {}
        }

        // Latest write before `safe_point` can be deleted if its type is Delete,
        // Rollback or Lock.
        match write.write_type {
            WriteType::Delete => {
                latest_delete = Some(commit);
            }
            WriteType::Rollback | WriteType::Lock => {
                txn.delete_write(key.clone(), commit);
                deleted_versions += 1;
            }
            WriteType::Put => {}
        }
    }
    if let Some(commit) = latest_delete {
        txn.delete_write(key, commit);
        deleted_versions += 1;
    }
    MVCC_VERSIONS_HISTOGRAM.observe(found_versions as f64);
    if deleted_versions > 0 {
        GC_DELETE_VERSIONS_HISTOGRAM.observe(deleted_versions as f64);
    }
    Ok(GcInfo {
        found_versions,
        deleted_versions,
        is_completed,
    })
}

pub mod tests {
    use super::*;
    use crate::storage::kv::SnapContext;
    use crate::storage::mvcc::tests::write;
    use crate::storage::{Engine, ScanMode};
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;

    #[cfg(test)]
    use crate::storage::{
        mvcc::tests::{must_get, must_get_none},
        txn::tests::*,
        RocksEngine, TestEngineBuilder,
    };
    #[cfg(test)]
    use txn_types::SHORT_VALUE_MAX_LEN;

    pub fn must_succeed<E: Engine>(engine: &E, key: &[u8], safe_point: impl Into<TimeStamp>) {
        let ctx = SnapContext::default();
        let snapshot = engine.snapshot(ctx).unwrap();
        let cm = ConcurrencyManager::new(1.into());
        let mut txn = MvccTxn::for_scan(
            snapshot,
            Some(ScanMode::Forward),
            TimeStamp::zero(),
            true,
            cm,
        );
        gc(&mut txn, Key::from_raw(key), safe_point.into()).unwrap();
        write(engine, &Context::default(), txn.into_modifies());
    }

    #[cfg(test)]
    fn test_gc_imp<F>(k: &[u8], v1: &[u8], v2: &[u8], v3: &[u8], v4: &[u8], gc: F)
    where
        F: Fn(&RocksEngine, &[u8], u64),
    {
        let engine = TestEngineBuilder::new().build().unwrap();

        must_prewrite_put(&engine, k, v1, k, 5);
        must_commit(&engine, k, 5, 10);
        must_prewrite_put(&engine, k, v2, k, 15);
        must_commit(&engine, k, 15, 20);
        must_prewrite_delete(&engine, k, k, 25);
        must_commit(&engine, k, 25, 30);
        must_prewrite_put(&engine, k, v3, k, 35);
        must_commit(&engine, k, 35, 40);
        must_prewrite_lock(&engine, k, k, 45);
        must_commit(&engine, k, 45, 50);
        must_prewrite_put(&engine, k, v4, k, 55);
        must_rollback(&engine, k, 55);

        // Transactions:
        // startTS commitTS Command
        // --
        // 55      -        PUT "x55" (Rollback)
        // 45      50       LOCK
        // 35      40       PUT "x35"
        // 25      30       DELETE
        // 15      20       PUT "x15"
        //  5      10       PUT "x5"

        // CF data layout:
        // ts CFDefault   CFWrite
        // --
        // 55             Rollback(PUT,50)
        // 50             Commit(LOCK,45)
        // 45
        // 40             Commit(PUT,35)
        // 35   x35
        // 30             Commit(Delete,25)
        // 25
        // 20             Commit(PUT,15)
        // 15   x15
        // 10             Commit(PUT,5)
        // 5    x5

        gc(&engine, k, 12);
        must_get(&engine, k, 12, v1);

        gc(&engine, k, 22);
        must_get(&engine, k, 22, v2);
        must_get_none(&engine, k, 12);

        gc(&engine, k, 32);
        must_get_none(&engine, k, 22);
        must_get_none(&engine, k, 35);

        gc(&engine, k, 60);
        must_get(&engine, k, 62, v3);
    }

    #[test]
    fn test_gc() {
        test_gc_imp(b"k1", b"v1", b"v2", b"v3", b"v4", must_succeed);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"k2", &v1, &v2, &v3, &v4, must_succeed);
    }

    #[test]
    fn test_gc_with_compaction_filter() {
        use crate::server::gc_worker::gc_by_compact;

        test_gc_imp(b"zk1", b"v1", b"v2", b"v3", b"v4", gc_by_compact);

        let v1 = "x".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v2 = "y".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v3 = "z".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        let v4 = "v".repeat(SHORT_VALUE_MAX_LEN + 1).into_bytes();
        test_gc_imp(b"zk2", &v1, &v2, &v3, &v4, gc_by_compact);
    }
}
