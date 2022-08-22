// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::{LockType, WriteType};

// #[PerformanceCriticalPath]
use crate::storage::{
    mvcc::{MvccScanner, MvccTxn, Result as MvccResult},
    Snapshot,
};

// Flashback all keys within [start_key, end_key) to the specified version by
// deleting all keys in `CF_WRITE` and `CF_DEFAULT` that are committed later
// than this version and clearing all locks in `CF_LOCK`.
pub fn flashback_to_version<S: Snapshot>(
    txn: &mut MvccTxn,
    scanner: &mut MvccScanner<S>,
) -> MvccResult<()> {
    // To flashback the `CF_WRITE`, we need to delete all write records whose
    // `commit_ts` is greater than the specified version.
    loop {
        let writes = scanner.scan_next_batch_write()?;
        if writes.is_empty() {
            break;
        }
        for (key, write) in writes {
            let encoded_key = key.clone().truncate_ts()?;
            let commit_ts = key.decode_ts()?;
            txn.delete_write(encoded_key.clone(), commit_ts);
            // If the short value is none and it's a `WriteType::Put`, we should delete the
            // corresponding key from `CF_DEFAULT` as well.
            if write.short_value.is_none() && write.write_type == WriteType::Put {
                txn.delete_value(encoded_key, write.start_ts);
            }
        }
    }
    // To flashback the `CF_LOCK`, we need to delete all locks inside.
    // TODO: `resolved_ts` better be taken into account here.
    loop {
        let locks = scanner.scan_next_batch_lock()?;
        if locks.is_empty() {
            break;
        }
        for (key, lock) in locks {
            txn.unlock_key(key.clone(), lock.is_pessimistic_txn());
            // If the short value is none and it's a `LockType::Put`, we should delete the
            // corresponding key from `CF_DEFAULT` as well.
            if lock.short_value.is_none() && lock.lock_type == LockType::Put {
                txn.delete_value(key, lock.ts);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use concurrency_manager::ConcurrencyManager;
    use kvproto::kvrpcpb::Context;
    use txn_types::TimeStamp;

    use crate::storage::{
        mvcc::{tests::*, MvccScanner, MvccTxn},
        txn::{flashback_to_version, tests::*},
        Engine, TestEngineBuilder,
    };

    pub fn must_flashback_to_version<E: Engine>(engine: &E, version: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let version = version.into();

        let cm = ConcurrencyManager::new(1.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut txn = MvccTxn::new(TimeStamp::zero(), cm);
        let mut scanner = MvccScanner::new(snapshot, 1, version, TimeStamp::zero(), None, None);
        flashback_to_version(&mut txn, &mut scanner).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    #[test]
    fn test_basic_flashback() {
        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"k", b"v@1", b"k", 1);
        must_commit(&engine, b"k", 1, 2);
        must_prewrite_put(&engine, b"k", b"v@3", b"k", 3);
        must_commit(&engine, b"k", 3, 4);
        must_prewrite_put(&engine, b"k", b"v@5", b"k", 5);
        must_commit(&engine, b"k", 5, 6);
        must_prewrite_put(&engine, b"k", b"v@7", b"k", 7);

        must_flashback_to_version(&engine, 6);
        must_get(&engine, b"k", 7, b"v@5");
        must_flashback_to_version(&engine, 4);
        must_get(&engine, b"k", 7, b"v@3");
        must_flashback_to_version(&engine, 2);
        must_get(&engine, b"k", 7, b"v@1");
        must_flashback_to_version(&engine, 0);
        must_get_none(&engine, b"k", 7);
    }
}
