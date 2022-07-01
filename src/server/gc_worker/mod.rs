// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod applied_lock_collector;
pub mod compaction_filter;
mod config;
mod gc_manager;
mod gc_worker;
pub mod rawkv_compaction_filter;

use std::sync::Arc;

// TODO: Use separated error type for GCWorker instead.
#[cfg(any(test, feature = "failpoints"))]
pub use compaction_filter::test_utils::{gc_by_compact, TestGCRunner};
pub use compaction_filter::WriteCompactionFilterFactory;
pub use config::{GcConfig, GcWorkerConfigManager, DEFAULT_GC_BATCH_KEYS};
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{KvEngine, MvccProperties};
pub use gc_manager::AutoGcConfig;
pub use gc_worker::{
    sync_gc, GcRunner, GcSafePointProvider, GcTask, GcWorker, GC_MAX_EXECUTING_TASKS,
    STAT_RAW_KEYMODE, STAT_TXN_KEYMODE,
};
use kvproto::{
    kvrpcpb::Context,
    metapb::{Peer, Region},
};
use raftstore::store::RegionSnapshot;
pub use rawkv_compaction_filter::RawCompactionFilterFactory;
use tikv_kv::{write_modifies, Modify, SnapContext, WriteData};
use txn_types::{Key, TimeStamp};

pub use crate::storage::{
    kv,
    kv::{Callback as EngineCallback, Result as EngineResult},
    Callback, Engine, Error, ErrorInner, Result,
};

/// A wrapper of engine that adds the 'z' prefix to keys internally.
/// For test engines, they writes keys into db directly, but in production a 'z' prefix will be
/// added to keys by raftstore layer before writing to db. Some functionalities of `GCWorker`
/// bypasses Raft layer, so they needs to know how data is actually represented in db. This
/// wrapper allows test engines write 'z'-prefixed keys to db.
#[derive(Clone)]
#[cfg(any(test, feature = "testexport"))]
pub struct PrefixedEngine(pub kv::RocksEngine);

impl Engine for PrefixedEngine {
    // Use RegionSnapshot which can remove the z prefix internally.
    type Snap = RegionSnapshot<RocksSnapshot>;
    type Local = RocksEngine;

    fn kv_engine(&self) -> RocksEngine {
        self.0.kv_engine()
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> kv::Result<Self::Snap> {
        let mut region = Region::default();
        region.set_start_key(start_key.to_owned());
        region.set_end_key(end_key.to_owned());
        // Use a fake peer to avoid panic.
        region.mut_peers().push(Default::default());
        Ok(RegionSnapshot::from_snapshot(
            Arc::new(self.kv_engine().snapshot()),
            Arc::new(region),
        ))
    }

    fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
        for modify in &mut modifies {
            match modify {
                Modify::Delete(_, ref mut key) => {
                    let bytes = keys::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::Put(_, ref mut key, _) => {
                    let bytes = keys::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::PessimisticLock(ref mut key, _) => {
                    let bytes = keys::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::DeleteRange(_, ref mut key1, ref mut key2, _) => {
                    let bytes = keys::data_key(key1.as_encoded());
                    *key1 = Key::from_encoded(bytes);
                    let bytes = keys::data_end_key(key2.as_encoded());
                    *key2 = Key::from_encoded(bytes);
                }
            }
        }
        write_modifies(&self.kv_engine(), modifies)
    }

    fn async_write(
        &self,
        ctx: &Context,
        mut batch: WriteData,
        callback: EngineCallback<()>,
    ) -> EngineResult<()> {
        batch.modifies.iter_mut().for_each(|modify| match modify {
            Modify::Delete(_, ref mut key) => {
                *key = Key::from_encoded(keys::data_key(key.as_encoded()));
            }
            Modify::Put(_, ref mut key, _) => {
                *key = Key::from_encoded(keys::data_key(key.as_encoded()));
            }
            Modify::PessimisticLock(ref mut key, _) => {
                *key = Key::from_encoded(keys::data_key(key.as_encoded()));
            }
            Modify::DeleteRange(_, ref mut start_key, ref mut end_key, _) => {
                *start_key = Key::from_encoded(keys::data_key(start_key.as_encoded()));
                *end_key = Key::from_encoded(keys::data_end_key(end_key.as_encoded()));
            }
        });
        self.0.async_write(ctx, batch, callback)
    }

    fn async_snapshot(
        &self,
        ctx: SnapContext<'_>,
        callback: EngineCallback<Self::Snap>,
    ) -> EngineResult<()> {
        self.0.async_snapshot(
            ctx,
            Box::new(move |r| {
                callback(r.map(|snap| {
                    let mut region = Region::default();
                    // Add a peer to pass initialized check.
                    region.mut_peers().push(Peer::default());
                    RegionSnapshot::from_snapshot(snap, Arc::new(region))
                }))
            }),
        )
    }
}

pub struct MockSafePointProvider(pub u64);
impl GcSafePointProvider for MockSafePointProvider {
    fn get_safe_point(&self) -> Result<TimeStamp> {
        Ok(self.0.into())
    }
}

// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
fn check_need_gc(safe_point: TimeStamp, ratio_threshold: f64, props: &MvccProperties) -> bool {
    // Always GC.
    if ratio_threshold < 1.0 {
        return true;
    }

    // No data older than safe_point to GC.
    if props.min_ts > safe_point {
        return false;
    }

    // Note: Since the properties are file-based, it can be false positive.
    // For example, multiple files can have a different version of the same row.

    // A lot of MVCC versions to GC.
    if props.num_versions as f64 > props.num_rows as f64 * ratio_threshold {
        return true;
    }
    // A lot of non-effective MVCC versions to GC.
    if props.num_versions as f64 > props.num_puts as f64 * ratio_threshold {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_rocks::{raw::DB, Compat};
    use engine_traits::{MvccPropertiesExt, CF_WRITE};
    use kvproto::metapb::Region;

    use super::*;
    use crate::storage::mvcc::reader_tests::{make_region, open_db, RegionEngine};

    fn get_mvcc_properties_and_check_gc(
        db: Arc<DB>,
        region: Region,
        safe_point: impl Into<TimeStamp>,
        need_gc: bool,
    ) -> Option<MvccProperties> {
        let safe_point = safe_point.into();

        let start = keys::data_key(region.get_start_key());
        let end = keys::data_end_key(region.get_end_key());
        let props = db
            .c()
            .get_mvcc_properties_cf(CF_WRITE, safe_point, &start, &end);
        if let Some(props) = props.as_ref() {
            assert_eq!(check_need_gc(safe_point, 1.0, props), need_gc);
        }
        props
    }

    #[test]
    fn test_need_gc() {
        let path = tempfile::Builder::new()
            .prefix("test_storage_mvcc_reader")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![10]);
        test_without_properties(path, &region);
        test_with_properties(path, &region);
    }

    fn test_without_properties(path: &str, region: &Region) {
        let db = open_db(path, false);
        let mut engine = RegionEngine::new(&db, region);

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        assert!(
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).is_none()
        );
        engine.flush();
        // After this flush, we have a SST file without properties.
        // Without properties, we always need GC.
        assert!(
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).is_none()
        );
    }

    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, region);

        // Put 2 keys.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        // After this flush, we have a SST file w/ properties, plus the SST
        // file w/o properties from previous flush. We always need GC as
        // long as we can't get properties from any SST files.
        assert!(
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).is_none()
        );
        engine.compact();
        // After this compact, the two SST files are compacted into a new
        // SST file with properties. Now all SST files have properties and
        // all keys have only one version, so we don't need gc.
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // Put 2 more keys and delete them.
        engine.put(&[5], 5, 5);
        engine.put(&[6], 6, 6);
        engine.delete(&[5], 7, 7);
        engine.delete(&[6], 8, 8);
        engine.flush();
        // After this flush, keys 5,6 in the new SST file have more than one
        // versions, so we need gc.
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 8.into());
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 0, false).unwrap();
        assert_eq!(props.min_ts, TimeStamp::max());
        assert_eq!(props.max_ts, TimeStamp::zero());
        assert_eq!(props.num_rows, 0);
        assert_eq!(props.num_puts, 0);
        assert_eq!(props.num_versions, 0);
        assert_eq!(props.max_row_versions, 0);

        // We gc the two deleted keys manually.
        engine.gc(&[5], 10);
        engine.gc(&[6], 10);
        engine.compact();
        // After this compact, all versions of keys 5,6 are deleted,
        // no keys have more than one versions, so we don't need gc.
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, false).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props =
            get_mvcc_properties_and_check_gc(Arc::clone(&db), region.clone(), 10, true).unwrap();
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 9.into());
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }
}
