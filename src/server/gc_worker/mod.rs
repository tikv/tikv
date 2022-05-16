// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod applied_lock_collector;
mod compaction_filter;
mod config;
mod gc_manager;
mod gc_worker;
mod rawkv_compaction_filter;

// TODO: Use separated error type for GCWorker instead.
#[cfg(any(test, feature = "failpoints"))]
pub use compaction_filter::test_utils::{gc_by_compact, TestGCRunner};
pub use compaction_filter::WriteCompactionFilterFactory;
pub use config::{GcConfig, GcWorkerConfigManager, DEFAULT_GC_BATCH_KEYS};
use engine_traits::MvccProperties;
pub use gc_manager::AutoGcConfig;
pub use gc_worker::{sync_gc, GcSafePointProvider, GcTask, GcWorker, GC_MAX_EXECUTING_TASKS};
pub use rawkv_compaction_filter::RawCompactionFilterFactory;
use txn_types::TimeStamp;

pub use crate::storage::{Callback, Error, ErrorInner, Result};

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
