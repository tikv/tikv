// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

pub mod compaction_filter;
mod config;
mod gc_manager;
mod gc_worker;
pub mod rawkv_compaction_filter;

// TODO: Use separated error type for GcWorker instead.
#[cfg(any(test, feature = "failpoints"))]
pub use compaction_filter::test_utils::{gc_by_compact, TestGcRunner};
pub use compaction_filter::WriteCompactionFilterFactory;
pub use config::{GcConfig, GcWorkerConfigManager, DEFAULT_GC_BATCH_KEYS};
use engine_traits::MvccProperties;
pub use gc_manager::{AutoGcConfig};
#[cfg(any(test, feature = "testexport"))]
pub use gc_worker::test_gc_worker::{MockSafePointProvider, PrefixedEngine};
pub use gc_worker::{
    sync_gc, GcSafePointProvider, GcTask, GcWorker, STAT_RAW_KEYMODE, STAT_TXN_KEYMODE,
};
pub use rawkv_compaction_filter::RawCompactionFilterFactory;
use txn_types::TimeStamp;

pub use crate::storage::{Callback, Error, ErrorInner, Result};

// Returns true if it needs gc.
// This is for optimization purpose, does not mean to be accurate.
fn check_need_gc(safe_point: TimeStamp, ratio_threshold: f64, props: &MvccProperties) -> bool {
    // Disable GC directly once the config is negative or +inf.
    // Disabling GC is useful in some abnormal scenarios where the transaction model
    // would be break (e.g. writes with higher commit TS would be written BEFORE
    // writes with lower commit TS, or write data with TS lower than current GC safe
    // point). Use this at your own risk.
    if ratio_threshold.is_sign_negative() || ratio_threshold.is_infinite() {
        return false;
    }
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
    use engine_rocks::RocksEngine;
    use engine_traits::{MvccPropertiesExt, CF_WRITE};
    use kvproto::metapb::Region;

    use super::*;
    use crate::storage::mvcc::reader_tests::{make_region, open_db, RegionEngine};

    fn get_mvcc_properties_and_check_gc(
        db: &RocksEngine,
        region: Region,
        safe_point: impl Into<TimeStamp>,
        need_gc: bool,
    ) -> MvccProperties {
        let safe_point = safe_point.into();

        let start = keys::data_key(region.get_start_key());
        let end = keys::data_end_key(region.get_end_key());
        let props = db
            .get_mvcc_properties_cf(CF_WRITE, safe_point, &start, &end)
            .unwrap();
        assert_eq!(check_need_gc(safe_point, 1.0, &props), need_gc);
        props
    }

    #[test]
    fn test_check_need_gc() {
        let props = MvccProperties::default();
        assert!(!check_need_gc(TimeStamp::max(), -1.0, &props));
        assert!(!check_need_gc(TimeStamp::max(), f64::INFINITY, &props));
        assert!(check_need_gc(TimeStamp::max(), 0.9, &props));
    }

    #[test]
    fn test_need_gc() {
        let path = tempfile::Builder::new()
            .prefix("test_storage_mvcc_reader")
            .tempdir()
            .unwrap();
        let path = path.path().to_str().unwrap();
        let region = make_region(1, vec![0], vec![10]);
        test_with_properties(path, &region);
    }

    fn test_with_properties(path: &str, region: &Region) {
        let db = open_db(path, true);
        let mut engine = RegionEngine::new(&db, region);

        // Put 2 keys.
        engine.put(&[1], 1, 1);
        engine.put(&[4], 2, 2);
        // Put 2 keys.
        engine.put(&[2], 3, 3);
        engine.put(&[3], 4, 4);
        engine.flush();
        engine.compact();
        let props = get_mvcc_properties_and_check_gc(&db, region.clone(), 10, false);
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
        let props = get_mvcc_properties_and_check_gc(&db, region.clone(), 10, true);
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 8.into());
        assert_eq!(props.num_rows, 6);
        assert_eq!(props.num_puts, 6);
        assert_eq!(props.num_versions, 8);
        assert_eq!(props.max_row_versions, 2);
        // But if the `safe_point` is older than all versions, we don't need gc too.
        let props = get_mvcc_properties_and_check_gc(&db, region.clone(), 0, false);
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
        let props = get_mvcc_properties_and_check_gc(&db, region.clone(), 10, false);
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 4.into());
        assert_eq!(props.num_rows, 4);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 4);
        assert_eq!(props.max_row_versions, 1);

        // A single lock version need gc.
        engine.lock(&[7], 9, 9);
        engine.flush();
        let props = get_mvcc_properties_and_check_gc(&db, region.clone(), 10, true);
        assert_eq!(props.min_ts, 1.into());
        assert_eq!(props.max_ts, 9.into());
        assert_eq!(props.num_rows, 5);
        assert_eq!(props.num_puts, 4);
        assert_eq!(props.num_versions, 5);
        assert_eq!(props.max_row_versions, 1);
    }
}
