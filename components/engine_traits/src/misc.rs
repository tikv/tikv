// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::{
    cf_names::CfNamesExt, errors::Result, flow_control_factors::FlowControlFactorsExt,
    range::Range, WriteBatchExt, WriteOptions,
};

#[derive(Clone, Debug)]
pub enum DeleteStrategy {
    /// Delete the SST files that are fullly fit in range. However, the SST
    /// files that are partially overlapped with the range will not be
    /// touched.
    ///
    /// Note:
    ///    - After this operation, some keys in the range might still exist in
    ///      the database.
    ///    - After this operation, some keys in the range might be removed from
    ///      existing snapshot, so you shouldn't expect to be able to read data
    ///      from the range using existing snapshots any more.
    ///
    /// Ref: <https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys>
    DeleteFiles,
    /// Delete the data stored in Titan.
    DeleteBlobs,
    /// Scan for keys and then delete. Useful when we know the keys in range are
    /// not too many.
    DeleteByKey,
    /// Delete by range. Note that this is experimental and you should check
    /// whether it is enbaled in config before using it.
    DeleteByRange,
    /// Delete by ingesting a SST file with deletions. Useful when the number of
    /// ranges is too many.
    DeleteByWriter { sst_path: String },
}

/// `StatisticsReporter` can be used to report engine's private statistics to
/// prometheus metrics. For one single engine, using it is equivalent to calling
/// `KvEngine::flush_metrics("name")`. For multiple engines, it can aggregate
/// statistics accordingly.
/// Note that it is not responsible for managing the statistics from
/// user-provided collectors that are potentially shared between engines.
pub trait StatisticsReporter<T: ?Sized> {
    fn new(name: &str) -> Self;

    /// Collect statistics from one single engine.
    fn collect(&mut self, engine: &T);

    /// Aggregate and report statistics to prometheus metrics counters. The
    /// statistics are not cleared afterwards.
    fn flush(&mut self);
}

#[derive(Default)]
pub struct RangeStats {
    // The number of entries in write cf.
    pub num_entries: u64,
    // The number of MVCC versions of all rows (num_entries - tombstones).
    pub num_versions: u64,
    // The number of rows.
    pub num_rows: u64,
    // The number of MVCC deletes of all rows.
    pub num_deletes: u64,
}

impl RangeStats {
    /// The number of redundant keys in the range.
    /// It's calculated by `num_entries - num_versions + num_deleted`.
    pub fn redundant_keys(&self) -> u64 {
        // Consider the number of `mvcc_deletes` as the number of redundant keys.
        self.num_entries
            .saturating_sub(self.num_rows)
            .saturating_add(self.num_deletes)
    }
}

pub trait MiscExt: CfNamesExt + FlowControlFactorsExt + WriteBatchExt {
    type StatisticsReporter: StatisticsReporter<Self>;

    /// Flush all specified column families at once.
    ///
    /// If `cfs` is empty, it will try to flush all available column families.
    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()>;

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()>;

    /// Returns `false` if all memtables are created after `threshold`.
    fn flush_oldest_cf(&self, wait: bool, threshold: Option<std::time::SystemTime>)
    -> Result<bool>;

    /// Returns whether there's data written through kv interface.
    fn delete_ranges_cfs(
        &self,
        wopts: &WriteOptions,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<bool> {
        let mut written = false;
        for cf in self.cf_names() {
            written |= self.delete_ranges_cf(wopts, cf, strategy.clone(), ranges)?;
        }
        Ok(written)
    }

    /// Returns whether there's data written through kv interface.
    fn delete_ranges_cf(
        &self,
        wopts: &WriteOptions,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<bool>;

    /// Return the approximate number of records and size in the range of
    /// memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool>;

    fn get_sst_key_ranges(&self, cf: &str, level: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Gets total used size of rocksdb engine, including:
    /// * total size (bytes) of all SST files.
    /// * total size (bytes) of active and unflushed immutable memtables.
    /// * total size (bytes) of all blob files.
    fn get_engine_used_size(&self) -> Result<u64>;

    /// The path to the directory on the filesystem where the database is stored
    fn path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;

    /// Disable manual compactions, some on-going manual compactions may be
    /// aborted.
    fn disable_manual_compaction(&self) -> Result<()>;

    fn enable_manual_compaction(&self) -> Result<()>;

    /// Depending on the implementation, some on-going manual compactions may be
    /// aborted.
    fn pause_background_work(&self) -> Result<()>;

    fn continue_background_work(&self) -> Result<()>;

    /// Check whether a database exists at a given path
    fn exists(path: &str) -> bool;

    fn locked(path: &str) -> Result<bool>;

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>;

    fn get_latest_sequence_number(&self) -> u64;

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64>;

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>>;

    fn get_num_keys(&self) -> Result<u64>;

    fn get_range_stats(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Option<RangeStats>>;

    fn is_stalled_or_stopped(&self) -> bool;

    /// Returns size and creation time of active memtable if there's one.
    fn get_active_memtable_stats_cf(
        &self,
        cf: &str,
    ) -> Result<Option<(u64, std::time::SystemTime)>>;

    /// Whether there's active memtable with creation time older than
    /// `threshold`.
    fn has_old_active_memtable(&self, threshold: std::time::SystemTime) -> bool {
        for cf in self.cf_names() {
            if let Ok(Some((_, age))) = self.get_active_memtable_stats_cf(cf) {
                if age < threshold {
                    return true;
                }
            }
        }
        false
    }

    // Global method.
    fn get_accumulated_flush_count_cf(cf: &str) -> Result<u64>;

    fn get_accumulated_flush_count() -> Result<u64> {
        let mut n = 0;
        for cf in crate::ALL_CFS {
            n += Self::get_accumulated_flush_count_cf(cf)?;
        }
        Ok(n)
    }

    type DiskEngine;
    fn get_disk_engine(&self) -> &Self::DiskEngine;
}
