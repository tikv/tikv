// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::{
    cf_names::CfNamesExt, errors::Result, flow_control_factors::FlowControlFactorsExt, range::Range,
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
    // The number of entries
    pub num_entries: u64,
    // The number of MVCC versions of all rows (num_entries - tombstones).
    pub num_versions: u64,
    // The number of rows.
    pub num_rows: u64,
}

pub trait MiscExt: CfNamesExt + FlowControlFactorsExt {
    type StatisticsReporter: StatisticsReporter<Self>;

    /// Flush all specified column families at once.
    ///
    /// If `cfs` is empty, it will try to flush all available column families.
    fn flush_cfs(&self, cfs: &[&str], wait: bool) -> Result<()>;

    fn flush_cf(&self, cf: &str, wait: bool) -> Result<()>;

    fn delete_ranges_cfs(&self, strategy: DeleteStrategy, ranges: &[Range<'_>]) -> Result<()> {
        for cf in self.cf_names() {
            self.delete_ranges_cf(cf, strategy.clone(), ranges)?;
        }
        Ok(())
    }

    fn delete_ranges_cf(
        &self,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()>;

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
}
