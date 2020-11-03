// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::cf_names::CFNamesExt;
use crate::errors::Result;
use crate::range::Range;

#[derive(Clone, Debug)]
pub enum DeleteStrategy {
    DeleteFiles,
    DeleteBlobs,
    DeleteByKey,
    DeleteByRange,
    DeleteByWriter { sst_path: String },
}

pub trait MiscExt: CFNamesExt {
    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()>;

    fn delete_all_in_range(&self, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        for cf in self.cf_names() {
            self.delete_ranges_cf(cf, strategy.clone(), ranges)?;
        }
        Ok(())
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()>;

    /// Return the approximate number of records and size in the range of memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)>;

    fn get_latest_sequence_number(&self) -> u64 {
        0
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        None
    }
}
