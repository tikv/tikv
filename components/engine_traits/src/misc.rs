// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::cf_names::CFNamesExt;
use crate::errors::Result;
<<<<<<< HEAD
use crate::iterable::{Iterable, Iterator};
use crate::mutable::Mutable;
use crate::options::IterOptions;
use crate::range::Range;
use crate::write_batch::{WriteBatch, WriteBatchExt};

use tikv_util::keybuilder::KeyBuilder;
=======
use crate::range::Range;
>>>>>>> d4880b2... titan: Fix possible panic of generating snap when doing UnsafeDestroyRange (#8708)

// FIXME: Find somewhere else to put this?
pub const MAX_DELETE_BATCH_COUNT: usize = 512;

<<<<<<< HEAD
pub trait MiscExt: Iterable + WriteBatchExt + CFNamesExt {
    fn is_titan(&self) -> bool {
        false
    }
=======
pub trait MiscExt: CFNamesExt {
    fn flush(&self, sync: bool) -> Result<()>;
>>>>>>> d4880b2... titan: Fix possible panic of generating snap when doing UnsafeDestroyRange (#8708)

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()>;

    fn delete_all_files_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_files_in_range_cf(cf, start_key, end_key, false)?;
        }

        Ok(())
    }

    fn delete_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        include_end: bool,
    ) -> Result<()>;

    fn delete_blob_files_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_blob_files_in_range_cf(cf, start_key, end_key, false)?;
        }

        Ok(())
    }

    fn delete_blob_files_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        include_end: bool,
    ) -> Result<()>;

    fn delete_all_in_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        use_delete_range: bool,
    ) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_all_in_range_cf(cf, start_key, end_key, use_delete_range)?;
        }

        Ok(())
    }

    fn delete_all_in_range_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        use_delete_range: bool,
    ) -> Result<()>;

    /// Return the approximate number of records and size in the range of memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)>;
}
