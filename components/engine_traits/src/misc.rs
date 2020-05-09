// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::cf_defs::CF_LOCK;
use crate::cf_names::CFNamesExt;
use crate::errors::Result;
use crate::iterable::{Iterable, Iterator};
use crate::mutable::Mutable;
use crate::options::IterOptions;
use crate::range::Range;
use crate::write_batch::{WriteBatch, WriteBatchExt};

use tikv_util::keybuilder::KeyBuilder;

// FIXME: Find somewhere else to put this?
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

pub trait MiscExt: Iterable + WriteBatchExt + CFNamesExt {
    fn is_titan(&self) -> bool {
        false
    }

    fn flush(&self, sync: bool) -> Result<()>;

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()>;

    fn delete_files_in_range_cf(
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
    ) -> Result<()> {
        let mut wb = self.write_batch();
        if use_delete_range && cf != CF_LOCK {
            wb.delete_range_cf(cf, start_key, end_key)?;
        } else {
            let start = KeyBuilder::from_slice(start_key, 0, 0);
            let end = KeyBuilder::from_slice(end_key, 0, 0);
            let mut iter_opt = IterOptions::new(Some(start), Some(end), false);
            if self.is_titan() {
                // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
                // to avoid referring to missing blob files.
                iter_opt.set_key_only(true);
            }
            let mut it = self.iterator_cf_opt(cf, iter_opt)?;
            let mut it_valid = it.seek(start_key.into())?;
            while it_valid {
                wb.delete_cf(cf, it.key())?;
                if wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                    // Can't use write_without_wal here.
                    // Otherwise it may cause dirty data when applying snapshot.
                    self.write(&wb)?;
                    wb.clear();
                }
                it_valid = it.next()?;
            }
        }

        if wb.count() > 0 {
            self.write(&wb)?;
        }

        Ok(())
    }

    fn delete_all_files_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        for cf in self.cf_names() {
            self.delete_files_in_range_cf(cf, start_key, end_key, false)?;
        }

        Ok(())
    }

    /// Return the approximate number of records and size in the range of memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool>;

    /// Gets total used size of rocksdb engine, including:
    /// *  total size (bytes) of all SST files.
    /// *  total size (bytes) of active and unflushed immutable memtables.
    /// *  total size (bytes) of all blob files.
    ///
    fn get_engine_used_size(&self) -> Result<u64>;

    /// Roughly deletes files in multiple ranges.
    ///
    /// Note:
    ///    - After this operation, some keys in the range might still exist in the database.
    ///    - After this operation, some keys in the range might be removed from existing snapshot,
    ///      so you shouldn't expect to be able to read data from the range using existing snapshots
    ///      any more.
    ///
    /// Ref: https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys
    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()>;

    fn path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;
}
