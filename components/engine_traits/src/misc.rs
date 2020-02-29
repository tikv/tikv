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
use crate::write_batch::{WriteBatch, WriteBatchExt};

use tikv_util::keybuilder::KeyBuilder;

// FIXME: Find somewhere else to put this?
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

pub trait MiscExt: Iterable + WriteBatchExt + CFNamesExt {
    fn is_titan(&self) -> bool {
        false
    }

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
        let wb = self.write_batch();
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
}
