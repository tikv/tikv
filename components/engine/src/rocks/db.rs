// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use tikv_util::file::calc_crc32;
use tikv_util::keybuilder::KeyBuilder;

use super::{
    set_external_sst_file_global_seq_no, util, DBIterator, RocksIterator, RocksSnapshot,
    RocksWriteBatch, Writable, WriteBatch, DB,
};
use crate::options::*;
use crate::{Error, Iterable, KvEngine, Peekable, Result, CF_LOCK, MAX_DELETE_BATCH_SIZE};

#[derive(Clone)]
pub struct Rocks(Arc<DB>);

impl Deref for Rocks {
    type Target = DB;

    fn deref(&self) -> &DB {
        &self.0
    }
}

impl KvEngine for Rocks {
    type Snap = RocksSnapshot;
    type Batch = RocksWriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &RocksWriteBatch) -> Result<()> {
        DB::write_opt(self, wb.raw_ref(), &opts.into()).map_err(Error::Engine)
    }

    fn write_batch(&self, cap: usize) -> Result<RocksWriteBatch> {
        Ok(RocksWriteBatch::with_capacity(self.0.clone(), cap))
    }

    fn snapshot(&self) -> Result<RocksSnapshot> {
        Ok(RocksSnapshot::new(self.0.clone()))
    }

    fn sync(&self) -> Result<()> {
        self.sync_wal().map_err(Error::Engine)
    }

    fn cf_names(&self) -> Vec<&str> {
        DB::cf_names(self)
    }

    fn delete_all_in_range_cf(
        &self,
        opts: &DeleteRangeOptions,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        let handle = util::get_cf_handle(self, cf)?;
        if opts.use_delete_files {
            self.delete_files_in_range_cf(handle, start_key, end_key, false)?;
        } else {
            let wb = WriteBatch::default();
            if opts.use_delete_range && cf != CF_LOCK {
                wb.delete_range_cf(handle, start_key, end_key)
                    .map_err(Error::Engine)?;
            } else {
                let start = KeyBuilder::from_slice(start_key, 0, 0);
                let end = KeyBuilder::from_slice(end_key, 0, 0);
                let mut iter_opt = IterOptions::new(Some(start), Some(end), false);
                if self.is_titan() {
                    // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
                    // to avoid referring to missing blob files.
                    iter_opt.key_only(true);

                    let mut it = DBIterator::new_cf(self.clone(), handle, iter_opt.into());
                    it.seek(start_key.into());
                    while it.valid() {
                        wb.delete_cf(handle, it.key()).map_err(Error::Engine)?;
                        if wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                            // Can't use write_without_wal here.
                            // Otherwise it may cause dirty data when applying snapshot.
                            DB::write(self, &wb).map_err(Error::Engine)?;
                            wb.clear();
                        }

                        if !it.next() {
                            break;
                        }
                    }
                    it.status().map_err(Error::Engine)?;
                }
            }

            if wb.count() > 0 {
                DB::write(self, &wb).map_err(Error::Engine)?;
            }
        }
        Ok(())
    }

    fn ingest_external_file_cf(
        &self,
        opts: &IngestExternalFileOptions,
        cf: &str,
        files: &[&str],
    ) -> Result<()> {
        let handle = util::get_cf_handle(self, cf)?;
        DB::ingest_external_file_cf(self, &handle, &opts.into(), files)?;
        Ok(())
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        let f = File::open(path)?;

        let meta = f.metadata()?;
        if meta.len() != expected_size {
            return Err(Error::Engine(format!(
                "invalid size {} for {}, expected {}",
                meta.len(),
                path,
                expected_size
            )));
        }

        let checksum = calc_crc32(path)?;
        if checksum == expected_checksum {
            return Ok(());
        }

        // RocksDB may have modified the global seqno.
        let cf_handle = util::get_cf_handle(self, cf)?;
        set_external_sst_file_global_seq_no(self, cf_handle, path, 0)?;
        f.sync_all()
            .map_err(|e| format!("sync {}: {:?}", path, e))?;

        let checksum = calc_crc32(path)?;
        if checksum != expected_checksum {
            return Err(Error::Engine(format!(
                "invalid checksum {} for {}, expected {}",
                checksum, path, expected_checksum
            )));
        }

        Ok(())
    }
}

impl Iterable for Rocks {
    type Iter = RocksIterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        Ok(DBIterator::new(self.0.clone(), opts.into()))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let handle = util::get_cf_handle(self, cf)?;
        Ok(DBIterator::new_cf(self.0.clone(), handle, opts.into()))
    }
}

impl Peekable for Rocks {
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let v = DB::get_opt(self, key, &opts.into())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let handle = util::get_cf_handle(self, cf)?;
        let v = DB::get_cf_opt(self, handle, key, &opts.into())?;
        Ok(v.map(|v| v.to_vec()))
    }
}
