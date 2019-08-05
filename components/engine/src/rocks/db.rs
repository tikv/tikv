// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::{
    util, DBIterator, DBOptions, RocksIterator, RocksSnapshot, RocksWriteBatch, Writable,
    WriteBatch, DB,
};
use crate::options::*;
use crate::{Error, Iterable, KVEngine, Result, CF_LOCK, MAX_DELETE_BATCH_SIZE};
use tikv_util::keybuilder::KeyBuilder;

pub struct Rocks {
    db: Arc<DB>,
}

impl Rocks {
    pub fn open(opts: DBOptions, path: &str) -> Result<Self> {
        let db = DB::open(opts, path).map_err(Error::Engine)?;
        Ok(Rocks { db: Arc::new(db) })
    }

    pub fn get_db(&self) -> Arc<DB> {
        self.db.clone()
    }
}

impl KVEngine for Rocks {
    type Snap = RocksSnapshot;
    type Batch = RocksWriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &RocksWriteBatch) -> Result<()> {
        self.db
            .write_opt(wb.raw_ref(), &opts.into())
            .map_err(Error::Engine)
    }

    fn write_batch(&self) -> Result<RocksWriteBatch> {
        Ok(RocksWriteBatch::new(self.db.clone()))
    }

    fn snapshot(&self) -> Result<RocksSnapshot> {
        Ok(RocksSnapshot::new(self.db.clone()))
    }

    fn sync(&self) -> Result<()> {
        self.db.sync_wal().map_err(Error::Engine)
    }

    fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
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

        let handle = util::get_cf_handle(&self.db, cf)?;
        if opts.get_use_delete_files() {
            self.db
                .delete_files_in_range_cf(handle, start_key, end_key, false)?;
        } else {
            let wb = WriteBatch::default();
            if opts.get_use_delete_range() && cf != CF_LOCK {
                wb.delete_range_cf(handle, start_key, end_key)
                    .map_err(Error::Engine)?;
            } else {
                let start = KeyBuilder::from_slice(start_key, 0, 0);
                let end = KeyBuilder::from_slice(end_key, 0, 0);
                let mut iter_opt = IterOptions::new(Some(start), Some(end), false);
                if self.db.is_titan() {
                    // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
                    // to avoid referring to missing blob files.
                    iter_opt.set_key_only(true);

                    let mut it = DBIterator::new_cf(self.db.clone(), handle, iter_opt.into());
                    it.seek(start_key.into());
                    while it.valid() {
                        wb.delete_cf(handle, it.key()).map_err(Error::Engine)?;
                        if wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                            // Can't use write_without_wal here.
                            // Otherwise it may cause dirty data when applying snapshot.
                            self.db.write(&wb).map_err(Error::Engine)?;
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
                self.db.write(&wb).map_err(Error::Engine)?;
            }
        }
        Ok(())
    }

    //    fn ingest_file_cf(&self, meta: &DataFileMeta) {
    //        self.db.ingest_external_file_cf()
    //    }
}

impl Iterable for Rocks {
    type Iter = RocksIterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        Ok(DBIterator::new(self.db.clone(), opts.into()))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let handle = util::get_cf_handle(&self.db, cf)?;
        Ok(DBIterator::new_cf(self.db.clone(), handle, opts.into()))
    }
}
