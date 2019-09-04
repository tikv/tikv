// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use tikv_util::file::calc_crc32;

use super::{
    set_external_sst_file_global_seq_no, util, DBIterator, Iterator, Snapshot, Writable,
    WriteBatch, DB,
};
use crate::options::*;
use crate::rocks::util::get_cf_handle;
use crate::util as engine_util;
use crate::{Error, Iterable, KvEngine, Mutable, Peekable, Result};

#[derive(Clone)]
#[repr(transparent)]
pub struct Rocks(Arc<DB>);

impl Rocks {
    pub fn from_db(db: Arc<DB>) -> Self {
        Rocks(db)
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.0.clone()
    }
}

impl AsRef<DB> for Rocks {
    fn as_ref(&self) -> &DB {
        self.0.deref()
    }
}

impl AsMut<DB> for Rocks {
    fn as_mut(&mut self) -> &mut DB {
        Arc::get_mut(&mut self.0).unwrap()
    }
}

impl AsRef<Rocks> for Arc<DB> {
    fn as_ref(&self) -> &Rocks {
        unsafe { &*(self as *const Arc<DB> as *const Rocks) }
    }
}

impl AsMut<Rocks> for Arc<DB> {
    fn as_mut(&mut self) -> &mut Rocks {
        unsafe { &mut *(self as *mut Arc<DB> as *mut Rocks) }
    }
}

impl KvEngine for Rocks {
    type Snap = Snapshot;
    type Batch = WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &WriteBatch) -> Result<()> {
        if wb.get_db().path() != self.0.path() {
            return Err(Error::Engine("mismatched db path".to_owned()));
        }
        DB::write_opt(&self.0, wb.as_ref(), &opts.into()).map_err(Error::Engine)
    }

    fn write_batch(&self, cap: usize) -> WriteBatch {
        WriteBatch::with_capacity(self.0.clone(), cap)
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.0.clone())
    }

    fn sync(&self) -> Result<()> {
        self.0.sync_wal().map_err(Error::Engine)
    }

    fn cf_names(&self) -> Vec<&str> {
        DB::cf_names(self.as_ref())
    }

    fn delete_all_in_range_cf(
        &self,
        t: DeleteRangeType,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }

        let db: &DB = self.0.as_ref();
        match t {
            DeleteRangeType::Normal => engine_util::delete_all_in_range_cf(
                db, cf, start_key, end_key, false, /* use_delete_range*/
            ),
            DeleteRangeType::DeleteRange => engine_util::delete_all_in_range_cf(
                db, cf, start_key, end_key, true, /* use_delete_range*/
            ),
            DeleteRangeType::DeleteFiles => {
                let handle = get_cf_handle(db, cf)?;
                db.delete_files_in_range_cf(handle, start_key, end_key, false)?;
                Ok(())
            }
        }
    }

    fn ingest_external_file_cf(
        &self,
        opts: &IngestExternalFileOptions,
        cf: &str,
        files: &[&str],
    ) -> Result<()> {
        let handle = util::get_cf_handle(self.as_ref(), cf)?;
        DB::ingest_external_file_cf(self.as_ref(), &handle, &opts.into(), files)?;
        Ok(())
    }

    fn validate_file_for_ingestion<P: AsRef<Path>>(
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
        let cf_handle = util::get_cf_handle(self.as_ref(), cf)?;
        set_external_sst_file_global_seq_no(self.as_ref(), cf_handle, path, 0)?;
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
    type Iter = Iterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        Ok(DBIterator::new(self.0.clone(), opts.into()))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let handle = util::get_cf_handle(self.as_ref(), cf)?;
        Ok(DBIterator::new_cf(self.0.clone(), handle, opts.into()))
    }
}

impl Peekable for Rocks {
    fn get_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let v = DB::get_opt(self.as_ref(), key, &opts.into())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = util::get_cf_handle(self.as_ref(), cf)?;
        let v = DB::get_cf_opt(self.as_ref(), handle, key, &opts.into())?;
        Ok(v.map(|v| v.to_vec()))
    }
}

impl Mutable for Rocks {
    fn put_opt(&self, _: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        DB::put(self.as_ref(), key, value).map_err(Error::Engine)
    }

    fn put_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = util::get_cf_handle(self.as_ref(), cf)?;
        DB::put_cf(self.as_ref(), handle, key, value).map_err(Error::Engine)
    }

    fn delete_opt(&self, _: &WriteOptions, key: &[u8]) -> Result<()> {
        DB::delete(self.as_ref(), key).map_err(Error::Engine)
    }

    fn delete_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8]) -> Result<()> {
        let handle = util::get_cf_handle(self.as_ref(), cf)?;
        DB::delete_cf(self.as_ref(), handle, key).map_err(Error::Engine)
    }
}
