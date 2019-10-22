// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use engine_traits::{
    Error, IterOptions, Iterable, KvEngine, Mutable, Peekable, ReadOptions, Result, WriteOptions,
};
use rocksdb::{DBIterator, Writable, DB};

use crate::options::{RocksReadOptions, RocksWriteOptions};
use crate::util::get_cf_handle;
use crate::{RocksEngineIterator, Snapshot};

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct RocksEngine(Arc<DB>);

impl RocksEngine {
    pub fn from_db(db: Arc<DB>) -> Self {
        RocksEngine(db)
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const RocksEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.0
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.0.clone()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty directory
        // but db has not been created, `DB::list_column_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }
}

impl KvEngine for RocksEngine {
    type Snapshot = Snapshot;
    type WriteBatch = crate::WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::WriteBatch) -> Result<()> {
        if wb.get_db().path() != self.0.path() {
            return Err(Error::Engine("mismatched db path".to_owned()));
        }
        let opt: RocksWriteOptions = opts.into();
        self.0
            .write_opt(wb.as_ref(), &opt.into_raw())
            .map_err(Error::Engine)
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        Self::WriteBatch::with_capacity(Arc::clone(&self.0), cap)
    }

    fn write_batch(&self) -> Self::WriteBatch {
        Self::WriteBatch::new(Arc::clone(&self.0))
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.0.clone())
    }

    fn sync(&self) -> Result<()> {
        self.0.sync_wal().map_err(Error::Engine)
    }

    fn cf_names(&self) -> Vec<&str> {
        self.0.cf_names()
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new(
            self.0.clone(),
            opt.into_raw(),
        )))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iterator> {
        let handle = get_cf_handle(&self.0, cf)?;
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.0.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for RocksEngine {
    fn get_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt: RocksReadOptions = opts.into();
        let v = self.0.get_opt(key, &opt.into_raw())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt: RocksReadOptions = opts.into();
        let handle = get_cf_handle(&self.0, cf)?;
        let v = self.0.get_cf_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(|v| v.to_vec()))
    }
}

impl Mutable for RocksEngine {
    fn put_opt(&self, _: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.put(key, value).map_err(Error::Engine)
    }

    fn put_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete_opt(&self, _: &WriteOptions, key: &[u8]) -> Result<()> {
        self.0.delete(key).map_err(Error::Engine)
    }

    fn delete_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.delete_cf(handle, key).map_err(Error::Engine)
    }
}
