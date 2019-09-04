// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{util, RawWriteBatch, Writable, DB};
use crate::{Error, Mutable, Result, WriteOptions};
use std::sync::Arc;

pub struct RocksWriteBatch {
    db: Arc<DB>,
    wb: RawWriteBatch,
}

impl AsRef<RawWriteBatch> for RocksWriteBatch {
    fn as_ref(&self) -> &RawWriteBatch {
        &self.wb
    }
}

impl RocksWriteBatch {
    pub fn new(db: Arc<DB>) -> RocksWriteBatch {
        RocksWriteBatch {
            db,
            wb: RawWriteBatch::default(),
        }
    }

    pub fn with_capacity(db: Arc<DB>, cap: usize) -> RocksWriteBatch {
        RocksWriteBatch {
            db,
            wb: RawWriteBatch::with_capacity(cap),
        }
    }

    pub fn from_raw(db: Arc<DB>, wb: RawWriteBatch) -> RocksWriteBatch {
        RocksWriteBatch { db, wb }
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }
}

impl crate::WriteBatch for RocksWriteBatch {
    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn clear(&self) {
        self.wb.clear();
    }

    fn set_save_point(&mut self) {
        self.wb.set_save_point();
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.wb.pop_save_point().map_err(Error::Engine)
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.wb.rollback_to_save_point().map_err(Error::Engine)
    }
}

impl Mutable for RocksWriteBatch {
    fn put_opt(&self, _: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::Engine)
    }

    fn put_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = util::get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete_opt(&self, _: &WriteOptions, key: &[u8]) -> Result<()> {
        self.wb.delete(key).map_err(Error::Engine)
    }

    fn delete_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8]) -> Result<()> {
        let handle = util::get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.delete_cf(handle, key).map_err(Error::Engine)
    }
}
