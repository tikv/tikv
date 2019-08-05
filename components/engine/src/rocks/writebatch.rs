// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{util, Writable, WriteBatch, DB};
use crate::{Error, Mutable, Result, WriteOptions};
use std::sync::Arc;

pub struct RocksWriteBatch {
    db: Arc<DB>,
    wb: WriteBatch,
}

impl RocksWriteBatch {
    pub fn new(db: Arc<DB>) -> RocksWriteBatch {
        RocksWriteBatch {
            db,
            wb: WriteBatch::default(),
        }
    }

    pub fn raw_ref(&self) -> &WriteBatch {
        &self.wb
    }
}

impl crate::WriteBatch for RocksWriteBatch {}

impl Mutable for RocksWriteBatch {
    fn put_value_opt(&self, _: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::Engine)
    }

    fn put_value_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
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
