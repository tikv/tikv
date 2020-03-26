// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::engine::RocksEngine;
use crate::options::RocksWriteOptions;
use crate::util::get_cf_handle;
use engine_traits::{self, Error, Mutable, Result, WriteBatchExt, WriteBatchVecExt, WriteOptions};
use rocksdb::{Writable, WriteBatch as RawWriteBatch, DB};

pub const WRITE_BATCH_MAX_KEYS: usize = 256;
pub const WRITE_BATCH_MAX_BATCH: usize = 16;

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatch;
    type WriteBatchVec = RocksWriteBatchVec;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()> {
        debug_assert_eq!(
            wb.get_db().path(),
            self.as_inner().path(),
            "mismatched db path"
        );
        let opt: RocksWriteOptions = opts.into();
        self.as_inner()
            .write_opt(wb.as_inner(), &opt.into_raw())
            .map_err(Error::Engine)
    }

    fn write_vec_opt(&self, wb: &RocksWriteBatchVec, opts: &WriteOptions) -> Result<()> {
        let opt: RocksWriteOptions = opts.into();
        if wb.index > 0 {
            self.as_inner()
                .multi_batch_write(wb.as_inner(), &opt.into_raw())
                .map_err(Error::Engine)
        } else {
            self.as_inner()
                .write_opt(&wb.wbs[0], &opt.into_raw())
                .map_err(Error::Engine)
        }
    }

    fn support_write_batch_vec(&self) -> bool {
        let options = self.as_inner().get_db_options();
        options.is_enable_multi_batch_write()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        Self::WriteBatch::new(Arc::clone(&self.as_inner()))
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        Self::WriteBatch::with_capacity(Arc::clone(&self.as_inner()), cap)
    }

    fn write_batch_vec(&self, vec_size: usize, cap: usize) -> Self::WriteBatchVec {
        RocksWriteBatchVec::new(Arc::clone(&self.as_inner()), vec_size, cap)
    }
}

pub struct RocksWriteBatch {
    db: Arc<DB>,
    wb: RawWriteBatch,
}

impl RocksWriteBatch {
    pub fn new(db: Arc<DB>) -> RocksWriteBatch {
        RocksWriteBatch {
            db,
            wb: RawWriteBatch::default(),
        }
    }

    pub fn as_inner(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn with_capacity(db: Arc<DB>, cap: usize) -> RocksWriteBatch {
        let wb = if cap == 0 {
            RawWriteBatch::default()
        } else {
            RawWriteBatch::with_capacity(cap)
        };
        RocksWriteBatch { db, wb }
    }

    pub fn from_raw(db: Arc<DB>, wb: RawWriteBatch) -> RocksWriteBatch {
        RocksWriteBatch { db, wb }
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }
}

impl engine_traits::WriteBatch for RocksWriteBatch {
    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.wb.count() > WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
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
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wb.delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.delete_cf(handle, key).map_err(Error::Engine)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

/// `RocksWriteBatchVec` is for method `multi_batch_write` of RocksDB, which splits a large WriteBatch
/// into many smaller ones and then any thread could help to deal with these small WriteBatch when it
/// is calling `AwaitState` and wait to become leader of WriteGroup. `multi_batch_write` will perform
/// much better than traditional `pipelined_write` when TiKV writes very large data into RocksDB. We
/// will remove this feature when `unordered_write` of RocksDB becomes more stable and becomes compatible
/// with Titan.
pub struct RocksWriteBatchVec {
    db: Arc<DB>,
    wbs: Vec<RawWriteBatch>,
    save_points: Vec<usize>,
    index: usize,
    cur_batch_size: usize,
    batch_size_limit: usize,
}

impl RocksWriteBatchVec {
    pub fn new(db: Arc<DB>, batch_size_limit: usize, cap: usize) -> RocksWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            db,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            cur_batch_size: 0,
            batch_size_limit,
        }
    }

    pub fn as_inner(&self) -> &[RawWriteBatch] {
        &self.wbs[0..=self.index]
    }

    pub fn as_raw(&self) -> &RawWriteBatch {
        &self.wbs[0]
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    /// `check_switch_batch` will split a large WriteBatch into many smaller ones. This is to avoid
    /// a large WriteBatch blocking write_thread too long.
    fn check_switch_batch(&mut self) {
        if self.batch_size_limit > 0 && self.cur_batch_size >= self.batch_size_limit {
            self.index += 1;
            self.cur_batch_size = 0;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::default());
            }
        }
        self.cur_batch_size += 1;
    }
}

impl engine_traits::WriteBatch for RocksWriteBatchVec {
    fn data_size(&self) -> usize {
        self.wbs.iter().fold(0, |a, b| a + b.data_size())
    }

    fn count(&self) -> usize {
        self.cur_batch_size + self.index * self.batch_size_limit
    }

    fn is_empty(&self) -> bool {
        self.wbs[0].is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.wbs.len() > WRITE_BATCH_MAX_BATCH
    }

    fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        self.index = 0;
        self.cur_batch_size = 0;
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(Error::Engine);
        }
        Err(Error::Engine("no save point".into()))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(Error::Engine);
        }
        Err(Error::Engine("no save point".into()))
    }
}

impl Mutable for RocksWriteBatchVec {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .put_cf(handle, key, value)
            .map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .delete_cf(handle, key)
            .map_err(Error::Engine)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}
impl WriteBatchVecExt<RocksEngine> for RocksWriteBatch {
    fn write_batch_vec(e: &RocksEngine, _vec_size: usize, cap: usize) -> RocksWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_to_engine(&self, e: &RocksEngine, opts: &WriteOptions) -> Result<()> {
        e.write_opt(self, opts)
    }
}

impl WriteBatchVecExt<RocksEngine> for RocksWriteBatchVec {
    fn write_batch_vec(e: &RocksEngine, vec_size: usize, cap: usize) -> RocksWriteBatchVec {
        e.write_batch_vec(vec_size, cap)
    }

    fn write_to_engine(&self, e: &RocksEngine, opts: &WriteOptions) -> Result<()> {
        e.write_vec_opt(self, opts)
    }
}

#[cfg(test)]
mod tests {
    use super::super::util::new_default_engine;
    use super::*;
    use engine_traits::WriteBatch;
    use tempfile::Builder;

    #[test]
    fn test_should_write_to_engine() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let engine = new_default_engine(path.path().join("db").to_str().unwrap()).unwrap();
        let mut wb = engine.write_batch();
        for _i in 0..WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        let mut wb = engine.write_batch_vec(4, 1024);
        for _i in 0..WRITE_BATCH_MAX_BATCH * 4 {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
    }
}
