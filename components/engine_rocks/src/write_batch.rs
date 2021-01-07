// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::engine::RocksEngine;
use crate::options::RocksWriteOptions;
use crate::util::get_cf_handle;
use engine_traits::{self, Error, Mutable, Result, WriteBatchExt, WriteOptions};
use rocksdb::{
    DBValueType, Writable, WriteBatch as RawWriteBatch, WriteBatchIter, WriteBatchRef, DB,
};

const WRITE_BATCH_LIMIT: usize = 16;

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatch;
    type WriteBatchVec = RocksWriteBatchVec;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

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

impl engine_traits::WriteBatch<RocksEngine> for RocksWriteBatch {
    fn with_capacity(e: &RocksEngine, cap: usize) -> RocksWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
        let opt: RocksWriteOptions = opts.into();
        self.get_db()
            .write_opt(self.as_inner(), &opt.into_raw())
            .map_err(Error::Engine)
    }
}

impl Mutable for RocksWriteBatch {
    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn data(&self) -> &[u8] {
        self.wb.data()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.wb.count() > RocksEngine::WRITE_BATCH_MAX_KEYS
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
    total_count: usize,
    cap: usize,
}

impl RocksWriteBatchVec {
    pub fn new(db: Arc<DB>, cap: usize) -> RocksWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            db,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            total_count: 0,
            cap,
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
        let cur_count = self.wbs[self.index].count();
        if cur_count >= WRITE_BATCH_LIMIT {
            self.index += 1;
            self.total_count += cur_count;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::with_capacity(self.cap));
            }
        }
    }
}

impl engine_traits::WriteBatch<RocksEngine> for RocksWriteBatchVec {
    fn with_capacity(e: &RocksEngine, cap: usize) -> RocksWriteBatchVec {
        RocksWriteBatchVec::new(e.as_inner().clone(), cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
        let opt: RocksWriteOptions = opts.into();
        if self.index > 0 {
            self.get_db()
                .multi_batch_write(self.as_inner(), &opt.into_raw())
                .map_err(Error::Engine)
        } else {
            self.get_db()
                .write_opt(&self.wbs[0], &opt.into_raw())
                .map_err(Error::Engine)
        }
    }
}

impl Mutable for RocksWriteBatchVec {
    fn data_size(&self) -> usize {
        self.wbs[0..=self.index]
            .iter()
            .fold(0, |a, b| a + b.data_size())
    }

    fn data(&self) -> &[u8] {
        panic!()
    }

    fn count(&self) -> usize {
        self.wbs[self.index].count() + self.total_count
    }

    fn is_empty(&self) -> bool {
        self.wbs[0].is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.count() > RocksEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        self.index = 0;
        self.total_count = 0;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValueType {
    Delete,
    Put,
    DeleteRange,
    Merge,
    Invalid,
}

pub struct RocksWriteBatchIter<'a, 'b> {
    cfs: &'a [String],
    inner: WriteBatchIter<'b>,
}

impl<'a, 'b> Iterator for RocksWriteBatchIter<'a, 'b> {
    type Item = (ValueType, &'a str, &'b [u8], &'b [u8]);

    fn next(&mut self) -> Option<(ValueType, &'a str, &'b [u8], &'b [u8])> {
        self.inner.next().map(|(t, cf_id, key, value)| {
            let value_type = match t {
                DBValueType::TypeDeletion => ValueType::Delete,
                DBValueType::TypeValue => ValueType::Put,
                DBValueType::TypeRangeDeletion => ValueType::DeleteRange,
                DBValueType::TypeMerge => ValueType::Merge,
                _ => ValueType::Invalid,
            };
            let cf_id = cf_id as usize;
            if cf_id >= self.cfs.len() {
                (ValueType::Invalid, "", key, value)
            } else {
                let cf = self.cfs[cf_id].as_str();
                (value_type, cf, key, value)
            }
        })
    }
}

#[derive(Clone)]
pub struct RocksWriteBatchReader {
    cfs: Vec<String>,
}

impl RocksWriteBatchReader {
    pub fn new(cfs: Vec<String>) -> RocksWriteBatchReader {
        RocksWriteBatchReader { cfs }
    }

    pub fn iter<'a, 'b>(&'a self, data: &'b [u8]) -> RocksWriteBatchIter<'a, 'b> {
        let wb = WriteBatchRef::new(data);
        let inner = wb.iter();
        RocksWriteBatchIter {
            cfs: self.cfs.as_ref(),
            inner,
        }
    }

    pub fn count(data: &[u8]) -> usize {
        let wb = WriteBatchRef::new(data);
        wb.count()
    }
}

#[cfg(test)]
mod tests {
    use super::super::util::{new_engine, new_engine_opt};
    use super::super::RocksDBOptions;
    use super::*;
    use engine_traits::WriteBatchExt;
    use engine_traits::{CFNamesExt, Peekable, SyncMutable, WriteBatch};
    use rocksdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    #[test]
    fn test_should_write_to_engine() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_multi_batch_write(true);
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(true);
        let engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        )
        .unwrap();
        assert!(engine.support_write_batch_vec());
        let mut wb = engine.write_batch();
        for _i in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        let mut wb = RocksWriteBatchVec::with_capacity(&engine, 1024);
        for _i in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.clear();
        assert!(!wb.should_write_to_engine());
    }

    #[test]
    fn test_write_batch_reader_iter() {
        let path = Builder::new()
            .prefix("test-writebatch-iter")
            .tempdir()
            .unwrap();
        let engine = new_engine(
            path.path().join("db").to_str().unwrap(),
            None,
            &["default", "cf1", "cf2"],
            None,
        )
        .unwrap();
        let cfs = engine.cf_names().iter().map(|cf| cf.to_string()).collect();
        let mut wb = engine.write_batch();
        engine.put_cf("cf1", b"k1", b"v0").unwrap();
        engine.put_cf("cf2", b"k2", b"v0").unwrap();
        assert!(engine.get_value_cf("cf1", b"k1").unwrap().is_some());
        assert!(engine.get_value_cf("cf2", b"k2").unwrap().is_some());
        wb.delete_cf("cf1", b"k1").unwrap();
        wb.delete_cf("cf2", b"k2").unwrap();
        wb.put_cf("default", b"k3", b"v0").unwrap();
        let reader = RocksWriteBatchReader::new(cfs);
        assert_eq!(3, RocksWriteBatchReader::count(wb.data()));
        for (t, cf, k, v) in reader.iter(wb.data()) {
            match t {
                ValueType::Put => engine.put_cf(cf, k, v).unwrap(),
                ValueType::Delete => engine.delete_cf(cf, k).unwrap(),
                _ => (),
            }
        }
        assert_eq!(&*engine.get_value(b"k3").unwrap().unwrap(), b"v0");
        assert!(engine.get_value_cf("cf1", b"k1").unwrap().is_none());
        assert!(engine.get_value_cf("cf2", b"k2").unwrap().is_none());
    }
}
