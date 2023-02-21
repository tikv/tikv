// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused_variables)]
use std::sync::Arc;

use engine_traits::{self, Mutable, Result, WriteBatchExt, WriteOptions};
use proxy_ffi::interfaces_ffi::RawCppPtr;
use rocksdb::{WriteBatch as RawWriteBatch, DB};

use crate::{engine::RocksEngine, ps_engine::add_prefix, r2e, PageStorageExt};

const WRITE_BATCH_MAX_BATCH: usize = 16;
const WRITE_BATCH_LIMIT: usize = 16;

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatchVec;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> RocksWriteBatchVec {
        RocksWriteBatchVec::new(
            Arc::clone(self.as_inner()),
            self.ps_ext.clone(),
            self.ps_ext.as_ref().unwrap().create_write_batch(),
            WRITE_BATCH_LIMIT,
            1,
            self.support_multi_batch_write(),
        )
    }

    fn write_batch_with_cap(&self, cap: usize) -> RocksWriteBatchVec {
        RocksWriteBatchVec::with_unit_capacity(
            self,
            self.ps_ext.as_ref().unwrap().create_write_batch(),
            cap,
        )
    }
}

/// `RocksWriteBatchVec` is for method `MultiBatchWrite` of RocksDB, which
/// splits a large WriteBatch into many smaller ones and then any thread could
/// help to deal with these small WriteBatch when it is calling
/// `MultiBatchCommit` and wait the front writer to finish writing.
/// `MultiBatchWrite` will perform much better than traditional
/// `pipelined_write` when TiKV writes very large data into RocksDB.
/// We will remove this feature when `unordered_write` of RocksDB becomes more
/// stable and becomes compatible with Titan.
pub struct RocksWriteBatchVec {
    pub db: Arc<DB>,
    pub wbs: Vec<RawWriteBatch>,
    pub ps_ext: Option<PageStorageExt>,
    pub ps_wb: RawCppPtr,
    save_points: Vec<usize>,
    index: usize,
    batch_size_limit: usize,
    support_write_batch_vec: bool,
}

impl Drop for RocksWriteBatchVec {
    fn drop(&mut self) {
        if !self.ps_wb.ptr.is_null() {
            self.ps_ext
                .as_ref()
                .unwrap()
                .destroy_write_batch(&self.ps_wb);
        }
        self.ps_wb.ptr = std::ptr::null_mut();
    }
}

impl RocksWriteBatchVec {
    pub fn new(
        db: Arc<DB>,
        ps_ext: Option<PageStorageExt>,
        ps_wb: RawCppPtr,
        batch_size_limit: usize,
        cap: usize,
        support_write_batch_vec: bool,
    ) -> RocksWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            db,
            wbs: vec![wb],
            ps_ext,
            ps_wb,
            save_points: vec![],
            index: 0,
            batch_size_limit,
            support_write_batch_vec,
        }
    }

    pub fn with_unit_capacity(
        engine: &RocksEngine,
        ps_wb: RawCppPtr,
        cap: usize,
    ) -> RocksWriteBatchVec {
        Self::new(
            engine.as_inner().clone(),
            engine.ps_ext.clone(),
            ps_wb,
            WRITE_BATCH_LIMIT,
            cap,
            engine.support_multi_batch_write(),
        )
    }

    pub fn as_inner(&self) -> &[RawWriteBatch] {
        &self.wbs[0..=self.index]
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    /// `check_switch_batch` will split a large WriteBatch into many smaller
    /// ones. This is to avoid a large WriteBatch blocking write_thread too
    /// long.
    #[inline(always)]
    fn check_switch_batch(&mut self) {
        if self.support_write_batch_vec
            && self.batch_size_limit > 0
            && self.wbs[self.index].count() >= self.batch_size_limit
        {
            self.index += 1;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::default());
            }
        }
    }
}

impl engine_traits::WriteBatch for RocksWriteBatchVec {
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        // write into ps
        self.ps_ext
            .as_ref()
            .unwrap()
            .consume_write_batch(self.ps_wb.ptr);
        Ok(self
            .ps_ext
            .as_ref()
            .unwrap()
            .write_batch_size(self.ps_wb.ptr) as u64)
    }

    fn data_size(&self) -> usize {
        self.ps_ext
            .as_ref()
            .unwrap()
            .write_batch_size(self.ps_wb.ptr)
    }

    fn count(&self) -> usize {
        // FIXME
        // TODO
        0
    }

    fn is_empty(&self) -> bool {
        self.ps_ext
            .as_ref()
            .unwrap()
            .write_batch_is_empty(self.ps_wb.ptr)
    }

    fn should_write_to_engine(&self) -> bool {
        // Disable TiKV's logic, and using Proxy's instead.
        false
    }

    fn clear(&mut self) {
        self.ps_ext
            .as_ref()
            .unwrap()
            .write_batch_clear(self.ps_wb.ptr);
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(r2e);
        }
        Err(r2e("no save point"))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(r2e);
        }
        Err(r2e("no save point"))
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        self.ps_ext
            .as_ref()
            .unwrap()
            .write_batch_merge(self.ps_wb.ptr, other.ps_wb.ptr);
        Ok(())
    }
}

impl RocksWriteBatchVec {
    fn do_write(&self, cf: &str, key: &[u8]) -> bool {
        crate::do_write(cf, key)
    }
}

impl Mutable for RocksWriteBatchVec {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.do_write(engine_traits::CF_DEFAULT, key) {
            return Ok(());
        }
        self.ps_ext.as_ref().unwrap().write_batch_put_page(
            self.ps_wb.ptr,
            add_prefix(key).as_slice(),
            value,
        );
        Ok(())
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.do_write(cf, key) {
            return Ok(());
        }
        self.ps_ext.as_ref().unwrap().write_batch_put_page(
            self.ps_wb.ptr,
            add_prefix(key).as_slice(),
            value,
        );
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.do_write(engine_traits::CF_DEFAULT, key) {
            return Ok(());
        }
        self.ps_ext
            .as_ref()
            .unwrap()
            .write_batch_del_page(self.ps_wb.ptr, add_prefix(key).as_slice());
        Ok(())
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        if !self.do_write(cf, key) {
            return Ok(());
        }
        self.ps_ext
            .as_ref()
            .unwrap()
            .write_batch_del_page(self.ps_wb.ptr, add_prefix(key).as_slice());
        Ok(())
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        Ok(())
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Peekable, WriteBatch, CF_DEFAULT};
    use rocksdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    use super::{
        super::{util::new_engine_opt, RocksDbOptions},
        *,
    };
    use crate::RocksCfOptions;

    #[test]
    fn test_should_write_to_engine_with_pipeline_write_mode() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(true);
        opt.enable_multi_batch_write(false);
        let engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDbOptions::from_raw(opt),
            vec![(CF_DEFAULT, RocksCfOptions::default())],
        )
        .unwrap();
        assert!(
            !engine
                .as_inner()
                .get_db_options()
                .is_enable_multi_batch_write()
        );
        let mut wb = engine.write_batch();
        for _i in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.write().unwrap();

        let v = engine.get_value(b"aaa").unwrap();

        assert!(v.is_some());
        assert_eq!(v.unwrap(), b"bbb");
        let mut wb = RocksWriteBatchVec::with_unit_capacity(&engine, 1024);
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
    fn test_should_write_to_engine_with_multi_batch_write_mode() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(false);
        opt.enable_multi_batch_write(true);
        let engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDbOptions::from_raw(opt),
            vec![(CF_DEFAULT, RocksCfOptions::default())],
        )
        .unwrap();
        assert!(
            engine
                .as_inner()
                .get_db_options()
                .is_enable_multi_batch_write()
        );
        let mut wb = engine.write_batch();
        for _i in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        let mut wb = RocksWriteBatchVec::with_unit_capacity(&engine, 1024);
        for _i in 0..WRITE_BATCH_MAX_BATCH * WRITE_BATCH_LIMIT {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.clear();
        assert!(!wb.should_write_to_engine());
    }
}
