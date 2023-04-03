// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused_variables)]

use std::sync::Arc;

use engine_traits::{self, Mutable, Result, WriteBatchExt, WriteOptions};
use rocksdb::{Writable, WriteBatch as RawWriteBatch, DB};

use super::elementary::ElementaryWriteBatch;
use crate::{options::RocksWriteOptions, r2e, util::get_cf_handle, RocksEngine};

const WRITE_BATCH_MAX_BATCH: usize = 16;
const WRITE_BATCH_LIMIT: usize = 16;

impl RocksWriteBatchVec {
    fn do_write(&self, cf: &str, key: &[u8]) -> bool {
        crate::do_write(cf, key)
    }
}

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatchVec;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> RocksWriteBatchVec {
        RocksWriteBatchVec::new(
            self.element_engine.as_ref().unwrap().element_wb(),
            Arc::clone(self.as_inner()),
            WRITE_BATCH_LIMIT,
            1,
            self.support_multi_batch_write(),
        )
    }

    fn write_batch_with_cap(&self, cap: usize) -> RocksWriteBatchVec {
        RocksWriteBatchVec::with_unit_capacity(
            self.element_engine.as_ref().unwrap().element_wb(),
            self,
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
    pub element_wb: Box<dyn ElementaryWriteBatch>,
    pub db: Arc<DB>,
    pub wbs: Vec<RawWriteBatch>,
    save_points: Vec<usize>,
    index: usize,
    batch_size_limit: usize,
    support_write_batch_vec: bool,
}

impl RocksWriteBatchVec {
    pub fn new(
        element_wb: Box<dyn ElementaryWriteBatch>,
        db: Arc<DB>,
        batch_size_limit: usize,
        cap: usize,
        support_write_batch_vec: bool,
    ) -> RocksWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            element_wb,
            db,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            batch_size_limit,
            support_write_batch_vec,
        }
    }

    pub fn with_unit_capacity(
        element_wb: Box<dyn ElementaryWriteBatch>,
        engine: &RocksEngine,
        cap: usize,
    ) -> RocksWriteBatchVec {
        Self::new(
            element_wb,
            engine.as_inner().clone(),
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

    #[inline]
    fn write_impl(&mut self, opts: &WriteOptions, mut cb: impl FnMut()) -> Result<u64> {
        let opt: RocksWriteOptions = opts.into();
        let mut seq = 0;
        if self.support_write_batch_vec {
            // FIXME(tabokie): Callback for empty write batch won't be called.
            self.get_db()
                .multi_batch_write_callback(self.as_inner(), &opt.into_raw(), |s| {
                    seq = s;
                    cb();
                })
                .map_err(r2e)?;
        } else {
            self.get_db()
                .write_callback(&self.wbs[0], &opt.into_raw(), |s| {
                    seq = s;
                    cb();
                })
                .map_err(r2e)?;
        }
        Ok(seq)
    }
}

impl engine_traits::WriteBatch for RocksWriteBatchVec {
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        if crate::log_check_double_write(self) {
            return Ok(0);
        }
        if !self.element_wb.use_default() {
            self.element_wb.write_opt(opts)
        } else {
            self.write_impl(opts, || {})
        }
    }

    fn data_size(&self) -> usize {
        if !self.element_wb.use_default() {
            self.element_wb.data_size()
        } else {
            let mut size: usize = 0;
            for i in 0..=self.index {
                size += self.wbs[i].data_size();
            }
            size
        }
    }

    fn count(&self) -> usize {
        if !self.element_wb.use_default() {
            self.element_wb.count()
        } else {
            self.wbs[self.index].count() + self.index * self.batch_size_limit
        }
    }

    fn is_empty(&self) -> bool {
        if !self.element_wb.use_default() {
            self.element_wb.is_empty()
        } else {
            self.wbs[0].is_empty()
        }
    }

    fn should_write_to_engine(&self) -> bool {
        // Disable TiKV's logic, and using Proxy's instead.
        false
    }

    fn clear(&mut self) {
        if !self.element_wb.use_default() {
            self.element_wb.clear()
        } else {
            for i in 0..=self.index {
                self.wbs[i].clear();
            }
            self.save_points.clear();
            // Avoid making the wbs too big at one time, then the memory will be kept
            // after reusing
            if self.index > WRITE_BATCH_MAX_BATCH + 1 {
                self.wbs.shrink_to(WRITE_BATCH_MAX_BATCH + 1);
            }
            self.index = 0;
        }
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

    fn merge(&mut self, other: RocksWriteBatchVec) -> Result<()> {
        if !self.element_wb.use_default() {
            self.element_wb.merge(other)
        } else {
            for wb in other.as_inner() {
                self.check_switch_batch();
                self.wbs[self.index].append(wb.data());
            }
            Ok(())
        }
    }
}

impl Mutable for RocksWriteBatchVec {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.do_write(engine_traits::CF_DEFAULT, key) {
            return Ok(());
        }
        if !self.element_wb.use_default() {
            self.element_wb.put(key, value)
        } else {
            self.check_switch_batch();
            self.wbs[self.index].put(key, value).map_err(r2e)
        }
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.do_write(cf, key) {
            return Ok(());
        }
        if !self.element_wb.use_default() {
            self.element_wb.put_cf(cf, key, value)
        } else {
            self.check_switch_batch();
            let handle = get_cf_handle(self.db.as_ref(), cf)?;
            self.wbs[self.index].put_cf(handle, key, value).map_err(r2e)
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.do_write(engine_traits::CF_DEFAULT, key) {
            return Ok(());
        }
        if !self.element_wb.use_default() {
            self.element_wb.delete(key)
        } else {
            self.check_switch_batch();
            self.wbs[self.index].delete(key).map_err(r2e)
        }
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        if !self.do_write(cf, key) {
            return Ok(());
        }
        if !self.element_wb.use_default() {
            self.element_wb.delete_cf(cf, key)
        } else {
            self.check_switch_batch();
            let handle = get_cf_handle(self.db.as_ref(), cf)?;
            self.wbs[self.index].delete_cf(handle, key).map_err(r2e)
        }
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        Ok(())
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        Ok(())
    }
}
