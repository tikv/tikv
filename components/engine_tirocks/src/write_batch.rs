// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Result, WriteBatchExt as _};
use tirocks::{option::WriteOptions, WriteBatch};

use crate::{r2e, RocksEngine};

const WRITE_BATCH_MAX_BATCH_NUM: usize = 16;
const WRITE_BATCH_MAX_KEY_NUM: usize = 16;

impl engine_traits::WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatchVec;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    #[inline]
    fn write_batch(&self) -> RocksWriteBatchVec {
        self.write_batch_with_cap(1)
    }

    #[inline]
    fn write_batch_with_cap(&self, cap: usize) -> RocksWriteBatchVec {
        RocksWriteBatchVec::with_unit_capacity(self, cap)
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
    engine: RocksEngine,
    wbs: Vec<WriteBatch>,
    save_points: Vec<usize>,
    index: usize,
}

impl RocksWriteBatchVec {
    pub fn with_unit_capacity(engine: &RocksEngine, cap: usize) -> RocksWriteBatchVec {
        let wb = WriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            engine: engine.clone(),
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
        }
    }

    /// `check_switch_batch` will split a large WriteBatch into many smaller
    /// ones. This is to avoid a large WriteBatch blocking write_thread too
    /// long.
    #[inline(always)]
    fn check_switch_batch(&mut self) {
        if self.engine.multi_batch_write()
            && self.wbs[self.index].count() >= WRITE_BATCH_MAX_KEY_NUM
        {
            self.index += 1;
            if self.index >= self.wbs.len() {
                self.wbs.push(WriteBatch::default());
            }
        }
    }
}

/// Converts engine_traits options to tirocks write options.
pub fn to_tirocks_opt(opt: &engine_traits::WriteOptions) -> WriteOptions {
    let mut r = WriteOptions::default();
    r.set_sync(opt.sync())
    .set_no_slowdown(opt.no_slowdown())
    .set_disable_wal(opt.disable_wal())

    // TODO: enable it.
    .set_memtable_insert_hint_per_batch(false);
    r
}

impl engine_traits::WriteBatch for RocksWriteBatchVec {
    fn write_opt(&mut self, opts: &engine_traits::WriteOptions) -> Result<u64> {
        let opts = to_tirocks_opt(opts);
        if self.engine.multi_batch_write() {
            self.engine
                .as_inner()
                .write_multi(&opts, &mut self.wbs[..=self.index])
                .map_err(r2e)
        } else {
            self.engine
                .as_inner()
                .write(&opts, &mut self.wbs[0])
                .map_err(r2e)
        }
    }

    fn data_size(&self) -> usize {
        let mut size = 0;
        for w in &self.wbs[..=self.index] {
            size += w.as_bytes().len();
        }
        size
    }

    fn count(&self) -> usize {
        let mut size = 0;
        for w in &self.wbs[..=self.index] {
            size += w.count();
        }
        size
    }

    fn is_empty(&self) -> bool {
        self.wbs[0].as_bytes().is_empty()
    }

    #[inline]
    fn should_write_to_engine(&self) -> bool {
        if self.engine.multi_batch_write() {
            self.index >= WRITE_BATCH_MAX_BATCH_NUM
        } else {
            self.wbs[0].count() > RocksEngine::WRITE_BATCH_MAX_KEYS
        }
    }

    fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        // Avoid making the wbs too big at one time, then the memory will be kept
        // after reusing
        if self.index > WRITE_BATCH_MAX_BATCH_NUM {
            self.wbs.shrink_to(WRITE_BATCH_MAX_BATCH_NUM);
        }
        self.index = 0;
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(r2e);
        }
        Err(engine_traits::Error::Engine(
            engine_traits::Status::with_error(
                engine_traits::Code::InvalidArgument,
                "no save point",
            ),
        ))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(r2e);
        }
        Err(engine_traits::Error::Engine(
            engine_traits::Status::with_error(
                engine_traits::Code::InvalidArgument,
                "no save point",
            ),
        ))
    }

    fn merge(&mut self, mut other: Self) -> Result<()> {
        if !self.engine.multi_batch_write() {
            let self_wb = &mut self.wbs[0];
            for wb in &other.wbs[..=other.index] {
                self_wb.append(wb).map_err(r2e)?;
            }
            return Ok(());
        }
        let self_wb = &mut self.wbs[self.index];
        let mut other_start = 0;
        if self_wb.count() < WRITE_BATCH_MAX_KEY_NUM {
            self_wb.append(&other.wbs[0]).map_err(r2e)?;
            other_start = 1;
        }
        // From this point, either of following statements is true:
        // - self_wb.count() >= WRITE_BATCH_MAX_KEY_NUM
        // - other.index == 0
        if other.index >= other_start {
            for wb in other.wbs.drain(other_start..=other.index) {
                self.index += 1;
                if self.wbs.len() == self.index {
                    self.wbs.push(wb);
                } else {
                    self.wbs[self.index] = wb;
                }
            }
        }
        Ok(())
    }
}

impl engine_traits::Mutable for RocksWriteBatchVec {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = self.engine.as_inner().default_cf();
        self.wbs[self.index].put(handle, key, value).map_err(r2e)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = self.engine.cf(cf)?;
        self.wbs[self.index].put(handle, key, value).map_err(r2e)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = self.engine.as_inner().default_cf();
        self.wbs[self.index].delete(handle, key).map_err(r2e)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = self.engine.cf(cf)?;
        self.wbs[self.index].delete(handle, key).map_err(r2e)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = self.engine.as_inner().default_cf();
        self.wbs[self.index]
            .delete_range(handle, begin_key, end_key)
            .map_err(r2e)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = self.engine.cf(cf)?;
        self.wbs[self.index]
            .delete_range(handle, begin_key, end_key)
            .map_err(r2e)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use engine_traits::{Mutable, Peekable, WriteBatch, WriteBatchExt, CF_DEFAULT};
    use tempfile::Builder;

    use super::*;
    use crate::{
        cf_options::RocksCfOptions, db_options::RocksDbOptions, new_engine_opt, RocksEngine,
    };

    fn new_engine(path: &Path, multi_batch_write: bool) -> RocksEngine {
        let mut db_opt = RocksDbOptions::default();
        db_opt
            .set_unordered_write(false)
            .set_enable_pipelined_write(!multi_batch_write)
            .set_multi_batch_write(multi_batch_write);
        let engine = new_engine_opt(
            &path.join("db"),
            db_opt,
            vec![(CF_DEFAULT, RocksCfOptions::default())],
        )
        .unwrap();
        assert_eq!(
            engine.as_inner().db_options().multi_batch_write(),
            multi_batch_write
        );
        engine
    }

    #[test]
    fn test_should_write_to_engine_with_pipeline_write_mode() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let engine = new_engine(path.path(), false);
        let mut wb = engine.write_batch();
        for _ in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
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
        let engine = new_engine(path.path(), true);
        let mut wb = engine.write_batch();
        for _ in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        let mut wb = RocksWriteBatchVec::with_unit_capacity(&engine, 1024);
        for _ in 0..WRITE_BATCH_MAX_BATCH_NUM * WRITE_BATCH_MAX_KEY_NUM {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.clear();
        assert!(!wb.should_write_to_engine());
    }

    #[test]
    fn test_write_batch_merge() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        for multi_batch_write in &[false, true] {
            let engine = new_engine(path.path(), *multi_batch_write);
            let mut wb = engine.write_batch();
            for _ in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
                wb.put(b"aaa", b"bbb").unwrap();
            }
            assert_eq!(wb.count(), RocksEngine::WRITE_BATCH_MAX_KEYS);

            let mut wb2 = engine.write_batch();
            for _ in 0..WRITE_BATCH_MAX_KEY_NUM / 2 {
                wb2.put(b"aaa", b"bbb").unwrap();
            }
            assert_eq!(wb2.count(), WRITE_BATCH_MAX_KEY_NUM / 2);
            // The only batch should be moved directly.
            wb.merge(wb2).unwrap();
            assert_eq!(
                wb.count(),
                RocksEngine::WRITE_BATCH_MAX_KEYS + WRITE_BATCH_MAX_KEY_NUM / 2
            );
            if *multi_batch_write {
                assert_eq!(
                    wb.wbs.len(),
                    RocksEngine::WRITE_BATCH_MAX_KEYS / WRITE_BATCH_MAX_KEY_NUM + 1
                );
            }

            let mut wb3 = engine.write_batch();
            for _ in 0..WRITE_BATCH_MAX_KEY_NUM / 2 * 3 {
                wb3.put(b"aaa", b"bbb").unwrap();
            }
            assert_eq!(wb3.count(), WRITE_BATCH_MAX_KEY_NUM / 2 * 3);
            // The half batch should be merged together, and then move the left one.
            wb.merge(wb3).unwrap();
            assert_eq!(
                wb.count(),
                RocksEngine::WRITE_BATCH_MAX_KEYS + WRITE_BATCH_MAX_KEY_NUM * 2
            );
            if *multi_batch_write {
                assert_eq!(
                    wb.wbs.len(),
                    RocksEngine::WRITE_BATCH_MAX_KEYS / WRITE_BATCH_MAX_KEY_NUM + 2
                );
            }
        }
    }
}
