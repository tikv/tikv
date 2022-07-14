// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{self, Error, Mutable, Result, WriteBatchExt, WriteOptions};
use rocksdb::{Writable, WriteBatch as RawWriteBatch, DB};

use crate::{engine::RocksEngine, options::RocksWriteOptions, util::get_cf_handle};

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> RocksWriteBatch {
        RocksWriteBatch::new(self.as_inner().clone())
    }

    fn write_batch_with_cap(&self, cap: usize) -> RocksWriteBatch {
        RocksWriteBatch::with_capacity(self, cap)
    }
}

pub struct RocksWriteBatch {
    db: Arc<DB>,
    wb: RawWriteBatch,
}

impl RocksWriteBatch {
    pub fn new(db: Arc<DB>) -> RocksWriteBatch {
        let wb = RawWriteBatch::new();
        RocksWriteBatch { db, wb }
    }

    pub fn with_capacity(engine: &RocksEngine, cap: usize) -> RocksWriteBatch {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatch {
            db: engine.as_inner().clone(),
            wb,
        }
    }

    pub fn as_inner(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn as_raw(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }
}

impl engine_traits::WriteBatch for RocksWriteBatch {
    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
        let opt: RocksWriteOptions = opts.into();
        self.get_db()
            .write_opt(&self.wb, &opt.into_raw())
            .map_err(Error::Engine)
    }

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
        // Always not write to engine for kv
        false
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

    fn merge(&mut self, other: Self) -> Result<()> {
        self.wb.append(other.wb.data());
        Ok(())
    }
}

pub fn do_write(cf: &str, key: &[u8]) -> bool {
    #[cfg(feature = "compat_new_proxy")]
    {
        return match cf {
            engine_traits::CF_RAFT => true,
            engine_traits::CF_DEFAULT => {
                key == keys::PREPARE_BOOTSTRAP_KEY || key == keys::STORE_IDENT_KEY
            }
            _ => false,
        };
    }
    return true;
}

impl RocksWriteBatch {
    fn do_write(&self, cf: &str, key: &[u8]) -> bool {
        do_write(cf, key)
    }
}

impl Mutable for RocksWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.wb.put(key, value).map_err(Error::Engine);
        }
        Ok(())
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let handle = get_cf_handle(self.db.as_ref(), cf)?;
            return self.wb.put_cf(handle, key, value).map_err(Error::Engine);
        }
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.wb.delete(key).map_err(Error::Engine);
        }
        Ok(())
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let handle = get_cf_handle(self.db.as_ref(), cf)?;
            return self.wb.delete_cf(handle, key).map_err(Error::Engine);
        }
        Ok(())
    }

    fn delete_range(&mut self, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn delete_range_cf(&mut self, _cf: &str, _begin_key: &[u8], _end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Peekable, WriteBatch};
    use rocksdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    use super::{
        super::{util::new_engine_opt, RocksDBOptions},
        *,
    };

    #[test]
    fn test_should_write_to_engine() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(false);
        opt.enable_pipelined_commit(true);
        let engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        )
        .unwrap();
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
        let mut wb = RocksWriteBatch::with_capacity(&engine, 1024);
        for _i in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.clear();
        assert!(!wb.should_write_to_engine());
    }
}
