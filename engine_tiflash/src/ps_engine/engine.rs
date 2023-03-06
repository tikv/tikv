// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngineIterator;
use engine_traits::{IterOptions, Iterable, ReadOptions, Result};

use crate::{
    mixed_engine::{
        elementary::{ElementaryEngine, ElementaryWriteBatch},
        MixedDbVector,
    },
    PageStorageExt,
};

#[derive(Clone, Debug)]
pub struct PSElementEngine {
    pub ps_ext: PageStorageExt,
    pub rocks: engine_rocks::RocksEngine,
}

unsafe impl Send for PSElementEngine {}
unsafe impl Sync for PSElementEngine {}

impl ElementaryEngine for PSElementEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext
            .write_batch_put_page(ps_wb.ptr, add_prefix(key).as_slice(), value);
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn put_cf(&self, _cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext
            .write_batch_put_page(ps_wb.ptr, add_prefix(key).as_slice(), value);
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext
            .write_batch_del_page(ps_wb.ptr, add_prefix(key).as_slice());
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn delete_cf(&self, _cf: &str, key: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext
            .write_batch_del_page(ps_wb.ptr, add_prefix(key).as_slice());
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn get_value_opt(&self, _opts: &ReadOptions, key: &[u8]) -> Result<Option<MixedDbVector>> {
        let result = self.ps_ext.read_page(add_prefix(key).as_slice());
        match result {
            None => Ok(None),
            Some(v) => Ok(Some(MixedDbVector::from_raw_ps(v))),
        }
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        _cf: &str,
        key: &[u8],
    ) -> Result<Option<MixedDbVector>> {
        self.get_value_opt(opts, key)
    }

    fn scan(
        &self,
        _cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        _fill_cache: bool,
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()> {
        self.ps_ext.scan_page(
            add_prefix(start_key).as_slice(),
            add_prefix(end_key).as_slice(),
            f,
        );
        Ok(())
    }

    #[allow(unused_variables)]
    #[allow(unreachable_code)]
    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<RocksEngineIterator> {
        let r = self.rocks.iterator_opt(cf, opts);
        panic!("iterator_opt should not be called in PS engine");
        r
    }

    fn element_wb(&self) -> Box<dyn ElementaryWriteBatch> {
        Box::new(super::PSElementWriteBatch {
            ps_ext: self.ps_ext.clone(),
            ps_wb: self.ps_ext.create_write_batch(),
        })
    }
}

// Some data may be migrated from kv engine to raft engine in the future,
// so kv engine and raft engine may write and delete the same key in the code
// base. To distinguish data managed by kv engine and raft engine, we prepend an
// `0x02` to the key written by kv engine.
// So kv engine won't scan any key from raft engine, and vice versa.
pub fn add_prefix(key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.push(0x02);
    v.extend_from_slice(key);
    v
}
