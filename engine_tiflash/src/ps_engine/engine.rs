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
        self.ps_ext.write_batch_put_page(ps_wb.ptr, key, value);
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn put_cf(&self, _cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext.write_batch_put_page(ps_wb.ptr, key, value);
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext.write_batch_del_page(ps_wb.ptr, key);
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn delete_cf(&self, _cf: &str, key: &[u8]) -> Result<()> {
        let ps_wb = self.ps_ext.create_write_batch();
        self.ps_ext.write_batch_del_page(ps_wb.ptr, key);
        self.ps_ext.consume_write_batch(ps_wb.ptr);
        Ok(())
    }

    fn get_value_opt(&self, _opts: &ReadOptions, key: &[u8]) -> Result<Option<MixedDbVector>> {
        let result = self.ps_ext.read_page(key);
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
        self.ps_ext.scan_page(start_key, end_key, f);
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
