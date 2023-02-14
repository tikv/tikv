// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::RocksEngineIterator;
use engine_traits::{Iterable, Peekable, ReadOptions, Result, SyncMutable};

use crate::RocksEngine;

impl Peekable for RocksEngine {
    type DbVector = crate::ps_engine::PsDbVector;

    fn get_value_opt(
        &self,
        opts: &ReadOptions,
        key: &[u8],
    ) -> Result<Option<crate::ps_engine::PsDbVector>> {
        let result = self.ps_ext.as_ref().unwrap().read_page(key);
        return match result {
            None => Ok(None),
            Some(v) => Ok(Some(crate::ps_engine::PsDbVector::from_raw(v))),
        };
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<crate::ps_engine::PsDbVector>> {
        self.get_value_opt(opts, key)
    }
}

impl SyncMutable for RocksEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_put_page(ps_wb.ptr, key, value);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_put_page(ps_wb.ptr, key, value);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_del_page(ps_wb.ptr, key);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_del_page(ps_wb.ptr, key);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }
}
