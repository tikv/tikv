// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused_variables)]
use engine_traits::{self, Result, WriteOptions};
use proxy_ffi::interfaces_ffi::RawCppPtr;

use crate::{
    mixed_engine::{elementary::ElementaryWriteBatch, write_batch::RocksWriteBatchVec},
    PageStorageExt,
};

pub struct PSElementWriteBatch {
    pub ps_ext: PageStorageExt,
    pub ps_wb: RawCppPtr,
}

impl ElementaryWriteBatch for PSElementWriteBatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn use_default(&self) -> bool {
        false
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ps_ext.write_batch_put_page(self.ps_wb.ptr, key, value);
        Ok(())
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.ps_ext.write_batch_put_page(self.ps_wb.ptr, key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.ps_ext.write_batch_del_page(self.ps_wb.ptr, key);
        Ok(())
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.ps_ext.write_batch_del_page(self.ps_wb.ptr, key);
        Ok(())
    }

    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        // write into ps
        self.ps_ext.consume_write_batch(self.ps_wb.ptr);
        Ok(self.ps_ext.write_batch_size(self.ps_wb.ptr) as u64)
    }

    fn data_size(&self) -> usize {
        self.ps_ext.write_batch_size(self.ps_wb.ptr)
    }

    fn count(&self) -> usize {
        // FIXME
        // TODO
        0
    }

    fn is_empty(&self) -> bool {
        self.ps_ext.write_batch_is_empty(self.ps_wb.ptr)
    }

    fn clear(&mut self) {
        self.ps_ext.write_batch_clear(self.ps_wb.ptr);
    }

    fn merge(&mut self, other: RocksWriteBatchVec) -> Result<()> {
        let pswb: &PSElementWriteBatch = match other
            .element_wb
            .as_any()
            .downcast_ref::<PSElementWriteBatch>()
        {
            Some(pswb) => pswb,
            None => panic!("must merge a PSElementWriteBatch"),
        };
        self.ps_ext
            .write_batch_merge(self.ps_wb.ptr, pswb.ps_wb.ptr);
        Ok(())
    }
}

impl Drop for PSElementWriteBatch {
    fn drop(&mut self) {
        if !self.ps_wb.ptr.is_null() {
            self.ps_ext.destroy_write_batch(&self.ps_wb);
        }
        self.ps_wb.ptr = std::ptr::null_mut();
    }
}
