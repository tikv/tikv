// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Mutable, Result, WriteBatch, WriteOptions};

pub struct PanicWriteBatch;

impl WriteBatch for PanicWriteBatch {
    fn data_size(&self) -> usize {
        panic!()
    }
    fn count(&self) -> usize {
        panic!()
    }
    fn is_empty(&self) -> bool {
        panic!()
    }
    fn clear(&self) {
        panic!()
    }

    fn set_save_point(&mut self) {
        panic!()
    }
    fn pop_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        panic!()
    }
}

impl Mutable for PanicWriteBatch {
    fn put_opt(&self, opts: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_cf_opt(&self, opts: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete_opt(&self, opts: &WriteOptions, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cf_opt(&self, opts: &WriteOptions, cf: &str, key: &[u8]) -> Result<()> {
        panic!()
    }
}
