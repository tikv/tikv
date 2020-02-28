// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use crate::engine::PanicEngine;

impl WriteBatchExt for PanicEngine {
    type WriteBatch = PanicWriteBatch;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()> {
        panic!()
    }
    fn write_batch(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
}

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
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}
