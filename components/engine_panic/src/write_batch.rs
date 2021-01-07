// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{Mutable, Result, ValueType, WriteBatch, WriteBatchExt, WriteOptions};

impl WriteBatchExt for PanicEngine {
    type WriteBatch = PanicWriteBatch;
    type WriteBatchVec = PanicWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 1;

    fn support_write_batch_vec(&self) -> bool {
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

impl WriteBatch<PanicEngine> for PanicWriteBatch {
    fn with_capacity(_: &PanicEngine, _: usize) -> Self {
        panic!()
    }

    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        panic!()
    }
}

impl Mutable for PanicWriteBatch {
    fn data_size(&self) -> usize {
        panic!()
    }
    fn data(&self) -> &[u8] {
        panic!()
    }
    fn count(&self) -> usize {
        panic!()
    }
    fn is_empty(&self) -> bool {
        panic!()
    }
    fn should_write_to_engine(&self) -> bool {
        panic!()
    }

    fn clear(&mut self) {
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
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}
