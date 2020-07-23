// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteBatchVecExt, WriteOptions};

impl WriteBatchExt for SkiplistEngine {
    type WriteBatch = SkiplistWriteBatch;
    type WriteBatchVec = SkiplistWriteBatch;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()> {
        panic!()
    }

    fn support_write_batch_vec(&self) -> bool {
        false
    }

    fn write_vec_opt(&self, wb: &Self::WriteBatchVec, opts: &WriteOptions) -> Result<()> {
        panic!()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
    fn write_batch_vec(&self, vec_size: usize, cap: usize) -> Self::WriteBatchVec {
        panic!()
    }
}

pub struct SkiplistWriteBatch;

impl WriteBatch for SkiplistWriteBatch {
    fn data_size(&self) -> usize {
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
}

impl Mutable for SkiplistWriteBatch {
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

impl WriteBatchVecExt<SkiplistEngine> for SkiplistWriteBatch {
    fn write_batch_vec(e: &SkiplistEngine, _vec_size: usize, cap: usize) -> SkiplistWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_to_engine(&self, e: &SkiplistEngine, opts: &WriteOptions) -> Result<()> {
        e.write_opt(self, opts)
    }
}
