// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused_variables)]

use engine_traits::{self, Result, WriteOptions};

use crate::mixed_engine::{elementary::ElementaryWriteBatch, write_batch::RocksWriteBatchVec};

pub struct RocksElementWriteBatch {}

impl ElementaryWriteBatch for RocksElementWriteBatch {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn use_default(&self) -> bool {
        true
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        unreachable!()
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        unreachable!()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        unreachable!()
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        unreachable!()
    }

    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        unreachable!()
    }

    fn data_size(&self) -> usize {
        unreachable!()
    }

    fn count(&self) -> usize {
        unreachable!()
    }

    fn is_empty(&self) -> bool {
        unreachable!()
    }

    fn clear(&mut self) {
        unreachable!()
    }

    fn merge(&mut self, other: RocksWriteBatchVec) -> Result<()> {
        unreachable!()
    }
}
