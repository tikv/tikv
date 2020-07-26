// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{
    CfName, Error, Mutable, Result, WriteBatch, WriteBatchExt, WriteBatchVecExt, WriteOptions,
    CF_DEFAULT,
};

impl WriteBatchExt for SkiplistEngine {
    type WriteBatch = SkiplistWriteBatch;
    type WriteBatchVec = SkiplistWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

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

enum WriteAction {
    Put((String, Vec<u8>, Vec<u8>)),
    Delete((String, Vec<u8>)),
    DeleteRange((String, Vec<u8>, Vec<u8>)),
}

pub struct SkiplistWriteBatch {
    data_size: usize,
    actions: Vec<WriteAction>,
    safe_points: Vec<usize>,
}

impl WriteBatch for SkiplistWriteBatch {
    fn data_size(&self) -> usize {
        self.data_size
    }
    fn count(&self) -> usize {
        self.actions.len()
    }
    fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
    fn should_write_to_engine(&self) -> bool {
        panic!()
    }
    fn clear(&mut self) {
        self.actions.clear();
    }
    fn set_save_point(&mut self) {
        self.safe_points.push(self.actions.len());
    }
    fn pop_save_point(&mut self) -> Result<()> {
        self.safe_points.pop();
        Ok(())
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        let p = self
            .safe_points
            .pop()
            .ok_or_else(|| Error::Engine("no save point".to_owned()))?;
        self.actions.truncate(p);
        Ok(())
    }
}

impl Mutable for SkiplistWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data_size += key.len() + value.len();
        self.actions.push(WriteAction::Put((
            CF_DEFAULT.to_owned(),
            key.to_vec(),
            value.to_vec(),
        )));
        Ok(())
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.data_size += key.len() + value.len();
        self.actions.push(WriteAction::Put((
            cf.to_owned(),
            key.to_vec(),
            value.to_vec(),
        )));
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.actions
            .push(WriteAction::Delete((CF_DEFAULT.to_owned(), key.to_vec())));
        Ok(())
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.actions
            .push(WriteAction::Delete((cf.to_owned(), key.to_vec())));
        Ok(())
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.actions.push(WriteAction::DeleteRange((
            cf.to_owned(),
            begin_key.to_vec(),
            end_key.to_vec(),
        )));
        Ok(())
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
