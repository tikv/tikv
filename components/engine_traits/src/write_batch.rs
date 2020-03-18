// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::mutable::Mutable;
use crate::options::WriteOptions;

pub trait WriteBatchExt {
    type WriteBatch: WriteBatch;
    type WriteBatchVec: WriteBatch;

    fn write_opt(&self, wb: &Self::WriteBatch, opts: &WriteOptions) -> Result<()>;
    fn write_vec_opt(&self, wb: &Self::WriteBatchVec, opts: &WriteOptions) -> Result<()>;
    fn support_write_batch_vec(&self) -> bool;
    fn write(&self, wb: &Self::WriteBatch) -> Result<()> {
        self.write_opt(wb, &WriteOptions::default())
    }
    fn write_batch(&self) -> Self::WriteBatch;
    fn write_batch_vec(&self, limit: usize, cap: usize) -> Self::WriteBatchVec;
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch;
}

pub trait WriteBatch: Mutable + Send {
    fn data_size(&self) -> usize;
    fn count(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn should_write_to_engine(&self) -> bool;

    fn clear(&mut self);
    fn write_to_engine(&mut self, opts: &WriteOptions) -> Result<()>;
    fn set_save_point(&mut self);
    fn pop_save_point(&mut self) -> Result<()>;
    fn rollback_to_save_point(&mut self) -> Result<()>;
}
