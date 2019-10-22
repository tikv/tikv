// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::path::Path;

use crate::*;

pub trait Snapshot: 'static + Peekable + Send + Sync + Debug {
    fn cf_names(&self) -> Vec<&str>;
}

pub trait WriteBatch: Mutable {
    fn data_size(&self) -> usize;
    fn count(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn clear(&self);

    fn set_save_point(&mut self);
    fn pop_save_point(&mut self) -> Result<()>;
    fn rollback_to_save_point(&mut self) -> Result<()>;
}

pub trait KvEngine: Peekable + Mutable + Iterable + Send + Sync + Clone {
    type Snap: Snapshot;
    type Batch: WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::Batch) -> Result<()>;
    fn write(&self, wb: &Self::Batch) -> Result<()> {
        self.write_opt(&WriteOptions::default(), wb)
    }
    fn write_batch(&self, cap: usize) -> Self::Batch;
    fn snapshot(&self) -> Self::Snap;
    fn sync(&self) -> Result<()>;
    fn cf_names(&self) -> Vec<&str>;
    fn delete_all_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }
        for cf in self.cf_names() {
            self.delete_all_in_range_cf(cf, start_key, end_key)?;
        }
        Ok(())
    }
    fn delete_all_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn ingest_external_file_cf(&self, cf: &str, files: &[&str]) -> Result<()>;
    fn validate_file_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()>;
}
