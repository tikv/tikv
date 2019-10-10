// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::path::Path;
use std::result::Result as StdResult;

use crate::*;

pub trait Snapshot: 'static + Peekable + Send + Sync + Debug {
    fn cf_names(&self) -> Vec<&str>;
}

pub trait WriteBatch: Mutable + Send {
    fn data_size(&self) -> usize;
    fn count(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn clear(&self);

    fn set_save_point(&mut self);
    fn pop_save_point(&mut self) -> Result<()>;
    fn rollback_to_save_point(&mut self) -> Result<()>;
}

pub trait KvEngine: Peekable + Mutable + Iterable + Send + Sync + Clone + Debug + 'static {
    type Snap: Snapshot;
    type Batch: WriteBatch;
    type DBOptions: DBOptions;
    type CFHandle: CFHandle;
    type CFOptions: CFOptions;

    fn get_db_options(&self) -> Self::DBOptions;
    // FIXME: return type
    fn set_db_options(&self, options: &[(&str, &str)]) -> StdResult<(), String>;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::Batch) -> Result<()>;
    fn write(&self, wb: &Self::Batch) -> Result<()> {
        self.write_opt(&WriteOptions::default(), wb)
    }
    fn write_batch(&self) -> Self::Batch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::Batch;
    fn snapshot(&self) -> Self::Snap;
    fn sync(&self) -> Result<()>;

    fn cf_names(&self) -> Vec<&str>;
    fn cf_handle(&self, name: &str) -> Option<&Self::CFHandle>;
    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::CFOptions;
    // FIXME: return type
    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> StdResult<(), String>;

    fn delete_all_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }
        for cf in self.cf_names() {
            self.delete_all_in_range_cf(cf, start_key, end_key, false)?;
        }
        Ok(())
    }
    fn delete_all_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], use_delete_range: bool) -> Result<()>;
    fn delete_files_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], include_end: bool) -> Result<()>;

    fn ingest_external_file_cf(&self, cf: &str, opts: &IngestExternalFileOptions, files: &[&str]) -> Result<()>;
    fn validate_file_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()>;
}
