// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait Snapshot: Peekable {}

pub trait WriteBatch: Mutable {}

pub trait KVEngine {
    type Snap: Snapshot;
    type Batch: WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::Batch) -> Result<()>;
    fn write(&self, wb: &Self::Batch) -> Result<()> {
        self.write_opt(&WriteOptions::default(), wb)
    }
    fn write_batch(&self) -> Result<Self::Batch>;
    fn snapshot(&self) -> Result<Self::Snap>;
    fn sync(&self) -> Result<()>;
    fn cf_names(&self) -> Vec<&str>;
    fn delete_all_in_range(
        &self,
        opts: &DeleteRangeOptions,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }
        for cf in self.cf_names() {
            self.delete_all_in_range_cf(opts, cf, start_key, end_key)?;
        }
        Ok(())
    }
    fn delete_all_in_range_cf(
        &self,
        opts: &DeleteRangeOptions,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()>;
    //    fn ingest_file_cf(&self, meta: &DataFileMeta);
}
