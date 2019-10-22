// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::*;

pub trait KvEngine:
    Peekable
    + Mutable
    + Iterable
    + DBOptionsExt
    + CFHandleExt
    + ImportExt
    + Send
    + Sync
    + Clone
    + Debug
    + 'static
{
    type Snap: Snapshot;
    type Batch: WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::Batch) -> Result<()>;
    fn write(&self, wb: &Self::Batch) -> Result<()> {
        self.write_opt(&WriteOptions::default(), wb)
    }
    fn write_batch(&self) -> Self::Batch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::Batch;
    fn snapshot(&self) -> Self::Snap;
    fn sync(&self) -> Result<()>;

    fn cf_names(&self) -> Vec<&str>;
}
