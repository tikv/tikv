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
    + SstExt
    + IOLimiterExt
    + TablePropertiesExt
    + Send
    + Sync
    + Clone
    + Debug
    + 'static
{
    type Snapshot: Snapshot;
    type WriteBatch: WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::WriteBatch) -> Result<()>;
    fn write(&self, wb: &Self::WriteBatch) -> Result<()> {
        self.write_opt(&WriteOptions::default(), wb)
    }
    fn write_batch(&self) -> Self::WriteBatch;
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch;
    fn snapshot(&self) -> Self::Snapshot;
    fn sync(&self) -> Result<()>;

    fn cf_names(&self) -> Vec<&str>;
}
