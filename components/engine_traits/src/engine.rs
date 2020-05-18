// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::*;

// FIXME: Revisit the remaining types and methods on KvEngine. Some of these are
// here for lack of somewhere better to put them at the time of writing.
// Consider moving everything into other traits and making KvEngine essentially
// a trait typedef.

pub trait KvEngine:
    Peekable
    + SyncMutable
    + Iterable
    + WriteBatchExt
    + DBOptionsExt
    + CFNamesExt
    + CFHandleExt
    + ImportExt
    + SstExt
    + TablePropertiesExt
    + CompactExt
    + MiscExt
    + Send
    + Sync
    + Clone
    + Debug
    + 'static
{
    type Snapshot: Snapshot;

    fn snapshot(&self) -> Self::Snapshot;
    fn sync(&self) -> Result<()>;

    /// Flush out metrics. `instance` indicates its name.
    /// TODO: remove `shared_block_cache`.
    fn flush_metrics(&self, _instance: &str, _shared_block_cache: bool) {}
    /// Reset internal statistics.
    fn reset_statistics(&self) {}

    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}

pub trait WriteBatchVecExt<E: KvEngine> {
    fn write_batch_vec(e: &E, vec_size: usize, cap: usize) -> Self;
    fn write_to_engine(&self, e: &E, opts: &WriteOptions) -> Result<()>;
}
