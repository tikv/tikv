// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::*;

// FIXME: Revisit the remaining types and methods on KvEngine. Some of these are
// here for lack of somewhere better to put them at the time of writing.
// Consider moving everything into other traits and making KvEngine essentially
// a trait typedef.

/// A TiKV key-value store
pub trait KvEngine:
    Peekable
    + SyncMutable
    + Iterable
    + WriteBatchExt
    + DBOptionsExt
    + CFNamesExt
    + CFOptionsExt
    + ImportExt
    + SstExt
    + CompactExt
    + RangePropertiesExt
    + MvccPropertiesExt
    + TtlPropertiesExt
    + TablePropertiesExt
    + PerfContextExt
    + MiscExt
    + Send
    + Sync
    + Clone
    + Debug
    + Unpin
    + 'static
{
    /// A consistent read-only snapshot of the database
    type Snapshot: Snapshot;

    /// Create a snapshot
    fn snapshot(&self) -> Self::Snapshot;

    /// Syncs any writes to disk
    fn sync(&self) -> Result<()>;

    /// Flush metrics to prometheus
    ///
    /// `instance` is the label of the metric to flush.
    fn flush_metrics(&self, _instance: &str) {}

    /// Reset internal statistics
    fn reset_statistics(&self) {}

    /// Cast to a concrete engine type
    ///
    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}

/// A factory trait to create new engine.
///
// It should be named as `EngineFactory` for consistency, but we are about to rename
// engine to tablet, so always use tablet for new traits/types.
pub trait TabletFactory<EK> {
    fn create_tablet(&self) -> Result<EK>;
}
