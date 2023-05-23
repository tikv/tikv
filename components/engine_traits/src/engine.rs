// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Debug, str};

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
    + DbOptionsExt
    + CfNamesExt
    + CfOptionsExt
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
    + Checkpointable
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
    fn flush_metrics(&self, instance: &str) {
        let mut reporter = Self::StatisticsReporter::new(instance);
        reporter.collect(self);
        reporter.flush();
    }

    /// Cast to a concrete engine type
    ///
    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;

    /// Returns false if KvEngine can't apply snapshot for this region now.
    /// Some KvEngines need to do some transforms before apply data from
    /// snapshot. These procedures can be batched in background if there are
    /// more than one incoming snapshots, thus not blocking applying thread.
    fn can_apply_snapshot(&self, _is_timeout: bool, _new_batch: bool, _region_id: u64) -> bool {
        true
    }

    /// A method for test to expose inner db refcount in order to make sure a
    /// full release of engine.
    #[cfg(feature = "testexport")]
    fn inner_refcount(&self) -> usize;
}
