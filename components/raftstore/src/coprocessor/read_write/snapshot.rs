// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;

use engine_traits::SnapshotContext;

/// ObservedSnapshot is a trait that represents data that are observed during
/// taking snapshot.
/// It inherits from Any to allow downcasting to concrete types.
pub trait ObservedSnapshot: Any + Send + Sync {}

/// SnapshotObserver is a trait that observes the snapshot process.
pub trait SnapshotObserver: Send {
    /// on_snapshot is called when raftstore is taking RegionSnapshot.
    fn on_snapshot(
        &self,
        ctx: Option<SnapshotContext>,
        sequence_number: u64,
    ) -> Box<dyn ObservedSnapshot>;
}
