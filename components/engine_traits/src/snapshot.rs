// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{iterable::Iterable, peekable::Peekable, CfNamesExt, SnapshotMiscExt};

/// A consistent read-only view of the database.
///
/// Snapshots can be sent and shared, but not cloned. To make a snapshot
/// clonable, call `into_sync` to create a `SyncSnapshot`.
pub trait Snapshot
where
    Self:
        'static + Peekable + Iterable + CfNamesExt + SnapshotMiscExt + Send + Sync + Sized + Debug,
{
    /// Whether the snapshot acquired hit the cached region in the region cache
    /// engine. It always returns false if the region cahce engine is not
    /// enabled.
    fn region_cache_engine_hit(&self) -> bool {
        false
    }
}
