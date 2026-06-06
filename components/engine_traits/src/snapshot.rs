// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{CfNamesExt, Result, SnapshotMiscExt, iterable::Iterable, peekable::Peekable};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBlockKeyAnchor {
    pub user_key: Vec<u8>,
    pub range_size: u64,
}

/// A consistent read-only view of the database.
///
/// Snapshots can be sent and shared, but not cloned. To make a snapshot
/// clonable, call `into_sync` to create a `SyncSnapshot`.
pub trait Snapshot
where
    Self:
        'static + Peekable + Iterable + CfNamesExt + SnapshotMiscExt + Send + Sync + Sized + Debug,
{
    /// Whether the snapshot acquired hit the in memory engine. It always
    /// returns false if the in memory engine is disabled.
    fn in_memory_engine_hit(&self) -> bool {
        false
    }

    fn approximate_key_anchors_cf(
        &self,
        _cf: &str,
        _lower_bound: Option<&[u8]>,
        _upper_bound: Option<&[u8]>,
    ) -> Result<Vec<DataBlockKeyAnchor>> {
        Ok(Vec::new())
    }
}
