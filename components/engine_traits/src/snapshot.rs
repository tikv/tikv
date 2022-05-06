// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{iterable::Iterable, peekable::Peekable};

/// A consistent read-only view of the database.
///
/// Snapshots can be sent and shared, but not cloned. To make a snapshot
/// clonable, call `into_sync` to create a `SyncSnapshot`.
pub trait Snapshot
where
    Self: 'static + Peekable + Iterable + Send + Sync + Sized + Debug,
{
    fn cf_names(&self) -> Vec<&str>;
}
