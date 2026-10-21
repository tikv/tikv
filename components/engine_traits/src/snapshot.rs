// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{
    CfNamesExt, MvccBlockSampleOptions, MvccBlockSampleStats, Result, SnapshotMiscExt,
    iterable::Iterable, peekable::Peekable,
};

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

    /// Samples the rows of the MVCC data in `[start, end)` that are visible
    /// at `read_ts` by reading a fraction of the data blocks of the write CF.
    /// Every visible row is returned with probability `1 / spacing`. `start`
    /// and `end` are data keys without a timestamp; `f` gets the data key of
    /// the row (without timestamp) and its value. Returns `None` if the engine
    /// cannot sample blocks. The third argument of `f` identifies the sampled
    /// block (or memtable row) that the row came from.
    fn sample_mvcc_rows(
        &self,
        _start: &[u8],
        _end: &[u8],
        _read_ts: u64,
        _opts: &MvccBlockSampleOptions,
        _f: &mut dyn FnMut(&[u8], &[u8], u64),
    ) -> Result<Option<MvccBlockSampleStats>> {
        Ok(None)
    }
}
