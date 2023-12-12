// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{Iterable, Snapshot, WriteBatchExt};

/// RegionCacheEngine works as a region cache caching some regions (in Memory or
/// NVME for instance) to improve the read performance.
pub trait RegionCacheEngine:
    WriteBatchExt + Iterable + Debug + Clone + Unpin + Send + Sync + 'static
{
    type Snapshot: Snapshot;

    // If None is returned, the RegionCacheEngine is currently not readable for this
    // region or read_ts.
    fn snapshot(&self, region_id: u64, read_ts: u64) -> Option<Self::Snapshot>;
}
