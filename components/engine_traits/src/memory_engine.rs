// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{Iterable, WriteBatchExt};

/// A memory enigne works as a region cache caching some regions to improve the
/// read performance.
pub trait MemoryEngine:
    WriteBatchExt + Iterable + Debug + Clone + Unpin + Send + Sync + 'static
{
}
