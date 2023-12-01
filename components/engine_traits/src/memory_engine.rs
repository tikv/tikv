// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::{Iterable, WriteBatchExt};

pub trait MemoryEngine:
    WriteBatchExt + Iterable + Debug + Clone + Unpin + Send + Sync + 'static
{
}
