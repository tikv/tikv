// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use crate::Iterable;

pub trait MemoryEngine: Debug + Clone + Iterable + Unpin + Send + Sync + 'static {}
