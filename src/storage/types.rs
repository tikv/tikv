// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use std::u64;

use crate::storage::mvcc::{Lock, Write};

pub use keys::{Key, KvPair, Value};

/// `MvccInfo` stores all mvcc information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default)]
pub struct MvccInfo {
    pub lock: Option<Lock>,
    /// commit_ts and write
    pub writes: Vec<(u64, Write)>,
    /// start_ts and value
    pub values: Vec<(u64, Value)>,
}
