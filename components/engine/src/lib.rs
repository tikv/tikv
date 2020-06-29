// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub use rocksdb::{CFHandle, DBIterator, Env, Range, ReadOptions, WriteOptions, DB};
mod errors;
pub use crate::errors::*;
