// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod rocks;
pub use crate::rocks::{CFHandle, DBIterator, Env, Range, ReadOptions, WriteOptions, DB};
mod errors;
pub use crate::errors::*;
