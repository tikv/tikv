// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A new implementation of engine_traits using tirocks.
//!
//! When all features of engine_rocks are implemented in this module,
//! engine_rocks will be removed and TiKV will switch to tirocks.

extern crate tikv_alloc as _;

mod cf_options;
mod db_options;
mod db_vector;
mod engine;
mod engine_iterator;
mod snapshot;
mod status;
mod util;

pub use engine::*;
pub use engine_iterator::*;
pub use snapshot::RocksSnapshot;
pub use status::*;
pub use util::*;
