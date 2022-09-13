// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A new implementation of engine_traits using tirocks.
//!
//! When all features of engine_rocks are implemented in this module,
//! engine_rocks will be removed and TiKV will switch to tirocks.

extern crate tikv_alloc as _;

mod status;

pub use status::*;
