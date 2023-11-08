// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(assert_matches)]

mod test_cdc;
mod test_flow_control;

#[path = "../mod.rs"]
mod testsuite;
pub use testsuite::*;
