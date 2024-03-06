// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]

mod test_endpoint;
mod test_observe;
mod test_register;
mod test_resolve;

#[path = "../mod.rs"]
mod testsuite;
pub use testsuite::*;
