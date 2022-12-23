// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(box_patterns)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]

#[macro_use]
extern crate tikv_util;

mod raftstore_v2;
