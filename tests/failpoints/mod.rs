// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(box_patterns)]
#![feature(test)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]
#![recursion_limit = "100"]

extern crate test;
#[macro_use]
extern crate tikv_util;

mod cases;
