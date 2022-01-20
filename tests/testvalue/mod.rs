// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]

extern crate test;

mod test_unsafe_recovery;
