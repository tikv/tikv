// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(box_patterns)]
#![feature(custom_test_frameworks)]
#![feature(get_mut_unchecked)]
#![test_runner(test_util::run_tests)]

extern crate test;

#[macro_use]
extern crate tikv_util;

mod backup;
mod config;
mod coprocessor;
mod import;
mod pd;
mod raftstore;
mod resource_metering;
mod server;
mod server_encryption;
mod storage;
