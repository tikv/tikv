// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(box_patterns)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]
#![allow(clippy::needless_pass_by_ref_mut)]
#![allow(clippy::extra_unused_type_parameters)]

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
