// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(test)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]
#![recursion_limit = "100"]
#![feature(vec_into_raw_parts)]
#![feature(slice_pattern)]

#[macro_use]
extern crate slog_global;

mod config;
mod engine;
mod fast_add_peer;
mod ffi;
mod ingest;
mod mock;
mod normal;
mod region;
mod replica_read;
mod server_cluster_test;
mod snapshot;
mod store;
mod utils;
mod v1_specific;
mod v2_specific;
mod write;
