// Copyright 2016 TiKV Project Authors.
#![feature(test)]

use grpcio as grpc;

extern crate test;

#[macro_use(
    slog_kv,
    slog_error,
    slog_info,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate tikv;
#[macro_use]
extern crate test_util;

mod config;
mod coprocessor;
mod import;
mod pd;
mod raftstore;
mod server;
mod storage;

// The prefix "_" here is to guarantee running this case first.
#[test]
fn _0_ci_setup() {
    test_util::setup_for_ci();
}

#[test]
fn _1_check_system_requirement() {
    if let Err(e) = tikv::util::config::check_max_open_fds(4096) {
        panic!(
            "To run test, please make sure the maximum number of open file descriptors not \
             less than 2000: {:?}",
            e
        );
    }
}
