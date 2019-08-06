// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;

#[macro_use(slog_error, slog_info, slog_debug)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate tikv_util;
extern crate pd_client;

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
    if let Err(e) = tikv_util::config::check_max_open_fds(4096) {
        panic!(
            "To run test, please make sure the maximum number of open file descriptors not \
             less than 2000: {:?}",
            e
        );
    }
}
