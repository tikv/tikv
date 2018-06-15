// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(mpsc_recv_timeout, plugin, test, btree_range, collections_bound, box_syntax, const_fn)]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(stable_features, new_without_default, needless_pass_by_value, unreadable_literal)
)]

extern crate crc;
extern crate futures;
extern crate futures_cpupool;
extern crate grpcio as grpc;
#[cfg(feature = "mem-profiling")]
extern crate jemallocator;
extern crate kvproto;
#[macro_use]
extern crate log;
#[macro_use(slog_o, slog_kv)]
extern crate slog;
extern crate protobuf;
extern crate raft;
extern crate rand;
extern crate rocksdb;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;
extern crate tempdir;
extern crate test;
#[macro_use]
extern crate tikv;
extern crate time;
extern crate tipb;
extern crate tokio_timer;
extern crate toml;
extern crate uuid;

mod config;
mod coprocessor;
mod import;
mod pd;
mod raftstore;
mod raftstore_cases;
mod storage;
mod storage_cases;
mod util;

// The prefix "_" here is to guarantee running this case first.
#[test]
fn _0_ci_setup() {
    util::ci_setup();
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
