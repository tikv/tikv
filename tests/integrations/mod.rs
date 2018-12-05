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

#![feature(box_syntax)]
#![feature(test)]

extern crate crc;
extern crate futures;
extern crate futures_cpupool;
extern crate grpcio as grpc;
extern crate kvproto;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate raft;
extern crate rand;
extern crate rocksdb;
extern crate slog;
extern crate tempdir;
extern crate test;
extern crate tipb;
extern crate toml;
extern crate uuid;

#[macro_use]
extern crate tikv;
extern crate test_coprocessor;
extern crate test_raftstore;
extern crate test_storage;
#[macro_use]
extern crate test_util;

mod config;
mod coprocessor;
mod import;
mod pd;
mod raftstore;
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
