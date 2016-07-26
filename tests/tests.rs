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

#![feature(plugin)]
#![feature(test)]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![feature(btree_range, collections_bound)]
#![feature(box_syntax)]
#![allow(new_without_default)]
#![feature(const_fn)]

#[macro_use]
extern crate log;
extern crate protobuf;
#[macro_use]
extern crate tikv;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate uuid;
extern crate mio;
extern crate kvproto;
extern crate tipb;
extern crate time;
extern crate rustc_serialize;
extern crate test;

use std::env;

mod test_raft;
mod test_raft_snap;
mod test_raft_paper;
mod test_raft_flow_control;
mod test_raw_node;
mod raftstore;
mod coprocessor;
mod storage;
mod util;
mod pd;

#[test]
fn _travis_setup() {
    // Set up travis test fail case log.
    // The prefix "_" here is to guarantee running this case first.
    if env::var("TRAVIS").is_ok() && env::var("LOG_FILE").is_ok() {
        self::util::init_log();
    }
}
