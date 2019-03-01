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

#![feature(test)]

extern crate test;

use tikv;

mod channel;
mod coprocessor;
mod raftkv;
mod serialization;
mod storage;
mod util;
mod writebatch;

#[bench]
fn _bench_check_requirement(_: &mut test::Bencher) {
    tikv::util::config::check_max_open_fds(4096).unwrap();
}
