// Copyright 2017 PingCAP, Inc.
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

// TODO: deny it once Manishearth/rust-clippy#1586 is fixed.
#![allow(never_loop)]
#![allow(needless_pass_by_value)]

extern crate log;
extern crate test;
extern crate mio;
extern crate rand;
extern crate tikv;
extern crate time;
extern crate tempdir;
extern crate rocksdb;

mod channel;
mod writebatch;

#[allow(dead_code)]
#[path="../tests/util.rs"]
mod util;

use test::Bencher;

use util::KvGenerator;

#[bench]
fn _bench_check_requirement(_: &mut test::Bencher) {
    if let Err(e) = tikv::util::config::check_max_open_fds(4096) {
        panic!("To run bench, please make sure the maximum number of open file descriptors not \
                less than 4096: {:?}",
               e);
    }
}

#[bench]
fn bench_kv_iter(b: &mut Bencher) {
    let mut g = KvGenerator::new(100, 1000);
    b.iter(|| g.next());
}
