// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;

mod coprocessor;
mod keybuilder;
mod raftkv;
mod serialization;
mod storage;
mod util;
mod writebatch;

#[bench]
fn _bench_check_requirement(_: &mut test::Bencher) {
    tikv_util::config::check_max_open_fds(4096).unwrap();
}
