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

#![feature(mpsc_recv_timeout)]
#![feature(test)]
#![feature(box_syntax)]
#![feature(integer_atomics)]
#![feature(btree_range, collections_bound)]

extern crate clap;
extern crate kvproto;
extern crate rocksdb;
extern crate test;
extern crate tikv;
extern crate tikv_test;

#[allow(dead_code)]
#[macro_use]
mod utils;
mod mvcc;
mod raftstore;

use clap::{App, Arg, ArgGroup};
use test::BenchSamples;

fn print_result(smp: BenchSamples) {
    println!("{}", test::fmt_bench_samples(&smp));
}

fn main() {
    if let Err(e) = tikv::util::config::check_max_open_fds(4096) {
        panic!(
            "To run bench, please make sure the maximum number of open file descriptors not \
             less than 4096: {:?}",
            e
        );
    }

    let available_benches = ["raftstore", "mvcc"];

    let matches = App::new("TiKV Benchmark")
        .args(
            &available_benches
                .iter()
                .map(|name| Arg::with_name(name))
                .collect::<Vec<_>>(),
        )
        .group(
            ArgGroup::with_name("benches")
                .args(&available_benches)
                .multiple(true),
        )
        .get_matches();

    let benches: Vec<_> = if let Some(args) = matches.values_of("benches") {
        args.collect()
    } else {
        available_benches.to_vec()
    };

    println!("Begin to run: {}", benches.join(", "));

    for item in benches {
        match item {
            "raftstore" => raftstore::bench_raftstore(),
            "mvcc" => mvcc::bench_engine(),
            _ => eprintln!("error: Unknown bench item {}", item),
        }
    }
}
