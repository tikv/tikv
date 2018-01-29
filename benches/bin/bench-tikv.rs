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

#![allow(stable_features)]
#![feature(mpsc_recv_timeout)]
#![feature(plugin)]
#![feature(test)]
#![feature(box_syntax)]
#![feature(integer_atomics)]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![feature(btree_range, collections_bound)]
#![allow(new_without_default)]
#![allow(needless_pass_by_value)]
#![allow(unreadable_literal)]

extern crate futures;
extern crate grpcio as grpc;
extern crate kvproto;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate test;
#[macro_use]
extern crate tikv;

#[allow(dead_code)]
#[path = "../../tests/util/mod.rs"]
mod test_util;
#[allow(dead_code)]
#[path = "../../tests/raftstore/util.rs"]
mod util;
#[allow(dead_code)]
#[path = "../../tests/raftstore/cluster.rs"]
mod cluster;
#[path = "../../tests/raftstore/node.rs"]
mod node;
#[path = "../../tests/raftstore/server.rs"]
mod server;
#[allow(dead_code)]
#[path = "../../tests/raftstore/pd.rs"]
mod pd;
#[allow(dead_code)]
#[path = "../../tests/raftstore/transport_simulate.rs"]
mod transport_simulate;

use test::BenchSamples;

/// shortcut to bench a function.
macro_rules! bench {
    ($($stmt:stmt);+) => ({
        use test::bench;
        bench::benchmark(|b| {
            b.iter(|| {
                $($stmt);+
            })
        }
    )});
}

/// Same as print, but will flush automatically.
macro_rules! printf {
    ($($arg:tt)*) => ({
        use std::io::{self, Write};
        print!($($arg)*);
        io::stdout().flush().unwrap();
    });
}

#[allow(dead_code)]
mod utils;

mod raftstore;
mod mvcc;
mod mvcctxn;

fn print_result(smp: BenchSamples) {
    println!("{}", test::fmt_bench_samples(&smp));
}

use std::env;

fn main() {
    if let Err(e) = tikv::util::config::check_max_open_fds(4096) {
        panic!(
            "To run bench, please make sure the maximum number of open file descriptors not \
             less than 4096: {:?}",
            e
        );
    }

    let mut args: Vec<_> = env::args().skip(1).collect();

    let available_options = vec![
        "raftstore",
        "tombstone-scan",
        "mvcctxn",
        "concurrent-batch-mvcctxn",
    ];

    if args.is_empty() {
        args = available_options.iter().map(|&s| String::from(s)).collect();
    }

    if args[0] == "-h" || args[0] == "--help" {
        eprintln!(
            "Usage: bench-tikv [item1] [item2] [item3] ...\n\
             where item1, item2, item3, ... are bench items.\n\n\
             Available options are:"
        );
        for item in available_options {
            eprintln!("    {}", item);
        }

        eprintln!(
            "\nRun with no args to do all benches.\n\
             Run with -h or --help to show this help."
        );
    } else {
        for item in args {
            match item.as_ref() {
                "raftstore" => raftstore::bench_raftstore(),
                "tombstone-scan" => mvcc::bench_engine(),
                "mvcctxn" => mvcctxn::bench_mvcctxn(),
                _ => eprintln!("*** Unknown bench item {}", item),
            }
        }
    }
}
