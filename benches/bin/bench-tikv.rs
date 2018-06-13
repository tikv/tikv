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

extern crate clap;
extern crate futures;
extern crate grpcio as grpc;
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
extern crate time;
#[macro_use]
extern crate tikv;
extern crate tokio_timer;

#[allow(dead_code)]
#[path = "../../tests/raftstore/cluster.rs"]
mod cluster;
#[path = "../../tests/raftstore/node.rs"]
mod node;
#[allow(dead_code)]
#[path = "../../tests/raftstore/pd.rs"]
mod pd;
#[path = "../../tests/raftstore/server.rs"]
mod server;
#[allow(dead_code)]
#[path = "../../tests/util/mod.rs"]
mod test_util;
#[allow(dead_code)]
#[path = "../../tests/raftstore/transport_simulate.rs"]
mod transport_simulate;
#[allow(dead_code)]
#[path = "../../tests/raftstore/util.rs"]
mod util;

use clap::{App, Arg, ArgGroup};
use test::BenchSamples;

/// shortcut to bench a function.
macro_rules! bench {
    ($name:expr, $block:expr) => {{
        use std::sync::mpsc::channel;
        use test::{self, bench};
        let desc = test::TestDesc {
            name: test::TestName::DynTestName($name.into()),
            ignore: false,
            should_panic: test::ShouldPanic::No,
            allow_fail: false,
        };
        let (tx, rx) = channel();
        bench::benchmark(desc, tx, false, |b| b.iter($block));
        let (_desc, result, _stdout) = rx.recv().unwrap();
        match result {
            test::TestResult::TrBench(samples) => samples,
            _ => unreachable!(),
        }
    }};
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

mod mvcc;
mod raftstore;

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
