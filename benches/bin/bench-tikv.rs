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
#![plugin(clippy)]
#![feature(btree_range, collections_bound)]

#[macro_use]
extern crate log;
extern crate protobuf;
#[macro_use]
extern crate tikv;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate uuid;
extern crate test;
extern crate kvproto;

#[path="../../tests/util.rs"]
mod test_util;
#[path="../../tests/raftstore/util.rs"]
mod util;
#[path="../../tests/raftstore/cluster.rs"]
mod cluster;
#[path="../../tests/raftstore/node.rs"]
mod node;
#[path="../../tests/raftstore/server.rs"]
mod server;
#[path="../../tests/raftstore/pd.rs"]
mod pd;
#[path="../../tests/raftstore/pd_ask.rs"]
mod pd_ask;

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

mod raftstore;
mod mvcc;

fn print_result(smp: BenchSamples) {
    println!("{}", test::fmt_bench_samples(&smp));
}

fn main() {
    // TODO allow user to specify flag to just bench some cases.
    raftstore::bench_raftstore();
    mvcc::bench_engine();
}
