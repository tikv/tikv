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
#[path="../../tests/raftserver/util.rs"]
mod util;
#[path="../../tests/raftserver/cluster.rs"]
mod cluster;
#[path="../../tests/raftserver/node.rs"]
mod node;
#[path="../../tests/raftserver/server.rs"]
mod server;
#[path="../../tests/raftserver/pd.rs"]
mod pd;
#[path="../../tests/raftserver/pd_ask.rs"]
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

mod raftserver;
mod mvcc;

fn print_result(smp: BenchSamples) {
    println!("{}", test::fmt_bench_samples(&smp));
}

fn main() {
    // TODO allow user to specify flag to just bench some cases.
    raftserver::bench_raftserver();
    mvcc::bench_engine();
}
