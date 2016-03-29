#![feature(plugin)]
#![feature(test)]
#![plugin(clippy)]
#![feature(btree_range, collections_bound)]

#[macro_use]
extern crate log;
extern crate protobuf;
extern crate env_logger;
#[macro_use]
extern crate tikv;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate uuid;
extern crate test;
extern crate kvproto;
extern crate mio;

mod raftserver;
mod mvcc;
mod channel;

#[path="../tests/util.rs"]
mod util;

use test::Bencher;

use util::KVGenerator;

#[bench]
fn bench_kv_iter(b: &mut Bencher) {
    let mut g = KVGenerator::new(100, 1000);
    b.iter(|| g.next());
}
