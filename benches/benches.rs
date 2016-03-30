#![feature(plugin)]
#![feature(test)]
#![plugin(clippy)]

#[macro_use]
extern crate log;
extern crate test;
extern crate mio;
extern crate rand;

mod channel;

#[path="../tests/util.rs"]
mod util;

use test::Bencher;

use util::KvGenerator;

#[bench]
fn bench_kv_iter(b: &mut Bencher) {
    let mut g = KvGenerator::new(100, 1000);
    b.iter(|| g.next());
}
