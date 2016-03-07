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

mod raftserver;
