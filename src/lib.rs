#![crate_type = "lib"]
#![allow(unused_features)]
#![feature(test)]
#![feature(btree_range, collections_bound)]
#![feature(std_panic, recover)]
#![feature(fnbox)]
#![feature(plugin)]
#![feature(box_syntax)]
#![plugin(clippy)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate quick_error;
extern crate test;
extern crate protobuf;
extern crate bytes;
extern crate byteorder;
extern crate rand;
extern crate mio;
extern crate tempdir;
extern crate rocksdb;
extern crate uuid;
extern crate kvproto;
extern crate time;

#[cfg(test)]
extern crate env_logger;

#[macro_use]
pub mod util;
pub mod raft;
#[allow(clippy)]
pub mod storage;

pub use storage::{Storage, Dsn};
pub mod raftstore;
pub mod pd;
pub mod server;
