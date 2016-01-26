#![crate_type = "lib"]
#![allow(unused_features)]
#![feature(test)]
#![feature(btree_range, collections_bound)]
#![feature(std_panic, recover)]
#![feature(fnbox)]

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

pub mod util;
pub mod raft;
pub mod proto;
pub mod storage;
pub mod kvserver;
pub mod rpc;

pub use storage::{Storage, Dsn};
pub mod raftserver;
