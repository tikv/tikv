#![crate_type = "lib"]
#![feature(test)]
#![feature(btree_range, collections_bound)]

#[macro_use]
extern crate log;
extern crate test;
extern crate protobuf;
extern crate bytes;
extern crate byteorder;
extern crate rand;
extern crate mio;

pub mod util;
pub mod raft;
pub mod proto;
pub mod storage;
pub mod kvserver;
pub mod rpc;

pub use storage::{Storage, Dsn};
