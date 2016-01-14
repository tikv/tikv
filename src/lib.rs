#![crate_type = "lib"]
#![feature(test)]
#![feature(vec_push_all)]

#[macro_use]
extern crate log;
extern crate test;
extern crate protobuf;
extern crate bytes;
extern crate byteorder;

pub use storage::{Storage, Dsn};

pub mod util;
pub mod raft;
pub mod proto;
mod storage;
