#![crate_type = "lib"]
#![feature(vec_push_all)]

#[macro_use]
extern crate log;
extern crate protobuf;
extern crate bytes;
extern crate byteorder;

pub use storage::{Storage, Descriptor};

pub mod util;
pub mod raft;
mod storage;
