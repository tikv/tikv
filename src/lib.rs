#![crate_type = "lib"]

#[macro_use]
extern crate log;
extern crate protobuf;
extern crate bytes;
extern crate byteorder;

pub use storage::{Storage, Descriptor};

pub mod util;
pub mod raft;
mod storage;
