#![crate_type = "lib"]

#[macro_use]
extern crate log;
extern crate protobuf;

pub use storage::{Storage, Descriptor};

pub mod util;
pub mod raft;
mod storage;
