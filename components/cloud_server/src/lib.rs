// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate tikv_util;

#[macro_use]
pub mod setup;
pub mod memory;
mod node;
mod raftkv;
pub mod server;
mod service;
pub mod signal_handler;
pub use raftkv::*;
mod inner_server;
mod raft_client;
mod transport;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run() {
        println!("run")
    }
}
