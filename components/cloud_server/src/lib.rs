// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(num_as_ne_bytes)]
#![feature(shrink_to)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate tikv_util;

#[macro_use]
pub mod setup;
pub mod memory;
mod node;
mod raftkv;
pub mod server;
pub mod service;
pub mod signal_handler;
pub use raftkv::*;
mod metrics;
mod raft_client;
mod resolve;
mod tikv_server;
mod transport;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run() {
        println!("run")
    }
}
