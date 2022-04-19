// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(shrink_to)]
#![recursion_limit = "400"]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate tikv_util;

#[macro_use]
pub mod setup;
mod node;
mod raftkv;
pub mod server;
pub mod service;
pub mod signal_handler;
pub use raftkv::*;
mod raft_client;
mod resolve;
mod status_server;
mod tikv_server;
mod transport;

pub use tikv_server::run_tikv;

#[cfg(test)]
mod tests {
    use crate::tikv_server::run_tikv;
    use tikv::config::TiKvConfig;

    #[test]
    fn test_run() {
        println!("run")
    }

    #[test]
    fn test_run_tikv() {
        let config = TiKvConfig::default();
        run_tikv(config);
    }
}
