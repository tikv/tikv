// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
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
mod node;
mod raftkv;
pub mod server;
pub mod service;
pub mod signal_handler;
pub use raftkv::*;
mod raft_client;
mod resolve;
mod tikv_server;
mod transport;

#[cfg(test)]
mod tests {
    use super::*;
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
