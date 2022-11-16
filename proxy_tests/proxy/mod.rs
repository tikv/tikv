#![feature(box_patterns)]
#![feature(test)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]
#![recursion_limit = "100"]

#[macro_use]
extern crate slog_global;

mod config;
mod flashback;
mod normal;
mod proxy;
mod server_cluster_test;
mod snapshot;
mod util;
mod write;
