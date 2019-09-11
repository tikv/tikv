// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! TiKV - A distributed key/value database
//!
//! TiKV ("Ti" stands for Titanium) is an open source distributed
//! transactional key-value database. Unlike other traditional NoSQL
//! systems, TiKV not only provides classical key-value APIs, but also
//! transactional APIs with ACID compliance. TiKV was originally
//! created to complement [TiDB], a distributed HTAP database
//! compatible with the MySQL protocol.
//!
//! [TiDB]: https://github.com/pingcap/tidb
//!
//! The design of TiKV is inspired by some great distributed systems
//! from Google, such as BigTable, Spanner, and Percolator, and some
//! of the latest achievements in academia in recent years, such as
//! the Raft consensus algorithm.

#![crate_type = "lib"]
#![cfg_attr(test, feature(test))]
#![recursion_limit = "200"]
#![feature(cell_update)]
#![feature(proc_macro_hygiene)]
#![feature(specialization)]
#![feature(const_fn)]
#![feature(mem_take)]

#[macro_use]
extern crate bitflags;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use(slog_trace, slog_error, slog_warn, slog_info, slog_debug)]
extern crate slog;
#[macro_use]
extern crate slog_derive;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate more_asserts;
#[macro_use]
extern crate vlog;
#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate failure;

#[cfg(test)]
extern crate test;

pub mod config;
pub mod coprocessor;
pub mod import;
pub mod raftstore;
pub mod server;
pub mod storage;

/// Returns the tikv version information.
pub fn tikv_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("TIKV_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_TIME").unwrap_or(fallback),
        option_env!("TIKV_BUILD_RUSTC_VERSION").unwrap_or(fallback),
    )
}

/// Prints the tikv version information to the standard output.
pub fn log_tikv_info() {
    info!("Welcome to TiKV");
    for line in tikv_version_info().lines() {
        info!("{}", line);
    }
}
