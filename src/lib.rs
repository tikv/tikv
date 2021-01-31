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
#![recursion_limit = "400"]
#![feature(cell_update)]
#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(const_fn)]
#![feature(box_patterns)]
#![feature(shrink_to)]
#![feature(drain_filter)]
#![feature(negative_impls)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
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
pub mod read_pool;
pub mod server;
pub mod storage;

/// Returns the tikv version information.
pub fn tikv_version_info(build_time: Option<&str>) -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nEdition:           {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}\
         \nEnable Features:   {}\
         \nProfile:           {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("TIKV_EDITION").unwrap_or("Community"),
        option_env!("TIKV_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or(fallback),
        build_time.unwrap_or(fallback),
        option_env!("TIKV_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        option_env!("TIKV_ENABLE_FEATURES")
            .unwrap_or(fallback)
            .trim(),
        option_env!("TIKV_PROFILE").unwrap_or(fallback),
    )
}

/// Prints the tikv version information to the standard output.
pub fn log_tikv_info(build_time: Option<&str>) {
    info!("Welcome to TiKV");
    for line in tikv_version_info(build_time)
        .lines()
        .filter(|s| !s.is_empty())
    {
        info!("{}", line);
    }
}
