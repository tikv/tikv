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
#![feature(box_patterns)]

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
pub mod into_other;
pub mod raftstore;
pub mod server;
pub mod storage;

// Detect tikv enabled features.
fn detect_enabled_features() -> String {
    macro_rules! detect {
        ($f:expr, $feat:tt) => {
            if cfg!(feature = $feat) {
                $f.push_str($feat);
                $f.push(' ');
            }
        };
    }

    let mut flags = String::new();
    detect!(flags, "tcmalloc");
    detect!(flags, "jemalloc");
    detect!(flags, "mimalloc");
    detect!(flags, "portable");
    detect!(flags, "sse");
    detect!(flags, "mem-profiling");
    detect!(flags, "failpoints");
    detect!(flags, "prost-codec");
    detect!(flags, "protobuf-codec");
    flags.pop(); // Try to trim the last ' '.
    flags
}

// Detect tikv build profile.
fn detect_build_profile() -> &'static str {
    // Detect build profile by debug_assertions see more:
    // https://users.rust-lang.org/t/conditional-compilation-for-debug-release/1098
    // TODO: Find a better way to detect, since debug_assertions can be enabled
    //       in a custom profile.
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

/// Returns the tikv version information.
// TODO: Should we remove `UTC Build Time`? It breaks reproducible builds.
pub fn tikv_version_info() -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}\
         \nFeatures:          {}\
         \nBuild Profile:     {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("TIKV_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or(fallback),
        option_env!("TIKV_BUILD_TIME").unwrap_or(fallback),
        option_env!("TIKV_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        detect_enabled_features(),
        detect_build_profile(),
    )
}

/// Prints the tikv version information to the standard output.
pub fn log_tikv_info() {
    info!("Welcome to TiKV");
    for line in tikv_version_info().lines() {
        info!("{}", line);
    }
}
