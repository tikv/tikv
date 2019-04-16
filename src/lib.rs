// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![crate_type = "lib"]
#![cfg_attr(test, feature(test))]
#![recursion_limit = "200"]
#![feature(cell_update)]
#![feature(fnbox)]
#![feature(proc_macro_hygiene)]
#![feature(range_contains)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use(
    kv,
    slog_kv,
    slog_trace,
    slog_error,
    slog_warn,
    slog_info,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
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
#[cfg(test)]
extern crate test;

pub mod config;
pub mod coprocessor;
pub mod import;
pub mod pd;
pub mod raftstore;
pub mod server;
pub mod storage;
