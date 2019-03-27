// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![crate_type = "lib"]
#![cfg_attr(test, feature(test))]
#![recursion_limit = "200"]
#![feature(cell_update)]
#![feature(fnbox)]
#![feature(proc_macro_hygiene)]
#![feature(range_contains)]
#![feature(specialization)]
// Currently this raises some false positives, so we allow it:
// https://github.com/rust-lang-nursery/rust-clippy/issues/2638
#![allow(clippy::nonminimal_bool)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate fail;
#[macro_use]
extern crate futures;
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
    slog_o,
    slog_kv,
    slog_trace,
    slog_error,
    slog_warn,
    slog_info,
    slog_debug,
    slog_crit,
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
#[cfg(test)]
extern crate test;
use grpcio as grpc;

#[macro_use]
pub mod util;
pub mod config;
pub mod coprocessor;
pub mod import;
pub mod pd;
pub mod raftstore;
pub mod server;
pub mod storage;

pub use crate::storage::Storage;
