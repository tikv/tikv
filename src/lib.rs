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
#![feature(label_break_value)]
#![feature(try_from)]
#![feature(fnbox)]
#![feature(alloc)]
#![feature(slice_patterns)]
#![feature(box_syntax)]
#![feature(integer_atomics)]
#![feature(duration_as_u128)]
#![feature(proc_macro_hygiene)]
#![feature(const_int_ops)]
#![feature(cell_update)]
#![feature(ptr_offset_from)]
#![recursion_limit = "200"]
#![feature(range_contains)]
// Currently this raises some false positives, so we allow it:
// https://github.com/rust-lang-nursery/rust-clippy/issues/2638
#![cfg_attr(feature = "cargo-clippy", allow(clippy::nonminimal_bool))]

extern crate alloc;
extern crate backtrace;
#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate chrono;
extern crate chrono_tz;
extern crate crc;
extern crate crossbeam;
extern crate crypto;
#[macro_use]
extern crate fail;
extern crate fnv;
extern crate fs2;
#[macro_use]
extern crate futures;
extern crate futures_cpupool;
extern crate grpcio as grpc;
extern crate hashbrown;
extern crate hex;
extern crate indexmap;
extern crate kvproto;

#[macro_use]
extern crate lazy_static;
extern crate libc;
extern crate log;
extern crate murmur3;
extern crate num;
extern crate num_traits;
#[macro_use]
extern crate prometheus;
#[cfg(target_os = "linux")]
extern crate procinfo;
extern crate prometheus_static_metric;
extern crate protobuf;
#[macro_use]
extern crate quick_error;
extern crate raft;
extern crate rand;
extern crate regex;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
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
extern crate slog_async;
#[macro_use]
extern crate slog_derive;
#[macro_use]
extern crate slog_global;
extern crate slog_term;
extern crate sys_info;
extern crate tempdir;
#[cfg(test)]
extern crate test;
extern crate tikv_alloc;
extern crate time;
extern crate tipb;
extern crate tokio;
extern crate tokio_core;
extern crate tokio_executor;
extern crate tokio_timer;
#[cfg(test)]
extern crate toml;
extern crate url;
#[cfg(test)]
extern crate utime;
extern crate uuid;
extern crate zipf;
#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate more_asserts;
extern crate base64;
extern crate cop_datatype;
extern crate flate2;
extern crate hyper;
extern crate log_wrappers;
extern crate panic_hook;
extern crate safemem;
extern crate smallvec;
extern crate tokio_threadpool;
#[macro_use]
extern crate vlog;

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
