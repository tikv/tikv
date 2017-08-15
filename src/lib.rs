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
#![feature(test)]
#![feature(fnbox)]
#![feature(alloc)]
#![feature(slice_patterns)]
#![feature(box_syntax)]
#![feature(try_from)]
#![cfg_attr(feature = "dev", feature(plugin))]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![recursion_limit = "100"]
#![feature(ascii_ctype)]
#![allow(module_inception)]
#![allow(should_implement_trait)]
#![allow(large_enum_variant)]
#![allow(needless_pass_by_value)]
#![allow(unreadable_literal)]
#![allow(new_without_default_derive)]
#![allow(verbose_bit_mask)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate quick_error;
extern crate test;
extern crate protobuf;
extern crate byteorder;
extern crate rand;
extern crate mio;
extern crate tempdir;
extern crate rocksdb;
extern crate kvproto;
extern crate time;
extern crate tipb;
extern crate threadpool;
extern crate num;
extern crate libc;
extern crate crc;
#[cfg(unix)]
extern crate nix;
extern crate alloc;
extern crate chrono;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate lazy_static;
extern crate backtrace;
extern crate url;
extern crate fs2;
extern crate regex;
extern crate grpcio as grpc;
extern crate fnv;
extern crate ordermap;
extern crate flat_map;
extern crate futures;
extern crate tokio_core;
extern crate tokio_timer;
extern crate serde_json;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate toml;
extern crate sys_info;

#[macro_use]
pub mod util;
pub mod config;
pub mod raft;
pub mod storage;
pub mod raftstore;
pub mod pd;
pub mod server;
pub mod coprocessor;

pub use storage::Storage;
