// Copyright 2017 PingCAP, Inc.
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


#![feature(fnbox)]
#![feature(slice_patterns)]
#![feature(box_syntax)]
#![cfg_attr(feature = "dev", feature(plugin))]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![cfg_attr(feature = "no-fail", allow(dead_code))]
#![recursion_limit = "100"]
#![allow(module_inception)]
#![allow(should_implement_trait)]
#![allow(large_enum_variant)]
#![allow(needless_pass_by_value)]
#![allow(unreadable_literal)]
#![allow(new_without_default_derive)]
#![allow(verbose_bit_mask)]

#[macro_use]
extern crate log;
extern crate protobuf;
#[macro_use]
extern crate tikv;
extern crate rand;
extern crate rocksdb;
extern crate tempdir;
extern crate kvproto;
extern crate tipb;
extern crate grpcio as grpc;
extern crate futures;
extern crate futures_cpupool;
extern crate toml;
extern crate fail;
#[macro_use]
extern crate lazy_static;

#[allow(dead_code)]
mod raftstore;
#[cfg(not(feature = "no-fail"))]
mod failpoints_cases;

use std::sync::*;

lazy_static! {
    /// Failpoints are global structs, hence rules set in different cases
    /// may affect each other. So use a global lock to synchronize them.
    static ref LOCK: Mutex<()> = Mutex::new(());
}

fn setup<'a>() -> MutexGuard<'a, ()> {
    let guard = LOCK.lock().unwrap();
    fail::teardown();
    fail::setup();
    guard
}
