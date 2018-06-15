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

#![recursion_limit = "100"]
#![feature(slice_patterns, box_syntax, test)]
#![cfg_attr(feature = "no-fail", allow(dead_code))]
#![cfg_attr(
    feature = "cargo-clippy",
    allow(
        module_inception, should_implement_trait, large_enum_variant, needless_pass_by_value,
        unreadable_literal, new_without_default_derive, verbose_bit_mask
    )
)]

extern crate fail;
extern crate futures;
extern crate futures_cpupool;
extern crate grpcio as grpc;
extern crate kvproto;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use(slog_o, slog_kv)]
extern crate slog;
extern crate protobuf;
extern crate raft;
extern crate rand;
extern crate rocksdb;
extern crate slog_async;
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;
extern crate tempdir;
extern crate test;
#[macro_use]
extern crate tikv;
extern crate time;
extern crate tipb;
extern crate tokio_timer;
extern crate toml;

#[cfg(not(feature = "no-fail"))]
mod failpoints_cases;
#[allow(dead_code)]
mod raftstore;
#[allow(dead_code)]
mod storage;
#[allow(dead_code)]
mod util;

use std::sync::*;
use std::thread;

use tikv::util::panic_hook;

lazy_static! {
    /// Failpoints are global structs, hence rules set in different cases
    /// may affect each other. So use a global lock to synchronize them.
    static ref LOCK: Mutex<()> = {
        util::ci_setup();
        Mutex::new(())
    };
}

fn setup<'a>() -> MutexGuard<'a, ()> {
    // We don't want a failed test breaks others.
    let guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    fail::teardown();
    fail::setup();
    guard
}

#[test]
fn test_setup() {
    let _ = thread::spawn(move || {
        panic_hook::mute();
        let _g = setup();
        panic!("Poison!");
    }).join();

    let _g = setup();
}
