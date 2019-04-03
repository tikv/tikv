// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "100"]
#![cfg_attr(feature = "no-fail", allow(dead_code))]

#[macro_use]
extern crate lazy_static;
#[macro_use(slog_kv, slog_debug, slog_log, slog_record, slog_b, slog_record_static)]
extern crate slog;
#[macro_use]
extern crate slog_global;

#[cfg(not(feature = "no-fail"))]
mod cases;

use std::sync::*;
use std::thread;

lazy_static! {
    /// Failpoints are global structs, hence rules set in different cases
    /// may affect each other. So use a global lock to synchronize them.
    static ref LOCK: Mutex<()> = {
        test_util::setup_for_ci();
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
    })
    .join();

    let _g = setup();
}
