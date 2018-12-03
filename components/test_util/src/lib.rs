// Copyright 2018 PingCAP, Inc.
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

#![cfg_attr(test, feature(test))]
#[cfg(test)]
extern crate test;

extern crate rand;
#[macro_use]
extern crate slog;
extern crate slog_scope;
extern crate time;

extern crate tikv;

mod kv_generator;
mod logging;
mod macros;
mod security;

use std::env;

pub use kv_generator::*;
pub use logging::*;
pub use macros::*;
pub use security::*;

pub fn setup_for_ci() {
    let guard = if env::var("CI").is_ok() && env::var("LOG_FILE").is_ok() {
        Some(logging::init_log())
    } else {
        None
    };
    if env::var("PANIC_ABORT").is_ok() {
        // Panics as aborts, it's helpful for debugging,
        // but also stops tests immediately.
        tikv::util::set_exit_hook(true, guard, "./");
    } else if let Some(guard) = guard {
        // Do not reset the global logger.
        guard.cancel_reset();
    }
}
