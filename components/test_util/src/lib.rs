// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

extern crate test;
#[macro_use]
extern crate slog_global;

pub mod encryption;
mod kv_generator;
mod logging;
mod macros;
mod runner;
mod security;

use rand::Rng;
use std::env;
use std::sync::atomic::{AtomicU16, Ordering};

pub use crate::encryption::*;
pub use crate::kv_generator::*;
pub use crate::logging::*;
pub use crate::macros::*;
pub use crate::runner::{
    clear_failpoints, run_failpoint_tests, run_test_with_hook, run_tests, TestHook,
};
pub use crate::security::*;

pub fn setup_for_ci() {
    if env::var("CI").is_ok() {
        // HACK! Use `epollex` as the polling engine for gRPC when running CI tests on
        // Linux and it hasn't been set before.
        // See more: https://github.com/grpc/grpc/blob/v1.17.2/src/core/lib/iomgr/ev_posix.cc#L124
        // See more: https://grpc.io/grpc/core/md_doc_core_grpc-polling-engines.html
        #[cfg(target_os = "linux")]
        {
            if env::var("GRPC_POLL_STRATEGY").is_err() {
                env::set_var("GRPC_POLL_STRATEGY", "epollex");
            }
        }

        if env::var("LOG_FILE").is_ok() {
            logging::init_log_for_test();
        }
    } else {
        logging::init_log_for_test();
    }

    if env::var("PANIC_ABORT").is_ok() {
        // Panics as aborts, it's helpful for debugging,
        // but also stops tests immediately.
        tikv_util::set_panic_hook(true, "./");
    }

    tikv_util::check_environment_variables();

    if let Err(e) = tikv_util::config::check_max_open_fds(4096) {
        panic!(
            "To run test, please make sure the maximum number of open file descriptors not \
             less than 4096: {:?}",
            e
        );
    }
}

static INITIAL_PORT: AtomicU16 = AtomicU16::new(0);
/// Linux by default use [32768, 61000] for local port.
const MIN_LOCAL_PORT: u16 = 32767;

/// Allocates a port for testing purpose.
pub fn alloc_port() -> u16 {
    let p = INITIAL_PORT.load(Ordering::Relaxed);
    if p == 0 {
        let _ = INITIAL_PORT.compare_exchange(
            0,
            rand::thread_rng().gen_range(10240..MIN_LOCAL_PORT),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
    let mut p = INITIAL_PORT.load(Ordering::SeqCst);
    loop {
        let next = if p >= MIN_LOCAL_PORT { 10240 } else { p + 1 };
        match INITIAL_PORT.compare_exchange_weak(p, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return next,
            Err(e) => p = e,
        }
    }
}
