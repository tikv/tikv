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

use std::{
    env,
    sync::atomic::{AtomicU16, Ordering},
    thread,
};

use rand::Rng;
use tikv_util::sys::thread::StdThreadBuildWrapper;

pub use crate::{
    encryption::*,
    kv_generator::*,
    logging::*,
    macros::*,
    runner::{clear_failpoints, run_failpoint_tests, run_test_with_hook, run_tests, TestHook},
    security::*,
};

pub fn setup_for_ci() {
    // We use backtrace in tests to record suspicious problems. And loading backtrace
    // the first time can take several seconds. Spawning a thread and load it ahead
    // of time to avoid causing timeout.
    thread::Builder::new()
        .name(tikv_util::thd_name!("backtrace-loader"))
        .spawn_wrapper(::backtrace::Backtrace::new)
        .unwrap();

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

static MEM_DISK: &str = "TIKV_TEST_MEMORY_DISK_MOUNT_POINT";

/// Gets a temporary path. The directory will be removed when dropped.
///
/// The returned path will point to memory only when memory disk is available
/// and specified.
pub fn temp_dir(prefix: impl Into<Option<&'static str>>, prefer_mem: bool) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    if let Some(prefix) = prefix.into() {
        builder.prefix(prefix);
    }
    match env::var(MEM_DISK) {
        Ok(dir) if prefer_mem => {
            debug!("using memory disk"; "path" => %dir);
            builder.tempdir_in(dir).unwrap()
        }
        _ => builder.tempdir().unwrap(),
    }
}
