// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg_attr(test, feature(test))]
#[cfg(test)]
extern crate test;

mod kv_generator;
mod logging;
mod macros;
mod security;

use std::env;

pub use crate::kv_generator::*;
pub use crate::logging::*;
pub use crate::macros::*;
pub use crate::security::*;

pub fn setup_for_ci() {
    if env::var("CI").is_ok() {
        if env::var("LOG_FILE").is_ok() {
            logging::init_log_for_test();
        }

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
    }

    if env::var("PANIC_ABORT").is_ok() {
        // Panics as aborts, it's helpful for debugging,
        // but also stops tests immediately.
        tikv_util::set_panic_hook(true, "./");
    }

    tikv_util::check_environment_variables();
}
