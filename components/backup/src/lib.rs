// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]

#[macro_use(
    kv,
    slog_kv,
    slog_error,
    slog_warn,
    slog_info,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate failure;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod endpoint;
mod errors;
mod metrics;
mod service;
mod writer;

pub use endpoint::{Endpoint, Task};
pub use errors::{Error, Result};
pub use service::Service;
pub use writer::{BackupRawKVWriter, BackupWriter};
