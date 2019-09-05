// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]
// TODO: remove it after all code been merged.
#![allow(unused_imports)]

#[macro_use(
    kv,
    slog_kv,
    slog_trace,
    slog_debug,
    slog_info,
    slog_warn,
    slog_error,
    slog_record,
    slog_b,
    slog_log,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate serde_derive;
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
pub use writer::BackupWriter;
