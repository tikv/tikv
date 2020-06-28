// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![recursion_limit = "200"]
#![feature(box_patterns)]

#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate failure;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;
#[macro_use]
extern crate tikv_util;

mod endpoint;
mod errors;
mod metrics;
mod service;
mod writer;

pub use endpoint::{Endpoint, Task};
pub use errors::{Error, Result};
pub use service::Service;
pub use writer::{BackupRawKVWriter, BackupWriter};
