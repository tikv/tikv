// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]

#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate failure;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate tikv_util;
#[macro_use]
extern crate serde_derive;

pub mod config;
mod delegate;
mod endpoint;
mod errors;
mod metrics;
mod observer;
mod service;

pub use config::Config;
pub use endpoint::{Endpoint, Task};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::Service;
