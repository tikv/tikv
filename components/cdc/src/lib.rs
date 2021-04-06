// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(async_closure)]
#![recursion_limit = "512"]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate tikv_util;
extern crate futures;
extern crate tokio;

mod delegate;
mod endpoint;
mod errors;
pub mod metrics;
mod observer;
mod rate_limiter;
mod service;

pub use endpoint::{CdcTxnExtraScheduler, Endpoint, Task};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::Service;
