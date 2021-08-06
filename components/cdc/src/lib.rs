// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]

#[macro_use]
extern crate failure;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate tikv_util;
#[cfg(test)]
#[macro_use]
extern crate assert_matches;

mod channel;
mod delegate;
mod endpoint;
mod errors;
pub mod metrics;
mod observer;
mod old_value;
mod service;

pub use channel::{recv_timeout, MemoryQuota};
pub use endpoint::{CdcTxnExtraScheduler, Endpoint, Task, Validate};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::{CdcEvent, FeatureGate, Service};
