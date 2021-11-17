// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(matches_macro)]

#[macro_use]
extern crate failure;
#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate tikv_util;
#[cfg(test)]
#[macro_use]
extern crate matches;

mod channel;
mod config;
mod delegate;
mod endpoint;
mod errors;
mod metrics;
mod observer;
mod service;

pub use channel::{recv_timeout, MemoryQuota};
pub use config::CdcConfigManager;
pub use endpoint::{Endpoint, OldValueStats, Task, Validate};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::{CdcEvent, FeatureGate, Service};
