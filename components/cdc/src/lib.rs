// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(box_patterns)]
#![feature(assert_matches)]
#![feature(shrink_to)]

mod channel;
mod config;
mod delegate;
mod endpoint;
mod errors;
pub mod metrics;
mod observer;
mod old_value;
mod service;

pub use channel::{recv_timeout, CdcEvent, MemoryQuota};
pub use config::CdcConfigManager;
pub use endpoint::{CdcTxnExtraScheduler, Endpoint, Task, Validate};
pub use errors::{Error, Result};
pub use observer::CdcObserver;
pub use service::{FeatureGate, Service};
