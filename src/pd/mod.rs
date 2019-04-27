// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

pub use pd2::client;
pub use pd2::metrics;
pub use pd2::util;

pub use pd2::config;
pub use pd2::errors;
pub mod pd;
pub use pd2::client::RpcClient;
pub use pd2::config::Config;
pub use pd2::errors::{Error, Result};
pub use tikv_misc::pd_task::Task as PdTask;
pub use self::pd::Runner as PdRunner;
pub use pd2::util::validate_endpoints;
pub use pd2::util::RECONNECT_INTERVAL_SEC;

pub type Key = Vec<u8>;
pub use pd2::PdFuture;

pub use pd2::RegionStat;
pub use pd2::RegionInfo;

pub use tikv_misc::PD_INVALID_ID as INVALID_ID;

pub use pd2::PdClient;

pub use pd2::REQUEST_TIMEOUT;
