// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

pub use pd2::client;
pub use pd2::metrics;
pub use pd2::util;

pub use pd2::config;
pub use pd2::errors;
pub mod pd;
pub use self::client::RpcClient;
pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::pd::{Runner as PdRunner, Task as PdTask};
pub use self::util::validate_endpoints;
pub use self::util::RECONNECT_INTERVAL_SEC;

pub type Key = Vec<u8>;
pub use pd2::PdFuture;

pub use pd2::RegionStat;
pub use pd2::RegionInfo;

pub const INVALID_ID: u64 = 0;

pub use pd2::PdClient;

pub use pd2::REQUEST_TIMEOUT;
