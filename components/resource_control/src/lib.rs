// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]

use online_config::OnlineConfig;
use serde::{Deserialize, Serialize};

mod resource_group;
pub use resource_group::{
    ResourceConsumeType, ResourceController, ResourceGroupManager, TaskMetadata,
    MIN_PRIORITY_UPDATE_INTERVAL,
};

mod future;
pub use future::ControlledFuture;

#[cfg(test)]
extern crate test;

mod service;
pub use service::ResourceManagerService;

pub mod channel;
pub use channel::ResourceMetered;

mod resource_limiter;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self { enabled: true }
    }
}
