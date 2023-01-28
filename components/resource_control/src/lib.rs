// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use online_config::OnlineConfig;
use serde::{Deserialize, Serialize};

mod resource_group;
pub use resource_group::{
    ResourceConsumeType, ResourceController, ResourceGroupManager, MIN_PRIORITY_UPDATE_INTERVAL,
};

mod future;
pub use future::ControlledFuture;

mod service;
pub use service::ResourceManagerService;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[online_config(skip)]
    pub enabled: bool,
}
