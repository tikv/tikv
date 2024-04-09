// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]

use std::sync::Arc;

use online_config::OnlineConfig;
use pd_client::RpcClient;
use serde::{Deserialize, Serialize};

mod resource_group;
pub use resource_group::{
    ResourceConsumeType, ResourceController, ResourceGroupManager, MIN_PRIORITY_UPDATE_INTERVAL,
};
pub use tikv_util::resource_control::*;

mod future;
pub use future::{with_resource_limiter, ControlledFuture};

#[cfg(test)]
extern crate test;

mod service;
pub use service::ResourceManagerService;

pub mod channel;
pub use channel::ResourceMetered;

mod resource_limiter;
pub use resource_limiter::ResourceLimiter;
use tikv_util::worker::Worker;
use worker::{
    GroupQuotaAdjustWorker, PriorityLimiterAdjustWorker, BACKGROUND_LIMIT_ADJUST_DURATION,
};

mod metrics;
pub mod worker;

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

pub fn start_periodic_tasks(
    mgr: &Arc<ResourceGroupManager>,
    pd_client: Arc<RpcClient>,
    bg_worker: &Worker,
    io_bandwidth: u64,
) {
    let resource_mgr_service = ResourceManagerService::new(mgr.clone(), pd_client);
    // spawn a task to periodically update the minimal virtual time of all resource
    // groups.
    let resource_mgr = mgr.clone();
    bg_worker.spawn_interval_task(MIN_PRIORITY_UPDATE_INTERVAL, move || {
        resource_mgr.advance_min_virtual_time();
    });
    let mut resource_mgr_service_clone = resource_mgr_service.clone();
    // spawn a task to watch all resource groups update.
    bg_worker.spawn_async_task(async move {
        resource_mgr_service_clone.watch_resource_groups().await;
    });
    // spawn a task to auto adjust background quota limiter and priority quota
    // limiter.
    let mut worker = GroupQuotaAdjustWorker::new(mgr.clone(), io_bandwidth);
    let mut priority_worker = PriorityLimiterAdjustWorker::new(mgr.clone());
    bg_worker.spawn_interval_task(BACKGROUND_LIMIT_ADJUST_DURATION, move || {
        worker.adjust_quota();
        priority_worker.adjust();
    });
    // spawn a task to periodically upload resource usage statistics to PD.
    bg_worker.spawn_async_task(async move {
        resource_mgr_service.report_ru_metrics().await;
    });
}
