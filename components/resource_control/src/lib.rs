// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]
#![feature(let_chains)]

use std::sync::Arc;

use pd_client::RpcClient;

mod resource_group;
pub use resource_group::{
    MIN_PRIORITY_UPDATE_INTERVAL, ResourceConsumeType, ResourceController, ResourceGroupManager,
};
pub use tikv_util::resource_control::*;

mod future;
pub use future::{ControlledFuture, with_resource_limiter};

#[cfg(test)]
extern crate test;

mod service;
pub use service::ResourceManagerService;

pub mod channel;
pub use channel::ResourceMetered;

pub mod config;

mod resource_limiter;
pub use resource_limiter::ResourceLimiter;
use tikv_util::worker::Worker;
use worker::{BACKGROUND_LIMIT_ADJUST_DURATION, GroupQuotaAdjustWorker};

mod metrics;
pub mod worker;

mod collector;
mod controller;
mod event;
mod hub;
mod limiter;
mod publisher;
mod resource;
mod subscriber;
mod usage;
mod util;

pub use collector::{ResourceCollector, ThreadCollector};
pub use config::Config;
pub use controller::ResourceLimitController;
pub use event::{Metric, ResourceEvent, Scope, Severity, SeverityThreshold};
use hub::{EventHub, ResourceHub};
pub use limiter::{ReadLimiter, ResourceGroupReadLimiter};
pub use metrics::{ACTIVE_RESOURCE_GROUP_READ_BYTES, REQUEST_WAIT_HISTOGRAM_VEC};
use publisher::EventPublisher;
pub use publisher::ResourcePublisher;
pub use resource::{CpuType, Resource, ResourceType, ResourceValue, Usage};
pub use subscriber::{ReadSubscriber, ResourceSubscriber};
use usage::{GlobalInstantUsages, ResourceUsage, Usages};
pub use util::{AtomicDuration, AtomicTime, TimeUnit};

const MAX_BATCH_SIZE: usize = 1024 * 1024;

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
    // We disable the priority worker by default because the current adjust
    // algorithm is buggy. We may reenable it only we find a better algorithm.
    // let mut priority_worker = PriorityLimiterAdjustWorker::new(mgr.clone());
    bg_worker.spawn_interval_task(BACKGROUND_LIMIT_ADJUST_DURATION, move || {
        worker.adjust_quota();
        // priority_worker.adjust();
    });
    // spawn a task to periodically upload resource usage statistics to PD.
    bg_worker.spawn_async_task(async move {
        resource_mgr_service.report_ru_metrics().await;
    });
}
