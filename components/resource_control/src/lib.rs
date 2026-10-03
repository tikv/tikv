// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]

use std::{
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use pd_client::RpcClient;
use tikv_util::time::Instant;

mod resource_group;
pub use resource_group::{
    AdmissionDecision, CONTROL_TICK, CONTROL_TICK_OVERLOADED, DelaySlotGuard, LEEWAY_FACTOR,
    LEEWAY_FRACTION, MIN_PRIORITY_UPDATE_INTERVAL, NOISY_TENANT_REASON_SUFFIX, ResourceConsumeType,
    ResourceController, ResourceGroupManager, busy_reason,
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
use worker::GroupQuotaAdjustWorker;

mod metrics;
pub use metrics::READ_POOL_CPU_VEC;
mod score;
pub mod worker;

pub use score::{
    ResourceCapacities, ResourceScoreInputs, ResourceScores, ThreadGroupCpuTracker,
    compute_resource_scores,
};

pub fn start_periodic_tasks(
    mgr: &Arc<ResourceGroupManager>,
    pd_client: Arc<RpcClient>,
    bg_worker: &Worker,
    io_bandwidth: u64,
    compaction_pending_bytes_ratio: Arc<AtomicU32>,
    grpc_concurrency: usize,
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
    let mut worker = GroupQuotaAdjustWorker::new(
        mgr.clone(),
        io_bandwidth,
        compaction_pending_bytes_ratio,
        grpc_concurrency,
    );
    // We disable the priority worker by default because the current adjust
    // algorithm is buggy. We may reenable it only we find a better algorithm.
    // let mut priority_worker = PriorityLimiterAdjustWorker::new(mgr.clone());
    // Woken at the shorter of the two periods and gated down to the longer one
    // while the node is quiet, because spawn_interval_task's period is fixed.
    // TICK_SLACK absorbs timer jitter, which would otherwise defer a tick that
    // arrives a moment early by a whole wakeup.
    const TICK_SLACK: Duration = Duration::from_millis(500);
    let tick_mgr = mgr.clone();
    let mut last_tick = Instant::now_coarse();
    bg_worker.spawn_interval_task(mgr.overloaded_tick(), move || {
        let now = Instant::now_coarse();
        if now.saturating_duration_since(last_tick) + TICK_SLACK < tick_mgr.control_tick() {
            return;
        }
        last_tick = now;
        worker.adjust_quota();
        // priority_worker.adjust();
    });
    // spawn a task to periodically upload resource usage statistics to PD.
    bg_worker.spawn_async_task(async move {
        resource_mgr_service.report_ru_metrics().await;
    });
}
