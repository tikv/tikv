// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use super::CoprocessorHost;
pub use raftstore2::coprocessor::region_info_accessor::RegionInfoAccessor;
use raftstore2::coprocessor::region_info_accessor::RegionEventListener;
use tikv_util::worker::{Builder as WorkerBuilder, Scheduler};
use tikv_misc::kv_region_info::RegionInfoQuery;

/// Creates a new `RegionInfoAccessor` and register to `host`.
/// `RegionInfoAccessor` doesn't need, and should not be created more than once. If it's needed
/// in different places, just clone it, and their contents are shared.
pub fn new(host: &mut CoprocessorHost) -> RegionInfoAccessor {
    let worker = WorkerBuilder::new("region-collector-worker").create();
    let scheduler = worker.scheduler();

    register_region_event_listener(host, scheduler.clone());

    RegionInfoAccessor::from_linked_scheduler(worker, scheduler)
}

/// Creates an `RegionEventListener` and register it to given coprocessor host.
fn register_region_event_listener(
    host: &mut CoprocessorHost,
    scheduler: Scheduler<RegionInfoQuery>,
) {
    let listener = RegionEventListener::new(scheduler);

    host.registry
        .register_role_observer(1, Box::new(listener.clone()));
    host.registry
        .register_region_change_observer(1, Box::new(listener));
}
