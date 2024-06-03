// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(test)]

use std::sync::Arc;

use dashmap::DashMap;
use kvproto::keyspacepb;
use pd_client::RpcClient;
use tikv_util::worker::Worker;

pub mod service;
pub use service::{
    KeyspaceLevelGCService, KeyspaceLevelGCWatchService, GC_MGMT_TYPE_KEYSPACE_LEVEL_GC,
    KEYSPACE_CONFIG_KEY_GC_MGMT_TYPE,
};

use crate::service::KeyspaceMetaWatchService;

pub fn start_periodic_keyspace_level_gc_watcher(
    pd_client: Arc<RpcClient>,
    bg_worker: &Worker,
    keyspace_level_gc_map: Arc<DashMap<u32, u64>>,
) {
    let mut keyspace_level_gc_watch_service =
        KeyspaceLevelGCWatchService::new(pd_client, keyspace_level_gc_map);
    // spawn a task to watch all keyspace level gc update.
    bg_worker.spawn_async_task(async move {
        keyspace_level_gc_watch_service
            .watch_keyspace_level_gc()
            .await;
    });
}

pub fn start_periodic_keyspace_meta_watcher(
    pd_client: Arc<RpcClient>,
    bg_worker: &Worker,
    keyspace_id_meta_map: Arc<DashMap<u32, keyspacepb::KeyspaceMeta>>,
) {
    let mut keyspace_meta_watch_service =
        KeyspaceMetaWatchService::new(pd_client, keyspace_id_meta_map);
    // spawn a task to watch all keyspace meta update.
    bg_worker.spawn_async_task(async move {
        keyspace_meta_watch_service.watch_keyspace_meta().await;
    });
}
