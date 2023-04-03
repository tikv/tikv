// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fmt::Formatter,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc,
    },
};

use crate::proxy_utils::EngineStoreHub;

// This struct should be safe to copy.
#[derive(Clone)]
pub struct ProxyEngineExt {
    pub engine_store_server_helper: isize,
    pub pool_capacity: usize,
    pub pending_applies_count: Arc<AtomicIsize>,
    pub engine_store_hub: Option<Arc<dyn EngineStoreHub + Send + Sync>>,
    pub config_set: Option<Arc<crate::ProxyEngineConfigSet>>,
    pub cached_region_info_manager: Option<Arc<crate::CachedRegionInfoManager>>,
}

impl std::fmt::Debug for ProxyEngineExt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiFlashEngine")
            .field(
                "engine_store_server_helper",
                &self.engine_store_server_helper,
            )
            .field("pool_capacity", &self.pool_capacity)
            .field(
                "pending_applies_count",
                &self.pending_applies_count.load(Ordering::SeqCst),
            )
            .finish()
    }
}

impl Default for ProxyEngineExt {
    fn default() -> Self {
        ProxyEngineExt {
            engine_store_server_helper: 0,
            pool_capacity: 0,
            pending_applies_count: Arc::new(AtomicIsize::new(0)),
            engine_store_hub: None,
            config_set: None,
            cached_region_info_manager: None,
        }
    }
}

impl ProxyEngineExt {
    // The whole point is:
    // 1. When `handle_pending_applies` is called by `on_timeout`, we can handle at
    // least one.
    // 2. When `handle_pending_applies` is called when we receive a
    // new task, or when `handle_pending_applies` need to handle multiple snapshots.
    // We need to compare to what's in queue.
    pub fn can_apply_snapshot(&self, is_timeout: bool, new_batch: bool, region_id: u64) -> bool {
        fail::fail_point!("on_can_apply_snapshot", |e| e
            .unwrap()
            .parse::<bool>()
            .unwrap());
        if let Some(s) = self.config_set.as_ref() {
            if s.engine_store.enable_fast_add_peer {
                // TODO Return true if this is an empty snapshot.
                // We need to test if the region is still in fast add peer mode.
                let result = self
                    .cached_region_info_manager
                    .as_ref()
                    .expect("expect cached_region_info_manager")
                    .get_inited_or_fallback(region_id);
                match result {
                    Some(true) => {
                        // Do nothing.
                        tikv_util::debug!("can_apply_snapshot no fast path. do normal checking";
                            "region_id" => region_id,
                        );
                    }
                    None | Some(false) => {
                        // Otherwise, try fast path.
                        return true;
                    }
                };
            }
        }
        // is called after calling observer's pre_handle_snapshot
        let in_queue = self.pending_applies_count.load(Ordering::SeqCst);
        if is_timeout && new_batch {
            // If queue is full, we should begin to handle
            true
        } else {
            // Otherwise, we wait until the queue is full.
            // In order to batch more tasks.
            in_queue > (self.pool_capacity as isize)
        }
    }
}
