// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::core::{common::*, ProxyForwarder};

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    pub fn on_update_safe_ts(&self, region_id: u64, self_safe_ts: u64, leader_safe_ts: u64) {
        self.engine_store_server_helper.handle_safe_ts_update(
            region_id,
            self_safe_ts,
            leader_safe_ts,
        )
    }

    pub fn on_region_changed(&self, ob_region: &Region, e: RegionChangeEvent, _: StateRole) {
        let region_id = ob_region.get_id();
        if e == RegionChangeEvent::Destroy {
            info!(
                "observe destroy";
                "region_id" => region_id,
                "store_id" => self.store_id,
            );
            self.engine_store_server_helper.handle_destroy(region_id);
            if self.packed_envs.engine_store_cfg.enable_fast_add_peer {
                self.get_cached_manager()
                    .remove_cached_region_info(region_id);
            }
        }
    }

    #[allow(clippy::match_like_matches_macro)]
    pub fn pre_persist(
        &self,
        ob_region: &Region,
        is_finished: bool,
        cmd: Option<&RaftCmdRequest>,
    ) -> bool {
        let region_id = ob_region.get_id();
        let should_persist = if is_finished {
            true
        } else {
            let cmd = cmd.unwrap();
            if cmd.has_admin_request() {
                match cmd.get_admin_request().get_cmd_type() {
                    // Merge needs to get the latest apply index.
                    AdminCmdType::CommitMerge | AdminCmdType::RollbackMerge => true,
                    _ => false,
                }
            } else {
                false
            }
        };
        if should_persist {
            debug!(
            "observe pre_persist, persist";
            "region_id" => region_id,
            "store_id" => self.store_id,
            );
        } else {
            debug!(
            "observe pre_persist";
            "region_id" => region_id,
            "store_id" => self.store_id,
            "is_finished" => is_finished,
            );
        };
        should_persist
    }

    pub fn pre_write_apply_state(&self, _ob_region: &Region) -> bool {
        fail::fail_point!("on_pre_write_apply_state", |_| {
            // Some test need persist apply state for Leader logic,
            // including fast add peer.
            true
        });
        false
    }

    pub fn on_role_change(&self, ob_region: &Region, r: &RoleChange) {
        let region_id = ob_region.get_id();
        let is_replicated = !r.initialized;
        let f = |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
            MapEntry::Occupied(mut o) => {
                // Note the region info may be registered by maybe_fast_path
                info!("fast path: ongoing {}:{} {}, peer created",
                    self.store_id, region_id, 0;
                    "region_id" => region_id,
                    "is_replicated" => is_replicated,
                );
                if is_replicated {
                    o.get_mut()
                        .replicated_or_created
                        .store(true, Ordering::SeqCst);
                }
            }
            MapEntry::Vacant(v) => {
                // TODO support peer_id
                info!("fast path: ongoing {}:{} {}, peer created",
                    self.store_id, region_id, r.peer_id;
                    "region_id" => region_id,
                    "is_replicated" => is_replicated,
                );
                if is_replicated {
                    let c = CachedRegionInfo::default();
                    c.replicated_or_created.store(true, Ordering::SeqCst);
                    v.insert(Arc::new(c));
                }
            }
        };
        // TODO remove unwrap
        self.get_cached_manager()
            .access_cached_region_info_mut(region_id, f)
            .unwrap();
    }
}
