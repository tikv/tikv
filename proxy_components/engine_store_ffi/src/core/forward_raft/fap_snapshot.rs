// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    core::{common::*, ProxyForwarder},
    fatal,
};

#[derive(PartialEq)]
pub enum SnapshotDeducedType {
    Uncertain,
    Regular,
    Fap,
}

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    pub fn deduce_snapshot_type(_peer_id: u64, _snap: &store::Snapshot) -> SnapshotDeducedType {
        // TODO(fap) implement in seperated modes(serverless or op).
        SnapshotDeducedType::Uncertain
    }

    pub fn pre_apply_snapshot_for_fap_snapshot(
        &self,
        ob_region: &Region,
        peer_id: u64,
        snap_key: &store::SnapKey,
    ) -> bool {
        let region_id = ob_region.get_id();
        let mut should_skip = false;
        #[allow(clippy::collapsible_if)]
        if self.packed_envs.engine_store_cfg.enable_fast_add_peer {
            if self.get_cached_manager().access_cached_region_info_mut(
                region_id,
                |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                    MapEntry::Occupied(_) => {
                        if !self.engine_store_server_helper.kvstore_region_exist(region_id) {
                            if self.engine_store_server_helper.query_fap_snapshot_state(region_id, peer_id, snap_key.idx, snap_key.term) == proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted {
                                info!("fast path: prehandle first snapshot skipped {}:{} {}", self.store_id, region_id, peer_id;
                                    "snap_key" => ?snap_key,
                                    "region_id" => region_id,
                                );
                                should_skip = true;
                            }
                        }
                    }
                    MapEntry::Vacant(_) => {
                        // It won't go here because cached region info is inited after restart and on the first fap message.
                        let pstate = self.engine_store_server_helper.query_fap_snapshot_state(region_id, peer_id, snap_key.idx, snap_key.term);
                        if pstate == proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted {
                            // We have a fap snapshot now. skip
                            info!("fast path: prehandle first snapshot skipped after restart {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                            );
                            should_skip = true;
                        } else {
                            info!("fast path: prehandle first snapshot no skipped after restart {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                                "state" => ?pstate,
                                "inited" => false,
                                "should_skip" => should_skip,
                            );
                        }
                    }
                },
            ).is_err() {
                fatal!("pre_apply_snapshot_for_fap_snapshot poisoned")
            };
        }
        should_skip
    }

    pub fn post_apply_snapshot_for_fap_snapshot(
        &self,
        ob_region: &Region,
        peer_id: u64,
        snap_key: &store::SnapKey,
        maybe_snap: Option<&store::Snapshot>,
    ) -> bool {
        let region_id = ob_region.get_id();
        let try_apply_fap_snapshot = |c: Arc<CachedRegionInfo>| {
            let already_existed = self
                .engine_store_server_helper
                .kvstore_region_exist(region_id);
            if already_existed {
                debug!("fast path: skip apply snapshot because not first {}:{} {}", self.store_id, region_id, peer_id;
                    "snap_key" => ?snap_key,
                    "region_id" => region_id,
                );
                return false;
            }
            info!("fast path: start applying first fap snapshot {}:{} {}", self.store_id, region_id, peer_id;
                "snap_key" => ?snap_key,
                "region_id" => region_id,
            );
            // Even if the feature is not enabled, the snapshot could still be a previously
            // generated fap snapshot. So we have to also handle this snapshot,
            // to prevent error data.
            let current_enabled = self.packed_envs.engine_store_cfg.enable_fast_add_peer;
            let snapshot_sent_time = c.snapshot_inflight.load(Ordering::SeqCst);
            let fap_start_time = c.fast_add_peer_start.load(Ordering::SeqCst);
            let current = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            let snap = match maybe_snap {
                Some(s) => s,
                None => {
                    return false;
                }
            };

            // We can't rely on `snapshot_inflight`, because it will be undetermined ZERO
            // after restart.
            let expected_snapshot_type = Self::deduce_snapshot_type(peer_id, snap);

            let quit_apply_fap = |tag: &str| {
                info!("fast path: fap snapshot mismatch/nonexist {}:{} {}", self.store_id, region_id, peer_id;
                    "snap_key" => ?snap_key,
                    "region_id" => region_id,
                    "cost_snapshot" => current.as_millis() - snapshot_sent_time,
                    "cost_total" => current.as_millis() - fap_start_time,
                    "current_enabled" => current_enabled,
                    "tag" => tag
                );
                if expected_snapshot_type == SnapshotDeducedType::Fap {
                    // It won't actually happen because TiFlash will panic since `assert_exist` is
                    // true in this case.
                    fatal!(
                        "fast path: fap snapshot apply failed {}:{} {}, which is assert to be fap snapshot",
                        self.store_id,
                        region_id,
                        peer_id
                    );
                }
                c.snapshot_inflight.store(0, Ordering::SeqCst);
                c.fast_add_peer_start.store(0, Ordering::SeqCst);
                c.inited_or_fallback.store(true, Ordering::SeqCst);
                false
            };

            // If there is no fap snapshot with given (index, term) we shall quit.
            if self.engine_store_server_helper.query_fap_snapshot_state(
                region_id,
                peer_id,
                snap_key.idx,
                snap_key.term,
            ) != proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted
            {
                return quit_apply_fap("pre check");
            }

            // Only succeeds if (index, term) matches.
            // Returns false if `assert_exist` is false,
            // Panics if `assert_exist` is true.
            // The logic is kind of redundant, but we want to make it complete on both
            // sides.
            if !self.engine_store_server_helper.apply_fap_snapshot(
                region_id,
                peer_id,
                true,
                snap_key.idx,
                snap_key.term,
            ) {
                return quit_apply_fap("apply");
            }
            // If it's a reguar snapshot have the same (index, term) as the fap snapshot,
            // it make no difference which snapshot we actually applied.
            // So we always choose to apply a fap snapshot, since it saves as from
            // prehandling work.
            info!("fast path: finished applied first fap snapshot {}:{} {}, recover MsgAppend", self.store_id, region_id, peer_id;
                "snap_key" => ?snap_key,
                "region_id" => region_id,
                "cost_snapshot" => current.as_millis() - snapshot_sent_time,
                "cost_total" => current.as_millis() - fap_start_time,
                "current_enabled" => current_enabled,
                "replacement_of_regular" => expected_snapshot_type == SnapshotDeducedType::Regular
            );
            c.snapshot_inflight.store(0, Ordering::SeqCst);
            c.fast_add_peer_start.store(0, Ordering::SeqCst);
            c.inited_or_fallback.store(true, Ordering::SeqCst);
            true
        };

        // We should handle fap snapshot even if enable_fast_add_peer is false.
        // However, if enable_unips, by no means can we handle fap snapshot.
        #[allow(unused_mut)]
        let mut should_check_fap_snapshot = self.packed_envs.engine_store_cfg.enable_unips;
        #[allow(clippy::redundant_closure_call)]
        (|| {
            fail::fail_point!("post_apply_snapshot_allow_no_unips", |_| {
                // UniPS can't provide a snapshot currently
                should_check_fap_snapshot = true;
            });
        })();

        let mut applied_fap = false;
        #[allow(clippy::collapsible_if)]
        if should_check_fap_snapshot {
            if self
                .get_cached_manager()
                .access_cached_region_info_mut(
                    region_id,
                    |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                        MapEntry::Occupied(o) => {
                            applied_fap = try_apply_fap_snapshot(o.get().clone());
                        }
                        MapEntry::Vacant(_) => {
                            // It won't go here because cached region info is inited after restart and on the first fap message.
                            info!("fast path: check should apply fap snapshot noexist {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                            );
                            assert!(self.is_initialized(region_id));
                            let o = Arc::new(CachedRegionInfo::default());
                            applied_fap = try_apply_fap_snapshot(o);
                        }
                    },
                )
                .is_err()
            {
                fatal!("poisoned");
            }
        }
        applied_fap
    }
}
