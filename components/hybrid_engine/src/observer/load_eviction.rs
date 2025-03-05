// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use codec::prelude::NumberEncoder;
use engine_traits::{CacheRegion, EvictReason, KvEngine, RegionCacheEngineExt, RegionEvent};
use kvproto::{
    metapb::{Peer, Region},
    raft_cmdpb::AdminCmdType,
    raft_serverpb::{ExtraMessageType, RaftApplyState},
};
use protobuf::ProtobufEnum as _;
use raft::StateRole;
use raftstore::{
    coprocessor::{
        dispatcher::{BoxDestroyPeerObserver, BoxTransferLeaderObserver},
        AdminObserver, ApplyCtxInfo, ApplySnapshotObserver, BoxAdminObserver,
        BoxApplySnapshotObserver, BoxQueryObserver, BoxRoleObserver, Cmd, Coprocessor,
        CoprocessorHost, DestroyPeerObserver, ObserverContext, QueryObserver, RegionState,
        RoleObserver, TransferLeaderCustomContext, TransferLeaderObserver,
    },
    store::TransferLeaderContext,
};
use tikv_util::{codec::number::decode_var_i64, debug, warn};

use crate::metrics::IN_MEMORY_ENGINE_TRANSFER_LEADER_WARMUP_COUNTER_STATIC;

#[derive(Clone)]
pub struct LoadEvictionObserver {
    cache_engine: Arc<dyn RegionCacheEngineExt + Send + Sync>,
}

impl LoadEvictionObserver {
    pub fn new(cache_engine: Arc<dyn RegionCacheEngineExt + Send + Sync>) -> Self {
        LoadEvictionObserver { cache_engine }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        // This observer does not need high priority, use the default 100.
        let priority = 100;
        // Evict cache when a peer applies ingest sst.
        coprocessor_host
            .registry
            .register_query_observer(priority, BoxQueryObserver::new(self.clone()));
        // Evict cache when a peer applies region merge.
        coprocessor_host
            .registry
            .register_admin_observer(priority, BoxAdminObserver::new(self.clone()));
        // Evict cache when a peer applies snapshot.
        // Applying snapshot changes the data in rocksdb but not IME,
        // so we trigger region eviction to keep compatibility.
        coprocessor_host.registry.register_apply_snapshot_observer(
            priority,
            BoxApplySnapshotObserver::new(self.clone()),
        );
        // Evict cache when a leader steps down.
        coprocessor_host
            .registry
            .register_role_observer(priority, BoxRoleObserver::new(self.clone()));
        // Eviction the cached region when the peer is destroyed.
        coprocessor_host
            .registry
            .register_destroy_peer_observer(priority, BoxDestroyPeerObserver::new(self.clone()));
        coprocessor_host.registry.register_transfer_leader_observer(
            priority,
            BoxTransferLeaderObserver::new(self.clone()),
        );
    }

    fn post_exec_cmd(
        &self,
        ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        state: &RegionState,
        apply: &mut ApplyCtxInfo<'_>,
    ) {
        // Evict caches for successfully executed ingest commands and admin
        // commands that change region range.
        //
        // NB: We do not evict the cache for region splits, as the split ranges
        // still contain the latest data and hot regions are often split.
        // Evicting the cache for region splits is not worthwhile and may cause
        // performance regression due to frequent loading and evicting of
        // hot regions.
        if apply.pending_handle_ssts.is_some() {
            let cache_region = CacheRegion::from_region(ctx.region());
            debug!(
                "ime evict region due to ingest sst";
                "region" => ?cache_region,
            );
            self.evict_region(cache_region, EvictReason::IngestSST)
        }
        let cmd_type = cmd.request.get_admin_request().get_cmd_type();
        if let Some(modified_region) = &state.modified_region {
            if matches!(cmd_type, AdminCmdType::PrepareMerge) {
                let cache_region = CacheRegion::from_region(ctx.region());
                debug!(
                    "ime evict region due to apply commands";
                    "region" => ?cache_region,
                    "admin_command" => ?cmd_type
                );
                self.evict_region(cache_region, EvictReason::PrepareMerge)
            } else if matches!(
                cmd_type,
                AdminCmdType::CommitMerge | AdminCmdType::RollbackMerge
            ) {
                let cache_region = CacheRegion::from_region(ctx.region());
                debug!(
                    "ime evict region followed by reloading due to apply commands";
                    "region" => ?cache_region,
                    "admin_command" => ?cmd_type
                );
                self.merge_region(cache_region.clone(), modified_region.clone())
            }
        }
        // there are new_regions, this must be a split event.
        if !state.new_regions.is_empty() {
            assert!(cmd_type == AdminCmdType::BatchSplit || cmd_type == AdminCmdType::Split);
            debug!(
                "ime handle region split";
                "region_id" => ctx.region().get_id(),
                "admin_command" => ?cmd_type,
                "region" => ?state.modified_region.as_ref().unwrap(),
                "new_regions" => ?state.new_regions,
            );

            self.split_region(
                CacheRegion::from_region(ctx.region()),
                state
                    .new_regions
                    .iter()
                    .map(CacheRegion::from_region)
                    .collect(),
            );
        }
    }

    fn split_region(&self, source: CacheRegion, new_regions: Vec<CacheRegion>) {
        self.cache_engine.on_region_event(RegionEvent::Split {
            source,
            new_regions,
        });
    }

    // Merged regions are evicted first and will be reloaded immediately to
    // minimize the impact of the merge operations on the cache hit ratio.
    fn merge_region(&self, region: CacheRegion, modified_region: Region) {
        let cache_engine = self.cache_engine.clone();
        let modified_region = CacheRegion::from_region(&modified_region);
        self.cache_engine.on_region_event(RegionEvent::Eviction {
            region: region.clone(),
            reason: EvictReason::Merge,
            on_evict_finished: Some(Box::new(move || {
                Box::pin(async move {
                    cache_engine.on_region_event(RegionEvent::TryLoad {
                        region: modified_region,
                        for_manual_range: false,
                    });
                })
            })),
        });
    }

    // Try to load region. It will be loaded if it's overlapped with maunal range
    fn try_load_region(&self, region: CacheRegion) {
        self.cache_engine.on_region_event(RegionEvent::TryLoad {
            region,
            for_manual_range: true,
        });
    }

    fn evict_region(&self, region: CacheRegion, reason: EvictReason) {
        self.cache_engine.on_region_event(RegionEvent::Eviction {
            region,
            reason,
            on_evict_finished: None,
        });
    }
}

impl Coprocessor for LoadEvictionObserver {}

impl QueryObserver for LoadEvictionObserver {
    fn pre_exec_query(
        &self,
        _: &mut ObserverContext<'_>,
        reqs: &[kvproto::raft_cmdpb::Request],
        _: u64,
        _: u64,
    ) -> bool {
        reqs.iter().for_each(|r| {
            if r.has_delete_range() {
                self.cache_engine
                    .on_region_event(RegionEvent::EvictByRange {
                        range: CacheRegion::new(
                            0,
                            0,
                            keys::data_key(r.get_delete_range().get_start_key()),
                            keys::data_key(r.get_delete_range().get_end_key()),
                        ),
                        reason: EvictReason::DeleteRange,
                    })
            }
        });

        false
    }

    fn post_exec_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        _: &RaftApplyState,
        state: &RegionState,
        apply: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        self.post_exec_cmd(ctx, cmd, state, apply);
        // This observer does not require persisting the cmd to engine
        // immediately, so return false.
        false
    }
}

impl AdminObserver for LoadEvictionObserver {
    fn pre_exec_admin(
        &self,
        ctx: &mut ObserverContext<'_>,
        req: &kvproto::raft_cmdpb::AdminRequest,
        _: u64,
        _: u64,
    ) -> bool {
        if req.cmd_type == AdminCmdType::PrepareFlashback {
            let cache_region = CacheRegion::from_region(ctx.region());
            self.evict_region(cache_region, EvictReason::Flashback);
        }

        false
    }

    fn post_exec_admin(
        &self,
        ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        _: &RaftApplyState,
        state: &RegionState,
        apply: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        self.post_exec_cmd(ctx, cmd, state, apply);
        // This observer does not require persisting the cmd to engine
        // immediately, so return false.
        false
    }
}

const TRANSFER_LEADER_CONTEXT_KEY: &[u8] = b"ime";

impl TransferLeaderObserver for LoadEvictionObserver {
    fn pre_transfer_leader(
        &self,
        ctx: &mut ObserverContext<'_>,
        _tr: &kvproto::raft_cmdpb::TransferLeaderRequest,
    ) -> raftstore::coprocessor::Result<Option<TransferLeaderCustomContext>> {
        // Warm up transferee's cache when a region is in active or in loading.
        let active_only = false;
        if !self.cache_engine.region_cached(ctx.region(), active_only) {
            return Ok(None);
        }
        IN_MEMORY_ENGINE_TRANSFER_LEADER_WARMUP_COUNTER_STATIC
            .request
            .inc();
        let mut value = vec![];
        value
            .write_var_i64(ExtraMessageType::MsgPreLoadRegionRequest.value() as i64)
            .unwrap();
        Ok(Some(TransferLeaderCustomContext {
            key: TRANSFER_LEADER_CONTEXT_KEY.to_vec(),
            value,
        }))
    }

    fn pre_ack_transfer_leader(
        &self,
        r: &mut ObserverContext<'_>,
        msg: &raft::eraftpb::Message,
    ) -> bool {
        fn get_value(ctx: &[u8]) -> raftstore::Result<Option<ExtraMessageType>> {
            let ctx = TransferLeaderContext::from_bytes(ctx)?;
            let Some(mut value) = ctx.get_custom_ctx(TRANSFER_LEADER_CONTEXT_KEY) else {
                return Ok(None);
            };
            let value = decode_var_i64(&mut value)?;
            Ok(ExtraMessageType::from_i32(value as i32))
        }

        let region = r.region();
        let context = msg.get_context();
        let ty = match get_value(context) {
            Ok(Some(ty)) => ty,
            other => {
                // For compatibility, return ready if the context is not found
                // or invalid.
                if other.is_err() {
                    warn!("ime transfer leader warmup ignored";
                        "region_id" => ?region.get_id(),
                        "from" => ?msg.get_from(),
                        "error" => ?other.err());
                }
                return true;
            }
        };

        let need_warmup = ty == ExtraMessageType::MsgPreLoadRegionRequest;
        if !need_warmup {
            // Ready to ack.
            return true;
        }

        if region.get_peers().is_empty() {
            // MsgPreLoadRegionRequest is sent before leader issue a transfer leader
            // request. It is possible that the peer is not initialized yet.
            warn!("ime skip warmup an uninitialized region"; "region" => ?region);
            IN_MEMORY_ENGINE_TRANSFER_LEADER_WARMUP_COUNTER_STATIC
                .skip_warmup
                .inc();
            return true;
        }

        // Exclude loading states to make sure the region is active.
        let active_only = true;
        let has_cached = self.cache_engine.region_cached(r.region(), active_only);
        if has_cached {
            // Ready to ack.
            return true;
        }

        IN_MEMORY_ENGINE_TRANSFER_LEADER_WARMUP_COUNTER_STATIC
            .warmup
            .inc();
        self.cache_engine.load_region(r.region());
        false
    }
}

impl ApplySnapshotObserver for LoadEvictionObserver {
    fn post_apply_snapshot(
        &self,
        ctx: &mut ObserverContext<'_>,
        _: u64,
        _: &raftstore::store::SnapKey,
        _: Option<&raftstore::store::Snapshot>,
    ) {
        // While currently, we evict cached region after leader step down.
        // A region can may still be loaded when it's leader. E.g, to warmup
        // some hot regions before transferring leader.
        let cache_region = CacheRegion::from_region(ctx.region());
        self.evict_region(cache_region, EvictReason::ApplySnapshot)
    }
}

impl RoleObserver for LoadEvictionObserver {
    fn on_role_change(
        &self,
        ctx: &mut ObserverContext<'_>,
        change: &raftstore::coprocessor::RoleChange,
    ) {
        if let StateRole::Leader = change.state {
            // Currently, it is only used by the manual load.
            let cache_region = CacheRegion::from_region(ctx.region());
            debug!(
                "ime try to load region due to became leader";
                "region" => ?cache_region,
            );
            self.try_load_region(cache_region);
        } else if let StateRole::Follower = change.state
            && change.initialized
        {
            let cache_region = CacheRegion::from_region(ctx.region());
            debug!(
                "ime try to evict region due to became follower";
                "region" => ?cache_region,
            );
            self.evict_region(cache_region, EvictReason::BecomeFollower);
        }
    }
}

impl DestroyPeerObserver for LoadEvictionObserver {
    fn on_destroy_peer(&self, r: &Region) {
        let mut region = r.clone();
        if region.get_peers().is_empty() {
            warn!("ime evict an uninitialized region"; "region" => ?region);
            // In some cases, the region may have no peer, such as an
            // uninitialized peer being destroyed. We need to push an empty peer
            // to prevent panic in `CacheRegion::from_region`.
            region.mut_peers().push(Peer::default());
        }
        self.cache_engine.on_region_event(RegionEvent::Eviction {
            region: CacheRegion::from_region(&region),
            reason: EvictReason::DestroyPeer,
            on_evict_finished: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use engine_traits::{RegionEvent, SstMetaInfo};
    use kvproto::{
        import_sstpb::SstMeta,
        metapb::{Peer, Region},
        raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    };
    use raftstore::coprocessor::RoleChange;

    use super::*;

    #[derive(Default)]
    struct MockRegionCacheEngine {
        region_events: Arc<Mutex<Vec<RegionEvent>>>,
    }
    impl RegionCacheEngineExt for MockRegionCacheEngine {
        fn on_region_event(&self, event: RegionEvent) {
            self.region_events.lock().unwrap().push(event);
        }

        fn region_cached(&self, _: &Region, _: bool) -> bool {
            unreachable!()
        }

        fn load_region(&self, _: &Region) {
            unreachable!()
        }
    }

    fn new_admin_request_batch_split() -> RaftCmdRequest {
        let mut request = RaftCmdRequest::default();
        request
            .mut_admin_request()
            .set_cmd_type(AdminCmdType::BatchSplit);
        request
    }

    fn assert_eq_region_events(got: &RegionEvent, expected: &RegionEvent) {
        match (got, expected) {
            (
                RegionEvent::TryLoad {
                    region: got_region,
                    for_manual_range: got_for_manual_range,
                },
                RegionEvent::TryLoad {
                    region: expected_region,
                    for_manual_range: expected_for_manual_range,
                },
            ) => {
                assert_eq!(got_region, expected_region);
                assert_eq!(got_for_manual_range, expected_for_manual_range);
            }
            (
                RegionEvent::Eviction {
                    region: got_region,
                    reason: got_reason,
                    on_evict_finished: _,
                },
                RegionEvent::Eviction {
                    region: expected_region,
                    reason: expected_reason,
                    on_evict_finished: _,
                },
            ) => {
                assert_eq!(got_region, expected_region);
                assert_eq!(got_reason, expected_reason);
            }
            _ => panic!("unexpected region event"),
        }
    }

    #[test]
    fn test_do_not_evict_region_region_split() {
        let cache_engine = Arc::new(MockRegionCacheEngine::default());
        let observer = LoadEvictionObserver::new(cache_engine.clone());

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        let mut ctx = ObserverContext::new(&region);

        let mut pending_handle_ssts = None;
        let mut delete_ssts = Vec::new();
        let mut pending_delete_ssts = Vec::new();

        let mut apply = ApplyCtxInfo {
            pending_handle_ssts: &mut pending_handle_ssts,
            delete_ssts: &mut delete_ssts,
            pending_delete_ssts: &mut pending_delete_ssts,
        };
        let request = new_admin_request_batch_split();
        let response = RaftCmdResponse::default();
        let cmd = Cmd::new(0, 0, request, response);

        // Must not evict region for region split.
        observer.post_exec_cmd(&mut ctx, &cmd, &RegionState::default(), &mut apply);
        assert!(&cache_engine.region_events.lock().unwrap().is_empty());
    }

    #[test]
    fn test_evict_region_ingest_sst() {
        let cache_engine = Arc::new(MockRegionCacheEngine::default());
        let observer = LoadEvictionObserver::new(cache_engine.clone());

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        let mut ctx = ObserverContext::new(&region);

        let mut meta = SstMeta::default();
        meta.set_region_id(1);
        let meta = SstMetaInfo {
            total_bytes: 0,
            total_kvs: 0,
            meta,
        };
        let mut pending_handle_ssts = Some(vec![meta]);
        let mut delete_ssts = Vec::new();
        let mut pending_delete_ssts = Vec::new();

        let mut apply = ApplyCtxInfo {
            pending_handle_ssts: &mut pending_handle_ssts,
            delete_ssts: &mut delete_ssts,
            pending_delete_ssts: &mut pending_delete_ssts,
        };
        let request = RaftCmdRequest::default();
        let response = RaftCmdResponse::default();
        let cmd = Cmd::new(0, 0, request, response);

        observer.post_exec_cmd(&mut ctx, &cmd, &RegionState::default(), &mut apply);
        let cached_region = CacheRegion::from_region(&region);
        let expected = RegionEvent::Eviction {
            region: cached_region,
            reason: EvictReason::IngestSST,
            on_evict_finished: None,
        };
        assert_eq_region_events(&cache_engine.region_events.lock().unwrap()[0], &expected);
    }

    #[test]
    fn test_load_region_became_leader() {
        let cache_engine = Arc::new(MockRegionCacheEngine::default());
        let observer = LoadEvictionObserver::new(cache_engine.clone());

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        let mut ctx = ObserverContext::new(&region);
        let role_change = RoleChange::new_for_test(StateRole::Leader);
        observer.on_role_change(&mut ctx, &role_change);
        let cached_region = CacheRegion::from_region(&region);
        let expected = RegionEvent::TryLoad {
            region: cached_region,
            for_manual_range: true,
        };
        assert_eq_region_events(&cache_engine.region_events.lock().unwrap()[0], &expected);
    }
}
