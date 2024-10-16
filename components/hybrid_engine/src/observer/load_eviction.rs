// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{CacheRegion, EvictReason, KvEngine, RegionCacheEngineExt, RegionEvent};
use kvproto::{
    metapb::Region,
    raft_cmdpb::AdminCmdType,
    raft_serverpb::{ExtraMessage, ExtraMessageType, RaftApplyState},
};
use raft::StateRole;
use raftstore::coprocessor::{
    dispatcher::BoxExtraMessageObserver, AdminObserver, ApplyCtxInfo, ApplySnapshotObserver,
    BoxAdminObserver, BoxApplySnapshotObserver, BoxQueryObserver, BoxRoleObserver, Cmd,
    Coprocessor, CoprocessorHost, ExtraMessageObserver, ObserverContext, QueryObserver,
    RegionState, RoleObserver,
};
use tikv_util::info;

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
        // Pre load region in transfer leader
        coprocessor_host
            .registry
            .register_extra_message_observer(priority, BoxExtraMessageObserver::new(self.clone()));
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
        if apply.pending_handle_ssts.is_some()
            || (state.modified_region.is_some()
                && matches!(
                    cmd.request.get_admin_request().get_cmd_type(),
                    AdminCmdType::PrepareMerge | AdminCmdType::CommitMerge
                ))
        {
            let cache_region = CacheRegion::from_region(ctx.region());
            info!(
                "ime evict range due to apply commands";
                "region" => ?cache_region,
                "is_ingest_sst" => apply.pending_handle_ssts.is_some(),
                "admin_command" => ?cmd.request.get_admin_request().get_cmd_type(),
            );
            self.evict_region(cache_region, EvictReason::Merge)
        }
        // there are new_regions, this must be a split event.
        if !state.new_regions.is_empty() {
            let cmd_type = cmd.request.get_admin_request().get_cmd_type();
            assert!(cmd_type == AdminCmdType::BatchSplit || cmd_type == AdminCmdType::Split);
            info!(
                "ime handle region split";
                "region_id" => ctx.region().get_id(),
                "admin_command" => ?cmd.request.get_admin_request().get_cmd_type(),
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

    // Try to load region. It will be loaded if it's overlapped with maunal range
    fn try_load_region(&self, region: CacheRegion) {
        self.cache_engine.on_region_event(RegionEvent::TryLoad {
            region,
            for_manual_range: true,
        });
    }

    fn evict_region(&self, region: CacheRegion, reason: EvictReason) {
        self.cache_engine
            .on_region_event(RegionEvent::Eviction { region, reason });
    }
}

impl Coprocessor for LoadEvictionObserver {}

impl QueryObserver for LoadEvictionObserver {
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
        if req.has_prepare_flashback() {
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

    fn pre_transfer_leader(
        &self,
        ctx: &mut ObserverContext<'_>,
        _tr: &kvproto::raft_cmdpb::TransferLeaderRequest,
    ) -> raftstore::coprocessor::Result<Option<kvproto::raft_serverpb::ExtraMessage>> {
        if !self.cache_engine.region_cached(ctx.region()) {
            return Ok(None);
        }
        let mut msg = ExtraMessage::new();
        msg.set_type(ExtraMessageType::MsgPreLoadRegionRequest);
        Ok(Some(msg))
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
        // A region can may still be loaded when it's leader. E.g, to pre-load
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
            info!(
                "ime try to load region due to became leader";
                "region" => ?cache_region,
            );
            self.try_load_region(cache_region);
        } else if let StateRole::Follower = change.state
            && change.initialized
        {
            let cache_region = CacheRegion::from_region(ctx.region());
            info!(
                "ime try to evict region due to became follower";
                "region" => ?cache_region,
            );
            self.evict_region(cache_region, EvictReason::BecomeFollower);
        }
    }
}

impl ExtraMessageObserver for LoadEvictionObserver {
    fn on_extra_message(&self, r: &Region, extra_msg: &ExtraMessage) {
        if extra_msg.get_type() == ExtraMessageType::MsgPreLoadRegionRequest {
            self.cache_engine.load_region(r);
        }
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

        fn region_cached(&self, range: &Region) -> bool {
            unreachable!()
        }

        fn load_region(&self, range: &Region) {
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

        // Must not evict range for region split.
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
            reason: EvictReason::Merge,
        };
        assert_eq!(&cache_engine.region_events.lock().unwrap()[0], &expected);
    }

    #[test]
    fn test_load_region_became_leader() {
        let cache_engine = Arc::new(MockRegionCacheEngine::default());
        let observer = LoadEvictionObserver::new(cache_engine.clone());

        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(Peer::default());
        let mut ctx = ObserverContext::new(&region);
        let role_change = RoleChange::new(StateRole::Leader);
        observer.on_role_change(&mut ctx, &role_change);
        let cached_region = CacheRegion::from_region(&region);
        let expected = RegionEvent::TryLoad {
            region: cached_region,
            for_manual_range: true,
        };
        assert_eq!(&cache_engine.region_events.lock().unwrap()[0], &expected);
    }
}
