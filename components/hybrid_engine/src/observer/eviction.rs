// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use engine_traits::{CacheRange, EvictReason, KvEngine, RangeCacheEngineExt, RegionEvent};
use kvproto::{metapb::Region, raft_cmdpb::AdminCmdType, raft_serverpb::RaftApplyState};
use raft::StateRole;
use raftstore::coprocessor::{
    AdminObserver, ApplyCtxInfo, BoxAdminObserver, BoxCmdObserver, BoxQueryObserver,
    BoxRoleObserver, Cmd, CmdBatch, CmdObserver, Coprocessor, CoprocessorHost, ObserveLevel,
    ObserverContext, QueryObserver, RegionState, RoleObserver,
};
use tikv_util::info;

#[derive(Clone)]
pub struct EvictionObserver {
    // observer is per thread so there is no need to use mutex here,
    // but current inteface only provides `&self` but not `&mut self`,
    // so we use mutex to workaround this restriction.
    // TODO: change Observer's interface to `&mut self`.
    pending_events: Arc<Mutex<Vec<RegionEvent>>>,
    cache_engine: Arc<dyn RangeCacheEngineExt + Send + Sync>,
}

impl EvictionObserver {
    pub fn new(cache_engine: Arc<dyn RangeCacheEngineExt + Send + Sync>) -> Self {
        EvictionObserver {
            pending_events: Arc::default(),
            cache_engine,
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        // This observer does not need high priority, use the default 100.
        let priority = 100;
        coprocessor_host
            .registry
            .register_cmd_observer(priority, BoxCmdObserver::new(self.clone()));
        // Evict cache when a peer applies ingest sst.
        coprocessor_host
            .registry
            .register_query_observer(priority, BoxQueryObserver::new(self.clone()));
        // Evict cache when a peer applies region merge.
        coprocessor_host
            .registry
            .register_admin_observer(priority, BoxAdminObserver::new(self.clone()));
        // Evict cache when a leader steps down.
        coprocessor_host
            .registry
            .register_role_observer(priority, BoxRoleObserver::new(self.clone()));

        // NB: We do not evict the cache when applying a snapshot because
        // the peer must be in the follower role during this process.
        // The cache is already evicted when the leader steps down.
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
            let range = CacheRange::from_region(ctx.region());
            info!(
                "ime evict range due to apply commands";
                "region_id" => ctx.region().get_id(),
                "is_ingest_sst" => apply.pending_handle_ssts.is_some(),
                "admin_command" => ?cmd.request.get_admin_request().get_cmd_type(),
                "start_key" => ?log_wrappers::Value(&ctx.region().start_key),
                "end_key" => ?log_wrappers::Value(&ctx.region().end_key),
            );
            self.pending_events
                .lock()
                .unwrap()
                .push(RegionEvent::Eviction {
                    region: ctx.region().clone(),
                    reason: EvictReason::Merge,
                });
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

            self.pending_events
                .lock()
                .unwrap()
                .push(RegionEvent::Split {
                    source: ctx.region().clone(),
                    new_regions: state.new_regions.clone(),
                });
        }
    }

    fn on_flush_cmd(&self) {
        let events = std::mem::take(&mut *self.pending_events.lock().unwrap());
        for e in events {
            self.cache_engine.on_region_event(e);
        }
    }

    fn evict_range_on_leader_steps_down(&self, region: &Region) {
        let range = CacheRange::from_region(region);
        info!(
           "ime evict region due to leader step down";
           "region_id" => region.get_id(),
           "epoch" => ?region.get_region_epoch(),
           "start_key" => ?log_wrappers::Value(&region.start_key),
           "end_key" => ?log_wrappers::Value(&region.end_key),
        );
        self.pending_events
            .lock()
            .unwrap()
            .push(RegionEvent::Eviction {
                region: region.clone(),
                reason: EvictReason::BecomeFollower,
            });
    }
}

impl Coprocessor for EvictionObserver {}

impl QueryObserver for EvictionObserver {
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

impl AdminObserver for EvictionObserver {
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

impl RoleObserver for EvictionObserver {
    fn on_role_change(
        &self,
        ctx: &mut ObserverContext<'_>,
        change: &raftstore::coprocessor::RoleChange,
    ) {
        if let StateRole::Follower = change.state
            && change.initialized
        {
            self.evict_range_on_leader_steps_down(ctx.region())
        }
    }
}

impl<E> CmdObserver<E> for EvictionObserver {
    fn on_flush_applied_cmd_batch(
        &self,
        _max_level: ObserveLevel,
        _cmd_batches: &mut Vec<CmdBatch>,
        _engine: &E,
    ) {
        self.on_flush_cmd();
    }
    fn on_applied_current_term(&self, role: StateRole, region: &Region) {}
}

#[cfg(test)]
mod tests {
    use engine_traits::{RegionEvent, SstMetaInfo};
    use kvproto::{
        import_sstpb::SstMeta,
        metapb::Peer,
        raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    };

    use super::*;

    #[derive(Default)]
    struct MockRangeCacheEngine {
        region_events: Arc<Mutex<Vec<RegionEvent>>>,
    }
    impl RangeCacheEngineExt for MockRangeCacheEngine {
        fn on_region_event(&self, event: RegionEvent) {
            self.region_events.lock().unwrap().push(event);
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
    fn test_do_not_evict_range_region_split() {
        let cache_engine = Arc::new(MockRangeCacheEngine::default());
        let observer = EvictionObserver::new(cache_engine.clone());

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
        observer.on_flush_cmd();
        let expected = CacheRange::from_region(&region);
        assert!(&cache_engine.region_events.lock().unwrap().is_empty());
    }

    #[test]
    fn test_evict_range_ingest_sst() {
        let cache_engine = Arc::new(MockRangeCacheEngine::default());
        let observer = EvictionObserver::new(cache_engine.clone());

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
        observer.on_flush_cmd();
        let expected = CacheRange::from_region(&region);
        let expected = RegionEvent::Eviction {
            region,
            reason: EvictReason::Merge,
        };
        assert_eq!(&cache_engine.region_events.lock().unwrap()[0], &expected);
    }
}
