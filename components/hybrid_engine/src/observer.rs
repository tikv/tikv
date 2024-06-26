// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use engine_traits::{CacheRange, KvEngine, RangeCacheEngineExt};
use kvproto::{metapb::Region, raft_cmdpb::AdminCmdType, raft_serverpb::RaftApplyState};
use raft::StateRole;
use raftstore::coprocessor::{
    AdminObserver, ApplyCtxInfo, ApplySnapshotObserver, BoxAdminObserver, BoxApplySnapshotObserver,
    BoxCmdObserver, BoxQueryObserver, BoxRoleObserver, Cmd, CmdBatch, CmdObserver, Coprocessor,
    CoprocessorHost, ObserveLevel, ObserverContext, QueryObserver, RegionState, RoleObserver,
};

#[derive(Clone)]
pub struct Observer {
    pending_evict: Arc<Mutex<Vec<CacheRange>>>,
    cache_engine: Arc<dyn RangeCacheEngineExt + Send + Sync>,
}

impl Observer {
    pub fn new(cache_engine: Arc<dyn RangeCacheEngineExt + Send + Sync>) -> Self {
        Observer {
            pending_evict: Arc::default(),
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
        // Evict cache when a peer splits and merges.
        coprocessor_host
            .registry
            .register_admin_observer(priority, BoxAdminObserver::new(self.clone()));
        // Evict cache when a peer applies snapshot.
        coprocessor_host.registry.register_apply_snapshot_observer(
            priority,
            BoxApplySnapshotObserver::new(self.clone()),
        );
        // Evict cache when a leader steps down.
        coprocessor_host
            .registry
            .register_role_observer(priority, BoxRoleObserver::new(self.clone()));
    }

    fn post_exec_cmd(
        &self,
        ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        state: &RegionState,
        apply: &mut ApplyCtxInfo<'_>,
    ) {
        if !self.cache_engine.range_cache_engine_enabled() {
            return;
        }
        // Evict caches for successfully executed ingest commands and admin
        // commands that change region range.
        if apply.pending_handle_ssts.is_some()
            || (state.modified_region.is_some()
                && matches!(
                    cmd.request.get_admin_request().get_cmd_type(),
                    AdminCmdType::BatchSplit
                        | AdminCmdType::Split
                        | AdminCmdType::PrepareMerge
                        | AdminCmdType::CommitMerge
                ))
        {
            let range = CacheRange::from_region(ctx.region());
            tikv_util::info!(
                "evict range due to apply commands";
                "region_id" => ctx.region().get_id(),
                "is_ingest_sst" => apply.pending_handle_ssts.is_some(),
                "admin_command" => ?cmd.request.get_admin_request().get_cmd_type(),
                "range" => ?range,
            );
            self.pending_evict.lock().unwrap().push(range);
        }
    }

    fn on_flush_cmd(&self) {
        if !self.cache_engine.range_cache_engine_enabled() {
            return;
        }

        let ranges = {
            let mut ranges = self.pending_evict.lock().unwrap();
            std::mem::take(&mut *ranges)
        };
        for range in ranges {
            self.cache_engine.evict_range(&range);
        }
    }

    fn evict_region_range(&self, region: &Region) {
        if !self.cache_engine.range_cache_engine_enabled() {
            return;
        }

        let range = CacheRange::from_region(region);
        tikv_util::info!(
            "evict range due to apply snapshot";
            "region_id" => region.get_id(),
            "range" => ?range,
        );
        self.pending_evict.lock().unwrap().push(range);
    }
}

impl Coprocessor for Observer {}

impl QueryObserver for Observer {
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

impl AdminObserver for Observer {
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

impl ApplySnapshotObserver for Observer {
    fn post_apply_snapshot(
        &self,
        ctx: &mut ObserverContext<'_>,
        _: u64,
        _: &raftstore::store::SnapKey,
        _: Option<&raftstore::store::Snapshot>,
    ) {
        // TODO: If a region is applying snapshot, it must be a follower.
        //       Is it necessary to evict again?
        self.evict_region_range(ctx.region())
    }
}

impl RoleObserver for Observer {
    fn on_role_change(
        &self,
        ctx: &mut ObserverContext<'_>,
        change: &raftstore::coprocessor::RoleChange,
    ) {
        if let StateRole::Follower = change.state
            && change.initialized
        {
            self.evict_region_range(ctx.region())
        }
    }
}

impl<E> CmdObserver<E> for Observer {
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
    use std::sync::atomic::{AtomicBool, Ordering};

    use engine_traits::SstMetaInfo;
    use kvproto::{
        import_sstpb::SstMeta,
        metapb::Peer,
        raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    };

    use super::*;

    #[derive(Default)]
    struct MockRangeCacheEngine {
        enabled: AtomicBool,
        evicted_ranges: Arc<Mutex<Vec<CacheRange>>>,
    }
    impl RangeCacheEngineExt for MockRangeCacheEngine {
        fn range_cache_engine_enabled(&self) -> bool {
            self.enabled.load(Ordering::Relaxed)
        }
        fn evict_range(&self, range: &CacheRange) {
            self.evicted_ranges.lock().unwrap().push(range.clone());
        }
    }

    #[test]
    fn test_evict_range_ingest_sst() {
        let cache_engine = Arc::new(MockRangeCacheEngine::default());
        let observer = Observer::new(cache_engine.clone());

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

        // Must not evict range when range cache engine is disabled.
        observer.post_exec_cmd(&mut ctx, &cmd, &RegionState::default(), &mut apply);
        observer.on_flush_cmd();
        assert!(cache_engine.evicted_ranges.lock().unwrap().is_empty());

        // Enable range cache engine.
        cache_engine.enabled.store(true, Ordering::Relaxed);
        observer.post_exec_cmd(&mut ctx, &cmd, &RegionState::default(), &mut apply);
        observer.on_flush_cmd();
        let expected = CacheRange::from_region(&region);
        assert_eq!(&cache_engine.evicted_ranges.lock().unwrap()[0], &expected);
    }
}
