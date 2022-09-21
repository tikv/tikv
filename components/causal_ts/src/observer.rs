// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::{
    BoxRegionChangeObserver, BoxRoleObserver, Coprocessor, CoprocessorHost, ObserverContext,
    RegionChangeEvent, RegionChangeObserver, RegionChangeReason, RoleChange, RoleObserver,
};

use crate::CausalTsProvider;

/// CausalObserver appends timestamp for RawKV V2 data, and invoke
/// causal_ts_provider.flush() on specified event, e.g. leader
/// transfer, snapshot apply.
/// Should be used ONLY when API v2 is enabled.
pub struct CausalObserver<Ts: CausalTsProvider> {
    causal_ts_provider: Arc<Ts>,
}

impl<Ts: CausalTsProvider> Clone for CausalObserver<Ts> {
    fn clone(&self) -> Self {
        Self {
            causal_ts_provider: self.causal_ts_provider.clone(),
        }
    }
}

// Causal observer's priority should be higher than all other observers, to
// avoid being bypassed.
const CAUSAL_OBSERVER_PRIORITY: u32 = 0;
impl<Ts: CausalTsProvider + 'static> CausalObserver<Ts> {
    pub fn new(causal_ts_provider: Arc<Ts>) -> Self {
        Self { causal_ts_provider }
    }

    pub fn register_to<E: KvEngine>(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        coprocessor_host
            .registry
            .register_role_observer(CAUSAL_OBSERVER_PRIORITY, BoxRoleObserver::new(self.clone()));
        coprocessor_host.registry.register_region_change_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxRegionChangeObserver::new(self.clone()),
        );
    }
}

const REASON_LEADER_TRANSFER: &str = "leader_transfer";
const REASON_REGION_MERGE: &str = "region_merge";

impl<Ts: CausalTsProvider> CausalObserver<Ts> {
    fn flush_timestamp(&self, region: &Region, reason: &'static str) {
        fail::fail_point!("causal_observer_flush_timestamp", |_| ());

        if let Err(err) = self.causal_ts_provider.flush() {
            warn!("CausalObserver::flush_timestamp error"; "error" => ?err, "region_id" => region.get_id(), "region" => ?region, "reason" => reason);
        } else {
            debug!("CausalObserver::flush_timestamp succeed"; "region_id" => region.get_id(), "region" => ?region, "reason" => reason);
        }
    }
}

impl<Ts: CausalTsProvider> Coprocessor for CausalObserver<Ts> {}

impl<Ts: CausalTsProvider> RoleObserver for CausalObserver<Ts> {
    /// Observe becoming leader, to flush CausalTsProvider.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        // In scenario of frequent leader transfer, the observing of change from
        // follower to leader by `on_role_change` would be later than the real role
        // change in raft state and adjacent write commands.
        // This would lead to the late of flush, and violate causality. See issue
        // #12498. So we observe role change to Candidate to fix this issue.
        // Also note that when there is only one peer, it would become leader directly.
        if role_change.state == StateRole::Candidate
            || (ctx.region().peers.len() == 1 && role_change.state == StateRole::Leader)
        {
            self.flush_timestamp(ctx.region(), REASON_LEADER_TRANSFER);
        }
    }
}

impl<Ts: CausalTsProvider> RegionChangeObserver for CausalObserver<Ts> {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        if role != StateRole::Leader {
            return;
        }

        // In the scenario of region merge, the target region would merge some entries
        // from source region with larger timestamps (when leader of source region is in
        // another store with larger TSO batch than the store of target region's
        // leader). So we need a flush after commit merge. See issue #12680.
        // TODO: do not need flush if leaders of source & target region are in the same
        // store.
        if let RegionChangeEvent::Update(RegionChangeReason::CommitMerge) = event {
            self.flush_timestamp(ctx.region(), REASON_REGION_MERGE);
        }
    }
}
