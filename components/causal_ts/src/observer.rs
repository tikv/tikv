// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use api_version::{ApiV2, KeyMode, KvFormat};
use engine_traits::KvEngine;
use kvproto::{
    metapb::Region,
    raft_cmdpb::{CmdType, Request as RaftRequest},
};
use raft::StateRole;
use raftstore::{
    coprocessor,
    coprocessor::{
        BoxQueryObserver, BoxRegionChangeObserver, BoxRoleObserver, Coprocessor, CoprocessorHost,
        ObserverContext, QueryObserver, RegionChangeEvent, RegionChangeObserver,
        RegionChangeReason, RoleChange, RoleObserver,
    },
};

use crate::CausalTsProvider;

/// CausalObserver appends timestamp for RawKV V2 data,
/// and invoke causal_ts_provider.flush() on specified event, e.g. leader transfer, snapshot apply.
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

// Causal observer's priority should be higher than all other observers, to avoid being bypassed.
const CAUSAL_OBSERVER_PRIORITY: u32 = 0;

impl<Ts: CausalTsProvider + 'static> CausalObserver<Ts> {
    pub fn new(causal_ts_provider: Arc<Ts>) -> Self {
        Self { causal_ts_provider }
    }

    pub fn register_to<E: KvEngine>(&self, coprocessor_host: &mut CoprocessorHost<E>) {
        coprocessor_host.registry.register_query_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
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

impl<Ts: CausalTsProvider> QueryObserver for CausalObserver<Ts> {
    fn pre_propose_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        requests: &mut Vec<RaftRequest>,
    ) -> coprocessor::Result<()> {
        let region_id = ctx.region().get_id();
        let mut ts = None;

        for req in requests.iter_mut().filter(|r| {
            r.get_cmd_type() == CmdType::Put
                && ApiV2::parse_key_mode(r.get_put().get_key()) == KeyMode::Raw
        }) {
            if ts.is_none() {
                ts = Some(self.causal_ts_provider.get_ts().map_err(|err| {
                    coprocessor::Error::Other(box_err!("Get causal timestamp error: {:?}", err))
                })?);
            }

            ApiV2::append_ts_on_encoded_bytes(req.mut_put().mut_key(), ts.unwrap());
            trace!("CausalObserver::pre_propose_query, append_ts"; "region_id" => region_id,
                "key" => &log_wrappers::Value::key(req.get_put().get_key()), "ts" => ?ts.unwrap());
        }
        Ok(())
    }
}

impl<Ts: CausalTsProvider> RoleObserver for CausalObserver<Ts> {
    /// Observe becoming leader, to flush CausalTsProvider.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        // In scenario of frequent leader transfer, the observing of change from
        // follower to leader by `on_role_change` would be later than the real role
        // change in raft state and adjacent write commands.
        // This would lead to the late of flush, and violate causality. See issue #12498.
        // So we observe role change to Candidate to fix this issue.
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

        // In the scenario of region merge, the target region would merge some entries from source
        // region with larger timestamps (when leader of source region is in another store with
        // larger TSO batch than the store of target region's leader).
        // So we need a flush after commit merge. See issue #12680.
        // TODO: do not need flush if leaders of source & target region are in the same store.
        if let RegionChangeEvent::Update(RegionChangeReason::CommitMerge) = event {
            self.flush_timestamp(ctx.region(), REASON_REGION_MERGE);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{mem, sync::Arc, time::Duration};

    use api_version::{ApiV2, KvFormat};
    use futures::executor::block_on;
    use kvproto::{
        metapb::Region,
        raft_cmdpb::{RaftCmdRequest, Request as RaftRequest},
    };
    use test_raftstore::TestPdClient;
    use txn_types::{Key, TimeStamp};

    use super::*;
    use crate::BatchTsoProvider;

    fn init() -> CausalObserver<BatchTsoProvider<TestPdClient>> {
        let pd_cli = Arc::new(TestPdClient::new(0, true));
        pd_cli.set_tso(100.into());
        let causal_ts_provider =
            Arc::new(block_on(BatchTsoProvider::new_opt(pd_cli, Duration::ZERO, 100)).unwrap());
        CausalObserver::new(causal_ts_provider)
    }

    #[test]
    fn test_causal_observer() {
        let testcases: Vec<&[&[u8]]> = vec![
            &[b"r\0a", b"r\0b"],
            &[b"r\0c"],
            &[b"r\0d", b"r\0e", b"r\0f"],
        ];

        let ob = init();
        let mut region = Region::default();
        region.set_id(1);
        let mut ctx = ObserverContext::new(&region);

        for (i, keys) in testcases.into_iter().enumerate() {
            let mut cmd_req = RaftCmdRequest::default();

            for key in keys {
                let key = ApiV2::encode_raw_key(key, None);
                let value = b"value".to_vec();
                let mut req = RaftRequest::default();
                req.set_cmd_type(CmdType::Put);
                req.mut_put().set_key(key.into_encoded());
                req.mut_put().set_value(value);

                cmd_req.mut_requests().push(req);
            }

            let query = cmd_req.mut_requests();
            let mut vec_query: Vec<RaftRequest> = mem::take(query).into();
            ob.pre_propose_query(&mut ctx, &mut vec_query).unwrap();
            *query = vec_query.into();

            for req in cmd_req.get_requests() {
                let key = Key::from_encoded_slice(req.get_put().get_key());
                let (_, ts) = ApiV2::decode_raw_key_owned(key, true).unwrap();
                assert_eq!(ts, Some(TimeStamp::from(i as u64 + 101)));
            }
        }
    }
}
