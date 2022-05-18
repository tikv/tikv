// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use api_version::{ApiV2, KeyMode, KvFormat};
use collections::HashMap;
use engine_traits::KvEngine;
use kvproto::raft_cmdpb::{CmdType, Request as RaftRequest};
use parking_lot::RwLock;
use raft::StateRole;
use raftstore::{
    coprocessor,
    coprocessor::{
        BoxQueryObserver, BoxRegionChangeObserver, BoxRoleObserver, Coprocessor, CoprocessorHost,
        ObserverContext, QueryObserver, RegionChangeEvent, RegionChangeObserver, RoleChange,
        RoleObserver,
    },
};

use crate::CausalTsProvider;

#[derive(Default, Debug)]
struct RegionInfo {
    // Note that `is_leader` is observed by `on_role_change`. It would differ from current raft state.
    pub is_leader: AtomicBool,
    // TODO: Add more fields to indicate flush state of region.
    // To help regions without leader change to utilize existing batches.
    // See https://github.com/tikv/tikv/pull/12099#discussion_r832232629.
}

type RegionMap = HashMap<u64, RegionInfo>;

/// CausalObserver appends timestamp for RawKV V2 data,
/// and invoke causal_ts_provider.flush() on specified event, e.g. leader transfer, snapshot apply.
/// Should be used ONLY when API v2 is enabled.
pub struct CausalObserver<Ts: CausalTsProvider> {
    causal_ts_provider: Arc<Ts>,
    region_map: Arc<RwLock<RegionMap>>,
}

impl<Ts: CausalTsProvider> Clone for CausalObserver<Ts> {
    fn clone(&self) -> Self {
        Self {
            causal_ts_provider: self.causal_ts_provider.clone(),
            region_map: self.region_map.clone(),
        }
    }
}

// Causal observer's priority should be higher than all other observers, to avoid being bypassed.
const CAUSAL_OBSERVER_PRIORITY: u32 = 0;

impl<Ts: CausalTsProvider> CausalObserver<Ts> {
    // Note that `region_id` not in `region_map` would happen in test.
    // Some test cases do not have coprocessor to deal with "on_region_changed".
    // TODO: make test cases handle "on_region_changed".
    fn region_is_leader(&self, region_id: u64) -> bool {
        let region_map = self.region_map.read();
        region_map
            .get(&region_id)
            .map_or(true, |info| info.is_leader.load(Ordering::Relaxed))
    }
}

impl<Ts: CausalTsProvider + 'static> CausalObserver<Ts> {
    pub fn new(causal_ts_provider: Arc<Ts>) -> Self {
        Self {
            causal_ts_provider,
            region_map: Arc::new(RwLock::new(RegionMap::default())),
        }
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

impl<Ts: CausalTsProvider> Coprocessor for CausalObserver<Ts> {}

impl<Ts: CausalTsProvider> QueryObserver for CausalObserver<Ts> {
    fn pre_propose_query(
        &self,
        ctx: &mut ObserverContext<'_>,
        requests: &mut Vec<RaftRequest>,
    ) -> coprocessor::Result<()> {
        let mut ts = None;

        for req in requests.iter_mut().filter(|r| {
            r.get_cmd_type() == CmdType::Put
                && ApiV2::parse_key_mode(r.get_put().get_key()) == KeyMode::Raw
        }) {
            if ts.is_none() {
                let region_id = ctx.region().get_id();
                if !self.region_is_leader(region_id) {
                    // Fix issue #12498.
                    // In scenario of frequent leader transfer, the observing of change from
                    // follower to leader by `on_role_change` would be later than the real role
                    // change in raft state and adjacent write commands.
                    // As raft state must be leader when reach here, `!is_leader` just indicates that
                    // the change from follower to leader is not observed yet.
                    // So it is necessary to flush timestamp here for causality correctness.
                    // TODO: reduce the duplicated flush in following `on_role_change`.
                    // TODO: `flush()` return a ts.
                    self.causal_ts_provider
                        .flush()
                        .map_err(|err| -> coprocessor::Error {
                            box_err!("Flush causal timestamp in pre_propose error: {:?}", err)
                        })?;
                    debug!("CausalObserver::pre_propose_query, flush timestamp succeed"; "region" => region_id);
                }

                ts = Some(self.causal_ts_provider.get_ts().map_err(
                    |err| -> coprocessor::Error {
                        box_err!("Get causal timestamp error: {:?}", err)
                    },
                )?);
            }

            ApiV2::append_ts_on_encoded_bytes(req.mut_put().mut_key(), ts.unwrap());
        }
        Ok(())
    }
}

impl<Ts: CausalTsProvider> RoleObserver for CausalObserver<Ts> {
    /// Observe becoming leader, to flush CausalTsProvider.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        let region_id = ctx.region().get_id();
        let is_leader = role_change.state == StateRole::Leader;
        if is_leader {
            if let Err(err) = self.causal_ts_provider.flush() {
                warn!("CausalObserver::on_role_change, flush timestamp error"; "region" => region_id, "error" => ?err);
            } else {
                debug!("CausalObserver::on_role_change, flush timestamp succeed"; "region" => region_id);
            }
        }

        let region_map = self.region_map.read();
        if let Some(info) = region_map.get(&region_id) {
            // The region would have been destroyed.
            info.is_leader.store(is_leader, Ordering::Relaxed);
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
        let region_id = ctx.region().get_id();
        let is_leader = role == StateRole::Leader;
        match event {
            RegionChangeEvent::Create => {
                self.region_map.write().insert(
                    region_id,
                    RegionInfo {
                        is_leader: AtomicBool::new(is_leader),
                    },
                );
            }
            RegionChangeEvent::Destroy => {
                self.region_map.write().remove(&region_id);
            }
            _ => {}
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
