// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{APIVersion, KeyMode, APIV2};
use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::CfName;
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::CmdType;
use parking_lot::RwLock;
use raft::StateRole;
use raftstore::coprocessor::{
    ApplySnapshotObserver, BoxApplySnapshotObserver, BoxCmdObserver, BoxRegionChangeObserver,
    BoxRoleObserver, CmdBatch, CmdObserver, Coprocessor, CoprocessorHost, ObserveLevel,
    ObserverContext, RegionChangeObserver, RoleChange, RoleObserver,
};
use std::cmp;
use std::sync::Arc;
use txn_types::{Key, TimeStamp};

use crate::CausalTsProvider;

#[derive(Debug, Clone)]
pub(crate) struct RegionCausalInfo {
    pub(crate) max_ts: TimeStamp,
}

impl RegionCausalInfo {
    pub(crate) fn new(max_ts: TimeStamp) -> RegionCausalInfo {
        RegionCausalInfo { max_ts }
    }
}

pub(crate) type RegionsMap = HashMap<u64, RegionCausalInfo>;

#[derive(Debug, Default)]
pub struct RegionsCausalManager {
    regions_map: Arc<RwLock<RegionsMap>>,
}

impl RegionsCausalManager {
    pub fn new() -> RegionsCausalManager {
        RegionsCausalManager {
            regions_map: Arc::new(RwLock::new(RegionsMap::default())),
        }
    }

    pub fn update_max_ts(&self, region_id: u64, new_ts: TimeStamp) {
        let mut m = self.regions_map.write();
        m.entry(region_id)
            .and_modify(|r| {
                if new_ts > r.max_ts {
                    r.max_ts = new_ts;
                }
            })
            .or_insert_with(|| RegionCausalInfo::new(new_ts));
    }

    pub fn max_ts(&self, region_id: u64) -> TimeStamp {
        let m = self.regions_map.read();
        m.get(&region_id).map_or_else(TimeStamp::zero, |r| r.max_ts)
    }

    pub fn remove_region(&self, region_id: u64) {
        let mut m = self.regions_map.write();
        m.remove(&region_id);
    }
}

/// CausalObserver maintains causality of RawKV requests by observer raft commands
/// Should be used ONLY when API v2 is enabled.
#[derive(Clone)]
pub struct CausalObserver {
    causal_manager: Arc<RegionsCausalManager>,
    causal_ts: Arc<dyn CausalTsProvider>,
}

// Causality is most important and should be done first.
const CAUSAL_OBSERVER_PRIORITY: u32 = 0;

impl CausalObserver {
    pub fn new(
        causal_manager: Arc<RegionsCausalManager>,
        causal_ts: Arc<dyn CausalTsProvider>,
    ) -> Self {
        Self {
            causal_manager,
            causal_ts,
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<RocksEngine>) {
        coprocessor_host
            .registry
            .register_cmd_observer(CAUSAL_OBSERVER_PRIORITY, BoxCmdObserver::new(self.clone()));
        coprocessor_host.registry.register_apply_snapshot_observer(
            CAUSAL_OBSERVER_PRIORITY,
            BoxApplySnapshotObserver::new(self.clone()),
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

impl Coprocessor for CausalObserver {}

impl<E> CmdObserver<E> for CausalObserver {
    /// Observe cmd applied, to maintain maximum causal timestamp for every region.
    fn on_flush_applied_cmd_batch(&self, _: ObserveLevel, cmd_batches: &mut Vec<CmdBatch>, _: &E) {
        debug!("CausalObserver::on_flush_applied_cmd_batch");
        'outer: for batch in cmd_batches.iter() {
            // Timestamp is always increasing in batches & commands. So iterate reversely.
            for cmd in batch.cmds.iter().rev() {
                if cmd.response.get_header().has_error() || cmd.request.has_admin_request() {
                    continue;
                }
                for req in cmd.request.requests.iter().rev() {
                    let key_slice = req.get_put().get_key();
                    // RawKV using raft "Put" requests for both put & delete.
                    if matches!(req.get_cmd_type(), CmdType::Put)
                        && matches!(APIV2::parse_key_mode(key_slice), KeyMode::Raw)
                    {
                        debug_assert!({
                            // verify key encoding
                            let key = Key::from_encoded_slice(key_slice);
                            APIV2::decode_raw_key_owned(key, true).is_ok()
                        });
                        let ts = Key::decode_ts_from(key_slice).unwrap();
                        self.causal_manager.update_max_ts(batch.region_id, ts);
                        debug!("CausalObserver::on_flush_applied_cmd_batch"; "region" => batch.region_id, "ts" => ?ts);

                        continue 'outer;
                    }
                }
            }
        }
    }

    fn on_applied_current_term(&self, _: StateRole, _: &Region) {}
}

/// Observe snapshot applied, to maintain maximum causal timestamp for every region.
impl ApplySnapshotObserver for CausalObserver {
    fn apply_plain_kvs(
        &self,
        ctx: &mut ObserverContext<'_>,
        _cf: CfName,
        _kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        self.handle_snapshot(ctx.region().get_id());
    }

    fn apply_sst(&self, ctx: &mut ObserverContext<'_>, _cf: CfName, _path: &str) {
        self.handle_snapshot(ctx.region().get_id());
    }
}

impl CausalObserver {
    fn handle_snapshot(&self, region_id: u64) {
        // Simply update to latest timestamp.
        // As extracting timestamp by snapshot scan will be expensive.
        // TODO: build and carry max timestamp with snapshot
        let ts = self.causal_ts.get_ts().unwrap(); // will panic if un-initialized
        self.causal_manager.update_max_ts(region_id, ts);
        debug!("CausalObserver::handle_snapshot"; "region" => region_id, "latest-ts" => ts);
    }
}

impl RoleObserver for CausalObserver {
    /// Observe becoming leader, to advance CausalTsProvider not less than this region.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        if role_change.state == StateRole::Leader {
            let region_id = ctx.region().get_id();
            let max_ts = self.causal_manager.max_ts(region_id);
            self.causal_ts.flush().unwrap();
            debug!("CausalObserver::on_role_change: become leader & advance timestamp"; "region" => region_id, "max_ts" => max_ts);
        }
    }
}
impl RegionChangeObserver for CausalObserver {
    /// On region split, copy maximum timestamp to new regions.
    fn on_region_split(&self, ctx: &mut ObserverContext<'_>, new_region_ids: &[u64]) {
        let region_id = ctx.region().get_id();
        let max_ts = self.causal_manager.max_ts(region_id);
        for new_region_id in new_region_ids {
            self.causal_manager.update_max_ts(*new_region_id, max_ts);
            debug!("CausalObserver::on_role_split: set timestamp for new region"; "origin region" => region_id, "new region" => new_region_id, "ts" => max_ts);
        }
    }

    /// On region merge, set maximum timestamp to larger one of target & source region.
    fn on_region_merge(&self, ctx: &mut ObserverContext<'_>, source_region_id: u64) {
        let source_region_ts = self.causal_manager.max_ts(source_region_id);
        let target_region_id = ctx.region().get_id();
        let target_region_ts = self.causal_manager.max_ts(target_region_id);
        let max_ts = cmp::max(source_region_ts, target_region_ts);

        self.causal_manager.update_max_ts(target_region_id, max_ts);
        self.causal_manager.remove_region(source_region_id);
        debug!("CausalObserver::on_role_merge: set timestamp for merge"; "source region" => ?source_region_ts, "target region" => ?target_region_ts, "ts" => max_ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleTsoProvider;
    use api_version::{APIVersion, APIV2};
    use engine_rocks::util::new_temp_engine;
    use engine_rocks::RocksEngine;
    use engine_traits::Engines;
    use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, Request};
    use raftstore::coprocessor::{
        Cmd, CmdBatch, CmdObserveInfo, CmdObserver, ObserveHandle, ObserveLevel,
    };
    use std::sync::Arc;
    use test_raftstore::TestPdClient;

    fn init() -> (
        CausalObserver,
        Arc<RegionsCausalManager>,
        Engines<RocksEngine, RocksEngine>,
    ) {
        let pd_client = TestPdClient::new(0, true);
        let causal_ts = Arc::new(SimpleTsoProvider::new(Arc::new(pd_client)));
        let manager = Arc::new(RegionsCausalManager::default());
        let ob = CausalObserver::new(manager.clone(), causal_ts);

        let path = tempfile::Builder::new()
            .prefix("test-causal-observer")
            .tempdir()
            .unwrap();
        let engines = new_temp_engine(&path);

        (ob, manager, engines)
    }

    #[test]
    fn test_causal_observer() {
        let (ob, manager, engines) = init();
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());

        let testcases: Vec<(u64, &[u64])> = vec![(10, &[100, 200]), (20, &[101]), (20, &[102])];

        let mut expected = RegionsMap::default();
        for (region_id, ts_list) in testcases.into_iter() {
            let mut cmd_req = RaftCmdRequest::default();

            for ts in ts_list {
                expected
                    .entry(region_id)
                    .and_modify(|v| v.max_ts = ts.into())
                    .or_insert_with(|| RegionCausalInfo::new(ts.into()));

                let key = APIV2::encode_raw_key(b"rkey", Some(ts.into()));
                let value = b"value".to_vec();
                let mut req = Request::default();
                req.set_cmd_type(CmdType::Put);
                req.mut_put().set_key(key.into_encoded());
                req.mut_put().set_value(value);

                cmd_req.mut_requests().push(req);
            }

            let cmd = Cmd::new(0, cmd_req, RaftCmdResponse::default());
            let mut cb = CmdBatch::new(&observe_info, region_id);
            cb.push(&observe_info, region_id, cmd);

            <CausalObserver as CmdObserver<RocksEngine>>::on_flush_applied_cmd_batch(
                &ob,
                ObserveLevel::All,
                &mut vec![cb],
                &engines.kv,
            );
        }

        for (k, v) in expected {
            assert_eq!(v.max_ts, manager.max_ts(k));
        }
    }
}
