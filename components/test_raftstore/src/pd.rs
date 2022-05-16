// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    collections::{
        BTreeMap,
        Bound::{Excluded, Unbounded},
    },
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use collections::{HashMap, HashMapEntry, HashSet};
use fail::fail_point;
use futures::{
    channel::mpsc::{self, UnboundedReceiver, UnboundedSender},
    compat::Future01CompatExt,
    executor::block_on,
    future::{err, ok, ready, BoxFuture, FutureExt},
    stream,
    stream::StreamExt,
};
use keys::{self, data_key, enc_end_key, enc_start_key};
use kvproto::{
    metapb::{self, PeerRole},
    pdpb,
    replication_modepb::{
        DrAutoSyncState, RegionReplicationStatus, ReplicationMode, ReplicationStatus,
        StoreDrAutoSyncStatus,
    },
};
use pd_client::{
    BucketStat, Error, FeatureGate, Key, PdClient, PdFuture, RegionInfo, RegionStat, Result,
};
use raft::eraftpb::ConfChangeType;
use raftstore::store::{
    util::{check_key_in_region, find_peer, is_learner},
    QueryStats, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
};
use tikv_util::{
    time::{Instant, UnixSecs},
    timer::GLOBAL_TIMER_HANDLE,
    Either, HandyRwLock,
};
use tokio_timer::timer::Handle;
use txn_types::TimeStamp;

use super::*;

struct Store {
    store: metapb::Store,
    region_ids: HashSet<u64>,
    sender: UnboundedSender<pdpb::RegionHeartbeatResponse>,
    receiver: Option<UnboundedReceiver<pdpb::RegionHeartbeatResponse>>,
}

impl Default for Store {
    fn default() -> Store {
        let (tx, rx) = mpsc::unbounded();
        Store {
            store: Default::default(),
            region_ids: Default::default(),
            sender: tx,
            receiver: Some(rx),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SchedulePolicy {
    /// Repeat an Operator.
    Repeat(isize),
    /// Repeat till succcess.
    TillSuccess,
    /// Stop immediately.
    Stop,
}

impl SchedulePolicy {
    fn schedule(&mut self) -> bool {
        match *self {
            SchedulePolicy::Repeat(ref mut c) => {
                if *c > 0 {
                    *c -= 1;
                    true
                } else {
                    false
                }
            }
            SchedulePolicy::TillSuccess => true,
            SchedulePolicy::Stop => false,
        }
    }
}

#[derive(Clone, Debug)]
enum Operator {
    AddPeer {
        // Left: to be added.
        // Right: pending peer.
        peer: Either<metapb::Peer, metapb::Peer>,
        policy: SchedulePolicy,
    },
    RemovePeer {
        peer: metapb::Peer,
        policy: SchedulePolicy,
    },
    TransferLeader {
        peer: metapb::Peer,
        peers: Vec<metapb::Peer>,
        policy: SchedulePolicy,
    },
    MergeRegion {
        source_region_id: u64,
        target_region_id: u64,
        policy: Arc<RwLock<SchedulePolicy>>,
    },
    SplitRegion {
        region_epoch: metapb::RegionEpoch,
        policy: pdpb::CheckPolicy,
        keys: Vec<Vec<u8>>,
    },
    LeaveJoint {
        policy: SchedulePolicy,
    },
    JointConfChange {
        add_peers: Vec<metapb::Peer>,
        to_add_peers: Vec<metapb::Peer>,
        remove_peers: Vec<metapb::Peer>,
        policy: SchedulePolicy,
    },
}

fn change_peer(change_type: ConfChangeType, peer: metapb::Peer) -> pdpb::ChangePeer {
    let mut cp = pdpb::ChangePeer::default();
    cp.set_change_type(change_type);
    cp.set_peer(peer);
    cp
}

impl Operator {
    fn make_region_heartbeat_response(
        &self,
        region_id: u64,
        cluster: &PdCluster,
    ) -> pdpb::RegionHeartbeatResponse {
        match *self {
            Operator::AddPeer { ref peer, .. } => {
                if let Either::Left(ref peer) = *peer {
                    let conf_change_type = if is_learner(peer) {
                        ConfChangeType::AddLearnerNode
                    } else {
                        ConfChangeType::AddNode
                    };
                    new_pd_change_peer(conf_change_type, peer.clone())
                } else {
                    pdpb::RegionHeartbeatResponse::default()
                }
            }
            Operator::RemovePeer { ref peer, .. } => {
                new_pd_change_peer(ConfChangeType::RemoveNode, peer.clone())
            }
            Operator::TransferLeader {
                ref peer,
                ref peers,
                ..
            } => new_pd_transfer_leader(peer.clone(), peers.clone()),
            Operator::MergeRegion {
                target_region_id, ..
            } => {
                if target_region_id == region_id {
                    pdpb::RegionHeartbeatResponse::default()
                } else {
                    let region = cluster.get_region_by_id(target_region_id).unwrap().unwrap();
                    if cluster.check_merge_target_integrity {
                        let mut all_exist = true;
                        for peer in region.get_peers() {
                            if cluster.pending_peers.contains_key(&peer.get_id()) {
                                all_exist = false;
                                break;
                            }
                        }
                        if all_exist {
                            new_pd_merge_region(region)
                        } else {
                            pdpb::RegionHeartbeatResponse::default()
                        }
                    } else {
                        new_pd_merge_region(region)
                    }
                }
            }
            Operator::SplitRegion {
                policy, ref keys, ..
            } => new_split_region(policy, keys.clone()),
            Operator::LeaveJoint { .. } => new_pd_change_peer_v2(vec![]),
            Operator::JointConfChange {
                ref to_add_peers,
                ref remove_peers,
                ..
            } => {
                let mut cps = Vec::with_capacity(to_add_peers.len() + remove_peers.len());
                for peer in to_add_peers.iter() {
                    let conf_change_type = if is_learner(peer) {
                        ConfChangeType::AddLearnerNode
                    } else {
                        ConfChangeType::AddNode
                    };
                    cps.push(change_peer(conf_change_type, peer.clone()));
                }
                for peer in remove_peers.iter() {
                    cps.push(change_peer(ConfChangeType::RemoveNode, peer.clone()));
                }
                new_pd_change_peer_v2(cps)
            }
        }
    }

    fn try_finished(
        &mut self,
        cluster: &PdCluster,
        region: &metapb::Region,
        leader: &metapb::Peer,
    ) -> bool {
        match *self {
            Operator::AddPeer {
                ref mut peer,
                ref mut policy,
            } => {
                if !policy.schedule() {
                    return true;
                }
                let pr = peer.clone();
                if let Either::Left(pr) = pr {
                    if region.get_peers().iter().any(|p| p == &pr) {
                        // TiKV is adding the peer right now,
                        // set it to Right so it will not be scheduled again.
                        *peer = Either::Right(pr);
                    } else {
                        // TiKV rejects AddNode.
                        return false;
                    }
                }
                if let Either::Right(ref pr) = *peer {
                    // Still adding peer?
                    return !cluster.pending_peers.contains_key(&pr.get_id());
                }
                unreachable!()
            }
            Operator::SplitRegion {
                ref region_epoch, ..
            } => region.get_region_epoch() != region_epoch,
            Operator::RemovePeer {
                ref peer,
                ref mut policy,
            } => region.get_peers().iter().all(|p| p != peer) || !policy.schedule(),
            Operator::TransferLeader {
                ref peer,
                ref peers,
                ref mut policy,
            } => leader == peer || peers.iter().any(|peer| leader == peer) || !policy.schedule(),
            Operator::MergeRegion {
                source_region_id,
                ref mut policy,
                ..
            } => {
                if cluster
                    .get_region_by_id(source_region_id)
                    .unwrap()
                    .is_none()
                {
                    *policy.write().unwrap() = SchedulePolicy::Stop;
                    false
                } else {
                    !policy.write().unwrap().schedule()
                }
            }
            Operator::LeaveJoint { ref mut policy } => {
                region.get_peers().iter().all(|p| {
                    p.get_role() != PeerRole::IncomingVoter
                        && p.get_role() != PeerRole::DemotingVoter
                }) || !policy.schedule()
            }
            Operator::JointConfChange {
                ref add_peers,
                ref remove_peers,
                ref mut policy,
                ..
            } => {
                let add = add_peers
                    .iter()
                    .all(|peer| region.get_peers().iter().any(|p| p == peer));

                let remove = remove_peers
                    .iter()
                    .all(|peer| region.get_peers().iter().all(|p| p != peer));

                add && remove || !policy.schedule()
            }
        }
    }
}

struct PdCluster {
    meta: metapb::Cluster,
    stores: HashMap<u64, Store>,
    regions: BTreeMap<Key, metapb::Region>,
    region_id_keys: HashMap<u64, Key>,
    region_approximate_size: HashMap<u64, u64>,
    region_approximate_keys: HashMap<u64, u64>,
    region_last_report_ts: HashMap<u64, UnixSecs>,
    region_last_report_term: HashMap<u64, u64>,
    base_id: AtomicUsize,
    buckets: HashMap<u64, BucketStat>,

    store_stats: HashMap<u64, pdpb::StoreStats>,
    store_hotspots: HashMap<u64, HashMap<u64, pdpb::PeerStat>>,
    split_count: usize,

    // region id -> Operator
    operators: HashMap<u64, Operator>,
    enable_peer_count_check: bool,

    // region id -> leader
    leaders: HashMap<u64, metapb::Peer>,
    down_peers: HashMap<u64, pdpb::PeerStats>,
    pending_peers: HashMap<u64, metapb::Peer>,
    is_bootstraped: bool,

    gc_safe_point: u64,
    min_resolved_ts: u64,

    replication_status: Option<ReplicationStatus>,
    region_replication_status: HashMap<u64, RegionReplicationStatus>,

    // for merging
    pub check_merge_target_integrity: bool,

    unsafe_recovery_require_report: bool,
    unsafe_recovery_store_reported: HashMap<u64, i32>,
}

impl PdCluster {
    fn new(cluster_id: u64) -> PdCluster {
        let mut meta = metapb::Cluster::default();
        meta.set_id(cluster_id);
        meta.set_max_peer_count(5);

        PdCluster {
            meta,
            stores: HashMap::default(),
            regions: BTreeMap::new(),
            region_id_keys: HashMap::default(),
            region_approximate_size: HashMap::default(),
            region_approximate_keys: HashMap::default(),
            region_last_report_ts: HashMap::default(),
            region_last_report_term: HashMap::default(),
            base_id: AtomicUsize::new(1000),
            store_stats: HashMap::default(),
            store_hotspots: HashMap::default(),
            split_count: 0,
            operators: HashMap::default(),
            enable_peer_count_check: true,
            leaders: HashMap::default(),
            down_peers: HashMap::default(),
            pending_peers: HashMap::default(),
            is_bootstraped: false,

            gc_safe_point: 0,
            min_resolved_ts: 0,
            replication_status: None,
            region_replication_status: HashMap::default(),
            check_merge_target_integrity: true,
            unsafe_recovery_require_report: false,
            unsafe_recovery_store_reported: HashMap::default(),
            buckets: HashMap::default(),
        }
    }

    fn bootstrap(&mut self, store: metapb::Store, region: metapb::Region) {
        // Now, some tests use multi peers in bootstrap,
        // disable this check.
        // TODO: enable this check later.
        // assert_eq!(region.get_peers().len(), 1);
        let store_id = store.get_id();
        let mut s = Store {
            store,
            ..Default::default()
        };

        s.region_ids.insert(region.get_id());

        self.stores.insert(store_id, s);

        self.add_region(&region);
        self.is_bootstraped = true;
    }

    fn set_bootstrap(&mut self, is_bootstraped: bool) {
        self.is_bootstraped = is_bootstraped
    }

    // We don't care cluster id here, so any value like 0 in tests is ok.
    fn alloc_id(&self) -> Result<u64> {
        Ok(self.base_id.fetch_add(1, Ordering::Relaxed) as u64)
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<()> {
        let store_id = store.get_id();
        // There is a race between put_store and handle_region_heartbeat_response. If store id is
        // 0, it means it's a placeholder created by latter, we just need to update the meta.
        // Otherwise we should overwrite it.
        if self
            .stores
            .get(&store_id)
            .map_or(true, |s| s.store.get_id() != 0)
        {
            self.stores.insert(
                store_id,
                Store {
                    store,
                    ..Default::default()
                },
            );
        } else {
            self.stores.get_mut(&store_id).unwrap().store = store;
        }
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        match self.stores.get(&store_id) {
            Some(s) if s.store.get_id() != 0 => Ok(s.store.clone()),
            _ => Err(box_err!("store {} not found", store_id)),
        }
    }

    fn get_all_stores(&self) -> Result<Vec<metapb::Store>> {
        Ok(self
            .stores
            .values()
            .filter_map(|s| {
                if s.store.get_id() != 0 {
                    Some(s.store.clone())
                } else {
                    None
                }
            })
            .collect())
    }

    fn get_region(&self, key: Vec<u8>) -> Option<metapb::Region> {
        self.regions
            .range((Excluded(key), Unbounded))
            .next()
            .map(|(_, region)| region.clone())
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        Ok(self
            .region_id_keys
            .get(&region_id)
            .and_then(|k| self.regions.get(k).cloned()))
    }

    fn get_region_approximate_size(&self, region_id: u64) -> Option<u64> {
        self.region_approximate_size.get(&region_id).cloned()
    }

    fn get_region_approximate_keys(&self, region_id: u64) -> Option<u64> {
        self.region_approximate_keys.get(&region_id).cloned()
    }

    fn get_region_last_report_ts(&self, region_id: u64) -> Option<UnixSecs> {
        self.region_last_report_ts.get(&region_id).cloned()
    }

    fn get_region_last_report_term(&self, region_id: u64) -> Option<u64> {
        self.region_last_report_term.get(&region_id).cloned()
    }

    fn get_store_hotspots(&self, store_id: u64) -> Option<HashMap<u64, pdpb::PeerStat>> {
        self.store_hotspots.get(&store_id).cloned()
    }

    fn get_stores(&self) -> Vec<metapb::Store> {
        self.stores
            .values()
            .filter(|s| s.store.get_id() != 0)
            .map(|s| s.store.clone())
            .collect()
    }

    fn get_regions_number(&self) -> usize {
        self.regions.len()
    }

    fn add_region(&mut self, region: &metapb::Region) {
        let end_key = enc_end_key(region);
        assert!(
            self.regions
                .insert(end_key.clone(), region.clone())
                .is_none()
        );
        assert!(
            self.region_id_keys
                .insert(region.get_id(), end_key)
                .is_none()
        );
    }

    fn remove_region(&mut self, region: &metapb::Region) {
        let end_key = enc_end_key(region);
        assert!(self.regions.remove(&end_key).is_some());
        assert!(self.region_id_keys.remove(&region.get_id()).is_some());
    }

    fn get_overlap(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Vec<metapb::Region> {
        self.regions
            .range((Excluded(start_key), Unbounded))
            .map(|(_, r)| r.clone())
            .take_while(|exist_region| end_key > enc_start_key(exist_region))
            .collect()
    }

    fn check_put_region(&mut self, region: metapb::Region) -> Result<Vec<metapb::Region>> {
        let (start_key, end_key, incoming_epoch) = (
            enc_start_key(&region),
            enc_end_key(&region),
            region.get_region_epoch().clone(),
        );
        assert!(end_key > start_key);
        let created_by_unsafe_recover = (!start_key.is_empty() || !end_key.is_empty())
            && incoming_epoch.get_version() == 1
            && incoming_epoch.get_conf_ver() == 1;
        let overlaps = self.get_overlap(start_key, end_key);
        if created_by_unsafe_recover {
            // Allow recreated region by unsafe recover to overwrite other regions with a "older"
            // epoch.
            return Ok(overlaps);
        }
        for r in overlaps.iter() {
            if incoming_epoch.get_version() < r.get_region_epoch().get_version() {
                return Err(box_err!("epoch {:?} is stale.", incoming_epoch));
            }
        }
        if let Some(o) = self.get_region_by_id(region.get_id())? {
            let exist_epoch = o.get_region_epoch();
            if incoming_epoch.get_version() < exist_epoch.get_version()
                || incoming_epoch.get_conf_ver() < exist_epoch.get_conf_ver()
            {
                return Err(box_err!("epoch {:?} is stale.", incoming_epoch));
            }
        }
        Ok(overlaps)
    }

    fn handle_heartbeat(
        &mut self,
        mut region: metapb::Region,
        leader: metapb::Peer,
    ) -> Result<pdpb::RegionHeartbeatResponse> {
        let overlaps = self.check_put_region(region.clone())?;
        let same_region = {
            let (ver, conf_ver, start_key, end_key) = (
                region.get_region_epoch().get_version(),
                region.get_region_epoch().get_conf_ver(),
                region.get_start_key(),
                region.get_end_key(),
            );
            overlaps.len() == 1
                && overlaps[0].get_id() == region.get_id()
                && overlaps[0].get_region_epoch().get_version() == ver
                && overlaps[0].get_region_epoch().get_conf_ver() == conf_ver
                && overlaps[0].get_start_key() == start_key
                && overlaps[0].get_end_key() == end_key
        };
        if !same_region {
            debug!("region changed"; "from" => ?overlaps, "to" => ?region, "leader" => ?leader);
            // remove overlap regions
            for r in overlaps {
                self.remove_region(&r);
            }
            // remove stale region that have same id but different key range
            if let Some(o) = self.get_region_by_id(region.get_id())? {
                self.remove_region(&o);
            }
            self.add_region(&region);
        }
        let resp = self
            .poll_heartbeat_responses(region.clone(), leader.clone())
            .unwrap_or_else(|| {
                let mut resp = pdpb::RegionHeartbeatResponse::default();
                resp.set_region_id(region.get_id());
                resp.set_region_epoch(region.take_region_epoch());
                resp.set_target_peer(leader);
                resp
            });
        Ok(resp)
    }

    // max_peer_count check, the default operator for handling region heartbeat.
    fn handle_heartbeat_max_peer_count(
        &mut self,
        region: &metapb::Region,
        leader: &metapb::Peer,
    ) -> Option<Operator> {
        let max_peer_count = self.meta.get_max_peer_count() as usize;
        let peer_count = region.get_peers().len();
        match peer_count.cmp(&max_peer_count) {
            cmp::Ordering::Less => {
                // find the first store which the region has not covered.
                for store_id in self.stores.keys() {
                    if region
                        .get_peers()
                        .iter()
                        .all(|x| x.get_store_id() != *store_id)
                    {
                        let peer = Either::Left(new_peer(*store_id, self.alloc_id().unwrap()));
                        let policy = SchedulePolicy::Repeat(1);
                        return Some(Operator::AddPeer { peer, policy });
                    }
                }
            }
            cmp::Ordering::Greater => {
                // find the first peer which not leader.
                let pos = region
                    .get_peers()
                    .iter()
                    .position(|x| x.get_store_id() != leader.get_store_id())
                    .unwrap();
                let peer = region.get_peers()[pos].clone();
                let policy = SchedulePolicy::Repeat(1);
                return Some(Operator::RemovePeer { peer, policy });
            }
            _ => {}
        }

        None
    }

    fn poll_heartbeat_responses_for(
        &mut self,
        store_id: u64,
    ) -> Vec<pdpb::RegionHeartbeatResponse> {
        let mut resps = vec![];
        for (region_id, leader) in self.leaders.clone() {
            if leader.get_store_id() != store_id {
                continue;
            }
            if let Ok(Some(region)) = self.get_region_by_id(region_id) {
                if let Some(resp) = self.poll_heartbeat_responses(region, leader) {
                    resps.push(resp);
                }
            }
        }

        resps
    }

    fn poll_heartbeat_responses(
        &mut self,
        mut region: metapb::Region,
        leader: metapb::Peer,
    ) -> Option<pdpb::RegionHeartbeatResponse> {
        let region_id = region.get_id();
        let mut operator = None;
        if let Some(mut op) = self.operators.remove(&region_id) {
            if !op.try_finished(self, &region, &leader) {
                operator = Some(op);
            };
        } else if self.enable_peer_count_check {
            // There is no on-going operator, start next round.
            operator = self.handle_heartbeat_max_peer_count(&region, &leader);
        }

        let operator = operator?;
        debug!(
            "[region {}] schedule {:?} to {:?}, region: {:?}",
            region_id, operator, leader, region
        );

        let mut resp = operator.make_region_heartbeat_response(region.get_id(), self);
        self.operators.insert(region_id, operator);
        resp.set_region_id(region_id);
        resp.set_region_epoch(region.take_region_epoch());
        resp.set_target_peer(leader);
        Some(resp)
    }

    fn region_heartbeat(
        &mut self,
        term: u64,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
    ) -> Result<pdpb::RegionHeartbeatResponse> {
        for peer in region.get_peers() {
            self.down_peers.remove(&peer.get_id());
            self.pending_peers.remove(&peer.get_id());
        }
        for peer in region_stat.down_peers {
            self.down_peers.insert(peer.get_peer().get_id(), peer);
        }
        for p in region_stat.pending_peers {
            self.pending_peers.insert(p.get_id(), p);
        }
        self.leaders.insert(region.get_id(), leader.clone());

        self.region_approximate_size
            .insert(region.get_id(), region_stat.approximate_size);
        self.region_approximate_keys
            .insert(region.get_id(), region_stat.approximate_keys);
        self.region_last_report_ts
            .insert(region.get_id(), region_stat.last_report_ts);
        self.region_last_report_term.insert(region.get_id(), term);

        if let Some(status) = replication_status {
            self.region_replication_status.insert(region.id, status);
        }
        fail_point!("test_raftstore::pd::region_heartbeat");

        self.handle_heartbeat(region, leader)
    }

    fn set_gc_safe_point(&mut self, safe_point: u64) {
        self.gc_safe_point = safe_point;
    }

    fn get_gc_safe_point(&self) -> u64 {
        self.gc_safe_point
    }

    fn set_min_resolved_ts(&mut self, min_resolved_ts: u64) {
        self.min_resolved_ts = min_resolved_ts;
    }

    fn get_min_resolved_ts(&self) -> u64 {
        self.min_resolved_ts
    }

    fn handle_store_heartbeat(&mut self) -> Result<pdpb::StoreHeartbeatResponse> {
        let mut resp = pdpb::StoreHeartbeatResponse::default();
        resp.set_require_detailed_report(self.unsafe_recovery_require_report);
        self.unsafe_recovery_require_report = false;

        Ok(resp)
    }

    fn set_require_report(&mut self, require_report: bool) {
        self.unsafe_recovery_require_report = require_report;
    }

    fn get_store_reported(&self, store_id: &u64) -> i32 {
        *self
            .unsafe_recovery_store_reported
            .get(store_id)
            .unwrap_or(&0)
    }

    fn store_reported_inc(&mut self, store_id: u64) {
        let reported = self
            .unsafe_recovery_store_reported
            .entry(store_id)
            .or_insert(0);
        *reported += 1;
    }
}

fn check_stale_region(region: &metapb::Region, check_region: &metapb::Region) -> Result<()> {
    let epoch = region.get_region_epoch();
    let check_epoch = check_region.get_region_epoch();

    if check_epoch.get_version() < epoch.get_version()
        || check_epoch.get_conf_ver() < epoch.get_conf_ver()
    {
        return Err(box_err!(
            "epoch not match {:?}, we are now {:?}",
            check_epoch,
            epoch
        ));
    }
    Ok(())
}

// For test when a node is already bootstraped the cluster with the first region
pub fn bootstrap_with_first_region(pd_client: Arc<TestPdClient>) -> Result<()> {
    let mut region = metapb::Region::default();
    region.set_id(1);
    region.set_start_key(keys::EMPTY_KEY.to_vec());
    region.set_end_key(keys::EMPTY_KEY.to_vec());
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    let peer = new_peer(1, 1);
    region.mut_peers().push(peer);
    pd_client.add_region(&region);
    pd_client.set_bootstrap(true);
    Ok(())
}

pub struct TestPdClient {
    cluster_id: u64,
    cluster: Arc<RwLock<PdCluster>>,
    timer: Handle,
    is_incompatible: bool,
    tso: AtomicU64,
    trigger_tso_failure: AtomicBool,
    feature_gate: FeatureGate,
    trigger_leader_info_loss: AtomicBool,
}

impl TestPdClient {
    pub fn new(cluster_id: u64, is_incompatible: bool) -> TestPdClient {
        let feature_gate = FeatureGate::default();
        // For easy testing, most cases don't test upgrading.
        feature_gate.set_version("999.0.0").unwrap();
        TestPdClient {
            cluster_id,
            cluster: Arc::new(RwLock::new(PdCluster::new(cluster_id))),
            timer: GLOBAL_TIMER_HANDLE.clone(),
            is_incompatible,
            tso: AtomicU64::new(1),
            trigger_tso_failure: AtomicBool::new(false),
            trigger_leader_info_loss: AtomicBool::new(false),
            feature_gate,
        }
    }

    pub fn get_stores(&self) -> Result<Vec<metapb::Store>> {
        Ok(self.cluster.rl().get_stores())
    }

    fn check_bootstrap(&self) -> Result<()> {
        if !self.is_cluster_bootstrapped().unwrap() {
            return Err(Error::ClusterNotBootstrapped(self.cluster_id));
        }

        Ok(())
    }

    fn is_regions_empty(&self) -> bool {
        self.cluster.rl().regions.is_empty()
    }

    fn schedule_operator(&self, region_id: u64, op: Operator) {
        let mut cluster = self.cluster.wl();
        match cluster.operators.entry(region_id) {
            HashMapEntry::Occupied(mut e) => {
                debug!(
                    "[region {}] schedule operator {:?} and remove {:?}",
                    region_id,
                    op,
                    e.get()
                );
                e.insert(op);
            }
            HashMapEntry::Vacant(e) => {
                debug!("[region {}] schedule operator {:?}", region_id, op);
                e.insert(op);
            }
        }
    }

    pub fn get_region_epoch(&self, region_id: u64) -> metapb::RegionEpoch {
        block_on(self.get_region_by_id(region_id))
            .unwrap()
            .unwrap()
            .take_region_epoch()
    }

    pub fn get_regions_number(&self) -> usize {
        self.cluster.rl().get_regions_number()
    }

    pub fn disable_default_operator(&self) {
        self.cluster.wl().enable_peer_count_check = false;
    }

    pub fn enable_default_operator(&self) {
        self.cluster.wl().enable_peer_count_check = true;
    }

    pub fn must_have_peer(&self, region_id: u64, peer: metapb::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            let region = match block_on(self.get_region_by_id(region_id)).unwrap() {
                Some(region) => region,
                None => continue,
            };

            if let Some(p) = find_peer(&region, peer.get_store_id()) {
                if p == &peer {
                    return;
                }
            }
        }
        let region = block_on(self.get_region_by_id(region_id)).unwrap();
        panic!("region {:?} has no peer {:?}", region, peer);
    }

    pub fn must_none_peer(&self, region_id: u64, peer: metapb::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            let region = match block_on(self.get_region_by_id(region_id)).unwrap() {
                Some(region) => region,
                None => continue,
            };
            match find_peer(&region, peer.get_store_id()) {
                None => return,
                Some(p) if p != &peer => return,
                _ => continue,
            }
        }
        let region = block_on(self.get_region_by_id(region_id)).unwrap();
        panic!("region {:?} has peer {:?}", region, peer);
    }

    pub fn must_none_pending_peer(&self, peer: metapb::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            if self.cluster.rl().pending_peers.contains_key(&peer.get_id()) {
                continue;
            }
            return;
        }
        panic!("peer {:?} shouldn't be pending any more", peer);
    }

    pub fn must_finish_joint_confchange(
        &self,
        region_id: u64,
        add_peers: Vec<metapb::Peer>,
        remove_peers: Vec<metapb::Peer>,
    ) {
        for _ in 1..500 {
            sleep_ms(10);
            let region = match block_on(self.get_region_by_id(region_id)).unwrap() {
                Some(region) => region,
                None => continue,
            };
            let add = add_peers
                .iter()
                .all(|peer| find_peer(&region, peer.get_store_id()).map_or(false, |p| p == peer));
            let remove = remove_peers
                .iter()
                .all(|peer| find_peer(&region, peer.get_store_id()).map_or(true, |p| p != peer));
            if add && remove {
                return;
            }
        }
        let region = block_on(self.get_region_by_id(region_id)).unwrap();
        panic!(
            "region {:?} did not apply joint confchange, add peers: {:?}, remove peers: {:?}",
            region, add_peers, remove_peers
        );
    }

    pub fn must_not_in_joint(&self, region_id: u64) {
        for _ in 1..500 {
            sleep_ms(10);
            let region = match block_on(self.get_region_by_id(region_id)).unwrap() {
                Some(region) => region,
                None => continue,
            };
            let in_joint = region.get_peers().iter().any(|p| {
                p.get_role() == PeerRole::IncomingVoter || p.get_role() == PeerRole::DemotingVoter
            });
            if !in_joint {
                return;
            }
        }
        let region = block_on(self.get_region_by_id(region_id)).unwrap();
        panic!("region {:?} failed to leave joint", region);
    }

    pub fn add_region(&self, region: &metapb::Region) {
        self.cluster.wl().add_region(region)
    }

    pub fn transfer_leader(&self, region_id: u64, peer: metapb::Peer, peers: Vec<metapb::Peer>) {
        let op = Operator::TransferLeader {
            peer,
            peers,
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(region_id, op);
    }

    pub fn add_peer(&self, region_id: u64, peer: metapb::Peer) {
        let op = Operator::AddPeer {
            peer: Either::Left(peer),
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(region_id, op);
    }

    pub fn remove_peer(&self, region_id: u64, peer: metapb::Peer) {
        let op = Operator::RemovePeer {
            peer,
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(region_id, op);
    }

    pub fn joint_confchange(
        &self,
        region_id: u64,
        mut changes: Vec<(ConfChangeType, metapb::Peer)>,
    ) -> (Vec<metapb::Peer>, Vec<metapb::Peer>) {
        let region = block_on(self.get_region_by_id(region_id)).unwrap().unwrap();
        let (mut add_peers, mut remove_peers) = (Vec::new(), Vec::new());

        let to_add_peers = changes
            .iter()
            .filter(|(c, _)| *c != ConfChangeType::RemoveNode)
            .map(|(_, p)| p)
            .cloned()
            .collect();

        // Simple confchange
        if changes.len() == 1 {
            match changes.pop().unwrap() {
                (ConfChangeType::RemoveNode, p) => remove_peers.push(p),
                (_, p) => add_peers.push(p),
            }
        } else {
            // Joint confchange
            for (change, mut peer) in changes {
                match (
                    find_peer(&region, peer.get_store_id()).map(|p| p.get_role()),
                    change,
                ) {
                    (None, ConfChangeType::AddNode) => {
                        peer.set_role(PeerRole::IncomingVoter);
                        add_peers.push(peer);
                    }
                    (Some(PeerRole::Voter), ConfChangeType::AddLearnerNode) => {
                        peer.set_role(PeerRole::DemotingVoter);
                        add_peers.push(peer);
                    }
                    (Some(PeerRole::Learner), ConfChangeType::AddNode) => {
                        peer.set_role(PeerRole::IncomingVoter);
                        add_peers.push(peer);
                    }
                    (_, ConfChangeType::RemoveNode) => remove_peers.push(peer),
                    _ => add_peers.push(peer),
                }
            }
        }
        let op = Operator::JointConfChange {
            add_peers: add_peers.clone(),
            to_add_peers,
            remove_peers: remove_peers.clone(),
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(region_id, op);
        (add_peers, remove_peers)
    }

    pub fn leave_joint(&self, region_id: u64) {
        let op = Operator::LeaveJoint {
            policy: SchedulePolicy::TillSuccess,
        };
        self.schedule_operator(region_id, op);
    }

    pub fn is_in_joint(&self, region_id: u64) -> bool {
        let region = block_on(self.get_region_by_id(region_id))
            .unwrap()
            .expect("region not exist");
        region.get_peers().iter().any(|p| {
            p.get_role() == PeerRole::IncomingVoter || p.get_role() == PeerRole::DemotingVoter
        })
    }

    pub fn split_region(
        &self,
        mut region: metapb::Region,
        policy: pdpb::CheckPolicy,
        keys: Vec<Vec<u8>>,
    ) {
        let op = Operator::SplitRegion {
            region_epoch: region.take_region_epoch(),
            policy,
            keys,
        };
        self.schedule_operator(region.get_id(), op);
    }

    pub fn must_split_region(
        &self,
        region: metapb::Region,
        policy: pdpb::CheckPolicy,
        keys: Vec<Vec<u8>>,
    ) {
        let expect_region_count = self.get_regions_number()
            + if policy == pdpb::CheckPolicy::Usekey {
                keys.len()
            } else {
                1
            };
        self.split_region(region.clone(), policy, keys);
        for _ in 1..500 {
            sleep_ms(10);
            if self.get_regions_number() == expect_region_count {
                return;
            }
        }
        panic!("region {:?} is still not split.", region);
    }

    pub fn must_add_peer(&self, region_id: u64, peer: metapb::Peer) {
        self.add_peer(region_id, peer.clone());
        self.must_have_peer(region_id, peer);
    }

    pub fn must_remove_peer(&self, region_id: u64, peer: metapb::Peer) {
        self.remove_peer(region_id, peer.clone());
        self.must_none_peer(region_id, peer);
    }

    pub fn must_joint_confchange(
        &self,
        region_id: u64,
        changes: Vec<(ConfChangeType, metapb::Peer)>,
    ) {
        let (add, remove) = self.joint_confchange(region_id, changes);
        self.must_finish_joint_confchange(region_id, add, remove);
    }

    pub fn must_leave_joint(&self, region_id: u64) {
        self.leave_joint(region_id);
        self.must_not_in_joint(region_id);
    }

    pub fn merge_region(&self, from: u64, target: u64) {
        let op = Operator::MergeRegion {
            source_region_id: from,
            target_region_id: target,
            policy: Arc::new(RwLock::new(SchedulePolicy::TillSuccess)),
        };
        self.schedule_operator(from, op.clone());
        self.schedule_operator(target, op);
    }

    pub fn must_merge(&self, from: u64, target: u64) {
        self.merge_region(from, target);

        self.check_merged_timeout(from, Duration::from_secs(5));
    }

    pub fn check_merged(&self, from: u64) -> bool {
        block_on(self.get_region_by_id(from)).unwrap().is_none()
    }

    pub fn check_merged_timeout(&self, from: u64, duration: Duration) {
        let timer = Instant::now();
        loop {
            let region = block_on(self.get_region_by_id(from)).unwrap();
            if let Some(r) = region {
                if timer.saturating_elapsed() > duration {
                    panic!("region {:?} is still not merged.", r);
                }
            } else {
                return;
            }
            sleep_ms(10);
        }
    }

    pub fn region_leader_must_be(&self, region_id: u64, peer: metapb::Peer) {
        for _ in 0..500 {
            sleep_ms(10);
            if let Some(p) = self.cluster.rl().leaders.get(&region_id) {
                if *p == peer {
                    return;
                }
            }
        }
        panic!("region {} must have leader: {:?}", region_id, peer);
    }

    // check whether region is split by split_key or not.
    pub fn check_split(&self, region: &metapb::Region, split_key: &[u8]) -> bool {
        // E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c)
        // use a to find new [a, b).
        // use b to find new [b, c)
        let left = match self.get_region(region.get_start_key()) {
            Err(_) => return false,
            Ok(left) => left,
        };

        if left.get_end_key() != split_key {
            return false;
        }

        let right = match self.get_region(split_key) {
            Err(_) => return false,
            Ok(right) => right,
        };

        if right.get_start_key() != split_key {
            return false;
        }
        left.get_region_epoch().get_version() > region.get_region_epoch().get_version()
            && right.get_region_epoch().get_version() > region.get_region_epoch().get_version()
    }

    pub fn get_store_stats(&self, store_id: u64) -> Option<pdpb::StoreStats> {
        self.cluster.rl().store_stats.get(&store_id).cloned()
    }

    pub fn get_split_count(&self) -> usize {
        self.cluster.rl().split_count
    }

    pub fn get_down_peers(&self) -> HashMap<u64, pdpb::PeerStats> {
        self.cluster.rl().down_peers.clone()
    }

    pub fn get_pending_peers(&self) -> HashMap<u64, metapb::Peer> {
        self.cluster.rl().pending_peers.clone()
    }

    pub fn set_bootstrap(&self, is_bootstraped: bool) {
        self.cluster.wl().set_bootstrap(is_bootstraped);
    }

    pub fn configure_dr_auto_sync(&self, label_key: &str) {
        let mut status = ReplicationStatus::default();
        status.set_mode(ReplicationMode::DrAutoSync);
        status.mut_dr_auto_sync().label_key = label_key.to_owned();
        let mut cluster = self.cluster.wl();
        status.mut_dr_auto_sync().state_id = cluster
            .replication_status
            .as_ref()
            .map(|s| s.get_dr_auto_sync().state_id + 1)
            .unwrap_or(1);
        cluster.replication_status = Some(status);
    }

    pub fn switch_replication_mode(&self, state: DrAutoSyncState, available_stores: Vec<u64>) {
        let mut cluster = self.cluster.wl();
        let status = cluster.replication_status.as_mut().unwrap();
        let mut dr = status.mut_dr_auto_sync();
        dr.state_id += 1;
        dr.set_state(state);
        dr.available_stores = available_stores;
    }

    pub fn region_replication_status(&self, region_id: u64) -> RegionReplicationStatus {
        self.cluster
            .rl()
            .region_replication_status
            .get(&region_id)
            .unwrap()
            .to_owned()
    }

    pub fn get_region_approximate_size(&self, region_id: u64) -> Option<u64> {
        self.cluster.rl().get_region_approximate_size(region_id)
    }

    pub fn get_region_approximate_keys(&self, region_id: u64) -> Option<u64> {
        self.cluster.rl().get_region_approximate_keys(region_id)
    }

    pub fn get_region_last_report_ts(&self, region_id: u64) -> Option<UnixSecs> {
        self.cluster.rl().get_region_last_report_ts(region_id)
    }

    pub fn get_region_last_report_term(&self, region_id: u64) -> Option<u64> {
        self.cluster.rl().get_region_last_report_term(region_id)
    }

    pub fn get_store_hotspots(&self, store_id: u64) -> Option<HashMap<u64, pdpb::PeerStat>> {
        self.cluster.rl().get_store_hotspots(store_id)
    }

    pub fn set_gc_safe_point(&self, safe_point: u64) {
        self.cluster.wl().set_gc_safe_point(safe_point);
    }

    pub fn get_min_resolved_ts(&self) -> u64 {
        self.cluster.rl().get_min_resolved_ts()
    }

    pub fn trigger_tso_failure(&self) {
        self.trigger_tso_failure.store(true, Ordering::SeqCst);
    }

    pub fn trigger_leader_info_loss(&self) {
        self.trigger_leader_info_loss.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_store(&self, store_id: u64) {
        match self.cluster.write() {
            Ok(mut c) => {
                c.stores.remove(&store_id);
            }
            Err(e) => {
                safe_panic!("failed to acquire write lock: {:?}", e)
            }
        }
    }

    pub fn ignore_merge_target_integrity(&self) {
        self.cluster.wl().check_merge_target_integrity = false;
    }

    /// The next generated TSO will be `ts + 1`. See `get_tso()` and `batch_get_tso()`.
    pub fn set_tso(&self, ts: TimeStamp) {
        let old = self.tso.swap(ts.into_inner(), Ordering::SeqCst);
        if old > ts.into_inner() {
            panic!(
                "cannot decrease tso. current tso: {}; trying to set to: {}",
                old, ts
            );
        }
    }

    pub fn reset_version(&self, version: &str) {
        unsafe { self.feature_gate.reset_version(version).unwrap() }
    }

    pub fn must_set_require_report(&self, require_report: bool) {
        self.cluster.wl().set_require_report(require_report);
    }

    pub fn must_get_store_reported(&self, store_id: &u64) -> i32 {
        self.cluster.rl().get_store_reported(store_id)
    }

    pub fn get_buckets(&self, region_id: u64) -> Option<BucketStat> {
        self.cluster.rl().buckets.get(&region_id).cloned()
    }
}

impl PdClient for TestPdClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(
        &self,
        store: metapb::Store,
        region: metapb::Region,
    ) -> Result<Option<ReplicationStatus>> {
        if self.is_cluster_bootstrapped().unwrap() || !self.is_regions_empty() {
            self.cluster.wl().set_bootstrap(true);
            return Err(Error::ClusterBootstrapped(self.cluster_id));
        }

        let mut cluster = self.cluster.wl();
        cluster.bootstrap(store, region);
        Ok(cluster.replication_status.clone())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        Ok(self.cluster.rl().is_bootstraped)
    }

    fn alloc_id(&self) -> Result<u64> {
        self.cluster.rl().alloc_id()
    }

    fn put_store(&self, store: metapb::Store) -> Result<Option<ReplicationStatus>> {
        self.check_bootstrap()?;
        let mut cluster = self.cluster.wl();
        cluster.put_store(store)?;
        Ok(cluster.replication_status.clone())
    }

    fn get_all_stores(&self, _exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        self.check_bootstrap()?;
        self.cluster.rl().get_all_stores()
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        self.check_bootstrap()?;
        self.cluster.rl().get_store(store_id)
    }

    fn get_store_async(&self, store_id: u64) -> PdFuture<metapb::Store> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        match self.cluster.rl().get_store(store_id) {
            Ok(store) => Box::pin(ok(store)),
            Err(e) => Box::pin(err(e)),
        }
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        self.check_bootstrap()?;

        for _ in 1..500 {
            sleep_ms(10);
            if let Some(region) = self.cluster.rl().get_region(data_key(key)) {
                if check_key_in_region(key, &region).is_ok() {
                    return Ok(region);
                }
            }
        }

        Err(box_err!(
            "no region contains key {}",
            log_wrappers::hex_encode_upper(key)
        ))
    }

    fn get_region_info(&self, key: &[u8]) -> Result<RegionInfo> {
        let region = self.get_region(key)?;
        let leader = self.cluster.rl().leaders.get(&region.get_id()).cloned();
        Ok(RegionInfo::new(region, leader))
    }

    fn get_region_by_id(&self, region_id: u64) -> PdFuture<Option<metapb::Region>> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        match self.cluster.rl().get_region_by_id(region_id) {
            Ok(resp) => Box::pin(ok(resp)),
            Err(e) => Box::pin(err(e)),
        }
    }

    fn get_region_leader_by_id(
        &self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        let cluster = self.cluster.rl();
        match cluster.get_region_by_id(region_id) {
            Ok(resp) => {
                let leader = if self.trigger_leader_info_loss.load(Ordering::SeqCst) {
                    new_peer(0, 0)
                } else {
                    cluster.leaders.get(&region_id).cloned().unwrap_or_default()
                };
                Box::pin(ok(resp.map(|r| (r, leader))))
            }
            Err(e) => Box::pin(err(e)),
        }
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        self.check_bootstrap()?;
        Ok(self.cluster.rl().meta.clone())
    }

    fn region_heartbeat(
        &self,
        term: u64,
        region: metapb::Region,
        leader: metapb::Peer,
        region_stat: RegionStat,
        replication_status: Option<RegionReplicationStatus>,
    ) -> PdFuture<()> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        let resp = self.cluster.wl().region_heartbeat(
            term,
            region,
            leader.clone(),
            region_stat,
            replication_status,
        );
        match resp {
            Ok(resp) => {
                let store_id = leader.get_store_id();
                if let Some(store) = self.cluster.wl().stores.get(&store_id) {
                    store.sender.unbounded_send(resp).unwrap();
                }
                Box::pin(ok(()))
            }
            Err(e) => Box::pin(err(e)),
        }
    }

    fn handle_region_heartbeat_response<F>(&self, store_id: u64, f: F) -> PdFuture<()>
    where
        Self: Sized,
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        let cluster1 = Arc::clone(&self.cluster);
        let timer = self.timer.clone();
        let mut cluster = self.cluster.wl();
        let store = cluster
            .stores
            .entry(store_id)
            .or_insert_with(Store::default);
        let rx = store.receiver.take().unwrap();
        let st1 = rx.map(|resp| vec![resp]);
        let st2 = stream::unfold(
            (timer, cluster1, store_id),
            |(timer, cluster1, store_id)| async move {
                timer
                    .delay(std::time::Instant::now() + Duration::from_millis(500))
                    .compat()
                    .await
                    .unwrap();
                let mut cluster = cluster1.wl();
                let resps = cluster.poll_heartbeat_responses_for(store_id);
                drop(cluster);
                Some((resps, (timer, cluster1, store_id)))
            },
        );
        Box::pin(
            stream::select(st1, st2)
                .for_each(move |resps| {
                    for resp in resps {
                        f(resp);
                    }
                    futures::future::ready(())
                })
                .map(|_| Ok(())),
        )
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        // Must ConfVer and Version be same?
        let cur_region = self
            .cluster
            .rl()
            .get_region_by_id(region.get_id())
            .unwrap()
            .unwrap();
        if let Err(e) = check_stale_region(&cur_region, &region) {
            return Box::pin(err(e));
        }

        let mut resp = pdpb::AskSplitResponse::default();
        resp.set_new_region_id(self.alloc_id().unwrap());
        let mut peer_ids = vec![];
        for _ in region.get_peers() {
            peer_ids.push(self.alloc_id().unwrap());
        }
        resp.set_new_peer_ids(peer_ids);

        Box::pin(ok(resp))
    }

    fn ask_batch_split(
        &self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        if self.is_incompatible {
            return Box::pin(err(Error::Incompatible));
        }

        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        // Must ConfVer and Version be same?
        let cur_region = self
            .cluster
            .rl()
            .get_region_by_id(region.get_id())
            .unwrap()
            .unwrap();
        if let Err(e) = check_stale_region(&cur_region, &region) {
            return Box::pin(err(e));
        }

        let mut resp = pdpb::AskBatchSplitResponse::default();
        for _ in 0..count {
            let mut id = pdpb::SplitId::default();
            id.set_new_region_id(self.alloc_id().unwrap());
            for _ in region.get_peers() {
                id.mut_new_peer_ids().push(self.alloc_id().unwrap());
            }
            resp.mut_ids().push(id);
        }

        Box::pin(ok(resp))
    }

    fn store_heartbeat(
        &self,
        stats: pdpb::StoreStats,
        report: Option<pdpb::StoreReport>,
        _: Option<StoreDrAutoSyncStatus>,
    ) -> PdFuture<pdpb::StoreHeartbeatResponse> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        // Cache it directly now.
        let store_id = stats.get_store_id();
        let mut cluster = self.cluster.wl();
        let hot_spots = cluster
            .store_hotspots
            .entry(store_id)
            .or_insert_with(HashMap::default);
        for peer_stat in stats.get_peer_stats() {
            let region_id = peer_stat.get_region_id();
            let peer_stat_sum = hot_spots
                .entry(region_id)
                .or_insert_with(pdpb::PeerStat::default);
            let read_keys = peer_stat.get_read_keys() + peer_stat_sum.get_read_keys();
            let read_bytes = peer_stat.get_read_bytes() + peer_stat_sum.get_read_bytes();
            let mut read_query_stats = QueryStats::default();
            read_query_stats.add_query_stats(peer_stat.get_query_stats());
            read_query_stats.add_query_stats(peer_stat_sum.get_query_stats());
            peer_stat_sum.set_read_keys(read_keys);
            peer_stat_sum.set_read_bytes(read_bytes);
            peer_stat_sum.set_query_stats(read_query_stats.0);
            peer_stat_sum.set_region_id(region_id);
        }

        cluster.store_stats.insert(store_id, stats);

        if report.is_some() {
            cluster.store_reported_inc(store_id);
        }

        let mut resp = cluster.handle_store_heartbeat().unwrap();

        if let Some(ref status) = cluster.replication_status {
            resp.set_replication_status(status.clone());
        }
        Box::pin(ok(resp))
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        // pd just uses this for history show, so here we just count it.
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        self.cluster.wl().split_count += regions.len() - 1;
        Box::pin(ok(()))
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }

        let safe_point = self.cluster.rl().get_gc_safe_point();
        Box::pin(ok(safe_point))
    }

    fn get_store_stats_async(&self, store_id: u64) -> BoxFuture<'_, Result<pdpb::StoreStats>> {
        let cluster = self.cluster.rl();
        let stats = cluster.store_stats.get(&store_id);
        ready(match stats {
            Some(s) => Ok(s.clone()),
            None => Err(Error::StoreTombstone(format!("store_id:{}", store_id))),
        })
        .boxed()
    }

    fn get_operator(&self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let mut header = pdpb::ResponseHeader::default();
        header.set_cluster_id(self.cluster_id);
        let mut resp = pdpb::GetOperatorResponse::default();
        resp.set_header(header);
        resp.set_region_id(region_id);
        Ok(resp)
    }

    fn batch_get_tso(&self, count: u32) -> PdFuture<TimeStamp> {
        fail_point!("test_raftstore_get_tso", |t| {
            let duration = Duration::from_millis(t.map_or(1000, |t| t.parse().unwrap()));
            Box::pin(async move {
                let _ = GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + duration)
                    .compat()
                    .await;
                Err(box_err!("get tso fail"))
            })
        });
        if self.trigger_tso_failure.swap(false, Ordering::SeqCst) {
            return Box::pin(err(pd_client::errors::Error::Grpc(
                grpcio::Error::RpcFailure(grpcio::RpcStatus::with_message(
                    grpcio::RpcStatusCode::UNKNOWN,
                    "tso error".to_owned(),
                )),
            )));
        }
        let tso = self.tso.fetch_add(count as u64, Ordering::SeqCst);
        Box::pin(ok(TimeStamp::new(tso + count as u64)))
    }

    fn update_service_safe_point(
        &self,
        _name: String,
        _safepoint: TimeStamp,
        _ttl: Duration,
    ) -> PdFuture<()> {
        Box::pin(ok(()))
    }

    fn feature_gate(&self) -> &FeatureGate {
        &self.feature_gate
    }

    fn report_min_resolved_ts(&self, _store_id: u64, min_resolved_ts: u64) -> PdFuture<()> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        self.cluster.wl().set_min_resolved_ts(min_resolved_ts);
        Box::pin(ok(()))
    }

    fn report_region_buckets(&self, buckets: &BucketStat, _period: Duration) -> PdFuture<()> {
        if let Err(e) = self.check_bootstrap() {
            return Box::pin(err(e));
        }
        let mut buckets = buckets.clone();
        self.cluster
            .wl()
            .buckets
            .entry(buckets.meta.region_id)
            .and_modify(|current| {
                if current.meta < buckets.meta {
                    std::mem::swap(current, &mut buckets);
                }

                pd_client::merge_bucket_stats(
                    &current.meta.keys,
                    &mut current.stats,
                    &buckets.meta.keys,
                    &buckets.stats,
                );
            })
            .or_insert(buckets);
        ready(Ok(())).boxed()
    }
}
