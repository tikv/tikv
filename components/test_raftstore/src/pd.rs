// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::{err, ok};
use futures::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::{Future, Stream};
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{cmp, thread};
use tokio_timer::timer::Handle;

use kvproto::metapb;
use kvproto::pdpb;
use raft::eraftpb;

use keys::{self, data_key, enc_end_key, enc_start_key};
use pd_client::{Error, Key, PdClient, PdFuture, RegionInfo, RegionStat, Result};
use raftstore::store::util::check_key_in_region;
use raftstore::store::{INIT_EPOCH_CONF_VER, INIT_EPOCH_VER};
use tikv_util::collections::{HashMap, HashMapEntry, HashSet};
use tikv_util::time::{Instant, UnixSecs};
use tikv_util::timer::GLOBAL_TIMER_HANDLE;
use tikv_util::{Either, HandyRwLock};
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
}

impl Operator {
    fn make_region_heartbeat_response(
        &self,
        region_id: u64,
        cluster: &Cluster,
    ) -> pdpb::RegionHeartbeatResponse {
        match *self {
            Operator::AddPeer { ref peer, .. } => {
                if let Either::Left(ref peer) = *peer {
                    let conf_change_type = if peer.get_is_learner() {
                        eraftpb::ConfChangeType::AddLearnerNode
                    } else {
                        eraftpb::ConfChangeType::AddNode
                    };
                    new_pd_change_peer(conf_change_type, peer.clone())
                } else {
                    pdpb::RegionHeartbeatResponse::default()
                }
            }
            Operator::RemovePeer { ref peer, .. } => {
                new_pd_change_peer(eraftpb::ConfChangeType::RemoveNode, peer.clone())
            }
            Operator::TransferLeader { ref peer, .. } => new_pd_transfer_leader(peer.clone()),
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
        }
    }

    fn try_finished(
        &mut self,
        cluster: &Cluster,
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
                ref mut policy,
            } => leader == peer || !policy.schedule(),
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
        }
    }
}

struct Cluster {
    meta: metapb::Cluster,
    stores: HashMap<u64, Store>,
    regions: BTreeMap<Key, metapb::Region>,
    region_id_keys: HashMap<u64, Key>,
    region_approximate_size: HashMap<u64, u64>,
    region_approximate_keys: HashMap<u64, u64>,
    region_last_report_ts: HashMap<u64, UnixSecs>,
    region_last_report_term: HashMap<u64, u64>,
    base_id: AtomicUsize,

    store_stats: HashMap<u64, pdpb::StoreStats>,
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

    // for merging
    pub check_merge_target_integrity: bool,
}

impl Cluster {
    fn new(cluster_id: u64) -> Cluster {
        let mut meta = metapb::Cluster::default();
        meta.set_id(cluster_id);
        meta.set_max_peer_count(5);

        Cluster {
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
            split_count: 0,
            operators: HashMap::default(),
            enable_peer_count_check: true,
            leaders: HashMap::default(),
            down_peers: HashMap::default(),
            pending_peers: HashMap::default(),
            is_bootstraped: false,

            gc_safe_point: 0,
            check_merge_target_integrity: true,
        }
    }

    fn bootstrap(&mut self, store: metapb::Store, region: metapb::Region) {
        // Now, some tests use multi peers in bootstrap,
        // disable this check.
        // TODO: enable this check later.
        // assert_eq!(region.get_peers().len(), 1);
        let store_id = store.get_id();
        let mut s = Store::default();
        s.store = store;

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
            let mut s = Store::default();
            s.store = store;
            self.stores.insert(store_id, s);
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
        assert!(self
            .regions
            .insert(end_key.clone(), region.clone())
            .is_none());
        assert!(self
            .region_id_keys
            .insert(region.get_id(), end_key)
            .is_none());
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
        let overlaps = self.get_overlap(start_key, end_key);
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
            let (ver, conf_ver) = (
                region.get_region_epoch().get_version(),
                region.get_region_epoch().get_conf_ver(),
            );
            overlaps.len() == 1
                && overlaps[0].get_id() == region.get_id()
                && overlaps[0].get_region_epoch().get_version() == ver
                && overlaps[0].get_region_epoch().get_conf_ver() == conf_ver
        };
        if !same_region {
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

        self.handle_heartbeat(region, leader)
    }

    fn set_gc_safe_point(&mut self, safe_point: u64) {
        self.gc_safe_point = safe_point;
    }

    fn get_gc_safe_point(&self) -> u64 {
        self.gc_safe_point
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
    cluster: Arc<RwLock<Cluster>>,
    timer: Handle,
    is_incompatible: bool,
    tso: AtomicUsize,
    trigger_tso_failure: AtomicBool,
}

impl TestPdClient {
    pub fn new(cluster_id: u64, is_incompatible: bool) -> TestPdClient {
        TestPdClient {
            cluster_id,
            cluster: Arc::new(RwLock::new(Cluster::new(cluster_id))),
            timer: GLOBAL_TIMER_HANDLE.clone(),
            is_incompatible,
            tso: AtomicUsize::new(1),
            trigger_tso_failure: AtomicBool::new(false),
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
        self.get_region_by_id(region_id)
            .wait()
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
            let region = match self.get_region_by_id(region_id).wait().unwrap() {
                Some(region) => region,
                None => continue,
            };

            if let Some(p) = find_peer(&region, peer.get_store_id()) {
                if p == &peer {
                    return;
                }
            }
        }
        let region = self.get_region_by_id(region_id).wait().unwrap();
        panic!("region {:?} has no peer {:?}", region, peer);
    }

    pub fn must_none_peer(&self, region_id: u64, peer: metapb::Peer) {
        for _ in 1..500 {
            sleep_ms(10);
            let region = match self.get_region_by_id(region_id).wait().unwrap() {
                Some(region) => region,
                None => continue,
            };
            match find_peer(&region, peer.get_store_id()) {
                None => return,
                Some(p) if p != &peer => return,
                _ => continue,
            }
        }
        let region = self.get_region_by_id(region_id).wait().unwrap();
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

    pub fn add_region(&self, region: &metapb::Region) {
        self.cluster.wl().add_region(region)
    }

    pub fn transfer_leader(&self, region_id: u64, peer: metapb::Peer) {
        let op = Operator::TransferLeader {
            peer,
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
        self.get_region_by_id(from).wait().unwrap().is_none()
    }

    pub fn check_merged_timeout(&self, from: u64, duration: Duration) {
        let timer = Instant::now();
        loop {
            let region = self.get_region_by_id(from).wait().unwrap();
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

    pub fn set_gc_safe_point(&self, safe_point: u64) {
        self.cluster.wl().set_gc_safe_point(safe_point);
    }

    pub fn trigger_tso_failure(&self) {
        self.trigger_tso_failure.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_store(&self, store_id: u64) {
        match self.cluster.write() {
            Ok(mut c) => {
                c.stores.remove(&store_id);
            }
            Err(e) => {
                if !thread::panicking() {
                    panic!("failed to acquire write lock: {:?}", e)
                }
            }
        }
    }

    pub fn ignore_merge_target_integrity(&self) {
        self.cluster.wl().check_merge_target_integrity = false;
    }
}

impl PdClient for TestPdClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        if self.is_cluster_bootstrapped().unwrap() || !self.is_regions_empty() {
            self.cluster.wl().set_bootstrap(true);
            return Err(Error::ClusterBootstrapped(self.cluster_id));
        }

        self.cluster.wl().bootstrap(store, region);

        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        Ok(self.cluster.rl().is_bootstraped)
    }

    fn alloc_id(&self) -> Result<u64> {
        self.cluster.rl().alloc_id()
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        self.check_bootstrap()?;
        self.cluster.wl().put_store(store)
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        self.check_bootstrap()?;
        self.cluster.rl().get_store(store_id)
    }

    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        self.check_bootstrap()?;
        if let Some(region) = self.cluster.rl().get_region(data_key(key)) {
            if check_key_in_region(key, &region).is_ok() {
                return Ok(region);
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
            return Box::new(err(e));
        }
        match self.cluster.rl().get_region_by_id(region_id) {
            Ok(resp) => Box::new(ok(resp)),
            Err(e) => Box::new(err(e)),
        }
    }

    fn get_region_leader_by_id(
        &self,
        region_id: u64,
    ) -> PdFuture<Option<(metapb::Region, metapb::Peer)>> {
        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }
        let cluster = self.cluster.rl();
        match cluster.get_region_by_id(region_id) {
            Ok(resp) => {
                let leader = cluster.leaders.get(&region_id).cloned().unwrap_or_default();
                Box::new(ok(resp.map(|r| (r, leader))))
            }
            Err(e) => Box::new(err(e)),
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
    ) -> PdFuture<()> {
        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }
        let resp = self
            .cluster
            .wl()
            .region_heartbeat(term, region, leader.clone(), region_stat);
        match resp {
            Ok(resp) => {
                let store_id = leader.get_store_id();
                if let Some(store) = self.cluster.wl().stores.get(&store_id) {
                    store.sender.unbounded_send(resp).unwrap();
                }
                Box::new(ok(()))
            }
            Err(e) => Box::new(err(e)),
        }
    }

    fn handle_region_heartbeat_response<F>(&self, store_id: u64, f: F) -> PdFuture<()>
    where
        Self: Sized,
        F: Fn(pdpb::RegionHeartbeatResponse) + Send + 'static,
    {
        use futures::stream;
        let cluster1 = Arc::clone(&self.cluster);
        let timer = self.timer.clone();
        let mut cluster = self.cluster.wl();
        let store = cluster
            .stores
            .entry(store_id)
            .or_insert_with(Store::default);
        let rx = store.receiver.take().unwrap();
        Box::new(
            rx.map(|resp| vec![resp])
                .select(
                    stream::unfold(timer, |timer| {
                        let interval =
                            timer.delay(std::time::Instant::now() + Duration::from_millis(500));
                        Some(interval.then(|_| Ok(((), timer))))
                    })
                    .map(move |_| {
                        let mut cluster = cluster1.wl();
                        cluster.poll_heartbeat_responses_for(store_id)
                    }),
                )
                .map_err(|e| box_err!("failed to receive next heartbeat response: {:?}", e))
                .for_each(move |resps| {
                    for resp in resps {
                        f(resp);
                    }
                    Ok(())
                }),
        )
    }

    fn ask_split(&self, region: metapb::Region) -> PdFuture<pdpb::AskSplitResponse> {
        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }

        // Must ConfVer and Version be same?
        let cur_region = self
            .cluster
            .rl()
            .get_region_by_id(region.get_id())
            .unwrap()
            .unwrap();
        if let Err(e) = check_stale_region(&cur_region, &region) {
            return Box::new(err(e));
        }

        let mut resp = pdpb::AskSplitResponse::default();
        resp.set_new_region_id(self.alloc_id().unwrap());
        let mut peer_ids = vec![];
        for _ in region.get_peers() {
            peer_ids.push(self.alloc_id().unwrap());
        }
        resp.set_new_peer_ids(peer_ids);

        Box::new(ok(resp))
    }

    fn ask_batch_split(
        &self,
        region: metapb::Region,
        count: usize,
    ) -> PdFuture<pdpb::AskBatchSplitResponse> {
        if self.is_incompatible {
            return Box::new(err(Error::Incompatible));
        }

        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }

        // Must ConfVer and Version be same?
        let cur_region = self
            .cluster
            .rl()
            .get_region_by_id(region.get_id())
            .unwrap()
            .unwrap();
        if let Err(e) = check_stale_region(&cur_region, &region) {
            return Box::new(err(e));
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

        Box::new(ok(resp))
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> PdFuture<()> {
        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }

        // Cache it directly now.
        let store_id = stats.get_store_id();
        self.cluster.wl().store_stats.insert(store_id, stats);

        Box::new(ok(()))
    }

    fn report_batch_split(&self, regions: Vec<metapb::Region>) -> PdFuture<()> {
        // pd just uses this for history show, so here we just count it.
        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }
        self.cluster.wl().split_count += regions.len() - 1;
        Box::new(ok(()))
    }

    fn get_gc_safe_point(&self) -> PdFuture<u64> {
        if let Err(e) = self.check_bootstrap() {
            return Box::new(err(e));
        }

        let safe_point = self.cluster.rl().get_gc_safe_point();
        Box::new(ok(safe_point))
    }

    fn get_store_stats(&self, store_id: u64) -> Result<pdpb::StoreStats> {
        let cluster = self.cluster.rl();
        let stats = cluster.store_stats.get(&store_id);
        match stats {
            Some(s) => Ok(s.clone()),
            None => Err(Error::StoreTombstone(format!("store_id:{}", store_id))),
        }
    }

    fn get_operator(&self, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        let mut header = pdpb::ResponseHeader::default();
        header.set_cluster_id(self.cluster_id);
        let mut resp = pdpb::GetOperatorResponse::default();
        resp.set_header(header);
        resp.set_region_id(region_id);
        Ok(resp)
    }

    fn get_tso(&self) -> PdFuture<TimeStamp> {
        if self.trigger_tso_failure.swap(false, Ordering::SeqCst) {
            return Box::new(futures::future::result(Err(
                pd_client::errors::Error::Grpc(grpcio::Error::RpcFailure(grpcio::RpcStatus::new(
                    grpcio::RpcStatusCode::UNKNOWN,
                    Some("tso error".to_owned()),
                ))),
            )));
        }
        let tso = self.tso.fetch_add(1, Ordering::SeqCst);
        Box::new(futures::future::result(Ok(TimeStamp::new(tso as _))))
    }
}
