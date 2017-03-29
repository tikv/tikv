// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use std::collections::{HashMap, BTreeMap, HashSet};
use std::vec::Vec;
use std::collections::Bound::{Excluded, Unbounded};
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use kvproto::metapb;
use kvproto::pdpb;
use kvproto::eraftpb;
use tikv::pd::{PdClient, Result, Error, Key};
use tikv::raftstore::store::keys::{enc_end_key, enc_start_key, data_key};
use tikv::raftstore::store::util::check_key_in_region;
use tikv::util::{HandyRwLock, escape};
use super::util::*;

// Rule is just for special test which we want do more accurate control
// instead of origin max_peer_count check.
// E.g, for region a, change peers 1,2,3 -> 1,2,4.
// But unlike real pd, Rule is global, and if you set rule,
// we won't check the peer count later.
pub type Rule =
    Box<Fn(&metapb::Region, &metapb::Peer) -> Option<pdpb::RegionHeartbeatResponse> + Send + Sync>;

#[derive(Default)]
struct Store {
    store: metapb::Store,
    region_ids: HashSet<u64>,
}

struct Cluster {
    meta: metapb::Cluster,
    stores: HashMap<u64, Store>,
    regions: BTreeMap<Key, metapb::Region>,
    region_id_keys: HashMap<u64, Key>,
    base_id: AtomicUsize,
    rule: Option<Rule>,

    store_stats: HashMap<u64, pdpb::StoreStats>,
    split_count: usize,

    down_peers: HashMap<u64, pdpb::PeerStats>,
    pending_peers: HashMap<u64, metapb::Peer>,
}

impl Cluster {
    fn new(cluster_id: u64) -> Cluster {
        let mut meta = metapb::Cluster::new();
        meta.set_id(cluster_id);
        meta.set_max_peer_count(5);

        Cluster {
            meta: meta,
            stores: HashMap::new(),
            regions: BTreeMap::new(),
            region_id_keys: HashMap::new(),
            base_id: AtomicUsize::new(1000),
            rule: None,
            store_stats: HashMap::new(),
            split_count: 0,
            down_peers: HashMap::new(),
            pending_peers: HashMap::new(),
        }
    }

    fn bootstrap(&mut self, store: metapb::Store, region: metapb::Region) {
        // Now, some tests use multi peers in bootstrap,
        // disable this check.
        // TODO: enable this check later.
        // assert_eq!(region.get_peers().len(), 1);
        let store_id = store.get_id();
        let mut s = Store {
            store: store,
            region_ids: HashSet::new(),
        };


        s.region_ids.insert(region.get_id());

        self.stores.insert(store_id, s);

        self.add_region(&region);
    }

    // We don't care cluster id here, so any value like 0 in tests is ok.
    fn alloc_id(&self) -> Result<u64> {
        Ok(self.base_id.fetch_add(1, Ordering::Relaxed) as u64)
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<()> {
        let mut s = self.stores.entry(store.get_id()).or_insert_with(Store::default);
        s.store = store;
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        match self.stores.get(&store_id) {
            None => Err(box_err!("store {} not found", store_id)),
            Some(s) => Ok(s.store.clone()),
        }
    }

    fn get_region(&self, key: Vec<u8>) -> Option<metapb::Region> {
        self.regions
            .range((Excluded(key), Unbounded))
            .next()
            .map(|(_, region)| region.clone())
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        Ok(self.region_id_keys.get(&region_id).and_then(|k| self.regions.get(k).cloned()))
    }

    fn get_stores(&self) -> Vec<metapb::Store> {
        self.stores.values().map(|s| s.store.clone()).collect()
    }

    fn add_region(&mut self, region: &metapb::Region) {
        let end_key = enc_end_key(region);
        assert!(self.regions.insert(end_key.clone(), region.clone()).is_none());
        assert!(self.region_id_keys.insert(region.get_id(), end_key.clone()).is_none());
    }

    fn remove_region(&mut self, region: &metapb::Region) {
        let end_key = enc_end_key(region);
        assert!(self.regions.remove(&end_key).is_some());
        assert!(self.region_id_keys.remove(&region.get_id()).is_some());
    }

    fn handle_heartbeat_version(&mut self, region: metapb::Region) -> Result<()> {
        // For split, we should handle heartbeat carefully.
        // E.g, for region 1 [a, c) -> 1 [a, b) + 2 [b, c).
        // after split, region 1 and 2 will do heartbeat independently.
        let start_key = enc_start_key(&region);
        let end_key = enc_end_key(&region);
        assert!(end_key > start_key);

        let version = region.get_region_epoch().get_version();
        let conf_ver = region.get_region_epoch().get_conf_ver();

        let search_key = data_key(region.get_start_key());
        let search_region = match self.get_region(search_key) {
            None => {
                // Find no range after start key, insert directly.
                self.add_region(&region);
                return Ok(());
            }
            Some(search_region) => search_region,
        };

        let search_start_key = enc_start_key(&search_region);
        let search_end_key = enc_end_key(&search_region);

        let search_version = search_region.get_region_epoch().get_version();
        let search_conf_ver = search_region.get_region_epoch().get_conf_ver();

        if start_key == search_start_key && end_key == search_end_key {
            // we are the same, must check epoch here.
            return check_stale_region(&search_region, &region);
        }

        if search_start_key >= end_key {
            // No range covers [start, end) now, insert directly.
            self.add_region(&region);
        } else {
            // overlap, remove old, insert new.
            // E.g, 1 [a, c) -> 1 [a, b) + 2 [b, c), either new 1 or 2 reports, the region
            // is overlapped with origin [a, c).
            if version <= search_version || conf_ver < search_conf_ver {
                return Err(box_err!("epoch {:?} is stale.", region.get_region_epoch()));
            }

            self.remove_region(&search_region);
            self.add_region(&region);
        }

        Ok(())
    }

    fn handle_heartbeat_conf_ver(&mut self,
                                 region: metapb::Region,
                                 leader: metapb::Peer)
                                 -> Result<pdpb::RegionHeartbeatResponse> {
        let conf_ver = region.get_region_epoch().get_conf_ver();
        let end_key = enc_end_key(&region);

        // it can pass handle_heartbeat_version means it must exist.
        let cur_region = self.get_region_by_id(region.get_id()).unwrap().unwrap();

        let cur_conf_ver = cur_region.get_region_epoch().get_conf_ver();
        try!(check_stale_region(&cur_region, &region));

        let region_peer_len = region.get_peers().len();
        let cur_region_peer_len = cur_region.get_peers().len();

        if conf_ver > cur_conf_ver {
            // If ConfVer changed, TiKV has added/removed one peer already.
            // So pd and TiKV can't have same peer count and can only have
            // only one different peer.
            // E.g, we can't meet following cases:
            // 1) pd is (1, 2, 3), TiKV is (1)
            // 2) pd is (1), TiKV is (1, 2, 3)
            // 3) pd is (1, 2), TiKV is (3)
            // 4) pd id (1), TiKV is (2, 3)

            assert_ne!(region_peer_len, cur_region_peer_len);

            if cur_region_peer_len > region_peer_len {
                // must pd is (1, 2), TiKV is (1)
                assert_eq!(cur_region_peer_len - region_peer_len, 1);
                let peers = setdiff_peers(&cur_region, &region);
                assert_eq!(peers.len(), 1);
                assert!(setdiff_peers(&region, &cur_region).is_empty());
            } else {
                // must pd is (1), TiKV is (1, 2)
                assert_eq!(region_peer_len - cur_region_peer_len, 1);
                let peers = setdiff_peers(&region, &cur_region);
                assert_eq!(peers.len(), 1);
                assert!(setdiff_peers(&cur_region, &region).is_empty());
            }

            // update the region.
            assert!(self.regions.insert(end_key, region.clone()).is_some());
        } else {
            must_same_peers(&cur_region, &region);
        }

        let mut resp = pdpb::RegionHeartbeatResponse::new();

        if let Some(ref rule) = self.rule {
            return Ok(rule(&region, &leader).unwrap_or(resp));
        }

        // If no rule, use default max_peer_count check.
        let mut change_peer = pdpb::ChangePeer::new();

        let max_peer_count = self.meta.get_max_peer_count() as usize;
        let peer_count = region.get_peers().len();

        if peer_count < max_peer_count {
            // find the first store which the region has not covered.
            for store_id in self.stores.keys() {
                if region.get_peers().iter().all(|x| x.get_store_id() != *store_id) {
                    let peer = new_peer(*store_id, self.alloc_id().unwrap());
                    change_peer.set_change_type(eraftpb::ConfChangeType::AddNode);
                    change_peer.set_peer(peer.clone());
                    resp.set_change_peer(change_peer);
                    break;
                }
            }
        } else if peer_count > max_peer_count {
            // find the first peer which not leader.
            let pos = region.get_peers()
                .iter()
                .position(|x| x.get_store_id() != leader.get_store_id())
                .unwrap();

            change_peer.set_change_type(eraftpb::ConfChangeType::RemoveNode);
            change_peer.set_peer(region.get_peers()[pos].clone());
            resp.set_change_peer(change_peer);

        }

        Ok(resp)
    }

    fn region_heartbeat(&mut self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        for peer in region.get_peers() {
            self.down_peers.remove(&peer.get_id());
            self.pending_peers.remove(&peer.get_id());
        }
        for peer in down_peers {
            self.down_peers.insert(peer.get_peer().get_id(), peer);
        }
        for p in pending_peers {
            self.pending_peers.insert(p.get_id(), p);
        }

        try!(self.handle_heartbeat_version(region.clone()));
        self.handle_heartbeat_conf_ver(region, leader)
    }
}

fn check_stale_region(region: &metapb::Region, check_region: &metapb::Region) -> Result<()> {
    let epoch = region.get_region_epoch();
    let check_epoch = check_region.get_region_epoch();
    if check_epoch.get_conf_ver() >= epoch.get_conf_ver() &&
       check_epoch.get_version() >= epoch.get_version() {
        return Ok(());
    }

    Err(box_err!("stale epoch {:?}, we are now {:?}", check_epoch, epoch))
}

fn must_same_peers(left: &metapb::Region, right: &metapb::Region) {
    assert_eq!(left.get_peers().len(), right.get_peers().len());
    for peer in left.get_peers() {
        let p = find_peer(right, peer.get_store_id()).unwrap();
        assert_eq!(p.get_id(), peer.get_id());
    }
}

// Left - Right, left (1, 2, 3), right (1, 2), left - right = (3)
fn setdiff_peers(left: &metapb::Region, right: &metapb::Region) -> Vec<metapb::Peer> {
    let mut peers = vec![];
    for peer in left.get_peers() {
        if let Some(p) = find_peer(right, peer.get_store_id()) {
            assert_eq!(p.get_id(), peer.get_id());
            continue;
        }

        peers.push(peer.clone())
    }

    peers
}

pub struct TestPdClient {
    cluster_id: u64,
    cluster: RwLock<Cluster>,
}

impl TestPdClient {
    pub fn new(cluster_id: u64) -> TestPdClient {
        TestPdClient {
            cluster_id: cluster_id,
            cluster: RwLock::new(Cluster::new(cluster_id)),
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

    // Set a customized rule to overwrite default max peer count check rule.
    pub fn set_rule(&self, rule: Rule) {
        self.cluster.wl().rule = Some(rule);
    }

    // Clear the customized rule set before and use default rule again.
    pub fn reset_rule(&self) {
        self.cluster.wl().rule = None;
    }

    pub fn get_region_epoch(&self, region_id: u64) -> metapb::RegionEpoch {
        self.get_region_by_id(region_id).unwrap().unwrap().take_region_epoch()
    }

    // Set an empty rule which nothing to do to disable default max peer count
    // check rule, we can use reset_rule to enable default again.
    pub fn disable_default_rule(&self) {
        self.set_rule(box move |_, _| None);
    }

    pub fn must_have_peer(&self, region_id: u64, peer: metapb::Peer) {
        for _ in 1..500 {
            sleep_ms(10);

            let region = match self.get_region_by_id(region_id).unwrap() {
                Some(region) => region,
                None => continue,
            };

            if let Some(p) = find_peer(&region, peer.get_store_id()) {
                if p.get_id() == peer.get_id() {
                    return;
                }
            }
        }

        let region = self.get_region_by_id(region_id)
            .unwrap();
        panic!("region {:?} has no peer {:?}", region, peer);
    }

    pub fn must_none_peer(&self, region_id: u64, peer: metapb::Peer) {
        for _ in 1..500 {
            sleep_ms(10);

            let region = match self.get_region_by_id(region_id).unwrap() {
                Some(region) => region,
                None => continue,
            };

            if find_peer(&region, peer.get_store_id()).is_none() {
                return;
            }
        }

        let region = self.get_region_by_id(region_id)
            .unwrap();
        panic!("region {:?} has peer {:?}", region, peer);
    }

    pub fn add_peer(&self, region_id: u64, peer: metapb::Peer) {
        self.set_rule(box move |region: &metapb::Region, _: &metapb::Peer| {
            if region.get_id() != region_id {
                return None;
            }
            new_pd_add_change_peer(region, peer.clone())
        });
    }

    pub fn must_add_peer(&self, region_id: u64, peer: metapb::Peer) {
        self.add_peer(region_id, peer.clone());
        self.must_have_peer(region_id, peer);
    }

    pub fn remove_peer(&self, region_id: u64, peer: metapb::Peer) {
        self.set_rule(box move |region: &metapb::Region, _: &metapb::Peer| {
            if region.get_id() != region_id {
                return None;
            }
            new_pd_remove_change_peer(region, peer.clone())
        });
    }

    pub fn must_remove_peer(&self, region_id: u64, peer: metapb::Peer) {
        self.remove_peer(region_id, peer.clone());
        self.must_none_peer(region_id, peer);
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

        assert!(left.get_region_epoch().get_version() > region.get_region_epoch().get_version());
        assert!(right.get_region_epoch().get_version() > region.get_region_epoch().get_version());
        true
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
}

impl PdClient for TestPdClient {
    fn get_cluster_id(&self) -> Result<u64> {
        Ok(self.cluster_id)
    }

    fn bootstrap_cluster(&self, store: metapb::Store, region: metapb::Region) -> Result<()> {
        if self.is_cluster_bootstrapped().unwrap() {
            return Err(Error::ClusterBootstrapped(self.cluster_id));
        }

        self.cluster.wl().bootstrap(store, region);

        Ok(())
    }

    fn is_cluster_bootstrapped(&self) -> Result<bool> {
        Ok(!self.cluster.rl().stores.is_empty())
    }

    fn alloc_id(&self) -> Result<u64> {
        self.cluster.rl().alloc_id()
    }

    fn put_store(&self, store: metapb::Store) -> Result<()> {
        try!(self.check_bootstrap());
        self.cluster.wl().put_store(store)
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        try!(self.check_bootstrap());
        self.cluster.rl().get_store(store_id)
    }


    fn get_region(&self, key: &[u8]) -> Result<metapb::Region> {
        try!(self.check_bootstrap());
        if let Some(region) = self.cluster.rl().get_region(data_key(key)) {
            if check_key_in_region(key, &region).is_ok() {
                return Ok(region);
            }
        }

        Err(box_err!("no region contains key {:?}", escape(key)))
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<Option<metapb::Region>> {
        try!(self.check_bootstrap());
        self.cluster.rl().get_region_by_id(region_id)
    }

    fn get_cluster_config(&self) -> Result<metapb::Cluster> {
        try!(self.check_bootstrap());
        Ok(self.cluster.rl().meta.clone())
    }


    fn region_heartbeat(&self,
                        region: metapb::Region,
                        leader: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>,
                        pending_peers: Vec<metapb::Peer>)
                        -> Result<pdpb::RegionHeartbeatResponse> {
        try!(self.check_bootstrap());
        self.cluster.wl().region_heartbeat(region, leader, down_peers, pending_peers)
    }

    fn ask_split(&self, region: metapb::Region) -> Result<pdpb::AskSplitResponse> {
        try!(self.check_bootstrap());

        // Must ConfVer and Version be same?
        let cur_region = self.cluster.rl().get_region_by_id(region.get_id()).unwrap().unwrap();
        try!(check_stale_region(&cur_region, &region));

        let mut resp = pdpb::AskSplitResponse::new();
        resp.set_new_region_id(self.alloc_id().unwrap());
        let mut peer_ids = vec![];
        for _ in region.get_peers() {
            peer_ids.push(self.alloc_id().unwrap());
        }
        resp.set_new_peer_ids(peer_ids);

        Ok(resp)
    }

    fn store_heartbeat(&self, stats: pdpb::StoreStats) -> Result<()> {
        try!(self.check_bootstrap());

        // Cache it directly now.
        let store_id = stats.get_store_id();
        self.cluster.wl().store_stats.insert(store_id, stats);

        Ok(())
    }

    fn report_split(&self, _: metapb::Region, _: metapb::Region) -> Result<()> {
        // pd just uses this for history show, so here we just count it.
        try!(self.check_bootstrap());
        self.cluster.wl().split_count += 1;
        Ok(())
    }
}
