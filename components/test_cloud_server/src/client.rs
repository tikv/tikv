// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    ops::{
        Bound::{Excluded, Unbounded},
        Range,
    },
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use futures::executor::block_on;
use grpcio::Channel;
use kvproto::{
    errorpb::Error,
    kvrpcpb::{CommitRequest, Context, Mutation, Op, PrewriteRequest, SplitRegionRequest},
    metapb::{Peer, Region, RegionEpoch},
    tikvpb::TikvClient,
};
use pd_client::PdClient;
use test_raftstore::TestPdClient;
use tikv::storage::mvcc::TimeStamp;
use tikv_util::codec::bytes::{decode_bytes, encode_bytes};

pub struct ClusterClient {
    pub pd_client: Arc<TestPdClient>,
    pub channels: HashMap<u64, Channel>,
    /// region_raw_end_key -> region_id
    pub(crate) region_ranges: BTreeMap<Vec<u8>, u64>,
    /// region_id -> region
    pub(crate) regions: HashMap<u64, RawRegion>,
}

#[derive(Clone)]
pub(crate) struct RawRegion {
    id: u64,
    raw_start: Vec<u8>,
    raw_end: Vec<u8>,
    epoch: RegionEpoch,
    peers: Vec<Peer>,
    leader_idx: usize,
}

impl From<Region> for RawRegion {
    fn from(mut region: Region) -> Self {
        let raw_start = if region.start_key.is_empty() {
            vec![]
        } else {
            let mut slice = region.start_key.as_slice();
            decode_bytes(&mut slice, false).unwrap()
        };
        let raw_end = if region.end_key.is_empty() {
            vec![255; 8]
        } else {
            let mut slice = region.end_key.as_slice();
            decode_bytes(&mut slice, false).unwrap()
        };
        RawRegion {
            id: region.id,
            raw_start,
            raw_end,
            epoch: region.take_region_epoch(),
            peers: region.take_peers().into_vec(),
            leader_idx: 0,
        }
    }
}

impl RawRegion {
    fn get_leader(&self) -> Peer {
        self.peers[self.leader_idx].clone()
    }
}

impl ClusterClient {
    pub fn get_ts(&self) -> TimeStamp {
        block_on(self.pd_client.get_tso()).unwrap()
    }

    pub fn put_kv<F, G>(&mut self, rng: Range<usize>, gen_key: F, gen_val: G)
    where
        F: Fn(usize) -> Vec<u8>,
        G: Fn(usize) -> Vec<u8>,
    {
        let start_key = gen_key(rng.start);
        let start_ts = self.get_ts();

        let mut mutations = vec![];
        for i in rng.clone() {
            let mut m = Mutation::default();
            m.set_op(Op::Put);
            m.set_key(gen_key(i));
            m.set_value(gen_val(i));
            mutations.push(m)
        }
        let keys = mutations.iter().map(|m| m.get_key().to_vec()).collect();
        self.kv_prewrite(mutations, start_key, start_ts);
        let commit_ts = self.get_ts();
        self.kv_commit(keys, start_ts, commit_ts);
    }

    pub fn kv_prewrite(&mut self, muts: Vec<Mutation>, pk: Vec<u8>, ts: TimeStamp) {
        let groups = self.group_mutations_by_region(muts);
        for (region_id, group_muts) in groups {
            self.kv_prewrite_single_region(region_id, group_muts, pk.clone(), ts);
        }
    }

    pub fn kv_prewrite_single_region(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
    ) {
        loop {
            let ctx = self.new_rpc_ctx(region_id);
            let kv_client = self.get_kv_client(ctx.get_peer().get_store_id());
            let mut prewrite_req = PrewriteRequest::default();
            prewrite_req.set_context(ctx);
            prewrite_req.set_mutations(muts.clone().into());
            prewrite_req.primary_lock = pk.clone();
            prewrite_req.start_version = ts.into_inner();
            prewrite_req.lock_ttl = 3000;
            prewrite_req.min_commit_ts = prewrite_req.start_version + 1;
            let result = kv_client.kv_prewrite(&prewrite_req);
            if result.is_err() {
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }
            let resp = result.unwrap();
            if resp.has_region_error() {
                let region_err = resp.get_region_error();
                if self.handle_retryable_error(region_id, region_err) {
                    continue;
                }
                if self.handle_region_epoch_not_match(region_err) {
                    self.kv_prewrite(muts, pk, ts);
                    return;
                }
                panic!("unexpected error {:?}", region_err);
            }
            return;
        }
    }

    pub fn kv_commit(&mut self, keys: Vec<Vec<u8>>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let groups = self.group_keys_by_region(keys);
        for (region_id, group_keys) in groups {
            self.kv_commit_single_region(region_id, group_keys, start_ts, commit_ts);
        }
    }

    pub fn kv_commit_single_region(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    ) {
        loop {
            let ctx = self.new_rpc_ctx(region_id);
            let kv_client = self.get_kv_client(ctx.get_peer().get_store_id());
            let mut commit_req = CommitRequest::default();
            commit_req.set_context(ctx);
            commit_req.start_version = start_ts.into_inner();
            commit_req.set_keys(keys.clone().into());
            commit_req.commit_version = commit_ts.into_inner();
            let result = kv_client.kv_commit(&commit_req);
            if result.is_err() {
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
                continue;
            }
            let commit_resp = result.unwrap();
            if commit_resp.has_region_error() {
                let region_err = commit_resp.get_region_error();
                if self.handle_retryable_error(region_id, region_err) {
                    continue;
                }
                if self.handle_region_epoch_not_match(region_err) {
                    self.kv_commit(keys, start_ts, commit_ts);
                    return;
                }
                panic!("unexpected error {:?}", region_err);
            }
            return;
        }
    }

    fn group_mutations_by_region(
        &mut self,
        mut mutations: Vec<Mutation>,
    ) -> HashMap<u64, Vec<Mutation>> {
        let mut groups: HashMap<u64, Vec<Mutation>> = HashMap::new();
        for m in mutations.drain(..) {
            let region = self.get_region_by_key(m.get_key());
            groups.entry(region.id).or_insert(vec![]).push(m);
        }
        groups
    }

    fn group_keys_by_region(&mut self, mut keys: Vec<Vec<u8>>) -> HashMap<u64, Vec<Vec<u8>>> {
        let mut groups: HashMap<u64, Vec<Vec<u8>>> = HashMap::new();
        for key in keys.drain(..) {
            let region = self.get_region_by_key(&key);
            groups.entry(region.id).or_insert(vec![]).push(key);
        }
        groups
    }

    pub fn get_region_id(&mut self, key: &[u8]) -> u64 {
        let region = self.get_region_by_key(key);
        region.id
    }

    fn get_region_by_key(&mut self, key: &[u8]) -> RawRegion {
        if let Some(region) = self.get_region_from_cache(key) {
            return region;
        }
        let region: RawRegion = self
            .pd_client
            .get_region(&encode_bytes(key))
            .unwrap()
            .into();
        self.region_ranges.insert(region.raw_end.clone(), region.id);
        self.regions.insert(region.id, region);
        self.get_region_from_cache(key).unwrap()
    }

    fn update_cache_by_id(&mut self, region_id: u64, opt_region: Option<RawRegion>) {
        if let Some(x) = self.regions.remove(&region_id) {
            self.region_ranges.remove(&x.raw_end);
        }
        let region: RawRegion = opt_region.unwrap_or_else(|| {
            block_on(self.pd_client.get_region_by_id(region_id))
                .unwrap()
                .unwrap()
                .into()
        });
        self.region_ranges.insert(region.raw_end.clone(), region.id);
        self.regions.insert(region.id, region);
    }

    fn get_region_from_cache(&self, key: &[u8]) -> Option<RawRegion> {
        if let Some((_, id)) = self
            .region_ranges
            .range((Excluded(key.to_vec()), Unbounded))
            .next()
        {
            let region = self.regions.get(id).unwrap();
            if region.raw_start.as_slice() <= key {
                return Some(region.clone());
            }
        }
        None
    }

    fn handle_retryable_error(&mut self, region_id: u64, region_err: &Error) -> bool {
        if region_err.has_not_leader() {
            let region = self.regions.get_mut(&region_id).unwrap();
            if region_err.get_not_leader().has_leader() {
                let leader = region_err.get_not_leader().get_leader();
                for (i, peer) in region.peers.iter().enumerate() {
                    if peer.id == leader.id {
                        region.leader_idx = i;
                        sleep(Duration::from_millis(100));
                        return true;
                    }
                }
                sleep(Duration::from_millis(100));
                self.update_cache_by_id(region_id, None);
            } else {
                sleep(Duration::from_millis(100));
            }
            return true;
        }
        if region_err.has_region_not_found() {
            self.update_cache_by_id(region_id, None);
            return true;
        }
        if region_err.has_stale_command() {
            sleep(Duration::from_millis(100));
            self.update_cache_by_id(region_id, None);
            return true;
        }
        if region_err.has_server_is_busy() {
            sleep(Duration::from_millis(100));
            return true;
        }
        if region_err.has_read_index_not_ready() {
            sleep(Duration::from_millis(100));
            return true;
        }
        if region_err.get_message().contains("mismatch peer") {
            sleep(Duration::from_millis(100));
            self.update_cache_by_id(region_id, None);
            return true;
        }
        false
    }

    fn handle_region_epoch_not_match(&mut self, region_err: &Error) -> bool {
        if region_err.has_epoch_not_match() {
            let not_match = region_err.get_epoch_not_match();
            for region in not_match.get_current_regions() {
                self.update_cache_by_id(region.id, Some(region.clone().into()));
            }
            return true;
        }
        false
    }

    pub fn get_kv_client(&self, store_id: u64) -> TikvClient {
        TikvClient::new(self.get_client_channel(store_id))
    }

    pub fn get_client_channel(&self, store_id: u64) -> Channel {
        self.channels.get(&store_id).unwrap().clone()
    }

    pub fn get_stores(&self) -> Vec<u64> {
        self.channels.keys().copied().collect()
    }

    pub fn new_rpc_ctx(&self, region_id: u64) -> Context {
        let region = self.regions.get(&region_id).unwrap();
        let mut ctx = Context::new();
        ctx.set_region_id(region_id);
        ctx.set_region_epoch(region.epoch.clone());
        ctx.set_peer(region.get_leader());
        ctx
    }

    pub fn split(&mut self, key: &[u8]) {
        for _ in 0..10 {
            let region_id = self.get_region_id(key);
            let ctx = self.new_rpc_ctx(region_id);
            let client = self.get_kv_client(ctx.get_peer().get_store_id());
            let mut split_req = SplitRegionRequest::default();
            split_req.set_context(ctx);
            split_req.set_split_key(key.to_vec());
            let resp = client.split_region(&split_req).unwrap();
            if resp.has_region_error() {
                let region_err = resp.get_region_error();
                if self.handle_retryable_error(region_id, region_err) {
                    sleep(Duration::from_millis(100));
                    continue;
                }
                if self.handle_region_epoch_not_match(region_err) {
                    sleep(Duration::from_millis(100));
                    continue;
                }
                panic!("failed to split key {:?} error {:?}", key, region_err);
            }
            return;
        }
        panic!("failed to split key {:?}", key);
    }
}
