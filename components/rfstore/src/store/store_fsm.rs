// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::Config;
use crate::RaftRouter;
use kvproto::import_sstpb::SstMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::QueryStats;
use kvproto::pdpb::StoreStats;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest};
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use raftstore::coprocessor::CoprocessorHost;
use std::collections::{BTreeMap, HashMap};
use tikv_util::RingQueue;

use super::*;

pub(crate) fn create_raft_batch_system(conf: &Config) -> (RaftRouter, RaftBatchSystem) {
    todo!()
}

pub struct RaftBatchSystem {}

pub struct StoreInfo<E> {
    pub engine: E,
    pub capacity: u64,
}

pub struct StoreMeta {
    /// store id
    pub store_id: Option<u64>,
    /// region_end_key -> region_id
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    /// region_id -> region
    pub regions: HashMap<u64, Region>,
    /// region_id -> reader
    pub readers: HashMap<u64, ReadDelegate>,
    /// region_id -> (term, leader_peer_id)
    pub leaders: HashMap<u64, (u64, u64)>,
    /// `MsgRequestPreVote`, `MsgRequestVote` or `MsgAppend` messages from newly split Regions shouldn't be
    /// dropped if there is no such Region in this store now. So the messages are recorded temporarily and
    /// will be handled later.
    pub pending_msgs: RingQueue<RaftMessage>,
    /// The regions with pending snapshots.
    pub pending_snapshot_regions: Vec<Region>,
    /// A marker used to indicate the peer of a Region has received a merge target message and waits to be destroyed.
    /// target_region_id -> (source_region_id -> merge_target_region)
    pub pending_merge_targets: HashMap<u64, HashMap<u64, metapb::Region>>,
    /// An inverse mapping of `pending_merge_targets` used to let source peer help target peer to clean up related entry.
    /// source_region_id -> target_region_id
    pub targets_map: HashMap<u64, u64>,
    /// `atomic_snap_regions` and `destroyed_region_for_snap` are used for making destroy overlapped regions
    /// and apply snapshot atomically.
    /// region_id -> wait_destroy_regions_map(source_region_id -> is_ready)
    /// A target peer must wait for all source peer to ready before applying snapshot.
    pub atomic_snap_regions: HashMap<u64, HashMap<u64, bool>>,
    /// source_region_id -> need_atomic
    /// Used for reminding the source peer to switch to ready in `atomic_snap_regions`.
    pub destroyed_region_for_snap: HashMap<u64, bool>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize) -> StoreMeta {
        StoreMeta {
            store_id: None,
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
            readers: HashMap::default(),
            leaders: HashMap::default(),
            pending_msgs: RingQueue::with_capacity(vote_capacity),
            pending_snapshot_regions: Vec::default(),
            pending_merge_targets: HashMap::default(),
            targets_map: HashMap::default(),
            atomic_snap_regions: HashMap::default(),
            destroyed_region_for_snap: HashMap::default(),
        }
    }

    #[inline]
    pub(crate) fn set_region(
        &mut self,
        host: &CoprocessorHost<kvengine::Engine>,
        region: Region,
        peer: &mut crate::store::Peer,
    ) {
        let prev = self.regions.insert(region.get_id(), region.clone());
        if prev.map_or(true, |r| r.get_id() != region.get_id()) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.region_id);
        }
        let reader = self.readers.get_mut(&region.get_id()).unwrap();
        peer.set_region(host, reader, region);
    }
}

pub(crate) struct StoreFSM {}
