// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};

use crate::raftstore::coprocessor::CoprocessorHost;
use crate::raftstore::store::worker::ReadDelegate;
use crate::raftstore::store::Peer;
use kvproto::metapb::{Region, RegionEpoch};
use kvproto::raft_serverpb::RaftMessage;

use tikv_util::collections::HashMap;
use tikv_util::{HandyRwLock, RingQueue};

const PENDING_VOTES_CAP: usize = 20;
const READERS_SLOT_COUNT: usize = 1 << 6; // 64 slots.

type SharedReader = Arc<RwLock<ReadDelegate>>;

pub struct StoreMeta {
    pub(super) inner: Mutex<StoreMetaInner>,
    /// store id
    pub store_id: AtomicU64,
    /// region_id -> reader
    readers: Vec<RwLock<HashMap<u64, SharedReader>>>,
}

impl StoreMeta {
    pub fn new() -> Self {
        let mut readers = Vec::with_capacity(READERS_SLOT_COUNT);
        for _ in 0..READERS_SLOT_COUNT {
            readers.push(RwLock::new(HashMap::new()));
        }
        StoreMeta {
            inner: Mutex::new(StoreMetaInner::default()),
            store_id: AtomicU64::new(0),
            readers,
        }
    }

    pub(super) fn insert_reader(&self, region_id: u64, reader: ReadDelegate) {
        let slot = region_id as usize % READERS_SLOT_COUNT;
        let mut readers = self.readers[slot].write().unwrap();
        readers.insert(region_id, Arc::new(RwLock::new(reader)));
    }

    pub(super) fn remove_reader(&self, region_id: u64) {
        let slot = region_id as usize % READERS_SLOT_COUNT;
        let mut readers = self.readers[slot].write().unwrap();
        readers.remove(&region_id);
    }

    pub fn with_reader<F, R>(&self, region_id: u64, f: F) -> R
    where
        F: FnOnce(Option<&ReadDelegate>) -> R,
    {
        let slot = region_id as usize % READERS_SLOT_COUNT;
        match self.readers[slot].rl().get(&region_id).map(Arc::clone) {
            Some(reader) => f(Some(reader.read().unwrap().deref())),
            None => f(None),
        }
    }

    pub(super) fn with_mut_reader<F, R>(&self, region_id: u64, f: F) -> R
    where
        F: FnOnce(Option<&mut ReadDelegate>) -> R,
    {
        let slot = region_id as usize % READERS_SLOT_COUNT;
        match self.readers[slot].rl().get(&region_id).map(Arc::clone) {
            Some(reader) => f(Some(reader.write().unwrap().deref_mut())),
            None => f(None),
        }
    }

    pub(super) fn set_region(&self, host: &CoprocessorHost, region: Region, peer: &mut Peer) {
        let region_id = region.get_id();
        let mut inner = self.inner.lock().unwrap();
        let prev = inner.regions.insert(region_id, region.clone());
        drop(inner);
        if prev.map_or(true, |r| r.get_id() != region_id) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.tag);
        }
        self.with_mut_reader(region_id, |reader| {
            peer.set_region(host, reader.unwrap(), region);
        });
    }

    /// Sometimes `set_region` needs to be called with a locked `inner`.
    pub(super) fn set_region_with_inner(
        &self,
        inner: &mut StoreMetaInner,
        host: &CoprocessorHost,
        region: Region,
        peer: &mut Peer,
    ) {
        let region_id = region.get_id();
        let prev = inner.regions.insert(region_id, region.clone());
        if prev.map_or(true, |r| r.get_id() != region_id) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.tag);
        }
        self.with_mut_reader(region_id, |reader| {
            peer.set_region(host, reader.unwrap(), region);
        });
    }
}

pub(super) struct StoreMetaInner {
    /// region_end_key -> region_id
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    /// region_id -> region
    pub regions: HashMap<u64, Region>,
    /// `MsgRequestPreVote` or `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
    /// such Region in this store now. So the messages are recorded temporarily and will be handled later.
    pub pending_votes: RingQueue<RaftMessage>,
    /// The regions with pending snapshots.
    pub pending_snapshot_regions: Vec<Region>,
    /// A marker used to indicate the peer of a Region has received a merge target message and waits to be destroyed.
    /// target_region_id -> (source_region_id -> merge_target_epoch)
    pub pending_merge_targets: HashMap<u64, HashMap<u64, RegionEpoch>>,
    /// An inverse mapping of `pending_merge_targets` used to let source peer help target peer to clean up related entry.
    /// source_region_id -> target_region_id
    pub targets_map: HashMap<u64, u64>,
    /// In raftstore, the execute order of `PrepareMerge` and `CommitMerge` is not certain because of the messages
    /// belongs two regions. To make them in order, `PrepareMerge` will set this structure and `CommitMerge` will retry
    /// later if there is no related lock.
    /// source_region_id -> (version, BiLock).
    pub merge_locks: HashMap<u64, (u64, Option<Arc<AtomicBool>>)>,
}

impl Default for StoreMetaInner {
    fn default() -> StoreMetaInner {
        StoreMetaInner {
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
            pending_votes: RingQueue::with_capacity(PENDING_VOTES_CAP),
            pending_snapshot_regions: Vec::default(),
            pending_merge_targets: HashMap::default(),
            targets_map: HashMap::default(),
            merge_locks: HashMap::default(),
        }
    }
}
