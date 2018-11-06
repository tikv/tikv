// Copyright 2018 PingCAP, Inc.
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

//! Generally peers are state machines that represent a replica of a region,
//! and store is also a special state machine that handles all requests across
//! stores. They are mixed for now, will be separated in the future.

pub mod apply;
pub mod apply_transport;
pub mod peer;
pub mod store;
pub mod transport;

pub use self::apply::{
    Apply, ApplyMetrics, ApplyRes, Proposal, RegionProposal, Registration, Task as ApplyTask,
    TaskRes as ApplyTaskRes,
};
pub use self::apply_transport::Router as ApplyRouter;
pub use self::peer::{DestroyPeerJob, Peer};
pub use self::store::{new_compaction_listener, Store, StoreInfo};
pub use self::transport::{create_router, OneshotNotifier, Router};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::u64;

use kvproto::metapb::{Region, RegionEpoch};
use kvproto::raft_serverpb::RaftMessage;

use raftstore::coprocessor::CoprocessorHost;
use util::collections::HashMap;
use util::worker::Scheduler;
use util::RingQueue;

use super::config::Config;
use super::worker::{ReadTask, RegionTask};
use super::Engines;

type Key = Vec<u8>;

pub struct StoreMeta {
    // region end key -> region id
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    // region_id -> region
    pub regions: HashMap<u64, Region>,
    // A marker used to indicate if the peer of a region is going to apply a snapshot
    // with different range.
    // It assumes that when a peer is going to accept snapshot, it can never
    // captch up by normal log replication.
    pub pending_cross_snap: HashMap<u64, RegionEpoch>,
    pub pending_votes: RingQueue<RaftMessage>,
    // the regions with pending snapshots.
    pub pending_snapshot_regions: Vec<Region>,
    // (region_id, version) -> BiLock.
    pub merge_locks: HashMap<(u64, u64), Option<OneshotNotifier>>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize) -> StoreMeta {
        StoreMeta {
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
            pending_cross_snap: HashMap::default(),
            pending_votes: RingQueue::with_capacity(vote_capacity),
            pending_snapshot_regions: Vec::default(),
            merge_locks: HashMap::default(),
        }
    }

    #[inline]
    pub fn set_region(&mut self, region: Region, peer: &mut super::peer::Peer) {
        let prev = self.regions.insert(region.get_id(), region.clone());
        if prev.map_or(true, |r| r.get_id() != region.get_id()) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.tag);
        }
        peer.set_region(region);
    }
}

pub trait ConfigProvider {
    fn store_id(&self) -> u64;
    fn snap_scheduler(&self) -> Scheduler<RegionTask>;
    fn engines(&self) -> &Engines;
    fn apply_scheduler(&self) -> ApplyRouter;
    fn read_scheduler(&self) -> Scheduler<ReadTask>;
    fn coprocessor_host(&self) -> Arc<CoprocessorHost>;
    fn config(&self) -> Arc<Config>;
}

#[derive(Default)]
pub struct StoreStat {
    pub lock_cf_bytes_written: AtomicU64,

    pub engine_total_bytes_written: AtomicU64,
    pub engine_total_keys_written: AtomicU64,

    pub is_busy: AtomicBool,
}

#[derive(Clone, Default)]
pub struct GlobalStoreStat {
    pub stat: Arc<StoreStat>,
}

pub struct LocalStoreStat {
    pub lock_cf_bytes_written: u64,
    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,
    pub is_busy: bool,

    global: GlobalStoreStat,
}

impl GlobalStoreStat {
    #[inline]
    fn local(&self) -> LocalStoreStat {
        LocalStoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,
            is_busy: false,

            global: self.clone(),
        }
    }
}

impl Clone for LocalStoreStat {
    #[inline]
    fn clone(&self) -> LocalStoreStat {
        self.global.local()
    }
}

impl LocalStoreStat {
    pub fn flush(&mut self) {
        if self.lock_cf_bytes_written != 0 {
            self.global
                .stat
                .lock_cf_bytes_written
                .fetch_add(self.lock_cf_bytes_written, Ordering::Relaxed);
            self.lock_cf_bytes_written = 0;
        }
        if self.engine_total_bytes_written != 0 {
            self.global
                .stat
                .engine_total_bytes_written
                .fetch_add(self.engine_total_bytes_written, Ordering::Relaxed);
            self.engine_total_bytes_written = 0;
        }
        if self.engine_total_keys_written != 0 {
            self.global
                .stat
                .engine_total_keys_written
                .fetch_add(self.engine_total_keys_written, Ordering::Relaxed);
            self.engine_total_keys_written = 0;
        }
        if self.is_busy {
            self.global.stat.is_busy.store(true, Ordering::Relaxed);
            self.is_busy = false;
        }
    }
}
