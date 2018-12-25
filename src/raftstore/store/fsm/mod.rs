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

mod peer;
mod router;
mod store;

pub use self::peer::DestroyPeerJob;
pub use self::store::{
    create_event_loop, new_compaction_listener, StoreChannel, StoreInfo, StoreStat,
};

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::mpsc::Receiver as StdReceiver;
use std::sync::Arc;
use std::u64;
use time::Timespec;

use kvproto::metapb;
use kvproto::raft_serverpb::RaftMessage;

use pd::PdTask;
use raftstore::coprocessor::CoprocessorHost;
use util::collections::{HashMap, HashSet};
use util::transport::SendCh;
use util::worker::{FutureWorker, Worker};
use util::RingQueue;

use super::config::Config;
use super::local_metrics::RaftMetrics;
use super::peer::Peer;
use super::peer_storage::CacheQueryStats;
use super::worker::{
    ApplyTask, ApplyTaskRes, CleanupSSTTask, CompactTask, ConsistencyCheckTask, RaftlogGcTask,
    ReadTask, RegionTask, SplitCheckTask,
};
use super::{Engines, Msg, SignificantMsg, SnapManager};
use import::SSTImporter;

type Key = Vec<u8>;

pub struct Store<T, C: 'static> {
    cfg: Rc<Config>,
    engines: Engines,
    store: metapb::Store,
    sendch: SendCh<Msg>,

    significant_msg_receiver: StdReceiver<SignificantMsg>,

    // region_id -> peers
    region_peers: HashMap<u64, Peer>,
    merging_regions: Option<Vec<metapb::Region>>,
    pending_raft_groups: HashSet<u64>,
    // region end key -> region id
    region_ranges: BTreeMap<Key, u64>,
    // the regions with pending snapshots between two mio ticks.
    pending_snapshot_regions: Vec<metapb::Region>,
    // A marker used to indicate if the peer of a region is going to apply a snapshot
    // with different range.
    // It assumes that when a peer is going to accept snapshot, it can never
    // captch up by normal log replication.
    pending_cross_snap: HashMap<u64, metapb::RegionEpoch>,

    split_check_worker: Worker<SplitCheckTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    region_worker: Worker<RegionTask>,
    compact_worker: Worker<CompactTask>,
    pd_worker: FutureWorker<PdTask>,
    consistency_check_worker: Worker<ConsistencyCheckTask>,
    cleanup_sst_worker: Worker<CleanupSSTTask>,
    pub apply_worker: Worker<ApplyTask>,
    local_reader: Worker<ReadTask>,
    apply_res_receiver: Option<StdReceiver<ApplyTaskRes>>,

    last_compact_checked_key: Key,

    trans: T,
    pd_client: Arc<C>,

    pub coprocessor_host: Arc<CoprocessorHost>,

    pub importer: Arc<SSTImporter>,

    snap_mgr: SnapManager,

    raft_metrics: RaftMetrics,
    pub entry_cache_metries: Rc<RefCell<CacheQueryStats>>,

    tag: String,

    start_time: Timespec,
    is_busy: bool,

    pending_votes: RingQueue<RaftMessage>,

    store_stat: StoreStat,
}
