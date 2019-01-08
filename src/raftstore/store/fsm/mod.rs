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

mod batch;
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
    /// store_tag, "[store <store_id>]"
    tag: String,
    /// the store meta information of the store
    store: metapb::Store,
    /// the config of TiKV
    cfg: Rc<Config>,
    /// the local engine
    engines: Engines,
    /// the start time of this TiKV
    start_time: Timespec,
    /// whether the process of handling Raft ready is slow
    is_busy: bool,

    /// It is used to transport messages between different Raft peers.
    trans: T,
    /// the sender pointing to itself, used to send some periodic tasks
    sendch: SendCh<Msg>,
    /// Unreachable reports can be ignored if the event loop's channel is full,
    /// which may lead to abandoned follower. So use a special receiver to handle
    /// significant messages separately, like `Unreachable` and `SnapshotStatus`.
    significant_msg_receiver: StdReceiver<SignificantMsg>,

    /// region_id -> peers
    region_peers: HashMap<u64, Peer>,
    /// region_end_key -> region_id
    region_ranges: BTreeMap<Key, u64>,
    /// the Regions in Merging state
    merging_regions: Option<Vec<metapb::Region>>,

    /// the Regions for which there will probably be some readiness which
    /// needs to be handled between two mio ticks.
    pending_raft_groups: HashSet<u64>,
    /// the Regions with pending snapshots between two mio ticks.
    pending_snapshot_regions: Vec<metapb::Region>,
    /// A marker used to indicate if the peer of a region is going to apply a snapshot
    /// with different range.
    /// It assumes that when a peer is going to accept snapshot, it can never
    /// catch up by normal log replication.
    pending_cross_snap: HashMap<u64, metapb::RegionEpoch>,
    /// `MsgRequestPreVote` or `MsgRequestVote` messages from new split regions shouldn't be dropped
    /// if there is no such region in this store now. So the messages are recorded temporarily and will
    /// be handled later.
    pending_votes: RingQueue<RaftMessage>,

    /// the worker to gc Raft logs asynchronously
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    /// the worker to do consistency check asynchronously
    consistency_check_worker: Worker<ConsistencyCheckTask>,
    /// the worker to do read requests asynchronously to avoid the blocking of write requests
    local_reader: Worker<ReadTask>,

    /// the worker to split Regions asynchronously
    split_check_worker: Worker<SplitCheckTask>,
    /// It is a host for some observers which is used to do some information collection works.
    coprocessor_host: Arc<CoprocessorHost>,

    /// the worker to clean up already ingested SST files asynchronously, mainly used for importer
    cleanup_sst_worker: Worker<CleanupSSTTask>,
    /// It manages SST files that are waiting for ingesting.
    importer: Arc<SSTImporter>,

    /// the worker to check and compact RocksDB asynchronously to speed up disk space reclamation
    compact_worker: Worker<CompactTask>,
    /// the last checked key of the last task for compact worker
    last_compact_checked_key: Key,

    /// the worker to schedule PD related tasks asynchronously
    pd_worker: FutureWorker<PdTask>,
    /// the actual client to send PD requests and receive responses
    pd_client: Arc<C>,

    /// the worker to apply raft logs asynchronously
    apply_worker: Worker<ApplyTask>,
    /// the receiver to receive the apply results from apply worker
    apply_res_receiver: Option<StdReceiver<ApplyTaskRes>>,

    /// the worker to do snapshot related works asynchronously
    region_worker: Worker<RegionTask>,
    /// It is used to manage all the snapshots, including generation and deletion.
    snap_mgr: SnapManager,

    // some local metrics
    entry_cache_metrics: Rc<RefCell<CacheQueryStats>>,
    raft_metrics: RaftMetrics,
    store_stat: StoreStat,
}
