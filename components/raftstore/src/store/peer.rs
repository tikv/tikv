// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{cmp, mem, u64, usize};

use crossbeam::atomic::AtomicCell;
use crossbeam::channel::TrySendError;
use engine_traits::{Engines, KvEngine, RaftEngine, Snapshot, WriteBatch, WriteOptions};
use error_code::ErrorCodeExt;
use kvproto::errorpb;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::{self, PeerRole};
use kvproto::pdpb::PeerStats;
use kvproto::raft_cmdpb::{
    self, AdminCmdType, AdminResponse, ChangePeerRequest, CmdType, CommitMergeRequest,
    RaftCmdRequest, RaftCmdResponse, TransferLeaderRequest, TransferLeaderResponse,
};
use kvproto::raft_serverpb::{
    ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftApplyState, RaftMessage,
};
use kvproto::replication_modepb::{
    DrAutoSyncState, RegionReplicationState, RegionReplicationStatus, ReplicationMode,
};
use protobuf::Message;
use raft::eraftpb::{self, ConfChangeType, Entry, EntryType, MessageType};
use raft::{
    self, Changer, LightReady, ProgressState, ProgressTracker, RawNode, Ready, SnapshotStatus,
    StateRole, INVALID_INDEX, NO_LIMIT,
};
use raft_proto::ConfChangeI;
use smallvec::SmallVec;
use time::Timespec;
use uuid::Uuid;

use crate::coprocessor::{CoprocessorHost, RegionChangeEvent};
use crate::errors::RAFTSTORE_IS_BUSY;
use crate::store::fsm::apply::CatchUpLogs;
use crate::store::fsm::store::PollContext;
use crate::store::fsm::{apply, Apply, ApplyMetrics, ApplyTask, CollectedReady, Proposal};
use crate::store::hibernate_state::GroupState;
use crate::store::msg::RaftCommand;
use crate::store::worker::{HeartbeatTask, ReadDelegate, ReadExecutor, ReadProgress, RegionTask};
use crate::store::{
    Callback, Config, GlobalReplicationState, PdTask, ReadIndexContext, ReadResponse,
    SplitCheckTask,
};
use crate::{Error, Result};
use collections::{HashMap, HashSet};
use pd_client::INVALID_ID;
use tikv_util::time::{duration_to_sec, monotonic_raw_now};
use tikv_util::time::{Instant as UtilInstant, ThreadReadId};
use tikv_util::worker::{FutureScheduler, Scheduler};
use tikv_util::Either;

use super::cmd_resp;
use super::local_metrics::{RaftMessageMetrics, RaftReadyMetrics};
use super::metrics::*;
use super::peer_storage::{
    write_peer_state, ApplySnapResult, CheckApplyingSnapStatus, InvokeContext, PeerStorage,
};
use super::read_queue::{ReadIndexQueue, ReadIndexRequest};
use super::transport::Transport;
use super::util::{
    self, check_region_epoch, is_initial_msg, AdminCmdEpochState, ChangePeerI, ConfChangeKind,
    Lease, LeaseState, ADMIN_CMD_EPOCH_MAP, NORMAL_REQ_CHECK_CONF_VER, NORMAL_REQ_CHECK_VER,
};
use super::DestroyPeerJob;

const SHRINK_CACHE_CAPACITY: usize = 64;
const MIN_BCAST_WAKE_UP_INTERVAL: u64 = 1_000; // 1s

/// The returned states of the peer after checking whether it is stale
#[derive(Debug, PartialEq, Eq)]
pub enum StaleState {
    Valid,
    ToValidate,
    LeaderMissing,
}

struct ProposalQueue<S>
where
    S: Snapshot,
{
    tag: String,
    queue: VecDeque<Proposal<S>>,
}

impl<S: Snapshot> ProposalQueue<S> {
    fn new(tag: String) -> ProposalQueue<S> {
        ProposalQueue {
            tag,
            queue: VecDeque::new(),
        }
    }

    fn find_propose_time(&self, term: u64, index: u64) -> Option<Timespec> {
        let key = (term, index);
        let map = |p: &Proposal<_>| (p.term, p.index);
        let (front, back) = self.queue.as_slices();
        let idx = front
            .binary_search_by_key(&key, map)
            .or_else(|_| back.binary_search_by_key(&key, map));
        idx.ok().map(|i| self.queue[i].renew_lease_time).flatten()
    }

    // Find proposal in front or at the given term and index
    fn pop(&mut self, term: u64, index: u64) -> Option<Proposal<S>> {
        self.queue.pop_front().and_then(|p| {
            // Comparing the term first then the index, because the term is
            // increasing among all log entries and the index is increasing
            // inside a given term
            if (p.term, p.index) > (term, index) {
                self.queue.push_front(p);
                return None;
            }
            Some(p)
        })
    }

    /// Find proposal at the given term and index and notify stale proposals
    /// in front that term and index
    fn find_proposal(&mut self, term: u64, index: u64, current_term: u64) -> Option<Proposal<S>> {
        while let Some(p) = self.pop(term, index) {
            if p.term == term {
                if p.index == index {
                    return if p.cb.is_none() { None } else { Some(p) };
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        self.tag, term, p.index, index
                    );
                }
            } else {
                apply::notify_stale_req(current_term, p.cb);
            }
        }
        None
    }

    fn push(&mut self, p: Proposal<S>) {
        if let Some(f) = self.queue.back() {
            // The term must be increasing among all log entries and the index
            // must be increasing inside a given term
            assert!((p.term, p.index) > (f.term, f.index));
        }
        self.queue.push_back(p);
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn gc(&mut self) {
        if self.queue.capacity() > SHRINK_CACHE_CAPACITY && self.queue.len() < SHRINK_CACHE_CAPACITY
        {
            self.queue.shrink_to_fit();
        }
    }
}

bitflags! {
    // TODO: maybe declare it as protobuf struct is better.
    /// A bitmap contains some useful flags when dealing with `eraftpb::Entry`.
    pub struct ProposalContext: u8 {
        const SYNC_LOG       = 0b0000_0001;
        const SPLIT          = 0b0000_0010;
        const PREPARE_MERGE  = 0b0000_0100;
    }
}

impl ProposalContext {
    /// Converts itself to a vector.
    pub fn to_vec(self) -> Vec<u8> {
        if self.is_empty() {
            return vec![];
        }
        let ctx = self.bits();
        vec![ctx]
    }

    /// Initializes a `ProposalContext` from a byte slice.
    pub fn from_bytes(ctx: &[u8]) -> ProposalContext {
        if ctx.is_empty() {
            ProposalContext::empty()
        } else if ctx.len() == 1 {
            ProposalContext::from_bits_truncate(ctx[0])
        } else {
            panic!("invalid ProposalContext {:?}", ctx);
        }
    }
}

/// `ConsistencyState` is used for consistency check.
pub struct ConsistencyState {
    pub last_check_time: Instant,
    // (computed_result_or_to_be_verified, index, hash)
    pub index: u64,
    pub context: Vec<u8>,
    pub hash: Vec<u8>,
}

/// Statistics about raft peer.
#[derive(Default, Clone)]
pub struct PeerStat {
    pub written_bytes: u64,
    pub written_keys: u64,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct CheckTickResult {
    leader: bool,
    up_to_date: bool,
    reason: &'static str,
}

pub struct ProposedAdminCmd<S: Snapshot> {
    cmd_type: AdminCmdType,
    epoch_state: AdminCmdEpochState,
    index: u64,
    cbs: Vec<Callback<S>>,
}

impl<S: Snapshot> ProposedAdminCmd<S> {
    fn new(
        cmd_type: AdminCmdType,
        epoch_state: AdminCmdEpochState,
        index: u64,
    ) -> ProposedAdminCmd<S> {
        ProposedAdminCmd {
            cmd_type,
            epoch_state,
            index,
            cbs: Vec::new(),
        }
    }
}

struct CmdEpochChecker<S: Snapshot> {
    // Although it's a deque, because of the characteristics of the settings from `ADMIN_CMD_EPOCH_MAP`,
    // the max size of admin cmd is 2, i.e. split/merge and change peer.
    proposed_admin_cmd: VecDeque<ProposedAdminCmd<S>>,
    term: u64,
}

impl<S: Snapshot> Default for CmdEpochChecker<S> {
    fn default() -> CmdEpochChecker<S> {
        CmdEpochChecker {
            proposed_admin_cmd: VecDeque::new(),
            term: 0,
        }
    }
}

impl<S: Snapshot> CmdEpochChecker<S> {
    fn maybe_update_term(&mut self, term: u64) {
        assert!(term >= self.term);
        if term > self.term {
            self.term = term;
            for cmd in self.proposed_admin_cmd.drain(..) {
                for cb in cmd.cbs {
                    apply::notify_stale_req(term, cb);
                }
            }
        }
    }

    /// Check if the proposal can be proposed on the basis of its epoch and previous proposed admin cmds.
    ///
    /// Returns None if passing the epoch check, otherwise returns a index which is the last
    /// admin cmd index conflicted with this proposal.
    pub fn propose_check_epoch(&mut self, req: &RaftCmdRequest, term: u64) -> Option<u64> {
        self.maybe_update_term(term);
        let (check_ver, check_conf_ver) = if !req.has_admin_request() {
            (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
        } else {
            let cmd_type = req.get_admin_request().get_cmd_type();
            // Due to `test_admin_cmd_epoch_map_include_all_cmd_type`, using unwrap is ok.
            let epoch_state = *ADMIN_CMD_EPOCH_MAP.get(&cmd_type).unwrap();
            (epoch_state.check_ver, epoch_state.check_conf_ver)
        };
        self.last_conflict_index(check_ver, check_conf_ver)
    }

    pub fn post_propose(&mut self, cmd_type: AdminCmdType, index: u64, term: u64) {
        self.maybe_update_term(term);
        // Due to `test_admin_cmd_epoch_map_include_all_cmd_type`, using unwrap is ok.
        let epoch_state = *ADMIN_CMD_EPOCH_MAP.get(&cmd_type).unwrap();
        assert!(self
            .last_conflict_index(epoch_state.check_ver, epoch_state.check_conf_ver)
            .is_none());

        if epoch_state.change_conf_ver || epoch_state.change_ver {
            if let Some(cmd) = self.proposed_admin_cmd.back() {
                assert!(cmd.index < index);
            }
            self.proposed_admin_cmd
                .push_back(ProposedAdminCmd::new(cmd_type, epoch_state, index));
        }
    }

    fn last_conflict_index(&self, check_ver: bool, check_conf_ver: bool) -> Option<u64> {
        self.proposed_admin_cmd
            .iter()
            .rev()
            .find(|cmd| {
                (check_ver && cmd.epoch_state.change_ver)
                    || (check_conf_ver && cmd.epoch_state.change_conf_ver)
            })
            .map(|cmd| cmd.index)
    }

    /// Returns the last proposed admin cmd index.
    ///
    /// Note that the cmd of this type must change epoch otherwise it can not be
    /// recorded to `proposed_admin_cmd`.
    pub fn last_cmd_index(&mut self, cmd_type: AdminCmdType) -> Option<u64> {
        self.proposed_admin_cmd
            .iter()
            .rev()
            .find(|cmd| cmd.cmd_type == cmd_type)
            .map(|cmd| cmd.index)
    }

    pub fn advance_apply(&mut self, index: u64, term: u64, region: &metapb::Region) {
        self.maybe_update_term(term);
        while !self.proposed_admin_cmd.is_empty() {
            let cmd = self.proposed_admin_cmd.front_mut().unwrap();
            if cmd.index <= index {
                for cb in cmd.cbs.drain(..) {
                    let mut resp = cmd_resp::new_error(Error::EpochNotMatch(
                        format!(
                            "current epoch of region {} is {:?}",
                            region.get_id(),
                            region.get_region_epoch(),
                        ),
                        vec![region.to_owned()],
                    ));
                    cmd_resp::bind_term(&mut resp, term);
                    cb.invoke_with_response(resp);
                }
            } else {
                break;
            }
            self.proposed_admin_cmd.pop_front();
        }
    }

    pub fn attach_to_conflict_cmd(&mut self, index: u64, cb: Callback<S>) {
        if let Some(cmd) = self
            .proposed_admin_cmd
            .iter_mut()
            .rev()
            .find(|cmd| cmd.index == index)
        {
            cmd.cbs.push(cb);
        } else {
            panic!(
                "index {} can not found in proposed_admin_cmd, callback {:?}",
                index, cb
            );
        }
    }
}

impl<S: Snapshot> Drop for CmdEpochChecker<S> {
    fn drop(&mut self) {
        for state in self.proposed_admin_cmd.drain(..) {
            for cb in state.cbs {
                apply::notify_stale_req(self.term, cb);
            }
        }
    }
}

pub struct Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    /// The ID of the Region which this Peer belongs to.
    region_id: u64,
    // TODO: remove it once panic!() support slog fields.
    /// Peer_tag, "[region <region_id>] <peer_id>"
    pub tag: String,
    /// The Peer meta information.
    pub peer: metapb::Peer,

    /// The Raft state machine of this Peer.
    pub raft_group: RawNode<PeerStorage<EK, ER>>,
    /// The cache of meta information for Region's other Peers.
    peer_cache: RefCell<HashMap<u64, metapb::Peer>>,
    /// Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: HashMap<u64, Instant>,

    proposals: ProposalQueue<EK::Snapshot>,
    leader_missing_time: Option<Instant>,
    leader_lease: Lease,
    pending_reads: ReadIndexQueue<EK::Snapshot>,

    /// If it fails to send messages to leader.
    pub leader_unreachable: bool,
    /// Indicates whether the peer should be woken up.
    pub should_wake_up: bool,
    /// Whether this peer is destroyed asynchronously.
    /// If it's true,
    /// 1. when merging, its data in storeMeta will be removed early by the target peer.
    /// 2. all read requests must be rejected.
    pub pending_remove: bool,

    /// Record the instants of peers being added into the configuration.
    /// Remove them after they are not pending any more.
    pub peers_start_pending_time: Vec<(u64, Instant)>,
    /// A inaccurate cache about which peer is marked as down.
    down_peer_ids: Vec<u64>,

    /// An inaccurate difference in region size since last reset.
    /// It is used to decide whether split check is needed.
    pub size_diff_hint: u64,
    /// The count of deleted keys since last reset.
    delete_keys_hint: u64,
    /// An inaccurate difference in region size after compaction.
    /// It is used to trigger check split to update approximate size and keys after space reclamation
    /// of deleted entries.
    pub compaction_declined_bytes: u64,
    /// Approximate size of the region.
    pub approximate_size: Option<u64>,
    /// Approximate keys of the region.
    pub approximate_keys: Option<u64>,

    /// The state for consistency check.
    pub consistency_state: ConsistencyState,

    /// The counter records pending snapshot requests.
    pub pending_request_snapshot_count: Arc<AtomicUsize>,
    /// The index of last scheduled committed raft log.
    pub last_applying_idx: u64,
    /// The index of last compacted raft log. It is used for the next compact log task.
    pub last_compacted_idx: u64,
    /// The index of the latest urgent proposal index.
    last_urgent_proposal_idx: u64,
    /// The index of the latest committed split command.
    last_committed_split_idx: u64,
    /// Approximate size of logs that is applied but not compacted yet.
    pub raft_log_size_hint: u64,

    /// The index of the latest proposed prepare merge command.
    last_proposed_prepare_merge_idx: u64,
    /// The index of the latest committed prepare merge command.
    last_committed_prepare_merge_idx: u64,
    /// The merge related state. It indicates this Peer is in merging.
    pub pending_merge_state: Option<MergeState>,
    /// The rollback merge proposal can be proposed only when the number
    /// of peers is greater than the majority of all peers.
    /// There are more details in the annotation above
    /// `test_node_merge_write_data_to_source_region_after_merging`
    /// The peers who want to rollback merge.
    pub want_rollback_merge_peers: HashSet<u64>,
    /// Source region is catching up logs for merge.
    pub catch_up_logs: Option<CatchUpLogs>,

    /// Write Statistics for PD to schedule hot spot.
    pub peer_stat: PeerStat,

    /// Time of the last attempt to wake up inactive leader.
    pub bcast_wake_up_time: Option<UtilInstant>,
    /// Current replication mode version.
    pub replication_mode_version: u64,
    /// The required replication state at current version.
    pub dr_auto_sync_state: DrAutoSyncState,
    /// A flag that caches sync state. It's set to true when required replication
    /// state is reached for current region.
    pub replication_sync: bool,

    /// The known newest conf version and its corresponding peer list
    /// Send to these peers to check whether itself is stale.
    pub check_stale_conf_ver: u64,
    pub check_stale_peers: Vec<metapb::Peer>,
    /// Whether this peer is created by replication and is the first
    /// one of this region on local store.
    pub local_first_replicate: bool,

    pub txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,

    /// The max timestamp recorded in the concurrency manager is only updated at leader.
    /// So if a peer becomes leader from a follower, the max timestamp can be outdated.
    /// We need to update the max timestamp with a latest timestamp from PD before this
    /// peer can work.
    /// From the least significant to the most, 1 bit marks whether the timestamp is
    /// updated, 31 bits for the current epoch version, 32 bits for the current term.
    /// The version and term are stored to prevent stale UpdateMaxTimestamp task from
    /// marking the lowest bit.
    pub max_ts_sync_status: Arc<AtomicU64>,

    /// Check whether this proposal can be proposed based on its epoch.
    cmd_epoch_checker: CmdEpochChecker<EK::Snapshot>,

    /// The number of the last unpersisted ready.
    last_unpersisted_number: u64,

    /// The number of pending pd heartbeat tasks. Pd heartbeat task may be blocked by
    /// reading rocksdb. To avoid unnecessary io operations, we always let the later
    /// task run when there are more than 1 pending tasks.
    pub pending_pd_heartbeat_tasks: Arc<AtomicU64>,
}

impl<EK, ER> Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        store_id: u64,
        cfg: &Config,
        sched: Scheduler<RegionTask<EK::Snapshot>>,
        engines: Engines<EK, ER>,
        region: &metapb::Region,
        peer: metapb::Peer,
    ) -> Result<Peer<EK, ER>> {
        if peer.get_id() == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let tag = format!("[region {}] {}", region.get_id(), peer.get_id());

        let ps = PeerStorage::new(engines, region, sched, peer.get_id(), tag.clone())?;

        let applied_index = ps.applied_index();

        let raft_cfg = raft::Config {
            id: peer.get_id(),
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            min_election_tick: cfg.raft_min_election_timeout_ticks,
            max_election_tick: cfg.raft_max_election_timeout_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg.0,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            skip_bcast_commit: true,
            pre_vote: cfg.prevote,
            ..Default::default()
        };

        let logger = slog_global::get_global().new(slog::o!("region_id" => region.get_id()));
        let raft_group = RawNode::new(&raft_cfg, ps, &logger)?;
        let mut peer = Peer {
            peer,
            region_id: region.get_id(),
            raft_group,
            proposals: ProposalQueue::new(tag.clone()),
            pending_reads: Default::default(),
            peer_cache: RefCell::new(HashMap::default()),
            peer_heartbeats: HashMap::default(),
            peers_start_pending_time: vec![],
            down_peer_ids: vec![],
            size_diff_hint: 0,
            delete_keys_hint: 0,
            approximate_size: None,
            approximate_keys: None,
            compaction_declined_bytes: 0,
            leader_unreachable: false,
            pending_remove: false,
            should_wake_up: false,
            pending_merge_state: None,
            want_rollback_merge_peers: HashSet::default(),
            pending_request_snapshot_count: Arc::new(AtomicUsize::new(0)),
            last_proposed_prepare_merge_idx: 0,
            last_committed_prepare_merge_idx: 0,
            leader_missing_time: Some(Instant::now()),
            tag,
            last_applying_idx: applied_index,
            last_compacted_idx: 0,
            last_urgent_proposal_idx: u64::MAX,
            last_committed_split_idx: 0,
            consistency_state: ConsistencyState {
                last_check_time: Instant::now(),
                index: INVALID_INDEX,
                context: vec![],
                hash: vec![],
            },
            raft_log_size_hint: 0,
            leader_lease: Lease::new(cfg.raft_store_max_leader_lease()),
            peer_stat: PeerStat::default(),
            catch_up_logs: None,
            bcast_wake_up_time: None,
            replication_mode_version: 0,
            dr_auto_sync_state: DrAutoSyncState::Async,
            replication_sync: false,
            check_stale_conf_ver: 0,
            check_stale_peers: vec![],
            local_first_replicate: false,
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::Noop)),
            max_ts_sync_status: Arc::new(AtomicU64::new(0)),
            cmd_epoch_checker: Default::default(),
            last_unpersisted_number: 0,
            pending_pd_heartbeat_tasks: Arc::new(AtomicU64::new(0)),
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(peer)
    }

    /// Sets commit group to the peer.
    pub fn init_replication_mode(&mut self, state: &mut GlobalReplicationState) {
        debug!("init commit group"; "state" => ?state, "region_id" => self.region_id, "peer_id" => self.peer.id);
        if self.is_initialized() {
            let version = state.status().get_dr_auto_sync().state_id;
            let gb = state.calculate_commit_group(version, self.get_store().region().get_peers());
            self.raft_group.raft.assign_commit_groups(gb);
        }
        self.replication_sync = false;
        if state.status().get_mode() == ReplicationMode::Majority {
            self.raft_group.raft.enable_group_commit(false);
            self.replication_mode_version = 0;
            self.dr_auto_sync_state = DrAutoSyncState::Async;
            return;
        }
        self.replication_mode_version = state.status().get_dr_auto_sync().state_id;
        let enable = state.status().get_dr_auto_sync().get_state() != DrAutoSyncState::Async;
        self.raft_group.raft.enable_group_commit(enable);
        self.dr_auto_sync_state = state.status().get_dr_auto_sync().get_state();
    }

    /// Updates replication mode.
    pub fn switch_replication_mode(&mut self, state: &Mutex<GlobalReplicationState>) {
        self.replication_sync = false;
        let mut guard = state.lock().unwrap();
        let enable_group_commit = if guard.status().get_mode() == ReplicationMode::Majority {
            self.replication_mode_version = 0;
            self.dr_auto_sync_state = DrAutoSyncState::Async;
            false
        } else {
            self.dr_auto_sync_state = guard.status().get_dr_auto_sync().get_state();
            self.replication_mode_version = guard.status().get_dr_auto_sync().state_id;
            guard.status().get_dr_auto_sync().get_state() != DrAutoSyncState::Async
        };
        if enable_group_commit {
            let ids = mem::replace(
                guard.calculate_commit_group(
                    self.replication_mode_version,
                    self.region().get_peers(),
                ),
                Vec::with_capacity(self.region().get_peers().len()),
            );
            drop(guard);
            self.raft_group.raft.clear_commit_group();
            self.raft_group.raft.assign_commit_groups(&ids);
        } else {
            drop(guard);
        }
        self.raft_group
            .raft
            .enable_group_commit(enable_group_commit);
        info!("switch replication mode"; "version" => self.replication_mode_version, "region_id" => self.region_id, "peer_id" => self.peer.id);
    }

    /// Register self to apply_scheduler so that the peer is then usable.
    /// Also trigger `RegionChangeEvent::Create` here.
    pub fn activate<T>(&self, ctx: &PollContext<EK, ER, T>) {
        ctx.apply_router
            .schedule_task(self.region_id, ApplyTask::register(self));

        ctx.coprocessor_host.on_region_changed(
            self.region(),
            RegionChangeEvent::Create,
            self.get_role(),
        );
    }

    #[inline]
    fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    #[inline]
    pub fn get_index_term(&self, idx: u64) -> u64 {
        match self.raft_group.raft.raft_log.term(idx) {
            Ok(t) => t,
            Err(e) => panic!("{} fail to load term for {}: {:?}", self.tag, idx, e),
        }
    }

    pub fn maybe_append_merge_entries(&mut self, merge: &CommitMergeRequest) -> Option<u64> {
        let mut entries = merge.get_entries();
        if entries.is_empty() {
            // Though the entries is empty, it is possible that one source peer has caught up the logs
            // but commit index is not updated. If other source peers are already destroyed, so the raft
            // group will not make any progress, namely the source peer can not get the latest commit index anymore.
            // Here update the commit index to let source apply rest uncommitted entries.
            return if merge.get_commit() > self.raft_group.raft.raft_log.committed {
                self.raft_group.raft.raft_log.commit_to(merge.get_commit());
                Some(merge.get_commit())
            } else {
                None
            };
        }
        let first = entries.first().unwrap();
        // make sure message should be with index not smaller than committed
        let mut log_idx = first.get_index() - 1;
        debug!(
            "append merge entries";
            "log_index" => log_idx,
            "merge_commit" => merge.get_commit(),
            "commit_index" => self.raft_group.raft.raft_log.committed,
        );
        if log_idx < self.raft_group.raft.raft_log.committed {
            // There are maybe some logs not included in CommitMergeRequest's entries, like CompactLog,
            // so the commit index may exceed the last index of the entires from CommitMergeRequest.
            // If that, no need to append
            if self.raft_group.raft.raft_log.committed - log_idx > entries.len() as u64 {
                return None;
            }
            entries = &entries[(self.raft_group.raft.raft_log.committed - log_idx) as usize..];
            log_idx = self.raft_group.raft.raft_log.committed;
        }
        let log_term = self.get_index_term(log_idx);

        self.raft_group
            .raft
            .raft_log
            .maybe_append(log_idx, log_term, merge.get_commit(), entries)
            .map(|(_, last_index)| last_index)
    }

    /// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
    pub fn maybe_destroy<T>(&mut self, ctx: &PollContext<EK, ER, T>) -> Option<DestroyPeerJob> {
        if self.pending_remove {
            info!(
                "is being destroyed, skip";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return None;
        }
        {
            let meta = ctx.store_meta.lock().unwrap();
            if meta.atomic_snap_regions.contains_key(&self.region_id) {
                info!(
                    "stale peer is applying atomic snapshot, will destroy next time";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                return None;
            }
        }

        if self.is_applying_snapshot() && !self.mut_store().cancel_applying_snap() {
            info!(
                "stale peer is applying snapshot, will destroy next time";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return None;
        }

        self.pending_remove = true;

        Some(DestroyPeerJob {
            initialized: self.get_store().is_initialized(),
            region_id: self.region_id,
            peer: self.peer.clone(),
        })
    }

    /// Does the real destroy task which includes:
    /// 1. Set the region to tombstone;
    /// 2. Clear data;
    /// 3. Notify all pending requests.
    pub fn destroy<T>(&mut self, ctx: &PollContext<EK, ER, T>, keep_data: bool) -> Result<()> {
        fail_point!("raft_store_skip_destroy_peer", |_| Ok(()));
        let t = Instant::now();

        let region = self.region().clone();
        info!(
            "begin to destroy";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        // Set Tombstone state explicitly
        let mut kv_wb = ctx.engines.kv.write_batch();
        let mut raft_wb = ctx.engines.raft.log_batch(1024);
        self.mut_store().clear_meta(&mut kv_wb, &mut raft_wb)?;
        write_peer_state(
            &mut kv_wb,
            &region,
            PeerState::Tombstone,
            self.pending_merge_state.clone(),
        )?;
        // write kv rocksdb first in case of restart happen between two write
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        kv_wb.write_opt(&write_opts)?;
        ctx.engines.raft.consume(&mut raft_wb, true)?;

        if self.get_store().is_initialized() && !keep_data {
            // If we meet panic when deleting data and raft log, the dirty data
            // will be cleared by a newer snapshot applying or restart.
            if let Err(e) = self.get_store().clear_data() {
                error!(?e;
                    "failed to schedule clear data task";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
            }
        }

        self.pending_reads.clear_all(Some(region.get_id()));

        for Proposal { cb, .. } in self.proposals.queue.drain(..) {
            apply::notify_req_region_removed(region.get_id(), cb);
        }

        info!(
            "peer destroy itself";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "takes" => ?t.elapsed(),
        );

        Ok(())
    }

    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.get_store().is_initialized()
    }

    #[inline]
    pub fn region(&self) -> &metapb::Region {
        self.get_store().region()
    }

    /// Check whether the peer can be hibernated.
    ///
    /// This should be used with `check_after_tick` to get a correct conclusion.
    pub fn check_before_tick(&self, cfg: &Config) -> CheckTickResult {
        let mut res = CheckTickResult::default();
        if !self.is_leader() {
            return res;
        }
        res.leader = true;
        if self.raft_group.raft.election_elapsed + 1 < cfg.raft_election_timeout_ticks {
            return res;
        }
        let status = self.raft_group.status();
        let last_index = self.raft_group.raft.raft_log.last_index();
        for (id, pr) in status.progress.unwrap().iter() {
            // Even a recent inactive node is also considered. If we put leader into sleep,
            // followers or learners may not sync its logs for a long time and become unavailable.
            // We choose availability instead of performance in this case.
            if *id == self.peer.get_id() {
                continue;
            }
            if pr.matched != last_index {
                res.reason = "replication";
                return res;
            }
        }
        if self.raft_group.raft.pending_read_count() > 0 {
            res.reason = "pending read";
            return res;
        }
        if self.raft_group.raft.lead_transferee.is_some() {
            res.reason = "transfer leader";
            return res;
        }
        // Unapplied entries can change the configuration of the group.
        if self.get_store().applied_index() < last_index {
            res.reason = "unapplied";
            return res;
        }
        if self.replication_mode_need_catch_up() {
            res.reason = "replication mode";
            return res;
        }
        res.up_to_date = true;
        res
    }

    pub fn check_after_tick(&self, state: GroupState, res: CheckTickResult) -> bool {
        if res.leader {
            if res.up_to_date {
                self.is_leader()
            } else {
                if !res.reason.is_empty() {
                    debug!("rejecting sleeping"; "reason" => res.reason, "region_id" => self.region_id, "peer_id" => self.peer_id());
                }
                false
            }
        } else {
            // If follower keeps receiving data from leader, then it's safe to stop
            // ticking, as leader will make sure it has the latest logs.
            // Checking term to make sure campaign has finished and the leader starts
            // doing its job, it's not required but a safe options.
            state != GroupState::Chaos
                && self.has_valid_leader()
                && self.raft_group.raft.raft_log.last_term() == self.raft_group.raft.term
                && !self.has_unresolved_reads()
                // If it becomes leader, the stats is not valid anymore.
                && !self.is_leader()
        }
    }

    #[inline]
    pub fn has_valid_leader(&self) -> bool {
        if self.raft_group.raft.leader_id == raft::INVALID_ID {
            return false;
        }
        for p in self.region().get_peers() {
            if p.get_id() == self.raft_group.raft.leader_id && p.get_role() != PeerRole::Learner {
                return true;
            }
        }
        false
    }

    /// Pings if followers are still connected.
    ///
    /// Leader needs to know exact progress of followers, and
    /// followers just need to know whether leader is still alive.
    pub fn ping(&mut self) {
        if self.is_leader() {
            self.raft_group.ping();
        }
    }

    pub fn has_uncommitted_log(&self) -> bool {
        self.raft_group.raft.raft_log.committed < self.raft_group.raft.raft_log.last_index()
    }

    /// Set the region of a peer.
    ///
    /// This will update the region of the peer, caller must ensure the region
    /// has been preserved in a durable device.
    pub fn set_region(
        &mut self,
        host: &CoprocessorHost<impl KvEngine>,
        reader: &mut ReadDelegate,
        region: metapb::Region,
    ) {
        if self.region().get_region_epoch().get_version() < region.get_region_epoch().get_version()
        {
            // Epoch version changed, disable read on the localreader for this region.
            self.leader_lease.expire_remote_lease();
        }
        self.mut_store().set_region(region.clone());
        let progress = ReadProgress::region(region);
        // Always update read delegate's region to avoid stale region info after a follower
        // becoming a leader.
        self.maybe_update_read_progress(reader, progress);

        if !self.pending_remove {
            host.on_region_changed(self.region(), RegionChangeEvent::Update, self.get_role());
        }
    }

    #[inline]
    pub fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    #[inline]
    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn get_role(&self) -> StateRole {
        self.raft_group.raft.state
    }

    #[inline]
    pub fn get_store(&self) -> &PeerStorage<EK, ER> {
        self.raft_group.store()
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut PeerStorage<EK, ER> {
        self.raft_group.mut_store()
    }

    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        self.get_store().is_applying_snapshot()
    }

    /// Returns `true` if the raft group has replicated a snapshot but not committed it yet.
    #[inline]
    pub fn has_pending_snapshot(&self) -> bool {
        self.get_pending_snapshot().is_some()
    }

    #[inline]
    pub fn get_pending_snapshot(&self) -> Option<&eraftpb::Snapshot> {
        self.raft_group.snap()
    }

    fn add_ready_metric(&self, ready: &Ready, metrics: &mut RaftReadyMetrics) {
        metrics.message += ready.messages().len() as u64;
        metrics.commit += ready.committed_entries().len() as u64;
        metrics.append += ready.entries().len() as u64;

        if !ready.snapshot().is_empty() {
            metrics.snapshot += 1;
        }
    }

    fn add_light_ready_metric(&self, light_ready: &LightReady, metrics: &mut RaftReadyMetrics) {
        metrics.message += light_ready.messages().len() as u64;
        metrics.commit += light_ready.committed_entries().len() as u64;
    }

    #[inline]
    pub fn in_joint_state(&self) -> bool {
        self.region().get_peers().iter().any(|p| {
            p.get_role() == PeerRole::IncomingVoter || p.get_role() == PeerRole::DemotingVoter
        })
    }

    #[inline]
    fn send<T, I>(&mut self, trans: &mut T, msgs: I, metrics: &mut RaftMessageMetrics)
    where
        T: Transport,
        I: IntoIterator<Item = eraftpb::Message>,
    {
        for msg in msgs {
            let msg_type = msg.get_msg_type();
            match msg_type {
                MessageType::MsgAppend => metrics.append += 1,
                MessageType::MsgAppendResponse => {
                    if msg.get_request_snapshot() != raft::INVALID_INDEX {
                        metrics.request_snapshot += 1;
                    }
                    metrics.append_resp += 1;
                }
                MessageType::MsgRequestPreVote => metrics.prevote += 1,
                MessageType::MsgRequestPreVoteResponse => metrics.prevote_resp += 1,
                MessageType::MsgRequestVote => metrics.vote += 1,
                MessageType::MsgRequestVoteResponse => metrics.vote_resp += 1,
                MessageType::MsgSnapshot => metrics.snapshot += 1,
                MessageType::MsgHeartbeat => metrics.heartbeat += 1,
                MessageType::MsgHeartbeatResponse => metrics.heartbeat_resp += 1,
                MessageType::MsgTransferLeader => metrics.transfer_leader += 1,
                MessageType::MsgReadIndex => metrics.read_index += 1,
                MessageType::MsgReadIndexResp => metrics.read_index_resp += 1,
                MessageType::MsgTimeoutNow => {
                    // After a leader transfer procedure is triggered, the lease for
                    // the old leader may be expired earlier than usual, since a new leader
                    // may be elected and the old leader doesn't step down due to
                    // network partition from the new leader.
                    // For lease safety during leader transfer, transit `leader_lease`
                    // to suspect.
                    self.leader_lease.suspect(monotonic_raw_now());

                    metrics.timeout_now += 1;
                }
                // We do not care about these message types for metrics.
                // Explicitly declare them so when we add new message types we are forced to
                // decide.
                MessageType::MsgHup
                | MessageType::MsgBeat
                | MessageType::MsgPropose
                | MessageType::MsgUnreachable
                | MessageType::MsgSnapStatus
                | MessageType::MsgCheckQuorum => {}
            }
            self.send_raft_message(msg, trans);
        }
    }

    /// Steps the raft message.
    pub fn step<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        mut m: eraftpb::Message,
    ) -> Result<()> {
        fail_point!(
            "step_message_3_1",
            self.peer.get_store_id() == 3 && self.region_id == 1,
            |_| Ok(())
        );
        if self.is_leader() && m.get_from() != INVALID_ID {
            self.peer_heartbeats.insert(m.get_from(), Instant::now());
            // As the leader we know we are not missing.
            self.leader_missing_time.take();
        } else if m.get_from() == self.leader_id() {
            // As another role know we're not missing.
            self.leader_missing_time.take();
        }
        let msg_type = m.get_msg_type();
        if msg_type == MessageType::MsgReadIndex {
            ctx.coprocessor_host.on_step_read_index(&mut m);
            // Must use the commit index of `PeerStorage` instead of the commit index
            // in raft-rs which may be greater than the former one.
            // For more details, see the annotations above `on_leader_commit_idx_changed`.
            let index = self.get_store().commit_index();
            // Check if the log term of this index is equal to current term, if so,
            // this index can be used to reply the read index request if the leader holds
            // the lease. Please also take a look at raft-rs.
            if self.get_store().term(index).unwrap() == self.term() {
                let state = self.inspect_lease();
                if let LeaseState::Valid = state {
                    // If current peer has valid lease, then we could handle the
                    // request directly, rather than send a heartbeat to check quorum.
                    let mut resp = eraftpb::Message::default();
                    resp.set_msg_type(MessageType::MsgReadIndexResp);
                    resp.term = self.term();
                    resp.to = m.from;

                    resp.index = index;
                    resp.set_entries(m.take_entries());

                    self.raft_group.raft.msgs.push(resp);
                    return Ok(());
                }
                self.should_wake_up = state == LeaseState::Expired;
            }
        } else if msg_type == MessageType::MsgTransferLeader {
            self.execute_transfer_leader(ctx, &m);
            return Ok(());
        }

        let from_id = m.get_from();
        let has_snap_task = self.get_store().has_gen_snap_task();
        self.raft_group.step(m)?;

        let mut for_balance = false;
        if !has_snap_task && self.get_store().has_gen_snap_task() {
            if let Some(progress) = self.raft_group.status().progress {
                if let Some(pr) = progress.get(from_id) {
                    // When a peer is uninitialized (e.g. created by load balance),
                    // the last index of the peer is 0 which makes the matched index to be 0.
                    if pr.matched == 0 {
                        for_balance = true;
                    }
                }
            }
        }
        if for_balance {
            if let Some(gen_task) = self.mut_store().mut_gen_snap_task() {
                gen_task.set_for_balance();
            }
        }
        Ok(())
    }

    /// Checks and updates `peer_heartbeats` for the peer.
    pub fn check_peers(&mut self) {
        if !self.is_leader() {
            self.peer_heartbeats.clear();
            self.peers_start_pending_time.clear();
            return;
        }

        if self.peer_heartbeats.len() == self.region().get_peers().len() {
            return;
        }

        // Insert heartbeats in case that some peers never response heartbeats.
        let region = self.raft_group.store().region();
        for peer in region.get_peers() {
            self.peer_heartbeats
                .entry(peer.get_id())
                .or_insert_with(Instant::now);
        }
    }

    /// Collects all down peers.
    pub fn collect_down_peers(&mut self, max_duration: Duration) -> Vec<PeerStats> {
        let mut down_peers = Vec::new();
        let mut down_peer_ids = Vec::new();
        for p in self.region().get_peers() {
            if p.get_id() == self.peer.get_id() {
                continue;
            }
            if let Some(instant) = self.peer_heartbeats.get(&p.get_id()) {
                if instant.elapsed() >= max_duration {
                    let mut stats = PeerStats::default();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(instant.elapsed().as_secs());
                    down_peers.push(stats);
                    down_peer_ids.push(p.get_id());
                }
            }
        }
        self.down_peer_ids = down_peer_ids;
        down_peers
    }

    /// Collects all pending peers and update `peers_start_pending_time`.
    pub fn collect_pending_peers<T>(&mut self, ctx: &PollContext<EK, ER, T>) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group.status();
        let truncated_idx = self.get_store().truncated_index();

        if status.progress.is_none() {
            return pending_peers;
        }

        let progresses = status.progress.unwrap().iter();
        for (&id, progress) in progresses {
            if id == self.peer.get_id() {
                continue;
            }
            // The `matched` is 0 only in these two cases:
            // 1. Current leader hasn't communicated with this peer.
            // 2. This peer does not exist yet(maybe it is created but not initialized)
            //
            // The correctness of region merge depends on the fact that all target peers must exist during merging.
            // (PD rely on `pending_peers` to check whether all target peers exist)
            //
            // So if the `matched` is 0, it must be a pending peer.
            // It can be ensured because `truncated_index` must be greater than `RAFT_INIT_LOG_INDEX`(5).
            if progress.matched < truncated_idx {
                if let Some(p) = self.get_peer_from_cache(id) {
                    pending_peers.push(p);
                    if !self
                        .peers_start_pending_time
                        .iter()
                        .any(|&(pid, _)| pid == id)
                    {
                        let now = Instant::now();
                        self.peers_start_pending_time.push((id, now));
                        debug!(
                            "peer start pending";
                            "region_id" => self.region_id,
                            "peer_id" => self.peer.get_id(),
                            "time" => ?now,
                        );
                    }
                } else {
                    if ctx.cfg.dev_assert {
                        panic!("{} failed to get peer {} from cache", self.tag, id);
                    }
                    error!(
                        "failed to get peer from cache";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "get_peer_id" => id,
                    );
                }
            }
        }
        pending_peers
    }

    /// Returns `true` if any peer recover from connectivity problem.
    ///
    /// A peer can become pending or down if it has not responded for a
    /// long time. If it becomes normal again, PD need to be notified.
    pub fn any_new_peer_catch_up(&mut self, peer_id: u64) -> bool {
        if self.peers_start_pending_time.is_empty() && self.down_peer_ids.is_empty() {
            return false;
        }
        if !self.is_leader() {
            self.down_peer_ids = vec![];
            self.peers_start_pending_time = vec![];
            return false;
        }
        for i in 0..self.peers_start_pending_time.len() {
            if self.peers_start_pending_time[i].0 != peer_id {
                continue;
            }
            let truncated_idx = self.raft_group.store().truncated_index();
            if let Some(progress) = self.raft_group.raft.prs().get(peer_id) {
                if progress.matched >= truncated_idx {
                    let (_, pending_after) = self.peers_start_pending_time.swap_remove(i);
                    let elapsed = duration_to_sec(pending_after.elapsed());
                    debug!(
                        "peer has caught up logs";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "takes" => elapsed,
                    );
                    return true;
                }
            }
        }
        if self.down_peer_ids.contains(&peer_id) {
            return true;
        }
        false
    }

    pub fn check_stale_state<T>(&mut self, ctx: &mut PollContext<EK, ER, T>) -> StaleState {
        if self.is_leader() {
            // Leaders always have valid state.
            //
            // We update the leader_missing_time in the `fn step`. However one peer region
            // does not send any raft messages, so we have to check and update it before
            // reporting stale states.
            self.leader_missing_time = None;
            return StaleState::Valid;
        }
        let naive_peer = !self.is_initialized() || !self.raft_group.raft.promotable();
        // Updates the `leader_missing_time` according to the current state.
        //
        // If we are checking this it means we suspect the leader might be missing.
        // Mark down the time when we are called, so we can check later if it's been longer than it
        // should be.
        match self.leader_missing_time {
            None => {
                self.leader_missing_time = Instant::now().into();
                StaleState::Valid
            }
            Some(instant) if instant.elapsed() >= ctx.cfg.max_leader_missing_duration.0 => {
                // Resets the `leader_missing_time` to avoid sending the same tasks to
                // PD worker continuously during the leader missing timeout.
                self.leader_missing_time = Instant::now().into();
                StaleState::ToValidate
            }
            Some(instant)
                if instant.elapsed() >= ctx.cfg.abnormal_leader_missing_duration.0
                    && !naive_peer =>
            {
                // A peer is considered as in the leader missing state
                // if it's initialized but is isolated from its leader or
                // something bad happens that the raft group can not elect a leader.
                StaleState::LeaderMissing
            }
            _ => StaleState::Valid,
        }
    }

    fn on_role_changed<T>(&mut self, ctx: &mut PollContext<EK, ER, T>, ready: &Ready) {
        // Update leader lease when the Raft state changes.
        if let Some(ss) = ready.ss() {
            match ss.raft_state {
                StateRole::Leader => {
                    // The local read can only be performed after a new leader has applied
                    // the first empty entry on its term. After that the lease expiring time
                    // should be updated to
                    //   send_to_quorum_ts + max_lease
                    // as the comments in `Lease` explain.
                    // It is recommended to update the lease expiring time right after
                    // this peer becomes leader because it's more convenient to do it here and
                    // it has no impact on the correctness.
                    let progress_term = ReadProgress::term(self.term());
                    self.maybe_renew_leader_lease(monotonic_raw_now(), ctx, Some(progress_term));
                    debug!(
                        "becomes leader with lease";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "lease" => ?self.leader_lease,
                    );
                    // If the predecessor reads index during transferring leader and receives
                    // quorum's heartbeat response after that, it may wait for applying to
                    // current term to apply the read. So broadcast eagerly to avoid unexpected
                    // latency.
                    //
                    // TODO: Maybe the predecessor should just drop all the read requests directly?
                    // All the requests need to be redirected in the end anyway and executing
                    // prewrites or commits will be just a waste.
                    self.last_urgent_proposal_idx = self.raft_group.raft.raft_log.last_index();
                    self.raft_group.skip_bcast_commit(false);

                    // A more recent read may happen on the old leader. So max ts should
                    // be updated after a peer becomes leader.
                    self.require_updating_max_ts(&ctx.pd_scheduler);
                }
                StateRole::Follower => {
                    self.leader_lease.expire();
                }
                _ => {}
            }
            self.on_leader_changed(ctx, ss.leader_id, self.term());
            ctx.coprocessor_host
                .on_role_change(self.region(), ss.raft_state);
            self.cmd_epoch_checker.maybe_update_term(self.term());
        } else if ready.must_sync() {
            match ready.hs() {
                Some(hs) if hs.get_term() != self.get_store().hard_state().get_term() => {
                    self.on_leader_changed(ctx, self.leader_id(), hs.get_term());
                }
                _ => (),
            }
        }
    }

    /// Correctness depends on the order between calling this function and notifying other peers
    /// the new commit index.
    /// It is due to the interaction between lease and split/merge.(details are decribed below)
    ///
    /// Note that in addition to the hearbeat/append msg, the read index response also can notify
    /// other peers the new commit index. There are three place where TiKV handles read index resquest.
    /// The first place is in raft-rs, so it's like hearbeat/append msg, call this function and
    /// then send the response. The second place is in `Step`, we should use the commit index
    /// of `PeerStorage` which is the greatest commit index that can be observed outside.
    /// The third place is in `read_index`, handle it like the second one.
    fn on_leader_commit_idx_changed(&mut self, pre_commit_index: u64, commit_index: u64) {
        if commit_index <= pre_commit_index || !self.is_leader() {
            return;
        }

        // The admin cmds in `CmdEpochChecker` are proposed by the current leader so we can
        // use it to get the split/prepare-merge cmds which was committed just now.

        // BatchSplit and Split cmd are mutually exclusive because they both change epoch's
        // version so only one of them can be proposed and the other one will be rejected
        // by `CmdEpochChecker`.
        let last_split_idx = self
            .cmd_epoch_checker
            .last_cmd_index(AdminCmdType::BatchSplit)
            .or_else(|| self.cmd_epoch_checker.last_cmd_index(AdminCmdType::Split));
        if let Some(idx) = last_split_idx {
            if idx > pre_commit_index && idx <= commit_index {
                // We don't need to suspect its lease because peers of new region that
                // in other store do not start election before theirs election timeout
                // which is longer than the max leader lease.
                // It's safe to read local within its current lease, however, it's not
                // safe to renew its lease.
                self.last_committed_split_idx = idx;
            }
        } else {
            // BatchSplit/Split and PrepareMerge cmd are mutually exclusive too.
            // So if there is no Split cmd, we should check PrepareMerge cmd.
            let last_prepare_merge_idx = self
                .cmd_epoch_checker
                .last_cmd_index(AdminCmdType::PrepareMerge);
            if let Some(idx) = last_prepare_merge_idx {
                if idx > pre_commit_index && idx <= commit_index {
                    // We committed prepare merge, to prevent unsafe read index,
                    // we must record its index.
                    self.last_committed_prepare_merge_idx = idx;
                    // After prepare_merge is committed and the leader broadcasts commit
                    // index to followers, the leader can not know when the target region
                    // merges majority of this region, also it can not know when the target
                    // region writes new values.
                    // To prevent unsafe local read, we suspect its leader lease.
                    self.leader_lease.suspect(monotonic_raw_now());
                }
            }
        }
    }

    fn on_leader_changed<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        leader_id: u64,
        term: u64,
    ) {
        debug!(
            "insert leader info to meta";
            "region_id" => self.region_id,
            "leader_id" => leader_id,
            "term" => term,
            "peer_id" => self.peer_id(),
        );
        let mut meta = ctx.store_meta.lock().unwrap();
        meta.leaders.insert(self.region_id, (term, leader_id));
    }

    #[inline]
    pub fn ready_to_handle_pending_snap(&self) -> bool {
        // If apply worker is still working, written apply state may be overwritten
        // by apply worker. So we have to wait here.
        // Please note that commit_index can't be used here. When applying a snapshot,
        // a stale heartbeat can make the leader think follower has already applied
        // the snapshot, and send remaining log entries, which may increase commit_index.
        // TODO: add more test
        self.last_applying_idx == self.get_store().applied_index()
            // Requesting snapshots also triggers apply workers to write
            // apply states even if there is no pending committed entry.
            // TODO: Instead of sharing the counter, we should apply snapshots
            //       in apply workers.
            && self.pending_request_snapshot_count.load(Ordering::SeqCst) == 0
    }

    #[inline]
    fn ready_to_handle_read(&self) -> bool {
        // TODO: It may cause read index to wait a long time.

        // There may be some values that are not applied by this leader yet but the old leader,
        // if applied_index_term isn't equal to current term.
        self.get_store().applied_index_term() == self.term()
            // There may be stale read if the old leader splits really slow,
            // the new region may already elected a new leader while
            // the old leader still think it owns the split range.
            && !self.is_splitting()
            // There may be stale read if a target leader is in another store and
            // applied commit merge, written new values, but the sibling peer in
            // this store does not apply commit merge, so the leader is not ready
            // to read, until the merge is rollbacked.
            && !self.is_merging()
    }

    fn ready_to_handle_unsafe_replica_read(&self, read_index: u64) -> bool {
        // Wait until the follower applies all values before the read. There is still a
        // problem if the leader applies fewer values than the follower, the follower read
        // could get a newer value, and after that, the leader may read a stale value,
        // which violates linearizability.
        self.get_store().applied_index() >= read_index
            // If it is in pending merge state(i.e. applied PrepareMerge), the data may be stale.
            // TODO: Add a test to cover this case
            && self.pending_merge_state.is_none()
            // a peer which is applying snapshot will clean up its data and ingest a snapshot file,
            // during between the two operations a replica read could read empty data.
            && !self.is_applying_snapshot()
    }

    #[inline]
    fn is_splitting(&self) -> bool {
        self.last_committed_split_idx > self.get_store().applied_index()
    }

    #[inline]
    fn is_merging(&self) -> bool {
        self.last_committed_prepare_merge_idx > self.get_store().applied_index()
            || self.pending_merge_state.is_some()
    }

    // Checks merge strictly, it checks whether there is any ongoing merge by
    // tracking last proposed prepare merge.
    // TODO: There is a false positives, proposed prepare merge may never be
    //       committed.
    fn is_merging_strict(&self) -> bool {
        self.last_proposed_prepare_merge_idx > self.get_store().applied_index() || self.is_merging()
    }

    // Check if this peer can handle request_snapshot.
    pub fn ready_to_handle_request_snapshot(&mut self, request_index: u64) -> bool {
        let reject_reason = if !self.is_leader() {
            // Only leader can handle request snapshot.
            "not_leader"
        } else if self.get_store().applied_index_term() != self.term()
            || self.get_store().applied_index() < request_index
        {
            // Reject if there are any unapplied raft log.
            // We don't want to handle request snapshot if there is any ongoing
            // merge, because it is going to be destroyed. This check prevents
            // handling request snapshot after leadership being transferred.
            "stale_apply"
        } else if self.is_merging_strict() || self.is_splitting() {
            // Reject if it is merging or splitting.
            // `is_merging_strict` also checks last proposed prepare merge, it
            // prevents handling request snapshot while a prepare merge going
            // to be committed.
            "split_merge"
        } else {
            return true;
        };

        info!("can not handle request snapshot";
            "reason" => reject_reason,
            "region_id" => self.region().get_id(),
            "peer_id" => self.peer_id(),
            "request_index" => request_index);
        false
    }

    /// Checks if leader needs to keep sending logs for follower.
    ///
    /// In DrAutoSync mode, if leader goes to sleep before the region is sync,
    /// PD may wait longer time to reach sync state.
    pub fn replication_mode_need_catch_up(&self) -> bool {
        self.replication_mode_version > 0
            && self.dr_auto_sync_state != DrAutoSyncState::Async
            && !self.replication_sync
    }

    pub fn handle_raft_ready_append<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
    ) -> Option<CollectedReady> {
        if self.pending_remove {
            return None;
        }
        match self.mut_store().check_applying_snap() {
            CheckApplyingSnapStatus::Applying => {
                // If this peer is applying snapshot, we should not get a new ready.
                // There are two reasons in my opinion:
                //   1. If we handle a new ready and persist the data(e.g. entries),
                //      we can not tell raft-rs that this ready has been persisted because
                //      the ready need to be persisted one by one from raft-rs's view.
                //   2. When this peer is applying snapshot, the response msg should not
                //      be sent to leader, thus the leader will not send new entries to
                //      this peer. Although it's possible a new leader may send a AppendEntries
                //      msg to this peer, this possibility is very low. In most cases, there
                //      is no msg need to be handled.
                // So we choose to not get a new ready which makes the logic more clear.
                debug!(
                    "still applying snapshot, skip further handling";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                return None;
            }
            CheckApplyingSnapStatus::Success => {
                // 0 means snapshot is scheduled after being restarted.
                if self.last_unpersisted_number != 0 {
                    // Because we only handle raft ready when not applying snapshot, so following
                    // line won't be called twice for the same snapshot.
                    self.raft_group.advance_apply_to(self.last_applying_idx);
                    self.cmd_epoch_checker.advance_apply(
                        self.last_applying_idx,
                        self.term(),
                        self.raft_group.store().region(),
                    );
                }
                self.post_pending_read_index_on_replica(ctx);
            }
            CheckApplyingSnapStatus::Idle => {}
        }

        let mut destroy_regions = vec![];
        if self.has_pending_snapshot() {
            if !self.ready_to_handle_pending_snap() {
                let count = self.pending_request_snapshot_count.load(Ordering::SeqCst);
                debug!(
                    "not ready to apply snapshot";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "apply_index" => self.get_store().applied_index(),
                    "last_applying_index" => self.last_applying_idx,
                    "pending_request_snapshot_count" => count,
                );
                return None;
            }

            let meta = ctx.store_meta.lock().unwrap();
            // For merge process, the stale source peer is destroyed asynchronously when applying
            // snapshot or creating new peer. So here checks whether there is any overlap, if so,
            // wait and do not handle raft ready.
            if let Some(wait_destroy_regions) = meta.atomic_snap_regions.get(&self.region_id) {
                for (source_region_id, is_ready) in wait_destroy_regions {
                    if !is_ready {
                        info!(
                            "snapshot range overlaps, wait source destroy finish";
                            "region_id" => self.region_id,
                            "peer_id" => self.peer.get_id(),
                            "apply_index" => self.get_store().applied_index(),
                            "last_applying_index" => self.last_applying_idx,
                            "overlap_region_id" => source_region_id,
                        );
                        return None;
                    }
                    destroy_regions.push(meta.regions[source_region_id].clone());
                }
            }
        }

        if !self.raft_group.has_ready() {
            // Generating snapshot task won't set ready for raft group.
            if let Some(gen_task) = self.mut_store().take_gen_snap_task() {
                self.pending_request_snapshot_count
                    .fetch_add(1, Ordering::SeqCst);
                ctx.apply_router
                    .schedule_task(self.region_id, ApplyTask::Snapshot(gen_task));
            }
            return None;
        }

        fail_point!(
            "before_handle_raft_ready_1003",
            self.peer.get_id() == 1003 && self.is_leader(),
            |_| None
        );

        fail_point!(
            "before_handle_snapshot_ready_3",
            self.peer.get_id() == 3 && self.get_pending_snapshot().is_some(),
            |_| None
        );

        debug!(
            "handle raft ready";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        let mut ready = self.raft_group.ready();

        self.last_unpersisted_number = ready.number();

        if !ready.must_sync() {
            // If this ready need not to sync, the term, vote must not be changed,
            // entries and snapshot must be empty.
            if let Some(hs) = ready.hs() {
                assert_eq!(hs.get_term(), self.get_store().hard_state().get_term());
                assert_eq!(hs.get_vote(), self.get_store().hard_state().get_vote());
            }
            assert!(ready.entries().is_empty());
            assert!(ready.snapshot().is_empty());
        }

        self.add_ready_metric(&ready, &mut ctx.raft_metrics.ready);

        self.on_role_changed(ctx, &ready);

        if let Some(hs) = ready.hs() {
            let pre_commit_index = self.get_store().commit_index();
            assert!(hs.get_commit() >= pre_commit_index);
            if self.is_leader() {
                self.on_leader_commit_idx_changed(pre_commit_index, hs.get_commit());
            }
        }

        if !ready.messages().is_empty() {
            if !self.is_leader() {
                fail_point!("raft_before_follower_send");
            }
            for vec_msg in ready.take_messages() {
                self.send(&mut ctx.trans, vec_msg, &mut ctx.raft_metrics.message);
            }
        }

        self.apply_reads(ctx, &ready);

        if !ready.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, ready.take_committed_entries());
        }

        let invoke_ctx = match self
            .mut_store()
            .handle_raft_ready(ctx, &mut ready, destroy_regions)
        {
            Ok(r) => r,
            Err(e) => {
                // We may have written something to writebatch and it can't be reverted, so has
                // to panic here.
                panic!("{} failed to handle raft ready: {:?}", self.tag, e)
            }
        };

        Some(CollectedReady::new(invoke_ctx, ready))
    }

    pub fn post_raft_ready_append<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        invoke_ctx: InvokeContext,
    ) -> Option<ApplySnapResult> {
        if invoke_ctx.has_snapshot() {
            // When apply snapshot, there is no log applied and not compacted yet.
            self.raft_log_size_hint = 0;
        }

        let apply_snap_result = self.mut_store().post_ready(invoke_ctx);
        if apply_snap_result.is_some() {
            // The peer may change from learner to voter after snapshot applied.
            let peer = self
                .region()
                .get_peers()
                .iter()
                .find(|p| p.get_id() == self.peer.get_id())
                .unwrap()
                .clone();
            if peer != self.peer {
                info!(
                    "meta changed in applying snapshot";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "before" => ?self.peer,
                    "after" => ?peer,
                );
                self.peer = peer;
            };
        }

        if apply_snap_result.is_some() {
            self.activate(ctx);
            let mut meta = ctx.store_meta.lock().unwrap();
            meta.readers
                .insert(self.region_id, ReadDelegate::from_peer(self));
        }

        apply_snap_result
    }

    pub fn handle_raft_committed_entries<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        committed_entries: Vec<Entry>,
    ) {
        assert!(
            !self.is_applying_snapshot(),
            "{} is applying snapshot when it is ready to handle committed entries",
            self.tag
        );
        if !committed_entries.is_empty() {
            // We must renew current_time because this value may be created a long time ago.
            // If we do not renew it, this time may be smaller than propose_time of a command,
            // which was proposed in another thread while this thread receives its AppendEntriesResponse
            // and is ready to calculate its commit-log-duration.
            ctx.current_time.replace(monotonic_raw_now());
        }
        // Leader needs to update lease.
        let mut lease_to_be_updated = self.is_leader();
        for entry in committed_entries.iter().rev() {
            // raft meta is very small, can be ignored.
            self.raft_log_size_hint += entry.get_data().len() as u64;
            if lease_to_be_updated {
                let propose_time = self
                    .proposals
                    .find_propose_time(entry.get_term(), entry.get_index());
                if let Some(propose_time) = propose_time {
                    ctx.raft_metrics.commit_log.observe(duration_to_sec(
                        (ctx.current_time.unwrap() - propose_time).to_std().unwrap(),
                    ));
                    self.maybe_renew_leader_lease(propose_time, ctx, None);
                    lease_to_be_updated = false;
                }
            }

            fail_point!(
                "leader_commit_prepare_merge",
                {
                    let ctx = ProposalContext::from_bytes(&entry.context);
                    self.is_leader()
                        && entry.term == self.term()
                        && ctx.contains(ProposalContext::PREPARE_MERGE)
                },
                |_| {}
            );
        }
        if let Some(last_entry) = committed_entries.last() {
            self.last_applying_idx = last_entry.get_index();
            if self.last_applying_idx >= self.last_urgent_proposal_idx {
                // Urgent requests are flushed, make it lazy again.
                self.raft_group.skip_bcast_commit(true);
                self.last_urgent_proposal_idx = u64::MAX;
            }
            let cbs = if !self.proposals.is_empty() {
                let current_term = self.term();
                let cbs = committed_entries
                    .iter()
                    .filter_map(|e| {
                        self.proposals
                            .find_proposal(e.get_term(), e.get_index(), current_term)
                    })
                    .map(|mut p| {
                        if p.must_pass_epoch_check {
                            // In this case the apply can be guaranteed to be successful. Invoke the
                            // on_committed callback if necessary.
                            p.cb.invoke_committed();
                        }
                        p
                    })
                    .collect();
                self.proposals.gc();
                cbs
            } else {
                vec![]
            };
            let apply = Apply::new(
                self.peer_id(),
                self.region_id,
                self.term(),
                committed_entries,
                cbs,
            );
            ctx.apply_router
                .schedule_task(self.region_id, ApplyTask::apply(apply));
        }
        fail_point!("after_send_to_apply_1003", self.peer_id() == 1003, |_| {});
        // Check whether there is a pending generate snapshot task, the task
        // needs to be sent to the apply system.
        // Always sending snapshot task behind apply task, so it gets latest
        // snapshot.
        // TODO: maybe we should move this code to other place to make the logic more clear.
        if let Some(gen_task) = self.mut_store().take_gen_snap_task() {
            self.pending_request_snapshot_count
                .fetch_add(1, Ordering::SeqCst);
            ctx.apply_router
                .schedule_task(self.region_id, ApplyTask::Snapshot(gen_task));
        }
    }

    pub fn handle_raft_ready_advance<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        ready: Ready,
    ) {
        assert_eq!(ready.number(), self.last_unpersisted_number);
        if !ready.snapshot().is_empty() {
            // Snapshot's metadata has been applied.
            self.last_applying_idx = self.get_store().truncated_index();
            self.raft_group.advance_append_async(ready);
            // The ready is persisted, but we don't want to handle following light
            // ready immediately to avoid flow out of control, so use
            // `on_persist_ready` instead of `advance_append`.
            // We don't need to set `has_ready` to true, as snapshot is always
            // checked when ticking.
            self.raft_group
                .on_persist_ready(self.last_unpersisted_number);
            return;
        }

        let mut light_rd = self.raft_group.advance_append(ready);

        self.add_light_ready_metric(&light_rd, &mut ctx.raft_metrics.ready);

        if let Some(commit_index) = light_rd.commit_index() {
            let pre_commit_index = self.get_store().commit_index();
            assert!(commit_index >= pre_commit_index);
            // No need to persist the commit index but the one in memory
            // (i.e. commit of hardstate in PeerStorage) should be updated.
            self.mut_store().set_commit_index(commit_index);
            if self.is_leader() {
                self.on_leader_commit_idx_changed(pre_commit_index, commit_index);
            }
        }

        if !light_rd.messages().is_empty() {
            if !self.is_leader() {
                fail_point!("raft_before_follower_send");
            }
            for vec_msg in light_rd.take_messages() {
                self.send(&mut ctx.trans, vec_msg, &mut ctx.raft_metrics.message);
            }
        }

        if !light_rd.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, light_rd.take_committed_entries());
        }
    }

    fn response_read<T>(
        &self,
        read: &mut ReadIndexRequest<EK::Snapshot>,
        ctx: &mut PollContext<EK, ER, T>,
        replica_read: bool,
    ) {
        debug!(
            "handle reads with a read index";
            "request_id" => ?read.id,
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
        for (req, cb, mut read_index) in read.cmds.drain(..) {
            // leader reports key is locked
            if let Some(locked) = read.locked.take() {
                let mut response = raft_cmdpb::Response::default();
                response.mut_read_index().set_locked(*locked);
                let mut cmd_resp = RaftCmdResponse::default();
                cmd_resp.mut_responses().push(response);
                cb.invoke_read(ReadResponse {
                    response: cmd_resp,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
                continue;
            }
            if !replica_read {
                if read_index.is_none() {
                    // Actually, the read_index is none if and only if it's the first one in read.cmds.
                    // Starting from the second, all the following ones' read_index is not none.
                    read_index = read.read_index;
                }
                cb.invoke_read(self.handle_read(ctx, req, true, read_index));
                continue;
            }
            if req.get_header().get_replica_read() {
                // We should check epoch since the range could be changed.
                cb.invoke_read(self.handle_read(ctx, req, true, read.read_index));
            } else {
                // The request could be proposed when the peer was leader.
                // TODO: figure out that it's necessary to notify stale or not.
                let term = self.term();
                apply::notify_stale_req(term, cb);
            }
        }
    }

    /// Responses to the ready read index request on the replica, the replica is not a leader.
    fn post_pending_read_index_on_replica<T>(&mut self, ctx: &mut PollContext<EK, ER, T>) {
        while let Some(mut read) = self.pending_reads.pop_front() {
            // The response of this read index request is lost, but we need it for
            // the memory lock checking result. Resend the request.
            if let Some(read_index) = read.addition_request.take() {
                assert_eq!(read.cmds.len(), 1);
                let (mut req, cb, _) = read.cmds.pop().unwrap();
                assert_eq!(req.requests.len(), 1);
                req.requests[0].set_read_index(*read_index);
                let read_cmd = RaftCommand::new(req, cb);
                info!(
                    "re-propose read index request because the response is lost";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                RAFT_READ_INDEX_PENDING_COUNT.sub(1);
                self.send_read_command(ctx, read_cmd);
                continue;
            }

            assert!(read.read_index.is_some());
            let is_read_index_request = read.cmds.len() == 1
                && read.cmds[0].0.get_requests().len() == 1
                && read.cmds[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

            if is_read_index_request {
                self.response_read(&mut read, ctx, false);
            } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                self.response_read(&mut read, ctx, true);
            } else {
                // TODO: `ReadIndex` requests could be blocked.
                self.pending_reads.push_front(read);
                break;
            }
        }
    }

    fn send_read_command<T>(
        &self,
        ctx: &mut PollContext<EK, ER, T>,
        read_cmd: RaftCommand<EK::Snapshot>,
    ) {
        let mut err = errorpb::Error::default();
        let read_cb = match ctx.router.send_raft_command(read_cmd) {
            Ok(()) => return,
            Err(TrySendError::Full(cmd)) => {
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd.callback
            }
            Err(TrySendError::Disconnected(cmd)) => {
                err.set_message(format!("region {} is missing", self.region_id));
                err.mut_region_not_found().set_region_id(self.region_id);
                cmd.callback
            }
        };
        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        let read_resp = ReadResponse {
            response: resp,
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };
        read_cb.invoke_read(read_resp);
    }

    fn apply_reads<T>(&mut self, ctx: &mut PollContext<EK, ER, T>, ready: &Ready) {
        let mut propose_time = None;
        let states = ready.read_states().iter().map(|state| {
            let read_index_ctx = ReadIndexContext::parse(state.request_ctx.as_slice()).unwrap();
            (read_index_ctx.id, read_index_ctx.locked, state.index)
        });
        // The follower may lost `ReadIndexResp`, so the pending_reads does not
        // guarantee the orders are consistent with read_states. `advance` will
        // update the `read_index` of read request that before this successful
        // `ready`.
        if !self.is_leader() {
            // NOTE: there could still be some pending reads proposed by the peer when it was
            // leader. They will be cleared in `clear_uncommitted_on_role_change` later in
            // the function.
            self.pending_reads.advance_replica_reads(states);
            self.post_pending_read_index_on_replica(ctx);
        } else {
            self.pending_reads.advance_leader_reads(states);
            propose_time = self.pending_reads.last_ready().map(|r| r.renew_lease_time);
            if self.ready_to_handle_read() {
                while let Some(mut read) = self.pending_reads.pop_front() {
                    self.response_read(&mut read, ctx, false);
                }
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss().is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads.clear_uncommitted_on_role_change(term);
        }

        if let Some(propose_time) = propose_time {
            // `propose_time` is a placeholder, here cares about `Suspect` only,
            // and if it is in `Suspect` phase, the actual timestamp is useless.
            if self.leader_lease.inspect(Some(propose_time)) == LeaseState::Suspect {
                return;
            }
            self.maybe_renew_leader_lease(propose_time, ctx, None);
        }
    }

    pub fn post_apply<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        apply_state: RaftApplyState,
        applied_index_term: u64,
        apply_metrics: &ApplyMetrics,
    ) -> bool {
        let mut has_ready = false;

        if self.is_applying_snapshot() {
            panic!("{} should not applying snapshot.", self.tag);
        }

        self.raft_group
            .advance_apply_to(apply_state.get_applied_index());

        self.cmd_epoch_checker.advance_apply(
            apply_state.get_applied_index(),
            self.term(),
            self.raft_group.store().region(),
        );

        let progress_to_be_updated = self.mut_store().applied_index_term() != applied_index_term;
        self.mut_store().set_applied_state(apply_state);
        self.mut_store().set_applied_term(applied_index_term);

        self.peer_stat.written_keys += apply_metrics.written_keys;
        self.peer_stat.written_bytes += apply_metrics.written_bytes;
        self.delete_keys_hint += apply_metrics.delete_keys_hint;
        let diff = self.size_diff_hint as i64 + apply_metrics.size_diff_hint;
        self.size_diff_hint = cmp::max(diff, 0) as u64;

        if self.has_pending_snapshot() && self.ready_to_handle_pending_snap() {
            has_ready = true;
        }
        if !self.is_leader() {
            self.post_pending_read_index_on_replica(ctx)
        } else if self.ready_to_handle_read() {
            while let Some(mut read) = self.pending_reads.pop_front() {
                self.response_read(&mut read, ctx, false);
            }
        }
        self.pending_reads.gc();

        // Only leaders need to update applied_index_term.
        if progress_to_be_updated && self.is_leader() {
            let progress = ReadProgress::applied_index_term(applied_index_term);
            let mut meta = ctx.store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
        has_ready
    }

    pub fn post_split(&mut self) {
        // Reset delete_keys_hint and size_diff_hint.
        self.delete_keys_hint = 0;
        self.size_diff_hint = 0;
    }

    /// Try to renew leader lease.
    fn maybe_renew_leader_lease<T>(
        &mut self,
        ts: Timespec,
        ctx: &mut PollContext<EK, ER, T>,
        progress: Option<ReadProgress>,
    ) {
        // A nonleader peer should never has leader lease.
        let read_progress = if !self.is_leader() {
            None
        } else if self.is_splitting() {
            // A splitting leader should not renew its lease.
            // Because we split regions asynchronous, the leader may read stale results
            // if splitting runs slow on the leader.
            debug!(
                "prevents renew lease while splitting";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            None
        } else if self.is_merging() {
            // A merging leader should not renew its lease.
            // Because we merge regions asynchronous, the leader may read stale results
            // if commit merge runs slow on sibling peers.
            debug!(
                "prevents renew lease while merging";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            None
        } else {
            self.leader_lease.renew(ts);
            let term = self.term();
            if let Some(remote_lease) = self.leader_lease.maybe_new_remote_lease(term) {
                Some(ReadProgress::leader_lease(remote_lease))
            } else {
                None
            }
        };
        if let Some(progress) = progress {
            let mut meta = ctx.store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
        if let Some(progress) = read_progress {
            let mut meta = ctx.store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
    }

    fn maybe_update_read_progress(&self, reader: &mut ReadDelegate, progress: ReadProgress) {
        if self.pending_remove {
            return;
        }
        debug!(
            "update read progress";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "progress" => ?progress,
        );
        reader.update(progress);
    }

    pub fn maybe_campaign(&mut self, parent_is_leader: bool) -> bool {
        if self.region().get_peers().len() <= 1 {
            // The peer campaigned when it was created, no need to do it again.
            return false;
        }

        if !parent_is_leader {
            return false;
        }

        // If last peer is the leader of the region before split, it's intuitional for
        // it to become the leader of new split region.
        let _ = self.raft_group.campaign();
        true
    }

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
    pub fn propose<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        mut cb: Callback<EK::Snapshot>,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
    ) -> bool {
        if self.pending_remove {
            return false;
        }

        ctx.raft_metrics.propose.all += 1;

        let req_admin_cmd_type = if !req.has_admin_request() {
            None
        } else {
            Some(req.get_admin_request().get_cmd_type())
        };
        let is_urgent = is_request_urgent(&req);

        let policy = self.inspect(&req);
        let res = match policy {
            Ok(RequestPolicy::ReadLocal) => {
                self.read_local(ctx, req, cb);
                return false;
            }
            Ok(RequestPolicy::ReadIndex) => return self.read_index(ctx, req, err_resp, cb),
            Ok(RequestPolicy::ProposeNormal) => self.propose_normal(ctx, req),
            Ok(RequestPolicy::ProposeTransferLeader) => {
                return self.propose_transfer_leader(ctx, req, cb);
            }
            Ok(RequestPolicy::ProposeConfChange) => self.propose_conf_change(ctx, &req),
            Err(e) => Err(e),
        };

        match res {
            Err(e) => {
                cmd_resp::bind_error(&mut err_resp, e);
                cb.invoke_with_response(err_resp);
                false
            }
            Ok(Either::Right(idx)) => {
                if !cb.is_none() {
                    self.cmd_epoch_checker.attach_to_conflict_cmd(idx, cb);
                }
                false
            }
            Ok(Either::Left(idx)) => {
                let has_applied_to_current_term = self.has_applied_to_current_term();
                if has_applied_to_current_term {
                    // After this peer has applied to current term and passed above checking including `cmd_epoch_checker`,
                    // we can safely guarantee that this proposal will be committed if there is no abnormal leader transfer
                    // in the near future. Thus proposed callback can be called.
                    cb.invoke_proposed();
                }
                if is_urgent {
                    self.last_urgent_proposal_idx = idx;
                    // Eager flush to make urgent proposal be applied on all nodes as soon as
                    // possible.
                    self.raft_group.skip_bcast_commit(false);
                }
                self.should_wake_up = true;
                let p = Proposal {
                    is_conf_change: req_admin_cmd_type == Some(AdminCmdType::ChangePeer)
                        || req_admin_cmd_type == Some(AdminCmdType::ChangePeerV2),
                    index: idx,
                    term: self.term(),
                    cb,
                    renew_lease_time: None,
                    must_pass_epoch_check: has_applied_to_current_term,
                };
                if let Some(cmd_type) = req_admin_cmd_type {
                    self.cmd_epoch_checker
                        .post_propose(cmd_type, idx, self.term());
                }
                self.post_propose(ctx, p);
                true
            }
        }
    }

    fn post_propose<T>(
        &mut self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        mut p: Proposal<EK::Snapshot>,
    ) {
        // Try to renew leader lease on every consistent read/write request.
        if poll_ctx.current_time.is_none() {
            poll_ctx.current_time = Some(monotonic_raw_now());
        }
        p.renew_lease_time = poll_ctx.current_time;

        self.proposals.push(p);
    }

    // TODO: set higher election priority of voter/incoming voter than demoting voter
    /// Validate the `ConfChange` requests and check whether it's safe to
    /// propose these conf change requests.
    /// It's safe iff at least the quorum of the Raft group is still healthy
    /// right after all conf change is applied.
    /// If 'allow_remove_leader' is false then the peer to be removed should
    /// not be the leader.
    fn check_conf_change<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        change_peers: &[ChangePeerRequest],
        cc: &impl ConfChangeI,
    ) -> Result<()> {
        // Check whether current joint state can handle this request
        let mut after_progress = self.check_joint_state(cc)?;
        let current_progress = self.raft_group.status().progress.unwrap().clone();
        let kind = ConfChangeKind::confchange_kind(change_peers.len());

        if kind == ConfChangeKind::LeaveJoint {
            if self.peer.get_role() == PeerRole::DemotingVoter {
                return Err(box_err!(
                    "{} ignore leave joint command that demoting leader",
                    self.tag
                ));
            }
            // Leaving joint state, skip check
            return Ok(());
        }

        // Check whether this request is valid
        let mut check_dup = HashSet::default();
        let mut only_learner_change = true;
        let current_voter = current_progress.conf().voters().ids();
        for cp in change_peers.iter() {
            let (change_type, peer) = (cp.get_change_type(), cp.get_peer());
            match (change_type, peer.get_role()) {
                (ConfChangeType::RemoveNode, PeerRole::Voter) if kind != ConfChangeKind::Simple => {
                    return Err(box_err!(
                        "{} invalid conf change request: {:?}, can not remove voter directly",
                        self.tag,
                        cp
                    ));
                }
                (ConfChangeType::RemoveNode, _)
                | (ConfChangeType::AddNode, PeerRole::Voter)
                | (ConfChangeType::AddLearnerNode, PeerRole::Learner) => {}
                _ => {
                    return Err(box_err!(
                        "{} invalid conf change request: {:?}",
                        self.tag,
                        cp
                    ));
                }
            }

            if !check_dup.insert(peer.get_id()) {
                return Err(box_err!(
                    "{} invalid conf change request, have multiple commands for the same peer {}",
                    self.tag,
                    peer.get_id()
                ));
            }

            if peer.get_id() == self.peer_id()
                && (change_type == ConfChangeType::RemoveNode
                    // In Joint confchange, the leader is allowed to be DemotingVoter
                    || (kind == ConfChangeKind::Simple
                        && change_type == ConfChangeType::AddLearnerNode))
                && !ctx.cfg.allow_remove_leader
            {
                return Err(box_err!(
                    "{} ignore remove leader or demote leader",
                    self.tag
                ));
            }

            if current_voter.contains(peer.get_id()) || change_type == ConfChangeType::AddNode {
                only_learner_change = false;
            }
        }

        // Multiple changes that only effect learner will not product `IncommingVoter` or `DemotingVoter`
        // after apply, but raftstore layer and PD rely on these roles to detect joint state
        if kind != ConfChangeKind::Simple && only_learner_change {
            return Err(box_err!(
                "{} invalid conf change request, multiple changes that only effect learner",
                self.tag
            ));
        }

        let promoted_commit_index = after_progress.maximal_committed_index().0;
        if current_progress.is_singleton() // It's always safe if there is only one node in the cluster.
            || promoted_commit_index >= self.get_store().truncated_index()
        {
            return Ok(());
        }

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["conf_change", "reject_unsafe"])
            .inc();

        // Waking it up to replicate logs to candidate.
        self.should_wake_up = true;
        Err(box_err!(
            "{} unsafe to perform conf change {:?}, before: {:?}, after: {:?}, truncated index {}, promoted commit index {}",
            self.tag,
            change_peers,
            current_progress.conf().to_conf_state(),
            after_progress.conf().to_conf_state(),
            self.get_store().truncated_index(),
            promoted_commit_index
        ))
    }

    /// Check if current joint state can handle this confchange
    fn check_joint_state(&mut self, cc: &impl ConfChangeI) -> Result<ProgressTracker> {
        let cc = &cc.as_v2();
        let mut prs = self.raft_group.status().progress.unwrap().clone();
        let mut changer = Changer::new(&prs);
        let (cfg, changes) = if cc.leave_joint() {
            changer.leave_joint()?
        } else if let Some(auto_leave) = cc.enter_joint() {
            changer.enter_joint(auto_leave, &cc.changes)?
        } else {
            changer.simple(&cc.changes)?
        };
        prs.apply_conf(cfg, changes, self.raft_group.raft.raft_log.last_index());
        Ok(prs)
    }

    fn transfer_leader(&mut self, peer: &metapb::Peer) {
        info!(
            "transfer leader";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "peer" => ?peer,
        );

        self.raft_group.transfer_leader(peer.get_id());
    }

    fn pre_transfer_leader(&mut self, peer: &metapb::Peer) -> bool {
        // Checks if safe to transfer leader.
        if self.raft_group.raft.has_pending_conf() {
            info!(
                "reject transfer leader due to pending conf change";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "peer" => ?peer,
            );
            return false;
        }

        // Broadcast heartbeat to make sure followers commit the entries immediately.
        // It's only necessary to ping the target peer, but ping all for simplicity.
        self.raft_group.ping();
        let mut msg = eraftpb::Message::new();
        msg.set_to(peer.get_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_from(self.peer_id());
        // log term here represents the term of last log. For leader, the term of last
        // log is always its current term. Not just set term because raft library forbids
        // setting it for MsgTransferLeader messages.
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);
        true
    }

    fn ready_to_transfer_leader<T>(
        &self,
        ctx: &mut PollContext<EK, ER, T>,
        mut index: u64,
        peer: &metapb::Peer,
    ) -> Option<&'static str> {
        let peer_id = peer.get_id();
        let status = self.raft_group.status();
        let progress = status.progress.unwrap();

        if !progress.conf().voters().contains(peer_id) {
            return Some("non voter");
        }

        for (id, pr) in progress.iter() {
            if pr.state == ProgressState::Snapshot {
                return Some("pending snapshot");
            }
            if *id == peer_id && index == 0 {
                // index will be zero if it's sent from an instance without
                // pre-transfer-leader feature. Set it to matched to make it
                // possible to transfer leader to an older version. It may be
                // useful during rolling restart.
                index = pr.matched;
            }
        }

        if self.raft_group.raft.has_pending_conf()
            || self.raft_group.raft.pending_conf_index > index
        {
            return Some("pending conf change");
        }

        let last_index = self.get_store().last_index();
        if last_index >= index + ctx.cfg.leader_transfer_max_log_lag {
            return Some("log gap");
        }
        None
    }

    fn read_local<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) {
        ctx.raft_metrics.propose.local_read += 1;
        cb.invoke_read(self.handle_read(ctx, req, false, Some(self.get_store().commit_index())))
    }

    fn pre_read_index(&self) -> Result<()> {
        fail_point!(
            "before_propose_readindex",
            |s| if s.map_or(true, |s| s.parse().unwrap_or(true)) {
                Ok(())
            } else {
                Err(box_err!(
                    "{} can not read due to injected failure",
                    self.tag
                ))
            }
        );

        // See more in ready_to_handle_read().
        if self.is_splitting() {
            return Err(Error::ReadIndexNotReady(
                "can not read index due to split",
                self.region_id,
            ));
        }
        if self.is_merging() {
            return Err(Error::ReadIndexNotReady(
                "can not read index due to merge",
                self.region_id,
            ));
        }
        Ok(())
    }

    pub fn has_unresolved_reads(&self) -> bool {
        self.pending_reads.has_unresolved()
    }

    /// `ReadIndex` requests could be lost in network, so on followers commands could queue in
    /// `pending_reads` forever. Sending a new `ReadIndex` periodically can resolve this.
    pub fn retry_pending_reads(&mut self, cfg: &Config) {
        if self.is_leader()
            || !self.pending_reads.check_needs_retry(cfg)
            || self.pre_read_index().is_err()
        {
            return;
        }

        let read = self.pending_reads.back_mut().unwrap();
        debug_assert!(read.read_index.is_none());
        self.raft_group
            .read_index(ReadIndexContext::fields_to_bytes(
                read.id,
                read.addition_request.as_deref(),
                None,
            ));
        debug!(
            "request to get a read index";
            "request_id" => ?read.id,
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );
    }

    // Returns a boolean to indicate whether the `read` is proposed or not.
    // For these cases it won't be proposed:
    // 1. The region is in merging or splitting;
    // 2. The message is stale and dropped by the Raft group internally;
    // 3. There is already a read request proposed in the current lease;
    fn read_index<T: Transport>(
        &mut self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        cb: Callback<EK::Snapshot>,
    ) -> bool {
        if let Err(e) = self.pre_read_index() {
            debug!(
                "prevents unsafe read index";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
            poll_ctx.raft_metrics.propose.unsafe_read_index += 1;
            cmd_resp::bind_error(&mut err_resp, e);
            cb.invoke_with_response(err_resp);
            self.should_wake_up = true;
            return false;
        }

        let renew_lease_time = monotonic_raw_now();
        if self.is_leader() {
            match self.inspect_lease() {
                // Here combine the new read request with the previous one even if the lease expired is
                // ok because in this case, the previous read index must be sent out with a valid
                // lease instead of a suspect lease. So there must no pending transfer-leader proposals
                // before or after the previous read index, and the lease can be renewed when get
                // heartbeat responses.
                LeaseState::Valid | LeaseState::Expired => {
                    // Must use the commit index of `PeerStorage` instead of the commit index
                    // in raft-rs which may be greater than the former one.
                    // For more details, see the annotations above `on_leader_commit_idx_changed`.
                    let commit_index = self.get_store().commit_index();
                    if let Some(read) = self.pending_reads.back_mut() {
                        let max_lease = poll_ctx.cfg.raft_store_max_leader_lease();
                        if read.renew_lease_time + max_lease > renew_lease_time {
                            read.push_command(req, cb, commit_index);
                            return false;
                        }
                    }
                }
                // If the current lease is suspect, new read requests can't be appended into
                // `pending_reads` because if the leader is transferred, the latest read could
                // be dirty.
                _ => {}
            }
        }

        // When a replica cannot detect any leader, `MsgReadIndex` will be dropped, which would
        // cause a long time waiting for a read response. Then we should return an error directly
        // in this situation.
        if !self.is_leader() && self.leader_id() == INVALID_ID {
            poll_ctx.raft_metrics.invalid_proposal.read_index_no_leader += 1;
            // The leader may be hibernated, send a message for trying to awaken the leader.
            if self.bcast_wake_up_time.is_none()
                || self.bcast_wake_up_time.as_ref().unwrap().elapsed()
                    >= Duration::from_millis(MIN_BCAST_WAKE_UP_INTERVAL)
            {
                self.bcast_wake_up_message(poll_ctx);
                self.bcast_wake_up_time = Some(UtilInstant::now_coarse());

                let task = PdTask::QueryRegionLeader {
                    region_id: self.region_id,
                };
                if let Err(e) = poll_ctx.pd_scheduler.schedule(task) {
                    error!(
                        "failed to notify pd";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer_id(),
                        "err" => %e,
                    )
                }
            }
            self.should_wake_up = true;
            cmd_resp::bind_error(&mut err_resp, Error::NotLeader(self.region_id, None));
            cb.invoke_with_response(err_resp);
            return false;
        }

        // Should we call pre_propose here?
        let last_pending_read_count = self.raft_group.raft.pending_read_count();
        let last_ready_read_count = self.raft_group.raft.ready_read_count();

        poll_ctx.raft_metrics.propose.read_index += 1;

        self.bcast_wake_up_time = None;

        let id = Uuid::new_v4();
        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        self.raft_group
            .read_index(ReadIndexContext::fields_to_bytes(
                id,
                request.as_ref(),
                None,
            ));

        let pending_read_count = self.raft_group.raft.pending_read_count();
        let ready_read_count = self.raft_group.raft.ready_read_count();

        if pending_read_count == last_pending_read_count
            && ready_read_count == last_ready_read_count
            && self.is_leader()
        {
            // The message gets dropped silently, can't be handled anymore.
            apply::notify_stale_req(self.term(), cb);
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, renew_lease_time);
        read.addition_request = request.map(Box::new);
        self.pending_reads.push_back(read, self.is_leader());
        self.should_wake_up = true;

        debug!(
            "request to get a read index";
            "request_id" => ?id,
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "is_leader" => self.is_leader(),
        );

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.inspect(Some(renew_lease_time)) == LeaseState::Suspect {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    renew_lease_time: Some(renew_lease_time),
                    must_pass_epoch_check: false,
                };
                self.post_propose(poll_ctx, p);
            }
        }

        true
    }

    /// Returns (minimal matched, minimal committed_index)
    ///
    /// For now, it is only used in merge.
    pub fn get_min_progress(&self) -> Result<(u64, u64)> {
        let (mut min_m, mut min_c) = (None, None);
        if let Some(progress) = self.raft_group.status().progress {
            for (id, pr) in progress.iter() {
                // Reject merge if there is any pending request snapshot,
                // because a target region may merge a source region which is in
                // an invalid state.
                if pr.state == ProgressState::Snapshot
                    || pr.pending_request_snapshot != INVALID_INDEX
                {
                    return Err(box_err!(
                        "there is a pending snapshot peer {} [{:?}], skip merge",
                        id,
                        pr
                    ));
                }
                if min_m.unwrap_or(u64::MAX) > pr.matched {
                    min_m = Some(pr.matched);
                }
                if min_c.unwrap_or(u64::MAX) > pr.committed_index {
                    min_c = Some(pr.committed_index);
                }
            }
        }
        Ok((min_m.unwrap_or(0), min_c.unwrap_or(0)))
    }

    fn pre_propose_prepare_merge<T>(
        &self,
        ctx: &mut PollContext<EK, ER, T>,
        req: &mut RaftCmdRequest,
    ) -> Result<()> {
        let last_index = self.raft_group.raft.raft_log.last_index();
        let (min_matched, min_committed) = self.get_min_progress()?;
        if min_matched == 0
            || min_committed == 0
            || last_index - min_matched > ctx.cfg.merge_max_log_gap
            || last_index - min_committed > ctx.cfg.merge_max_log_gap * 2
        {
            return Err(box_err!(
                "log gap from matched: {} or committed: {} to last index: {} is too large, skip merge",
                min_matched,
                min_committed,
                last_index
            ));
        }
        assert!(min_matched >= min_committed);
        let mut entry_size = 0;
        for entry in self
            .raft_group
            .raft
            .raft_log
            .entries(min_committed + 1, NO_LIMIT)?
        {
            // commit merge only contains entries start from min_matched + 1
            if entry.index > min_matched {
                entry_size += entry.get_data().len();
            }
            if entry.get_entry_type() == EntryType::EntryConfChange
                || entry.get_entry_type() == EntryType::EntryConfChangeV2
            {
                return Err(box_err!(
                    "{} log gap contains conf change, skip merging.",
                    self.tag
                ));
            }
            if entry.get_data().is_empty() {
                continue;
            }
            let cmd: RaftCmdRequest =
                util::parse_data_at(entry.get_data(), entry.get_index(), &self.tag);
            if !cmd.has_admin_request() {
                continue;
            }
            let cmd_type = cmd.get_admin_request().get_cmd_type();
            match cmd_type {
                AdminCmdType::TransferLeader
                | AdminCmdType::ComputeHash
                | AdminCmdType::VerifyHash
                | AdminCmdType::InvalidAdmin => continue,
                _ => {}
            }
            // Any command that can change epoch or log gap should be rejected.
            return Err(box_err!(
                "log gap contains admin request {:?}, skip merging.",
                cmd_type
            ));
        }
        if entry_size as f64 > ctx.cfg.raft_entry_max_size.0 as f64 * 0.9 {
            return Err(box_err!(
                "log gap size exceed entry size limit, skip merging."
            ));
        }
        req.mut_admin_request()
            .mut_prepare_merge()
            .set_min_index(min_matched + 1);
        Ok(())
    }

    fn pre_propose<T>(
        &self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        req: &mut RaftCmdRequest,
    ) -> Result<ProposalContext> {
        poll_ctx.coprocessor_host.pre_propose(self.region(), req)?;
        let mut ctx = ProposalContext::empty();

        if get_sync_log_from_request(req) {
            ctx.insert(ProposalContext::SYNC_LOG);
        }

        if !req.has_admin_request() {
            return Ok(ctx);
        }

        match req.get_admin_request().get_cmd_type() {
            AdminCmdType::Split | AdminCmdType::BatchSplit => ctx.insert(ProposalContext::SPLIT),
            AdminCmdType::PrepareMerge => {
                self.pre_propose_prepare_merge(poll_ctx, req)?;
                ctx.insert(ProposalContext::PREPARE_MERGE);
            }
            _ => {}
        }

        Ok(ctx)
    }

    /// Propose normal request to raft
    ///
    /// Returns Ok(Either::Left(index)) means the proposal is proposed successfully and is located on `index` position.
    /// Ok(Either::Right(index)) means the proposal is rejected by `CmdEpochChecker` and the `index` is the position of
    /// the last conflict admin cmd.
    fn propose_normal<T>(
        &mut self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        if self.pending_merge_state.is_some()
            && req.get_admin_request().get_cmd_type() != AdminCmdType::RollbackMerge
        {
            return Err(Error::ProposalInMergingMode(self.region_id));
        }

        poll_ctx.raft_metrics.propose.normal += 1;

        if self.has_applied_to_current_term() {
            // Only when applied index's term is equal to current leader's term, the information
            // in epoch checker is up to date and can be used to check epoch.
            if let Some(index) = self
                .cmd_epoch_checker
                .propose_check_epoch(&req, self.term())
            {
                return Ok(Either::Right(index));
            }
        } else if req.has_admin_request() {
            // The admin request is rejected because it may need to update epoch checker which
            // introduces an uncertainty and may breaks the correctness of epoch checker.
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag,
                self.get_store().applied_index_term(),
                self.term()
            ));
        }

        // TODO: validate request for unexpected changes.
        let ctx = match self.pre_propose(poll_ctx, &mut req) {
            Ok(ctx) => ctx,
            Err(e) => {
                warn!(
                    "skip proposal";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "err" => ?e,
                    "error_code" => %e.error_code(),
                );
                return Err(e);
            }
        };

        let data = req.write_to_bytes()?;

        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        if data.len() as u64 > poll_ctx.cfg.raft_entry_max_size.0 {
            error!(
                "entry is too large";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "size" => data.len(),
            );
            return Err(Error::RaftEntryTooLarge(self.region_id, data.len() as u64));
        }

        let propose_index = self.next_proposal_index();
        self.raft_group.propose(ctx.to_vec(), data)?;
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }

        if ctx.contains(ProposalContext::PREPARE_MERGE) {
            self.last_proposed_prepare_merge_idx = propose_index;
        }

        Ok(Either::Left(propose_index))
    }

    fn execute_transfer_leader<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        msg: &eraftpb::Message,
    ) {
        // log_term is set by original leader, represents the term last log is written
        // in, which should be equal to the original leader's term.
        if msg.get_log_term() != self.term() {
            return;
        }

        if self.is_leader() {
            let from = match self.get_peer_from_cache(msg.get_from()) {
                Some(p) => p,
                None => return,
            };
            match self.ready_to_transfer_leader(ctx, msg.get_index(), &from) {
                Some(reason) => {
                    info!(
                        "reject to transfer leader";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "to" => ?from,
                        "reason" => reason,
                        "index" => msg.get_index(),
                        "last_index" => self.get_store().last_index(),
                    );
                }
                None => {
                    self.transfer_leader(&from);
                    self.should_wake_up = true;
                }
            }
            return;
        }

        #[allow(clippy::suspicious_operation_groupings)]
        if self.is_applying_snapshot()
            || self.has_pending_snapshot()
            || msg.get_from() != self.leader_id()
        {
            info!(
                "reject transferring leader";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "from" => msg.get_from(),
            );
            return;
        }

        let mut msg = eraftpb::Message::new();
        msg.set_from(self.peer_id());
        msg.set_to(self.leader_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_index(self.get_store().applied_index());
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);
    }

    /// Return true to if the transfer leader request is accepted.
    ///
    /// When transferring leadership begins, leader sends a pre-transfer
    /// to target follower first to ensures it's ready to become leader.
    /// After that the real transfer leader process begin.
    ///
    /// 1. pre_transfer_leader on leader:
    ///     Leader will send a MsgTransferLeader to follower.
    /// 2. execute_transfer_leader on follower
    ///     If follower passes all necessary checks, it will reply an
    ///     ACK with type MsgTransferLeader and its promised persistent index.
    /// 3. execute_transfer_leader on leader:
    ///     Leader checks if it's appropriate to transfer leadership. If it
    ///     does, it calls raft transfer_leader API to do the remaining work.
    ///
    /// See also: tikv/rfcs#37.
    fn propose_transfer_leader<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) -> bool {
        ctx.raft_metrics.propose.transfer_leader += 1;

        let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
        let peer = transfer_leader.get_peer();

        let transferred = self.pre_transfer_leader(peer);

        // transfer leader command doesn't need to replicate log and apply, so we
        // return immediately. Note that this command may fail, we can view it just as an advice
        cb.invoke_with_response(make_transfer_leader_response());

        transferred
    }

    // Fails in such cases:
    // 1. A pending conf change has not been applied yet;
    // 2. Removing the leader is not allowed in the configuration;
    // 3. The conf change makes the raft group not healthy;
    // 4. The conf change is dropped by raft group internally.
    /// Returns Ok(Either::Left(index)) means the proposal is proposed successfully and is located on `index` position.
    /// Ok(Either::Right(index)) means the proposal is rejected by `CmdEpochChecker` and the `index` is the position of
    /// the last conflict admin cmd.
    fn propose_conf_change<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        req: &RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        if self.pending_merge_state.is_some() {
            return Err(Error::ProposalInMergingMode(self.region_id));
        }
        if self.raft_group.raft.pending_conf_index > self.get_store().applied_index() {
            info!(
                "there is a pending conf change, try later";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return Err(box_err!(
                "{} there is a pending conf change, try later",
                self.tag
            ));
        }
        // Actually, according to the implementation of conf change in raft-rs, this check must be
        // passed if the previous check that `pending_conf_index` should be less than or equal to
        // `self.get_store().applied_index()` is passed.
        if self.get_store().applied_index_term() != self.term() {
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag,
                self.get_store().applied_index_term(),
                self.term()
            ));
        }
        if let Some(index) = self
            .cmd_epoch_checker
            .propose_check_epoch(&req, self.term())
        {
            return Ok(Either::Right(index));
        }

        let data = req.write_to_bytes()?;
        let admin = req.get_admin_request();
        let res = if admin.has_change_peer() {
            self.propose_conf_change_internal(ctx, admin.get_change_peer(), data)
        } else if admin.has_change_peer_v2() {
            self.propose_conf_change_internal(ctx, admin.get_change_peer_v2(), data)
        } else {
            unreachable!()
        };
        if let Err(ref e) = res {
            warn!("failed to propose confchange"; "error" => ?e);
        }
        res.map(Either::Left)
    }

    // Fails in such cases:
    // 1. A pending conf change has not been applied yet;
    // 2. Removing the leader is not allowed in the configuration;
    // 3. The conf change makes the raft group not healthy;
    // 4. The conf change is dropped by raft group internally.
    fn propose_conf_change_internal<T, CP: ChangePeerI>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        change_peer: CP,
        data: Vec<u8>,
    ) -> Result<u64> {
        let data_size = data.len();
        let cc = change_peer.to_confchange(data);
        let changes = change_peer.get_change_peers();

        self.check_conf_change(ctx, changes.as_ref(), &cc)?;

        ctx.raft_metrics.propose.conf_change += 1;
        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data_size as f64);
        info!(
            "propose conf change peer";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "changes" => ?changes.as_ref(),
            "kind" => ?ConfChangeKind::confchange_kind(changes.as_ref().len()),
        );

        let propose_index = self.next_proposal_index();
        self.raft_group
            .propose_conf_change(ProposalContext::SYNC_LOG.to_vec(), cc)?;
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }

        Ok(propose_index)
    }

    fn handle_read<T>(
        &self,
        ctx: &mut PollContext<EK, ER, T>,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> ReadResponse<EK::Snapshot> {
        let region = self.region().clone();
        if check_epoch {
            if let Err(e) = check_region_epoch(&req, &region, true) {
                debug!("epoch not match"; "region_id" => region.get_id(), "err" => ?e);
                let mut response = cmd_resp::new_error(e);
                cmd_resp::bind_term(&mut response, self.term());
                return ReadResponse {
                    response,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                };
            }
        }
        let mut resp = ctx.execute(&req, &Arc::new(region), read_index, None);
        if let Some(snap) = resp.snapshot.as_mut() {
            snap.max_ts_sync_status = Some(self.max_ts_sync_status.clone());
        }
        resp.txn_extra_op = self.txn_extra_op.load();
        cmd_resp::bind_term(&mut resp.response, self.term());
        resp
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub fn stop(&mut self) {
        self.mut_store().cancel_applying_snap();
        self.pending_reads.clear_all(None);
    }

    pub fn maybe_add_want_rollback_merge_peer(&mut self, peer_id: u64, extra_msg: &ExtraMessage) {
        if !self.is_leader() {
            return;
        }
        if let Some(ref state) = self.pending_merge_state {
            if state.get_commit() == extra_msg.get_premerge_commit() {
                self.add_want_rollback_merge_peer(peer_id);
            }
        }
    }

    pub fn add_want_rollback_merge_peer(&mut self, peer_id: u64) {
        assert!(self.pending_merge_state.is_some());
        self.want_rollback_merge_peers.insert(peer_id);
    }
}

impl<EK, ER> Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        self.peer_cache.borrow_mut().insert(peer.get_id(), peer);
    }

    pub fn remove_peer_from_cache(&mut self, peer_id: u64) {
        self.peer_cache.borrow_mut().remove(&peer_id);
    }

    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if peer_id == 0 {
            return None;
        }
        fail_point!("stale_peer_cache_2", peer_id == 2, |_| None);
        if let Some(peer) = self.peer_cache.borrow().get(&peer_id) {
            return Some(peer.clone());
        }

        // Try to find in region, if found, set in cache.
        for peer in self.region().get_peers() {
            if peer.get_id() == peer_id {
                self.peer_cache.borrow_mut().insert(peer_id, peer.clone());
                return Some(peer.clone());
            }
        }

        None
    }

    fn region_replication_status(&mut self) -> Option<RegionReplicationStatus> {
        if self.replication_mode_version == 0 {
            return None;
        }
        let mut status = RegionReplicationStatus {
            state_id: self.replication_mode_version,
            ..Default::default()
        };
        let state = if !self.replication_sync {
            if self.dr_auto_sync_state != DrAutoSyncState::Async {
                let res = self.raft_group.raft.check_group_commit_consistent();
                if Some(true) != res {
                    let mut buffer: SmallVec<[(u64, u64, u64); 5]> = SmallVec::new();
                    if self.get_store().applied_index_term() >= self.term() {
                        let progress = self.raft_group.raft.prs();
                        for (id, p) in progress.iter() {
                            if !progress.conf().voters().contains(*id) {
                                continue;
                            }
                            buffer.push((*id, p.commit_group_id, p.matched));
                        }
                    };
                    info!(
                        "still not reach integrity over label";
                        "status" => ?res,
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.id,
                        "progress" => ?buffer
                    );
                } else {
                    self.replication_sync = true;
                }
                match res {
                    Some(true) => RegionReplicationState::IntegrityOverLabel,
                    Some(false) => RegionReplicationState::SimpleMajority,
                    None => RegionReplicationState::Unknown,
                }
            } else {
                RegionReplicationState::SimpleMajority
            }
        } else {
            RegionReplicationState::IntegrityOverLabel
        };
        status.set_state(state);
        Some(status)
    }

    pub fn is_region_size_or_keys_none(&self) -> bool {
        fail_point!("region_size_or_keys_none", |_| true);
        self.approximate_size.is_none() || self.approximate_keys.is_none()
    }

    pub fn heartbeat_pd<T>(&mut self, ctx: &PollContext<EK, ER, T>) {
        let task = PdTask::Heartbeat(HeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(ctx.cfg.max_peer_down_duration.0),
            pending_peers: self.collect_pending_peers(ctx),
            written_bytes: self.peer_stat.written_bytes,
            written_keys: self.peer_stat.written_keys,
            approximate_size: self.approximate_size.unwrap_or_default(),
            approximate_keys: self.approximate_keys.unwrap_or_default(),
            replication_status: self.region_replication_status(),
        });
        if !self.is_region_size_or_keys_none() {
            if let Err(e) = ctx.pd_scheduler.schedule(task) {
                error!(
                    "failed to notify pd";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "err" => ?e,
                );
            }
            return;
        }

        if self.pending_pd_heartbeat_tasks.load(Ordering::SeqCst) > 2 {
            return;
        }
        let region_id = self.region_id;
        let peer_id = self.peer.get_id();
        let scheduler = ctx.pd_scheduler.clone();
        let split_check_task = SplitCheckTask::GetRegionApproximateSizeAndKeys {
            region: self.region().clone(),
            pending_tasks: self.pending_pd_heartbeat_tasks.clone(),
            cb: Box::new(move |size: u64, keys: u64| {
                if let PdTask::Heartbeat(mut h) = task {
                    h.approximate_size = size;
                    h.approximate_keys = keys;
                    if let Err(e) = scheduler.schedule(PdTask::Heartbeat(h)) {
                        error!(
                            "failed to notify pd";
                            "region_id" => region_id,
                            "peer_id" => peer_id,
                            "err" => ?e,
                        );
                    }
                }
            }),
        };
        self.pending_pd_heartbeat_tasks
            .fetch_add(1, Ordering::SeqCst);
        if let Err(e) = ctx.split_check_scheduler.schedule(split_check_task) {
            error!(
                "failed to notify pd";
                "region_id" => region_id,
                "peer_id" => peer_id,
                "err" => ?e,
            );
            self.pending_pd_heartbeat_tasks
                .fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn prepare_raft_message(&self) -> RaftMessage {
        let mut send_msg = RaftMessage::default();
        send_msg.set_region_id(self.region_id);
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        send_msg.set_from_peer(self.peer.clone());
        send_msg
    }

    pub fn send_extra_message<T: Transport>(
        &self,
        msg: ExtraMessage,
        trans: &mut T,
        to: &metapb::Peer,
    ) {
        let mut send_msg = self.prepare_raft_message();
        let ty = msg.get_type();
        debug!("send extra msg";
        "region_id" => self.region_id,
        "peer_id" => self.peer.get_id(),
        "msg_type" => ?ty,
        "to" => to.get_id());
        send_msg.set_extra_msg(msg);
        send_msg.set_to_peer(to.clone());
        if let Err(e) = trans.send(send_msg) {
            error!(?e;
                "failed to send extra message";
                "type" => ?ty,
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "target" => ?to,
            );
        }
    }

    fn send_raft_message<T: Transport>(&mut self, msg: eraftpb::Message, trans: &mut T) {
        let mut send_msg = self.prepare_raft_message();

        let to_peer = match self.get_peer_from_cache(msg.get_to()) {
            Some(p) => p,
            None => {
                warn!(
                    "failed to look up recipient peer";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "to_peer" => msg.get_to(),
                );
                return;
            }
        };

        let to_peer_id = to_peer.get_id();
        let to_store_id = to_peer.get_store_id();
        let msg_type = msg.get_msg_type();
        debug!(
            "send raft msg";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "msg_type" => ?msg_type,
            "msg_size" => msg.compute_size(),
            "to" => to_peer_id,
        );

        send_msg.set_to_peer(to_peer);

        // There could be two cases:
        // 1. Target peer already exists but has not established communication with leader yet
        // 2. Target peer is added newly due to member change or region split, but it's not
        //    created yet
        // For both cases the region start key and end key are attached in RequestVote and
        // Heartbeat message for the store of that peer to check whether to create a new peer
        // when receiving these messages, or just to wait for a pending region split to perform
        // later.
        if self.get_store().is_initialized() && is_initial_msg(&msg) {
            let region = self.region();
            send_msg.set_start_key(region.get_start_key().to_vec());
            send_msg.set_end_key(region.get_end_key().to_vec());
        }
        send_msg.set_message(msg);

        if let Err(e) = trans.send(send_msg) {
            warn!(
                "failed to send msg to other peer";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "target_peer_id" => to_peer_id,
                "target_store_id" => to_store_id,
                "err" => ?e,
                "error_code" => %e.error_code(),
            );
            if to_peer_id == self.leader_id() {
                self.leader_unreachable = true;
            }
            // unreachable store
            self.raft_group.report_unreachable(to_peer_id);
            if msg_type == eraftpb::MessageType::MsgSnapshot {
                self.raft_group
                    .report_snapshot(to_peer_id, SnapshotStatus::Failure);
            }
        }
    }

    pub fn bcast_wake_up_message<T: Transport>(&self, ctx: &mut PollContext<EK, ER, T>) {
        for peer in self.region().get_peers() {
            if peer.get_id() == self.peer_id() {
                continue;
            }
            self.send_wake_up_message(ctx, peer);
        }
    }

    pub fn send_wake_up_message<T: Transport>(
        &self,
        ctx: &mut PollContext<EK, ER, T>,
        peer: &metapb::Peer,
    ) {
        let mut msg = ExtraMessage::default();
        msg.set_type(ExtraMessageType::MsgRegionWakeUp);
        self.send_extra_message(msg, &mut ctx.trans, peer);
    }

    pub fn bcast_check_stale_peer_message<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
    ) {
        if self.check_stale_conf_ver < self.region().get_region_epoch().get_conf_ver() {
            self.check_stale_conf_ver = self.region().get_region_epoch().get_conf_ver();
            self.check_stale_peers = self.region().get_peers().to_vec();
        }
        for peer in &self.check_stale_peers {
            if peer.get_id() == self.peer_id() {
                continue;
            }
            let mut extra_msg = ExtraMessage::default();
            extra_msg.set_type(ExtraMessageType::MsgCheckStalePeer);
            self.send_extra_message(extra_msg, &mut ctx.trans, peer);
        }
    }

    pub fn on_check_stale_peer_response(
        &mut self,
        check_conf_ver: u64,
        check_peers: Vec<metapb::Peer>,
    ) {
        if self.check_stale_conf_ver < check_conf_ver {
            self.check_stale_conf_ver = check_conf_ver;
            self.check_stale_peers = check_peers;
        }
    }

    pub fn send_want_rollback_merge<T: Transport>(
        &self,
        premerge_commit: u64,
        ctx: &mut PollContext<EK, ER, T>,
    ) {
        let to_peer = match self.get_peer_from_cache(self.leader_id()) {
            Some(p) => p,
            None => {
                warn!(
                    "failed to look up recipient peer";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "to_peer" => self.leader_id(),
                );
                return;
            }
        };
        let mut extra_msg = ExtraMessage::default();
        extra_msg.set_type(ExtraMessageType::MsgWantRollbackMerge);
        extra_msg.set_premerge_commit(premerge_commit);
        self.send_extra_message(extra_msg, &mut ctx.trans, &to_peer);
    }

    pub fn require_updating_max_ts(&self, pd_scheduler: &FutureScheduler<PdTask<EK>>) {
        let epoch = self.region().get_region_epoch();
        let term_low_bits = self.term() & ((1 << 32) - 1); // 32 bits
        let version_lot_bits = epoch.get_version() & ((1 << 31) - 1); // 31 bits
        let initial_status = (term_low_bits << 32) | (version_lot_bits << 1);
        self.max_ts_sync_status
            .store(initial_status, Ordering::SeqCst);
        info!(
            "require updating max ts";
            "region_id" => self.region_id,
            "initial_status" => initial_status,
        );
        if let Err(e) = pd_scheduler.schedule(PdTask::UpdateMaxTimestamp {
            region_id: self.region_id,
            initial_status,
            max_ts_sync_status: self.max_ts_sync_status.clone(),
        }) {
            error!(
                "failed to update max ts";
                "err" => ?e,
            );
        }
    }
}

/// `RequestPolicy` decides how we handle a request.
#[derive(Clone, PartialEq, Debug)]
pub enum RequestPolicy {
    // Handle the read request directly without dispatch.
    ReadLocal,
    // Handle the read request via raft's SafeReadIndex mechanism.
    ReadIndex,
    ProposeNormal,
    ProposeTransferLeader,
    ProposeConfChange,
}

/// `RequestInspector` makes `RequestPolicy` for requests.
pub trait RequestInspector {
    /// Has the current term been applied?
    fn has_applied_to_current_term(&mut self) -> bool;
    /// Inspects its lease.
    fn inspect_lease(&mut self) -> LeaseState;

    /// Inspect a request, return a policy that tells us how to
    /// handle the request.
    fn inspect(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        if req.has_admin_request() {
            if apply::is_conf_change_cmd(req) {
                return Ok(RequestPolicy::ProposeConfChange);
            }
            if get_transfer_leader_cmd(req).is_some() {
                return Ok(RequestPolicy::ProposeTransferLeader);
            }
            return Ok(RequestPolicy::ProposeNormal);
        }

        let mut has_read = false;
        let mut has_write = false;
        for r in req.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => has_read = true,
                CmdType::Delete | CmdType::Put | CmdType::DeleteRange | CmdType::IngestSst => {
                    has_write = true
                }
                CmdType::Prewrite | CmdType::Invalid => {
                    return Err(box_err!(
                        "invalid cmd type {:?}, message maybe corrupted",
                        r.get_cmd_type()
                    ));
                }
            }

            if has_read && has_write {
                return Err(box_err!("read and write can't be mixed in one batch"));
            }
        }

        if has_write {
            return Ok(RequestPolicy::ProposeNormal);
        }

        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // Local read should be performed, if and only if leader is in lease.
        // None for now.
        match self.inspect_lease() {
            LeaseState::Valid => Ok(RequestPolicy::ReadLocal),
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(RequestPolicy::ReadIndex)
            }
        }
    }
}

impl<EK, ER> RequestInspector for Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn has_applied_to_current_term(&mut self) -> bool {
        self.get_store().applied_index_term() == self.term()
    }

    fn inspect_lease(&mut self) -> LeaseState {
        if !self.raft_group.raft.in_lease() {
            return LeaseState::Suspect;
        }
        // None means now.
        let state = self.leader_lease.inspect(None);
        if LeaseState::Expired == state {
            debug!(
                "leader lease is expired";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "lease" => ?self.leader_lease,
            );
            // The lease is expired, call `expire` explicitly.
            self.leader_lease.expire();
        }
        state
    }
}

impl<EK, ER, T> ReadExecutor<EK> for PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn get_engine(&self) -> &EK {
        &self.engines.kv
    }

    fn get_snapshot(&mut self, _: Option<ThreadReadId>) -> Arc<EK::Snapshot> {
        Arc::new(self.engines.kv.snapshot())
    }
}

fn get_transfer_leader_cmd(msg: &RaftCmdRequest) -> Option<&TransferLeaderRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_transfer_leader() {
        return None;
    }

    Some(req.get_transfer_leader())
}

fn get_sync_log_from_request(msg: &RaftCmdRequest) -> bool {
    if msg.has_admin_request() {
        let req = msg.get_admin_request();
        return matches!(
            req.get_cmd_type(),
            AdminCmdType::ChangePeer
                | AdminCmdType::ChangePeerV2
                | AdminCmdType::Split
                | AdminCmdType::BatchSplit
                | AdminCmdType::PrepareMerge
                | AdminCmdType::CommitMerge
                | AdminCmdType::RollbackMerge
        );
    }

    msg.get_header().get_sync_log()
}

/// We enable follower lazy commit to get a better performance.
/// But it may not be appropriate for some requests. This function
/// checks whether the request should be committed on all followers
/// as soon as possible.
fn is_request_urgent(req: &RaftCmdRequest) -> bool {
    if !req.has_admin_request() {
        return false;
    }

    matches!(
        req.get_admin_request().get_cmd_type(),
        AdminCmdType::Split
            | AdminCmdType::BatchSplit
            | AdminCmdType::ChangePeer
            | AdminCmdType::ChangePeerV2
            | AdminCmdType::ComputeHash
            | AdminCmdType::VerifyHash
            | AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge
    )
}

fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::default();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::default());
    let mut resp = RaftCmdResponse::default();
    resp.set_admin_response(response);
    resp
}

/// A poor version of `Peer` to avoid port generic variables everywhere.
pub trait AbstractPeer {
    fn meta_peer(&self) -> &metapb::Peer;
    fn group_state(&self) -> GroupState;
    fn region(&self) -> &metapb::Region;
    fn apply_state(&self) -> &RaftApplyState;
    fn raft_status(&self) -> raft::Status;
    fn raft_commit_index(&self) -> u64;
    fn raft_request_snapshot(&mut self, index: u64);
    fn pending_merge_state(&self) -> Option<&MergeState>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::msg::ExtCallback;
    use kvproto::raft_cmdpb;
    #[cfg(feature = "protobuf-codec")]
    use protobuf::ProtobufEnum;

    #[test]
    fn test_sync_log() {
        let white_list = [
            AdminCmdType::InvalidAdmin,
            AdminCmdType::CompactLog,
            AdminCmdType::TransferLeader,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
        ];
        for tp in AdminCmdType::values() {
            let mut msg = RaftCmdRequest::default();
            msg.mut_admin_request().set_cmd_type(*tp);
            assert_eq!(
                get_sync_log_from_request(&msg),
                !white_list.contains(tp),
                "{:?}",
                tp
            );
        }
    }

    #[test]
    fn test_urgent() {
        let urgent_types = [
            AdminCmdType::Split,
            AdminCmdType::BatchSplit,
            AdminCmdType::ChangePeer,
            AdminCmdType::ChangePeerV2,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
            AdminCmdType::PrepareMerge,
            AdminCmdType::CommitMerge,
            AdminCmdType::RollbackMerge,
        ];
        for tp in AdminCmdType::values() {
            let mut req = RaftCmdRequest::default();
            req.mut_admin_request().set_cmd_type(*tp);
            assert_eq!(
                is_request_urgent(&req),
                urgent_types.contains(tp),
                "{:?}",
                tp
            );
        }
        assert!(!is_request_urgent(&RaftCmdRequest::default()));
    }

    #[test]
    fn test_entry_context() {
        let tbl: Vec<&[ProposalContext]> = vec![
            &[ProposalContext::SPLIT],
            &[ProposalContext::SYNC_LOG],
            &[ProposalContext::PREPARE_MERGE],
            &[ProposalContext::SPLIT, ProposalContext::SYNC_LOG],
            &[ProposalContext::PREPARE_MERGE, ProposalContext::SYNC_LOG],
        ];

        for flags in tbl {
            let mut ctx = ProposalContext::empty();
            for f in flags {
                ctx.insert(*f);
            }

            let ser = ctx.to_vec();
            let de = ProposalContext::from_bytes(&ser);

            for f in flags {
                assert!(de.contains(*f), "{:?}", de);
            }
        }
    }

    #[test]
    fn test_request_inspector() {
        struct DummyInspector {
            applied_to_index_term: bool,
            lease_state: LeaseState,
        }
        impl RequestInspector for DummyInspector {
            fn has_applied_to_current_term(&mut self) -> bool {
                self.applied_to_index_term
            }
            fn inspect_lease(&mut self) -> LeaseState {
                self.lease_state
            }
        }

        let mut table = vec![];

        // Ok(_)
        let mut req = RaftCmdRequest::default();
        let mut admin_req = raft_cmdpb::AdminRequest::default();

        req.set_admin_request(admin_req.clone());
        table.push((req.clone(), RequestPolicy::ProposeNormal));

        admin_req.set_change_peer(raft_cmdpb::ChangePeerRequest::default());
        req.set_admin_request(admin_req.clone());
        table.push((req.clone(), RequestPolicy::ProposeConfChange));
        admin_req.clear_change_peer();

        admin_req.set_change_peer_v2(raft_cmdpb::ChangePeerV2Request::default());
        req.set_admin_request(admin_req.clone());
        table.push((req.clone(), RequestPolicy::ProposeConfChange));
        admin_req.clear_change_peer_v2();

        admin_req.set_transfer_leader(raft_cmdpb::TransferLeaderRequest::default());
        req.set_admin_request(admin_req.clone());
        table.push((req.clone(), RequestPolicy::ProposeTransferLeader));
        admin_req.clear_transfer_leader();
        req.clear_admin_request();

        for (op, policy) in vec![
            (CmdType::Get, RequestPolicy::ReadLocal),
            (CmdType::Snap, RequestPolicy::ReadLocal),
            (CmdType::Put, RequestPolicy::ProposeNormal),
            (CmdType::Delete, RequestPolicy::ProposeNormal),
            (CmdType::DeleteRange, RequestPolicy::ProposeNormal),
            (CmdType::IngestSst, RequestPolicy::ProposeNormal),
        ] {
            let mut request = raft_cmdpb::Request::default();
            request.set_cmd_type(op);
            req.set_requests(vec![request].into());
            table.push((req.clone(), policy));
        }

        for &applied_to_index_term in &[true, false] {
            for &lease_state in &[LeaseState::Expired, LeaseState::Suspect, LeaseState::Valid] {
                for (req, mut policy) in table.clone() {
                    let mut inspector = DummyInspector {
                        applied_to_index_term,
                        lease_state,
                    };
                    // Leader can not read local as long as
                    // it has not applied to its term or it does has a valid lease.
                    if policy == RequestPolicy::ReadLocal
                        && (!applied_to_index_term || LeaseState::Valid != inspector.lease_state)
                    {
                        policy = RequestPolicy::ReadIndex;
                    }
                    assert_eq!(inspector.inspect(&req).unwrap(), policy);
                }
            }
        }

        // Read quorum.
        let mut request = raft_cmdpb::Request::default();
        request.set_cmd_type(CmdType::Snap);
        req.set_requests(vec![request].into());
        req.mut_header().set_read_quorum(true);
        let mut inspector = DummyInspector {
            applied_to_index_term: true,
            lease_state: LeaseState::Valid,
        };
        assert_eq!(inspector.inspect(&req).unwrap(), RequestPolicy::ReadIndex);
        req.clear_header();

        // Err(_)
        let mut err_table = vec![];
        for &op in &[CmdType::Prewrite, CmdType::Invalid] {
            let mut request = raft_cmdpb::Request::default();
            request.set_cmd_type(op);
            req.set_requests(vec![request].into());
            err_table.push(req.clone());
        }
        let mut snap = raft_cmdpb::Request::default();
        snap.set_cmd_type(CmdType::Snap);
        let mut put = raft_cmdpb::Request::default();
        put.set_cmd_type(CmdType::Put);
        req.set_requests(vec![snap, put].into());
        err_table.push(req);

        for req in err_table {
            let mut inspector = DummyInspector {
                applied_to_index_term: true,
                lease_state: LeaseState::Valid,
            };
            assert!(inspector.inspect(&req).is_err());
        }
    }

    #[test]
    fn test_propose_queue_find_proposal() {
        let mut pq: ProposalQueue<engine_panic::PanicSnapshot> =
            ProposalQueue::new("tag".to_owned());
        let t = monotonic_raw_now();
        let gen_term = |index: u64| (index / 10) + 1;
        for index in 1..=100 {
            let renew_lease_time = if index % 3 == 1 { None } else { Some(t) };
            pq.push(Proposal {
                is_conf_change: false,
                index,
                term: gen_term(index),
                cb: Callback::write(Box::new(|_| {})),
                renew_lease_time,
                must_pass_epoch_check: false,
            });
        }
        let mut pre_remove = 0;
        for remove_i in &[0, 1, 65, 98, 100] {
            let remove_i = *remove_i;
            // Find a proposal and remove all previous proposals
            for i in 1..=remove_i {
                let p = pq.find_proposal(gen_term(i), i, 0);
                let must_found_proposal = p.is_some() && (i > pre_remove);
                let proposal_removed_previous = p.is_none() && (i <= pre_remove);
                assert!(must_found_proposal || proposal_removed_previous);
                // `find_proposal` will remove proposal so `pop` must return None
                assert!(pq.pop(gen_term(i), i).is_none());
            }
            for i in 1..=100 {
                let pt = pq.find_propose_time(gen_term(i), i);
                if i <= remove_i || i % 3 == 1 {
                    assert!(pt.is_none())
                } else {
                    assert!(pt.is_some())
                };
            }
            pre_remove = remove_i;
        }
    }

    #[test]
    fn test_uncommitted_proposals() {
        struct DropPanic(bool);
        impl Drop for DropPanic {
            fn drop(&mut self) {
                if self.0 {
                    unreachable!()
                }
            }
        }
        fn must_call() -> ExtCallback {
            let mut d = DropPanic(true);
            Box::new(move || {
                d.0 = false;
            })
        }
        fn must_not_call() -> ExtCallback {
            Box::new(move || unreachable!())
        }
        let mut pq: ProposalQueue<engine_panic::PanicSnapshot> =
            ProposalQueue::new("tag".to_owned());

        // (1, 4) and (1, 5) is not committed
        let entries = vec![(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (2, 6), (2, 7)];
        let committed = vec![(1, 1), (1, 2), (1, 3), (2, 6), (2, 7)];
        for (index, term) in entries.clone() {
            if term != 1 {
                continue;
            }
            let cb = if committed.contains(&(index, term)) {
                Callback::write_ext(Box::new(|_| {}), None, Some(must_call()))
            } else {
                Callback::write_ext(Box::new(|_| {}), None, Some(must_not_call()))
            };
            pq.push(Proposal {
                index,
                term,
                cb,
                is_conf_change: false,
                renew_lease_time: None,
                must_pass_epoch_check: false,
            });
        }
        for (index, term) in entries {
            if let Some(mut p) = pq.find_proposal(term, index, 0) {
                p.cb.invoke_committed();
            }
        }
    }

    #[test]
    fn test_cmd_epoch_checker() {
        use engine_test::kv::KvTestSnapshot;
        use std::sync::mpsc;
        fn new_admin_request(cmd_type: AdminCmdType) -> RaftCmdRequest {
            let mut request = RaftCmdRequest::default();
            request.mut_admin_request().set_cmd_type(cmd_type);
            request
        }
        fn new_cb() -> (Callback<KvTestSnapshot>, mpsc::Receiver<()>) {
            let (tx, rx) = mpsc::channel();
            (Callback::write(Box::new(move |_| tx.send(()).unwrap())), rx)
        }

        let region = metapb::Region::default();
        let normal_cmd = RaftCmdRequest::default();
        let split_admin = new_admin_request(AdminCmdType::BatchSplit);
        let prepare_merge_admin = new_admin_request(AdminCmdType::PrepareMerge);
        let change_peer_admin = new_admin_request(AdminCmdType::ChangePeer);

        let mut epoch_checker = CmdEpochChecker::<KvTestSnapshot>::default();

        assert_eq!(epoch_checker.propose_check_epoch(&split_admin, 10), None);
        assert_eq!(epoch_checker.term, 10);
        epoch_checker.post_propose(AdminCmdType::BatchSplit, 5, 10);
        assert_eq!(epoch_checker.proposed_admin_cmd.len(), 1);

        // Both conflict with the split admin cmd
        assert_eq!(epoch_checker.propose_check_epoch(&normal_cmd, 10), Some(5));
        assert_eq!(
            epoch_checker.propose_check_epoch(&prepare_merge_admin, 10),
            Some(5)
        );

        assert_eq!(
            epoch_checker.propose_check_epoch(&change_peer_admin, 10),
            None
        );
        epoch_checker.post_propose(AdminCmdType::ChangePeer, 6, 10);
        assert_eq!(epoch_checker.proposed_admin_cmd.len(), 2);

        assert_eq!(
            epoch_checker.last_cmd_index(AdminCmdType::BatchSplit),
            Some(5)
        );
        assert_eq!(
            epoch_checker.last_cmd_index(AdminCmdType::ChangePeer),
            Some(6)
        );
        assert_eq!(
            epoch_checker.last_cmd_index(AdminCmdType::PrepareMerge),
            None
        );

        // Conflict with the change peer admin cmd
        assert_eq!(
            epoch_checker.propose_check_epoch(&change_peer_admin, 10),
            Some(6)
        );
        // Conflict with the split admin cmd
        assert_eq!(epoch_checker.propose_check_epoch(&normal_cmd, 10), Some(5));
        // Conflict with the change peer admin cmd
        assert_eq!(
            epoch_checker.propose_check_epoch(&prepare_merge_admin, 10),
            Some(6)
        );

        epoch_checker.advance_apply(4, 10, &region);
        // Have no effect on `proposed_admin_cmd`
        assert_eq!(epoch_checker.proposed_admin_cmd.len(), 2);

        epoch_checker.advance_apply(5, 10, &region);
        // Left one change peer admin cmd
        assert_eq!(epoch_checker.proposed_admin_cmd.len(), 1);

        assert_eq!(epoch_checker.propose_check_epoch(&normal_cmd, 10), None);

        assert_eq!(epoch_checker.propose_check_epoch(&split_admin, 10), Some(6));
        // Change term to 11
        assert_eq!(epoch_checker.propose_check_epoch(&split_admin, 11), None);
        assert_eq!(epoch_checker.term, 11);
        // Should be empty
        assert_eq!(epoch_checker.proposed_admin_cmd.len(), 0);

        // Test attaching multiple callbacks.
        epoch_checker.post_propose(AdminCmdType::BatchSplit, 7, 12);
        let mut rxs = vec![];
        for _ in 0..3 {
            let conflict_idx = epoch_checker.propose_check_epoch(&normal_cmd, 12).unwrap();
            let (cb, rx) = new_cb();
            epoch_checker.attach_to_conflict_cmd(conflict_idx, cb);
            rxs.push(rx);
        }
        epoch_checker.advance_apply(7, 12, &region);
        for rx in rxs {
            rx.try_recv().unwrap();
        }

        // Should invoke callbacks when term is increased.
        epoch_checker.post_propose(AdminCmdType::BatchSplit, 8, 12);
        let (cb, rx) = new_cb();
        epoch_checker.attach_to_conflict_cmd(8, cb);
        assert_eq!(epoch_checker.propose_check_epoch(&normal_cmd, 13), None);
        rx.try_recv().unwrap();

        // Should invoke callbacks when it's dropped.
        epoch_checker.post_propose(AdminCmdType::BatchSplit, 9, 13);
        let (cb, rx) = new_cb();
        epoch_checker.attach_to_conflict_cmd(9, cb);
        drop(epoch_checker);
        rx.try_recv().unwrap();
    }
}
