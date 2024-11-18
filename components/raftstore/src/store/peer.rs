// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::RefCell,
    cmp,
    collections::VecDeque,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
    u64, usize,
};

use bitflags::bitflags;
use bytes::Bytes;
use collections::{HashMap, HashSet};
use crossbeam::{atomic::AtomicCell, channel::TrySendError};
use engine_traits::{
    Engines, KvEngine, PerfContext, RaftEngine, Snapshot, WriteBatch, WriteOptions, CF_DEFAULT,
    CF_LOCK, CF_WRITE,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use getset::{Getters, MutGetters};
use keys::{enc_end_key, enc_start_key};
use kvproto::{
    errorpb,
    kvrpcpb::{DiskFullOpt, ExtraOp as TxnExtraOp},
    metapb::{self, PeerRole},
    pdpb::PeerStats,
    raft_cmdpb::{
        self, AdminCmdType, AdminResponse, CmdType, CommitMergeRequest, PutRequest, RaftCmdRequest,
        RaftCmdResponse, Request, TransferLeaderRequest, TransferLeaderResponse,
    },
    raft_serverpb::{
        ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftApplyState, RaftMessage,
    },
    replication_modepb::{
        DrAutoSyncState, RegionReplicationState, RegionReplicationStatus, ReplicationMode,
    },
};
use parking_lot::RwLockUpgradableReadGuard;
use pd_client::{Feature, INVALID_ID};
use protobuf::Message;
use raft::{
    self,
    eraftpb::{self, Entry, EntryType, MessageType},
    GetEntriesContext, LightReady, ProgressState, RawNode, Ready, SnapshotStatus, StateRole,
    INVALID_INDEX, NO_LIMIT,
};
use rand::seq::SliceRandom;
use smallvec::SmallVec;
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    box_err,
    codec::number::decode_u64,
    debug, error, info,
    store::find_peer_by_id,
    sys::disk::DiskUsage,
    time::{duration_to_sec, monotonic_raw_now, Instant as TiInstant, InstantExt},
    warn,
    worker::Scheduler,
    Either,
};
use time::{Duration as TimeDuration, Timespec};
use tracker::{TrackerTokenArray, GLOBAL_TRACKERS};
use txn_types::{TimeStamp, WriteBatchFlags};
use uuid::Uuid;

use super::{
    cmd_resp,
    local_metrics::{IoType, RaftMetrics},
    metrics::*,
    peer_storage::{write_peer_state, CheckApplyingSnapStatus, HandleReadyResult, PeerStorage},
    read_queue::{ReadIndexQueue, ReadIndexRequest},
    transport::Transport,
    util::{
        self, check_req_region_epoch, is_initial_msg, AdminCmdEpochState, ChangePeerI,
        ConfChangeKind, Lease, LeaseState, NORMAL_REQ_CHECK_CONF_VER, NORMAL_REQ_CHECK_VER,
    },
    worker::BucketStatsInfo,
    DestroyPeerJob, LocalReadContext,
};
use crate::{
    coprocessor::{
        split_observer::NO_VALID_SPLIT_KEY, CoprocessorHost, RegionChangeEvent, RegionChangeReason,
        RoleChange,
    },
    errors::RAFTSTORE_IS_BUSY,
    router::{RaftStoreRouter, ReadContext},
    store::{
        async_io::{read::ReadTask, write::WriteMsg, write_router::WriteRouter},
        fsm::{
            apply::{self, CatchUpLogs},
            store::PollContext,
            Apply, ApplyMetrics, ApplyTask, Proposal,
        },
        hibernate_state::GroupState,
        memory::{needs_evict_entry_cache, MEMTRACE_RAFT_ENTRIES},
        msg::{CasualMessage, ErrorCallback, RaftCommand},
        peer_storage::HandleSnapshotResult,
        snapshot_backup::{AbortReason, SnapshotBrState},
        txn_ext::LocksStatus,
        unsafe_recovery::{ForceLeaderState, UnsafeRecoveryState},
        util::{admin_cmd_epoch_lookup, RegionReadProgress},
        worker::{
            CleanupTask, CompactTask, HeartbeatTask, RaftlogGcTask, ReadDelegate, ReadExecutor,
            ReadProgress, RegionTask, SplitCheckTask,
        },
        Callback, Config, GlobalReplicationState, PdTask, PeerMsg, ReadCallback, ReadIndexContext,
        ReadResponse, TxnExt, WriteCallback, RAFT_INIT_LOG_INDEX,
    },
    Error, Result,
};

const SHRINK_CACHE_CAPACITY: usize = 64;
// 1s
const MIN_BCAST_WAKE_UP_INTERVAL: u64 = 1_000;
const REGION_READ_PROGRESS_CAP: usize = 128;

const SNAP_GEN_PRECHECK_FEATURE: Feature = Feature::require(8, 2, 0);

#[doc(hidden)]
pub const MAX_COMMITTED_SIZE_PER_READY: u64 = 16 * 1024 * 1024;

/// The returned states of the peer after checking whether it is stale
#[derive(Debug, PartialEq)]
pub enum StaleState {
    Valid,
    ToValidate,
    LeaderMissing,
    MaybeLeaderMissing,
}

#[derive(Debug)]
pub struct ProposalQueue<C> {
    region_id: u64,
    peer_id: u64,
    queue: VecDeque<Proposal<C>>,
}

impl<C: WriteCallback> ProposalQueue<C> {
    pub fn new(region_id: u64, peer_id: u64) -> ProposalQueue<C> {
        ProposalQueue {
            region_id,
            peer_id,
            queue: VecDeque::new(),
        }
    }

    /// Find the trackers of given index.
    /// Caller should check if term is matched before using trackers.
    pub fn find_trackers(&self, index: u64) -> Option<(u64, C::TimeTrackerListRef<'_>)> {
        self.queue
            .binary_search_by_key(&index, |p: &Proposal<_>| p.index)
            .ok()
            .map(|i| (self.queue[i].term, self.queue[i].cb.write_trackers()))
    }

    #[inline]
    pub fn queue_mut(&mut self) -> &mut VecDeque<Proposal<C>> {
        &mut self.queue
    }

    pub fn find_propose_time(&self, term: u64, index: u64) -> Option<Timespec> {
        self.queue
            .binary_search_by_key(&(term, index), |p: &Proposal<_>| (p.term, p.index))
            .ok()
            .and_then(|i| self.queue[i].propose_time)
    }

    // Find proposal in front or at the given term and index
    pub fn pop(&mut self, term: u64, index: u64) -> Option<Proposal<C>> {
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
    pub fn find_proposal(
        &mut self,
        term: u64,
        index: u64,
        current_term: u64,
    ) -> Option<Proposal<C>> {
        while let Some(p) = self.pop(term, index) {
            if p.term == term {
                if p.index == index {
                    return if p.cb.is_none() { None } else { Some(p) };
                } else {
                    panic!(
                        "[region {}] {} unexpected callback at term {}, found index {}, expected {}",
                        self.region_id, self.peer_id, term, p.index, index
                    );
                }
            } else {
                apply::notify_stale_req(current_term, p.cb);
            }
        }
        None
    }

    #[inline]
    pub fn oldest(&self) -> Option<&Proposal<C>> {
        self.queue.front()
    }

    pub fn push(&mut self, p: Proposal<C>) {
        if let Some(f) = self.queue.back() {
            // The term must be increasing among all log entries and the index
            // must be increasing inside a given term
            assert!((p.term, p.index) > (f.term, f.index));
        }
        self.queue.push_back(p);
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn gc(&mut self) {
        if self.queue.capacity() > SHRINK_CACHE_CAPACITY && self.queue.len() < SHRINK_CACHE_CAPACITY
        {
            self.queue.shrink_to_fit();
        }
    }

    pub fn back(&self) -> Option<&Proposal<C>> {
        self.queue.back()
    }
}

bitflags! {
    // TODO: maybe declare it as protobuf struct is better.
    /// A bitmap contains some useful flags when dealing with `eraftpb::Entry`.
    pub struct ProposalContext: u8 {
        const SYNC_LOG       = 0b0000_0001;
        const SPLIT          = 0b0000_0010;
        const PREPARE_MERGE  = 0b0000_0100;
        const COMMIT_MERGE   = 0b0000_1000;
        const ROLLBACK_MERGE = 0b0001_0000;
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
    // Although it's a deque, because of the characteristics of the settings from
    // `admin_cmd_epoch_lookup`, the max size of admin cmd is 2, i.e. split/merge and change
    // peer.
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

    /// Check if the proposal can be proposed on the basis of its epoch and
    /// previous proposed admin cmds.
    ///
    /// Returns None if passing the epoch check, otherwise returns a index which
    /// is the last admin cmd index conflicted with this proposal.
    fn propose_check_epoch(&mut self, req: &RaftCmdRequest, term: u64) -> Option<u64> {
        self.maybe_update_term(term);
        let (check_ver, check_conf_ver) = if !req.has_admin_request() {
            (NORMAL_REQ_CHECK_VER, NORMAL_REQ_CHECK_CONF_VER)
        } else {
            let cmd_type = req.get_admin_request().get_cmd_type();
            let epoch_state = admin_cmd_epoch_lookup(cmd_type);
            (epoch_state.check_ver, epoch_state.check_conf_ver)
        };
        self.last_conflict_index(check_ver, check_conf_ver)
    }

    fn post_propose(&mut self, cmd_type: AdminCmdType, index: u64, term: u64) {
        self.maybe_update_term(term);
        let epoch_state = admin_cmd_epoch_lookup(cmd_type);
        assert!(
            self.last_conflict_index(epoch_state.check_ver, epoch_state.check_conf_ver)
                .is_none()
        );

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
    fn last_cmd_index(&mut self, cmd_type: AdminCmdType) -> Option<u64> {
        self.proposed_admin_cmd
            .iter()
            .rev()
            .find(|cmd| cmd.cmd_type == cmd_type)
            .map(|cmd| cmd.index)
    }

    fn advance_apply(&mut self, index: u64, term: u64, region: &metapb::Region) {
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
                    cb.report_error(resp);
                }
            } else {
                break;
            }
            self.proposed_admin_cmd.pop_front();
        }
    }

    fn attach_to_conflict_cmd(&mut self, index: u64, cb: Callback<S>) {
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
        if tikv_util::thread_group::is_shutdown(!cfg!(test)) {
            for mut state in self.proposed_admin_cmd.drain(..) {
                state.cbs.clear();
            }
        } else {
            for state in self.proposed_admin_cmd.drain(..) {
                for cb in state.cbs {
                    apply::notify_stale_req(self.term, cb);
                }
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ApplySnapshotContext {
    /// The number of ready which has a snapshot.
    pub ready_number: u64,
    /// Whether this snapshot is scheduled.
    pub scheduled: bool,
    /// The message should be sent after snapshot is applied.
    pub msgs: Vec<eraftpb::Message>,
    pub persist_res: Option<PersistSnapshotResult>,
    /// Destroy the peer after apply task finished or aborted
    /// This flag is set to true when the peer destroy is skipped because of
    /// running snapshot task.
    /// This is to accelerate peer destroy without waiting for extra destory
    /// peer message.
    pub destroy_peer_after_apply: bool,
}

#[derive(PartialEq, Debug)]
pub struct PersistSnapshotResult {
    /// prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
    pub destroy_regions: Vec<metapb::Region>,
    pub for_witness: bool,
}

#[derive(Debug)]
pub struct UnpersistedReady {
    /// Number of ready.
    pub number: u64,
    /// Max number of following ready whose data to be persisted is empty.
    pub max_empty_number: u64,
    pub raft_msgs: Vec<Vec<eraftpb::Message>>,
}

pub struct ReadyResult {
    pub state_role: Option<StateRole>,
    pub has_new_entries: bool,
    pub has_write_ready: bool,
}

// Propose a read index request to the raft group, return the request id and
// whether this request had dropped silently
// #[RaftstoreCommon], copied from Peer::propose_read_index
pub fn propose_read_index<T: raft::Storage>(
    raft_group: &mut RawNode<T>,
    request: Option<&raft_cmdpb::ReadIndexRequest>,
) -> (Uuid, bool) {
    let last_pending_read_count = raft_group.raft.pending_read_count();
    let last_ready_read_count = raft_group.raft.ready_read_count();

    let id = Uuid::new_v4();
    raft_group.read_index(ReadIndexContext::fields_to_bytes(id, request, None));

    let pending_read_count = raft_group.raft.pending_read_count();
    let ready_read_count = raft_group.raft.ready_read_count();
    (
        id,
        pending_read_count == last_pending_read_count && ready_read_count == last_ready_read_count,
    )
}

pub fn should_renew_lease(
    is_leader: bool,
    is_splitting: bool,
    is_merging: bool,
    has_force_leader: bool,
) -> bool {
    // A splitting leader should not renew its lease.
    // Because we split regions asynchronous, the leader may read stale results
    // if splitting runs slow on the leader.
    // A merging leader should not renew its lease.
    // Because we merge regions asynchronous, the leader may read stale results
    // if commit merge runs slow on sibling peers.
    // when it enters force leader mode, should not renew lease.
    is_leader && !is_splitting && !is_merging && !has_force_leader
}

// check if the request can be amended to the last pending read?
// return true if it can.
pub fn can_amend_read<C>(
    last_pending_read: Option<&ReadIndexRequest<C>>,
    req: &RaftCmdRequest,
    lease_state: LeaseState,
    max_lease: TimeDuration,
    now: Timespec,
) -> bool {
    match lease_state {
        // Here, combining the new read request with the previous one even if the lease expired
        // is ok because in this case, the previous read index must be sent out with a valid
        // lease instead of a suspect lease. So there must no pending transfer-leader
        // proposals before or after the previous read index, and the lease can be renewed
        // when get heartbeat responses.
        LeaseState::Valid | LeaseState::Expired => {
            if let Some(read) = last_pending_read {
                let is_read_index_request = req
                    .get_requests()
                    .first()
                    .map(|req| req.has_read_index())
                    .unwrap_or_default();
                // A read index request or a read with addition request always needs the
                // response of checking memory lock for async
                // commit, so we cannot apply the optimization here
                if !is_read_index_request
                    && read.addition_request.is_none()
                    && read.propose_time + max_lease > now
                {
                    return true;
                }
            }
        }
        // If the current lease is suspect, new read requests can't be appended into
        // `pending_reads` because if the leader is transferred, the latest read could
        // be dirty.
        _ => {}
    }
    false
}

/// The SplitCheckTrigger maintains the internal status to determine
/// if a split check task should be triggered.
#[derive(Default, Debug)]
pub struct SplitCheckTrigger {
    /// An inaccurate difference in region size since last reset.
    /// It is used to decide whether split check is needed.
    size_diff_hint: u64,
    /// An inaccurate difference in region size after compaction.
    /// It is used to trigger check split to update approximate size and keys
    /// after space reclamation of deleted entries.
    pub compaction_declined_bytes: u64,
    /// Approximate size of the region.
    pub approximate_size: Option<u64>,
    may_split_size: Option<bool>,
    /// Approximate keys of the region.
    pub approximate_keys: Option<u64>,
    may_split_keys: Option<bool>,
    /// Whether this region has scheduled a split check task. If we just
    /// splitted  the region or ingested one file which may be overlapped
    /// with the existed data, reset the flag so that the region can be
    /// splitted again.
    may_skip_split_check: bool,
}

impl SplitCheckTrigger {
    pub fn should_skip(&self, threshold: u64) -> bool {
        self.may_skip_split_check
            && self.compaction_declined_bytes < threshold
            && self.size_diff_hint < threshold
    }

    pub fn post_triggered(&mut self) {
        self.size_diff_hint = 0;
        self.compaction_declined_bytes = 0;
        // The task is scheduled, the next tick may skip it only when the size and keys
        // are small.
        // If either size or keys are big enough to do a split,
        // keep split check tick until split is done
        if !matches!(self.may_split_size, Some(true)) && !matches!(self.may_split_keys, Some(true))
        {
            self.may_skip_split_check = true;
        }
    }

    pub fn post_split(&mut self) {
        self.size_diff_hint = 0;
        self.may_split_keys = None;
        self.may_split_size = None;
        // It's not correct anymore, so set it to false to schedule a split check task.
        self.may_skip_split_check = false;
    }

    pub fn add_size_diff(&mut self, size_diff: i64) {
        let diff = self.size_diff_hint as i64 + size_diff;
        self.size_diff_hint = cmp::max(diff, 0) as u64;
    }

    pub fn reset_skip_check(&mut self) {
        self.may_skip_split_check = false;
    }

    pub fn on_clear_region_size(&mut self) {
        self.approximate_size = None;
        self.approximate_keys = None;
        self.may_split_size = None;
        self.may_split_keys = None;
        self.may_skip_split_check = false;
    }

    pub fn on_approximate_region_size(&mut self, size: Option<u64>, splitable: Option<bool>) {
        // If size is none, it means no estimated size
        if size.is_some() {
            self.approximate_size = size;
        }

        if splitable.is_some() {
            self.may_split_size = splitable;
        }

        // If the region is truly splitable,
        // may_skip_split_check should be false
        if matches!(splitable, Some(true)) {
            self.may_skip_split_check = false;
        }
    }

    pub fn on_approximate_region_keys(&mut self, keys: Option<u64>, splitable: Option<bool>) {
        // if keys is none, it means no estimated keys
        if keys.is_some() {
            self.approximate_keys = keys;
        }

        if splitable.is_some() {
            self.may_split_keys = splitable;
        }

        // If the region is truly splitable,
        // may_skip_split_check should be false
        if matches!(splitable, Some(true)) {
            self.may_skip_split_check = false;
        }
    }

    pub fn on_ingest_sst_result(&mut self, size: u64, keys: u64) {
        self.approximate_size = Some(self.approximate_size.unwrap_or_default() + size);
        self.approximate_keys = Some(self.approximate_keys.unwrap_or_default() + keys);

        // The ingested file may be overlapped with the data in engine, so we need to
        // check it again to get the accurate value.
        self.may_skip_split_check = false;
    }
}

#[derive(Getters, MutGetters)]
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
    /// The online configurable Raft configurations
    raft_max_inflight_msgs: usize,
    /// The cache of meta information for Region's other Peers.
    peer_cache: RefCell<HashMap<u64, metapb::Peer>>,
    /// Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: HashMap<u64, Instant>,
    /// Record the waiting data status of each follower or learner peer.
    pub wait_data_peers: Vec<u64>,
    /// This peer is created by a raft message from `create_by_peer`.
    create_by_peer: Option<metapb::Peer>,

    proposals: ProposalQueue<Callback<EK::Snapshot>>,
    leader_missing_time: Option<Instant>,
    #[getset(get = "pub", get_mut = "pub")]
    leader_lease: Lease,
    pending_reads: ReadIndexQueue<Callback<EK::Snapshot>>,
    /// Threshold of long uncommitted proposals.
    ///
    /// Note that this is a dynamically changing value. Check the
    /// `has_long_uncommitted_proposals` method for details.
    long_uncommitted_threshold: Duration,

    /// If it fails to send messages to leader.
    pub leader_unreachable: bool,
    /// Indicates whether the peer should be woken up.
    pub should_wake_up: bool,
    /// Whether this peer is destroyed asynchronously.
    /// If it's true,
    /// - when merging, its data in storeMeta will be removed early by the
    ///   target peer.
    /// - all read requests must be rejected.
    pub pending_remove: bool,
    /// Currently it's used to indicate whether the witness -> non-witess
    /// convertion operation is complete. The meaning of completion is that
    /// this peer must contain the applied data, then PD can consider that
    /// the conversion operation is complete, and can continue to schedule
    /// other operators to prevent the existence of multiple witnesses in
    /// the same time period.
    pub wait_data: bool,

    /// When the witness becomes non-witness, it need to actively request a
    /// snapshot from the leader, but the request may fail, so we need to save
    /// the request index for retrying.
    pub request_index: u64,

    /// It's used to identify the situation where the region worker is
    /// generating and sending snapshots when the newly elected leader by Raft
    /// applies the switch witness cmd which commited before the election. This
    /// flag will prevent immediate data clearing and will be cleared after
    /// the successful transfer of leadership.
    pub delay_clean_data: bool,

    /// When the witness becomes non-witness, it need to actively request a
    /// snapshot from the leader, In order to avoid log lag, we need to reject
    /// the leader's `MsgAppend` request unless the `term` of the `last index`
    /// is less than the peer's current `term`.
    pub should_reject_msgappend: bool,

    /// Force leader state is only used in online recovery when the majority of
    /// peers are missing. In this state, it forces one peer to become leader
    /// out of accordance with Raft election rule, and forbids any
    /// read/write proposals. With that, we can further propose remove
    /// failed-nodes conf-change, to make the Raft group forms majority and
    /// works normally later on.
    ///
    /// For details, see the comment of `ForceLeaderState`.
    pub force_leader: Option<ForceLeaderState>,

    /// Record the instants of peers being added into the configuration.
    /// Remove them after they are not pending any more.
    pub peers_start_pending_time: Vec<(u64, Instant)>,
    /// A inaccurate cache about which peer is marked as down.
    down_peer_ids: Vec<u64>,
    /// the split check trigger
    pub split_check_trigger: SplitCheckTrigger,
    /// The count of deleted keys since last reset.
    delete_keys_hint: u64,

    /// The state for consistency check.
    pub consistency_state: ConsistencyState,

    /// The counter records pending snapshot requests.
    pub pending_request_snapshot_count: Arc<AtomicUsize>,
    /// The index of last scheduled committed raft log.
    pub last_applying_idx: u64,
    pub max_apply_unpersisted_log_limit: u64,
    /// The minimum raft index after which apply unpersisted raft log can be
    /// enabled. We force disable apply unpersisted raft log in following 2
    /// situation:
    /// 1) Raft term changes. In this case, the min index is set to the current
    ///    last index. This is to let apply unpersisted log only happen within
    ///    the same term so it's easier to if any applied but not persisted logs
    ///    has changed in which case we should just panic to avoid data
    ///    inconsistency.
    /// 2) Propose PrepareMerge. In this case, the min index is set to that raft
    ///    log's index. This is to make online unsafe recovery easier when
    ///    region state is PrepareMerge.
    pub min_safe_index_for_unpersisted_apply: u64,
    /// The index of last compacted raft log. It is used for the next compact
    /// log task.
    pub last_compacted_idx: u64,
    /// Record the time of the last raft log compact, the witness should query
    /// the leader periodically whether `voter_replicated_index` is updated
    /// if CompactLog admin command isn't triggered for a while.
    pub last_compacted_time: Instant,
    /// When the peer is witness, and there is any voter lagging behind, the
    /// log truncation of the witness shouldn't be triggered even if it's
    /// force mode, and this item will be set to `true`, after all pending
    /// compact cmds have been handled, it will be set to `false`.
    pub has_pending_compact_cmd: bool,
    /// The index of the latest urgent proposal index.
    last_urgent_proposal_idx: u64,
    /// The index of the latest committed split command.
    last_committed_split_idx: u64,
    /// The index of last sent snapshot
    last_sent_snapshot_idx: u64,
    /// Approximate size of logs that is applied but not compacted yet.
    pub raft_log_size_hint: u64,

    /// The write fence index.
    /// If there are pessimistic locks, PrepareMerge can be proposed after
    /// applying to this index. When a pending PrepareMerge exists, no more
    /// write commands should be proposed. This avoids proposing pessimistic
    /// locks that are already deleted before PrepareMerge.
    pub prepare_merge_fence: u64,
    pub pending_prepare_merge: Option<RaftCmdRequest>,

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
    pub bcast_wake_up_time: Option<TiInstant>,
    /// Current replication mode version.
    pub replication_mode_version: u64,
    /// The required replication state at current version.
    pub dr_auto_sync_state: DrAutoSyncState,
    /// A flag that caches sync state. It's set to true when required
    /// replication state is reached for current region.
    pub replication_sync: bool,

    /// The known newest conf version and its corresponding peer list
    /// Send to these peers to check whether itself is stale.
    pub check_stale_conf_ver: u64,
    pub check_stale_peers: Vec<metapb::Peer>,
    /// Whether this peer is created by replication and is the first
    /// one of this region on local store.
    pub local_first_replicate: bool,

    pub txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,

    /// Transaction extensions related to this peer.
    pub txn_ext: Arc<TxnExt>,

    /// Check whether this proposal can be proposed based on its epoch.
    cmd_epoch_checker: CmdEpochChecker<EK::Snapshot>,

    // disk full peer set.
    pub disk_full_peers: DiskFullPeers,

    // show whether an already disk full TiKV appears in the potential majority set.
    pub dangerous_majority_set: bool,

    // region merge logic need to be broadcast to all followers when disk full happens.
    pub has_region_merge_proposal: bool,

    pub region_merge_proposal_index: u64,

    pub read_progress: Arc<RegionReadProgress>,

    pub memtrace_raft_entries: usize,
    /// Used for sending write msg.
    write_router: WriteRouter<EK, ER>,
    /// Used for async write io.
    unpersisted_readies: VecDeque<UnpersistedReady>,
    /// The message count in `unpersisted_readies` for memory caculation.
    unpersisted_message_count: usize,
    /// Used for sync write io.
    unpersisted_ready: Option<Ready>,
    /// The last known persisted number.
    persisted_number: u64,
    /// The context of applying snapshot.
    apply_snap_ctx: Option<ApplySnapshotContext>,
    /// region buckets info in this region.
    region_buckets_info: BucketStatsInfo,
    /// lead_transferee if this peer(leader) is in a leadership transferring.
    pub lead_transferee: u64,
    pub unsafe_recovery_state: Option<UnsafeRecoveryState>,
    pub snapshot_recovery_state: Option<SnapshotBrState>,

    last_record_safe_point: u64,
    /// Used for checking whether the peer is busy on apply.
    /// * `None` => the peer has no pending logs for apply or already finishes
    ///   applying.
    /// * `Some(false)` => initial state, not be recorded.
    /// * `Some(true)` => busy on apply, and already recorded.
    pub busy_on_apply: Option<bool>,
    /// The index of last commited idx in the leader. It's used to check whether
    /// this peer has raft log gaps and whether should be marked busy on
    /// apply.
    pub last_leader_committed_idx: Option<u64>,

    /// Used to record uncampaigned regions, which are the new regions
    /// created when a follower applies a split. If the follower becomes a
    /// leader, a campaign is triggered for those regions.
    /// The first element is the region id, the second element is the time when
    /// pending regions are added, used to clear the pending regions after an
    /// election timeout.
    pub uncampaigned_new_regions: Option<(Vec<u64>, Instant)>,
}

impl<EK, ER> Peer<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        store_id: u64,
        cfg: &Config,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
        raftlog_fetch_scheduler: Scheduler<ReadTask<EK>>,
        engines: Engines<EK, ER>,
        region: &metapb::Region,
        peer: metapb::Peer,
        wait_data: bool,
        create_by_peer: Option<metapb::Peer>,
    ) -> Result<Peer<EK, ER>> {
        let peer_id = peer.get_id();
        if peer_id == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let tag = format!("[region {}] {}", region.get_id(), peer.get_id());

        let ps = PeerStorage::new(
            engines,
            region,
            region_scheduler,
            raftlog_fetch_scheduler,
            peer.get_id(),
            tag.clone(),
        )?;
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
            check_quorum: !cfg.unsafe_disable_check_quorum,
            skip_bcast_commit: true,
            pre_vote: cfg.prevote,
            max_committed_size_per_ready: MAX_COMMITTED_SIZE_PER_READY,
            priority: if peer.is_witness { -1 } else { 0 },
            // always disable applying unpersisted log at initialization,
            // will enable it after applying to the current last_index.
            max_apply_unpersisted_log_limit: 0,
            ..Default::default()
        };

        let logger = slog_global::get_global().new(slog::o!("region_id" => region.get_id()));
        let raft_group = RawNode::new(&raft_cfg, ps, &logger)?;
        let last_index = raft_group.store().last_index();
        // In order to avoid excessive log accumulation due to the loss of pending
        // compaction cmds after the witness is restarted, it will actively pull
        // voter_request_index once at start.
        let has_pending_compact_cmd = peer.is_witness;

        let mut peer = Peer {
            peer,
            region_id: region.get_id(),
            raft_group,
            raft_max_inflight_msgs: cfg.raft_max_inflight_msgs,
            proposals: ProposalQueue::new(region.get_id(), peer_id),
            pending_reads: ReadIndexQueue::new(tag.clone()),
            long_uncommitted_threshold: cfg.long_uncommitted_base_threshold.0,
            peer_cache: RefCell::new(HashMap::default()),
            peer_heartbeats: HashMap::default(),
            wait_data_peers: Vec::default(),
            create_by_peer,
            peers_start_pending_time: vec![],
            down_peer_ids: vec![],
            split_check_trigger: SplitCheckTrigger::default(),
            delete_keys_hint: 0,
            leader_unreachable: false,
            pending_remove: false,
            wait_data,
            request_index: last_index,
            delay_clean_data: false,
            should_reject_msgappend: false,
            should_wake_up: false,
            force_leader: None,
            pending_merge_state: None,
            want_rollback_merge_peers: HashSet::default(),
            pending_request_snapshot_count: Arc::new(AtomicUsize::new(0)),
            prepare_merge_fence: 0,
            pending_prepare_merge: None,
            last_committed_prepare_merge_idx: 0,
            leader_missing_time: Some(Instant::now()),
            tag: tag.clone(),
            last_applying_idx: applied_index,
            max_apply_unpersisted_log_limit: cfg.max_apply_unpersisted_log_limit,
            min_safe_index_for_unpersisted_apply: last_index,
            last_compacted_idx: 0,
            last_compacted_time: Instant::now(),
            has_pending_compact_cmd,
            last_urgent_proposal_idx: u64::MAX,
            last_committed_split_idx: 0,
            last_sent_snapshot_idx: 0,
            consistency_state: ConsistencyState {
                last_check_time: Instant::now(),
                index: INVALID_INDEX,
                context: vec![],
                hash: vec![],
            },
            raft_log_size_hint: 0,
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
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
            txn_ext: Arc::new(TxnExt::default()),
            cmd_epoch_checker: Default::default(),
            disk_full_peers: DiskFullPeers::default(),
            dangerous_majority_set: false,
            has_region_merge_proposal: false,
            region_merge_proposal_index: 0_u64,
            read_progress: Arc::new(RegionReadProgress::new(
                region,
                applied_index,
                REGION_READ_PROGRESS_CAP,
                peer_id,
            )),
            last_record_safe_point: 0,
            memtrace_raft_entries: 0,
            write_router: WriteRouter::new(tag),
            unpersisted_readies: VecDeque::default(),
            unpersisted_message_count: 0,
            unpersisted_ready: None,
            persisted_number: 0,
            apply_snap_ctx: None,
            region_buckets_info: BucketStatsInfo::default(),
            lead_transferee: raft::INVALID_ID,
            unsafe_recovery_state: None,
            snapshot_recovery_state: None,
            busy_on_apply: Some(false),
            last_leader_committed_idx: None,
            uncampaigned_new_regions: None,
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        let persisted_index = peer.raft_group.raft.raft_log.persisted;
        peer.mut_store().update_cache_persisted(persisted_index);

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
        let enable = !matches!(
            state.status().get_dr_auto_sync().get_state(),
            DrAutoSyncState::Async | DrAutoSyncState::SyncRecover
        );
        self.raft_group.raft.enable_group_commit(enable);
        self.dr_auto_sync_state = state.status().get_dr_auto_sync().get_state();
    }

    /// Updates replication mode.
    pub fn switch_replication_mode(&mut self, state: &Mutex<GlobalReplicationState>) {
        self.replication_sync = false;
        let guard = state.lock().unwrap();
        let (enable_group_commit, calculate_group_id) =
            if guard.status().get_mode() == ReplicationMode::Majority {
                self.replication_mode_version = 0;
                self.dr_auto_sync_state = DrAutoSyncState::Async;
                (false, false)
            } else {
                self.dr_auto_sync_state = guard.status().get_dr_auto_sync().get_state();
                self.replication_mode_version = guard.status().get_dr_auto_sync().state_id;
                match guard.status().get_dr_auto_sync().get_state() {
                    // SyncRecover will enable group commit after it catches up logs.
                    DrAutoSyncState::Async => (false, false),
                    DrAutoSyncState::SyncRecover => (false, true),
                    _ => (true, true),
                }
            };
        drop(guard);
        self.switch_group_commit(enable_group_commit, calculate_group_id, state);
    }

    fn switch_group_commit(
        &mut self,
        enable_group_commit: bool,
        calculate_group_id: bool,
        state: &Mutex<GlobalReplicationState>,
    ) {
        if enable_group_commit || calculate_group_id {
            let mut guard = state.lock().unwrap();
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
        }
        self.raft_group
            .raft
            .enable_group_commit(enable_group_commit);
        info!("switch replication mode"; "version" => self.replication_mode_version, "region_id" => self.region_id, "peer_id" => self.peer.id, "enable_group_commit" => enable_group_commit);
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
        self.maybe_gen_approximate_buckets(ctx);
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

    #[inline]
    pub fn maybe_update_apply_unpersisted_log_state(&mut self, applied_index: u64) {
        if self.min_safe_index_for_unpersisted_apply > 0
            && self.min_safe_index_for_unpersisted_apply <= applied_index
        {
            if self.max_apply_unpersisted_log_limit > 0
                && self
                    .raft_group
                    .raft
                    .raft_log
                    .max_apply_unpersisted_log_limit
                    == 0
            {
                RAFT_ENABLE_UNPERSISTED_APPLY_GAUGE.inc();
            }
            self.raft_group
                .raft
                .set_max_apply_unpersisted_log_limit(self.max_apply_unpersisted_log_limit);
            self.min_safe_index_for_unpersisted_apply = 0;
        }
    }

    #[inline]
    pub fn disable_apply_unpersisted_log(&mut self, min_enable_index: u64) {
        self.min_safe_index_for_unpersisted_apply =
            std::cmp::max(self.min_safe_index_for_unpersisted_apply, min_enable_index);
        if self
            .raft_group
            .raft
            .raft_log
            .max_apply_unpersisted_log_limit
            > 0
        {
            self.raft_group.raft.set_max_apply_unpersisted_log_limit(0);
            RAFT_ENABLE_UNPERSISTED_APPLY_GAUGE.dec();
        }
    }

    pub fn maybe_append_merge_entries(&mut self, merge: &CommitMergeRequest) -> Option<u64> {
        let mut entries = merge.get_entries();
        if entries.is_empty() {
            // Though the entries is empty, it is possible that one source peer has caught
            // up the logs but commit index is not updated. If other source peers are
            // already destroyed, so the raft group will not make any progress, namely the
            // source peer can not get the latest commit index anymore.
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
            // There are maybe some logs not included in CommitMergeRequest's entries, like
            // CompactLog, so the commit index may exceed the last index of the entires from
            // CommitMergeRequest. If that, no need to append
            if self.raft_group.raft.raft_log.committed - log_idx >= entries.len() as u64 {
                return None;
            }
            entries = &entries[(self.raft_group.raft.raft_log.committed - log_idx) as usize..];
            log_idx = self.raft_group.raft.raft_log.committed;
        }
        let log_term = self.get_index_term(log_idx);

        let last_log = entries.last().unwrap();
        if last_log.term > self.term() {
            // Hack: In normal flow, when leader sends the entries, it will use a term
            // that's not less than the last log term. And follower will update its states
            // correctly. For merge, we append the log without raft, so we have to take care
            // of term explicitly to get correct metadata.
            info!(
                "become follower for new logs";
                "first_log_term" => first.term,
                "first_log_index" => first.index,
                "new_log_term" => last_log.term,
                "new_log_index" => last_log.index,
                "term" => self.term(),
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            self.raft_group
                .raft
                .become_follower(last_log.term, INVALID_ID);
        }

        self.raft_group
            .raft
            .raft_log
            .maybe_append(log_idx, log_term, merge.get_commit(), entries)
            .map(|(_, last_index)| last_index)
    }

    /// Tries to destroy itself. Returns a job (if needed) to do more cleaning
    /// tasks.
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

        if let Some(snap_ctx) = self.apply_snap_ctx.as_mut() {
            if !snap_ctx.scheduled {
                info!(
                    "stale peer is persisting snapshot, will destroy next time";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                snap_ctx.destroy_peer_after_apply = true;
                return None;
            }
        }

        if self.get_store().is_applying_snapshot() && !self.mut_store().cancel_applying_snap() {
            info!(
                "stale peer is applying snapshot, will destroy next time";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            if let Some(snap_ctx) = self.apply_snap_ctx.as_mut() {
                snap_ctx.destroy_peer_after_apply = true;
            }
            return None;
        }

        // There is no applying snapshot or snapshot is canceled so the `apply_snap_ctx`
        // should be set to None.
        // - If the snapshot is canceled, the `apply_snap_ctx` should be None. Remember
        //   the snapshot should not be canceled and the context should be None only
        //   after applying snapshot in normal case. But here is safe because this peer
        //   is about to destroy and `pending_remove` will be true, namely no more ready
        //   will be fetched.
        // - If there is no applying snapshot, the `apply_snap_ctx` should also be None.
        //   It's possible that the snapshot was canceled successfully before but
        //   `cancel_applying_snap` returns false. If so, at this time, `apply_snap_ctx`
        //   is Some and should be set to None.
        self.apply_snap_ctx = None;

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
    pub fn destroy(
        &mut self,
        engines: &Engines<EK, ER>,
        perf_context: &mut ER::PerfContext,
        keep_data: bool,
        pending_create_peers: &Mutex<HashMap<u64, (u64, bool)>>,
    ) -> Result<()> {
        fail_point!("raft_store_skip_destroy_peer", |_| Ok(()));
        let t = TiInstant::now();

        let mut region = self.region().clone();
        info!(
            "begin to destroy";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        let (pending_create_peers, clean) = if self.local_first_replicate {
            let mut pending = pending_create_peers.lock().unwrap();
            if self.get_store().is_initialized() {
                assert_eq!(pending.get(&region.get_id()), None);
                (None, true)
            } else if let Some(status) = pending.get(&region.get_id()) {
                if *status == (self.peer.get_id(), false) {
                    pending.remove(&region.get_id());
                    // Hold the lock to avoid apply worker applies split.
                    (Some(pending), true)
                } else if *status == (self.peer.get_id(), true) {
                    // It's already marked to split by apply worker, skip delete.
                    (None, false)
                } else {
                    // Peer id can't be different as router should exist all the time, their is no
                    // chance for store to insert a different peer id. And apply worker should skip
                    // split when meeting a different id.
                    let status = *status;
                    // Avoid panic with lock.
                    drop(pending);
                    panic!("{} unexpected pending states {:?}", self.tag, status);
                }
            } else {
                // The status is inserted when it's created. It will be removed in following
                // cases:
                // - By apply worker as it fails to split due to region state key. This is
                //   impossible to reach this code path because the delete write batch is not
                //   persisted yet.
                // - By store fsm as it fails to create peer, which is also invalid obviously.
                // - By peer fsm after persisting snapshot, then it should be initialized.
                // - By peer fsm after split.
                // - By peer fsm when destroy, which should go the above branch instead.
                (None, false)
            }
        } else {
            (None, true)
        };
        if clean {
            // Set Tombstone state explicitly
            let mut kv_wb = engines.kv.write_batch();
            let mut raft_wb = engines.raft.log_batch(1024);
            // Raft log gc should be flushed before being destroyed, so last_compacted_idx
            // has to be the minimal index that may still have logs.
            let last_compacted_idx = self.last_compacted_idx;
            self.mut_store()
                .clear_meta(last_compacted_idx, &mut kv_wb, &mut raft_wb)?;

            // StoreFsmDelegate::check_msg use both epoch and region peer list to check
            // whether a message is targeting a staled peer. But for an uninitialized peer,
            // both epoch and peer list are empty, so a removed peer will be created again.
            // Saving current peer into the peer list of region will fix this problem.
            if !self.get_store().is_initialized() {
                region.mut_peers().push(self.peer.clone());
            }

            write_peer_state(
                &mut kv_wb,
                &region,
                PeerState::Tombstone,
                // Only persist the `merge_state` if the merge is known to be succeeded
                // which is determined by the `keep_data` flag
                if keep_data {
                    self.pending_merge_state.clone()
                } else {
                    None
                },
            )?;

            // write kv rocksdb first in case of restart happen between two write
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            kv_wb.write_opt(&write_opts)?;

            drop(pending_create_peers);

            perf_context.start_observe();
            engines.raft.consume(&mut raft_wb, true)?;
            perf_context.report_metrics(&[]);

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
        }

        self.pending_reads.clear_all(Some(region.get_id()));

        for Proposal { cb, .. } in self.proposals.queue.drain(..) {
            apply::notify_req_region_removed(region.get_id(), cb);
        }

        info!(
            "peer destroy itself";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "takes" => ?t.saturating_elapsed(),
            "clean" => clean,
            "keep_data" => keep_data,
        );

        fail_point!("raft_store_after_destroy_peer");

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

    #[inline]
    pub fn region_buckets_info_mut(&mut self) -> &mut BucketStatsInfo {
        &mut self.region_buckets_info
    }

    #[inline]
    pub fn region_buckets_info(&self) -> &BucketStatsInfo {
        &self.region_buckets_info
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
            // followers or learners may not sync its logs for a long time and become
            // unavailable. We choose availability instead of performance in this case.
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
        if self.force_leader.is_some() {
            res.reason = "force leader";
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
        if !self.disk_full_peers.is_empty() {
            res.reason = "has disk full peers";
            return res;
        }
        if !self.wait_data_peers.is_empty() {
            res.reason = "has wait data peers";
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
                // Keep ticking if it's waiting for snapshot.
                && !self.wait_data
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
        reason: RegionChangeReason,
    ) {
        if self.region().get_region_epoch().get_version() < region.get_region_epoch().get_version()
        {
            // Epoch version changed, disable read on the local reader for this region.
            self.leader_lease.expire_remote_lease();
        }
        self.mut_store().set_region(region.clone());
        let progress = ReadProgress::region(region);
        // Always update read delegate's region to avoid stale region info after a
        // follower becoming a leader.
        self.maybe_update_read_progress(reader, progress);

        // Update leader info
        self.read_progress
            .update_leader_info(self.leader_id(), self.term(), self.region());

        {
            let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
            pessimistic_locks.term = self.term();
            pessimistic_locks.version = self.region().get_region_epoch().get_version();
        }

        if !self.pending_remove {
            host.on_region_changed(
                self.region(),
                RegionChangeEvent::Update(reason),
                self.get_role(),
            );
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
    pub fn is_follower(&self) -> bool {
        self.raft_group.raft.state == StateRole::Follower && self.peer.role != PeerRole::Learner
    }

    #[inline]
    pub fn is_witness(&self) -> bool {
        self.peer.is_witness
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
    pub fn approximate_size(&self) -> Option<u64> {
        self.split_check_trigger.approximate_size
    }

    #[inline]
    pub fn approximate_keys(&self) -> Option<u64> {
        self.split_check_trigger.approximate_keys
    }

    #[inline]
    pub fn set_approximate_size(&mut self, approximate_size: Option<u64>) {
        self.split_check_trigger.approximate_size = approximate_size;
    }

    #[inline]
    pub fn set_approximate_keys(&mut self, approximate_keys: Option<u64>) {
        self.split_check_trigger.approximate_keys = approximate_keys;
    }

    /// Whether the snapshot is handling.
    /// See the comments of `check_snap_status` for more details.
    #[inline]
    pub fn is_handling_snapshot(&self) -> bool {
        self.apply_snap_ctx.is_some() || self.get_store().is_applying_snapshot()
    }

    #[inline]
    pub fn should_destroy_after_apply_snapshot(&self) -> bool {
        self.apply_snap_ctx
            .as_ref()
            .map_or(false, |ctx| ctx.destroy_peer_after_apply)
    }

    /// Returns `true` if the raft group has replicated a snapshot but not
    /// committed it yet.
    #[inline]
    pub fn has_pending_snapshot(&self) -> bool {
        self.get_pending_snapshot().is_some()
    }

    #[inline]
    pub fn get_pending_snapshot(&self) -> Option<&eraftpb::Snapshot> {
        self.raft_group.snap()
    }

    fn add_ready_metric(&self, ready: &Ready, metrics: &mut RaftMetrics) {
        metrics.ready.message.inc_by(ready.messages().len() as u64);
        metrics
            .ready
            .commit
            .inc_by(ready.committed_entries().len() as u64);
        metrics.ready.append.inc_by(ready.entries().len() as u64);

        if !ready.snapshot().is_empty() {
            metrics.ready.snapshot.inc();
        }
    }

    fn add_light_ready_metric(&self, light_ready: &LightReady, metrics: &mut RaftMetrics) {
        metrics
            .ready
            .message
            .inc_by(light_ready.messages().len() as u64);
        metrics
            .ready
            .commit
            .inc_by(light_ready.committed_entries().len() as u64);
    }

    #[inline]
    pub fn in_joint_state(&self) -> bool {
        self.region().get_peers().iter().any(|p| {
            p.get_role() == PeerRole::IncomingVoter || p.get_role() == PeerRole::DemotingVoter
        })
    }

    #[inline]
    pub fn send_raft_messages<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        msgs: Vec<RaftMessage>,
    ) {
        let mut now = None;
        let std_now = Instant::now();
        for msg in msgs {
            let msg_type = msg.get_message().get_msg_type();
            if msg_type == MessageType::MsgSnapshot {
                let snap_index = msg.get_message().get_snapshot().get_metadata().get_index();
                if snap_index > self.last_sent_snapshot_idx {
                    self.last_sent_snapshot_idx = snap_index;
                }
            }
            if msg_type == MessageType::MsgTimeoutNow && self.is_leader() {
                // After a leader transfer procedure is triggered, the lease for
                // the old leader may be expired earlier than usual, since a new leader
                // may be elected and the old leader doesn't step down due to
                // network partition from the new leader.
                // For lease safety during leader transfer, transit `leader_lease`
                // to suspect.
                self.leader_lease.suspect(*now.insert(monotonic_raw_now()));
            }

            let to_peer_id = msg.get_to_peer().get_id();
            let to_store_id = msg.get_to_peer().get_store_id();

            debug!(
                "send raft msg";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "msg_type" => %util::MsgType(&msg),
                "msg_size" => msg.get_message().compute_size(),
                "to" => to_peer_id,
                "disk_usage" => ?msg.get_disk_usage(),
            );

            for (term, index) in msg
                .get_message()
                .get_entries()
                .iter()
                .map(|e| (e.get_term(), e.get_index()))
            {
                if let Ok(idx) = self
                    .proposals
                    .queue
                    .binary_search_by_key(&index, |p: &Proposal<_>| p.index)
                {
                    let proposal = &self.proposals.queue[idx];
                    if term == proposal.term {
                        for tracker in proposal.cb.write_trackers() {
                            tracker.observe(std_now, &ctx.raft_metrics.wf_send_proposal, |t| {
                                &mut t.metrics.wf_send_proposal_nanos
                            });
                        }
                    }
                }
            }

            if let Err(e) = ctx.trans.send(msg) {
                // We use metrics to observe failure on production.
                debug!(
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
                ctx.raft_metrics.send_message.add(msg_type, false);
            } else {
                ctx.raft_metrics.send_message.add(msg_type, true);
            }
        }
    }

    #[inline]
    pub fn build_raft_messages<T>(
        &mut self,
        ctx: &PollContext<EK, ER, T>,
        msgs: Vec<eraftpb::Message>,
    ) -> Vec<RaftMessage> {
        let mut raft_msgs = Vec::with_capacity(msgs.len());
        for msg in msgs {
            if let Some(m) = self.build_raft_message(msg, ctx.self_disk_usage) {
                raft_msgs.push(m);
            }
        }
        raft_msgs
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
            fail_point!("on_step_read_index_msg");
            ctx.coprocessor_host
                .on_step_read_index(&mut m, self.get_role());
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
        } else if util::is_vote_msg(&m) {
            // Only by passing an election timeout can peers handle request vote safely.
            // See https://github.com/tikv/tikv/issues/15035
            if let Some(remain) = ctx.maybe_in_unsafe_vote_period() {
                debug!("drop request vote for one election timeout after node start";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "from_peer_id" => m.get_from(),
                    "remain_duration" => ?remain,
                );
                ctx.raft_metrics.message_dropped.unsafe_vote.inc();
                return Ok(());
            }
        }

        let from_id = m.get_from();
        let has_snap_task = self.get_store().has_gen_snap_task();
        let pre_commit_index = self.raft_group.raft.raft_log.committed;
        self.raft_group.step(m)?;
        self.report_commit_log_duration(pre_commit_index, &mut ctx.raft_metrics);

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
            self.get_store().set_gen_snap_task_for_balance();
        }
        Ok(())
    }

    fn report_persist_log_duration(&self, pre_persist_index: u64, metrics: &mut RaftMetrics) {
        if !metrics.waterfall_metrics || self.proposals.is_empty() {
            return;
        }
        let now = Instant::now();
        for index in pre_persist_index + 1..=self.raft_group.raft.raft_log.persisted {
            if let Some((term, trackers)) = self.proposals.find_trackers(index) {
                if self
                    .get_store()
                    .term(index)
                    .map(|t| t == term)
                    .unwrap_or(false)
                {
                    for tracker in trackers {
                        tracker.observe(now, &metrics.wf_persist_log, |t| {
                            &mut t.metrics.wf_persist_log_nanos
                        });
                    }
                }
            }
        }
    }

    fn report_commit_log_duration(&self, pre_commit_index: u64, metrics: &mut RaftMetrics) {
        if !metrics.waterfall_metrics || self.proposals.is_empty() {
            return;
        }
        let now = Instant::now();
        for index in pre_commit_index + 1..=self.raft_group.raft.raft_log.committed {
            if let Some((term, trackers)) = self.proposals.find_trackers(index) {
                if self
                    .get_store()
                    .term(index)
                    .map(|t| t == term)
                    .unwrap_or(false)
                {
                    let commit_persisted = index <= self.raft_group.raft.raft_log.persisted;
                    let hist = if commit_persisted {
                        &metrics.wf_commit_log
                    } else {
                        &metrics.wf_commit_not_persist_log
                    };
                    for tracker in trackers {
                        // Collect the metrics related to commit_log
                        // durations.
                        let duration = tracker.observe(now, hist, |t| {
                            t.metrics.commit_not_persisted = !commit_persisted;
                            &mut t.metrics.wf_commit_log_nanos
                        });
                        // Normally, commit_log_duration both contains the duraiton on persisting
                        // raft logs and transferring raft logs to other nodes. Therefore, it can
                        // reflects slowness of the node on I/Os, whatever the reason is.
                        // Here, health_stats uses the recorded commit_log_duration as the
                        // latency to perspect whether there exists jitters on network. It's not
                        // accurate, but it's proved that it's a good approximation.
                        metrics
                            .health_stats
                            .observe(Duration::from_nanos(duration), IoType::Network);
                    }
                }
            }
        }
    }

    /// Checks and updates `peer_heartbeats` for the peer.
    pub fn check_peers(&mut self) {
        if !self.is_leader() {
            self.peer_heartbeats.clear();
            self.peers_start_pending_time.clear();
            self.wait_data_peers.clear();
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
    pub fn collect_down_peers<T>(&mut self, ctx: &PollContext<EK, ER, T>) -> Vec<PeerStats> {
        let max_duration = ctx.cfg.max_peer_down_duration.0;
        let mut down_peers = Vec::new();
        let mut down_peer_ids = Vec::new();
        for p in self.region().get_peers() {
            if p.get_id() == self.peer.get_id() {
                continue;
            }
            if let Some(instant) = self.peer_heartbeats.get(&p.get_id()) {
                let elapsed = instant.saturating_elapsed();
                if elapsed >= max_duration {
                    let mut stats = PeerStats::default();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(elapsed.as_secs());
                    down_peers.push(stats);
                    down_peer_ids.push(p.get_id());
                }
            }
        }
        self.down_peer_ids = down_peer_ids;
        if !self.down_peer_ids.is_empty() {
            self.refill_disk_full_peers(ctx);
        }
        down_peers
    }

    /// Collects all pending peers and update `peers_start_pending_time`.
    pub fn collect_pending_peers<T>(&mut self, ctx: &PollContext<EK, ER, T>) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group.status();
        let truncated_idx = self.get_store().truncated_index();

        for peer_id in &self.wait_data_peers {
            if let Some(p) = self.get_peer_from_cache(*peer_id) {
                pending_peers.push(p);
            }
        }

        if status.progress.is_none() {
            return pending_peers;
        }

        for i in 0..self.peers_start_pending_time.len() {
            let (_, pending_after) = self.peers_start_pending_time[i];
            let elapsed = duration_to_sec(pending_after.saturating_elapsed());
            RAFT_PEER_PENDING_DURATION.observe(elapsed);
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
            // The correctness of region merge depends on the fact that all target peers
            // must exist during merging. (PD rely on `pending_peers` to check whether all
            // target peers exist)
            //
            // So if the `matched` is 0, it must be a pending peer.
            // It can be ensured because `truncated_index` must be greater than
            // `RAFT_INIT_LOG_INDEX`(5).
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
            if self.wait_data_peers.contains(&peer_id) {
                continue;
            }
            let truncated_idx = self.raft_group.store().truncated_index();
            if let Some(progress) = self.raft_group.raft.prs().get(peer_id) {
                if progress.matched >= truncated_idx {
                    let (_, pending_after) = self.peers_start_pending_time.swap_remove(i);
                    let elapsed = duration_to_sec(pending_after.saturating_elapsed());
                    RAFT_PEER_PENDING_DURATION.observe(elapsed);
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

    pub fn maybe_force_forward_commit_index(&mut self) -> bool {
        let failed_stores = match &self.force_leader {
            Some(ForceLeaderState::ForceLeader { failed_stores, .. }) => failed_stores,
            _ => unreachable!(),
        };

        let region = self.region();
        let mut replicated_idx = self.raft_group.raft.raft_log.persisted;
        for (peer_id, p) in self.raft_group.raft.prs().iter() {
            let store_id = region
                .get_peers()
                .iter()
                .find(|p| p.get_id() == *peer_id)
                .unwrap()
                .get_store_id();
            if failed_stores.contains(&store_id) {
                continue;
            }
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
        }

        if self.raft_group.store().term(replicated_idx).unwrap_or(0) < self.term() {
            // do not commit logs of previous term directly
            return false;
        }

        self.raft_group.raft.raft_log.committed =
            std::cmp::max(self.raft_group.raft.raft_log.committed, replicated_idx);
        true
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
        // Updates the `leader_missing_time` according to the current state.
        //
        // If we are checking this it means we suspect the leader might be missing.
        // Mark down the time when we are called, so we can check later if it's been
        // longer than it should be.
        match self.leader_missing_time {
            None => {
                self.leader_missing_time = Instant::now().into();
                StaleState::Valid
            }
            Some(instant)
                if instant.saturating_elapsed() >= ctx.cfg.max_leader_missing_duration.0 =>
            {
                // Resets the `leader_missing_time` to avoid sending the same tasks to
                // PD worker continuously during the leader missing timeout.
                self.leader_missing_time = Instant::now().into();
                StaleState::ToValidate
            }
            Some(instant)
                if instant.saturating_elapsed() >= ctx.cfg.abnormal_leader_missing_duration.0 =>
            {
                // A peer is considered as in the leader missing state
                // if it's initialized but is isolated from its leader or
                // something bad happens that the raft group can not elect a leader.
                if self.is_initialized() && self.raft_group.raft.promotable() {
                    StaleState::LeaderMissing
                } else {
                    // Uninitialized peer and learner may not have leader info,
                    // even if there is a valid leader.
                    StaleState::MaybeLeaderMissing
                }
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
                    self.last_sent_snapshot_idx = self.raft_group.raft.raft_log.last_index();

                    // A more recent read may happen on the old leader. So max ts should
                    // be updated after a peer becomes leader.
                    self.require_updating_max_ts(&ctx.pd_scheduler);
                    // Init the in-memory pessimistic lock table when the peer becomes leader.
                    self.activate_in_memory_pessimistic_locks();
                    // Exit entry cache warmup state when the peer becomes leader.
                    self.mut_store().clear_entry_cache_warmup_state();

                    if !ctx.store_disk_usages.is_empty() {
                        self.refill_disk_full_peers(ctx);
                        debug!(
                            "become leader refills disk full peers to {:?}",
                            self.disk_full_peers;
                            "region_id" => self.region_id,
                        );
                    }
                    // After the leadership changed, send `CasualMessage::Campaign
                    // {notify_by_parent: true}` to the target peer to campaign
                    // leader if there exists uncampaigned regions. It's used to
                    // ensure that a leader is elected promptly for the newly
                    // created Raft group, minimizing availability impact (e.g.
                    // #12410 and #17602.).
                    if let Some(new_regions) = &self.uncampaigned_new_regions {
                        for new_region in &new_regions.0 {
                            let _ = ctx.router.send(
                                *new_region,
                                PeerMsg::CasualMessage(Box::new(CasualMessage::Campaign {
                                    notify_by_parent: true,
                                })),
                            );
                        }
                    }
                    // Clear the uncampaigned list.
                    self.uncampaigned_new_regions = None;
                }
                StateRole::Follower => {
                    self.leader_lease.expire();
                    self.mut_store().cancel_generating_snap(None);
                    self.clear_disk_full_peers(ctx);
                    self.clear_in_memory_pessimistic_locks();
                    if self.peer.is_witness && self.delay_clean_data {
                        let _ = self.get_store().clear_data();
                        self.delay_clean_data = false;
                    }
                    // Clear the uncampaigned list.
                    self.uncampaigned_new_regions = None;
                }
                _ => {}
            }
            self.on_leader_changed(ss.leader_id, self.term());
            ctx.coprocessor_host.on_role_change(
                self.region(),
                RoleChange {
                    state: ss.raft_state,
                    leader_id: ss.leader_id,
                    prev_lead_transferee: self.lead_transferee,
                    vote: self.raft_group.raft.vote,
                    initialized: self.is_initialized(),
                    peer_id: self.peer.get_id(),
                },
            );
            self.cmd_epoch_checker.maybe_update_term(self.term());
        } else if let Some(hs) = ready.hs() {
            if hs.get_term() != self.get_store().hard_state().get_term() {
                self.on_leader_changed(self.leader_id(), hs.get_term());
            }
        }
        self.lead_transferee = self.raft_group.raft.lead_transferee.unwrap_or_default();
    }

    /// Correctness depends on the order between calling this function and
    /// notifying other peers the new commit index.
    /// It is due to the interaction between lease and split/merge.(details are
    /// described below)
    ///
    /// Note that in addition to the heartbeat/append msg, the read index
    /// response also can notify other peers the new commit index. There are
    /// three place where TiKV handles read index request. The first place is in
    /// raft-rs, so it's like heartbeat/append msg, call this function and then
    /// send the response. The second place is in `Step`, we should use the
    /// commit index of `PeerStorage` which is the greatest commit index that
    /// can be observed outside. The third place is in `read_index`, handle it
    /// like the second one.
    fn on_leader_commit_idx_changed(&mut self, pre_commit_index: u64, commit_index: u64) {
        if commit_index <= pre_commit_index || !self.is_leader() {
            return;
        }

        // The admin cmds in `CmdEpochChecker` are proposed by the current leader so we
        // can use it to get the split/prepare-merge cmds which was committed just now.

        // BatchSplit and Split cmd are mutually exclusive because they both change
        // epoch's version so only one of them can be proposed and the other one will be
        // rejected by `CmdEpochChecker`.
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
                    // Stop updating `safe_ts`
                    self.read_progress.discard();
                }
            }
        }
    }

    fn on_leader_changed(&mut self, leader_id: u64, term: u64) {
        debug!(
            "update leader info";
            "region_id" => self.region_id,
            "leader_id" => leader_id,
            "term" => term,
            "peer_id" => self.peer_id(),
        );

        // TODO: Set last_index as the min_index may not be correct on follower,
        // need to further consider a better solution.
        self.disable_apply_unpersisted_log(self.raft_group.raft.raft_log.last_index());

        self.read_progress
            .update_leader_info(leader_id, term, self.region());
    }

    #[inline]
    pub fn ready_to_handle_pending_snap(&self) -> bool {
        // If apply worker is still working, written apply state may be overwritten
        // by apply worker. So we have to wait here.
        // Please note that commit_index can't be used here. When applying a snapshot,
        // a stale heartbeat can make the leader think follower has already applied
        // the snapshot, and send remaining log entries, which may increase
        // commit_index.
        //
        // If it's witness before, but a command changes it to non-witness, it will stop
        // applying all following command, therefore, add the judgment of `wait_data` to
        // avoid applying snapshot is also blocked.
        // TODO: add more test
        (self.last_applying_idx == self.get_store().applied_index() || self.wait_data)
            // Requesting snapshots also triggers apply workers to write
            // apply states even if there is no pending committed entry.
            // TODO: Instead of sharing the counter, we should apply snapshots
            //       in apply workers.
            && self.pending_request_snapshot_count.load(Ordering::SeqCst) == 0
    }

    #[inline]
    fn ready_to_handle_read(&self) -> bool {
        // TODO: It may cause read index to wait a long time.

        // There may be some values that are not applied by this leader yet but the old
        // leader, if applied_term isn't equal to current term.
        self.get_store().applied_term() == self.term()
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
        // problem if the leader applies fewer values than the follower, the follower
        // read could get a newer value, and after that, the leader may read a
        // stale value, which violates linearizability.
        self.get_store().applied_index() >= read_index
            // If it is in pending merge state(i.e. applied PrepareMerge), the data may be stale.
            // TODO: Add a test to cover this case
            && self.pending_merge_state.is_none()
            // a peer which is applying snapshot will clean up its data and ingest a snapshot file,
            // during between the two operations a replica read could read empty data.
            && !self.is_handling_snapshot()
    }

    #[inline]
    pub fn is_splitting(&self) -> bool {
        self.last_committed_split_idx > self.get_store().applied_index()
    }

    #[inline]
    pub fn is_merging(&self) -> bool {
        self.last_committed_prepare_merge_idx > self.get_store().applied_index()
            || self.pending_merge_state.is_some()
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

    pub fn schedule_raftlog_gc<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        to: u64,
    ) -> bool {
        let task = RaftlogGcTask::gc(self.region_id, self.last_compacted_idx, to);
        debug!(
            "scheduling raft log gc task";
            "region_id" => self.region_id,
            "peer_id" => self.peer_id(),
            "task" => %task,
        );
        if let Err(e) = ctx.raftlog_gc_scheduler.schedule(task) {
            error!(
                "failed to schedule raft log gc task";
                "region_id" => self.region_id,
                "peer_id" => self.peer_id(),
                "err" => %e,
            );
            false
        } else {
            true
        }
    }

    /// Check the current snapshot status.
    /// Returns whether it's valid to handle raft ready.
    ///
    /// The snapshot process order would be:
    /// - Get the snapshot from the ready
    /// - Wait for the notify of persisting this ready through
    ///   `Peer::on_persist_ready`
    /// - Schedule the snapshot task to region worker through
    ///   `schedule_applying_snapshot`
    /// - Wait for applying snapshot to complete(`check_snap_status`)
    /// Then it's valid to handle the next ready.
    fn check_snap_status<T: Transport>(&mut self, ctx: &mut PollContext<EK, ER, T>) -> bool {
        if let Some(snap_ctx) = self.apply_snap_ctx.as_ref() {
            if !snap_ctx.scheduled {
                // There is a snapshot from ready but it is not scheduled because the ready has
                // not been persisted yet. We should wait for the notification of persisting
                // ready and do not get a new ready.
                return false;
            }
        }

        match self.mut_store().check_applying_snap() {
            CheckApplyingSnapStatus::Applying => {
                // If this peer is applying snapshot, we should not get a new ready.
                // There are two reasons in my opinion:
                //   1. If we handle a new ready and persist the data(e.g. entries), we can not
                //      tell raft-rs that this ready has been persisted because the ready need
                //      to be persisted one by one from raft-rs's view.
                //   2. When this peer is applying snapshot, the response msg should not be sent
                //      to leader, thus the leader will not send new entries to this peer.
                //      Although it's possible a new leader may send a AppendEntries msg to this
                //      peer, this possibility is very low. In most cases, there is no msg need
                //      to be handled.
                // So we choose to not get a new ready which makes the logic more clear.
                debug!(
                    "still applying snapshot, skip further handling";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                return false;
            }
            CheckApplyingSnapStatus::Success => {
                fail_point!("raft_before_applying_snap_finished");

                if let Some(snap_ctx) = self.apply_snap_ctx.take() {
                    // This snapshot must be scheduled
                    if !snap_ctx.scheduled {
                        panic!(
                            "{} snapshot was not scheduled before, apply_snap_ctx {:?}",
                            self.tag, snap_ctx
                        );
                    }

                    fail_point!("raft_before_follower_send");
                    let msgs = self.build_raft_messages(ctx, snap_ctx.msgs);
                    self.send_raft_messages(ctx, msgs);

                    // Snapshot has been applied.
                    self.last_applying_idx = self.get_store().truncated_index();
                    self.last_compacted_idx = self.last_applying_idx + 1;
                    self.raft_group.advance_apply_to(self.last_applying_idx);
                    self.cmd_epoch_checker.advance_apply(
                        self.last_applying_idx,
                        self.term(),
                        self.raft_group.store().region(),
                    );

                    if self.unsafe_recovery_state.is_some() {
                        debug!("unsafe recovery finishes applying a snapshot");
                        self.unsafe_recovery_maybe_finish_wait_apply(/* force= */ false);
                    }
                    if self.snapshot_recovery_state.is_some() {
                        debug!("snapshot recovery finishes applying a snapshot");
                        self.snapshot_recovery_maybe_finish_wait_apply(false);
                    }
                }
                // If `apply_snap_ctx` is none, it means this snapshot does not
                // come from the ready but comes from the unfinished snapshot task
                // after restarting.

                // Note that this function must be called after applied index is updated,
                // i.e. call `RawNode::advance_apply_to`.
                self.post_pending_read_index_on_replica(ctx);
                // Resume `read_progress`
                self.update_read_progress(ctx, ReadProgress::WaitData(false));
                self.read_progress.resume();
                // Update apply index to `last_applying_idx`
                self.read_progress
                    .update_applied(self.last_applying_idx, &ctx.coprocessor_host);
                if self.wait_data {
                    self.notify_leader_the_peer_is_available(ctx);
                    ctx.apply_router
                        .schedule_task(self.region_id, ApplyTask::Recover(self.region_id));
                    self.wait_data = false;
                    self.should_reject_msgappend = false;
                    return false;
                }
            }
            CheckApplyingSnapStatus::Idle => {
                // FIXME: It's possible that the snapshot applying task is canceled.
                // Although it only happens when shutting down the store or destroying
                // the peer, it's still dangerous if continue to handle ready for the
                // peer. So it's better to revoke `JOB_STATUS_CANCELLING` to ensure all
                // started tasks can get finished correctly.
                if self.apply_snap_ctx.is_some() {
                    return false;
                }
            }
        }
        assert_eq!(self.apply_snap_ctx, None);
        true
    }

    fn notify_leader_the_peer_is_available<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
    ) {
        fail_point!("ignore notify leader the peer is available", |_| {});
        let leader_id = self.leader_id();
        let leader = self.get_peer_from_cache(leader_id);
        if let Some(leader) = leader {
            let mut msg = ExtraMessage::default();
            msg.set_type(ExtraMessageType::MsgAvailabilityResponse);
            msg.wait_data = false;
            self.send_extra_message(msg, &mut ctx.trans, &leader);
            info!(
                "notify leader the peer is available";
                "region_id" => self.region().get_id(),
                "peer_id" => self.peer.id
            );
        }
    }

    /// Cancel the snapshot generation task under the following conditions:
    ///
    /// 1. The requesting peer is removed by a configuration change.
    /// 2. The requesting peer becomes unreachable, e.g., TiKV goes down or a
    ///    network partition occurs.
    pub fn maybe_cancel_gen_snap_task(&self, unreachable_store_id: Option<u64>) {
        let to_peer = match self.get_store().get_gen_snap_task().as_ref() {
            Some(task) => task.to_peer.clone(),
            None => return,
        };
        let cancel_by_unreachable_store =
            unreachable_store_id.map_or(false, |s| to_peer.get_store_id() == s);
        let cancel_by_peer_not_found = find_peer_by_id(self.region(), to_peer.get_id()).is_none();
        if cancel_by_unreachable_store || cancel_by_peer_not_found {
            self.get_store().cancel_generating_snap(None);
            warn!(
                "cancel generate snap task";
                "region_id" => self.region_id,
                "peer_id" => self.peer_id(),
                "to_peer" => ?to_peer,
                "cancel_by_peer_not_found" => cancel_by_peer_not_found,
                "cancel_by_unreachable_store" => cancel_by_unreachable_store,
            );
        }
    }

    pub fn handle_gen_snap_task<T: Transport>(&mut self, ctx: &mut PollContext<EK, ER, T>) {
        // Check if gen snap task need to be cancelled, otherwise it will block
        // snapshot and subsequent conf changes.
        self.maybe_cancel_gen_snap_task(None);

        if self.get_store().has_gen_snap_task() {
            // If the snapshot gen precheck feature is enabled, the leader needs
            // to complete a precheck with the target follower before the
            // snapshot generation.
            if ctx.feature_gate.can_enable(SNAP_GEN_PRECHECK_FEATURE) {
                // Continuously send snap gen precheck requests to the follower
                // until an approval is received.
                if let Some(to_peer) = self.get_store().need_gen_snap_precheck() {
                    self.send_snap_gen_precheck_request(ctx, &to_peer);
                }
            } else {
                let gen_task = self.mut_store().take_gen_snap_task().unwrap();
                self.pending_request_snapshot_count
                    .fetch_add(1, Ordering::SeqCst);
                ctx.apply_router
                    .schedule_task(self.region_id, ApplyTask::Snapshot(gen_task));
            }
        }
    }

    pub fn handle_raft_ready_append<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
    ) -> Option<ReadyResult> {
        if self.pending_remove {
            return None;
        }

        if !self.check_snap_status(ctx) {
            return None;
        }

        let mut destroy_regions = vec![];
        if self.has_pending_snapshot() {
            if !self.ready_to_handle_pending_snap() {
                let count = self.pending_request_snapshot_count.load(Ordering::SeqCst);
                debug!(
                    "not ready to apply snapshot";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "applied_index" => self.get_store().applied_index(),
                    "last_applying_index" => self.last_applying_idx,
                    "pending_request_snapshot_count" => count,
                );
                return None;
            }

            if !self.unpersisted_readies.is_empty() {
                debug!(
                    "not ready to apply snapshot because there are some unpersisted readies";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "unpersisted_readies" => ?self.unpersisted_readies,
                );
                return None;
            }

            let meta = ctx.store_meta.lock().unwrap();
            // For merge process, the stale source peer is destroyed asynchronously when
            // applying snapshot or creating new peer. So here checks whether there is any
            // overlap, if so, wait and do not handle raft ready.
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
            fail_point!("before_no_ready_gen_snap_task", |_| None);
            self.handle_gen_snap_task(ctx);
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

        fail_point!("panic_if_handle_ready_3", self.peer.get_id() == 3, |_| {
            panic!("{} wants to handle ready", self.tag);
        });

        debug!(
            "handle raft ready";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        let mut ready = self.raft_group.ready();

        self.add_ready_metric(&ready, &mut ctx.raft_metrics);

        // Update it after unstable entries pagination is introduced.
        debug_assert!(ready.entries().last().map_or_else(
            || true,
            |entry| entry.index == self.raft_group.raft.raft_log.last_index(),
        ));
        if self.memtrace_raft_entries != 0 {
            MEMTRACE_RAFT_ENTRIES.trace(TraceEvent::Sub(self.memtrace_raft_entries));
            self.memtrace_raft_entries = 0;
        }

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

        self.on_role_changed(ctx, &ready);

        if let Some(hs) = ready.hs() {
            let pre_commit_index = self.get_store().commit_index();
            let cur_commit_index = hs.get_commit();
            assert!(cur_commit_index >= pre_commit_index);
            if self.is_leader() {
                self.on_leader_commit_idx_changed(pre_commit_index, cur_commit_index);
            }
        }

        if !ready.messages().is_empty() {
            assert!(self.is_leader());
            let raft_msgs = self.build_raft_messages(ctx, ready.take_messages());
            self.send_raft_messages(ctx, raft_msgs);
        }

        self.apply_reads(ctx, &ready);

        if !ready.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, ready.take_committed_entries());
        }
        // Check whether there is a pending generate snapshot task, the task
        // needs to be sent to the apply system.
        // Always sending snapshot task behind apply task, so it gets latest
        // snapshot.
        self.handle_gen_snap_task(ctx);

        let state_role = ready.ss().map(|ss| ss.raft_state);
        let has_new_entries = !ready.entries().is_empty();
        let mut trackers = vec![];
        if ctx.raft_metrics.waterfall_metrics {
            let now = Instant::now();
            for entry in ready.entries() {
                if let Some((term, times)) = self.proposals.find_trackers(entry.get_index()) {
                    if entry.term == term {
                        for tracker in times {
                            trackers.push(*tracker);
                            tracker.observe(now, &ctx.raft_metrics.wf_send_to_queue, |t| {
                                &mut t.metrics.wf_send_to_queue_nanos
                            });
                        }
                    }
                }
            }
        }
        let (res, mut task) = match self
            .mut_store()
            .handle_raft_ready(&mut ready, destroy_regions)
        {
            Ok(r) => r,
            Err(e) => {
                // We may have written something to writebatch and it can't be reverted, so has
                // to panic here.
                panic!("{} failed to handle raft ready: {:?}", self.tag, e)
            }
        };

        let ready_number = ready.number();
        let persisted_msgs = ready.take_persisted_messages();
        let mut has_write_ready = false;
        match &res {
            HandleReadyResult::SendIoTask | HandleReadyResult::Snapshot { .. } => {
                if !persisted_msgs.is_empty() {
                    task.messages = self.build_raft_messages(ctx, persisted_msgs);
                }

                if !trackers.is_empty() {
                    task.trackers = trackers;
                }

                if let Some(write_worker) = &mut ctx.sync_write_worker {
                    write_worker.handle_write_task(task);

                    assert_eq!(self.unpersisted_ready, None);
                    self.unpersisted_ready = Some(ready);
                    has_write_ready = true;
                } else {
                    self.write_router.send_write_msg(
                        ctx,
                        self.unpersisted_readies.back().map(|r| r.number),
                        WriteMsg::WriteTask(task),
                    );

                    self.unpersisted_readies.push_back(UnpersistedReady {
                        number: ready_number,
                        max_empty_number: ready_number,
                        raft_msgs: vec![],
                    });

                    self.raft_group.advance_append_async(ready);
                }
            }
            HandleReadyResult::NoIoTask => {
                if let Some(last) = self.unpersisted_readies.back_mut() {
                    // Attach to the last unpersisted ready so that it can be considered to be
                    // persisted with the last ready at the same time.
                    if ready_number <= last.max_empty_number {
                        panic!(
                            "{} ready number is not monotonically increaing, {} <= {}",
                            self.tag, ready_number, last.max_empty_number
                        );
                    }
                    last.max_empty_number = ready_number;

                    if !persisted_msgs.is_empty() {
                        self.unpersisted_message_count += persisted_msgs.capacity();
                        last.raft_msgs.push(persisted_msgs);
                    }
                } else {
                    // If this ready don't need to be persisted and there is no previous unpersisted
                    // ready, we can safely consider it is persisted so the persisted msgs can be
                    // sent immediately.
                    self.persisted_number = ready_number;

                    if !persisted_msgs.is_empty() {
                        fail_point!("raft_before_follower_send");
                        let msgs = self.build_raft_messages(ctx, persisted_msgs);
                        self.send_raft_messages(ctx, msgs);
                    }

                    // The commit index and messages of light ready should be empty because no data
                    // needs to be persisted.
                    let mut light_rd = self.raft_group.advance_append(ready);

                    self.add_light_ready_metric(&light_rd, &mut ctx.raft_metrics);

                    if let Some(idx) = light_rd.commit_index() {
                        panic!(
                            "{} advance ready that has no io task but commit index is changed to {}",
                            self.tag, idx
                        );
                    }
                    if !light_rd.messages().is_empty() {
                        panic!(
                            "{} advance ready that has no io task but message is not empty {:?}",
                            self.tag,
                            light_rd.messages()
                        );
                    }
                    // The committed entries may not be empty when the size is too large to
                    // be fetched in the previous ready.
                    if !light_rd.committed_entries().is_empty() {
                        self.handle_raft_committed_entries(ctx, light_rd.take_committed_entries());
                    }
                }
            }
        }

        if let HandleReadyResult::Snapshot(box HandleSnapshotResult {
            msgs,
            snap_region,
            destroy_regions,
            last_first_index,
            for_witness,
        }) = res
        {
            if for_witness {
                // inform next round to check apply status
                ctx.router
                    .send_casual_msg(
                        snap_region.get_id(),
                        CasualMessage::SnapshotApplied {
                            peer_id: self.peer.get_id(),
                            tombstone: false,
                        },
                    )
                    .unwrap();
            }
            // When applying snapshot, there is no log applied and not compacted yet.
            self.raft_log_size_hint = 0;

            self.apply_snap_ctx = Some(ApplySnapshotContext {
                ready_number,
                scheduled: false,
                msgs,
                persist_res: Some(PersistSnapshotResult {
                    prev_region: self.region().clone(),
                    region: snap_region,
                    destroy_regions,
                    for_witness,
                }),
                destroy_peer_after_apply: false,
            });
            if self.last_compacted_idx == 0 && last_first_index >= RAFT_INIT_LOG_INDEX {
                // There may be stale logs in raft engine, so schedule a task to clean it
                // up. This is a best effort, if TiKV is shutdown before the task is
                // handled, there can still be stale logs not being deleted until next
                // log gc command is executed. This will delete range [0, last_first_index).
                self.schedule_raftlog_gc(ctx, last_first_index);
                self.last_compacted_idx = last_first_index;
            }
            // Pause `read_progress` to prevent serving stale read while applying snapshot
            self.read_progress.pause();
        }

        Some(ReadyResult {
            state_role,
            has_new_entries,
            has_write_ready,
        })
    }

    fn handle_raft_committed_entries<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        committed_entries: Vec<Entry>,
    ) {
        if committed_entries.is_empty() {
            return;
        }
        fail_point!(
            "before_leader_handle_committed_entries",
            self.is_leader(),
            |_| ()
        );

        assert!(
            !self.is_handling_snapshot(),
            "{} is applying snapshot when it is ready to handle committed entries",
            self.tag
        );
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
                    // We must renew current_time because this value may be created a long time ago.
                    // If we do not renew it, this time may be smaller than propose_time of a
                    // command, which was proposed in another thread while this thread receives its
                    // AppendEntriesResponse and is ready to calculate its commit-log-duration.
                    ctx.current_time.replace(monotonic_raw_now());
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

                            debug!("raft log is committed";
                                "req_info" => TrackerTokenArray::new(p.cb.write_trackers()
                                    .into_iter()
                                    .filter_map(|time_tracker| time_tracker.as_tracker_token())
                                    .collect::<Vec<_>>().as_slice())
                            );
                        }
                        p
                    })
                    .collect();
                self.proposals.gc();
                cbs
            } else {
                vec![]
            };

            // Note that the `commit_index` and `commit_term` here may be used to
            // forward the commit index. So it must be less than or equal to persist
            // index.
            let commit_index = cmp::min(
                self.raft_group.raft.raft_log.committed,
                self.raft_group.raft.raft_log.persisted,
            );
            let commit_term = self.get_store().term(commit_index).unwrap();

            let mut apply = Apply::new(
                self.peer_id(),
                self.region_id,
                self.term(),
                commit_index,
                commit_term,
                committed_entries,
                cbs,
                self.region_buckets_info()
                    .bucket_stat()
                    .map(|b| b.meta.clone()),
            );
            apply.on_schedule(&ctx.raft_metrics);
            self.mut_store()
                .trace_cached_entries(apply.entries[0].clone());
            if needs_evict_entry_cache(ctx.cfg.evict_cache_on_memory_ratio) {
                // Compact all cached entries instead of half evict.
                self.mut_store().evict_entry_cache(false);
            }
            ctx.apply_router
                .schedule_task(self.region_id, ApplyTask::apply(apply));
            let apply_ahead_delta = self
                .last_applying_idx
                .saturating_sub(self.raft_group.raft.r.raft_log.persisted);
            RAFT_APPLY_AHEAD_PERSIST_HISTOGRAM.observe(apply_ahead_delta as f64);
        }
        fail_point!("after_send_to_apply_1003", self.peer_id() == 1003, |_| {});
    }

    /// Check long uncommitted proposals and log some info to help find why.
    pub fn check_long_uncommitted_proposals<T>(&mut self, ctx: &mut PollContext<EK, ER, T>) {
        fail_point!(
            "on_check_long_uncommitted_proposals_1",
            self.peer_id() == 1,
            |_| {}
        );
        if self.has_long_uncommitted_proposals(ctx) {
            let status = self.raft_group.status();
            let mut buffer: Vec<(u64, u64, u64)> = Vec::new();
            if let Some(prs) = status.progress {
                for (id, p) in prs.iter() {
                    buffer.push((*id, p.commit_group_id, p.matched));
                }
            }
            warn!(
                "found long uncommitted proposals";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "progress" => ?buffer,
                "cache_first_index" => ?self.get_store().entry_cache_first_index(),
                "next_turn_threshold" => ?self.long_uncommitted_threshold,
            );
        }
    }

    /// Check if there is long uncommitted proposal.
    ///
    /// This will increase the threshold when a long uncommitted proposal is
    /// detected, and reset the threshold when there is no long uncommitted
    /// proposal.
    fn has_long_uncommitted_proposals<T>(&mut self, ctx: &mut PollContext<EK, ER, T>) -> bool {
        let mut has_long_uncommitted = false;
        let base_threshold = ctx.cfg.long_uncommitted_base_threshold.0;
        if let Some(propose_time) = self.proposals.oldest().and_then(|p| p.propose_time) {
            // When a proposal was proposed with this ctx before, the current_time can be
            // some.
            let current_time = *ctx.current_time.get_or_insert_with(monotonic_raw_now);
            let elapsed = match (current_time - propose_time).to_std() {
                Ok(elapsed) => elapsed,
                Err(_) => return false,
            };
            // Increase the threshold for next turn when a long uncommitted proposal is
            // detected.
            if elapsed >= self.long_uncommitted_threshold {
                has_long_uncommitted = true;
                self.long_uncommitted_threshold += base_threshold;
            } else if elapsed < base_threshold {
                self.long_uncommitted_threshold = base_threshold;
            }
        } else {
            self.long_uncommitted_threshold = base_threshold;
        }
        has_long_uncommitted
    }

    fn on_persist_snapshot<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        number: u64,
    ) -> PersistSnapshotResult {
        let snap_ctx = self.apply_snap_ctx.as_mut().unwrap();
        if snap_ctx.ready_number != number || snap_ctx.scheduled {
            panic!(
                "{} apply_snap_ctx {:?} is not valid after persisting snapshot, persist_number {}",
                self.tag, snap_ctx, number
            );
        }

        let persist_res = snap_ctx.persist_res.take().unwrap();
        // Schedule snapshot to apply
        snap_ctx.scheduled = true;
        self.mut_store().persist_snapshot(&persist_res);

        // The peer may change from learner to voter after snapshot persisted.
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
            self.raft_group
                .set_priority(if self.peer.is_witness { -1 } else { 0 });
        };

        self.activate(ctx);

        persist_res
    }

    pub fn on_persist_ready<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        number: u64,
    ) -> Option<PersistSnapshotResult> {
        assert!(ctx.sync_write_worker.is_none());
        if self.persisted_number >= number {
            return None;
        }
        let last_unpersisted_number = self.unpersisted_readies.back().unwrap().number;
        if number > last_unpersisted_number {
            panic!(
                "{} persisted number {} > last_unpersisted_number {}, unpersisted numbers {:?}",
                self.tag, number, last_unpersisted_number, self.unpersisted_readies
            );
        }
        // There must be a match in `self.unpersisted_readies`
        while let Some(v) = self.unpersisted_readies.pop_front() {
            if number < v.number {
                panic!(
                    "{} no match of persisted number {}, unpersisted readies: {:?} {:?}",
                    self.tag, number, v, self.unpersisted_readies
                );
            }
            for msgs in v.raft_msgs {
                fail_point!("raft_before_follower_send");
                self.unpersisted_message_count -= msgs.capacity();
                let m = self.build_raft_messages(ctx, msgs);
                self.send_raft_messages(ctx, m);
            }
            if number == v.number {
                self.persisted_number = v.max_empty_number;
                break;
            }
        }

        self.write_router
            .check_new_persisted(ctx, self.persisted_number);

        if !self.pending_remove {
            // If `pending_remove` is true, no need to call `on_persist_ready` to
            // update persist index.
            let pre_persist_index = self.raft_group.raft.raft_log.persisted;
            let pre_commit_index = self.raft_group.raft.raft_log.committed;
            self.raft_group.on_persist_ready(self.persisted_number);
            self.report_persist_log_duration(pre_persist_index, &mut ctx.raft_metrics);
            self.report_commit_log_duration(pre_commit_index, &mut ctx.raft_metrics);

            let persist_index = self.raft_group.raft.raft_log.persisted;
            self.mut_store().update_cache_persisted(persist_index);

            if self.is_in_force_leader() {
                // forward commit index, the committed entries will be applied in the next raft
                // base tick round
                self.maybe_force_forward_commit_index();
            }
        }

        if self.apply_snap_ctx.is_some() && self.unpersisted_readies.is_empty() {
            // Since the snapshot must belong to the last ready, so if `unpersisted_readies`
            // is empty, it means this persisted number is the last one.
            Some(self.on_persist_snapshot(ctx, number))
        } else {
            None
        }
    }

    pub fn handle_raft_ready_advance<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
    ) -> Option<PersistSnapshotResult> {
        assert!(ctx.sync_write_worker.is_some());
        let ready = self.unpersisted_ready.take()?;

        self.persisted_number = ready.number();

        if !ready.snapshot().is_empty() {
            self.raft_group.advance_append_async(ready);
            // The ready is persisted, but we don't want to handle following light
            // ready immediately to avoid flow out of control, so use
            // `on_persist_ready` instead of `advance_append`.
            // We don't need to set `has_ready` to true, as snapshot is always
            // checked when ticking.
            self.raft_group.on_persist_ready(self.persisted_number);
            return Some(self.on_persist_snapshot(ctx, self.persisted_number));
        }

        let pre_persist_index = self.raft_group.raft.raft_log.persisted;
        let pre_commit_index = self.raft_group.raft.raft_log.committed;
        let mut light_rd = self.raft_group.advance_append(ready);
        self.report_persist_log_duration(pre_persist_index, &mut ctx.raft_metrics);
        self.report_commit_log_duration(pre_commit_index, &mut ctx.raft_metrics);

        let persist_index = self.raft_group.raft.raft_log.persisted;
        if self.is_in_force_leader() {
            // forward commit index, the committed entries will be applied in the next raft
            // base tick round
            self.maybe_force_forward_commit_index();
        }
        self.mut_store().update_cache_persisted(persist_index);

        self.add_light_ready_metric(&light_rd, &mut ctx.raft_metrics);

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
            let msgs = light_rd.take_messages();
            let m = self.build_raft_messages(ctx, msgs);
            self.send_raft_messages(ctx, m);
        }

        if !light_rd.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, light_rd.take_committed_entries());
        }

        None
    }

    pub fn unpersisted_ready_len(&self) -> usize {
        self.unpersisted_readies.len()
    }

    pub fn has_unpersisted_ready(&self) -> bool {
        !self.unpersisted_readies.is_empty()
    }

    fn response_read<T>(
        &self,
        read: &mut ReadIndexRequest<Callback<EK::Snapshot>>,
        ctx: &mut PollContext<EK, ER, T>,
        replica_read: bool,
    ) {
        debug!(
            "handle reads with a read index";
            "request_id" => ?read.id,
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds().len() as i64);
        let time = monotonic_raw_now();
        for (req, cb, mut read_index) in read.take_cmds().drain(..) {
            cb.read_tracker().map(|tracker| {
                GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                    t.metrics.read_index_confirm_wait_nanos =
                        (time - read.propose_time).to_std().unwrap().as_nanos() as u64;
                })
            });
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
                match (read_index, read.read_index) {
                    (Some(local_responsed_index), Some(batch_index)) => {
                        // `read_index` could be less than `read.read_index` because the former is
                        // filled with `committed index` when proposed, and the latter is filled
                        // after a read-index procedure finished.
                        read_index = Some(cmp::max(local_responsed_index, batch_index));
                    }
                    (None, _) => {
                        // Actually, the read_index is none if and only if it's the first one in
                        // read.cmds. Starting from the second, all the following ones' read_index
                        // is not none.
                        read_index = read.read_index;
                    }
                    _ => {}
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

    pub(crate) fn respond_replica_read_error(
        &self,
        read_index_req: &mut ReadIndexRequest<Callback<EK::Snapshot>>,
        response: RaftCmdResponse,
    ) {
        debug!(
            "handle replica reads with a read index failed";
            "request_id" => ?read_index_req.id,
            "response" => ?response,
            "peer_id" => self.peer_id(),
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read_index_req.cmds().len() as i64);
        let time = monotonic_raw_now();
        for (_, ch, _) in read_index_req.take_cmds().drain(..) {
            ch.read_tracker().map(|tracker| {
                GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                    t.metrics.read_index_confirm_wait_nanos = (time - read_index_req.propose_time)
                        .to_std()
                        .unwrap()
                        .as_nanos()
                        as u64;
                })
            });
            ch.report_error(response.clone());
        }
    }

    /// Responses to the ready read index request on the replica, the replica is
    /// not a leader.
    fn post_pending_read_index_on_replica<T>(&mut self, ctx: &mut PollContext<EK, ER, T>) {
        while let Some(mut read) = self.pending_reads.pop_front() {
            // The response of this read index request is lost, but we need it for
            // the memory lock checking result. Resend the request.
            if let Some(read_index) = read.addition_request.take() {
                assert_eq!(read.cmds().len(), 1);
                let (mut req, cb, _) = read.take_cmds().pop().unwrap();
                assert_eq!(req.requests.len(), 1);
                req.requests[0].set_read_index(*read_index);
                let read_cmd = RaftCommand::new(req, cb);
                info!(
                    "re-propose read index request because the response is lost";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer_id(),
                );
                RAFT_READ_INDEX_PENDING_COUNT.sub(1);
                self.send_read_command(ctx, read_cmd);
                continue;
            }

            assert!(read.read_index.is_some());
            let is_read_index_request = read.cmds().len() == 1
                && read.cmds()[0].0.get_requests().len() == 1
                && read.cmds()[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

            let read_index = read.read_index.unwrap();
            if is_read_index_request {
                self.response_read(&mut read, ctx, false);
            } else if self.ready_to_handle_unsafe_replica_read(read_index) {
                self.response_read(&mut read, ctx, true);
            } else if self.get_store().applied_index() + ctx.cfg.follower_read_max_log_gap()
                <= read_index
            {
                let mut response = cmd_resp::new_error(Error::ReadIndexNotReady {
                    region_id: self.region_id,
                    reason: "applied index fail behind read index too long",
                });
                cmd_resp::bind_term(&mut response, self.term());
                self.respond_replica_read_error(&mut read, response);
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
            // NOTE: there could still be some pending reads proposed by the peer when it
            // was leader. They will be cleared in `clear_uncommitted_on_role_change` later
            // in the function.
            self.pending_reads.advance_replica_reads(states);
            self.post_pending_read_index_on_replica(ctx);
        } else {
            self.pending_reads.advance_leader_reads(states);
            propose_time = self.pending_reads.last_ready().map(|r| r.propose_time);
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
            if self.leader_lease.is_suspect() {
                return;
            }
            self.maybe_renew_leader_lease(propose_time, ctx, None);
        }
    }

    pub fn post_apply<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        apply_state: RaftApplyState,
        applied_term: u64,
        apply_metrics: &ApplyMetrics,
    ) -> bool {
        let mut has_ready = false;

        if self.is_handling_snapshot() {
            panic!("{} should not applying snapshot.", self.tag);
        }

        let applied_index = apply_state.get_applied_index();
        self.raft_group.advance_apply_to(applied_index);

        self.cmd_epoch_checker.advance_apply(
            applied_index,
            self.term(),
            self.raft_group.store().region(),
        );

        if !self.is_leader() {
            self.mut_store()
                .compact_entry_cache(apply_state.applied_index + 1);
        }

        let progress_to_be_updated = self.mut_store().applied_term() != applied_term;
        self.mut_store().set_apply_state(apply_state);
        self.mut_store().set_applied_term(applied_term);

        self.peer_stat.written_keys += apply_metrics.written_keys;
        self.peer_stat.written_bytes += apply_metrics.written_bytes;
        self.delete_keys_hint += apply_metrics.delete_keys_hint;
        self.split_check_trigger
            .add_size_diff(apply_metrics.size_diff_hint);

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

        self.read_progress
            .update_applied(applied_index, &ctx.coprocessor_host);

        // Only leaders need to update applied_term.
        if progress_to_be_updated && self.is_leader() {
            if applied_term == self.term() {
                ctx.coprocessor_host
                    .on_applied_current_term(StateRole::Leader, self.region());
            }
            let progress = ReadProgress::applied_term(applied_term);
            let mut meta = ctx.store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
        has_ready
    }

    pub fn post_split(&mut self) {
        self.delete_keys_hint = 0;
        self.split_check_trigger.post_split();

        self.reset_region_buckets();
    }

    pub fn reset_region_buckets(&mut self) {
        self.region_buckets_info_mut().set_bucket_stat(None);
    }

    /// Try to renew leader lease.
    fn maybe_renew_leader_lease<T>(
        &mut self,
        ts: Timespec,
        ctx: &mut PollContext<EK, ER, T>,
        progress: Option<ReadProgress>,
    ) {
        // A nonleader peer should never has leader lease.
        let read_progress = if !should_renew_lease(
            self.is_leader(),
            self.is_splitting(),
            self.is_merging(),
            self.force_leader.is_some(),
        ) {
            None
        } else if self.region().is_in_flashback {
            debug!(
                "prevents renew lease while in flashback state";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            None
        } else {
            self.leader_lease.renew(ts);
            let term = self.term();
            self.leader_lease
                .maybe_new_remote_lease(term)
                .map(ReadProgress::set_leader_lease)
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

    pub fn update_read_progress<T>(
        &self,
        ctx: &mut PollContext<EK, ER, T>,
        progress: ReadProgress,
    ) {
        let mut meta = ctx.store_meta.lock().unwrap();
        let reader = meta.readers.get_mut(&self.region_id).unwrap();
        self.maybe_update_read_progress(reader, progress);
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

    /// Proposes a request.
    ///
    /// Return whether the request has been proposed successfully.
    pub fn propose<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        mut cb: Callback<EK::Snapshot>,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        mut disk_full_opt: DiskFullOpt,
    ) -> bool {
        if self.pending_remove {
            return false;
        }

        ctx.raft_metrics.propose.all.inc();

        let req_admin_cmd_type = if !req.has_admin_request() {
            None
        } else {
            Some(req.get_admin_request().get_cmd_type())
        };
        let is_urgent = is_request_urgent(&req);

        let policy = self.inspect(&req);
        let res = match policy {
            Ok(RequestPolicy::ReadLocal) | Ok(RequestPolicy::StaleRead) => {
                self.read_local(ctx, req, cb);
                return false;
            }
            Ok(RequestPolicy::ReadIndex) => return self.read_index(ctx, req, err_resp, cb),
            Ok(RequestPolicy::ProposeTransferLeader) => {
                return self.propose_transfer_leader(ctx, req, cb);
            }
            Ok(RequestPolicy::ProposeNormal) => {
                // For admin cmds, only region split/merge comes here.
                if req.has_admin_request() {
                    disk_full_opt = DiskFullOpt::AllowedOnAlmostFull;
                }
                self.check_normal_proposal_with_disk_full_opt(ctx, disk_full_opt)
                    .and_then(|_| self.propose_normal(ctx, req))
            }
            Ok(RequestPolicy::ProposeConfChange) => self.propose_conf_change(ctx, req),
            Err(e) => Err(e),
        };
        fail_point!("after_propose");

        match res {
            Err(e) => {
                cmd_resp::bind_error(&mut err_resp, e);
                cb.invoke_with_response(err_resp);
                self.post_propose_fail(req_admin_cmd_type);
                false
            }
            Ok(Either::Right(idx)) => {
                if !cb.is_none() {
                    self.cmd_epoch_checker.attach_to_conflict_cmd(idx, cb);
                }
                self.post_propose_fail(req_admin_cmd_type);
                false
            }
            Ok(Either::Left(idx)) => {
                let has_applied_to_current_term = self.has_applied_to_current_term();
                if has_applied_to_current_term {
                    // After this peer has applied to current term and passed above checking
                    // including `cmd_epoch_checker`, we can safely guarantee
                    // that this proposal will be committed if there is no abnormal leader transfer
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
                    propose_time: None,
                    must_pass_epoch_check: has_applied_to_current_term,
                    sent: false,
                };
                if let Some(cmd_type) = req_admin_cmd_type {
                    self.cmd_epoch_checker
                        .post_propose(cmd_type, idx, self.term());
                }
                self.post_propose(ctx, p);
                if req_admin_cmd_type == Some(AdminCmdType::PrepareMerge) {
                    self.disable_apply_unpersisted_log(idx);
                }
                true
            }
        }
    }

    fn post_propose_fail(&mut self, req_admin_cmd_type: Option<AdminCmdType>) {
        if req_admin_cmd_type == Some(AdminCmdType::PrepareMerge) {
            // If we just failed to propose PrepareMerge, the pessimistic locks status
            // may become MergingRegion incorrectly. So, we have to revert it here.
            // But we have to rule out the case when the region has successfully
            // proposed PrepareMerge or has been in merging, which is decided by
            // the boolean expression below.
            let is_merging = self.is_merging()
                || self
                    .cmd_epoch_checker
                    .last_cmd_index(AdminCmdType::PrepareMerge)
                    .is_some();
            if !is_merging {
                let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
                if pessimistic_locks.status == LocksStatus::MergingRegion {
                    pessimistic_locks.status = LocksStatus::Normal;
                }
            }
        }
    }

    fn post_propose<T>(
        &mut self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        mut p: Proposal<Callback<EK::Snapshot>>,
    ) {
        // Try to renew leader lease on every consistent read/write request.
        if poll_ctx.current_time.is_none() {
            poll_ctx.current_time = Some(monotonic_raw_now());
        }
        p.propose_time = poll_ctx.current_time;

        self.proposals.push(p);
    }

    pub fn transfer_leader(&mut self, peer: &metapb::Peer) {
        info!(
            "transfer leader";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "peer" => ?peer,
        );

        self.raft_group.transfer_leader(peer.get_id());
        self.should_wake_up = true;
    }

    fn pre_transfer_leader<T: Transport>(
        &mut self,
        peer: &metapb::Peer,
        extra_msgs: Vec<ExtraMessage>,
        ctx: &mut PollContext<EK, ER, T>,
    ) -> bool {
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
        msg.set_index(self.get_store().entry_cache_first_index().unwrap_or(0));
        // log term here represents the term of last log. For leader, the term of last
        // log is always its current term. Not just set term because raft library
        // forbids setting it for MsgTransferLeader messages.
        msg.set_log_term(self.term());
        self.raft_group.raft.msgs.push(msg);

        extra_msgs.into_iter().for_each(|extra_msg| {
            let mut msg = RaftMessage::default();
            msg.set_region_id(self.region_id);
            msg.set_from_peer(self.peer.clone());
            msg.set_to_peer(peer.clone());
            msg.set_region_epoch(self.region().get_region_epoch().clone());
            msg.set_extra_msg(extra_msg);
            self.send_raft_messages(ctx, vec![msg]);
        });

        true
    }

    pub fn ready_to_transfer_leader<T>(
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
        ctx.raft_metrics.propose.local_read.inc();
        cb.invoke_read(self.handle_read(ctx, req, false, Some(self.get_store().commit_index())))
    }

    pub fn pre_read_index(&self) -> Result<()> {
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
            return Err(Error::ReadIndexNotReady {
                reason: "can not read index due to split",
                region_id: self.region_id,
            });
        }
        if self.is_merging() {
            return Err(Error::ReadIndexNotReady {
                reason: "can not read index due to merge",
                region_id: self.region_id,
            });
        }
        Ok(())
    }

    pub fn has_unresolved_reads(&self) -> bool {
        self.pending_reads.has_unresolved()
    }

    /// `ReadIndex` requests could be lost in network, so on followers commands
    /// could queue in `pending_reads` forever. Sending a new `ReadIndex`
    /// periodically can resolve this.
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

    pub fn push_pending_read(
        &mut self,
        read: ReadIndexRequest<Callback<EK::Snapshot>>,
        is_leader: bool,
    ) {
        self.pending_reads.push_back(read, is_leader);
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
            poll_ctx.raft_metrics.propose.unsafe_read_index.inc();
            cmd_resp::bind_error(&mut err_resp, e);
            cb.report_error(err_resp);
            self.should_wake_up = true;
            return false;
        }

        let now = monotonic_raw_now();
        if self.is_leader() {
            let lease_state = self.inspect_lease();
            if can_amend_read::<Callback<EK::Snapshot>>(
                self.pending_reads.back(),
                &req,
                lease_state,
                poll_ctx.cfg.raft_store_max_leader_lease(),
                now,
            ) {
                // Must use the commit index of `PeerStorage` instead of the commit index
                // in raft-rs which may be greater than the former one.
                // For more details, see the annotations above `on_leader_commit_idx_changed`.
                let commit_index = self.get_store().commit_index();
                if let Some(read) = self.pending_reads.back_mut() {
                    // A read request proposed in the current lease is found; combine the new
                    // read request to that previous one, so that no proposing needed.
                    read.push_command(req, cb, commit_index);
                    return false;
                }
            }
        }

        if !self.is_leader() && self.leader_id() == INVALID_ID {
            poll_ctx
                .raft_metrics
                .invalid_proposal
                .read_index_no_leader
                .inc();
            // The leader may be hibernated, send a message for trying to awaken the leader.
            if self.bcast_wake_up_time.is_none()
                || self
                    .bcast_wake_up_time
                    .as_ref()
                    .unwrap()
                    .saturating_elapsed()
                    >= Duration::from_millis(MIN_BCAST_WAKE_UP_INTERVAL)
            {
                self.bcast_wake_up_message(poll_ctx);
                self.bcast_wake_up_time = Some(TiInstant::now_coarse());

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
            cb.report_error(err_resp);
            return false;
        }

        poll_ctx.raft_metrics.propose.read_index.inc();
        self.bcast_wake_up_time = None;

        let request = req
            .mut_requests()
            .get_mut(0)
            .filter(|req| req.has_read_index())
            .map(|req| req.take_read_index());
        let (id, dropped) = self.propose_read_index(request.as_ref());
        if dropped && self.is_leader() {
            // The message gets dropped silently, can't be handled anymore.
            apply::notify_stale_req(self.term(), cb);
            poll_ctx.raft_metrics.propose.dropped_read_index.inc();
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.push_pending_read(read, self.is_leader());
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
        if self.leader_lease.is_suspect() {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(poll_ctx, req) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                    sent: false,
                };
                self.post_propose(poll_ctx, p);
            }
        }

        true
    }

    // Propose a read index request to the raft group, return the request id and
    // whether this request had dropped silently
    pub fn propose_read_index(
        &mut self,
        request: Option<&raft_cmdpb::ReadIndexRequest>,
    ) -> (Uuid, bool) {
        propose_read_index(&mut self.raft_group, request)
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
        let (mut min_m, min_c) = (min_m.unwrap_or(0), min_c.unwrap_or(0));
        if min_m < min_c {
            warn!(
                "min_matched < min_committed, raft progress is inaccurate";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "min_matched" => min_m,
                "min_committed" => min_c,
            );
            // Reset `min_matched` to `min_committed`, since the raft log at `min_committed`
            // is known to be committed in all peers, all of the peers should also have
            // replicated it
            min_m = min_c;
        }
        Ok((min_m, min_c))
    }

    fn pre_propose_prepare_merge<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        req: &mut RaftCmdRequest,
    ) -> Result<()> {
        // Check existing prepare_merge_fence.
        let mut passed_merge_fence = false;
        if self.prepare_merge_fence > 0 {
            let applied_index = self.get_store().applied_index();
            if applied_index >= self.prepare_merge_fence {
                // Check passed, clear fence and start proposing pessimistic locks and
                // PrepareMerge.
                self.prepare_merge_fence = 0;
                self.pending_prepare_merge = None;
                passed_merge_fence = true;
            } else {
                self.pending_prepare_merge = Some(mem::take(req));
                info!(
                    "reject PrepareMerge because applied_index has not reached prepare_merge_fence";
                    "region_id" => self.region_id,
                    "applied_index" => applied_index,
                    "prepare_merge_fence" => self.prepare_merge_fence
                );
                return Err(Error::PendingPrepareMerge);
            }
        }

        let last_index = self.raft_group.raft.raft_log.last_index();
        let (min_matched, min_committed) = self.get_min_progress()?;
        if min_matched == 0
            || min_committed == 0
            || last_index - min_matched > ctx.cfg.merge_max_log_gap
            || last_index - min_committed > ctx.cfg.merge_max_log_gap * 2
            || min_matched < self.last_sent_snapshot_idx
        {
            return Err(box_err!(
                "log gap too large, skip merge: matched: {}, committed: {}, last index: {}, last_snapshot: {}",
                min_matched,
                min_committed,
                last_index,
                self.last_sent_snapshot_idx
            ));
        }
        let mut entry_size = 0;
        for entry in self.raft_group.raft.raft_log.entries(
            min_committed + 1,
            NO_LIMIT,
            GetEntriesContext::empty(false),
        )? {
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
        let entry_size_limit = ctx.cfg.raft_entry_max_size.0 as usize * 9 / 10;
        if entry_size > entry_size_limit {
            return Err(box_err!(
                "log gap size exceed entry size limit, skip merging."
            ));
        };

        // Record current proposed index. If there are some in-memory pessimistic locks,
        // we should wait until applying to the proposed index before proposing
        // pessimistic locks and PrepareMerge. Otherwise, if an already proposed command
        // will remove a pessimistic lock, we will make some deleted locks appear again.
        if !passed_merge_fence {
            let pessimistic_locks = self.txn_ext.pessimistic_locks.read();
            if !pessimistic_locks.is_empty() {
                if pessimistic_locks.status != LocksStatus::Normal {
                    // If `status` is not `Normal`, it means the in-memory pessimistic locks are
                    // being transferred, probably triggered by transferring leader. In this case,
                    // we abort merging to simplify the situation.
                    return Err(box_err!(
                        "pessimistic locks status is {:?}, skip merging.",
                        pessimistic_locks.status
                    ));
                }
                if self.get_store().applied_index() < last_index {
                    self.prepare_merge_fence = last_index;
                    self.pending_prepare_merge = Some(mem::take(req));
                    info!(
                        "start rejecting new proposals before prepare merge";
                        "region_id" => self.region_id,
                        "prepare_merge_fence" => last_index
                    );
                    return Err(Error::PendingPrepareMerge);
                }
            }
        }

        fail_point!("before_propose_locks_on_region_merge");
        self.propose_locks_before_prepare_merge(ctx, entry_size_limit - entry_size)?;

        req.mut_admin_request()
            .mut_prepare_merge()
            .set_min_index(min_matched + 1);
        Ok(())
    }

    fn propose_locks_before_prepare_merge<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        size_limit: usize,
    ) -> Result<()> {
        let pessimistic_locks = self.txn_ext.pessimistic_locks.upgradable_read();
        if pessimistic_locks.is_empty() {
            let mut pessimistic_locks = RwLockUpgradableReadGuard::upgrade(pessimistic_locks);
            pessimistic_locks.status = LocksStatus::MergingRegion;
            return Ok(());
        }
        // The proposed pessimistic locks here will also be carried in CommitMerge.
        // Check the size to avoid CommitMerge exceeding the size limit of a raft entry.
        // This check is a inaccurate check. We will check the size again accurately
        // later using the protobuf encoding.
        if pessimistic_locks.memory_size > size_limit {
            return Err(box_err!(
                "pessimistic locks size {} exceed size limit {}, skip merging.",
                pessimistic_locks.memory_size,
                size_limit
            ));
        }

        let mut cmd = RaftCmdRequest::default();
        for (key, (lock, _deleted)) in &*pessimistic_locks {
            let mut put = PutRequest::default();
            put.set_cf(CF_LOCK.to_string());
            put.set_key(key.as_encoded().to_owned());
            put.set_value(lock.to_lock().to_bytes());
            let mut req = Request::default();
            req.set_cmd_type(CmdType::Put);
            req.set_put(put);
            cmd.mut_requests().push(req);
        }
        cmd.mut_header().set_region_id(self.region_id);
        cmd.mut_header()
            .set_region_epoch(self.region().get_region_epoch().clone());
        cmd.mut_header().set_peer(self.peer.clone());
        let proposal_size = cmd.compute_size();
        if proposal_size as usize > size_limit {
            return Err(box_err!(
                "pessimistic locks size {} exceed size limit {}, skip merging.",
                proposal_size,
                size_limit
            ));
        }

        {
            let mut pessimistic_locks = RwLockUpgradableReadGuard::upgrade(pessimistic_locks);
            pessimistic_locks.status = LocksStatus::MergingRegion;
        }
        debug!("propose {} pessimistic locks before prepare merge", cmd.get_requests().len();
            "region_id" => self.region_id);
        self.propose_normal(ctx, cmd)?;
        Ok(())
    }

    fn pre_propose<T: Transport>(
        &mut self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        req: &mut RaftCmdRequest,
    ) -> Result<ProposalContext> {
        poll_ctx
            .coprocessor_host
            .pre_propose(self.region(), req)
            .map_err(|e| {
                // If the error of prepropose contains str `NO_VALID_SPLIT_KEY`, it may mean the
                // split_key of the split request is the region start key which
                // means we may have so many potential duplicate mvcc versions
                // that we can not manage to get a valid split key. So, we
                // trigger a compaction to handle it.
                if e.to_string().contains(NO_VALID_SPLIT_KEY) {
                    let safe_ts = (|| {
                        fail::fail_point!("safe_point_inject", |t| {
                            t.unwrap().parse::<u64>().unwrap()
                        });
                        poll_ctx.safe_point.load(Ordering::Relaxed)
                    })();
                    if safe_ts <= self.last_record_safe_point {
                        debug!(
                            "skip schedule compact range due to safe_point not updated";
                            "region_id" => self.region_id,
                            "safe_point" => safe_ts,
                        );
                        return e;
                    }

                    let start_key = enc_start_key(self.region());
                    let end_key = enc_end_key(self.region());

                    let mut all_scheduled = true;
                    for cf in [CF_WRITE, CF_DEFAULT] {
                        let task = CompactTask::Compact {
                            cf_name: String::from(cf),
                            start_key: Some(start_key.clone()),
                            end_key: Some(end_key.clone()),
                            bottommost_level_force: true,
                        };

                        if let Err(e) = poll_ctx
                            .cleanup_scheduler
                            .schedule(CleanupTask::Compact(task))
                        {
                            error!(
                                "schedule compact range task failed";
                                "region_id" => self.region_id,
                                "cf" => ?cf,
                                "err" => ?e,
                            );
                            all_scheduled = false;
                            break;
                        }
                    }

                    if all_scheduled {
                        info!(
                            "schedule compact range due to no valid split keys";
                            "region_id" => self.region_id,
                            "safe_point" => safe_ts,
                            "region_start_key" => log_wrappers::Value::key(&start_key),
                            "region_end_key" => log_wrappers::Value::key(&end_key),
                        );
                        self.last_record_safe_point = safe_ts;
                    }
                }
                e
            })?;
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
            AdminCmdType::CommitMerge => ctx.insert(ProposalContext::COMMIT_MERGE),
            _ => {}
        }

        Ok(ctx)
    }

    /// Propose normal request to raft
    ///
    /// Returns Ok(Either::Left(index)) means the proposal is proposed
    /// successfully and is located on `index` position.
    /// Ok(Either::Right(index)) means the proposal is rejected by
    /// `CmdEpochChecker` and the `index` is the position of the last
    /// conflict admin cmd.
    fn propose_normal<T: Transport>(
        &mut self,
        poll_ctx: &mut PollContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        // Should not propose normal in force leader state.
        // In `pre_propose_raft_command`, it rejects all the requests expect conf-change
        // if in force leader state.
        if self.force_leader.is_some()
            && req.get_admin_request().get_cmd_type() != AdminCmdType::RollbackMerge
        {
            poll_ctx.raft_metrics.invalid_proposal.force_leader.inc();
            panic!(
                "{} propose normal in force leader state {:?}",
                self.tag, self.force_leader
            );
        };

        if (self.pending_merge_state.is_some()
            && req.get_admin_request().get_cmd_type() != AdminCmdType::RollbackMerge)
            || (self.prepare_merge_fence > 0
                && req.get_admin_request().get_cmd_type() != AdminCmdType::PrepareMerge)
        {
            return Err(Error::ProposalInMergingMode(self.region_id));
        }

        poll_ctx.raft_metrics.propose.normal.inc();

        if self.has_applied_to_current_term() {
            // Only when applied index's term is equal to current leader's term, the
            // information in epoch checker is up to date and can be used to check epoch.
            if let Some(index) = self
                .cmd_epoch_checker
                .propose_check_epoch(&req, self.term())
            {
                return Ok(Either::Right(index));
            }
        } else if req.has_admin_request() {
            // The admin request is rejected because it may need to update epoch checker
            // which introduces an uncertainty and may breaks the correctness of epoch
            // checker.
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag,
                self.get_store().applied_term(),
                self.term()
            ));
        }

        // TODO: validate request for unexpected changes.
        let ctx = match self.pre_propose(poll_ctx, &mut req) {
            Ok(ctx) => ctx,
            Err(e) => {
                // Skipping PrepareMerge is logged when the PendingPrepareMerge error is
                // generated.
                if !matches!(e, Error::PendingPrepareMerge) {
                    warn!(
                        "skip proposal";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "err" => ?e,
                        "error_code" => %e.error_code(),
                    );
                }
                return Err(e);
            }
        };

        let data = req.write_to_bytes()?;
        poll_ctx
            .raft_metrics
            .propose_log_size
            .observe(data.len() as f64);

        if data.len() as u64 > poll_ctx.cfg.raft_entry_max_size.0 {
            error!(
                "entry is too large";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "size" => data.len(),
            );
            return Err(Error::RaftEntryTooLarge {
                region_id: self.region_id,
                entry_size: data.len() as u64,
            });
        }

        fail_point!("raft_propose", |_| Ok(Either::Right(0)));
        let propose_index = self.next_proposal_index();
        self.raft_group.propose(ctx.to_vec(), data)?;
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }

        // Prepare Merge need to be broadcast to as many as followers when disk full.
        if req.has_admin_request()
            && (!matches!(poll_ctx.self_disk_usage, DiskUsage::Normal)
                || !self.disk_full_peers.is_empty())
        {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::PrepareMerge | AdminCmdType::RollbackMerge => {
                    self.has_region_merge_proposal = true;
                    self.region_merge_proposal_index = propose_index;
                    for (k, v) in &mut self.disk_full_peers.peers {
                        if !matches!(v.0, DiskUsage::AlreadyFull) {
                            v.1 = true;
                            self.raft_group
                                .raft
                                .adjust_max_inflight_msgs(*k, poll_ctx.cfg.raft_max_inflight_msgs);
                            debug!(
                                "{:?} adjust max inflight msgs to {} on peer: {:?}",
                                req.get_admin_request().get_cmd_type(),
                                poll_ctx.cfg.raft_max_inflight_msgs,
                                k
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(Either::Left(propose_index))
    }

    pub fn maybe_reject_transfer_leader_msg<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        msg: &eraftpb::Message,
        peer_disk_usage: DiskUsage,
    ) -> bool {
        let pending_snapshot = self.is_handling_snapshot() || self.has_pending_snapshot();
        // shouldn't transfer leader to witness peer or non-witness waiting data
        if self.is_witness() || self.wait_data
            || pending_snapshot
            || msg.get_from() != self.leader_id()
            // Transfer leader to node with disk full will lead to write availablity downback.
            // But if the current leader is disk full, and send such request, we should allow it,
            // because it may be a read leader balance request.
            || (!matches!(ctx.self_disk_usage, DiskUsage::Normal) &&
            matches!(peer_disk_usage, DiskUsage::Normal))
        {
            info!(
                "reject transferring leader";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "from" => msg.get_from(),
                "pending_snapshot" => pending_snapshot,
                "disk_usage" => ?ctx.self_disk_usage,
                "is_witness" => self.is_witness(),
                "wait_data" => self.wait_data,
            );
            return true;
        }
        false
    }

    /// Before ack the transfer leader message sent by the leader.
    /// Currently, it only warms up the entry cache in this stage.
    ///
    /// This return whether the msg should be acked. When cache is warmed up
    /// or the warmup operation is timeout, it is true.
    pub fn pre_ack_transfer_leader_msg<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        msg: &eraftpb::Message,
    ) -> bool {
        if !ctx.cfg.warmup_entry_cache_enabled() {
            return true;
        }

        // The start index of warmup range. It is leader's entry_cache_first_index,
        // which in general is equal to the lowest matched index.
        let mut low = msg.get_index();
        let last_index = self.get_store().last_index();
        let mut should_ack_now = false;

        // Need not to warm up when the index is 0.
        // There are two cases where index can be 0:
        // 1. During rolling upgrade, old instances may not support warmup.
        // 2. The leader's entry cache is empty.
        if low == 0 || low > last_index {
            // There is little possibility that the warmup_range_start
            // is larger than the last index. Check the test case
            // `test_when_warmup_range_start_is_larger_than_last_index`
            // for details.
            should_ack_now = true;
        } else {
            if low < self.last_compacted_idx {
                low = self.last_compacted_idx
            };
            // Check if the entry cache is already warmed up.
            if let Some(first_index) = self.get_store().entry_cache_first_index() {
                if low >= first_index {
                    fail_point!("entry_cache_already_warmed_up");
                    should_ack_now = true;
                }
            }
        }

        if should_ack_now {
            return true;
        }

        // Check if the warmup operation is timeout if warmup is already started.
        if let Some(state) = self.mut_store().entry_cache_warmup_state_mut() {
            // If it is timeout, this peer should ack the message so that
            // the leadership transfer process can continue.
            state.check_task_timeout(ctx.cfg.max_entry_cache_warmup_duration.0)
        } else {
            self.mut_store().async_warm_up_entry_cache(low).is_none()
        }
    }

    pub fn ack_transfer_leader_msg(
        &mut self,
        reply_cmd: bool, // whether it is a reply to a TransferLeader command
    ) {
        let mut msg = eraftpb::Message::new();
        msg.set_from(self.peer_id());
        msg.set_to(self.leader_id());
        msg.set_msg_type(eraftpb::MessageType::MsgTransferLeader);
        msg.set_index(self.get_store().applied_index());
        msg.set_log_term(self.term());
        if reply_cmd {
            msg.set_context(Bytes::from_static(TRANSFER_LEADER_COMMAND_REPLY_CTX));
        }
        self.raft_group.raft.msgs.push(msg);
    }

    /// Return true if the transfer leader request is accepted.
    ///
    /// When transferring leadership begins, leader sends a pre-transfer
    /// to target follower first to ensures it's ready to become leader.
    /// After that the real transfer leader process begin.
    ///
    /// 1. pre_transfer_leader on leader: Leader will send a MsgTransferLeader
    ///    to follower.
    /// 2. pre_ack_transfer_leader_msg on follower: If follower passes all
    ///    necessary checks, it will try to warmup the entry cache.
    /// 3. ack_transfer_leader_msg on follower: When the entry cache has been
    ///    warmed up or the operator is timeout, the follower reply an ACK with
    ///    type MsgTransferLeader and its promised persistent index.
    ///
    /// Additional steps when there are remaining pessimistic
    /// locks to propose (detected in function on_transfer_leader_msg).
    ///    1. Leader firstly proposes pessimistic locks and then proposes a
    ///       TransferLeader command.
    ///    2. ack_transfer_leader_msg on follower again: The follower applies
    ///       the TransferLeader command and replies an ACK with special context
    ///       TRANSFER_LEADER_COMMAND_REPLY_CTX.
    ///
    /// 4. ready_to_transfer_leader on leader: Leader checks if it's appropriate
    ///    to transfer leadership. If it does, it calls raft transfer_leader API
    ///    to do the remaining work.
    ///
    /// See also: tikv/rfcs#37.
    fn propose_transfer_leader<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        req: RaftCmdRequest,
        cb: Callback<EK::Snapshot>,
    ) -> bool {
        let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
        let extra_msgs = match ctx
            .coprocessor_host
            .pre_transfer_leader(self.region(), transfer_leader)
        {
            Err(err) => {
                warn!("Coprocessor rejected transfer leader."; "err" => ?err,
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "transferee" => transfer_leader.get_peer().get_id());
                let mut resp = RaftCmdResponse::new();
                *resp.mut_header().mut_error() = Error::from(err).into();
                cb.invoke_with_response(resp);
                return false;
            }
            Ok(msgs) => msgs,
        };
        ctx.raft_metrics.propose.transfer_leader.inc();

        let prs = self.raft_group.raft.prs();

        let (_, peers) = transfer_leader
            .get_peers()
            .iter()
            .filter(|peer| peer.id != self.peer.id)
            .fold((0, vec![]), |(max_matched, mut chosen), peer| {
                if let Some(pr) = prs.get(peer.id) {
                    match pr.matched.cmp(&max_matched) {
                        cmp::Ordering::Greater => (pr.matched, vec![peer]),
                        cmp::Ordering::Equal => {
                            chosen.push(peer);
                            (max_matched, chosen)
                        }
                        cmp::Ordering::Less => (max_matched, chosen),
                    }
                } else {
                    (max_matched, chosen)
                }
            });
        let peer = match peers.len() {
            0 => transfer_leader.get_peer(),
            1 => peers.first().unwrap(),
            _ => peers.choose(&mut rand::thread_rng()).unwrap(),
        };

        let transferred = if peer.id == self.peer.id {
            false
        } else {
            self.pre_transfer_leader(peer, extra_msgs, ctx)
        };

        // transfer leader command doesn't need to replicate log and apply, so we
        // return immediately. Note that this command may fail, we can view it just as
        // an advice
        cb.invoke_with_response(make_transfer_leader_response());

        transferred
    }

    // Fails in such cases:
    // 1. A pending conf change has not been applied yet;
    // 2. Removing the leader is not allowed in the configuration;
    // 3. The conf change makes the raft group not healthy;
    // 4. The conf change is dropped by raft group internally.
    /// Returns Ok(Either::Left(index)) means the proposal is proposed
    /// successfully and is located on `index` position. Ok(Either::
    /// Right(index)) means the proposal is rejected by `CmdEpochChecker` and
    /// the `index` is the position of the last conflict admin cmd.
    fn propose_conf_change<T>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
        if self.pending_merge_state.is_some() {
            return Err(Error::ProposalInMergingMode(self.region_id));
        }
        if self.raft_group.raft.has_pending_conf() {
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
        // Actually, according to the implementation of conf change in raft-rs, this
        // check must be passed if the previous check that `pending_conf_index`
        // should be less than or equal to `self.get_store().applied_index()` is
        // passed.
        if self.get_store().applied_term() != self.term() {
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag,
                self.get_store().applied_term(),
                self.term()
            ));
        }

        if let Err(err) = ctx.coprocessor_host.pre_propose(self.region(), &mut req) {
            warn!("Coprocessor rejected proposing conf change.";
                "err" => ?err,
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return Err(box_err!(
                "{} rejected by coprocessor(reason = {})",
                self.tag,
                err
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

        // Because the group is always woken up when there is log gap, so no need
        // to wake it up again when command is aborted by log gap.
        util::check_conf_change(
            &ctx.cfg,
            &self.raft_group,
            self.region(),
            &self.peer,
            changes.as_ref(),
            &cc,
            self.is_in_force_leader(),
            &self.peer_heartbeats,
        )?;

        ctx.raft_metrics.propose.conf_change.inc();
        ctx.raft_metrics.propose_log_size.observe(data_size as f64);
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
            if let Err(e) = check_req_region_epoch(&req, &region, true) {
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
        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
            let safe_ts = self.read_progress.safe_ts();
            if safe_ts < read_ts {
                // Advancing resolved ts may be expensive, only notify if read_ts - safe_ts >
                // 200ms.
                if TimeStamp::from(read_ts).physical() > TimeStamp::from(safe_ts).physical() + 200 {
                    self.read_progress.notify_advance_resolved_ts();
                }
                warn!(
                    "read rejected by safe timestamp";
                    "safe_ts" => safe_ts,
                    "read_ts" => read_ts,
                    "tag" => &self.tag,
                );
                let mut response = cmd_resp::new_error(Error::DataIsNotReady {
                    region_id: region.get_id(),
                    peer_id: self.peer_id(),
                    safe_ts,
                });
                cmd_resp::bind_term(&mut response, self.term());
                return ReadResponse {
                    response,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                };
            }
        }

        let read_ctx = if let Ok(read_ts) = decode_u64(&mut req.get_header().get_flag_data()) {
            ReadContext::new(None, Some(read_ts))
        } else {
            ReadContext::new(None, None)
        };

        let mut reader = PollContextReader {
            engines: &ctx.engines,
        };
        let mut resp = reader.execute(
            &read_ctx,
            &req,
            &Arc::new(region),
            read_index,
            None,
            &ctx.coprocessor_host,
        );
        if let Some(snap) = resp.snapshot.as_mut() {
            snap.txn_ext = Some(self.txn_ext.clone());
            snap.bucket_meta = self
                .region_buckets_info()
                .bucket_stat()
                .map(|s| s.meta.clone());
        }
        resp.txn_extra_op = self.txn_extra_op.load();
        cmd_resp::bind_term(&mut resp.response, self.term());
        resp
    }

    pub fn voters(&self) -> raft::util::Union<'_> {
        self.raft_group.raft.prs().conf().voters().ids()
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
            if state.get_commit() == extra_msg.get_index() {
                self.add_want_rollback_merge_peer(peer_id);
            }
        }
    }

    pub fn add_want_rollback_merge_peer(&mut self, peer_id: u64) {
        assert!(self.pending_merge_state.is_some());
        self.want_rollback_merge_peers.insert(peer_id);
    }

    pub fn clear_disk_full_peers<T>(&mut self, ctx: &PollContext<EK, ER, T>) {
        let disk_full_peers = mem::take(&mut self.disk_full_peers);
        let raft = &mut self.raft_group.raft;
        for peer in disk_full_peers.peers.into_keys() {
            raft.adjust_max_inflight_msgs(peer, ctx.cfg.raft_max_inflight_msgs);
        }
    }

    pub fn refill_disk_full_peers<T>(&mut self, ctx: &PollContext<EK, ER, T>) {
        self.clear_disk_full_peers(ctx);
        debug!(
            "region id {}, peer id {}, store id {}: refill disk full peers when peer disk usage status changed or merge triggered",
            self.region_id,
            self.peer.get_id(),
            self.peer.get_store_id()
        );

        // Collect disk full peers and all peers' `next_idx` to find a potential quorum.
        let peers_len = self.get_store().region().get_peers().len();
        let mut normal_peers = HashSet::default();
        let mut next_idxs = Vec::with_capacity(peers_len);
        let mut min_peer_index = u64::MAX;
        for peer in self.get_store().region().get_peers() {
            let (peer_id, store_id) = (peer.get_id(), peer.get_store_id());
            let usage = ctx.store_disk_usages.get(&store_id);
            if usage.is_none() {
                // Always treat the leader itself as normal.
                normal_peers.insert(peer_id);
            }
            if let Some(pr) = self.raft_group.raft.prs().get(peer_id) {
                // status 3-normal, 2-almostfull, 1-alreadyfull, only for simplying the sort
                // func belowing.
                let mut status = 3;
                if let Some(usg) = usage {
                    status = match usg {
                        DiskUsage::Normal => 3,
                        DiskUsage::AlmostFull => 2,
                        DiskUsage::AlreadyFull => 1,
                    };
                }

                if !self.down_peer_ids.contains(&peer_id) {
                    next_idxs.push((peer_id, pr.next_idx, usage, status));
                    if min_peer_index > pr.next_idx {
                        min_peer_index = pr.next_idx;
                    }
                }
            }
        }
        if self.has_region_merge_proposal {
            debug!(
                "region id {}, peer id {}, store id {} has a merge request, with region_merge_proposal_index {}",
                self.region_id,
                self.peer.get_id(),
                self.peer.get_store_id(),
                self.region_merge_proposal_index
            );
            if min_peer_index > self.region_merge_proposal_index {
                self.has_region_merge_proposal = false;
            }
        }

        if normal_peers.len() == peers_len {
            return;
        }

        // Reverse sort peers based on `next_idx`, `usage` and `store healthy status`,
        // then try to get a potential quorum.
        next_idxs.sort_by(|x, y| {
            if x.3 == y.3 {
                y.1.cmp(&x.1)
            } else {
                y.3.cmp(&x.3)
            }
        });

        let raft = &mut self.raft_group.raft;
        self.disk_full_peers.majority = !raft.prs().has_quorum(&normal_peers);

        // Here set all peers can be sent when merging.
        for &(peer, _, usage, ..) in &next_idxs {
            if let Some(usage) = usage {
                if self.has_region_merge_proposal && !matches!(*usage, DiskUsage::AlreadyFull) {
                    self.disk_full_peers.peers.insert(peer, (*usage, true));
                    raft.adjust_max_inflight_msgs(peer, ctx.cfg.raft_max_inflight_msgs);
                    debug!(
                        "refill disk full peer max inflight to {} on a merging region: region id {}, peer id {}",
                        ctx.cfg.raft_max_inflight_msgs, self.region_id, peer
                    );
                } else {
                    self.disk_full_peers.peers.insert(peer, (*usage, false));
                    raft.adjust_max_inflight_msgs(peer, 0);
                    debug!(
                        "refill disk full peer max inflight to {} on region without merging: region id {}, peer id {}",
                        0, self.region_id, peer
                    );
                }
            }
        }

        if !self.disk_full_peers.majority {
            // Less than majority peers are in disk full status.
            return;
        }

        let (mut potential_quorum, mut quorum_ok) = (HashSet::default(), false);
        let mut has_dangurous_set = false;
        for &(peer_id, _, _, status) in &next_idxs {
            potential_quorum.insert(peer_id);

            if status == 1 {
                // already full peer.
                has_dangurous_set = true;
            }

            if raft.prs().has_quorum(&potential_quorum) {
                quorum_ok = true;
                break;
            }
        }

        self.dangerous_majority_set = has_dangurous_set;

        // For the Peer with AlreadFull in potential quorum set, we still need to send
        // logs to it. To support incoming configure change.
        if quorum_ok {
            for peer in potential_quorum {
                if let Some(x) = self.disk_full_peers.peers.get_mut(&peer) {
                    // It can help to establish a quorum.
                    x.1 = true;
                    // for merge region, all peers have been set to the max.
                    if !self.has_region_merge_proposal {
                        raft.adjust_max_inflight_msgs(peer, 1);
                        debug!(
                            "refill disk full peer max inflight to 1 in potential quorum set: region id {}, peer id {}",
                            self.region_id, peer
                        );
                    }
                }
            }
        }
    }

    // Check disk usages for the peer itself and other peers in the raft group.
    // The return value indicates whether the proposal is allowed or not.
    fn check_normal_proposal_with_disk_full_opt<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        disk_full_opt: DiskFullOpt,
    ) -> Result<()> {
        let leader_allowed = match ctx.self_disk_usage {
            DiskUsage::Normal => true,
            DiskUsage::AlmostFull => !matches!(disk_full_opt, DiskFullOpt::NotAllowedOnFull),
            DiskUsage::AlreadyFull => false,
        };
        let mut disk_full_stores = Vec::new();
        if !leader_allowed {
            disk_full_stores.push(ctx.store.id);
            // Try to transfer leader to a node with disk usage normal to maintain write
            // availability. If majority node is disk full, to transfer leader or not is not
            // necessary. Note: Need to exclude learner node.
            if !self.disk_full_peers.majority {
                let target_peer = self
                    .get_store()
                    .region()
                    .get_peers()
                    .iter()
                    .find(|x| {
                        !self.disk_full_peers.has(x.get_id())
                            && x.get_id() != self.peer.get_id()
                            && !self.down_peer_ids.contains(&x.get_id())
                            && !matches!(x.get_role(), PeerRole::Learner)
                    })
                    .cloned();
                if let Some(p) = target_peer {
                    debug!(
                        "try to transfer leader because of current leader disk full";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "target_peer_id" => p.get_id(),
                    );
                    self.pre_transfer_leader(&p, vec![], ctx);
                }
            }
        } else {
            // Check followers.
            if self.disk_full_peers.is_empty() {
                return Ok(());
            }
            if !self.dangerous_majority_set {
                if !self.disk_full_peers.majority {
                    return Ok(());
                }
                // Majority peers are in disk full status but the request carries a special
                // flag.
                if matches!(disk_full_opt, DiskFullOpt::AllowedOnAlmostFull)
                    && self.disk_full_peers.peers.values().any(|x| x.1)
                {
                    return Ok(());
                }
            }
            for peer in self.get_store().region().get_peers() {
                let (peer_id, store_id) = (peer.get_id(), peer.get_store_id());
                if self.disk_full_peers.peers.get(&peer_id).is_some() {
                    disk_full_stores.push(store_id);
                }
            }
        }
        let errmsg = format!(
            "propose failed: tikv disk full, cmd diskFullOpt={:?}, leader diskUsage={:?}",
            disk_full_opt, ctx.self_disk_usage
        );
        Err(Error::DiskFull(disk_full_stores, errmsg))
    }

    /// Check if the command will be likely to pass all the check and propose.
    pub fn will_likely_propose(&mut self, cmd: &RaftCmdRequest) -> bool {
        !self.pending_remove
            && self.is_leader()
            && self.pending_merge_state.is_none()
            && self.prepare_merge_fence == 0
            && self.raft_group.raft.lead_transferee.is_none()
            && self.has_applied_to_current_term()
            && self
                .cmd_epoch_checker
                .propose_check_epoch(cmd, self.term())
                .is_none()
    }

    pub fn maybe_gen_approximate_buckets<T>(&self, ctx: &PollContext<EK, ER, T>) {
        if ctx.coprocessor_host.cfg.enable_region_bucket() && !self.region().get_peers().is_empty()
        {
            if let Err(e) = ctx
                .split_check_scheduler
                .schedule(SplitCheckTask::ApproximateBuckets(self.region().clone()))
            {
                error!(
                    "failed to schedule check approximate buckets";
                    "region_id" => self.region().get_id(),
                    "peer_id" => self.peer_id(),
                    "err" => %e,
                );
            }
        }
    }

    #[inline]
    pub fn is_in_force_leader(&self) -> bool {
        matches!(
            self.force_leader,
            Some(ForceLeaderState::ForceLeader { .. })
        )
    }

    pub fn unsafe_recovery_maybe_finish_wait_apply(&mut self, force: bool) {
        if let Some(UnsafeRecoveryState::WaitApply { target_index, .. }) =
            &self.unsafe_recovery_state
        {
            if self.raft_group.raft.raft_log.applied >= *target_index || force {
                if self.is_in_force_leader() {
                    info!(
                        "Unsafe recovery, finish wait apply";
                        "region_id" => self.region().get_id(),
                        "peer_id" => self.peer_id(),
                        "target_index" => target_index,
                        "applied" =>  self.raft_group.raft.raft_log.applied,
                        "force" => force,
                    );
                }
                self.unsafe_recovery_state = None;
            }
        }
    }

    pub fn snapshot_recovery_maybe_finish_wait_apply(&mut self, force: bool) {
        if let Some(SnapshotBrState::WaitLogApplyToLast {
            target_index,
            valid_for_term,
            ..
        }) = &self.snapshot_recovery_state
        {
            if valid_for_term
                .map(|vt| vt != self.raft_group.raft.term)
                .unwrap_or(false)
            {
                info!("leadership changed, aborting syncer because required."; "region_id" => self.region().id);
                match self.snapshot_recovery_state.take() {
                    Some(SnapshotBrState::WaitLogApplyToLast {
                        syncer,
                        valid_for_term,
                        ..
                    }) => {
                        syncer.abort(AbortReason::StaleCommand {
                            region_id: self.region().get_id(),
                            expected_term: valid_for_term.unwrap_or_default(),
                            current_term: self.raft_group.raft.term,
                        });
                    }
                    _ => unreachable!(),
                };
                return;
            }

            if self.raft_group.raft.raft_log.applied >= *target_index
                || force
                || self.pending_remove
            {
                info!("snapshot recovery wait apply finished";
                    "region_id" => self.region().get_id(),
                    "peer_id" => self.peer_id(),
                    "target_index" => target_index,
                    "applied" =>  self.raft_group.raft.raft_log.applied,
                    "force" => force,
                );
                self.snapshot_recovery_state = None;
            }
        }
    }

    pub fn update_last_leader_committed_idx(&mut self, committed_index: u64) {
        if self.is_leader() {
            // Ignore.
            return;
        }

        let local_committed_index = self.get_store().commit_index();
        if committed_index < local_committed_index {
            warn!(
                "stale committed index";
                "region_id" => self.region().get_id(),
                "peer_id" => self.peer_id(),
                "last_committed_index" => committed_index,
                "local_index" => local_committed_index,
            );
        } else {
            self.last_leader_committed_idx = Some(committed_index);
            debug!(
                "update last committed index from leader";
                "region_id" => self.region().get_id(),
                "peer_id" => self.peer_id(),
                "last_committed_index" => committed_index,
                "local_index" => local_committed_index,
            );
        }
    }

    pub fn needs_update_last_leader_committed_idx(&self) -> bool {
        self.busy_on_apply.is_some() && self.last_leader_committed_idx.is_none()
    }
}

#[derive(Default, Debug)]
pub struct DiskFullPeers {
    majority: bool,
    // Indicates whether a peer can help to establish a quorum.
    peers: HashMap<u64, (DiskUsage, bool)>,
}

impl DiskFullPeers {
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
    pub fn majority(&self) -> bool {
        self.majority
    }
    pub fn set_majority(&mut self, majority: bool) {
        self.majority = majority;
    }
    pub fn peers(&self) -> &HashMap<u64, (DiskUsage, bool)> {
        &self.peers
    }
    pub fn peers_mut(&mut self) -> &mut HashMap<u64, (DiskUsage, bool)> {
        &mut self.peers
    }
    pub fn has(&self, peer_id: u64) -> bool {
        !self.peers.is_empty() && self.peers.contains_key(&peer_id)
    }
    pub fn get(&self, peer_id: u64) -> Option<DiskUsage> {
        self.peers.get(&peer_id).map(|x| x.0)
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

    fn region_replication_status<T>(
        &mut self,
        ctx: &PollContext<EK, ER, T>,
    ) -> Option<RegionReplicationStatus> {
        if self.replication_mode_version == 0 {
            return None;
        }
        let mut status = RegionReplicationStatus {
            state_id: self.replication_mode_version,
            ..Default::default()
        };
        let state = if !self.replication_sync {
            if self.dr_auto_sync_state != DrAutoSyncState::Async {
                // use raft_log_gc_threshold, it's indicate the log is almost synced.
                let res = self.check_group_commit_consistent(ctx.cfg.raft_log_gc_threshold);
                if Some(true) != res {
                    let mut buffer: SmallVec<[(u64, u64, u64); 5]> = SmallVec::new();
                    if self.get_store().applied_term() >= self.term() {
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
                        "progress" => ?buffer,
                        "dr_auto_sync_state" => ?self.dr_auto_sync_state,
                    );
                } else {
                    // Once the DR replicas catch up the log during the `SyncRecover` phase, we
                    // should enable group commit to promise `IntegrityOverLabel`. then safe
                    // to switch to the `Sync` phase.
                    if self.dr_auto_sync_state == DrAutoSyncState::SyncRecover {
                        self.switch_group_commit(true, true, &ctx.global_replication_state)
                    }
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

    pub fn check_group_commit_consistent(&mut self, allow_gap: u64) -> Option<bool> {
        if !self.is_leader() || !self.raft_group.raft.apply_to_current_term() {
            return None;
        }
        let original = self.raft_group.raft.group_commit();
        let res = {
            // Hack: to check groups consistent we need to enable group commit first
            // otherwise `maximal_committed_index` will return the committed index
            // based on majorty instead of commit group
            // TODO: remove outdated workaround after fixing raft interface, but old version
            // keep this workaround.
            self.raft_group.raft.enable_group_commit(true);
            let (index, mut group_consistent) =
                self.raft_group.raft.mut_prs().maximal_committed_index();
            if self.raft_group.raft.raft_log.committed > index {
                group_consistent &= self.raft_group.raft.raft_log.committed - index < allow_gap;
            }
            Some(group_consistent)
        };
        self.raft_group.raft.enable_group_commit(original);
        res
    }

    pub fn heartbeat_pd<T>(&mut self, ctx: &PollContext<EK, ER, T>) {
        let task = PdTask::Heartbeat(HeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            down_peers: self.collect_down_peers(ctx),
            peer: self.peer.clone(),
            pending_peers: self.collect_pending_peers(ctx),
            written_bytes: self.peer_stat.written_bytes,
            written_keys: self.peer_stat.written_keys,
            approximate_size: self.approximate_size(),
            approximate_keys: self.approximate_keys(),
            replication_status: self.region_replication_status(ctx),
            wait_data_peers: self.wait_data_peers.clone(),
        });
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
            return;
        }
        fail_point!("schedule_check_split");
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
            "to" => ?to
        );
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

    fn build_raft_message(
        &mut self,
        msg: eraftpb::Message,
        disk_usage: DiskUsage,
    ) -> Option<RaftMessage> {
        let mut send_msg = self.prepare_raft_message();

        send_msg.set_disk_usage(disk_usage);

        let to_peer = match self.get_peer_from_cache(msg.get_to()) {
            Some(p) => p,
            None => {
                warn!(
                    "failed to look up recipient peer";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "to_peer" => msg.get_to(),
                );
                return None;
            }
        };

        send_msg.set_to_peer(to_peer);

        if msg.get_from() != self.peer.get_id() {
            debug!(
                "redirecting message";
                "msg_type" => ?msg.get_msg_type(),
                "from" => msg.get_from(),
                "to" => msg.get_to(),
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
        }

        // There could be two cases:
        // - Target peer already exists but has not established communication with
        //   leader yet
        // - Target peer is added newly due to member change or region split, but it's
        //   not created yet
        // For both cases the region start key and end key are attached in RequestVote
        // and Heartbeat message for the store of that peer to check whether to create a
        // new peer when receiving these messages, or just to wait for a pending region
        // split to perform later.
        if self.get_store().is_initialized() && is_initial_msg(&msg) {
            let region = self.region();
            send_msg.set_start_key(region.get_start_key().to_vec());
            send_msg.set_end_key(region.get_end_key().to_vec());
        }

        send_msg.set_message(msg);

        Some(send_msg)
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
        ctx.raft_metrics.check_stale_peer.inc();
        if self.check_stale_conf_ver < self.region().get_region_epoch().get_conf_ver()
            || self.region().get_region_epoch().get_conf_ver() == 0
        {
            self.check_stale_conf_ver = self.region().get_region_epoch().get_conf_ver();
            self.check_stale_peers = self.region().get_peers().to_vec();
            if let Some(create_by_peer) = self.create_by_peer.as_ref() {
                // Push create_by_peer in case the peer is removed before
                // initialization which has no peer in region.
                self.check_stale_peers.push(create_by_peer.clone());
            }
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

    pub fn send_snap_gen_precheck_request<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        to_peer: &metapb::Peer,
    ) {
        let mut extra_msg = ExtraMessage::default();
        extra_msg.set_type(ExtraMessageType::MsgSnapGenPrecheckRequest);
        self.send_extra_message(extra_msg, &mut ctx.trans, to_peer);
    }

    pub fn send_snap_gen_precheck_response<T: Transport>(
        &mut self,
        ctx: &mut PollContext<EK, ER, T>,
        to_peer: &metapb::Peer,
        passed: bool,
    ) {
        let mut extra_msg = ExtraMessage::default();
        extra_msg.set_type(ExtraMessageType::MsgSnapGenPrecheckResponse);
        extra_msg.set_snap_gen_precheck_passed(passed);
        self.send_extra_message(extra_msg, &mut ctx.trans, to_peer);
    }

    pub fn send_want_rollback_merge<T: Transport>(
        &mut self,
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
        extra_msg.set_index(premerge_commit);
        self.send_extra_message(extra_msg, &mut ctx.trans, &to_peer);
    }

    pub fn require_updating_max_ts(&self, pd_scheduler: &Scheduler<PdTask<EK, ER>>) {
        let epoch = self.region().get_region_epoch();
        let term_low_bits = self.term() & ((1 << 32) - 1); // 32 bits
        let version_lot_bits = epoch.get_version() & ((1 << 31) - 1); // 31 bits
        let initial_status = (term_low_bits << 32) | (version_lot_bits << 1);
        self.txn_ext
            .max_ts_sync_status
            .store(initial_status, Ordering::SeqCst);
        info!(
            "require updating max ts";
            "region_id" => self.region_id,
            "initial_status" => initial_status,
        );
        if let Err(e) = pd_scheduler.schedule(PdTask::UpdateMaxTimestamp {
            region_id: self.region_id,
            initial_status,
            txn_ext: self.txn_ext.clone(),
        }) {
            error!(
                "failed to update max ts";
                "err" => ?e,
            );
        }
    }

    fn activate_in_memory_pessimistic_locks(&mut self) {
        let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
        pessimistic_locks.status = LocksStatus::Normal;
        pessimistic_locks.term = self.term();
        pessimistic_locks.version = self.region().get_region_epoch().get_version();
    }

    fn clear_in_memory_pessimistic_locks(&mut self) {
        let mut pessimistic_locks = self.txn_ext.pessimistic_locks.write();
        pessimistic_locks.status = LocksStatus::NotLeader;
        pessimistic_locks.clear();
        pessimistic_locks.term = self.term();
        pessimistic_locks.version = self.region().get_region_epoch().get_version();

        // Also clear merge related states
        self.prepare_merge_fence = 0;
        self.pending_prepare_merge = None;
    }

    pub fn need_renew_lease_at<T>(
        &self,
        ctx: &PollContext<EK, ER, T>,
        current_time: Timespec,
    ) -> bool {
        let renew_bound = match self.leader_lease.need_renew(current_time) {
            Some(ts) => ts,
            None => return false,
        };
        let max_lease = ctx.cfg.raft_store_max_leader_lease();
        let has_overlapped_reads = self.pending_reads.back().map_or(false, |read| {
            // If there is any read index whose lease can cover till next heartbeat
            // then we don't need to propose a new one
            read.propose_time + max_lease > renew_bound
        });
        let has_overlapped_writes = self.proposals.back().map_or(false, |proposal| {
            // If there is any write whose lease can cover till next heartbeat
            // then we don't need to propose a new one
            proposal
                .propose_time
                .map_or(false, |propose_time| propose_time + max_lease > renew_bound)
        });
        !has_overlapped_reads && !has_overlapped_writes
    }

    pub fn adjust_cfg_if_changed<T>(&mut self, ctx: &PollContext<EK, ER, T>) {
        let raft_max_inflight_msgs = ctx.cfg.raft_max_inflight_msgs;
        if self.is_leader() && (raft_max_inflight_msgs != self.raft_max_inflight_msgs) {
            let peers: Vec<_> = self.region().get_peers().into();
            for p in peers {
                if p != self.peer {
                    self.raft_group
                        .raft
                        .adjust_max_inflight_msgs(p.get_id(), raft_max_inflight_msgs);
                }
            }
            self.raft_max_inflight_msgs = raft_max_inflight_msgs;
        }
        self.raft_group.raft.r.max_msg_size = ctx.cfg.raft_max_size_per_msg.0;
        self.max_apply_unpersisted_log_limit = ctx.cfg.max_apply_unpersisted_log_limit;
        if self
            .raft_group
            .raft
            .raft_log
            .max_apply_unpersisted_log_limit
            != self.max_apply_unpersisted_log_limit
        {
            if self.max_apply_unpersisted_log_limit == 0 {
                self.disable_apply_unpersisted_log(0);
            } else if self.is_leader() {
                // Currently only enable unpersisted apply on leader.
                self.maybe_update_apply_unpersisted_log_state(
                    self.raft_group.raft.raft_log.applied,
                );
            }
        }
    }

    /// Update states of the peer which can be changed in the previous raft
    /// tick.
    pub fn post_raft_group_tick(&mut self) {
        self.lead_transferee = self.raft_group.raft.lead_transferee.unwrap_or_default();
    }
}

/// `RequestPolicy` decides how we handle a request.
#[derive(Clone, PartialEq, Debug)]
pub enum RequestPolicy {
    // Handle the read request directly without dispatch.
    ReadLocal,
    StaleRead,
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
            if get_transfer_leader_cmd(req).is_some()
                && !WriteBatchFlags::from_bits_truncate(req.get_header().get_flags())
                    .contains(WriteBatchFlags::TRANSFER_LEADER_PROPOSAL)
            {
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

        fail_point!("perform_read_index", |_| Ok(RequestPolicy::ReadIndex));

        fail_point!("perform_read_local", |_| Ok(RequestPolicy::ReadLocal));

        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Ok(RequestPolicy::StaleRead);
        }

        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term differs from current raft's term, leader
        // transfer must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // Local read should be performed, if and only if leader is in lease.
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
        self.get_store().applied_term() == self.term()
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

struct PollContextReader<'a, EK, ER> {
    engines: &'a Engines<EK, ER>,
}

impl<'a, EK, ER> ReadExecutor for PollContextReader<'a, EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Tablet = EK;

    fn get_tablet(&mut self) -> &EK {
        &self.engines.kv
    }

    fn get_snapshot(&mut self, _: &Option<LocalReadContext<'_, EK>>) -> Arc<EK::Snapshot> {
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

pub fn get_sync_log_from_request(msg: &RaftCmdRequest) -> bool {
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
                | AdminCmdType::PrepareFlashback
                | AdminCmdType::FinishFlashback
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
            | AdminCmdType::BatchSwitchWitness
    )
}

pub fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::default();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::default());
    let mut resp = RaftCmdResponse::default();
    resp.set_admin_response(response);
    resp
}

// The Raft message context for a MsgTransferLeader if it is a reply of a
// TransferLeader command.
pub const TRANSFER_LEADER_COMMAND_REPLY_CTX: &[u8] = &[1];

mod memtrace {
    use std::mem;

    use tikv_util::memory::HeapSize;

    use super::*;

    impl<EK, ER> Peer<EK, ER>
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        pub fn proposal_size(&self) -> usize {
            let mut heap_size = self.pending_reads.approximate_heap_size();
            for prop in &self.proposals.queue {
                heap_size += prop.approximate_heap_size();
            }
            heap_size
        }

        pub fn rest_size(&self) -> usize {
            // 2 words for every item in `peer_heartbeats`.
            16 * self.peer_heartbeats.capacity()
            // 2 words for every item in `peers_start_pending_time`.
            + 16 * self.peers_start_pending_time.capacity()
            // 1 word for every item in `down_peer_ids`
            + 8 * self.down_peer_ids.capacity()
            + mem::size_of::<metapb::Peer>() * self.check_stale_peers.capacity()
            // 1 word for every item in `want_rollback_merge_peers`
            + 8 * self.want_rollback_merge_peers.capacity()
            // Ignore more heap content in `raft::eraftpb::Message`.
            + (self.unpersisted_message_count
                + self.apply_snap_ctx.as_ref().map_or(0, |ctx| ctx.msgs.len()))
                * mem::size_of::<eraftpb::Message>()
            + mem::size_of_val(self.pending_request_snapshot_count.as_ref())
        }
    }
}

#[cfg(test)]
mod tests {
    use kvproto::raft_cmdpb;
    use protobuf::ProtobufEnum;

    use super::*;
    use crate::store::{msg::ExtCallback, util::u64_to_timespec};

    #[test]
    fn test_sync_log() {
        let white_list = [
            AdminCmdType::InvalidAdmin,
            AdminCmdType::CompactLog,
            AdminCmdType::TransferLeader,
            AdminCmdType::ComputeHash,
            AdminCmdType::VerifyHash,
            AdminCmdType::BatchSwitchWitness,
            AdminCmdType::UpdateGcPeer,
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
            AdminCmdType::BatchSwitchWitness,
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
            &[ProposalContext::COMMIT_MERGE],
            &[ProposalContext::SPLIT, ProposalContext::SYNC_LOG],
            &[ProposalContext::PREPARE_MERGE, ProposalContext::SYNC_LOG],
            &[ProposalContext::COMMIT_MERGE, ProposalContext::SYNC_LOG],
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

        for (op, policy) in [
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

        // Stale read
        for op in &[CmdType::Get, CmdType::Snap] {
            let mut req = req.clone();
            let mut request = raft_cmdpb::Request::default();
            request.set_cmd_type(*op);
            req.set_requests(vec![request].into());
            req.mut_header()
                .set_flags(txn_types::WriteBatchFlags::STALE_READ.bits());
            table.push((req, RequestPolicy::StaleRead));
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
            inspector.inspect(&req).unwrap_err();
        }
    }

    #[test]
    fn test_propose_queue_find_proposal() {
        let mut pq: ProposalQueue<Callback<engine_panic::PanicSnapshot>> = ProposalQueue::new(1, 2);
        let gen_term = |index: u64| (index / 10) + 1;
        let push_proposal = |pq: &mut ProposalQueue<_>, index: u64| {
            pq.push(Proposal {
                is_conf_change: false,
                index,
                term: gen_term(index),
                cb: Callback::write(Box::new(|_| {})),
                propose_time: Some(u64_to_timespec(index)),
                must_pass_epoch_check: false,
                sent: false,
            });
        };
        for index in 1..=100 {
            push_proposal(&mut pq, index);
        }
        let mut pre_remove = 0;
        for remove_i in 1..=100 {
            let index = remove_i + 100;
            // Push more proposal
            push_proposal(&mut pq, index);
            // Find propose time
            for i in 1..=index {
                let pt = pq.find_propose_time(gen_term(i), i);
                if i <= pre_remove {
                    assert!(pt.is_none())
                } else {
                    assert_eq!(pt.unwrap(), u64_to_timespec(i))
                };
            }
            // Find a proposal and remove all previous proposals
            for i in 1..=remove_i {
                let p = pq.find_proposal(gen_term(i), i, 0);
                let must_found_proposal = p.is_some() && (i > pre_remove);
                let proposal_removed_previous = p.is_none() && (i <= pre_remove);
                assert!(must_found_proposal || proposal_removed_previous);
                // `find_proposal` will remove proposal so `pop` must return None
                assert!(pq.pop(gen_term(i), i).is_none());
                assert!(pq.find_propose_time(gen_term(i), i).is_none());
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
                // Must move the entire struct to closure,
                // or else it will be dropped early in 2021 edition
                // https://doc.rust-lang.org/edition-guide/rust-2021/disjoint-capture-in-closures.html
                let _ = &d;
                d.0 = false;
            })
        }
        fn must_not_call() -> ExtCallback {
            Box::new(move || unreachable!())
        }
        let mut pq: ProposalQueue<Callback<engine_panic::PanicSnapshot>> = ProposalQueue::new(1, 2);

        // (1, 4) and (1, 5) is not committed
        let entries = vec![(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (2, 6), (2, 7)];
        let committed = [(1, 1), (1, 2), (1, 3), (2, 6), (2, 7)];
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
                propose_time: None,
                must_pass_epoch_check: false,
                sent: false,
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
        use std::sync::mpsc;

        use engine_test::kv::KvTestSnapshot;
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
