// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::peer_storage::InvokeContext;
use super::*;
use crate::errors::*;
use crate::store::{Progress as ReadProgress, ReadDelegate, RegionTask};
use bitflags::bitflags;
use byteorder::BigEndian;
use bytes::{BufMut, Bytes, BytesMut};
use error_code::ErrorCodeExt;
use fail::fail_point;
use futures::channel::mpsc::UnboundedSender;
use kvproto::metapb::PeerRole;
use kvproto::pdpb::PeerStats;
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, TransferLeaderRequest};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::*;
use raft;
use raft::eraftpb::MessageType;
use raft::{INVALID_ID, LightReady, ProgressState, RawNode, Ready, SnapshotStatus, StateRole, Storage};
use raft_proto::*;
use raftstore::coprocessor;
use raftstore::store::{local_metrics::*, metrics::*, QueryStats, util as outil};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};
use protobuf::Message;
use tikv_util::mpsc::Sender;
use tikv_util::time::{duration_to_sec, monotonic_raw_now};
use tikv_util::{box_err, debug, Either, error, info, MustConsumeVec, warn};
use time::Timespec;
use uuid::Uuid;
use raftstore::store::util::{ChangePeerI, Lease, LeaseState};
use tikv_util::codec::number::decode_u64;
use tikv_util::sys::disk;
use txn_types::WriteBatchFlags;

pub(crate) struct ReadyICPair {
    pub(crate) ready: raft::Ready,
    pub(crate) ic: InvokeContext,
}

const SHRINK_CACHE_CAPACITY: usize = 64;
const MIN_BCAST_WAKE_UP_INTERVAL: u64 = 1_000; // 1s
const REGION_READ_PROGRESS_CAP: usize = 128;
const MAX_COMMITTED_SIZE_PER_READY: u64 = 16 * 1024 * 1024;

/// The returned states of the peer after checking whether it is stale
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StaleState {
    Valid,
    ToValidate,
    LeaderMissing,
}

pub(crate) fn notify_stale_req(term: u64, cb: Callback) {
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.invoke_with_response(resp);
}

pub(crate) fn notify_req_region_removed(region_id: u64, cb: Callback) {
    let region_not_found = Error::RegionNotFound(region_id);
    let resp = cmd_resp::new_error(region_not_found);
    cb.invoke_with_response(resp);
}

pub(crate) struct ProposalMeta {
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) renew_lease_time: Instant,
}

struct ProposalQueue {
    tag: String,
    queue: VecDeque<Proposal>,
}

impl ProposalQueue {
    fn new(tag: String) -> ProposalQueue {
        ProposalQueue {
            tag,
            queue: VecDeque::new(),
        }
    }

    fn find_propose_time(&self, term: u64, index: u64) -> Option<Timespec> {
        self.queue
            .binary_search_by_key(&(term, index), |p: &Proposal| (p.term, p.index))
            .ok()
            .map(|i| self.queue[i].propose_time)
            .flatten()
    }

    // Find proposal in front or at the given term and index
    fn pop(&mut self, term: u64, index: u64) -> Option<Proposal> {
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
    fn find_proposal(&mut self, term: u64, index: u64, current_term: u64) -> Option<Proposal> {
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
                notify_stale_req(current_term, p.cb);
            }
        }
        None
    }

    fn push(&mut self, p: Proposal) {
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
    pub approximate_size: u64,
    pub written_query_stats: QueryStats,
}

pub struct ProposedAdminCmd {
    cmd_type: AdminCmdType,
    epoch_state: AdminCmdEpochState,
    index: u64,
    cbs: Vec<Callback>,
}

impl ProposedAdminCmd {
    fn new(
        cmd_type: AdminCmdType,
        epoch_state: AdminCmdEpochState,
        index: u64,
    ) -> ProposedAdminCmd {
        ProposedAdminCmd {
            cmd_type,
            epoch_state,
            index,
            cbs: Vec::new(),
        }
    }
}

struct CmdEpochChecker {
    // Although it's a deque, because of the characteristics of the settings from `admin_cmd_epoch_lookup`,
    // the max size of admin cmd is 2, i.e. split/merge and change peer.
    proposed_admin_cmd: VecDeque<ProposedAdminCmd>,
    term: u64,
}

impl Default for CmdEpochChecker {
    fn default() -> CmdEpochChecker {
        CmdEpochChecker {
            proposed_admin_cmd: VecDeque::new(),
            term: 0,
        }
    }
}

impl CmdEpochChecker {
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
            let epoch_state = admin_cmd_epoch_lookup(cmd_type);
            (epoch_state.check_ver, epoch_state.check_conf_ver)
        };
        self.last_conflict_index(check_ver, check_conf_ver)
    }

    pub fn post_propose(&mut self, cmd_type: AdminCmdType, index: u64, term: u64) {
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

    pub fn attach_to_conflict_cmd(&mut self, index: u64, cb: Callback) {
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

impl Drop for CmdEpochChecker {
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

pub(crate) struct Peer {
    /// The ID of the Region which this Peer belongs to.
    pub(crate) region_id: u64,

    pub(crate) tag: String,
    /// The Peer meta information.
    pub(crate) peer: metapb::Peer,
    /// The Raft state machine of this Peer.
    pub(crate) raft_group: raft::RawNode<PeerStorage>,
    /// The cache of meta information for Region's other Peers.
    peer_cache: RefCell<HashMap<u64, metapb::Peer>>,
    /// Record the last instant of each peer's heartbeat response.
    pub(crate) peer_heartbeats: HashMap<u64, Instant>,

    pub(crate) proposals: ProposalQueue,
    pending_reads: ReadIndexQueue,

    /// If it fails to send messages to leader.
    pub leader_unreachable: bool,

    // Record the instants of peers being added into the configuration.
    // Remove them after they are not pending any more.
    pub(crate) peers_start_pending_time: Vec<(u64, Instant)>,
    pub(crate) consistency_state: ConsistencyState,

    // Index of last scheduled committed raft log.
    pub(crate) last_applying_idx: u64,
    pub(crate) last_compacted_index: u64,
    // The index of the latest urgent proposal index.
    pub(crate) last_urgent_proposal_idx: u64,
    // The index of the latest committed split command.
    pub(crate) last_committed_split_idx: u64,

    pub(crate) pending_remove: bool,

    // PendingSplit is set to true when there are Split admin command proposed.
    // It is set back to false when Split is finished.
    pending_split: bool,

    // The index of the latest committed prepare merge command.
    last_committed_prepare_merge_idx: u64,
    pub(crate) pending_merge_state: Option<raft_serverpb::MergeState>,
    leader_missing_time: Option<Instant>,
    leader_lease: Lease,

    // If a snapshot is being applied asynchronously, messages should not be sent.
    pending_messages: Vec<eraftpb::Message>,
    pub(crate) peer_stat: PeerStat,

    /// Whether this peer is created by replication and is the first
    /// one of this region on local store.
    pub local_first_replicate: bool,

    /// The max timestamp recorded in the concurrency manager is only updated at leader.
    /// So if a peer becomes leader from a follower, the max timestamp can be outdated.
    /// We need to update the max timestamp with a latest timestamp from PD before this
    /// peer can work.
    /// From the least significant to the most, 1 bit marks whether the timestamp is
    /// updated, 31 bits for the current epoch version, 32 bits for the current term.
    /// The version and term are stored to prevent stale UpdateMaxTimestamp task from
    /// marking the lowest bit.
    pub max_ts_sync_status: Arc<AtomicU64>,

    pub(crate) wait_follower_split_files: Option<MsgWaitFollowerSplitFiles>,
    pub(crate) followers_split_files_done: HashMap<u64, u64>,

    /// Check whether this proposal can be proposed based on its epoch.
    cmd_epoch_checker: CmdEpochChecker,

    /// The number of the last unpersisted ready.
    last_unpersisted_number: u64,
}

impl Peer {
    pub(crate) fn new(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        sched: Sender<RegionTask>,
        region: &metapb::Region,
        peer: metapb::Peer,
    ) -> Result<Peer> {
        if peer.get_id() == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let tag = format!(
            "[region {}:{}] {}",
            region.get_id(),
            region.get_region_epoch().get_version(),
            peer.get_id()
        );

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
            max_committed_size_per_ready: MAX_COMMITTED_SIZE_PER_READY,
            ..Default::default()
        };

        let logger = slog_global::get_global().new(slog::o!("region_id" => region.get_id()));
        let raft_group = RawNode::new(&raft_cfg, ps, &logger)?;
        let mut peer = Peer {
            peer,
            tag,
            region_id: region.get_id(),
            raft_group,
            peer_cache: Default::default(),
            proposals: ProposalQueue::new(tag.clone()),
            pending_reads: Default::default(),
            leader_unreachable: false,
            peer_heartbeats: Default::default(),
            peers_start_pending_time: Default::default(),
            consistency_state: ConsistencyState {
                last_check_time: Instant::now(),
                index: raft::INVALID_INDEX,
                context: vec![],
                hash: vec![],
            },
            leader_lease: Lease::new(cfg.raft_store_max_leader_lease()),
            last_applying_idx: 0,
            last_compacted_index: 0,
            last_urgent_proposal_idx: 0,
            last_committed_split_idx: 0,
            pending_remove: false,
            pending_split: false,
            last_committed_prepare_merge_idx: 0,
            pending_merge_state: Default::default(),
            leader_missing_time: Some(Instant::now()),
            pending_messages: vec![],
            peer_stat: PeerStat::default(),
            local_first_replicate: false,
            max_ts_sync_status: Arc::new(AtomicU64::new(0)),
            wait_follower_split_files: None,
            followers_split_files_done: Default::default(),
            cmd_epoch_checker: Default::default(),
            last_unpersisted_number: 0,
        };
        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(peer)
    }

    /// Register self to apply_scheduler so that the peer is then usable.
    /// Also trigger `RegionChangeEvent::Create` here.
    pub fn activate(&self, msgs: &mut ApplyMsgs) {
        let msg = ApplyMsg::Registration(MsgRegistration::new(&self));
        msgs.msgs.push(msg);
    }

    pub(crate) fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub(crate) fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    pub(crate) fn maybe_destroy(&mut self) -> Option<DestroyPeerJob> {
        if self.pending_remove {
            info!(
                "is being destroyed, skip";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return None;
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

    pub(crate) fn destroy(&mut self, engines: &Engines, keep_data: bool) -> Result<()> {
        let t = Instant::now();

        let region = self.region().clone();
        info!(
            "begin to destroy";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        // Set Tombstone state explicitly
        let mut raft_wb = rfengine::WriteBatch::new();
        self.mut_store().clear_meta(&mut raft_wb);
        write_peer_state(
            &mut raft_wb,
            &region,
            raft_serverpb::PeerState::Tombstone,
            self.pending_merge_state.clone(),
        );
        engines.raft.write(&raft_wb)?;

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
            notify_req_region_removed(region.get_id(), cb);
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

    /// Set the region of a peer.
    ///
    /// This will update the region of the peer, caller must ensure the region
    /// has been preserved in a durable device.
    pub fn set_region(
        &mut self,
        host: &coprocessor::CoprocessorHost<kvengine::Engine>,
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
            host.on_region_changed(
                self.region(),
                coprocessor::RegionChangeEvent::Update,
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
        self.raft_group.raft.state == raft::StateRole::Leader
    }

    #[inline]
    pub fn get_role(&self) -> raft::StateRole {
        self.raft_group.raft.state
    }

    #[inline]
    pub fn get_store(&self) -> &PeerStorage {
        self.raft_group.store()
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut PeerStorage {
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
    fn send<T, I>(&mut self, trans: &mut T, msgs: I, metrics: &mut RaftSendMessageMetrics)
    where
        T: Transport,
        I: IntoIterator<Item = eraftpb::Message>,
    {
        for msg in msgs {
            let msg_type = msg.get_msg_type();
            let snapshot_index = msg.get_request_snapshot();
            let i = self.send_raft_message(msg, trans) as usize;
            match msg_type {
                MessageType::MsgAppend => metrics.append[i] += 1,
                MessageType::MsgAppendResponse => {
                    if snapshot_index != raft::INVALID_INDEX {
                        metrics.request_snapshot[i] += 1;
                    }
                    metrics.append_resp[i] += 1;
                }
                MessageType::MsgRequestPreVote => metrics.prevote[i] += 1,
                MessageType::MsgRequestPreVoteResponse => metrics.prevote_resp[i] += 1,
                MessageType::MsgRequestVote => metrics.vote[i] += 1,
                MessageType::MsgRequestVoteResponse => metrics.vote_resp[i] += 1,
                MessageType::MsgSnapshot => metrics.snapshot[i] += 1,
                MessageType::MsgHeartbeat => metrics.heartbeat[i] += 1,
                MessageType::MsgHeartbeatResponse => metrics.heartbeat_resp[i] += 1,
                MessageType::MsgTransferLeader => metrics.transfer_leader[i] += 1,
                MessageType::MsgReadIndex => metrics.read_index[i] += 1,
                MessageType::MsgReadIndexResp => metrics.read_index_resp[i] += 1,
                MessageType::MsgTimeoutNow => {
                    // After a leader transfer procedure is triggered, the lease for
                    // the old leader may be expired earlier than usual, since a new leader
                    // may be elected and the old leader doesn't step down due to
                    // network partition from the new leader.
                    // For lease safety during leader transfer, transit `leader_lease`
                    // to suspect.
                    self.leader_lease.suspect(monotonic_raw_now());

                    metrics.timeout_now[i] += 1;
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
        }
    }

    fn send_raft_message<T: Transport>(&mut self, msg: eraftpb::Message, trans: &mut T) -> bool {
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
                return false;
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
        if self.get_store().is_initialized() && outil::is_initial_msg(&msg) {
            let region = self.region();
            send_msg.set_start_key(region.get_start_key().to_vec());
            send_msg.set_end_key(region.get_end_key().to_vec());
        }

        send_msg.set_message(msg);

        if let Err(e) = trans.send(send_msg) {
            let ec = e.error_code();
            // We use metrics to observe failure on production.
            debug!(
                "failed to send msg to other peer";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "target_peer_id" => to_peer_id,
                "target_store_id" => to_store_id,
                "err" => ?e,
                "error_code" => %ec,
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
            return false;
        }
        true
    }

    /// Steps the raft message.
    pub fn step(&mut self, mut m: eraftpb::Message) -> Result<()> {
        fail_point!(
            "step_message_3_1",
            self.peer.get_store_id() == 3 && self.region_id == 1,
            |_| Ok(())
        );
        if self.is_leader() && m.get_from() != raft::INVALID_ID {
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
            /// TODO: ctx.coprocessor_host.on_step_read_index(&mut m);
            /// Must use the commit index of `PeerStorage` instead of the commit index
            /// in raft-rs which may be greater than the former one.
            /// For more details, see the annotations above `on_leader_commit_idx_changed`.
            let index = self.get_store().commit_index();
            /// Check if the log term of this index is equal to current term, if so,
            /// this index can be used to reply the read index request if the leader holds
            /// the lease. Please also take a look at raft-rs.
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
            }
        }
        self.raft_group.step(m)?;
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
    pub fn collect_down_peers(&self, max_duration: Duration) -> Vec<PeerStats> {
        let mut down_peers = Vec::new();
        for p in self.region().get_peers() {
            if p.get_id() == self.peer.get_id() {
                continue;
            }
            if let Some(instant) = self.peer_heartbeats.get(&p.get_id()) {
                if instant.elapsed() >= max_duration {
                    let mut stats = PeerStats::new();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(instant.elapsed().as_secs());
                    down_peers.push(stats);
                }
            }
        }
        down_peers
    }

    /// Collects all pending peers and update `peers_start_pending_time`.
    pub fn collect_pending_peers(&mut self) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group.status();
        let truncated_idx = self.get_store().truncated_index();

        if status.progress.is_none() {
            return pending_peers;
        }

        /// TODO: add metrics
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

    pub fn check_stale_state(&mut self, cfg: &Config) -> StaleState {
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
            Some(instant) if instant.elapsed() >= cfg.max_leader_missing_duration.0 => {
                // Resets the `leader_missing_time` to avoid sending the same tasks to
                // PD worker continuously during the leader missing timeout.
                self.leader_missing_time = Instant::now().into();
                StaleState::ToValidate
            }
            Some(instant)
                if instant.elapsed() >= cfg.abnormal_leader_missing_duration.0 && !naive_peer =>
            {
                // A peer is considered as in the leader missing state
                // if it's initialized but is isolated from its leader or
                // something bad happens that the raft group can not elect a leader.
                StaleState::LeaderMissing
            }
            _ => StaleState::Valid,
        }
    }

    fn on_role_changed(&mut self, ctx: &mut RaftContext, ready: &Ready) {
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
            // TODO: it may possible that only the `leader_id` change and the role
            // didn't change
            ctx.global.coprocessor_host
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
                    // Stop updating `safe_ts`
                    self.read_progress.discard();
                }
            }
        }
    }

    fn on_leader_changed(
        &mut self,
        ctx: &mut RaftContext,
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

        self.read_progress
            .update_leader_info(leader_id, term, self.region());

        let mut meta = ctx.store_meta.lock().unwrap();
        meta.leaders.insert(self.region_id, (term, leader_id));
    }

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

    pub fn heartbeat_pd(&mut self, ctx: &RaftContext) {
        let task = PdTask::Heartbeat(HeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(ctx.cfg.max_peer_down_duration.0),
            pending_peers: self.collect_pending_peers(),
            written_bytes: self.peer_stat.written_bytes,
            written_keys: self.peer_stat.written_keys,
            written_query_stats: self.peer_stat.written_query_stats.clone(),
            approximate_size: self.peer_stat.approximate_size,
            approximate_keys: self.peer_stat.approximate_size / 256, // TODO: use real key number.
            replication_status: None,
        });
        if let Err(e) = ctx.global.pd_scheduler.schedule(task) {
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

    pub(crate) fn new_raft_ready(&self, ctx: &RaftContext) -> Option<CollectedReady> {
        todo!()
    }

    pub fn post_raft_ready_append(
        &mut self,
        ctx: &mut RaftContext,
        invoke_ctx: &mut InvokeContext,
        ready: &mut Ready,
    ) -> Option<ApplySnapResult> {
        todo!()
    }

    pub fn handle_raft_ready_advance(&mut self, ctx: &mut RaftContext, ready: Ready) {
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
            let msgs = light_rd.take_messages();
            self.send(&mut ctx.global.trans, msgs, &mut ctx.raft_metrics.send_message);
        }

        if !light_rd.committed_entries().is_empty() {
            self.handle_raft_committed_entries(ctx, light_rd.take_committed_entries());
        }
    }

    pub(crate) fn post_apply(&self, ctx: &mut RaftContext, apply_state: RaftApplyState, metrics: &ApplyMetrics) {
        todo!()
    }

    pub fn post_split(&mut self) {
        // Reset delete_keys_hint and size_diff_hint.
        self.delete_keys_hint = 0;
        self.size_diff_hint = 0;
    }

    /// Try to renew leader lease.
    fn maybe_renew_leader_lease(
        &mut self,
        ts: Timespec,
        ctx: &mut RaftContext,
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
            self.leader_lease
                .maybe_new_remote_lease(term)
                .map(ReadProgress::leader_lease)
        };
        if let Some(progress) = progress {
            let mut meta = ctx.global.store_meta.lock().unwrap();
            let reader = meta.readers.get_mut(&self.region_id).unwrap();
            self.maybe_update_read_progress(reader, progress);
        }
        if let Some(progress) = read_progress {
            let mut meta = ctx.global.store_meta.lock().unwrap();
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
    pub fn propose(
        &mut self,
        ctx: &mut RaftContext,
        mut cb: Callback,
        req: rlog::RaftLog,
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
            Ok(RequestPolicy::ReadLocal) | Ok(RequestPolicy::StaleRead) => {
                self.read_local(ctx, req, cb);
                return false;
            }
            Ok(RequestPolicy::ReadIndex) => return self.read_index(ctx, req.take_cmd_request(), err_resp, cb),
            Ok(RequestPolicy::ProposeNormal) => {
                let store_id = ctx.store_id();
                if disk::disk_full_precheck(store_id) || ctx.is_disk_full {
                    Err(Error::Timeout("disk full".to_owned()))
                } else {
                    self.propose_normal(ctx, req)
                }
            }
            Ok(RequestPolicy::ProposeTransferLeader) => {
                return self.propose_transfer_leader(ctx, req, cb);
            }
            Ok(RequestPolicy::ProposeConfChange) => self.propose_conf_change(ctx, req.get_cmd_request().unwrap()),
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
                    propose_time: None,
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

    fn post_propose(
        &mut self,
        ctx: &mut RaftContext,
        mut p: Proposal,
    ) {
        // Try to renew leader lease on every consistent read/write request.
        if ctx.current_time.is_none() {
            ctx.current_time = Some(monotonic_raw_now());
        }
        p.propose_time = ctx.current_time;

        self.proposals.push(p);
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

    fn ready_to_transfer_leader(
        &self,
        ctx: &mut RaftContext,
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

    fn read_local(
        &mut self,
        ctx: &mut RaftContext,
        req: rlog::RaftLog,
        cb: Callback,
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
    fn read_index(
        &mut self,
        ctx: &mut RaftContext,
        mut req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        cb: Callback,
    ) -> bool {
        if let Err(e) = self.pre_read_index() {
            debug!(
                "prevents unsafe read index";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
            ctx.raft_metrics.propose.unsafe_read_index += 1;
            cmd_resp::bind_error(&mut err_resp, e);
            cb.invoke_with_response(err_resp);
            self.should_wake_up = true;
            return false;
        }

        let now = monotonic_raw_now();
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
                        let max_lease = ctx.cfg.raft_store_max_leader_lease();
                        let is_read_index_request = req
                            .get_requests()
                            .get(0)
                            .map(|req| req.has_read_index())
                            .unwrap_or_default();
                        // A read index request or a read with addition request always needs the response of
                        // checking memory lock for async commit, so we cannot apply the optimization here
                        if !is_read_index_request
                            && read.addition_request.is_none()
                            && read.propose_time + max_lease > now
                        {
                            // A read request proposed in the current lease is found; combine the new
                            // read request to that previous one, so that no proposing needed.
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
            ctx.raft_metrics.invalid_proposal.read_index_no_leader += 1;
            // The leader may be hibernated, send a message for trying to awaken the leader.
            if self.bcast_wake_up_time.is_none()
                || self.bcast_wake_up_time.as_ref().unwrap().elapsed()
                >= Duration::from_millis(MIN_BCAST_WAKE_UP_INTERVAL)
            {
                self.bcast_wake_up_message(ctx);
                self.bcast_wake_up_time = Some(UtilInstant::now_coarse());

                let task = PdTask::QueryRegionLeader {
                    region_id: self.region_id,
                };
                if let Err(e) = ctx.pd_scheduler.schedule(task) {
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

        ctx.raft_metrics.propose.read_index += 1;

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

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
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
        if self.leader_lease.inspect(Some(now)) == LeaseState::Suspect {
            let req = RaftCmdRequest::default();
            if let Ok(Either::Left(index)) = self.propose_normal(ctx, RaftLog::Request(req)) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.post_propose(ctx, p);
            }
        }

        true
    }

    fn pre_propose(
        &self,
        raft_ctx: &mut RaftContext,
        req: &mut rlog::RaftLog,
    ) -> Result<ProposalContext> {
        // TODO:
        // raft_ctx.global.coprocessor_host.pre_propose(self.region(), req)?;
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
                self.pre_propose_prepare_merge(raft_ctx, req)?;
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
    fn propose_normal(
        &mut self,
        raft_ctx: &mut RaftContext,
        mut req: rlog::RaftLog,
    ) -> Result<Either<u64, u64>> {
        if self.pending_merge_state.is_some()
            && req.get_admin_request().get_cmd_type() != AdminCmdType::RollbackMerge
        {
            return Err(Error::ProposalInMergingMode(self.region_id));
        }

        raft_ctx.raft_metrics.propose.normal += 1;

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
        let ctx = match self.pre_propose(raft_ctx, &mut req) {
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

        if data.len() as u64 > raft_ctx.cfg.raft_entry_max_size.0 {
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

    fn execute_transfer_leader(
        &mut self,
        ctx: &mut RaftContext,
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
            // For followers whose disk is full.
            || disk::disk_full_precheck(ctx.store_id()) || ctx.is_disk_full
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
    fn propose_transfer_leader(
        &mut self,
        ctx: &mut RaftContext,
        req: rlog::RaftLog,
        cb: Callback,
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
    fn propose_conf_change(
        &mut self,
        ctx: &mut RaftContext,
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
    fn propose_conf_change_internal(
        &mut self,
        ctx: &mut RaftContext,
        change_peer: impl ChangePeerI,
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


    fn handle_read(
        &self,
        ctx: &mut RaftContext,
        req: rlog::RaftLog,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> ReadResponse {
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
        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
            let safe_ts = self.read_progress.safe_ts();
            if safe_ts < read_ts {
                warn!(
                    "read rejected by safe timestamp";
                    "safe ts" => safe_ts,
                    "read ts" => read_ts,
                    "tag" => &self.tag
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

        let mut resp = ctx.execute(&req, &Arc::new(region), read_index, None);
        if let Some(snap) = resp.snapshot.as_mut() {
            snap.max_ts_sync_status = Some(self.max_ts_sync_status.clone());
        }
        resp.txn_extra_op = self.txn_extra_op.load();
        cmd_resp::bind_term(&mut resp.response, self.term());
        resp
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

pub trait RequestInspector {
    /// Has the current term been applied?
    fn has_applied_to_current_term(&mut self) -> bool;
    /// Inspects its lease.
    fn inspect_lease(&mut self) -> LeaseState;

    /// Inspect a request, return a policy that tells us how to
    /// handle the request.
    fn inspect(&mut self, req: &rlog::RaftLog) -> RequestPolicy {
        if let Some(cmd) = req.get_cmd_request() {
            if apply::is_conf_change_cmd(cmd) {
                return RequestPolicy::ProposeConfChange;
            }
            if get_transfer_leader_cmd(req).is_some() {
                return RequestPolicy::ProposeTransferLeader;
            }
            let mut has_read = false;
            let mut has_non_read = false;
            for r in cmd.get_requests() {
                match r.get_cmd_type() {
                    CmdType::Get | CmdType::Snap | CmdType::ReadIndex => has_read = true,
                    _ => {
                        has_non_read = true;
                    }
                }
            }
            if has_read && has_non_read {
                return RequestPolicy::ProposeNormal;
            }
            if cmd.get_header().get_read_quorum() {
                return RequestPolicy::ReadIndex;
            }
        } else {
            return RequestPolicy::ProposeNormal;
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return RequestPolicy::ReadIndex;
        }

        // Local read should be performed, if and only if leader is in lease.
        // None for now.
        match self.inspect_lease() {
            LeaseState::Valid => RequestPolicy::ReadLocal,
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(RequestPolicy::ReadIndex)
            }
        }
    }
}

impl RequestInspector for Peer {
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

fn get_transfer_leader_cmd(msg: &rlog::RaftLog) -> Option<&TransferLeaderRequest> {
    let msg = msg.get_cmd_request();
    if msg.is_none() {
        return None;
    }
    let msg = msg.unwrap();
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_transfer_leader() {
        return None;
    }

    Some(req.get_transfer_leader())
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_run() {
        println!("I'll run")
    }
}
