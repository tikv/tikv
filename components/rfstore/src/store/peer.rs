// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::errors::*;
use bitflags::bitflags;
use collections::{HashMap, HashSet};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::disk_usage::DiskUsage;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::PeerRole;
use kvproto::pdpb::PeerStats;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminResponse, BatchSplitRequest, ChangePeerRequest, CmdType, CustomRequest,
    RaftCmdRequest, RaftCmdResponse, TransferLeaderRequest, TransferLeaderResponse,
};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::*;
use protobuf::Message;
use raft;
use raft::{
    Changer, LightReady, ProgressState, ProgressTracker, RawNode, Ready, SnapshotStatus, StateRole,
    Storage, INVALID_ID,
};
use raft_proto::eraftpb::{ConfChangeType, Entry, MessageType};
use raft_proto::*;
use raftstore::coprocessor;
use raftstore::coprocessor::RoleChange;
use raftstore::store::util::{
    admin_cmd_epoch_lookup, is_initial_msg, is_region_initialized, AdminCmdEpochState, ChangePeerI,
    ConfChangeKind, Lease, LeaseState,
};
use raftstore::store::{local_metrics::*, metrics::*, QueryStats, TxnExt};
use std::cell::RefCell;
use std::cmp;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tikv_util::time::{duration_to_sec, monotonic_raw_now, InstantExt};
use tikv_util::worker::Scheduler;
use tikv_util::{box_err, debug, error, info, warn, Either};
use time::Timespec;
use uuid::Uuid;

const SHRINK_CACHE_CAPACITY: usize = 64;
const MAX_COMMITTED_SIZE_PER_READY: u64 = 16 * 1024 * 1024;
pub(crate) const PENDING_CONF_CHANGE_ERR_MSG: &str = "pending conf change";

/// The returned states of the peer after checking whether it is stale
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StaleState {
    Valid,
    ToValidate,
    LeaderMissing,
}

pub(crate) fn notify_stale_req(term: u64, cb: Callback) {
    let bt = std::backtrace::Backtrace::force_capture();
    info!("notify stale req backtrace {:?}", bt);
    let resp = cmd_resp::err_resp(Error::StaleCommand, term);
    cb.invoke_with_response(resp);
}

pub(crate) fn notify_req_region_removed(region_id: u64, cb: Callback) {
    let region_not_found = Error::RegionNotFound(region_id, None);
    let resp = cmd_resp::new_error(region_not_found);
    cb.invoke_with_response(resp);
}

#[derive(Default)]
pub(crate) struct ProposalQueue {
    queue: VecDeque<Proposal>,
}

impl ProposalQueue {
    fn find_propose_time(&self, term: u64, index: u64) -> Option<Timespec> {
        self.queue
            .binary_search_by_key(&(term, index), |p: &Proposal| (p.term, p.index))
            .ok()
            .and_then(|i| self.queue[i].propose_time)
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
    fn find_proposal(
        &mut self,
        tag: RegionIDVer,
        term: u64,
        index: u64,
        current_term: u64,
    ) -> Option<Proposal> {
        while let Some(p) = self.pop(term, index) {
            if p.term == term {
                if p.index == index {
                    return if p.cb.is_none() { None } else { Some(p) };
                } else {
                    panic!(
                        "{} unexpected callback at term {}, found index {}, expected {}",
                        tag, term, p.index, index
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
        const PRE_PROCESS    = 0b0000_1000;
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

#[derive(Default)]
struct CmdEpochChecker {
    // Although it's a deque, because of the characteristics of the settings from `admin_cmd_epoch_lookup`,
    // the max size of admin cmd is 2, i.e. split/merge and change peer.
    proposed_admin_cmd: VecDeque<ProposedAdminCmd>,
    term: u64,
}

impl CmdEpochChecker {
    fn maybe_update_term(&mut self, term: u64) {
        assert!(term >= self.term);
        if term > self.term {
            self.term = term;
            for cmd in self.proposed_admin_cmd.drain(..) {
                for cb in cmd.cbs {
                    notify_stale_req(term, cb);
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
                    notify_stale_req(self.term, cb);
                }
            }
        }
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
    pub fn has(&self, peer_id: u64) -> bool {
        !self.peers.is_empty() && self.peers.contains_key(&peer_id)
    }
    pub fn get(&self, peer_id: u64) -> Option<DiskUsage> {
        self.peers.get(&peer_id).map(|x| x.0)
    }
}

pub(crate) struct Peer {
    /// The ID of the Region which this Peer belongs to.
    pub(crate) region_id: u64,

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

    // Index of last scheduled committed raft log.
    pub(crate) last_applying_idx: u64,
    pub(crate) last_compacted_index: u64,
    // The index of the latest urgent proposal index.
    pub(crate) last_urgent_proposal_idx: u64,
    // The index of the latest committed split command.
    pub(crate) last_committed_split_idx: u64,

    pub(crate) pending_remove: bool,

    /// Record the instants of peers being added into the configuration.
    /// Remove them after they are not pending any more.
    pub peers_start_pending_time: Vec<(u64, Instant)>,

    /// A inaccurate cache about which peer is marked as down.
    down_peer_ids: Vec<u64>,

    pub(crate) need_campaign: bool,

    leader_missing_time: Option<Instant>,
    leader_lease: Lease,

    pub(crate) peer_stat: PeerStat,

    /// Transaction extensions related to this peer.
    pub txn_ext: Arc<TxnExt>,

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
    cmd_epoch_checker: CmdEpochChecker,

    pub(crate) scheduled_change_sets: VecDeque<u64>,
    pub(crate) prepared_change_sets: HashMap<u64, kvengine::ChangeSet>,

    /// lead_transferee if the peer is in a leadership transferring.
    pub lead_transferee: u64,
}

impl Peer {
    pub fn new(
        store_id: u64,
        cfg: &Config,
        engines: Engines,
        region: &metapb::Region,
        peer: metapb::Peer,
    ) -> Result<Peer> {
        if peer.get_id() == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let ps = PeerStorage::new(engines, region.clone(), peer.get_id())?;
        let first = ps.first_index();
        let truncated = ps.truncated_index();
        let truncated_term = ps.truncated_term();
        let last = ps.last_index();
        let commit = ps.commit_index();

        let applied_index = ps.applied_index();
        debug!(
            "new peer storage first:{:?} truncated:{:?} t_term:{:?} last:{:?}, applied: {:?}, commit: {:?}",
            first, truncated, truncated_term, last, applied_index, commit,
        );

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
            region_id: region.get_id(),
            raft_group,
            proposals: ProposalQueue::default(),
            pending_reads: Default::default(),
            peer_cache: RefCell::new(HashMap::default()),
            peer_heartbeats: HashMap::default(),
            peers_start_pending_time: vec![],
            down_peer_ids: vec![],
            pending_remove: false,
            leader_missing_time: Some(Instant::now()),
            last_applying_idx: applied_index,
            last_compacted_index: 0,
            last_urgent_proposal_idx: u64::MAX,
            last_committed_split_idx: 0,
            leader_lease: Lease::new(
                cfg.raft_store_max_leader_lease(),
                cfg.renew_leader_lease_advance_duration(),
            ),
            peer_stat: PeerStat::default(),
            txn_ext: Arc::new(TxnExt::default()),
            max_ts_sync_status: Arc::new(Default::default()),
            cmd_epoch_checker: Default::default(),
            need_campaign: false,
            scheduled_change_sets: Default::default(),
            prepared_change_sets: Default::default(),
            lead_transferee: raft::INVALID_ID,
        };
        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(peer)
    }

    pub(crate) fn tag(&self) -> RegionIDVer {
        self.get_store().tag()
    }

    /// Register self to apply_scheduler so that the peer is then usable.
    /// Also trigger `RegionChangeEvent::Create` here.
    pub fn activate(&self, msgs: &mut ApplyMsgs) {
        let msg = ApplyMsg::Registration(MsgRegistration::new(self));
        msgs.msgs.push(msg);
    }

    pub(crate) fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub(crate) fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    pub(crate) fn maybe_destroy(&mut self) -> bool {
        if self.pending_remove {
            info!(
                "is being destroyed, skip";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return false;
        }
        if self.is_applying_snapshot() {
            info!(
                "stale peer is applying snapshot, will destroy next time";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return false;
        }
        self.pending_remove = true;
        true
    }

    pub(crate) fn destroy(&mut self, raft_wb: &mut rfengine::WriteBatch) -> Result<()> {
        let t = Instant::now();

        let region = self.region().clone();
        info!(
            "begin to destroy";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );
        self.mut_store().clear_meta(raft_wb);
        self.pending_reads.clear_all(Some(region.get_id()));

        for Proposal { cb, .. } in self.proposals.queue.drain(..) {
            notify_req_region_removed(region.get_id(), cb);
        }
        info!(
            "peer destroy itself";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "takes" => ?t.saturating_elapsed(),
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
        region: metapb::Region,
    ) {
        if self.region().get_region_epoch().get_version() < region.get_region_epoch().get_version()
        {
            // Epoch version changed, disable read on the localreader for this region.
            self.leader_lease.expire_remote_lease();
        }
        self.mut_store().set_region(region);

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

    pub fn ready_to_handle_pending_snapshot(&self) -> bool {
        // If apply worker is still working, written apply state may be overwritten
        // by apply worker. So we have to wait here.
        // Please note that commit_index can't be used here. When applying a snapshot,
        // a stale heartbeat can make the leader think follower has already applied
        // the snapshot, and send remaining log entries, which may increase commit_index.
        self.last_applying_idx == self.get_store().applied_index()
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
    pub fn send_raft_messages(&mut self, ctx: &mut RaftContext, msgs: Vec<RaftMessage>) {
        for msg in msgs {
            let msg_type = msg.get_message().get_msg_type();
            if msg_type == MessageType::MsgTimeoutNow && self.is_leader() {
                // After a leader transfer procedure is triggered, the lease for
                // the old leader may be expired earlier than usual, since a new leader
                // may be elected and the old leader doesn't step down due to
                // network partition from the new leader.
                // For lease safety during leader transfer, transit `leader_lease`
                // to suspect.
                self.leader_lease.suspect(monotonic_raw_now());
            }

            let to_peer_id = msg.get_to_peer().get_id();
            let to_store_id = msg.get_to_peer().get_store_id();

            debug!(
                "send raft msg";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "msg_type" => ?msg_type,
                "msg_size" => msg.get_message().compute_size(),
                "to" => to_peer_id,
                "disk_usage" => ?msg.get_disk_usage(),
            );

            if let Err(e) = ctx.global.trans.send(msg) {
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

    fn build_raft_message(&mut self, msg: eraftpb::Message) -> Option<RaftMessage> {
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
                return None;
            }
        };

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

        Some(send_msg)
    }

    #[inline]
    pub fn build_raft_messages(
        &mut self,
        _ctx: &RaftContext,
        msgs: Vec<eraftpb::Message>,
    ) -> Vec<RaftMessage> {
        let mut raft_msgs = Vec::with_capacity(msgs.len());
        for msg in msgs {
            if let Some(m) = self.build_raft_message(msg) {
                raft_msgs.push(m);
            }
        }
        raft_msgs
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
            // TODO: ctx.coprocessor_host.on_step_read_index(&mut m);
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
                if instant.saturating_elapsed() >= max_duration {
                    let mut stats = PeerStats::new();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(instant.saturating_elapsed().as_secs());
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

        // TODO: add metrics
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

    #[allow(dead_code)]
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
            Some(instant) if instant.saturating_elapsed() >= cfg.max_leader_missing_duration.0 => {
                // Resets the `leader_missing_time` to avoid sending the same tasks to
                // PD worker continuously during the leader missing timeout.
                self.leader_missing_time = Instant::now().into();
                StaleState::ToValidate
            }
            Some(instant)
                if instant.saturating_elapsed() >= cfg.abnormal_leader_missing_duration.0
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

    pub fn require_updating_max_ts(&self, pd_scheduler: &Scheduler<PdTask>) {
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
                    self.require_updating_max_ts(&ctx.global.pd_scheduler);
                    self.heartbeat_pd(ctx);
                }
                StateRole::Follower => {
                    self.leader_lease.expire();
                }
                _ => {}
            }
            // TODO: it may possible that only the `leader_id` change and the role
            // didn't change
            ctx.global.coprocessor_host.on_role_change(
                self.region(),
                RoleChange {
                    state: ss.raft_state,
                    leader_id: ss.leader_id,
                    prev_lead_transferee: self.lead_transferee,
                    vote: self.raft_group.raft.vote,
                },
            );
            self.cmd_epoch_checker.maybe_update_term(self.term());
        }
        self.lead_transferee = self.raft_group.raft.lead_transferee.unwrap_or_default();
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

    pub(crate) fn handle_raft_ready(
        &mut self,
        ctx: &mut RaftContext,
        mut store_meta: Option<&mut StoreMeta>,
    ) {
        if self.get_store().is_applying_snapshot() {
            // If we continue to handle all the messages, it may cause too many messages because
            // leader will send all the remaining messages to this follower, which can lead
            // to full message queue under high load.
            return;
        }
        if !self.raft_group.has_ready() {
            return;
        }
        if self.has_pending_snapshot() {
            if !self.ready_to_handle_pending_snapshot() {
                return;
            }
            if store_meta.is_none() {
                ctx.global
                    .router
                    .send_store(StoreMsg::SnapshotReady(self.region_id));
                return;
            }
        }
        let mut ready = self.raft_group.ready();
        self.on_role_changed(ctx, &ready);
        ctx.raft_metrics.ready.has_ready_region += 1;
        ctx.raft_metrics.ready.commit += ready.committed_entries().len() as u64;
        ctx.raft_metrics.ready.append += ready.entries().len() as u64;
        ctx.raft_metrics.ready.message += ready.messages().len() as u64;

        // TODO(x) on leader commit index change.

        if !ready.messages().is_empty() {
            assert!(self.is_leader());
            let raft_msgs = self.build_raft_messages(ctx, ready.take_messages());
            self.send_raft_messages(ctx, raft_msgs);
        }

        self.apply_reads(ctx, &ready);

        let new_role = ready.ss().map(|ss| ss.raft_state);
        self.handle_raft_committed_entries(ctx, ready.take_committed_entries(), new_role);
        if let Some(snap_res) = self
            .mut_store()
            .handle_raft_ready(ctx, &mut ready, &mut store_meta)
        {
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
            };
            if !self.update_store_meta_for_snap(ctx, snap_res, store_meta.unwrap()) {
                return;
            }
            self.activate(&mut ctx.apply_msgs);
        }
        if let Some(ss) = ready.ss() {
            if ss.raft_state == raft::StateRole::Leader {
                self.heartbeat_pd(ctx)
            }
        }
        let persist_messages = self.build_raft_messages(ctx, ready.take_persisted_messages());
        ctx.persist_readies.push(PersistReady {
            region_id: self.region_id,
            ready_number: ready.number(),
            peer_id: self.peer_id(),
            commit_idx: self.get_store().commit_index(),
            raft_messages: persist_messages,
        });
        self.raft_group.advance_append_async(ready)
    }

    pub(crate) fn handle_raft_committed_entries(
        &mut self,
        ctx: &mut RaftContext,
        committed_entries: Vec<Entry>,
        new_role: Option<raft::StateRole>,
    ) {
        if committed_entries.is_empty() && new_role.is_none() {
            return;
        }
        fail_point!(
            "before_leader_handle_committed_entries",
            self.is_leader(),
            |_| ()
        );
        let tag = self.tag();

        assert!(
            !self.is_applying_snapshot(),
            "{} is applying snapshot when it is ready to handle committed entries",
            tag
        );
        // Leader needs to update lease.
        let mut lease_to_be_updated = self.is_leader();
        for entry in committed_entries.iter().rev() {
            // raft meta is very small, can be ignored.
            if lease_to_be_updated {
                let propose_time = self
                    .proposals
                    .find_propose_time(entry.get_term(), entry.get_index());
                if let Some(propose_time) = propose_time {
                    // We must renew current_time because this value may be created a long time ago.
                    // If we do not renew it, this time may be smaller than propose_time of a command,
                    // which was proposed in another thread while this thread receives its AppendEntriesResponse
                    // and is ready to calculate its commit-log-duration.
                    ctx.current_time.replace(monotonic_raw_now());
                    ctx.raft_metrics.commit_log.observe(duration_to_sec(
                        (ctx.current_time.unwrap() - propose_time).to_std().unwrap(),
                    ));
                    self.maybe_renew_leader_lease(propose_time, ctx, None);
                    lease_to_be_updated = false;
                }
            }
        }
        for entry in committed_entries.iter() {
            self.preprocess_committed_entry(ctx, entry);
        }
        if let Some(last_entry) = committed_entries.last() {
            self.last_applying_idx = last_entry.get_index();
            if self.last_applying_idx >= self.last_urgent_proposal_idx {
                // Urgent requests are flushed, make it lazy again.
                self.raft_group.skip_bcast_commit(true);
                self.last_urgent_proposal_idx = u64::MAX;
            }
        }
        let cbs = if !self.proposals.is_empty() {
            let current_term = self.term();
            let cbs = committed_entries
                .iter()
                .filter_map(|e| {
                    self.proposals
                        .find_proposal(tag, e.get_term(), e.get_index(), current_term)
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
        let apply_msg = ApplyMsg::Apply(MsgApply {
            region_id: self.region_id,
            term: self.term(),
            entries: committed_entries,
            new_role,
            cbs,
        });
        ctx.apply_msgs.msgs.push(apply_msg);
        fail_point!("after_send_to_apply_1003", self.peer_id() == 1003, |_| {});
    }

    pub(crate) fn preprocess_committed_entry(&mut self, ctx: &mut RaftContext, entry: &Entry) {
        if let Some(cmd) = get_preprocess_cmd(entry) {
            if cmd.has_custom_request() {
                self.preprocess_change_set(ctx, entry, cmd.get_custom_request());
            } else {
                self.preprocess_pending_splits(ctx, entry, cmd.get_admin_request().get_splits());
            }
        }
    }

    pub(crate) fn preprocess_change_set(
        &mut self,
        ctx: &mut RaftContext,
        entry: &Entry,
        custom_req: &CustomRequest,
    ) {
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        let mut cs = custom_log.get_change_set().unwrap();
        cs.set_sequence(entry.get_index());
        let region_id = self.region_id;
        let tag = self.tag();
        let shard_meta = self.mut_store().mut_engine_meta();
        let mut rejected = false;
        if shard_meta.ver != cs.get_shard_ver() {
            rejected = true;
            warn!(
                "shard meta not match";
                "region" => tag,
                "shard_meta_ver" => shard_meta.ver,
                "version" => cs.get_shard_ver(),
            );
        } else if shard_meta.is_duplicated_change_set(&mut cs) {
            rejected = true;
            warn!(
                "shard meta is duplicated change set";
                "region" => tag,
                "seq" => cs.get_sequence(),
            );
        }
        if rejected {
            return;
        }
        shard_meta.apply_change_set(&cs);
        info!(
            "shard meta apply change set {:?}", &cs;
            "region" => tag,
        );
        ctx.raft_wb
            .set_state(region_id, KV_ENGINE_META_KEY, &shard_meta.marshal());
        self.schedule_prepare_change_set(ctx, cs);
    }

    pub(crate) fn preprocess_pending_splits(
        &mut self,
        ctx: &mut RaftContext,
        entry: &Entry,
        splits: &BatchSplitRequest,
    ) {
        self.last_committed_split_idx = entry.index;
        let regions = split_gen_new_region_metas(self.region(), splits).unwrap();
        let split = build_split_pb(self.region(), &regions, entry.term);
        let shard_meta = self.mut_store().mut_engine_meta();
        let new_metas = shard_meta.apply_split(&split, entry.index, RAFT_INIT_LOG_INDEX);
        let mut cs = shard_meta.to_change_set();
        cs.set_split(split);
        cs.set_sequence(entry.index);
        ctx.apply_msgs.msgs.push(ApplyMsg::PendingSplit(cs));
        for (i, new_meta) in new_metas.iter().enumerate() {
            let new_region = &regions[i];
            write_peer_state(&mut ctx.raft_wb, new_region);
            if new_meta.id == self.region_id {
                ctx.raft_wb
                    .set_state(new_meta.id, KV_ENGINE_META_KEY, &new_meta.marshal());
                let store = self.mut_store();
                // The raft state key changed when region version change, we need to set it here.
                store.write_raft_state(ctx);
                store.shard_meta = Some(new_meta.clone());
                store.initial_flushed = false;
            } else {
                let raft = &ctx.global.engines.raft;
                if raft.get_state(new_meta.id, KV_ENGINE_META_KEY).is_none() {
                    ctx.raft_wb
                        .set_state(new_meta.id, KV_ENGINE_META_KEY, &new_meta.marshal());
                }
            }
        }
    }

    pub(crate) fn post_apply(&mut self, ctx: &mut RaftContext, apply_result: &MsgApplyResult) {
        let apply_state = apply_result.apply_state;
        if self.get_store().is_applying_snapshot() {
            panic!("{} should not applying snapshot.", self.tag());
        }

        let applied_index = apply_state.applied_index;
        let applied_index_term = apply_state.applied_index_term;
        self.raft_group.advance_apply_to(applied_index);

        self.cmd_epoch_checker.advance_apply(
            applied_index,
            self.term(),
            self.raft_group.store().region(),
        );

        let progress_to_be_updated = self.mut_store().applied_index_term() != applied_index_term;
        self.mut_store().set_applied_state(apply_state);
        self.peer_stat.written_keys += apply_result.metrics.written_keys;
        self.peer_stat.written_bytes += apply_result.metrics.written_bytes;
        if !self.is_leader() {
            // TODO(x) post_pending_read_index_on_replica
        } else if self.ready_to_handle_read() {
            while let Some(mut read) = self.pending_reads.pop_front() {
                self.response_read(&mut read, ctx, false);
            }
        }
        self.pending_reads.gc();

        // Only leaders need to update applied_index_term.
        if progress_to_be_updated && self.is_leader() {
            if applied_index_term == self.term() {
                ctx.global
                    .coprocessor_host
                    .on_applied_current_term(StateRole::Leader, self.region());
            }
            let progress = ReadProgress::applied_index_term(applied_index_term);
            let mut reader = ctx.global.readers.get_mut(&self.region_id).unwrap();
            self.maybe_update_read_progress(reader.value_mut(), progress);
        }
    }

    fn response_read(
        &self,
        read: &mut ReadIndexRequest,
        ctx: &mut RaftContext,
        replica_read: bool,
    ) {
        debug!(
            "handle reads with a read index";
            "request_id" => ?read.id,
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds().len() as i64);
        for (req, cb, mut read_index) in read.take_cmds().drain(..) {
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
                notify_stale_req(term, cb);
            }
        }
    }

    /// Responses to the ready read index request on the replica, the replica is not a leader.
    fn post_pending_read_index_on_replica(&mut self, ctx: &mut RaftContext) {
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
                    "peer_id" => self.peer.get_id(),
                );
                RAFT_READ_INDEX_PENDING_COUNT.sub(1);
                self.send_read_command(ctx, read_cmd);
                continue;
            }

            assert!(read.read_index.is_some());
            let is_read_index_request = read.cmds().len() == 1
                && read.cmds()[0].0.get_requests().len() == 1
                && read.cmds()[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

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

    fn send_read_command(&self, ctx: &mut RaftContext, read_cmd: RaftCommand) {
        ctx.global
            .router
            .send(self.region_id, PeerMsg::RaftCommand(read_cmd));
    }

    fn apply_reads(&mut self, ctx: &mut RaftContext, ready: &Ready) {
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
            // `propose_time` is a placeholder, here cares about `Suspect` only,
            // and if it is in `Suspect` phase, the actual timestamp is useless.
            if self.leader_lease.inspect(Some(propose_time)) == LeaseState::Suspect {
                return;
            }
            self.maybe_renew_leader_lease(propose_time, ctx, None);
        }
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
        // TODO(x) && !self.is_merging()
    }

    fn ready_to_handle_unsafe_replica_read(&self, read_index: u64) -> bool {
        // Wait until the follower applies all values before the read. There is still a
        // problem if the leader applies fewer values than the follower, the follower read
        // could get a newer value, and after that, the leader may read a stale value,
        // which violates linearizability.
        self.get_store().applied_index() >= read_index
            // If it is in pending merge state(i.e. applied PrepareMerge), the data may be stale.
            // TODO: Add a test to cover this case
            // TODO(x) && self.pending_merge_state.is_none()
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
        false // TODO(x)
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
        if progress.is_some() || read_progress.is_some() {
            let mut reader = ctx.global.readers.get_mut(&self.region_id).unwrap();
            if let Some(progress) = progress {
                self.maybe_update_read_progress(reader.value_mut(), progress);
            }
            if let Some(read_progress) = read_progress {
                self.maybe_update_read_progress(reader.value_mut(), read_progress);
            }
        }
    }

    pub(crate) fn update_store_meta_for_snap(
        &mut self,
        ctx: &mut RaftContext,
        apply_result: ApplySnapResult,
        meta: &mut StoreMeta,
    ) -> bool {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;
        info!(
            "snapshot is applied";
            "region_id" => self.region_id,
            "peer_id" => self.peer_id(),
            "region" => ?region,
        );

        // TODO(x) update commit group

        if meta.pending_new_regions.contains_key(&self.region_id) {
            // The region is about to be created by split, do not update store meta.
            return false;
        }
        if let Some(meta_region) = meta.regions.get(&self.region_id) {
            if !is_region_initialized(&prev_region) && is_region_initialized(meta_region) {
                // The region is updated by split, the peer is already replaced.
                return false;
            }
        }
        debug!("meta reader insert read delegate {}", self.region_id);
        meta.readers
            .insert(self.region_id, ReadDelegate::from_peer(self));

        if is_region_initialized(&prev_region) {
            info!(
                "region changed after applying snapshot";
                "region_id" => self.region_id,
                "peer_id" => self.peer_id(),
                "prev_region" => ?prev_region,
                "region" => ?region,
            );
            let prev = meta.region_ranges.remove(&raw_end_key(&prev_region));
            if prev != Some(region.get_id()) {
                panic!(
                    "{} meta corrupted, expect {:?} got {:?}",
                    self.tag(),
                    prev_region,
                    prev,
                );
            }
        }

        if let Some(r) = meta
            .region_ranges
            .insert(raw_end_key(&region), region.get_id())
        {
            panic!("{} unexpected region {:?}", self.tag(), r);
        }
        let prev = meta.regions.insert(region.get_id(), region);
        assert_eq!(prev, Some(prev_region));
        self.schedule_prepare_change_set(ctx, apply_result.change_set);
        true
    }

    fn schedule_prepare_change_set(&mut self, ctx: &mut RaftContext, cs: kvenginepb::ChangeSet) {
        let kv = ctx.global.engines.kv.clone();
        let router = ctx.global.router.clone();
        self.scheduled_change_sets.push_back(cs.sequence);
        let is_leader = self.is_leader();
        std::thread::spawn(move || {
            let id = cs.shard_id;
            let res = kv.prepare_change_set(cs, !is_leader);
            router.send(id, PeerMsg::PrepareChangeSetResult(res));
        });
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

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
    pub fn propose(
        &mut self,
        ctx: &mut RaftContext,
        mut cb: Callback,
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
            RequestPolicy::ReadLocal | RequestPolicy::StaleRead => {
                self.read_local(ctx, req, cb);
                return false;
            }
            RequestPolicy::ReadIndex => {
                return self.read_index(ctx, req, err_resp, cb);
            }
            RequestPolicy::ProposeNormal => {
                // TODO: check disk usage.
                self.propose_normal(ctx, req)
            }
            RequestPolicy::ProposeTransferLeader => {
                return self.propose_transfer_leader(ctx, req, cb);
            }
            RequestPolicy::ProposeConfChange => self.propose_conf_change(ctx, &req),
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
                self.push_proposal(ctx, p);
                true
            }
        }
    }

    fn push_proposal(&mut self, ctx: &mut RaftContext, mut p: Proposal) {
        // Try to renew leader lease on every consistent read/write request.
        if ctx.current_time.is_none() {
            ctx.current_time = Some(monotonic_raw_now());
        }
        p.propose_time = ctx.current_time;

        self.proposals.push(p);
    }

    // TODO: set higher election priority of voter/incoming voter than demoting voter
    /// Validate the `ConfChange` requests and check whether it's safe to
    /// propose these conf change requests.
    /// It's safe iff at least the quorum of the Raft group is still healthy
    /// right after all conf change is applied.
    /// If 'allow_remove_leader' is false then the peer to be removed should
    /// not be the leader.
    fn check_conf_change(
        &mut self,
        ctx: &mut RaftContext,
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
                    self.tag()
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
                        self.tag(),
                        cp
                    ));
                }
                (ConfChangeType::RemoveNode, _)
                | (ConfChangeType::AddNode, PeerRole::Voter)
                | (ConfChangeType::AddLearnerNode, PeerRole::Learner) => {}
                _ => {
                    return Err(box_err!(
                        "{} invalid conf change request: {:?}",
                        self.tag(),
                        cp
                    ));
                }
            }

            if !check_dup.insert(peer.get_id()) {
                return Err(box_err!(
                    "{} invalid conf change request, have multiple commands for the same peer {}",
                    self.tag(),
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
                    self.tag()
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
                self.tag()
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

        Err(box_err!(
            "{} unsafe to perform conf change {:?}, before: {:?}, after: {:?}, truncated index {}, promoted commit index {}",
            self.tag(),
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

    pub(crate) fn transfer_leader(&mut self, peer: &metapb::Peer) {
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

    pub(crate) fn ready_to_transfer_leader(
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

    fn read_local(&mut self, ctx: &mut RaftContext, req: RaftCmdRequest, cb: Callback) {
        debug!("read local");
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
                    self.tag()
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
        Ok(())
    }

    /// `ReadIndex` requests could be lost in network, so on followers commands could queue in
    /// `pending_reads` forever. Sending a new `ReadIndex` periodically can resolve this.
    pub fn retry_pending_reads(&mut self, raft_election_timeout_ticks: usize) {
        if self.is_leader()
            || !self
                .pending_reads
                .check_needs_retry(raft_election_timeout_ticks)
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
            cmd_resp::bind_error(&mut err_resp, Error::NotLeader(self.region_id, None));
            cb.invoke_with_response(err_resp);
            return false;
        }

        // Should we call pre_propose here?
        let last_pending_read_count = self.raft_group.raft.pending_read_count();
        let last_ready_read_count = self.raft_group.raft.ready_read_count();

        ctx.raft_metrics.propose.read_index += 1;

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
            notify_stale_req(self.term(), cb);
            return false;
        }

        let mut read = ReadIndexRequest::with_command(id, req, cb, now);
        read.addition_request = request.map(Box::new);
        self.pending_reads.push_back(read, self.is_leader());

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
            if let Ok(Either::Left(index)) = self.propose_normal(ctx, req) {
                let p = Proposal {
                    is_conf_change: false,
                    index,
                    term: self.term(),
                    cb: Callback::None,
                    propose_time: Some(now),
                    must_pass_epoch_check: false,
                };
                self.push_proposal(ctx, p);
            }
        }

        true
    }

    fn pre_propose(
        &self,
        raft_ctx: &mut RaftContext,
        req: &mut RaftCmdRequest,
    ) -> Result<ProposalContext> {
        raft_ctx
            .global
            .coprocessor_host
            .pre_propose(self.region(), req)?;
        let mut ctx = ProposalContext::empty();
        if req.has_custom_request() {
            if rlog::is_engine_meta_log(req.get_custom_request().get_data()) {
                ctx.insert(ProposalContext::PRE_PROCESS);
            }
        } else if req.has_admin_request() {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::Split | AdminCmdType::BatchSplit => {
                    if self.raft_group.raft.pending_conf_index > self.get_store().applied_index() {
                        info!("there is a pending conf change, try later"; "region" => self.tag());
                        return Err(box_err!(PENDING_CONF_CHANGE_ERR_MSG));
                    }
                    ctx.insert(ProposalContext::PRE_PROCESS);
                }
                _ => {}
            }
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
        mut req: RaftCmdRequest,
    ) -> Result<Either<u64, u64>> {
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
                self.tag(),
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
        Ok(Either::Left(propose_index))
    }

    pub(crate) fn execute_transfer_leader(
        &mut self,
        _ctx: &mut RaftContext,
        msg: &eraftpb::Message,
    ) {
        let pending_snapshot =
            self.get_store().is_applying_snapshot() || self.has_pending_snapshot();
        if pending_snapshot || msg.get_from() != self.leader_id() {
            info!(
                "reject transferring leader";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "from" => msg.get_from(),
                "pending_snapshot" => pending_snapshot,
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
        req: RaftCmdRequest,
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
        if self.raft_group.raft.pending_conf_index > self.get_store().applied_index() {
            info!(
                "there is a pending conf change, try later";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return Err(box_err!(
                "{} there is a pending conf change, try later",
                self.tag()
            ));
        }
        // Actually, according to the implementation of conf change in raft-rs, this check must be
        // passed if the previous check that `pending_conf_index` should be less than or equal to
        // `self.get_store().applied_index()` is passed.
        if self.get_store().applied_index_term() != self.term() {
            return Err(box_err!(
                "{} peer has not applied to current term, applied_term {}, current_term {}",
                self.tag(),
                self.get_store().applied_index_term(),
                self.term()
            ));
        }
        if let Some(index) = self.cmd_epoch_checker.propose_check_epoch(req, self.term()) {
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
        let cc = change_peer.to_confchange(data);
        let changes = change_peer.get_change_peers();

        self.check_conf_change(ctx, changes.as_ref(), &cc)?;

        ctx.raft_metrics.propose.conf_change += 1;
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
        req: RaftCmdRequest,
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
        let mut resp = ctx.execute(&req, &Arc::new(region), read_index, None);
        cmd_resp::bind_term(&mut resp.response, self.term());
        resp
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

pub trait RequestInspector {
    /// Has the current term been applied?
    fn has_applied_to_current_term(&mut self) -> bool;
    /// Inspects its lease.
    fn inspect_lease(&mut self) -> LeaseState;

    /// Inspect a request, return a policy that tells us how to
    /// handle the request.
    fn inspect(&mut self, req: &RaftCmdRequest) -> RequestPolicy {
        if req.has_admin_request() {
            if apply::is_conf_change_cmd(req) {
                return RequestPolicy::ProposeConfChange;
            }
            if get_transfer_leader_cmd(req).is_some() {
                return RequestPolicy::ProposeTransferLeader;
            }
            return RequestPolicy::ProposeNormal;
        }
        if req.has_custom_request() {
            return RequestPolicy::ProposeNormal;
        }
        if req.get_header().get_read_quorum() {
            return RequestPolicy::ReadIndex;
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
                RequestPolicy::ReadIndex
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

impl ReadExecutor for RaftContext {
    fn get_snapshot(&self, region_id: u64, region_ver: u64) -> Result<RegionSnapshot> {
        if let Some(snap) = self.global.engines.kv.get_snap_access(region_id) {
            if snap.get_version() == region_ver {
                return Ok(RegionSnapshot {
                    snap,
                    max_ts_sync_status: None,
                    term: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
            }
        }
        Err(Error::StaleCommand)
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

pub(crate) fn get_preprocess_cmd(entry: &eraftpb::Entry) -> Option<RaftCmdRequest> {
    if entry.entry_type != eraftpb::EntryType::EntryNormal {
        return None;
    }
    let pc = ProposalContext::from_bytes(&entry.context);
    if !pc.contains(ProposalContext::PRE_PROCESS) {
        return None;
    }
    let mut cmd = RaftCmdRequest::default();
    cmd.merge_from_bytes(&entry.data).unwrap();
    Some(cmd)
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_run() {
        println!("I'll run")
    }
}
