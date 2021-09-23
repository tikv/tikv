// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::peer_storage::InvokeContext;
use super::*;
use crate::coprocessor;
use crate::errors::*;
use crate::store::worker::{ReadDelegate, ReadProgress, RegionTask};
use bitflags::bitflags;
use byteorder::BigEndian;
use bytes::{BufMut, Bytes, BytesMut};
use error_code::ErrorCodeExt;
use fail::fail_point;
use futures::channel::mpsc::UnboundedSender;
use kvproto::metapb::PeerRole;
use kvproto::pdpb::PeerStats;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, TransferLeaderRequest};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::*;
use raft;
use raft::eraftpb::MessageType;
use raft::{RawNode, SnapshotStatus, Storage};
use raft_proto::*;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU64;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use tikv_util::time::{duration_to_sec, monotonic_raw_now, Instant};
use tikv_util::{box_err, debug, error, info, warn};
use time::Timespec;

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

pub(crate) struct ReqCallbackPair {
    pub(crate) req: raft_cmdpb::RaftCmdRequest,
    pub(crate) callback: Callback,
}

pub(crate) struct ReadIndexRequest {
    pub(crate) id: u64,
    pub(crate) cmds: Vec<ReqCallbackPair>,
    pub(crate) renew_lease_time: Instant,
}

impl ReadIndexRequest {
    pub(crate) fn new(id: u64, cmds: Vec<ReqCallbackPair>, renew_lease_time: Instant) -> Self {
        Self {
            id,
            cmds,
            renew_lease_time,
        }
    }

    pub(crate) fn binary_id(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.put_u64(self.id);
        buf.freeze()
    }
}

pub(crate) struct ReadIndexQueue {
    id_allocator: u64,
    reads: VecDeque<ReadIndexRequest>,
    ready_cnt: usize,
}

impl Default for ReadIndexQueue {
    fn default() -> ReadIndexQueue {
        ReadIndexQueue {
            id_allocator: 1,
            reads: VecDeque::new(),
            ready_cnt: 0,
        }
    }
}

impl ReadIndexQueue {
    fn pop_front(&mut self) -> Option<ReadIndexRequest> {
        self.reads.pop_front()
    }

    fn next_id(&mut self) -> u64 {
        self.id_allocator += 1;
        self.id_allocator
    }

    fn clear_uncommitted(&mut self, term: u64) {
        let mut uncommitted = self.reads.split_off(self.ready_cnt);
        for mut read in uncommitted.drain(..) {
            for pair in read.cmds.drain(..) {
                notify_stale_req(term, &pair.callback)
            }
        }
    }

    pub(crate) fn clear_all(&self, notify_removed: Option<u64>) {
        todo!()
    }
}

pub(crate) fn notify_stale_req(term: u64, cb: &Callback) {
    todo!()
}

pub(crate) fn notify_req_region_removed(region_id: u64, cb: &Callback) {
    todo!()
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
                apply::notify_stale_req(current_term, p.cb);
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
}

pub(crate) struct DestroyPeerJob {
    initialized: bool,
    region_id: u64,
    peer: metapb::Peer,
}

pub(crate) struct Peer {
    /// The ID of the Region which this Peer belongs to.
    region_id: u64,
    /// The Peer meta information.
    pub(crate) peer: metapb::Peer,
    /// The Raft state machine of this Peer.
    pub(crate) raft_group: raft::RawNode<PeerStorage>,
    /// The cache of meta information for Region's other Peers.
    peer_cache: RefCell<HashMap<u64, metapb::Peer>>,
    /// Record the last instant of each peer's heartbeat response.
    peer_heartbeats: HashMap<u64, Instant>,

    proposals: ProposalQueue,
    pending_reads: ReadIndexQueue,

    /// If it fails to send messages to leader.
    pub leader_unreachable: bool,

    // Record the instants of peers being added into the configuration.
    // Remove them after they are not pending any more.
    peers_start_pending_time: Vec<(u64, Instant)>,
    consistency_state: ConsistencyState,

    // Index of last scheduled committed raft log.
    last_applying_index: u64,
    last_compacted_index: u64,
    // The index of the latest urgent proposal index.
    last_urgent_proposal_idx: u64,
    // The index of the latest committed split command.
    last_committed_split_idx: u64,

    pending_remove: bool,

    // PendingSplit is set to true when there are Split admin command proposed.
    // It is set back to false when Split is finished.
    pending_split: bool,

    // The index of the latest committed prepare merge command.
    last_committed_prepare_merge_idx: u64,
    pending_merge_state: Option<raft_serverpb::MergeState>,
    leader_missing_time: Option<Instant>,
    leader_lease: Lease,

    // If a snapshot is being applied asynchronously, messages should not be sent.
    pending_messages: Vec<eraftpb::Message>,
    peer_stat: PeerStat,

    /// The max timestamp recorded in the concurrency manager is only updated at leader.
    /// So if a peer becomes leader from a follower, the max timestamp can be outdated.
    /// We need to update the max timestamp with a latest timestamp from PD before this
    /// peer can work.
    /// From the least significant to the most, 1 bit marks whether the timestamp is
    /// updated, 31 bits for the current epoch version, 32 bits for the current term.
    /// The version and term are stored to prevent stale UpdateMaxTimestamp task from
    /// marking the lowest bit.
    pub max_ts_sync_status: Arc<AtomicU64>,

    wait_follower_split_files: Option<MsgWaitFollowerSplitFiles>,
    followers_split_files_done: HashMap<u64, u64>,
}

impl Peer {
    pub(crate) fn new(
        store_id: u64,
        cfg: Config,
        engines: Engines,
        sched: UnboundedSender<RegionTask>,
        region: &metapb::Region,
        peer: metapb::Peer,
    ) -> Result<Peer> {
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
            max_committed_size_per_ready: MAX_COMMITTED_SIZE_PER_READY,
            ..Default::default()
        };

        let logger = slog_global::get_global().new(slog::o!("region_id" => region.get_id()));
        let raft_group = RawNode::new(&raft_cfg, ps, &logger)?;
        let mut peer = Peer {
            peer,
            region_id: region.get_id(),
            raft_group,
            peer_cache: Default::default(),
            proposals: ProposalQueue::new(tag),
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
            last_applying_index: 0,
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
            max_ts_sync_status: Arc::new(AtomicU64::new(0)),
            wait_follower_split_files: None,
            followers_split_files_done: Default::default(),
        };
        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(peer)
    }

    /// Register self to apply_scheduler so that the peer is then usable.
    /// Also trigger `RegionChangeEvent::Create` here.
    pub fn activate<T>(&self, msgs: &mut ApplyMsgs) {
        let payload = PeerMsgPayload::ApplyRegistration(MsgRegistration::new(&self));
        let msg = PeerMsg::new(self.region_id, payload);
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
            notify_req_region_removed(region.get_id(), &cb);
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
        host: &coprocessor::CoprocessorHost,
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
        if self.get_store().is_initialized() && is_initial_msg(&msg) {
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
    pub fn step<T>(&mut self, mut m: eraftpb::Message) -> Result<()> {
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

    pub fn any_new_peer_catch_up(&mut self, peer_id: u64) -> bool {
        if self.peers_start_pending_time.is_empty() {
            return false;
        }
        if !self.is_leader() {
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
        false
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

    pub fn heartbeat_pd(&mut self, pd_sched: Sender<PdTask>) {
        todo!()
    }

    fn prepare_raft_message(&self) -> RaftMessage {
        let mut send_msg = RaftMessage::default();
        send_msg.set_region_id(self.region_id);
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        send_msg.set_from_peer(self.peer.clone());
        send_msg
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

#[cfg(test)]
mod tests {

    #[test]
    fn test_run() {
        println!("I'll run")
    }
}
