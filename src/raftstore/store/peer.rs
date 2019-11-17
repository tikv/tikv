// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::collections::Bound::{Excluded, Unbounded};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{atomic, Arc};
use std::time::{Duration, Instant};
use std::{cmp, mem, u64};

use engine::rocks::{Snapshot, SyncSnapshot, WriteBatch, WriteOptions, DB};
use engine::Engines;
use engine_traits::Peekable;
use engine_rocks::Compat;
use kvproto::metapb;
use kvproto::pdpb::PeerStats;
use kvproto::raft_cmdpb::{
    self, AdminCmdType, AdminResponse, CmdType, CommitMergeRequest, RaftCmdRequest,
    RaftCmdResponse, ReadIndexResponse, Request, Response, TransferLeaderRequest,
    TransferLeaderResponse,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RaftMessage, RaftSnapshotData,
};
use protobuf::Message;
use raft::eraftpb::{self, ConfChangeType, EntryType, MessageType};
use raft::{
    self, Progress, ProgressState, RawNode, Ready, SnapshotStatus, StateRole, INVALID_INDEX,
    NO_LIMIT,
};
use time::Timespec;
use uuid::Uuid;

use crate::raftstore::coprocessor::{CoprocessorHost, RegionChangeEvent};
use crate::raftstore::store::fsm::store::PollContext;
use crate::raftstore::store::fsm::{
    apply, Apply, ApplyMetrics, ApplyTask, ApplyTaskRes, GroupState, Proposal, RegionProposal,
};
use crate::raftstore::store::keys::{enc_end_key, enc_start_key};
use crate::raftstore::store::worker::{ReadDelegate, ReadProgress, RegionTask};
use crate::raftstore::store::PdTask;
use crate::raftstore::store::{keys, Callback, Config, ReadResponse, RegionSnapshot};
use crate::raftstore::{Error, Result};
use pd_client::INVALID_ID;
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_sec, monotonic_raw_now};
use tikv_util::worker::Scheduler;
use tikv_util::MustConsumeVec;

use super::cmd_resp;
use super::local_metrics::{RaftMessageMetrics, RaftReadyMetrics};
use super::metrics::*;
use super::peer_storage::{write_peer_state, ApplySnapResult, InvokeContext, PeerStorage};
use super::transport::Transport;
use super::util::{self, check_region_epoch, is_initial_msg, Lease, LeaseState};
use super::DestroyPeerJob;

const SHRINK_CACHE_CAPACITY: usize = 64;

struct ReadIndexRequest {
    id: Uuid,
    cmds: MustConsumeVec<(RaftCmdRequest, Callback)>,
    renew_lease_time: Timespec,
    read_index: Option<u64>,
}

impl ReadIndexRequest {
    // Transmutes `self.id` to a 8 bytes slice, so that we can use the payload to do read index.
    fn binary_id(&self) -> &[u8] {
        self.id.as_bytes()
    }

    fn push_command(&mut self, req: RaftCmdRequest, cb: Callback) {
        RAFT_READ_INDEX_PENDING_COUNT.inc();
        self.cmds.push((req, cb));
    }

    fn with_command(
        id: Uuid,
        req: RaftCmdRequest,
        cb: Callback,
        renew_lease_time: Timespec,
    ) -> Self {
        RAFT_READ_INDEX_PENDING_COUNT.inc();
        let mut cmds = MustConsumeVec::with_capacity("callback of index read", 1);
        cmds.push((req, cb));
        ReadIndexRequest {
            id,
            cmds,
            renew_lease_time,
            read_index: None,
        }
    }
}

impl Drop for ReadIndexRequest {
    fn drop(&mut self) {
        let dur = (monotonic_raw_now() - self.renew_lease_time)
            .to_std()
            .unwrap();
        RAFT_READ_INDEX_PENDING_DURATION.observe(duration_to_sec(dur));
    }
}

#[derive(Default)]
struct ReadIndexQueue {
    reads: VecDeque<ReadIndexRequest>,
    ready_cnt: usize,
}

impl ReadIndexQueue {
    fn clear_uncommitted(&mut self, term: u64) {
        for mut read in self.reads.drain(self.ready_cnt..) {
            RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_stale_req(term, cb);
            }
        }
    }

    /// update the read index of the requests that before the specified id.
    fn advance(&mut self, id: &[u8], read_index: u64) {
        if let Some(i) = self.reads.iter().position(|x| x.binary_id() == id) {
            for pos in 0..=i {
                let req = &mut self.reads[pos];
                let index = req.read_index.get_or_insert(read_index);
                if *index > read_index {
                    *index = read_index;
                }
            }
            if self.ready_cnt < i + 1 {
                self.ready_cnt = i + 1;
            }
        } else {
            error!(
                "cannot find corresponding read from pending reads";
                "id"=>?id, "read-index" =>read_index,
            );
        }
    }

    fn gc(&mut self) {
        if self.reads.capacity() > SHRINK_CACHE_CAPACITY && self.reads.len() < SHRINK_CACHE_CAPACITY
        {
            self.reads.shrink_to_fit();
        }
    }
}

/// The returned states of the peer after checking whether it is stale
#[derive(Debug, PartialEq, Eq)]
pub enum StaleState {
    Valid,
    ToValidate,
    LeaderMissing,
}

/// Meta information about proposals.
pub struct ProposalMeta {
    pub index: u64,
    pub term: u64,
    /// `renew_lease_time` contains the last time when a peer starts to renew lease.
    pub renew_lease_time: Option<Timespec>,
}

#[derive(Default)]
struct ProposalQueue {
    queue: VecDeque<ProposalMeta>,
}

impl ProposalQueue {
    fn pop(&mut self, term: u64) -> Option<ProposalMeta> {
        self.queue.pop_front().and_then(|meta| {
            if meta.term > term {
                self.queue.push_front(meta);
                return None;
            }
            Some(meta)
        })
    }

    fn push(&mut self, meta: ProposalMeta) {
        self.queue.push_back(meta);
    }

    fn clear(&mut self) {
        self.queue.clear();
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
        const SYNC_LOG       = 0b00000001;
        const SPLIT          = 0b00000010;
        const PREPARE_MERGE  = 0b00000100;
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
}

/// A struct that stores the state to wait for `PrepareMerge` apply result.
///
/// When handling the apply result of a `CommitMerge`, the source peer may have
/// not handle the apply result of the `PrepareMerge`, so the target peer has
/// to abort current handle process and wait for it asynchronously.
pub struct WaitApplyResultState {
    /// The following apply results waiting to be handled, including the `CommitMerge`.
    /// These will be handled once `ready_to_merge` is true.
    pub results: Vec<ApplyTaskRes>,
    /// It is used by target peer to check whether the apply result of `PrepareMerge` is handled.
    pub ready_to_merge: Arc<AtomicBool>,
}

pub struct Peer {
    /// The ID of the Region which this Peer belongs to.
    region_id: u64,
    // TODO: remove it once panic!() support slog fields.
    /// Peer_tag, "[region <region_id>] <peer_id>"
    pub tag: String,
    /// The Peer meta information.
    pub peer: metapb::Peer,

    /// The Raft state machine of this Peer.
    pub raft_group: RawNode<PeerStorage>,
    /// The cache of meta information for Region's other Peers.
    peer_cache: RefCell<HashMap<u64, metapb::Peer>>,
    /// Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: HashMap<u64, Instant>,

    proposals: ProposalQueue,
    apply_proposals: Vec<Proposal>,

    leader_missing_time: Option<Instant>,
    leader_lease: Lease,
    pending_reads: ReadIndexQueue,

    /// If it fails to send messages to leader.
    pub leader_unreachable: bool,
    /// Whether this peer is destroyed asynchronously.
    pub pending_remove: bool,
    /// If a snapshot is being applied asynchronously, messages should not be sent.
    pending_messages: Vec<eraftpb::Message>,

    /// Record the instants of peers being added into the configuration.
    /// Remove them after they are not pending any more.
    pub peers_start_pending_time: Vec<(u64, Instant)>,
    /// A inaccurate cache about which peer is marked as down.
    down_peer_ids: Vec<u64>,
    pub recent_conf_change_time: Instant,

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
    /// The state to wait for `PrepareMerge` apply result.
    pub pending_merge_apply_result: Option<WaitApplyResultState>,
    /// source region is catching up logs for merge
    pub catch_up_logs: Option<Arc<AtomicU64>>,

    /// Write Statistics for PD to schedule hot spot.
    pub peer_stat: PeerStat,
}

impl Peer {
    pub fn new(
        store_id: u64,
        cfg: &Config,
        sched: Scheduler<RegionTask>,
        engines: Engines,
        region: &metapb::Region,
        peer: metapb::Peer,
    ) -> Result<Peer> {
        if peer.get_id() == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let tag = format!("[region {}] {}", region.get_id(), peer.get_id());

        let ps = PeerStorage::new(engines.clone(), region, sched, peer.get_id(), tag.clone())?;

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
            tag: tag.clone(),
            skip_bcast_commit: true,
            pre_vote: cfg.prevote,
            ..Default::default()
        };

        let raft_group = RawNode::with_logger(&raft_cfg, ps, &slog_global::get_global())?;
        let mut peer = Peer {
            peer,
            region_id: region.get_id(),
            raft_group,
            proposals: Default::default(),
            apply_proposals: vec![],
            pending_reads: Default::default(),
            peer_cache: RefCell::new(HashMap::default()),
            peer_heartbeats: HashMap::default(),
            peers_start_pending_time: vec![],
            down_peer_ids: vec![],
            recent_conf_change_time: Instant::now(),
            size_diff_hint: 0,
            delete_keys_hint: 0,
            approximate_size: None,
            approximate_keys: None,
            compaction_declined_bytes: 0,
            leader_unreachable: false,
            pending_remove: false,
            pending_merge_state: None,
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
                hash: vec![],
            },
            raft_log_size_hint: 0,
            leader_lease: Lease::new(cfg.raft_store_max_leader_lease()),
            pending_messages: vec![],
            pending_merge_apply_result: None,
            peer_stat: PeerStat::default(),
            catch_up_logs: None,
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            peer.raft_group.campaign()?;
        }

        Ok(peer)
    }

    /// Register self to apply_scheduler so that the peer is then usable.
    /// Also trigger `RegionChangeEvent::Create` here.
    pub fn activate<T, C>(&self, ctx: &PollContext<T, C>) {
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

    #[inline]
    pub fn maybe_append_merge_entries(&mut self, merge: &CommitMergeRequest) -> Option<u64> {
        let mut entries = merge.get_entries();
        if entries.is_empty() {
            // Though the entries is empty, it is possible that one source peer has caught up the logs
            // but commit index is not updated. If Other source peers are already destroyed, so the raft
            // group will not make any progress, namely the source peer can not get the latest commit index anymore.
            // Here update the commit index to let source apply rest uncommitted entires.
            if merge.get_commit() > self.raft_group.raft.raft_log.committed {
                self.raft_group.raft.raft_log.commit_to(merge.get_commit());
                return Some(merge.get_commit());
            } else {
                return None;
            }
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
    }

    /// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
    pub fn maybe_destroy(&mut self) -> Option<DestroyPeerJob> {
        if self.pending_remove {
            info!(
                "is being destroyed, skip";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return None;
        }
        let initialized = self.get_store().is_initialized();
        let async_remove = if self.is_applying_snapshot() {
            if !self.mut_store().cancel_applying_snap() {
                info!(
                    "stale peer is applying snapshot, will destroy next time";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                return None;
            }
            // There is no tasks in apply/local read worker.
            false
        } else {
            initialized
        };
        self.pending_remove = true;

        Some(DestroyPeerJob {
            async_remove,
            initialized,
            region_id: self.region_id,
            peer: self.peer.clone(),
        })
    }

    /// Does the real destroy task which includes:
    /// 1. Set the region to tombstone;
    /// 2. Clear data;
    /// 3. Notify all pending requests.
    pub fn destroy<T, C>(&mut self, ctx: &PollContext<T, C>, keep_data: bool) -> Result<()> {
        fail_point!("raft_store_skip_destroy_peer", |_| Ok(()));
        let t = Instant::now();

        let region = self.region().clone();
        info!(
            "begin to destroy";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        // Set Tombstone state explicitly
        let kv_wb = WriteBatch::default();
        let raft_wb = WriteBatch::default();
        self.mut_store().clear_meta(&kv_wb, &raft_wb)?;
        write_peer_state(
            &ctx.engines.kv,
            &kv_wb,
            &region,
            PeerState::Tombstone,
            self.pending_merge_state.clone(),
        )?;
        // write kv rocksdb first in case of restart happen between two write
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(ctx.cfg.sync_log);
        ctx.engines.write_kv_opt(&kv_wb, &write_opts)?;
        ctx.engines.write_raft_opt(&raft_wb, &write_opts)?;

        if self.get_store().is_initialized() && !keep_data {
            // If we meet panic when deleting data and raft log, the dirty data
            // will be cleared by a newer snapshot applying or restart.
            if let Err(e) = self.get_store().clear_data() {
                error!(
                    "failed to schedule clear data task";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "err" => ?e,
                );
            }
        }

        for mut read in self.pending_reads.reads.drain(..) {
            RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_req_region_removed(region.get_id(), cb);
            }
        }

        for proposal in self.apply_proposals.drain(..) {
            apply::notify_req_region_removed(region.get_id(), proposal.cb);
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
        let status = self.raft_group.status_ref();
        let last_index = self.raft_group.raft.raft_log.last_index();
        for (id, pr) in status.progress.unwrap().iter() {
            // Only recent active peer is considerred, so that an isolated follower
            // won't cause a waste of leader's resource.
            if *id == self.peer.get_id() || !pr.recent_active {
                continue;
            }
            // Keep replicating data to active followers.
            if pr.matched != last_index {
                return res;
            }
        }
        // Unapplied entries can change the configuration of the group.
        res.up_to_date = self.get_store().applied_index() == last_index;
        res
    }

    pub fn check_after_tick(&self, state: GroupState, res: CheckTickResult) -> bool {
        if res.leader {
            res.up_to_date && self.is_leader() && self.raft_group.raft.pending_read_count() == 0
        } else {
            // If follower keeps receiving data from leader, then it's safe to stop
            // ticking, as leader will make sure it has the latest logs.
            // Checking term to make sure campaign has finished and the leader starts
            // doing its job, it's not required but a safe options.
            state != GroupState::Chaos
                && self.raft_group.raft.leader_id != raft::INVALID_ID
                && self.raft_group.raft.raft_log.last_term() == self.raft_group.raft.term
        }
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

    /// Set the region of a peer.
    ///
    /// This will update the region of the peer, caller must ensure the region
    /// has been preserved in a durable device.
    pub fn set_region(
        &mut self,
        host: &CoprocessorHost,
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
    pub fn get_raft_status(&self) -> raft::StatusRef<'_> {
        self.raft_group.status_ref()
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
    pub fn get_store(&self) -> &PeerStorage {
        self.raft_group.get_store()
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
        self.raft_group.get_snap()
    }

    fn add_ready_metric(&self, ready: &Ready, metrics: &mut RaftReadyMetrics) {
        metrics.message += ready.messages.len() as u64;
        metrics.commit += ready
            .committed_entries
            .as_ref()
            .map_or(0, |v| v.len() as u64);
        metrics.append += ready.entries().len() as u64;

        if !raft::is_empty_snap(ready.snapshot()) {
            metrics.snapshot += 1;
        }
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
                | MessageType::MsgCheckQuorum
                | MessageType::MsgReadIndex
                | MessageType::MsgReadIndexResp => {}
            }
            self.send_raft_message(msg, trans);
        }
    }

    /// Steps the raft message.
    pub fn step(&mut self, mut m: eraftpb::Message) -> Result<()> {
        fail_point!(
            "step_message_3_1",
            { self.peer.get_store_id() == 3 && self.region_id == 1 },
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
        // Here we hold up MsgReadIndex. If current peer has valid lease, then we could handle the
        // request directly, rather than send a heartbeat to check quorum.
        let msg_type = m.get_msg_type();
        if msg_type == MessageType::MsgReadIndex {
            if let LeaseState::Valid = self.inspect_lease() {
                let mut resp = eraftpb::Message::default();
                resp.set_msg_type(MessageType::MsgReadIndexResp);
                resp.to = m.from;
                resp.index = self.get_store().committed_index();
                resp.set_entries(m.take_entries());

                self.pending_messages.push(resp);
                return Ok(());
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
        let region = self.raft_group.get_store().region();
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
    pub fn collect_pending_peers(&mut self) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group.status_ref();
        let truncated_idx = self.get_store().truncated_index();

        if status.progress.is_none() {
            return pending_peers;
        }

        let progresses = status.progress.unwrap().iter();
        for (&id, progress) in progresses {
            if id == self.peer.get_id() {
                continue;
            }
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
            let truncated_idx = self.raft_group.get_store().truncated_index();
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

    pub fn check_stale_state<T, C>(&mut self, ctx: &mut PollContext<T, C>) -> StaleState {
        if self.is_leader() {
            // Leaders always have valid state.
            //
            // We update the leader_missing_time in the `fn step`. However one peer region
            // does not send any raft messages, so we have to check and update it before
            // reporting stale states.
            self.leader_missing_time = None;
            return StaleState::Valid;
        }
        let naive_peer = !self.is_initialized() || self.raft_group.raft.is_learner;
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

    fn on_role_changed<T, C>(&mut self, ctx: &mut PollContext<T, C>, ready: &Ready) {
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
                    // current term to apply the read. So broadcast eargerly to avoid unexpected
                    // latency.
                    //
                    // TODO: Maybe the predecessor should just drop all the read requests directly?
                    // All the requests need to be redirected in the end anyway and executing
                    // prewrites or commits will be just a waste.
                    self.last_urgent_proposal_idx = self.raft_group.raft.raft_log.last_index();
                    self.raft_group.skip_bcast_commit(false);
                }
                StateRole::Follower => {
                    self.leader_lease.expire();
                }
                _ => {}
            }
            ctx.coprocessor_host
                .on_role_change(self.region(), ss.raft_state);
        }
    }

    #[inline]
    pub fn ready_to_handle_pending_snap(&self) -> bool {
        // If apply worker is still working, written apply state may be overwritten
        // by apply worker. So we have to wait here.
        // Please note that committed_index can't be used here. When applying a snapshot,
        // a stale heartbeat can make the leader think follower has already applied
        // the snapshot, and send remaining log entries, which may increase committed_index.
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
            // the old leader still think it owns the splitted range.
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
            && !self.is_splitting()
            && !self.is_merging()
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

    // Checks merge strictly, it checks whether there is any onging merge by
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

    pub fn take_apply_proposals(&mut self) -> Option<RegionProposal> {
        if self.apply_proposals.is_empty() {
            return None;
        }

        let proposals = mem::replace(&mut self.apply_proposals, vec![]);
        let region_proposal = RegionProposal::new(self.peer_id(), self.region_id, proposals);
        Some(region_proposal)
    }

    pub fn handle_raft_ready_append<T: Transport, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
    ) -> Option<(Ready, InvokeContext)> {
        if self.pending_remove {
            return None;
        }
        if self.mut_store().check_applying_snap() {
            // If we continue to handle all the messages, it may cause too many messages because
            // leader will send all the remaining messages to this follower, which can lead
            // to full message queue under high load.
            debug!(
                "still applying snapshot, skip further handling";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
            );
            return None;
        }

        if !self.pending_messages.is_empty() {
            fail_point!("raft_before_follower_send");
            let messages = mem::replace(&mut self.pending_messages, vec![]);
            ctx.need_flush_trans = true;
            self.send(&mut ctx.trans, messages, &mut ctx.raft_metrics.message);
        }

        if let Some(snap) = self.get_pending_snapshot() {
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

            let mut snap_data = RaftSnapshotData::default();
            snap_data
                .merge_from_bytes(snap.get_data())
                .unwrap_or_else(|e| {
                    warn!(
                        "failed to parse snap data";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "err" => ?e,
                    );
                });
            let region = snap_data.take_region();

            let meta = ctx.store_meta.lock().unwrap();
            // Region's range changes if and only if epoch version change. So if the snapshot's
            // version is not larger than now, we can make sure there is no overlap.
            if region.get_region_epoch().get_version()
                > meta.regions[&region.get_id()]
                    .get_region_epoch()
                    .get_version()
            {
                // For merge process, when applying snapshot or create new peer the stale source
                // peer is destroyed asynchronously. So here checks whether there is any overlap, if
                // so, wait and do not handle raft ready.
                if let Some(r) = meta
                    .region_ranges
                    .range((Excluded(enc_start_key(&region)), Unbounded::<Vec<u8>>))
                    .map(|(_, &region_id)| &meta.regions[&region_id])
                    .take_while(|r| enc_start_key(r) < enc_end_key(&region))
                    .find(|r| r.get_id() != region.get_id())
                {
                    info!(
                        "snapshot range overlaps, wait source destroy finish";
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                        "apply_index" => self.get_store().applied_index(),
                        "last_applying_index" => self.last_applying_idx,
                        "overlap_region" => ?r,
                    );
                    return None;
                }
            }
        }

        if !self
            .raft_group
            .has_ready_since(Some(self.last_applying_idx))
        {
            return None;
        }

        debug!(
            "handle raft ready";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

        let mut ready = self.raft_group.ready_since(self.last_applying_idx);

        self.on_role_changed(ctx, &ready);

        self.add_ready_metric(&ready, &mut ctx.raft_metrics.ready);

        if !ready.committed_entries.as_ref().map_or(true, Vec::is_empty)
            && ctx.current_time.is_none()
        {
            ctx.current_time.replace(monotonic_raw_now());
        }

        // The leader can write to disk and replicate to the followers concurrently
        // For more details, check raft thesis 10.2.1.
        if self.is_leader() {
            fail_point!("raft_before_leader_send");
            let msgs = ready.messages.drain(..);
            ctx.need_flush_trans = true;
            self.send(&mut ctx.trans, msgs, &mut ctx.raft_metrics.message);
        }

        let invoke_ctx = match self.mut_store().handle_raft_ready(ctx, &ready) {
            Ok(r) => r,
            Err(e) => {
                // We may have written something to writebatch and it can't be reverted, so has
                // to panic here.
                panic!("{} failed to handle raft ready: {:?}", self.tag, e)
            }
        };

        Some((ready, invoke_ctx))
    }

    pub fn post_raft_ready_append<T: Transport, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        ready: &mut Ready,
        invoke_ctx: InvokeContext,
    ) -> Option<ApplySnapResult> {
        if invoke_ctx.has_snapshot() {
            // When apply snapshot, there is no log applied and not compacted yet.
            self.raft_log_size_hint = 0;
        }

        let apply_snap_result = self.mut_store().post_ready(invoke_ctx);
        if apply_snap_result.is_some() && self.peer.get_is_learner() {
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

        if !self.is_leader() {
            fail_point!("raft_before_follower_send");
            if self.is_applying_snapshot() {
                self.pending_messages = mem::replace(&mut ready.messages, vec![]);
            } else {
                self.send(
                    &mut ctx.trans,
                    ready.messages.drain(..),
                    &mut ctx.raft_metrics.message,
                );
                ctx.need_flush_trans = true;
            }
        }

        if apply_snap_result.is_some() {
            self.activate(ctx);
            let mut meta = ctx.store_meta.lock().unwrap();
            meta.readers
                .insert(self.region_id, ReadDelegate::from_peer(self));
        }

        apply_snap_result
    }

    pub fn handle_raft_ready_apply<T, C>(&mut self, ctx: &mut PollContext<T, C>, mut ready: Ready) {
        // Call `handle_raft_committed_entries` directly here may lead to inconsistency.
        // In some cases, there will be some pending committed entries when applying a
        // snapshot. If we call `handle_raft_committed_entries` directly, these updates
        // will be written to disk. Because we apply snapshot asynchronously, so these
        // updates will soon be removed. But the soft state of raft is still be updated
        // in memory. Hence when handle ready next time, these updates won't be included
        // in `ready.committed_entries` again, which will lead to inconsistency.
        if self.is_applying_snapshot() {
            // Snapshot's metadata has been applied.
            self.last_applying_idx = self.get_store().truncated_index();
        } else {
            let committed_entries = ready.committed_entries.take().unwrap();
            // leader needs to update lease and last committed split index.
            let mut lease_to_be_updated = self.is_leader();
            let mut split_to_be_updated = self.is_leader();
            let mut merge_to_be_update = self.is_leader();
            if !lease_to_be_updated {
                // It's not leader anymore, we are safe to clear proposals. If it becomes leader
                // again, the lease should be updated when election is finished, old proposals
                // have no effect.
                self.proposals.clear();
            }
            for entry in committed_entries.iter().rev() {
                // raft meta is very small, can be ignored.
                self.raft_log_size_hint += entry.get_data().len() as u64;
                if lease_to_be_updated {
                    let propose_time = self.find_propose_time(entry.get_index(), entry.get_term());
                    if let Some(propose_time) = propose_time {
                        ctx.raft_metrics.commit_log.observe(duration_to_sec(
                            (ctx.current_time.unwrap() - propose_time).to_std().unwrap(),
                        ));
                        self.maybe_renew_leader_lease(propose_time, ctx, None);
                        lease_to_be_updated = false;
                    }
                }

                // We care about split/merge commands that are committed in the current term.
                if entry.term == self.term() && (split_to_be_updated || merge_to_be_update) {
                    let ctx = ProposalContext::from_bytes(&entry.context);
                    if split_to_be_updated && ctx.contains(ProposalContext::SPLIT) {
                        // We don't need to suspect its lease because peers of new region that
                        // in other store do not start election before theirs election timeout
                        // which is longer than the max leader lease.
                        // It's safe to read local within its current lease, however, it's not
                        // safe to renew its lease.
                        self.last_committed_split_idx = entry.index;
                        split_to_be_updated = false;
                    }
                    if merge_to_be_update && ctx.contains(ProposalContext::PREPARE_MERGE) {
                        // We committed prepare merge, to prevent unsafe read index,
                        // we must record its index.
                        self.last_committed_prepare_merge_idx = entry.get_index();
                        // After prepare_merge is committed, the leader can not know
                        // when the target region merges majority of this region, also
                        // it can not know when the target region writes new values.
                        // To prevent unsafe local read, we suspect its leader lease.
                        self.leader_lease.suspect(monotonic_raw_now());
                        merge_to_be_update = false;
                    }
                }
            }
            if !committed_entries.is_empty() {
                self.last_applying_idx = committed_entries.last().unwrap().get_index();
                if self.last_applying_idx >= self.last_urgent_proposal_idx {
                    // Urgent requests are flushed, make it lazy again.
                    self.raft_group.skip_bcast_commit(true);
                    self.last_urgent_proposal_idx = u64::MAX;
                }
                let apply = Apply::new(self.region_id, self.term(), committed_entries);
                ctx.apply_router
                    .schedule_task(self.region_id, ApplyTask::apply(apply));
            }
            // Check whether there is a pending generate snapshot task, the task
            // needs to be sent to the apply system.
            // Always sending snapshot task behind apply task, so it gets latest
            // snapshot.
            if let Some(gen_task) = self.mut_store().take_gen_snap_task() {
                self.pending_request_snapshot_count
                    .fetch_add(1, Ordering::SeqCst);
                ctx.apply_router
                    .schedule_task(self.region_id, ApplyTask::Snapshot(gen_task));
            }
        }

        self.apply_reads(ctx, &ready);

        self.raft_group.advance_append(ready);
        if self.is_applying_snapshot() {
            // Because we only handle raft ready when not applying snapshot, so following
            // line won't be called twice for the same snapshot.
            self.raft_group.advance_apply(self.last_applying_idx);
        }
        self.proposals.gc();
    }

    /// Responses to the ready read index request on the replica, the replica is not a leader.
    fn post_pending_read_index_on_replica<T, C>(&mut self, ctx: &mut PollContext<T, C>) {
        if self.pending_reads.ready_cnt > 0 {
            for _ in 0..self.pending_reads.ready_cnt {
                let mut read = self.pending_reads.reads.pop_front().unwrap();
                RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
                assert!(read.read_index.is_some());

                let is_read_index_request = read.cmds.len() == 1
                    && read.cmds[0].0.get_requests().len() == 1
                    && read.cmds[0].0.get_requests()[0].get_cmd_type() == CmdType::ReadIndex;

                let term = self.term();
                if is_read_index_request {
                    debug!("handle reads with a read index";
                        "request_id" => ?read.binary_id(),
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                    );
                    for (req, cb) in read.cmds.drain(..) {
                        cb.invoke_read(self.handle_read(ctx, req, true, read.read_index));
                    }
                    self.pending_reads.ready_cnt -= 1;
                } else if self.ready_to_handle_unsafe_replica_read(read.read_index.unwrap()) {
                    debug!("handle reads with a read index";
                        "request_id" => ?read.binary_id(),
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                    );
                    for (req, cb) in read.cmds.drain(..) {
                        // We should check epoch since the range could be changed
                        if req.get_header().get_replica_read() {
                            cb.invoke_read(self.handle_read(ctx, req, true, read.read_index));
                        } else {
                            apply::notify_stale_req(term, cb);
                        }
                    }
                    self.pending_reads.ready_cnt -= 1;
                } else {
                    self.pending_reads.reads.push_front(read);
                }
            }
        }
    }

    fn apply_reads<T, C>(&mut self, ctx: &mut PollContext<T, C>, ready: &Ready) {
        let mut propose_time = None;
        // The follower may lost `ReadIndexResp`, so the pending_reads does not
        // guarantee the orders are consistent with read_states. `advance` will
        // update the `read_index` of read request that before this successful
        // `ready`.
        if !self.is_leader() {
            // NOTE: there could still be some read requests following, which will be cleared in
            // `clear_uncommitted` later.
            for state in ready.read_states() {
                self.pending_reads
                    .advance(state.request_ctx.as_slice(), state.index);
            }
            self.post_pending_read_index_on_replica(ctx);
        } else if self.ready_to_handle_read() {
            for state in ready.read_states() {
                let mut read = self.pending_reads.reads.pop_front().unwrap();
                assert_eq!(state.request_ctx.as_slice(), read.binary_id());
                RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
                debug!("handle reads with a read index";
                    "request_id" => ?read.binary_id(),
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                );
                for (req, cb) in read.cmds.drain(..) {
                    cb.invoke_read(self.handle_read(ctx, req, true, Some(state.index)));
                }
                propose_time = Some(read.renew_lease_time);
            }
        } else {
            for state in ready.read_states() {
                let read = &mut self.pending_reads.reads[self.pending_reads.ready_cnt];
                assert_eq!(state.request_ctx.as_slice(), read.binary_id());
                self.pending_reads.ready_cnt += 1;
                read.read_index = Some(state.index);
                propose_time = Some(read.renew_lease_time);
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss().is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads.clear_uncommitted(term);
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

    pub fn post_apply<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        apply_state: RaftApplyState,
        applied_index_term: u64,
        apply_metrics: &ApplyMetrics,
    ) -> bool {
        let mut has_ready = false;

        if self.is_applying_snapshot() {
            panic!("{} should not applying snapshot.", self.tag);
        }

        self.raft_group
            .advance_apply(apply_state.get_applied_index());

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
        } else {
            if self.pending_reads.ready_cnt > 0 && self.ready_to_handle_read() {
                for _ in 0..self.pending_reads.ready_cnt {
                    let mut read = self.pending_reads.reads.pop_front().unwrap();
                    debug!("handle reads with a read index";
                        "request_id" => ?read.binary_id(),
                        "region_id" => self.region_id,
                        "peer_id" => self.peer.get_id(),
                    );
                    RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
                    for (req, cb) in read.cmds.drain(..) {
                        cb.invoke_read(self.handle_read(ctx, req, true, read.read_index));
                    }
                }
                self.pending_reads.ready_cnt = 0;
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
    fn maybe_renew_leader_lease<T, C>(
        &mut self,
        ts: Timespec,
        ctx: &mut PollContext<T, C>,
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

    fn find_propose_time(&mut self, index: u64, term: u64) -> Option<Timespec> {
        while let Some(meta) = self.proposals.pop(term) {
            if meta.index == index && meta.term == term {
                return Some(meta.renew_lease_time.unwrap());
            }
        }
        None
    }

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
    pub fn propose<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        cb: Callback,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
    ) -> bool {
        if self.pending_remove {
            return false;
        }

        ctx.raft_metrics.propose.all += 1;

        let mut is_conf_change = false;
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
            Ok(RequestPolicy::ProposeConfChange) => {
                is_conf_change = true;
                self.propose_conf_change(ctx, &req)
            }
            Err(e) => Err(e),
        };

        match res {
            Err(e) => {
                cmd_resp::bind_error(&mut err_resp, e);
                cb.invoke_with_response(err_resp);
                false
            }
            Ok(idx) => {
                if is_urgent {
                    self.last_urgent_proposal_idx = idx;
                    // Eager flush to make urgent proposal be applied on all nodes as soon as
                    // possible.
                    self.raft_group.skip_bcast_commit(false);
                }
                let meta = ProposalMeta {
                    index: idx,
                    term: self.term(),
                    renew_lease_time: None,
                };
                self.post_propose(ctx, meta, is_conf_change, cb);
                true
            }
        }
    }

    fn post_propose<T, C>(
        &mut self,
        poll_ctx: &mut PollContext<T, C>,
        mut meta: ProposalMeta,
        is_conf_change: bool,
        cb: Callback,
    ) {
        // Try to renew leader lease on every consistent read/write request.
        if poll_ctx.current_time.is_none() {
            poll_ctx.current_time = Some(monotonic_raw_now());
        }
        meta.renew_lease_time = poll_ctx.current_time;

        if !cb.is_none() {
            let p = Proposal::new(is_conf_change, meta.index, meta.term, cb);
            self.apply_proposals.push(p);
        }

        self.proposals.push(meta);
    }

    /// Count the number of the healthy nodes.
    /// A node is healthy when
    /// 1. it's the leader of the Raft group, which has the latest logs
    /// 2. it's a follower, and it does not lag behind the leader a lot.
    ///    If a snapshot is involved between it and the Raft leader, it's not healthy since
    ///    it cannot works as a node in the quorum to receive replicating logs from leader.
    fn count_healthy_node<'a, I>(&self, progress: I) -> usize
    where
        I: Iterator<Item = (&'a u64, &'a Progress)>,
    {
        let mut healthy = 0;
        for (_, pr) in progress {
            if pr.matched >= self.get_store().truncated_index() {
                healthy += 1;
            }
        }
        healthy
    }

    /// Validate the `ConfChange` request and check whether it's safe to
    /// propose the specified conf change request.
    /// It's safe iff at least the quorum of the Raft group is still healthy
    /// right after that conf change is applied.
    /// Define the total number of nodes in current Raft cluster to be `total`.
    /// To ensure the above safety, if the cmd is
    /// 1. A `AddNode` request
    ///    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
    /// 2. A `RemoveNode` request
    ///    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
    ///    need to be up to date for now. If 'allow_remove_leader' is false then
    ///    the peer to be removed should not be the leader.
    fn check_conf_change<T, C>(
        &self,
        ctx: &mut PollContext<T, C>,
        cmd: &RaftCmdRequest,
    ) -> Result<()> {
        let change_peer = apply::get_change_peer_cmd(cmd).unwrap();
        let change_type = change_peer.get_change_type();
        let peer = change_peer.get_peer();

        // Check the request itself is valid or not.
        match (change_type, peer.get_is_learner()) {
            (ConfChangeType::AddNode, true) | (ConfChangeType::AddLearnerNode, false) => {
                warn!(
                    "invalid conf change request";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "request" => ?change_peer,
                );
                return Err(box_err!("invalid conf change request"));
            }
            _ => {}
        }

        if change_type == ConfChangeType::RemoveNode
            && !ctx.cfg.allow_remove_leader
            && peer.get_id() == self.peer_id()
        {
            warn!(
                "rejects remove leader request";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "request" => ?change_peer,
            );
            return Err(box_err!("ignore remove leader"));
        }

        let status = self.raft_group.status_ref();
        let total = status.progress.unwrap().voter_ids().len();
        if total == 1 {
            // It's always safe if there is only one node in the cluster.
            return Ok(());
        }
        let mut progress = status.progress.unwrap().clone();

        match change_type {
            ConfChangeType::AddNode => {
                if let Err(raft::Error::NotExists(_, _)) = progress.promote_learner(peer.get_id()) {
                    let _ = progress.insert_voter(peer.get_id(), Progress::new(0, 0));
                }
            }
            ConfChangeType::RemoveNode => {
                progress.remove(peer.get_id())?;
            }
            ConfChangeType::AddLearnerNode => {
                return Ok(());
            }
            ConfChangeType::BeginMembershipChange | ConfChangeType::FinalizeMembershipChange => unimplemented!(),
        }
        let healthy = self.count_healthy_node(progress.voters());
        let quorum_after_change = raft::majority(progress.voter_ids().len());
        if healthy >= quorum_after_change {
            return Ok(());
        }

        PEER_ADMIN_CMD_COUNTER_VEC
            .with_label_values(&["conf_change", "reject_unsafe"])
            .inc();

        info!(
            "rejects unsafe conf change request";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "request" => ?change_peer,
            "total" => total,
            "healthy" => healthy,
            "quorum_after_change" => quorum_after_change,
        );
        Err(box_err!(
            "unsafe to perform conf change {:?}, total {}, healthy {}, quorum after \
             change {}",
            change_peer,
            total,
            healthy,
            quorum_after_change
        ))
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

    fn ready_to_transfer_leader<T, C>(
        &self,
        ctx: &mut PollContext<T, C>,
        peer: &metapb::Peer,
    ) -> bool {
        let peer_id = peer.get_id();
        let status = self.raft_group.status_ref();
        let progress = status.progress.unwrap();

        if !progress.voter_ids().contains(&peer_id) {
            return false;
        }

        for (_, progress) in progress.voters() {
            if progress.state == ProgressState::Snapshot {
                return false;
            }
        }

        // Checks if safe to transfer leader.
        // Check `has_pending_conf` is necessary because `recent_conf_change_time` is updated
        // on applied. TODO: fix the transfer leader issue in Raft.
        if self.raft_group.raft.has_pending_conf()
            || duration_to_sec(self.recent_conf_change_time.elapsed())
                < ctx.cfg.raft_reject_transfer_leader_duration.as_secs() as f64
        {
            debug!(
                "reject transfer leader due to the region was config changed recently";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "peer" => ?peer,
            );
            return false;
        }

        let last_index = self.get_store().last_index();
        last_index <= progress.get(peer_id).unwrap().matched + ctx.cfg.leader_transfer_max_log_lag
    }

    fn read_local<T, C>(&mut self, ctx: &mut PollContext<T, C>, req: RaftCmdRequest, cb: Callback) {
        ctx.raft_metrics.propose.local_read += 1;
        cb.invoke_read(self.handle_read(ctx, req, false, Some(self.get_store().committed_index())))
    }

    fn pre_read_index(&self) -> Result<()> {
        fail_point!(
            "before_propose_readindex",
            |s| if s.map_or(true, |s| s.parse().unwrap_or(true)) {
                Ok(())
            } else {
                Err(box_err!("can not read due to injected failure"))
            }
        );

        // See more in ready_to_handle_read().
        if self.is_splitting() {
            return Err(box_err!("can not read index due to split"));
        }
        if self.is_merging() {
            return Err(box_err!("can not read index due to merge"));
        }
        Ok(())
    }

    // Returns a boolean to indicate whether the `read` is proposed or not.
    // For these cases it won't be proposed:
    // 1. The region is in merging or splitting;
    // 2. The message is stale and dropped by the Raft group internally;
    // 3. There is already a read request proposed in the current lease;
    fn read_index<T, C>(
        &mut self,
        poll_ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
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
            poll_ctx.raft_metrics.propose.unsafe_read_index += 1;
            cmd_resp::bind_error(&mut err_resp, e);
            cb.invoke_with_response(err_resp);
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
                    if let Some(read) = self.pending_reads.reads.back_mut() {
                        let max_lease = poll_ctx.cfg.raft_store_max_leader_lease();
                        if read.renew_lease_time + max_lease > renew_lease_time {
                            read.push_command(req, cb);
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
            cmd_resp::bind_error(
                &mut err_resp,
                box_err!("can not read index due to no leader"),
            );
            poll_ctx.raft_metrics.invalid_proposal.read_index_no_leader += 1;
            cb.invoke_with_response(err_resp);
            return false;
        }

        // Should we call pre_propose here?
        let last_pending_read_count = self.raft_group.raft.pending_read_count();
        let last_ready_read_count = self.raft_group.raft.ready_read_count();

        poll_ctx.raft_metrics.propose.read_index += 1;

        let id = Uuid::new_v4();
        self.raft_group.read_index(id.as_bytes().to_vec());
        debug!("request to get a read index";
            "request_id" => ?id,
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
        );

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

        let read_proposal = ReadIndexRequest::with_command(id, req, cb, renew_lease_time);
        self.pending_reads.reads.push_back(read_proposal);

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.
        if self.leader_lease.inspect(Some(renew_lease_time)) == LeaseState::Suspect {
            let req = RaftCmdRequest::default();
            if let Ok(index) = self.propose_normal(poll_ctx, req) {
                let meta = ProposalMeta {
                    index,
                    term: self.term(),
                    renew_lease_time: Some(renew_lease_time),
                };
                self.post_propose(poll_ctx, meta, false, Callback::None);
            }
        }

        true
    }

    // For now, it is only used in merge.
    pub fn get_min_progress(&self) -> Result<u64> {
        let mut min = None;
        if let Some(progress) = self.raft_group.status_ref().progress {
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
                if min.is_none() {
                    min = Some(pr.matched);
                }
                if min.unwrap() > pr.matched {
                    min = Some(pr.matched);
                }
            }
        }
        Ok(min.unwrap_or(0))
    }

    fn pre_propose_prepare_merge<T, C>(
        &self,
        ctx: &mut PollContext<T, C>,
        req: &mut RaftCmdRequest,
    ) -> Result<()> {
        let last_index = self.raft_group.raft.raft_log.last_index();
        let min_progress = self.get_min_progress()?;
        let min_index = min_progress + 1;
        if min_progress == 0 || last_index - min_progress > ctx.cfg.merge_max_log_gap {
            return Err(box_err!(
                "log gap ({}, {}] is too large, skip merge",
                min_progress,
                last_index
            ));
        }
        let mut entry_size = 0;
        for entry in self.raft_group.raft.raft_log.entries(min_index, NO_LIMIT)? {
            entry_size += entry.get_data().len();
            if entry.get_entry_type() == EntryType::EntryConfChange {
                return Err(box_err!("log gap contains conf change, skip merging."));
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
            .set_min_index(min_index);
        Ok(())
    }

    fn pre_propose<T, C>(
        &self,
        poll_ctx: &mut PollContext<T, C>,
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
            _ => {}
        }

        if req.get_admin_request().has_prepare_merge() {
            self.pre_propose_prepare_merge(poll_ctx, req)?;
            ctx.insert(ProposalContext::PREPARE_MERGE);
        }

        Ok(ctx)
    }

    fn propose_normal<T, C>(
        &mut self,
        poll_ctx: &mut PollContext<T, C>,
        mut req: RaftCmdRequest,
    ) -> Result<u64> {
        if self.pending_merge_state.is_some()
            && req.get_admin_request().get_cmd_type() != AdminCmdType::RollbackMerge
        {
            return Err(box_err!("peer in merging mode, can't do proposal."));
        }

        poll_ctx.raft_metrics.propose.normal += 1;

        // TODO: validate request for unexpected changes.
        let ctx = match self.pre_propose(poll_ctx, &mut req) {
            Ok(ctx) => ctx,
            Err(e) => {
                warn!(
                    "skip proposal";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "err" => ?e,
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

        Ok(propose_index)
    }

    // Return true to if the transfer leader request is accepted.
    fn propose_transfer_leader<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
        cb: Callback,
    ) -> bool {
        ctx.raft_metrics.propose.transfer_leader += 1;

        let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
        let peer = transfer_leader.get_peer();

        let transferred = if self.ready_to_transfer_leader(ctx, peer) {
            self.transfer_leader(peer);
            true
        } else {
            info!(
                "transfer leader message ignored directly";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "message" => ?req,
            );
            false
        };

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
    fn propose_conf_change<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        req: &RaftCmdRequest,
    ) -> Result<u64> {
        if self.pending_merge_state.is_some() {
            return Err(box_err!("peer in merging mode, can't do proposal."));
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

        self.check_conf_change(ctx, req)?;

        ctx.raft_metrics.propose.conf_change += 1;

        let data = req.write_to_bytes()?;

        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        let change_peer = apply::get_change_peer_cmd(req).unwrap();
        let mut cc = eraftpb::ConfChange::default();
        cc.set_change_type(change_peer.get_change_type());
        cc.set_node_id(change_peer.get_peer().get_id());
        cc.set_context(data);

        info!(
            "propose conf change peer";
            "region_id" => self.region_id,
            "peer_id" => self.peer.get_id(),
            "change_type" => ?cc.get_change_type(),
            "change_peer" => cc.get_node_id(),
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

    fn handle_read<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> ReadResponse {
        let mut resp = ReadExecutor::new(
            ctx.engines.kv.clone(),
            check_epoch,
            false, /* we don't need snapshot time */
        )
        .execute(&req, self.region(), read_index);

        cmd_resp::bind_term(&mut resp.response, self.term());
        resp
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub fn stop(&mut self) {
        self.mut_store().cancel_applying_snap();
        for mut read in self.pending_reads.reads.drain(..) {
            RAFT_READ_INDEX_PENDING_COUNT.sub(read.cmds.len() as i64);
            read.cmds.clear();
        }
    }
}

impl Peer {
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

    pub fn heartbeat_pd<T, C>(&mut self, ctx: &PollContext<T, C>) {
        let task = PdTask::Heartbeat {
            term: self.term(),
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(ctx.cfg.max_peer_down_duration.0),
            pending_peers: self.collect_pending_peers(),
            written_bytes: self.peer_stat.written_bytes,
            written_keys: self.peer_stat.written_keys,
            approximate_size: self.approximate_size,
            approximate_keys: self.approximate_keys,
        };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "err" => ?e,
            );
        }
    }

    fn send_raft_message<T: Transport>(&mut self, msg: eraftpb::Message, trans: &mut T) {
        let mut send_msg = RaftMessage::default();
        send_msg.set_region_id(self.region_id);
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());

        let from_peer = self.peer.clone();
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
            "from" => from_peer.get_id(),
            "to" => to_peer_id,
        );

        send_msg.set_from_peer(from_peer);
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
            if apply::get_change_peer_cmd(req).is_some() {
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
                return Err(box_err!("read and write can't be mixed in one batch."));
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

#[derive(Debug)]
pub struct ReadExecutor {
    check_epoch: bool,
    engine: Arc<DB>,
    snapshot: Option<SyncSnapshot>,
    snapshot_time: Option<Timespec>,
    need_snapshot_time: bool,
}

impl ReadExecutor {
    pub fn new(engine: Arc<DB>, check_epoch: bool, need_snapshot_time: bool) -> Self {
        ReadExecutor {
            check_epoch,
            engine,
            snapshot: None,
            snapshot_time: None,
            need_snapshot_time,
        }
    }

    #[inline]
    pub fn snapshot_time(&mut self) -> Option<Timespec> {
        self.maybe_update_snapshot();
        self.snapshot_time
    }

    #[inline]
    fn maybe_update_snapshot(&mut self) {
        if self.snapshot.is_some() {
            return;
        }
        let engine = self.engine.clone();
        self.snapshot = Some(Snapshot::new(engine).into_sync());
        // Reading current timespec after snapshot, in case we do not
        // expire lease in time.
        atomic::fence(atomic::Ordering::Release);
        if self.need_snapshot_time {
            self.snapshot_time = Some(monotonic_raw_now());
        }
    }

    fn do_get(&self, req: &Request, region: &metapb::Region) -> Result<Response> {
        // TODO: the get_get looks weird, maybe we should figure out a better name later.
        let key = req.get_get().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, region)?;

        let mut resp = Response::default();
        let snapshot = self.snapshot.as_ref().unwrap();
        let res = if !req.get_get().get_cf().is_empty() {
            let cf = req.get_get().get_cf();
            // TODO: check whether cf exists or not.
            snapshot.c()
                .get_value_cf(cf, &keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to get {} with cf {}: {:?}",
                        region.get_id(),
                        hex::encode_upper(key),
                        cf,
                        e
                    )
                })
        } else {
            snapshot.c()
                .get_value(&keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to get {}: {:?}",
                        region.get_id(),
                        hex::encode_upper(key),
                        e
                    )
                })
        };
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    pub fn execute(
        &mut self,
        msg: &RaftCmdRequest,
        region: &metapb::Region,
        read_index: Option<u64>,
    ) -> ReadResponse {
        if self.check_epoch {
            if let Err(e) = check_region_epoch(msg, region, true) {
                debug!(
                    "epoch not match";
                    "region_id" => region.get_id(),
                    "err" => ?e,
                );
                return ReadResponse {
                    response: cmd_resp::new_error(e),
                    snapshot: None,
                };
            }
        }
        self.maybe_update_snapshot();
        let mut need_snapshot = false;
        let requests = msg.get_requests();
        let mut responses = Vec::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => match self.do_get(req, region) {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(
                            "failed to execute get command";
                            "region_id" => region.get_id(),
                            "err" => ?e,
                        );
                        return ReadResponse {
                            response: cmd_resp::new_error(e),
                            snapshot: None,
                        };
                    }
                },
                CmdType::Snap => {
                    need_snapshot = true;
                    raft_cmdpb::Response::default()
                }
                CmdType::ReadIndex => {
                    let mut resp = raft_cmdpb::Response::default();
                    if let Some(read_index) = read_index {
                        let mut res = ReadIndexResponse::default();
                        res.set_read_index(read_index);
                        resp.set_read_index(res);
                    } else {
                        panic!("[region {}] can not get readindex", region.get_id(),);
                    }
                    resp
                }
                CmdType::Prewrite
                | CmdType::Put
                | CmdType::Delete
                | CmdType::DeleteRange
                | CmdType::IngestSst
                | CmdType::Invalid => unreachable!(),
            };
            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }

        let mut response = RaftCmdResponse::default();
        response.set_responses(responses.into());
        let snapshot = if need_snapshot {
            Some(RegionSnapshot::from_snapshot(
                self.snapshot.clone().unwrap(),
                region.to_owned(),
            ))
        } else {
            None
        };
        ReadResponse { response, snapshot }
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
        return match req.get_cmd_type() {
            AdminCmdType::ChangePeer
            | AdminCmdType::Split
            | AdminCmdType::BatchSplit
            | AdminCmdType::PrepareMerge
            | AdminCmdType::CommitMerge
            | AdminCmdType::RollbackMerge => true,
            _ => false,
        };
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

    match req.get_admin_request().get_cmd_type() {
        AdminCmdType::Split
        | AdminCmdType::BatchSplit
        | AdminCmdType::ChangePeer
        | AdminCmdType::ComputeHash
        | AdminCmdType::VerifyHash
        | AdminCmdType::PrepareMerge
        | AdminCmdType::CommitMerge
        | AdminCmdType::RollbackMerge => true,
        _ => false,
    }
}

fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::default();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::default());
    let mut resp = RaftCmdResponse::default();
    resp.set_admin_response(response);
    resp
}

#[cfg(test)]
mod tests {
    use protobuf::ProtobufEnum;

    use super::*;

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
        err_table.push(req.clone());

        for req in err_table {
            let mut inspector = DummyInspector {
                applied_to_index_term: true,
                lease_state: LeaseState::Valid,
            };
            assert!(inspector.inspect(&req).is_err());
        }
    }
}
