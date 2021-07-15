// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{atomic, Arc};
use std::time::{Duration, Instant};
use std::{cmp, mem, u64, usize};

use crossbeam::atomic::AtomicCell;
use engine::Engines;
use engine_rocks::{Compat, RocksEngine};
use engine_traits::{KvEngine, Peekable, Snapshot, WriteBatchExt, WriteOptions};
use error_code::ErrorCodeExt;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb;
use kvproto::pdpb::PeerStats;
use kvproto::raft_cmdpb::{
    self, AdminCmdType, AdminResponse, CmdType, CommitMergeRequest, RaftCmdRequest,
    RaftCmdResponse, ReadIndexResponse, Request, Response, TransferLeaderRequest,
    TransferLeaderResponse,
};
use kvproto::raft_serverpb::{
    ExtraMessage, ExtraMessageType, MergeState, PeerState, RaftApplyState, RaftMessage,
};
use protobuf::Message;
use raft::eraftpb::{self, ConfChangeType, EntryType, MessageType};
use raft::{
    self, Progress, ProgressState, RawNode, Ready, SnapshotStatus, StateRole, INVALID_INDEX,
    NO_LIMIT,
};
use time::Timespec;
use txn_types::TxnExtra;
use uuid::Uuid;

use crate::coprocessor::{CoprocessorHost, RegionChangeEvent};
use crate::store::fsm::apply::CatchUpLogs;
use crate::store::fsm::store::PollContext;
use crate::store::fsm::{
    apply, Apply, ApplyMetrics, ApplyTask, CollectedReady, GroupState, Proposal, RegionProposal,
};
use crate::store::worker::{HeartbeatTask, ReadDelegate, ReadProgress, RegionTask};
use crate::store::{Callback, Config, PdTask, ReadResponse, RegionSnapshot};
use crate::{Error, Result};
use pd_client::INVALID_ID;
use tikv_util::collections::{HashMap, HashSet};
use tikv_util::time::Instant as UtilInstant;
use tikv_util::time::{duration_to_sec, monotonic_raw_now};
use tikv_util::worker::Scheduler;
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
    self, check_region_epoch, is_initial_msg, AdminCmdEpochState, Lease, LeaseState,
    ADMIN_CMD_EPOCH_MAP, NORMAL_REQ_CHECK_CONF_VER, NORMAL_REQ_CHECK_VER,
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

/// Meta information about proposals.
pub struct ProposalMeta {
    pub index: u64,
    pub term: u64,
    /// `renew_lease_time` contains the last time when a peer starts to renew lease.
    pub renew_lease_time: Option<Timespec>,
    pub txn_extra: TxnExtra,
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

pub struct ProposedAdminCmd {
    epoch_state: AdminCmdEpochState,
    index: u64,
    cbs: Vec<Callback<RocksEngine>>,
}

impl ProposedAdminCmd {
    fn new(epoch_state: AdminCmdEpochState, index: u64) -> ProposedAdminCmd {
        ProposedAdminCmd {
            epoch_state,
            index,
            cbs: Vec::new(),
        }
    }
}

struct CmdEpochChecker {
    // Although it's a deque, because of the characteristics of the settings from `ADMIN_CMD_EPOCH_MAP`,
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
                .push_back(ProposedAdminCmd::new(epoch_state, index));
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

    pub fn attach_to_conflict_cmd(&mut self, index: u64, cb: Callback<RocksEngine>) {
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
        for state in self.proposed_admin_cmd.drain(..) {
            for cb in state.cbs {
                apply::notify_stale_req(self.term, cb);
            }
        }
    }
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
    /// Indicates whether the peer should be woken up.
    pub should_wake_up: bool,
    /// Whether this peer is destroyed asynchronously.
    /// If it's true,
    /// 1. when merging, its data in storeMeta will be removed early by the target peer.
    /// 2. all read requests must be rejected.
    pub pending_remove: bool,
    /// If a snapshot is being applied asynchronously, messages should not be sent.
    pending_messages: Vec<eraftpb::Message>,

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
    pub approximate_size: u64,
    /// Approximate keys of the region.
    pub approximate_keys: u64,
    /// Whether this region has calculated region size by split-check thread. If we just splitted
    ///  the region or ingested one file which may be overlapped with the existed data, the
    /// `approximate_size` is not very accurate.
    pub has_calculated_region_size: bool,

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
    /// The peers who want to rollback merge
    pub want_rollback_merge_peers: HashSet<u64>,
    /// source region is catching up logs for merge
    pub catch_up_logs: Option<CatchUpLogs>,

    /// Write Statistics for PD to schedule hot spot.
    pub peer_stat: PeerStat,

    /// Time of the last attempt to wake up inactive leader.
    pub bcast_wake_up_time: Option<UtilInstant>,
    /// The known newest conf version and its corresponding peer list
    /// Send to these peers to check whether itself is stale.
    pub check_stale_conf_ver: u64,
    pub check_stale_peers: Vec<metapb::Peer>,
    /// Whether this peer is created by replication and is the first
    /// one of this region on local store.
    pub local_first_replicate: bool,

    pub txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    /// Check whether this proposal can be proposed based on its epoch
    cmd_epoch_checker: CmdEpochChecker,
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
            proposals: Default::default(),
            apply_proposals: vec![],
            pending_reads: Default::default(),
            peer_cache: RefCell::new(HashMap::default()),
            peer_heartbeats: HashMap::default(),
            peers_start_pending_time: vec![],
            down_peer_ids: vec![],
            size_diff_hint: 0,
            delete_keys_hint: 0,
            approximate_size: 0,
            approximate_keys: 0,
            has_calculated_region_size: false,
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
                hash: vec![],
            },
            raft_log_size_hint: 0,
            leader_lease: Lease::new(cfg.raft_store_max_leader_lease()),
            pending_messages: vec![],
            peer_stat: PeerStat::default(),
            catch_up_logs: None,
            bcast_wake_up_time: None,
            check_stale_conf_ver: 0,
            check_stale_peers: vec![],
            local_first_replicate: false,
            txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::Noop)),
            cmd_epoch_checker: Default::default(),
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
    pub fn maybe_destroy<T, C>(&mut self, ctx: &PollContext<T, C>) -> Option<DestroyPeerJob> {
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
        let mut kv_wb = ctx.engines.kv.c().write_batch();
        let mut raft_wb = ctx.engines.raft.c().write_batch();
        self.mut_store().clear_meta(&mut kv_wb, &mut raft_wb)?;
        write_peer_state(
            &mut kv_wb,
            &region,
            PeerState::Tombstone,
            self.pending_merge_state.clone(),
        )?;
        // write kv rocksdb first in case of restart happen between two write
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(ctx.cfg.sync_log);
        ctx.engines.kv.c().write_opt(&kv_wb, &write_opts)?;
        ctx.engines.raft.c().write_opt(&raft_wb, &write_opts)?;

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
                return res;
            }
        }
        if self.raft_group.raft.pending_read_count() > 0 {
            return res;
        }
        if self.raft_group.raft.lead_transferee.is_some() {
            return res;
        }
        // Unapplied entries can change the configuration of the group.
        res.up_to_date = self.get_store().applied_index() == last_index;
        res
    }

    pub fn check_after_tick(&self, state: GroupState, res: CheckTickResult) -> bool {
        if res.leader {
            res.up_to_date && self.is_leader()
        } else {
            // If follower keeps receiving data from leader, then it's safe to stop
            // ticking, as leader will make sure it has the latest logs.
            // Checking term to make sure campaign has finished and the leader starts
            // doing its job, it's not required but a safe options.
            state != GroupState::Chaos
                && self.raft_group.raft.leader_id != raft::INVALID_ID
                && self.raft_group.raft.raft_log.last_term() == self.raft_group.raft.term
                && !self.has_unresolved_reads()
                // If it becomes leader, the stats is not valid anymore.
                && !self.is_leader()
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
    pub fn step<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        mut m: eraftpb::Message,
    ) -> Result<()> {
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
        let index = self.get_store().committed_index();
        if msg_type == MessageType::MsgReadIndex
            && self.get_store().term(index).unwrap_or(0) == self.term()
        {
            // If the leader hasn't committed any entries in its term, it can't response read only
            // requests. Please also take a look at raft-rs.
            let state = self.inspect_lease();
            if let LeaseState::Valid = state {
                let mut resp = eraftpb::Message::default();
                resp.set_msg_type(MessageType::MsgReadIndexResp);
                resp.term = self.term();
                resp.to = m.from;
                resp.index = index;
                resp.set_entries(m.take_entries());

                self.pending_messages.push(resp);
                return Ok(());
            }
            self.should_wake_up = state == LeaseState::Expired;
        }
        if msg_type == MessageType::MsgTransferLeader {
            self.execute_transfer_leader(ctx, &m);
            return Ok(());
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
    pub fn collect_pending_peers<T, C>(&mut self, ctx: &PollContext<T, C>) -> Vec<metapb::Peer> {
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
            self.cmd_epoch_checker.maybe_update_term(self.term());
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

    /// Whether a log can be applied before writing raft batch.
    ///
    /// If TiKV crashes, it's possible apply index > commit index. If logs are still
    /// available in other nodes, it's possible to be recovered. But for singleton, logs are
    /// only available on single node, logs are gone forever.
    ///
    /// Note we can't just check singleton. Because conf change takes effect on apply, so even
    /// there are two nodes, previous logs can still be committed by leader alone. Those logs
    /// can't be applied early. After introducing joint consensus, the node number can be
    /// undetermined. So here check whether log is persisted on disk instead.
    ///
    /// Only apply existing logs has another benefit that we don't need to deal with snapshots
    /// that are older than apply index as apply index <= last index <= index of snapshot.
    pub fn can_early_apply(&self, term: u64, index: u64) -> bool {
        self.get_store().last_index() >= index && self.get_store().last_term() >= term
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
    ) -> Option<CollectedReady> {
        if self.pending_remove {
            return None;
        }
        match self.mut_store().check_applying_snap() {
            CheckApplyingSnapStatus::Applying => {
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
            CheckApplyingSnapStatus::Success => {
                self.post_pending_read_index_on_replica(ctx);
            }
            CheckApplyingSnapStatus::Idle => {}
        }

        if !self.pending_messages.is_empty() {
            fail_point!("raft_before_follower_send");
            let messages = mem::replace(&mut self.pending_messages, vec![]);
            ctx.need_flush_trans = true;
            self.send(&mut ctx.trans, messages, &mut ctx.raft_metrics.message);
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

        if !self
            .raft_group
            .has_ready_since(Some(self.last_applying_idx))
        {
            // Generating snapshot task won't set ready for raft group.
            if let Some(gen_task) = self.mut_store().take_gen_snap_task() {
                self.pending_request_snapshot_count
                    .fetch_add(1, Ordering::SeqCst);
                ctx.apply_router
                    .schedule_task(self.region_id, ApplyTask::Snapshot(gen_task));
            }
            return None;
        }

        let before_handle_raft_ready_1003 = || {
            fail_point!(
                "before_handle_raft_ready_1003",
                self.peer.get_id() == 1003 && self.is_leader(),
                |_| {}
            );
        };
        before_handle_raft_ready_1003();

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

        let mut ready = self.raft_group.ready_since(self.last_applying_idx);

        self.on_role_changed(ctx, &ready);

        self.add_ready_metric(&ready, &mut ctx.raft_metrics.ready);

        if !ready.committed_entries.as_ref().map_or(true, Vec::is_empty) {
            // We must renew current_time because this value may be created a long time ago.
            // If we do not renew it, this time may be smaller than propose_time of a command,
            // which was proposed in another thread while this thread receives its AppendEntriesResponse
            //  and is ready to calculate its commit-log-duration.
            ctx.current_time.replace(monotonic_raw_now());
        }

        if self.is_leader() {
            if let Some(hs) = ready.hs() {
                // Correctness depends on the fact that the leader lease must be suspected before
                // other followers know the `PrepareMerge` log is committed, i.e. sends msg to others.
                // Because other followers may complete the merge process, if so, the source region's
                // leader may get a stale data.
                //
                // Check the committed entries.
                // TODO: It can change to not rely on the `committed_entries` must have the latest committed entry
                // and become O(1) by maintaining these not-committed admin requests that changes epoch.
                if hs.get_commit() > self.get_store().committed_index() {
                    assert_eq!(
                        ready
                            .committed_entries
                            .as_ref()
                            .unwrap()
                            .last()
                            .unwrap()
                            .index,
                        hs.get_commit()
                    );
                    let mut split_to_be_updated = true;
                    let mut merge_to_be_updated = true;
                    for entry in ready.committed_entries.as_ref().unwrap().iter().rev() {
                        // We care about split/merge commands that are committed in the current term.
                        if entry.term == self.term() && (split_to_be_updated || merge_to_be_updated)
                        {
                            let ctx = ProposalContext::from_bytes(&entry.context);
                            if split_to_be_updated && ctx.contains(ProposalContext::SPLIT) {
                                // We don't need to suspect its lease because peers of new region that
                                // in other store do not start election before theirs election timeout
                                // which is longer than the max leader lease.
                                // It's safe to read local within its current lease, however, it's not
                                // safe to renew its lease.
                                self.last_committed_split_idx = entry.index;
                                split_to_be_updated = false;
                            } else if merge_to_be_updated
                                && ctx.contains(ProposalContext::PREPARE_MERGE)
                            {
                                // We committed prepare merge, to prevent unsafe read index,
                                // we must record its index.
                                self.last_committed_prepare_merge_idx = entry.get_index();
                                // After prepare_merge is committed, the leader can not know
                                // when the target region merges majority of this region, also
                                // it can not know when the target region writes new values.
                                // To prevent unsafe local read, we suspect its leader lease.
                                self.leader_lease.suspect(monotonic_raw_now());
                                merge_to_be_updated = false;
                            }
                        }
                    }
                }
            }
            // The leader can write to disk and replicate to the followers concurrently
            // For more details, check raft thesis 10.2.1.
            fail_point!("raft_before_leader_send");
            let msgs = ready.messages.drain(..);
            ctx.need_flush_trans = true;
            self.send(&mut ctx.trans, msgs, &mut ctx.raft_metrics.message);
        }

        let invoke_ctx = match self
            .mut_store()
            .handle_raft_ready(ctx, &ready, destroy_regions)
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

    pub fn handle_raft_ready_apply<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        ready: &mut CollectedReady,
    ) {
        // Call `handle_raft_committed_entries` directly here may lead to inconsistency.
        // In some cases, there will be some pending committed entries when applying a
        // snapshot. If we call `handle_raft_committed_entries` directly, these updates
        // will be written to disk. Because we apply snapshot asynchronously, so these
        // updates will soon be removed. But the soft state of raft is still be updated
        // in memory. Hence when handle ready next time, these updates won't be included
        // in `ready.committed_entries` again, which will lead to inconsistency.
        if raft::is_empty_snap(ready.ready.snapshot()) {
            debug_assert!(!ready.ctx.has_snapshot() && !self.get_store().is_applying_snapshot());
            let committed_entries = ready.ready.committed_entries.take().unwrap();
            // leader needs to update lease and last committed split index.
            let mut lease_to_be_updated = self.is_leader();
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

                fail_point!(
                    "before_send_rollback_merge_1003",
                    if self.peer_id() != 1003 {
                        false
                    } else {
                        let index = entry.get_index();
                        let data = entry.get_data();
                        if data.is_empty() || entry.get_entry_type() != EntryType::EntryNormal {
                            false
                        } else {
                            let cmd: RaftCmdRequest = util::parse_data_at(data, index, &self.tag);
                            cmd.has_admin_request()
                                && cmd.get_admin_request().get_cmd_type()
                                    == AdminCmdType::RollbackMerge
                        }
                    },
                    |_| {}
                );
            }
            if !committed_entries.is_empty() {
                self.last_applying_idx = committed_entries.last().unwrap().get_index();
                if self.last_applying_idx >= self.last_urgent_proposal_idx {
                    // Urgent requests are flushed, make it lazy again.
                    self.raft_group.skip_bcast_commit(true);
                    self.last_urgent_proposal_idx = u64::MAX;
                }
                let committed_index = self.raft_group.raft.raft_log.committed;
                let term = self.raft_group.raft.raft_log.term(committed_index).unwrap();
                let apply = Apply::new(
                    self.region_id,
                    self.term(),
                    committed_entries,
                    self.get_store().committed_index(),
                    term,
                    committed_index,
                );
                ctx.apply_router
                    .schedule_task(self.region_id, ApplyTask::apply(apply));
            }
            fail_point!("after_send_to_apply_1003", self.peer_id() == 1003, |_| {});
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

        self.apply_reads(ctx, &ready.ready);
    }

    pub fn handle_raft_ready_advance(&mut self, ready: Ready) {
        if !raft::is_empty_snap(ready.snapshot()) {
            // Snapshot's metadata has been applied.
            self.last_applying_idx = self.get_store().truncated_index();
            self.raft_group.advance_append(ready);
            // Because we only handle raft ready when not applying snapshot, so following
            // line won't be called twice for the same snapshot.
            self.raft_group.advance_apply(self.last_applying_idx);
            self.cmd_epoch_checker.advance_apply(
                self.last_applying_idx,
                self.term(),
                self.raft_group.store().region(),
            );
        } else {
            self.raft_group.advance_append(ready);
        }
        self.proposals.gc();
    }

    fn response_read<T, C>(
        &self,
        read: &mut ReadIndexRequest,
        ctx: &mut PollContext<T, C>,
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
    fn post_pending_read_index_on_replica<T, C>(&mut self, ctx: &mut PollContext<T, C>) {
        while let Some(mut read) = self.pending_reads.pop_front() {
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

    fn apply_reads<T, C>(&mut self, ctx: &mut PollContext<T, C>, ready: &Ready) {
        let mut propose_time = None;
        let states = ready.read_states().iter().map(|state| {
            let uuid = Uuid::from_slice(state.request_ctx.as_slice()).unwrap();
            (uuid, state.index)
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
    pub fn propose<T: Transport, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        mut cb: Callback<RocksEngine>,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        txn_extra: TxnExtra,
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
                if self.has_applied_to_current_term() {
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
                let meta = ProposalMeta {
                    index: idx,
                    term: self.term(),
                    txn_extra,
                    renew_lease_time: None,
                };
                if let Some(cmd_type) = req_admin_cmd_type {
                    self.cmd_epoch_checker
                        .post_propose(cmd_type, idx, self.term());
                }
                self.post_propose(
                    ctx,
                    meta,
                    req_admin_cmd_type == Some(AdminCmdType::ChangePeer),
                    cb,
                );
                true
            }
        }
    }

    fn post_propose<T, C>(
        &mut self,
        poll_ctx: &mut PollContext<T, C>,
        mut meta: ProposalMeta,
        is_conf_change: bool,
        cb: Callback<RocksEngine>,
    ) {
        // Try to renew leader lease on every consistent read/write request.
        if poll_ctx.current_time.is_none() {
            poll_ctx.current_time = Some(monotonic_raw_now());
        }
        meta.renew_lease_time = poll_ctx.current_time;

        if !cb.is_none() {
            let p = Proposal::new(
                is_conf_change,
                meta.index,
                meta.term,
                cb,
                meta.txn_extra.clone(),
            );
            self.apply_proposals.push(p);
        }

        self.proposals.push(meta);
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
        &mut self,
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
                return Err(box_err!("{} invalid conf change request", self.tag));
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
            return Err(box_err!("{} ignore remove leader", self.tag));
        }

        let (total, mut progress) = {
            let status = self.raft_group.status();
            let total = status.progress.unwrap().voter_ids().len();
            if total == 1 {
                // It's always safe if there is only one node in the cluster.
                return Ok(());
            }
            (total, status.progress.unwrap().clone())
        };

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
        }
        let promoted_commit_index = progress.maximal_committed_index();
        if promoted_commit_index >= self.get_store().truncated_index() {
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
            "after" => progress.voter_ids().len(),
            "truncated_index" => self.get_store().truncated_index(),
            "promoted_commit_index" => promoted_commit_index,
        );
        // Waking it up to replicate logs to candidate.
        self.should_wake_up = true;
        Err(box_err!(
            "unsafe to perform conf change {:?}, total {}, truncated index {}, promoted commit index {}",
            change_peer,
            total,
            self.get_store().truncated_index(),
            promoted_commit_index
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

    fn ready_to_transfer_leader<T, C>(
        &self,
        ctx: &mut PollContext<T, C>,
        mut index: u64,
        peer: &metapb::Peer,
    ) -> Option<&'static str> {
        let peer_id = peer.get_id();
        let status = self.raft_group.status();
        let progress = status.progress.unwrap();

        if !progress.voter_ids().contains(&peer_id) {
            return Some("non voter");
        }

        for (id, progress) in progress.voters() {
            if progress.state == ProgressState::Snapshot {
                return Some("pending snapshot");
            }
            if *id == peer_id && index == 0 {
                // index will be zero if it's sent from an instance without
                // pre-transfer-leader feature. Set it to matched to make it
                // possible to transfer leader to an older version. It may be
                // useful during rolling restart.
                index = progress.matched;
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

    fn read_local<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
        cb: Callback<RocksEngine>,
    ) {
        ctx.raft_metrics.propose.local_read += 1;
        cb.invoke_read(self.handle_read(ctx, req, false, Some(self.get_store().committed_index())))
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
        self.raft_group.read_index(read.id.as_bytes().to_vec());
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
    fn read_index<T: Transport, C>(
        &mut self,
        poll_ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
        mut err_resp: RaftCmdResponse,
        cb: Callback<RocksEngine>,
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
                    let committed_index = self.get_store().committed_index();
                    if let Some(read) = self.pending_reads.back_mut() {
                        let max_lease = poll_ctx.cfg.raft_store_max_leader_lease();
                        if read.renew_lease_time + max_lease > renew_lease_time {
                            read.push_command(req, cb, committed_index);
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
        self.raft_group.read_index(id.as_bytes().to_vec());

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

        let read = ReadIndexRequest::with_command(id, req, cb, renew_lease_time);
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
                let meta = ProposalMeta {
                    index,
                    term: self.term(),
                    txn_extra: TxnExtra::default(),
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
    fn propose_normal<T, C>(
        &mut self,
        poll_ctx: &mut PollContext<T, C>,
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

    fn execute_transfer_leader<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
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

        if self.is_applying_snapshot()
            || self.has_pending_snapshot()
            || msg.get_from() != self.leader_id()
        {
            info!(
                "reject transferring leader";
                "region_id" =>self.region_id,
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
    fn propose_transfer_leader<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
        cb: Callback<RocksEngine>,
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
    fn propose_conf_change<T, C>(
        &mut self,
        ctx: &mut PollContext<T, C>,
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

        Ok(Either::Left(propose_index))
    }

    fn handle_read<T, C>(
        &self,
        ctx: &mut PollContext<T, C>,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> ReadResponse<RocksEngine> {
        let mut resp = ReadExecutor::new(
            ctx.engines.kv.c().clone(),
            check_epoch,
            false, /* we don't need snapshot time */
        )
        .execute(&req, self.region(), read_index);
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
        let task = PdTask::Heartbeat(HeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(ctx.cfg.max_peer_down_duration.0),
            pending_peers: self.collect_pending_peers(ctx),
            written_bytes: self.peer_stat.written_bytes,
            written_keys: self.peer_stat.written_keys,
            approximate_size: self.approximate_size,
            approximate_keys: self.approximate_keys,
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

    pub fn bcast_wake_up_message<T: Transport, C>(&self, ctx: &mut PollContext<T, C>) {
        for peer in self.region().get_peers() {
            if peer.get_id() == self.peer_id() {
                continue;
            }
            self.send_wake_up_message(ctx, peer);
        }
    }

    pub fn send_wake_up_message<T: Transport, C>(
        &self,
        ctx: &mut PollContext<T, C>,
        peer: &metapb::Peer,
    ) {
        let mut send_msg = RaftMessage::default();
        send_msg.set_region_id(self.region_id);
        send_msg.set_from_peer(self.peer.clone());
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        send_msg.set_to_peer(peer.clone());
        let extra_msg = send_msg.mut_extra_msg();
        extra_msg.set_type(ExtraMessageType::MsgRegionWakeUp);
        if let Err(e) = ctx.trans.send(send_msg) {
            error!(?e;
                "failed to send wake up message";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "target_peer_id" => peer.get_id(),
                "target_store_id" => peer.get_store_id(),
            );
        } else {
            ctx.need_flush_trans = true;
        }
    }

    pub fn bcast_check_stale_peer_message<T: Transport, C>(&mut self, ctx: &mut PollContext<T, C>) {
        if self.check_stale_conf_ver < self.region().get_region_epoch().get_conf_ver() {
            self.check_stale_conf_ver = self.region().get_region_epoch().get_conf_ver();
            self.check_stale_peers = self.region().get_peers().to_vec();
        }
        for peer in &self.check_stale_peers {
            if peer.get_id() == self.peer_id() {
                continue;
            }
            let mut send_msg = RaftMessage::default();
            send_msg.set_region_id(self.region_id);
            send_msg.set_from_peer(self.peer.clone());
            send_msg.set_region_epoch(self.region().get_region_epoch().clone());
            send_msg.set_to_peer(peer.clone());
            let extra_msg = send_msg.mut_extra_msg();
            extra_msg.set_type(ExtraMessageType::MsgCheckStalePeer);
            if let Err(e) = ctx.trans.send(send_msg) {
                error!(?e;
                    "failed to send check stale peer message";
                    "region_id" => self.region_id,
                    "peer_id" => self.peer.get_id(),
                    "target_peer_id" => peer.get_id(),
                    "target_store_id" => peer.get_store_id(),
                );
            } else {
                ctx.need_flush_trans = true;
            }
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

    pub fn send_want_rollback_merge<T: Transport, C>(
        &self,
        premerge_commit: u64,
        ctx: &mut PollContext<T, C>,
    ) {
        let mut send_msg = RaftMessage::default();
        send_msg.set_region_id(self.region_id);
        send_msg.set_from_peer(self.peer.clone());
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
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
        send_msg.set_to_peer(to_peer.clone());
        let extra_msg = send_msg.mut_extra_msg();
        extra_msg.set_type(ExtraMessageType::MsgWantRollbackMerge);
        extra_msg.set_premerge_commit(premerge_commit);
        if let Err(e) = ctx.trans.send(send_msg) {
            error!(?e;
                "failed to send want rollback merge message";
                "region_id" => self.region_id,
                "peer_id" => self.peer.get_id(),
                "target_peer_id" => to_peer.get_id(),
                "target_store_id" => to_peer.get_store_id(),
            );
        } else {
            ctx.need_flush_trans = true;
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

#[derive(Debug)]
pub struct ReadExecutor<E: KvEngine> {
    check_epoch: bool,
    engine: E,
    snapshot: Option<<E::Snapshot as Snapshot<E>>::SyncSnapshot>,
    snapshot_time: Option<Timespec>,
    need_snapshot_time: bool,
}

impl<E> ReadExecutor<E>
where
    E: KvEngine,
{
    pub fn new(engine: E, check_epoch: bool, need_snapshot_time: bool) -> Self {
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
        self.snapshot = Some(self.engine.snapshot().into_sync());
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
            snapshot
                .get_value_cf(cf, &keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to get {} with cf {}: {:?}",
                        region.get_id(),
                        &log_wrappers::Value::key(key),
                        cf,
                        e
                    )
                })
        } else {
            snapshot
                .get_value(&keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to get {}: {:?}",
                        region.get_id(),
                        &log_wrappers::Value::key(key),
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
    ) -> ReadResponse<E> {
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
                    txn_extra_op: TxnExtraOp::Noop,
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
                            txn_extra_op: TxnExtraOp::Noop,
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
        ReadResponse {
            response,
            snapshot,
            txn_extra_op: TxnExtraOp::Noop,
        }
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
    #[cfg(feature = "protobuf-codec")]
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
    fn test_cmd_epoch_checker() {
        use std::sync::mpsc;
        fn new_admin_request(cmd_type: AdminCmdType) -> RaftCmdRequest {
            let mut request = RaftCmdRequest::default();
            request.mut_admin_request().set_cmd_type(cmd_type);
            request
        }
        fn new_cb() -> (Callback<RocksEngine>, mpsc::Receiver<()>) {
            let (tx, rx) = mpsc::channel();
            (Callback::write(Box::new(move |_| tx.send(()).unwrap())), rx)
        }

        let region = metapb::Region::default();
        let normal_cmd = RaftCmdRequest::default();
        let split_admin = new_admin_request(AdminCmdType::BatchSplit);
        let prepare_merge_admin = new_admin_request(AdminCmdType::PrepareMerge);
        let change_peer_admin = new_admin_request(AdminCmdType::ChangePeer);

        let mut epoch_checker = CmdEpochChecker::default();

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
