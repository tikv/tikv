// Copyright 2016 PingCAP, Inc.
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

use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::{mem, slice};
use std::time::{Instant, Duration};

use time::Timespec;
use rocksdb::{DB, WriteBatch};
use protobuf::{self, Message, MessageStatic};
use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, CmdType, AdminCmdType, AdminResponse,
                          TransferLeaderRequest, TransferLeaderResponse};
use kvproto::raft_serverpb::{RaftMessage, PeerState};
use kvproto::pdpb::PeerStats;

use raft::{self, RawNode, StateRole, SnapshotStatus, Ready, ProgressState, Progress, INVALID_INDEX};
use raftstore::{Result, Error};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::Config;
use raftstore::store::worker::{apply, PdTask, Proposal};
use raftstore::store::worker::apply::ExecResult;

use util::worker::{FutureWorker, Scheduler, Worker};
use raftstore::store::worker::{ApplyTask, ApplyRes, Apply};
use util::{clocktime, Either};
use util::collections::{HashSet, FlatMap, FlatMapValues as Values};

use pd::INVALID_ID;

use super::store::Store;
use super::peer_storage::{PeerStorage, ApplySnapResult, write_peer_state, InvokeContext};
use super::util;
use super::msg::Callback;
use super::cmd_resp;
use super::transport::Transport;
use super::engine::Snapshot;
use super::metrics::*;
use super::local_metrics::{RaftReadyMetrics, RaftMessageMetrics, RaftProposeMetrics, RaftMetrics};

const TRANSFER_LEADER_ALLOW_LOG_LAG: u64 = 10;
const DEFAULT_APPEND_WB_SIZE: usize = 4 * 1024;

struct ReadIndexRequest {
    id: u64,
    cmds: Vec<(RaftCmdRequest, Callback)>,
    renew_lease_time: Timespec,
}

impl ReadIndexRequest {
    fn binary_id(&self) -> &[u8] {
        unsafe {
            let id = &self.id as *const u64 as *const u8;
            slice::from_raw_parts(id, 8)
        }
    }
}

impl Drop for ReadIndexRequest {
    fn drop(&mut self) {
        if !self.cmds.is_empty() {
            panic!("callback of index read at {} is leak.", self.id);
        }
    }
}

#[derive(Default)]
struct ReadIndexQueue {
    id_allocator: u64,
    reads: VecDeque<ReadIndexRequest>,
    ready_cnt: usize,
}

impl ReadIndexQueue {
    fn next_id(&mut self) -> u64 {
        self.id_allocator += 1;
        self.id_allocator
    }

    fn clear_uncommitted(&mut self, term: u64) {
        for mut read in self.reads.drain(self.ready_cnt..) {
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_stale_req(term, cb);
            }
        }
    }
}

/// The returned states of the peer after checking whether it is stale
#[derive(Debug)]
pub enum StaleState {
    Valid,
    ToValidate,
}

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
}

pub struct ReadyContext<'a, T: 'a> {
    pub wb: WriteBatch,
    pub metrics: &'a mut RaftMetrics,
    pub trans: &'a T,
    pub ready_res: Vec<(Ready, InvokeContext)>,
}

impl<'a, T> ReadyContext<'a, T> {
    pub fn new(metrics: &'a mut RaftMetrics, t: &'a T, cap: usize) -> ReadyContext<'a, T> {
        ReadyContext {
            wb: WriteBatch::with_capacity(DEFAULT_APPEND_WB_SIZE),
            metrics: metrics,
            trans: t,
            ready_res: Vec::with_capacity(cap),
        }
    }
}

// TODO: make sure received entries are not corrupted
// If this happens, TiKV will panic and can't recover without extra effort.
#[inline]
pub fn parse_data_at<T: Message + MessageStatic>(data: &[u8], index: u64, tag: &str) -> T {
    protobuf::parse_from_bytes::<T>(data).unwrap_or_else(|e| {
        panic!("{} data is corrupted at {}: {:?}", tag, index, e);
    })
}

pub struct ConsistencyState {
    pub last_check_time: Instant,
    // (computed_result_or_to_be_verified, index, hash)
    pub index: u64,
    pub hash: Vec<u8>,
}

enum RequestPolicy {
    // Handle the read request directly without dispatch.
    ReadLocal,
    // Handle the read request via raft's SafeReadIndex mechanism.
    ReadIndex,
    ProposeNormal,
    ProposeTransferLeader,
    ProposeConfChange,
}

#[derive(Default, Clone)]
pub struct PeerStat {
    pub written_bytes: u64,
    pub written_keys: u64,
    pub last_written_bytes: u64,
    pub last_written_keys: u64,
}

pub struct Peer {
    engine: Arc<DB>,
    cfg: Rc<Config>,
    peer_cache: RefCell<FlatMap<u64, metapb::Peer>>,
    pub peer: metapb::Peer,
    region_id: u64,
    pub raft_group: RawNode<PeerStorage>,
    proposals: ProposalQueue,
    apply_proposals: Vec<Proposal>,
    pending_reads: ReadIndexQueue,
    // Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: FlatMap<u64, Instant>,
    coprocessor_host: Arc<CoprocessorHost>,
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: u64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub consistency_state: ConsistencyState,

    pub tag: String,

    // Index of last scheduled committed raft log.
    pub last_applying_idx: u64,
    pub last_compacted_idx: u64,
    // Approximate size of logs that is applied but not compacted yet.
    pub raft_log_size_hint: u64,
    // When entry exceed max size, reject to propose the entry.
    pub raft_entry_max_size: u64,

    apply_scheduler: Scheduler<ApplyTask>,

    pub pending_remove: bool,

    marked_to_be_checked: bool,

    leader_missing_time: Option<Instant>,

    // `leader_lease_expired_time` contains either timestamps of
    //   1. Either::Left<Timespec>
    //      A safe leader lease expired time, which marks the leader holds the lease for now.
    //      The lease is safe until the clock time goes over this timestamp.
    //      It would increase when raft log entries are applied in current term.
    //   2. Either::Right<Timespec>
    //      An unsafe leader lease expired time, which marks the leader may still hold or lose
    //      its lease until the clock time goes over this timestamp.
    //      It would be set after the message MsgTimeoutNow is sent by current peer.
    //      The message MsgTimeoutNow starts a leader transfer procedure. During this procedure,
    //      current peer as an old leader may still hold its lease or lose it.
    //      It's possible there is a new leader elected and current peer as an old leader
    //      doesn't step down due to network partition from the new leader. In that case,
    //      current peer lose its leader lease.
    //      Within this unsafe leader lease expire time, read requests could not be performed
    //      locally.
    leader_lease_expired_time: Option<Either<Timespec, Timespec>>,

    pub peer_stat: PeerStat,
}

impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create<T, C>(store: &mut Store<T, C>, region: &metapb::Region) -> Result<Peer> {
        let store_id = store.store_id();
        let peer_id = match util::find_peer(region, store_id) {
            None => {
                return Err(box_err!("find no peer for store {} in region {:?}", store_id, region))
            }
            Some(peer) => peer.get_id(),
        };

        info!("[region {}] create peer with id {}",
              region.get_id(),
              peer_id);
        Peer::new(store, region, peer_id)
    }

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after applying snapshot.
    pub fn replicate<T, C>(store: &mut Store<T, C>, region_id: u64, peer_id: u64) -> Result<Peer> {
        // We will remove tombstone key when apply snapshot
        info!("[region {}] replicate peer with id {}", region_id, peer_id);

        let mut region = metapb::Region::new();
        region.set_id(region_id);
        Peer::new(store, &region, peer_id)
    }

    fn new<T, C>(store: &mut Store<T, C>, region: &metapb::Region, peer_id: u64) -> Result<Peer> {
        if peer_id == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let cfg = store.config();

        let store_id = store.store_id();
        let sched = store.snap_scheduler();
        let peer_cache = FlatMap::default();
        let tag = format!("[region {}] {}", region.get_id(), peer_id);

        let ps = try!(PeerStorage::new(store.engine(), region, sched, tag.clone()));

        let applied_index = ps.applied_index();

        let raft_cfg = raft::Config {
            id: peer_id,
            peers: vec![],
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            tag: tag.clone(),
            skip_bcast_commit: true,
            ..Default::default()
        };

        let raft_group = try!(RawNode::new(&raft_cfg, ps, &[]));

        let mut peer = Peer {
            engine: store.engine(),
            peer: util::new_peer(store_id, peer_id),
            region_id: region.get_id(),
            raft_group: raft_group,
            proposals: Default::default(),
            apply_proposals: vec![],
            pending_reads: Default::default(),
            peer_cache: RefCell::new(peer_cache),
            peer_heartbeats: FlatMap::default(),
            coprocessor_host: store.coprocessor_host.clone(),
            size_diff_hint: 0,
            delete_keys_hint: 0,
            apply_scheduler: store.apply_scheduler(),
            pending_remove: false,
            marked_to_be_checked: false,
            leader_missing_time: Some(Instant::now()),
            tag: tag,
            last_applying_idx: applied_index,
            last_compacted_idx: 0,
            consistency_state: ConsistencyState {
                last_check_time: Instant::now(),
                index: INVALID_INDEX,
                hash: vec![],
            },
            raft_log_size_hint: 0,
            raft_entry_max_size: cfg.raft_entry_max_size,
            cfg: cfg,
            leader_lease_expired_time: None,
            peer_stat: PeerStat::default(),
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    #[inline]
    fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    pub fn mark_to_be_checked(&mut self, pending_raft_groups: &mut HashSet<u64>) {
        if !self.marked_to_be_checked {
            self.marked_to_be_checked = true;
            pending_raft_groups.insert(self.region_id);
        }
    }

    pub fn destroy(&mut self) -> Result<()> {
        let t = Instant::now();

        let region = self.get_store().get_region().clone();
        info!("{} begin to destroy", self.tag);

        // Set Tombstone state explicitly
        let wb = WriteBatch::new();
        try!(self.get_store().clear_meta(&wb));
        try!(write_peer_state(&wb, &region, PeerState::Tombstone));
        try!(self.engine.write(wb));

        if self.get_store().is_initialized() {
            // If we meet panic when deleting data and raft log, the dirty data
            // will be cleared by a newer snapshot applying or restart.
            if let Err(e) = self.get_store().clear_data() {
                error!("{} failed to schedule clear data task: {:?}", self.tag, e);
            }
        }

        for mut read in self.pending_reads.reads.drain(..) {
            for (_, cb) in read.cmds.drain(..) {
                apply::notify_req_region_removed(region.get_id(), cb);
            }
        }

        for proposal in self.apply_proposals.drain(..) {
            apply::notify_req_region_removed(region.get_id(), proposal.cb);
        }

        info!("{} destroy itself, takes {:?}", self.tag, t.elapsed());

        Ok(())
    }

    pub fn is_initialized(&self) -> bool {
        self.get_store().is_initialized()
    }

    pub fn engine(&self) -> Arc<DB> {
        self.engine.clone()
    }

    pub fn region(&self) -> &metapb::Region {
        self.get_store().get_region()
    }

    pub fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
    }

    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
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

    #[inline]
    pub fn has_pending_snapshot(&self) -> bool {
        self.raft_group.get_snap().is_some()
    }

    fn add_ready_metric(&self, ready: &Ready, metrics: &mut RaftReadyMetrics) {
        metrics.message += ready.messages.len() as u64;
        metrics.commit += ready.committed_entries.as_ref().map_or(0, |v| v.len() as u64);
        metrics.append += ready.entries.len() as u64;

        if !raft::is_empty_snap(&ready.snapshot) {
            metrics.snapshot += 1;
        }
    }

    #[inline]
    fn send<T, I>(&mut self, trans: &T, msgs: I, metrics: &mut RaftMessageMetrics) -> Result<()>
        where T: Transport,
              I: IntoIterator<Item = eraftpb::Message>
    {
        for msg in msgs {
            let msg_type = msg.get_msg_type();

            try!(self.send_raft_message(msg, trans));

            match msg_type {
                MessageType::MsgAppend => metrics.append += 1,
                MessageType::MsgAppendResponse => metrics.append_resp += 1,
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
                    // For lease safty during leader transfer, mark `leader_lease_expired_time`
                    // to be unsafe until next_lease_expired_time from now
                    self.leader_lease_expired_time =
                        Some(Either::Right(self.next_lease_expired_time(clocktime::raw_now())));

                    metrics.timeout_now += 1;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn step(&mut self, m: eraftpb::Message) -> Result<()> {
        if self.is_leader() && m.get_from() != INVALID_ID {
            self.peer_heartbeats.insert(m.get_from(), Instant::now());
        }
        try!(self.raft_group.step(m));
        Ok(())
    }

    pub fn check_peers(&mut self) {
        if !self.is_leader() {
            self.peer_heartbeats.clear();
            return;
        }

        if self.peer_heartbeats.len() == self.region().get_peers().len() {
            return;
        }

        // Insert heartbeats in case that some peers never response heartbeats.
        for peer in self.region().get_peers().to_owned() {
            self.peer_heartbeats.entry(peer.get_id()).or_insert_with(Instant::now);
        }
    }

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

    pub fn collect_pending_peers(&self) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group.status();
        let truncated_idx = self.get_store().truncated_index();
        for (id, progress) in status.progress {
            if id == self.peer.get_id() {
                continue;
            }
            if progress.matched < truncated_idx {
                if let Some(p) = self.get_peer_from_cache(id) {
                    pending_peers.push(p);
                }
            }
        }
        pending_peers
    }

    pub fn check_stale_state(&mut self, d: Duration) -> StaleState {
        // Updates the `leader_missing_time` according to the current state.
        if self.leader_id() == raft::INVALID_ID {
            if self.leader_missing_time.is_none() {
                self.leader_missing_time = Some(Instant::now())
            }
        } else if self.is_initialized() {
            // A peer is considered as in the leader missing state if it's uninitialized or
            // if it's initialized but is isolated from its leader.
            // For an uninitialized peer, even if its leader sends heartbeats to it,
            // it cannot successfully receive the snapshot from the leader and apply the snapshot.
            // The raft state machine cannot work in an uninitialized peer to detect
            // if the leader is working.
            self.leader_missing_time = None
        }

        // Checks whether the current peer is stale.
        let duration = match self.leader_missing_time {
            Some(t) => t.elapsed(),
            None => Duration::new(0, 0),
        };
        if duration >= d {
            // Resets the `leader_missing_time` to avoid sending the same tasks to
            // PD worker continuously during the leader missing timeout.
            self.leader_missing_time = None;
            return StaleState::ToValidate;
        }
        StaleState::Valid
    }

    fn next_lease_expired_time(&self, send_to_quorum_ts: Timespec) -> Timespec {
        // The valid leader lease should be
        // "lease = max_lease - (quorum_commit_ts - send_to_quorum_ts)"
        // And the expired timestamp for that leader lease is "quorum_commit_ts + lease",
        // which is "send_to_quorum_ts + max_lease" in short.
        send_to_quorum_ts + self.cfg.raft_store_max_leader_lease
    }

    fn on_role_changed(&mut self, ready: &Ready, worker: &FutureWorker<PdTask>) {
        // Update leader lease when the Raft state changes.
        if let Some(ref ss) = ready.ss {
            match ss.raft_state {
                StateRole::Leader => {
                    // The local read can only be performed after a new leader has applied
                    // the first empty entry on its term. After that the lease expiring time
                    // should be updated to
                    //   send_to_quorum_ts + max_lease
                    // as the comments in `next_lease_expired_time` function explain.
                    // It is recommended to update the lease expiring time right after
                    // this peer becomes leader because it's more convenient to do it here and
                    // it has no impact on the correctness.
                    let next_expired_time = self.next_lease_expired_time(clocktime::raw_now());
                    self.leader_lease_expired_time = Some(Either::Left(next_expired_time));
                    debug!("{} becomes leader and lease expired time is {:?}",
                           self.tag,
                           next_expired_time);
                    self.heartbeat_pd(worker)
                }
                StateRole::Follower => {
                    self.leader_lease_expired_time = None;
                }
                _ => {}
            }
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
    }

    #[inline]
    pub fn ready_to_handle_read(&self) -> bool {
        // If applied_index_term isn't equal to current term, there may be some values that are not
        // applied by this leader yet but the old leader.
        self.get_store().applied_index_term == self.term()
    }

    pub fn schedule_apply_proposals(&mut self, worker: &Worker<ApplyTask>) {
        if self.apply_proposals.is_empty() {
            return;
        }

        let proposals = mem::replace(&mut self.apply_proposals, vec![]);
        let t = ApplyTask::proposals(self.peer_id(), self.region_id, proposals);
        worker.schedule(t).unwrap();
    }

    pub fn handle_raft_ready_append<T: Transport>(&mut self,
                                                  ctx: &mut ReadyContext<T>,
                                                  worker: &FutureWorker<PdTask>) {
        self.marked_to_be_checked = false;
        if self.pending_remove {
            return;
        }
        if self.mut_store().check_applying_snap() {
            // If we continue to handle all the messages, it may cause too many messages because
            // leader will send all the remaining messages to this follower, which can lead
            // to full message queue under high load.
            debug!("{} still applying snapshot, skip further handling.",
                   self.tag);
            return;
        }

        if self.has_pending_snapshot() && !self.ready_to_handle_pending_snap() {
            debug!("{} [apply_idx: {}, last_applying_idx: {}] is not ready to apply snapshot.",
                   self.tag,
                   self.get_store().applied_index(),
                   self.last_applying_idx);
            return;
        }

        if !self.raft_group.has_ready_since(Some(self.last_applying_idx)) {
            return;
        }

        debug!("{} handle raft ready", self.tag);

        let mut ready = self.raft_group.ready_since(self.last_applying_idx);

        self.on_role_changed(&ready, worker);

        self.add_ready_metric(&ready, &mut ctx.metrics.ready);

        // The leader can write to disk and replicate to the followers concurrently
        // For more details, check raft thesis 10.2.1.
        if self.is_leader() {
            let msgs = ready.messages.drain(..);
            self.send(ctx.trans, msgs, &mut ctx.metrics.message).unwrap_or_else(|e| {
                // We don't care that the message is sent failed, so here just log this error.
                warn!("{} leader send messages err {:?}", self.tag, e);
            });
        }

        let invoke_ctx = match self.mut_store().handle_raft_ready(ctx, &ready) {
            Ok(r) => r,
            Err(e) => {
                // We may have written something to writebatch and it can't be reverted, so has
                // to panic here.
                panic!("{} failed to handle raft ready: {:?}", self.tag, e);
            }
        };

        ctx.ready_res.push((ready, invoke_ctx));
    }

    pub fn post_raft_ready_append<T: Transport>(&mut self,
                                                metrics: &mut RaftMetrics,
                                                trans: &T,
                                                ready: &mut Ready,
                                                invoke_ctx: InvokeContext)
                                                -> Option<ApplySnapResult> {
        if invoke_ctx.has_snapshot() {
            // When apply snapshot, there is no log applied and not compacted yet.
            self.raft_log_size_hint = 0;
        }

        let apply_snap_result = self.mut_store().post_ready(invoke_ctx);

        if !self.is_leader() {
            self.send(trans, ready.messages.drain(..), &mut metrics.message).unwrap_or_else(|e| {
                warn!("{} follower send messages err {:?}", self.tag, e);
            });
        }

        if apply_snap_result.is_some() {
            let reg = ApplyTask::register(self);
            self.apply_scheduler.schedule(reg).unwrap();
        }

        apply_snap_result
    }

    pub fn handle_raft_ready_apply(&mut self, mut ready: Ready, apply_tasks: &mut Vec<Apply>) {
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
            // leader needs to update lease.
            let mut to_be_updated = self.is_leader();
            if !to_be_updated {
                // It's not leader anymore, we are safe to clear proposals. If it becomes leader
                // again, the lease should be updated when election is finished, old proposals
                // have no effect.
                self.proposals.clear();
            }
            for entry in committed_entries.iter().rev() {
                // raft meta is very small, can be ignored.
                self.raft_log_size_hint += entry.get_data().len() as u64;
                if to_be_updated {
                    to_be_updated = !self.maybe_update_lease(entry);
                }
            }
            if !committed_entries.is_empty() {
                self.last_applying_idx = committed_entries.last().unwrap().get_index();
                apply_tasks.push(Apply::new(self.region_id, self.term(), committed_entries));
            }
        }

        self.apply_reads(&ready);

        self.raft_group.advance_append(ready);
        if self.is_applying_snapshot() {
            // Because we only handle raft ready when not applying snapshot, so following
            // line won't be called twice for the same snapshot.
            self.raft_group.advance_apply(self.last_applying_idx);
        }
    }

    fn apply_reads(&mut self, ready: &Ready) {
        let mut propose_time = None;
        if self.ready_to_handle_read() {
            for state in &ready.read_states {
                let mut read = self.pending_reads.reads.pop_front().unwrap();
                assert_eq!(state.request_ctx.as_slice(), read.binary_id());
                for (req, cb) in read.cmds.drain(..) {
                    // TODO: we should add test case that a split happens before pending
                    // read-index is handled. To do this we need to control async-apply
                    // procedure precisely.
                    self.handle_read(req, cb);
                }
                propose_time = Some(read.renew_lease_time);
            }
        } else {
            for state in &ready.read_states {
                let read = &self.pending_reads.reads[self.pending_reads.ready_cnt];
                assert_eq!(state.request_ctx.as_slice(), read.binary_id());
                self.pending_reads.ready_cnt += 1;
                propose_time = Some(read.renew_lease_time);
            }
        }

        // Note that only after handle read_states can we identify what requests are
        // actually stale.
        if ready.ss.is_some() {
            let term = self.term();
            // all uncommitted reads will be dropped silently in raft.
            self.pending_reads.clear_uncommitted(term);
        }

        if let Some(Either::Right(_)) = self.leader_lease_expired_time {
            return;
        }

        if let Some(propose_time) = propose_time {
            self.update_lease_with(propose_time);
        }
    }

    pub fn post_apply(&mut self, res: &ApplyRes, groups: &mut HashSet<u64>) {
        if self.is_applying_snapshot() {
            panic!("{} should not applying snapshot.", self.tag);
        }

        let has_split = res.exec_res.iter().any(|e| {
            match *e {
                ExecResult::SplitRegion { .. } => true,
                _ => false,
            }
        });

        self.raft_group.advance_apply(res.apply_state.get_applied_index());
        self.mut_store().apply_state = res.apply_state.clone();
        self.mut_store().applied_index_term = res.applied_index_term;
        self.peer_stat.written_keys += res.metrics.written_keys;
        self.peer_stat.written_bytes += res.metrics.written_bytes;

        if has_split {
            self.delete_keys_hint = res.metrics.delete_keys_hint;
            self.size_diff_hint = res.metrics.size_diff_hint as u64;
        } else {
            self.delete_keys_hint += res.metrics.delete_keys_hint;
            self.size_diff_hint = (self.size_diff_hint as i64 + res.metrics.size_diff_hint) as u64;
        }

        if self.has_pending_snapshot() && self.ready_to_handle_pending_snap() {
            self.mark_to_be_checked(groups);
        }

        if self.pending_reads.ready_cnt > 0 && self.ready_to_handle_read() {
            for _ in 0..self.pending_reads.ready_cnt {
                let mut read = self.pending_reads.reads.pop_front().unwrap();
                for (req, cb) in read.cmds.drain(..) {
                    self.handle_read(req, cb);
                }
            }
            self.pending_reads.ready_cnt = 0;
        }
    }

    fn update_lease_with(&mut self, propose_time: Timespec) {
        // Try to renew the leader lease as this command asks to.
        if self.leader_lease_expired_time.is_some() {
            let current_expired_time =
                match self.leader_lease_expired_time.as_ref().unwrap().as_ref() {
                    Either::Left(safe_expired_time) => *safe_expired_time,
                    Either::Right(unsafe_expired_time) => *unsafe_expired_time,
                };
            // This peer is leader and has recorded leader lease.
            // Calculate the renewed lease for this command. If the renewed lease lives longer
            // than the current leader lease, update the current leader lease to the renewed lease.
            let next_expired_time = self.next_lease_expired_time(propose_time);
            // Use the lease expired timestamp comparison here, so that these codes still
            // work no matter how the leader changes before applying this command.
            if current_expired_time < next_expired_time {
                debug!("{} update leader lease expired time from {:?} to {:?}",
                       self.tag,
                       current_expired_time,
                       next_expired_time);
                self.leader_lease_expired_time = Some(Either::Left(next_expired_time));
            }
        } else if self.is_leader() {
            // This peer is leader but its leader lease has expired.
            // Calculate the renewed lease for this command, and update the leader lease
            // for this peer.
            let next_expired_time = self.next_lease_expired_time(propose_time);
            debug!("{} update leader lease expired time from None to {:?}",
                   self.tag,
                   next_expired_time);
            self.leader_lease_expired_time = Some(Either::Left(next_expired_time));
        }
    }

    /// Try to update lease.
    ///
    /// If the it can make sure that its lease is the latest lease, returns true.
    fn maybe_update_lease(&mut self, entry: &eraftpb::Entry) -> bool {
        let propose_time = match self.find_propose_time(entry.get_index(), entry.get_term()) {
            Some(t) => t,
            _ => return false,
        };

        self.update_lease_with(propose_time);

        true
    }

    pub fn maybe_campaign(&mut self,
                          last_peer: &Peer,
                          pending_raft_groups: &mut HashSet<u64>)
                          -> bool {
        if self.region().get_peers().len() <= 1 {
            // The peer campaigned when it was created, no need to do it again.
            return false;
        }

        if !last_peer.is_leader() {
            return false;
        }

        // If last peer is the leader of the region before split, it's intuitional for
        // it to become the leader of new split region.
        let _ = self.raft_group.campaign();
        self.mark_to_be_checked(pending_raft_groups);

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
    pub fn propose(&mut self,
                   cb: Callback,
                   req: RaftCmdRequest,
                   mut err_resp: RaftCmdResponse,
                   metrics: &mut RaftProposeMetrics)
                   -> bool {
        if self.pending_remove {
            return false;
        }

        metrics.all += 1;

        let mut is_conf_change = false;

        let res = match self.get_handle_policy(&req) {
            Ok(RequestPolicy::ReadLocal) => {
                self.read_local(req, cb, metrics);
                return false;
            }
            Ok(RequestPolicy::ReadIndex) => return self.read_index(req, cb, metrics),
            Ok(RequestPolicy::ProposeNormal) => self.propose_normal(req, metrics),
            Ok(RequestPolicy::ProposeTransferLeader) => {
                return self.propose_transfer_leader(req, cb, metrics)
            }
            Ok(RequestPolicy::ProposeConfChange) => {
                is_conf_change = true;
                self.propose_conf_change(req, metrics)
            }
            Err(e) => Err(e),
        };

        match res {
            Err(e) => {
                cmd_resp::bind_error(&mut err_resp, e);
                cb(err_resp);
                false
            }
            Ok(idx) => {
                let meta = ProposalMeta {
                    index: idx,
                    term: self.term(),
                    renew_lease_time: None,
                };
                self.post_propose(meta, is_conf_change, cb);
                true
            }
        }
    }

    fn post_propose(&mut self, mut meta: ProposalMeta, is_conf_change: bool, cb: Callback) {
        // Try to renew leader lease on every consistent read/write request.
        meta.renew_lease_time = Some(clocktime::raw_now());

        let p = Proposal::new(is_conf_change, meta.index, meta.term, cb);
        self.apply_proposals.push(p);

        self.proposals.push(meta);
    }

    fn get_handle_policy(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        if req.has_admin_request() {
            if apply::get_change_peer_cmd(req).is_some() {
                return Ok(RequestPolicy::ProposeConfChange);
            }
            if get_transfer_leader_cmd(req).is_some() {
                return Ok(RequestPolicy::ProposeTransferLeader);
            }
            return Ok(RequestPolicy::ProposeNormal);
        }

        let mut is_read = false;
        let mut is_write = false;
        for r in req.get_requests() {
            match r.get_cmd_type() {
                CmdType::Get | CmdType::Snap => is_read = true,
                CmdType::Delete | CmdType::Put => is_write = true,
                CmdType::Prewrite | CmdType::Invalid => {
                    return Err(box_err!("invalid cmd type {:?}, message maybe currupted",
                                        r.get_cmd_type()));
                }
            }

            if is_read && is_write {
                return Err(box_err!("read and write can't be mixed in one batch."));
            }
        }

        if is_write {
            return Ok(RequestPolicy::ProposeNormal);
        }

        if (req.has_header() && req.get_header().get_read_quorum()) ||
           !self.raft_group.raft.in_lease() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if self.get_store().applied_index_term != self.raft_group.raft.term {
            // TODO: add it in queue directly.
            return Ok(RequestPolicy::ReadIndex);
        }

        // If the leader lease has expired, local read should not be performed.
        if self.leader_lease_expired_time.is_none() {
            return Ok(RequestPolicy::ReadIndex);
        }

        if let Some(Either::Left(safe_expired_time)) = self.leader_lease_expired_time {
            if clocktime::raw_now() <= safe_expired_time {
                return Ok(RequestPolicy::ReadLocal);
            }

            debug!("{} leader lease expired time {:?} is outdated",
                   self.tag,
                   safe_expired_time);
            // Reset leader lease expiring time.
            self.leader_lease_expired_time = None;
        }

        // Perform a consistent read to Raft quorum and try to renew the leader lease.
        Ok(RequestPolicy::ReadIndex)
    }

    /// Count the number of the healthy nodes.
    /// A node is healthy when
    /// 1. it's the leader of the Raft group, which has the latest logs
    /// 2. it's a follower, and it does not lag behind the leader a lot.
    ///    If a snapshot is involved between it and the Raft leader, it's not healthy since
    ///    it cannot works as a node in the quorum to receive replicating logs from leader.
    fn count_healthy_node(&self, progress: Values<u64, Progress>) -> usize {
        let mut healthy = 0;
        for pr in progress {
            if pr.matched >= self.get_store().truncated_index() {
                healthy += 1;
            }
        }
        healthy
    }

    /// Check whether it's safe to propose the specified conf change request.
    /// It's safe iff at least the quorum of the Raft group is still healthy
    /// right after that conf change is applied.
    /// Define the total number of nodes in current Raft cluster to be `total`.
    /// To ensure the above safety, if the cmd is
    /// 1. A `AddNode` request
    ///    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
    /// 2. A `RemoveNode` request
    ///    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
    ///    need to be up to date for now.
    fn check_conf_change(&self, cmd: &RaftCmdRequest) -> Result<()> {
        let change_peer = apply::get_change_peer_cmd(cmd).unwrap();

        let change_type = change_peer.get_change_type();
        let peer = change_peer.get_peer();

        let mut status = self.raft_group.status();
        let total = status.progress.len();
        if total == 1 {
            // It's always safe if there is only one node in the cluster.
            return Ok(());
        }

        match change_type {
            ConfChangeType::AddNode => {
                status.progress.insert(peer.get_id(), Progress::default());
            }
            ConfChangeType::RemoveNode => {
                if status.progress.remove(&peer.get_id()).is_none() {
                    // It's always safe to remove a unexisting node.
                    return Ok(());
                }
            }
        }
        let healthy = self.count_healthy_node(status.progress.values());
        let quorum_after_change = raft::quorum(status.progress.len());
        if healthy >= quorum_after_change {
            return Ok(());
        }

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["conf_change", "reject_unsafe"]).inc();

        info!("{} rejects unsafe conf change request {:?}, total {}, healthy {},  \
               quorum after change {}",
              self.tag,
              change_peer,
              total,
              healthy,
              quorum_after_change);
        Err(box_err!("unsafe to perform conf change {:?}, total {}, healthy {}, quorum after \
                      change {}",
                     change_peer,
                     total,
                     healthy,
                     quorum_after_change))
    }

    fn transfer_leader(&mut self, peer: &metapb::Peer) {
        info!("{} transfer leader to {:?}", self.tag, peer);

        self.raft_group.transfer_leader(peer.get_id());
    }

    fn is_tranfer_leader_allowed(&self, peer: &metapb::Peer) -> bool {
        let peer_id = peer.get_id();
        let status = self.raft_group.status();

        if !status.progress.contains_key(&peer_id) {
            return false;
        }

        for progress in status.progress.values() {
            if progress.state == ProgressState::Snapshot {
                return false;
            }
        }

        let last_index = self.get_store().last_index();
        last_index <= status.progress[&peer_id].matched + TRANSFER_LEADER_ALLOW_LOG_LAG
    }

    fn read_local(&mut self, req: RaftCmdRequest, cb: Callback, metrics: &mut RaftProposeMetrics) {
        metrics.local_read += 1;
        self.handle_read(req, cb);
    }

    fn read_index(&mut self,
                  req: RaftCmdRequest,
                  cb: Callback,
                  metrics: &mut RaftProposeMetrics)
                  -> bool {
        metrics.read_index += 1;

        let renew_lease_time = clocktime::raw_now();
        if let Some(read) = self.pending_reads.reads.back_mut() {
            if read.renew_lease_time + self.cfg.raft_store_max_leader_lease > renew_lease_time {
                read.cmds.push((req, cb));
                return false;
            }
        }

        // Should we call pre_propose here?
        let last_pending_read_count = self.raft_group.raft.pending_read_count();
        let last_ready_read_count = self.raft_group.raft.ready_read_count();

        let id = self.pending_reads.next_id();
        let ctx: [u8; 8] = unsafe { mem::transmute(id) };
        self.raft_group.read_index(ctx.to_vec());

        let pending_read_count = self.raft_group.raft.pending_read_count();
        let ready_read_count = self.raft_group.raft.ready_read_count();

        if pending_read_count == last_pending_read_count &&
           ready_read_count == last_ready_read_count {
            // The message gets dropped silently, can't be handled anymore.
            apply::notify_stale_req(self.term(), cb);
            return false;
        }

        self.pending_reads.reads.push_back(ReadIndexRequest {
            id: id,
            cmds: vec![(req, cb)],
            renew_lease_time: renew_lease_time,
        });

        match self.leader_lease_expired_time {
            Some(Either::Right(_)) => {}
            _ => return true,
        };

        // TimeoutNow has been sent out, so we need to propose explicitly to
        // update leader lease.

        let req = RaftCmdRequest::new();
        if let Ok(index) = self.propose_normal(req, metrics) {
            let meta = ProposalMeta {
                index: index,
                term: self.term(),
                renew_lease_time: Some(renew_lease_time),
            };
            self.post_propose(meta, false, box |_| {});
        }

        true
    }

    fn propose_normal(&mut self,
                      mut req: RaftCmdRequest,
                      metrics: &mut RaftProposeMetrics)
                      -> Result<u64> {
        metrics.normal += 1;

        // TODO: validate request for unexpected changes.
        try!(self.coprocessor_host.pre_propose(self.region(), &mut req));
        let data = try!(req.write_to_bytes());

        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        if data.len() as u64 > self.raft_entry_max_size {
            error!("entry is too large, entry size {}", data.len());
            return Err(Error::RaftEntryTooLarge(self.region_id, data.len() as u64));
        }

        let propose_index = self.next_proposal_index();
        try!(self.raft_group.propose(data));
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }

        Ok(propose_index)
    }

    // Return true to if the transfer leader request is accepted.
    fn propose_transfer_leader(&mut self,
                               req: RaftCmdRequest,
                               cb: Callback,
                               metrics: &mut RaftProposeMetrics)
                               -> bool {
        metrics.transfer_leader += 1;

        let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
        let peer = transfer_leader.get_peer();

        let transfered = if self.is_tranfer_leader_allowed(peer) {
            self.transfer_leader(peer);
            true
        } else {
            info!("{} transfer leader message {:?} ignored directly",
                  self.tag,
                  req);
            false
        };

        // transfer leader command doesn't need to replicate log and apply, so we
        // return immediately. Note that this command may fail, we can view it just as an advice
        cb(make_transfer_leader_response());

        transfered
    }

    fn propose_conf_change(&mut self,
                           req: RaftCmdRequest,
                           metrics: &mut RaftProposeMetrics)
                           -> Result<u64> {
        if self.raft_group.raft.pending_conf {
            info!("{} there is a pending conf change, try later", self.tag);
            return Err(box_err!("{} there is a pending conf change, try later", self.tag));
        }

        try!(self.check_conf_change(&req));

        metrics.conf_change += 1;

        let data = try!(req.write_to_bytes());

        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        let change_peer = apply::get_change_peer_cmd(&req).unwrap();

        let mut cc = eraftpb::ConfChange::new();
        cc.set_change_type(change_peer.get_change_type());
        cc.set_node_id(change_peer.get_peer().get_id());
        cc.set_context(data);

        info!("{} propose conf change {:?} peer {:?}",
              self.tag,
              cc.get_change_type(),
              cc.get_node_id());

        let propose_index = self.next_proposal_index();
        try!(self.raft_group.propose_conf_change(cc));
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }

        Ok(propose_index)
    }

    fn handle_read(&mut self, req: RaftCmdRequest, cb: Callback) {
        let mut resp = self.exec_read(&req).unwrap_or_else(|e| {
            match e {
                Error::StaleEpoch(..) => info!("{} stale epoch err: {:?}", self.tag, e),
                _ => error!("{} execute raft command err: {:?}", self.tag, e),
            }
            cmd_resp::new_error(e)
        });

        cmd_resp::bind_term(&mut resp, self.term());
        cb(resp);
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub fn stop(&mut self) {
        self.mut_store().cancel_applying_snap();
        for mut read in self.pending_reads.reads.drain(..) {
            read.cmds.clear();
        }
    }
}

pub fn check_epoch(region: &metapb::Region, req: &RaftCmdRequest) -> Result<()> {
    let (mut check_ver, mut check_conf_ver) = (false, false);
    if req.has_admin_request() {
        match req.get_admin_request().get_cmd_type() {
            AdminCmdType::CompactLog |
            AdminCmdType::InvalidAdmin |
            AdminCmdType::ComputeHash |
            AdminCmdType::VerifyHash => {}
            AdminCmdType::Split => check_ver = true,
            AdminCmdType::ChangePeer => check_conf_ver = true,
            AdminCmdType::TransferLeader => {
                check_ver = true;
                check_conf_ver = true;
            }
        };
    } else {
        // for get/set/delete, we don't care conf_version.
        check_ver = true;
    }

    if !check_ver && !check_conf_ver {
        return Ok(());
    }

    if !req.get_header().has_region_epoch() {
        return Err(box_err!("missing epoch!"));
    }

    let from_epoch = req.get_header().get_region_epoch();
    let latest_epoch = region.get_region_epoch();

    // should we use not equal here?
    if (check_conf_ver && from_epoch.get_conf_ver() < latest_epoch.get_conf_ver()) ||
       (check_ver && from_epoch.get_version() < latest_epoch.get_version()) {
        debug!("[region {}] received stale epoch {:?}, mime: {:?}",
               region.get_id(),
               from_epoch,
               latest_epoch);
        return Err(Error::StaleEpoch(format!("latest_epoch of region {} is {:?}, but you \
                                                sent {:?}",
                                             region.get_id(),
                                             latest_epoch,
                                             from_epoch),
                                     vec![region.to_owned()]));
    }

    Ok(())
}

impl Peer {
    pub fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        self.peer_cache.borrow_mut().insert(peer.get_id(), peer);
    }

    pub fn remove_peer_from_cache(&mut self, peer_id: u64) {
        self.peer_cache.borrow_mut().remove(&peer_id);
    }

    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if let Some(peer) = self.peer_cache.borrow().get(&peer_id) {
            return Some(peer.clone());
        }

        // Try to find in region, if found, set in cache.
        for peer in self.get_store().get_region().get_peers() {
            if peer.get_id() == peer_id {
                self.peer_cache.borrow_mut().insert(peer_id, peer.clone());
                return Some(peer.clone());
            }
        }

        None
    }

    pub fn heartbeat_pd(&self, worker: &FutureWorker<PdTask>) {
        let task = PdTask::Heartbeat {
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(self.cfg.max_peer_down_duration),
            pending_peers: self.collect_pending_peers(),
            written_bytes: self.peer_stat.last_written_bytes,
            written_keys: self.peer_stat.last_written_keys,
        };
        if let Err(e) = worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn send_raft_message<T: Transport>(&mut self, msg: eraftpb::Message, trans: &T) -> Result<()> {
        let mut send_msg = RaftMessage::new();
        send_msg.set_region_id(self.region_id);
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());

        let from_peer = self.peer.clone();

        let to_peer = match self.get_peer_from_cache(msg.get_to()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to look up recipient peer {} in region {}",
                                    msg.get_to(),
                                    self.region_id))
            }
        };

        let to_peer_id = to_peer.get_id();
        let to_store_id = to_peer.get_store_id();
        let msg_type = msg.get_msg_type();
        debug!("{} send raft msg {:?}[size: {}] from {} to {}",
               self.tag,
               msg_type,
               msg.compute_size(),
               from_peer.get_id(),
               to_peer_id);

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
        if self.get_store().is_initialized() &&
           (msg_type == MessageType::MsgRequestVote ||
            // the peer has not been known to this leader, it may exist or not.
            (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == INVALID_INDEX)) {
            let region = self.region();
            send_msg.set_start_key(region.get_start_key().to_vec());
            send_msg.set_end_key(region.get_end_key().to_vec());
        }

        send_msg.set_message(msg);

        if let Err(e) = trans.send(send_msg) {
            warn!("{} failed to send msg to {} in store {}, err: {:?}",
                  self.tag,
                  to_peer_id,
                  to_store_id,
                  e);

            // unreachable store
            self.raft_group.report_unreachable(to_peer_id);
            if msg_type == eraftpb::MessageType::MsgSnapshot {
                self.raft_group.report_snapshot(to_peer_id, SnapshotStatus::Failure);
            }
        }

        Ok(())
    }

    fn exec_read(&mut self, req: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        try!(check_epoch(self.region(), req));
        let snap = Snapshot::new(self.engine.clone());
        let requests = req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => try!(apply::do_get(&self.tag, self.region(), &snap, req)),
                CmdType::Snap => try!(apply::do_snap(self.region().to_owned())),
                CmdType::Prewrite => unreachable!(),
                CmdType::Put | CmdType::Delete | CmdType::Invalid => unreachable!(),
            };

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
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

fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::new();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::new());
    let mut resp = RaftCmdResponse::new();
    resp.set_admin_response(response);
    resp
}
