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
use std::collections::hash_map::Values;
use std::vec::Vec;
use std::default::Default;
use std::time::{Instant, Duration};

use time::Timespec;
use rocksdb::{DB, WriteBatch};
use protobuf::{self, Message, MessageStatic};
use uuid::Uuid;
use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, CmdType, AdminCmdType, AdminResponse,
                          TransferLeaderRequest, TransferLeaderResponse};
use kvproto::raft_serverpb::{RaftMessage, PeerState};
use kvproto::pdpb::PeerStats;

use raft::{self, RawNode, StateRole, SnapshotStatus, Ready, ProgressState, Progress, INVALID_INDEX};
use raftstore::{Result, Error};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::store::Config;
use raftstore::store::worker::{apply, PdTask};

use util::worker::{Worker, Scheduler};
use raftstore::store::worker::ApplyTask;
use util::{clocktime, Either, HashMap, HashSet};

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

/// The returned states of the peer after checking whether it is stale
#[derive(Debug)]
pub enum StaleState {
    Valid,
    ToValidate,
}

pub struct ProposalMeta {
    pub uuid: Uuid,
    pub term: u64,
    /// `renew_lease_time` contains the last time when a peer starts to renew lease.
    pub renew_lease_time: Option<Timespec>,
}

#[derive(Default)]
struct ProposalQueue {
    queue: VecDeque<ProposalMeta>,
    uuids: HashSet<Uuid>,
}

impl ProposalQueue {
    pub fn contains(&self, uuid: &Uuid) -> bool {
        self.uuids.contains(uuid)
    }

    fn pop(&mut self, term: u64) -> Option<ProposalMeta> {
        self.queue.pop_front().and_then(|meta| {
            if meta.term > term {
                self.queue.push_front(meta);
                return None;
            }
            self.uuids.remove(&meta.uuid);
            Some(meta)
        })
    }

    fn push(&mut self, meta: ProposalMeta) {
        self.uuids.insert(meta.uuid);
        self.queue.push_back(meta);
    }

    fn clear(&mut self) {
        if !self.uuids.is_empty() {
            self.uuids.clear();
            self.queue.clear();
        }
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
            wb: WriteBatch::new(),
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

pub struct Peer {
    engine: Arc<DB>,
    cfg: Rc<Config>,
    peer_cache: Rc<RefCell<HashMap<u64, metapb::Peer>>>,
    pub peer: metapb::Peer,
    region_id: u64,
    pub raft_group: RawNode<PeerStorage>,
    proposals: ProposalQueue,
    // Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: HashMap<u64, Instant>,
    coprocessor_host: CoprocessorHost,
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: u64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub consistency_state: ConsistencyState,

    pub tag: String,

    pub last_ready_idx: u64,
    pub last_compacted_idx: u64,
    // Approximate size of logs that is applied but not compacted yet.
    pub raft_log_size_hint: u64,
    // When entry exceed max size, reject to propose the entry.
    pub raft_entry_max_size: u64,

    apply_scheduler: Scheduler<ApplyTask>,

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

    pub written_bytes: u64,
    pub written_keys: u64,
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
        let tag = format!("[region {}] {}", region.get_id(), peer_id);

        let ps = try!(PeerStorage::new(store.engine(), &region, sched, tag.clone()));

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
            ..Default::default()
        };

        let raft_group = try!(RawNode::new(&raft_cfg, ps, &[]));

        let mut peer = Peer {
            engine: store.engine(),
            peer: util::new_peer(store_id, peer_id),
            region_id: region.get_id(),
            raft_group: raft_group,
            proposals: Default::default(),
            peer_cache: store.peer_cache(),
            peer_heartbeats: HashMap::default(),
            coprocessor_host: CoprocessorHost::new(),
            size_diff_hint: 0,
            delete_keys_hint: 0,
            apply_scheduler: store.apply_scheduler(),
            marked_to_be_checked: false,
            leader_missing_time: Some(Instant::now()),
            tag: tag,
            last_ready_idx: applied_index,
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
            written_bytes: 0,
            written_keys: 0,
        };

        peer.load_all_coprocessors();

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
        self.coprocessor_host.shutdown();
        info!("{} destroy itself, takes {:?}", self.tag, t.elapsed());

        Ok(())
    }

    pub fn is_initialized(&self) -> bool {
        self.get_store().is_initialized()
    }

    pub fn load_all_coprocessors(&mut self) {
        // TODO load coprocessors from configuration
        self.coprocessor_host.registry.register_observer(100, box SplitObserver);
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

    // When this peer is the leader of the region before split,
    // `accelerate_campaign_ticks` specifies the tick number to be accelerated
    // after the region split. So this peer of new split region may campaign and become leader
    // earlier than other follower peers.
    pub fn accelerate_campaign_ticks(&self) -> usize {
        // The best case for the fast campaign after a split is:
        //   1. The leader of old region replicates the log entry (it's called S later)
        //      which contains split request to quorum by sending out AppendEntries requests.
        //   2. The leader of old region commits the log entry S.
        //   3. The leader of old region send AppendEntries (it's call C later) requests to
        //      tell followers to commit the log entry S.
        //   4. The leader of old region applies this log entry S, and then raftstore
        //      accelerates ticks for the peer of the new region. This peer shares the same store
        //      with old leader. It will start to campaign soon and send out RequestVote requests
        //      (they are called V later).
        //   5. The followers receive AppendEntries requests from leader for replication in 1,
        //      and then make it persistent and applie the split log entry so that
        //      create a new region.
        //   6. The followers receive RequestVote requests from leader in 4, and then vote
        //      the campaign.
        // However it's possible that 4 happens before 3, if RequestVote requests V in 4 is sent
        // to the followers earlier than AppendEntries requests C in 3, the followers would not
        // be ready to vote the campaign as 6 describes. Then the tick acceleration in 4 would be
        // useless, so the campaign still happens after the election timeout.
        // To make the best case happen at high degree of possibility, a time gap of
        // `accelerate_campaign_reserved_ticks * raft_base_tick_interval` is reserved for
        // the followers to receive AppendEntries request and to apply them.
        let ticks = self.raft_group.raft.get_randomized_election_timeout() -
                    self.cfg.accelerate_campaign_reserved_ticks;
        debug!("{} about to accelerate {} ticks for fast campaign after split",
               self.tag,
               ticks);
        ticks
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

    fn on_role_changed(&mut self, ready: &Ready, worker: &Worker<PdTask>) {
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
                    self.leader_lease_expired_time =
                        Some(Either::Left(self.next_lease_expired_time(clocktime::raw_now())));
                    debug!("{} becomes leader and lease expired time is {:?}",
                           self.tag,
                           self.leader_lease_expired_time);
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
        // If committed_index doesn't equal to applied_index, written apply state may be overwritten
        // by apply worker. So we have to wait here.
        self.get_store().committed_index() == self.get_store().applied_index()
    }

    pub fn handle_raft_ready_append<T: Transport>(&mut self,
                                                  ctx: &mut ReadyContext<T>,
                                                  worker: &Worker<PdTask>) {
        self.marked_to_be_checked = false;
        if self.mut_store().check_applying_snap() {
            // If we continue to handle all the messages, it may cause too many messages because
            // leader will send all the remaining messages to this follower, which can lead
            // to full message queue under high load.
            debug!("{} still applying snapshot, skip further handling.",
                   self.tag);
            return;
        }

        if self.has_pending_snapshot() && !self.ready_to_handle_pending_snap() {
            debug!("{} apply index {} != committed index {}, skip applying snapshot.",
                   self.tag,
                   self.get_store().applied_index(),
                   self.get_store().committed_index());
            return;
        }

        if !self.raft_group.has_ready_since(Some(self.last_ready_idx)) {
            return;
        }

        debug!("{} handle raft ready", self.tag);

        let mut ready = self.raft_group.ready_since(self.last_ready_idx);

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

    pub fn handle_raft_ready_apply(&mut self, mut ready: Ready) {
        // Call `handle_raft_committed_entries` directly here may lead to inconsistency.
        // In some cases, there will be some pending committed entries when applying a
        // snapshot. If we call `handle_raft_committed_entries` directly, these updates
        // will be written to disk. Because we apply snapshot asynchronously, so these
        // updates will soon be removed. But the soft state of raft is still be updated
        // in memory. Hence when handle ready next time, these updates won't be included
        // in `ready.committed_entries` again, which will lead to inconsistency.
        if self.is_applying_snapshot() {
            // Snapshot's metadata has been applied.
            self.last_ready_idx = self.get_store().truncated_index();
        } else {
            let committed_entries = ready.committed_entries.take().unwrap();
            // leader needs to update lease.
            let mut to_be_updated = self.is_leader();
            if !to_be_updated {
                // It's not leader anymore, we are safe to clear proposals. If it becomes leader
                // again, the lease should be updated when election is finished, old proposals
                // have no effect. Although this may allow old uuids to be reused before old
                // callbacks are called.
                self.proposals.clear();
            }
            for entry in committed_entries.iter().rev() {
                // raft meta is very small, can be ignored.
                self.raft_log_size_hint += entry.get_data().len() as u64;
                if to_be_updated {
                    to_be_updated = !self.maybe_update_lease(entry.get_term(), entry.get_data());
                }
            }
            if !committed_entries.is_empty() {
                self.last_ready_idx = committed_entries.last().unwrap().get_index();
                let apply_task = ApplyTask::apply(self.region_id, self.term(), committed_entries);
                self.apply_scheduler.schedule(apply_task).unwrap();
            }
        }

        self.raft_group.advance_append(ready);
        if self.is_applying_snapshot() {
            // Because we only handle raft ready when not applying snapshot, so following
            // line won't be called twice for the same snapshot.
            self.raft_group.advance_apply(self.last_ready_idx);
        }
    }

    /// Try to update lease.
    ///
    /// If the it can make sure that its lease is the latest lease, returns true.
    fn maybe_update_lease(&mut self, term: u64, data: &[u8]) -> bool {
        let mut req = RaftCmdRequest::new();
        let propose_time = match req.merge_from_bytes(data)
            .ok()
            .and_then(|_| util::get_uuid_from_req(&req))
            .and_then(|uuid| self.find_propose_time(uuid, term)) {
            Some(t) => t,
            _ => return false,
        };

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
                   self.leader_lease_expired_time);
            self.leader_lease_expired_time = Some(Either::Left(next_expired_time));
        }

        true
    }

    fn find_propose_time(&mut self, uuid: Uuid, term: u64) -> Option<Timespec> {
        while let Some(meta) = self.proposals.pop(term) {
            if meta.uuid == uuid {
                return Some(meta.renew_lease_time.unwrap());
            }
        }
        None
    }

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
    pub fn propose(&mut self,
                   mut meta: ProposalMeta,
                   cb: Callback,
                   req: RaftCmdRequest,
                   mut err_resp: RaftCmdResponse,
                   metrics: &mut RaftProposeMetrics)
                   -> bool {
        if self.proposals.contains(&meta.uuid) {
            cmd_resp::bind_error(&mut err_resp, box_err!("duplicated uuid {:?}", meta.uuid));
            cb(err_resp);
            return false;
        }

        debug!("{} propose command with uuid {:?}", self.tag, meta.uuid);
        metrics.all += 1;

        let mut is_conf_change = false;

        if self.should_read_local(&req) {
            metrics.local_read += 1;

            let mut resp = self.exec_read(&req).unwrap_or_else(|e| {
                error!("{} execute raft command err: {:?}", self.tag, e);
                cmd_resp::new_error(e)
            });

            cmd_resp::bind_uuid(&mut resp, meta.uuid);
            cmd_resp::bind_term(&mut resp, self.term());
            cb(resp);
            return false;
        } else if get_transfer_leader_cmd(&req).is_some() {
            metrics.transfer_leader += 1;

            let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
            let peer = transfer_leader.get_peer();

            if self.is_tranfer_leader_allowed(peer) {
                self.transfer_leader(peer);
            } else {
                info!("{} transfer leader message {:?} ignored directly",
                      self.tag,
                      req);
            }

            // transfer leader command doesn't need to replicate log and apply, so we
            // return immediately. Note that this command may fail, we can view it just as an advice
            cb(make_transfer_leader_response());
            return false;
        } else if apply::get_change_peer_cmd(&req).is_some() {
            if self.raft_group.raft.pending_conf {
                info!("{} there is a pending conf change, try later", self.tag);
                cmd_resp::bind_error(&mut err_resp,
                                     box_err!("{} there is a pending conf change, try later",
                                              self.tag));
                cb(err_resp);
                return false;
            }

            if let Err(e) = self.propose_conf_change(req, metrics) {
                cmd_resp::bind_error(&mut err_resp, e);
                cb(err_resp);
                return false;
            }

            is_conf_change = true;
        } else if let Err(e) = self.propose_normal(req, metrics) {
            cmd_resp::bind_error(&mut err_resp, e);
            cb(err_resp);
            return false;
        }

        // Try to renew leader lease on every consistent read/write request.
        meta.renew_lease_time = Some(clocktime::raw_now());


        let t = ApplyTask::propose(self.peer_id(),
                                   self.region_id,
                                   meta.uuid,
                                   is_conf_change,
                                   meta.term,
                                   cb);
        self.apply_scheduler.schedule(t).unwrap();

        self.proposals.push(meta);

        true
    }

    fn should_read_local(&mut self, req: &RaftCmdRequest) -> bool {
        if (req.has_header() && req.get_header().get_read_quorum()) ||
           !self.raft_group.raft.in_lease() || req.get_requests().len() == 0 {
            return false;
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if self.get_store().applied_index_term != self.raft_group.raft.term {
            return false;
        }

        for cmd_req in req.get_requests() {
            if cmd_req.get_cmd_type() != CmdType::Snap && cmd_req.get_cmd_type() != CmdType::Get {
                return false;
            }
        }

        // If the leader lease has expired, local read should not be performed.
        if self.leader_lease_expired_time.is_none() {
            return false;
        }

        let mut reset_lease_expired_time = false;
        let mut lost_lease = false;
        let now = clocktime::raw_now();
        match self.leader_lease_expired_time.as_ref().unwrap().as_ref() {
            Either::Left(safe_expired_time) => {
                if now > *safe_expired_time {
                    reset_lease_expired_time = true;
                    lost_lease = true;
                }
            }
            _ => lost_lease = true,
        }
        if reset_lease_expired_time {
            debug!("{} leader lease expired time {:?} is outdated",
                   self.tag,
                   self.leader_lease_expired_time);
            // Reset leader lease expiring time.
            self.leader_lease_expired_time = None;
        }
        if lost_lease {
            // Perform a consistent read to Raft quorum and try to renew the leader lease.
            return false;
        }

        true
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
                let progress = Progress { ..Default::default() };
                status.progress.insert(peer.get_id(), progress);
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

    fn propose_normal(&mut self,
                      mut cmd: RaftCmdRequest,
                      metrics: &mut RaftProposeMetrics)
                      -> Result<()> {
        metrics.normal += 1;

        // TODO: validate request for unexpected changes.
        try!(self.coprocessor_host.pre_propose(&self.raft_group.get_store(), &mut cmd));
        let data = try!(cmd.write_to_bytes());

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

        Ok(())
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

    fn propose_conf_change(&mut self,
                           cmd: RaftCmdRequest,
                           metrics: &mut RaftProposeMetrics)
                           -> Result<()> {
        try!(self.check_conf_change(&cmd));

        metrics.conf_change += 1;

        let data = try!(cmd.write_to_bytes());

        // TODO: use local histogram metrics
        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        let change_peer = apply::get_change_peer_cmd(&cmd).unwrap();

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

        Ok(())
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
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
    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if let Some(peer) = self.peer_cache.borrow().get(&peer_id).cloned() {
            return Some(peer);
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

    pub fn heartbeat_pd(&self, worker: &Worker<PdTask>) {
        let task = PdTask::Heartbeat {
            region: self.region().clone(),
            peer: self.peer.clone(),
            down_peers: self.collect_down_peers(self.cfg.max_peer_down_duration),
            pending_peers: self.collect_pending_peers(),
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
        let snap = Snapshot::new(self.engine.clone());
        let requests = req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => try!(apply::do_get(&self.tag, self.region(), &snap, req)),
                CmdType::Snap => try!(apply::do_snap(self.region().to_owned())),
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
