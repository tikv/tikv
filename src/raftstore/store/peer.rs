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
use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;
use std::default::Default;
use std::time::{Instant, Duration};

use rocksdb::{DB, WriteBatch, Writable};
use protobuf::{self, Message};
use uuid::Uuid;

use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, ChangePeerRequest, SplitRequest,
                          MergeRequest, CmdType, AdminCmdType, Request, Response, AdminRequest,
                          AdminResponse, TransferLeaderRequest, TransferLeaderResponse,
                          MergeResponse};
use kvproto::raft_serverpb::{RaftMessage, RaftApplyState, RaftTruncatedState, PeerState,
                             MergeState, RegionMergeState, RegionLocalState};
use kvproto::pdpb::PeerStats;
use raft::{self, RawNode, StateRole, SnapshotStatus, Ready, ProgressState, INVALID_INDEX};
use raftstore::{Result, Error};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::coprocessor::split_observer::SplitObserver;
use util::{escape, SlowTimer, rocksdb};
use pd::{PdClient, INVALID_ID};
use storage::{CF_LOCK, CF_RAFT};
use super::store::{Store, RaftReadyMetrics, RaftMessageMetrics, RaftMetrics};
use super::peer_storage::{PeerStorage, ApplySnapResult, write_initial_state, write_peer_state};
use super::util;
use super::msg::Callback;
use super::cmd_resp;
use super::transport::Transport;
use super::keys;
use super::engine::{Snapshot, Peekable, Mutable};
use super::metrics::*;

const TRANSFER_LEADER_ALLOW_LOG_LAG: u64 = 10;

/// The returned states of the peer after checking whether it is stale
#[derive(Debug)]
pub enum StaleState {
    Valid,
    ToValidate,
}

pub struct PendingCmd {
    pub uuid: Uuid,
    pub term: u64,
    pub cb: Option<Callback>,
}

impl PendingCmd {
    #[inline]
    fn call(&mut self, resp: RaftCmdResponse) {
        self.cb.take().unwrap().call_box((resp,));
    }
}

impl Drop for PendingCmd {
    fn drop(&mut self) {
        if self.cb.is_some() {
            panic!("callback of {} is leak.", self.uuid);
        }
    }
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer {
        change_type: ConfChangeType,
        peer: metapb::Peer,
        region: metapb::Region,
    },
    CompactLog { state: RaftTruncatedState },
    SplitRegion {
        left: metapb::Region,
        right: metapb::Region,
    },
    StartMerge {
        from_region: metapb::Region,
        into_region: metapb::Region,
    },
    SuspendRegion {
        from_region: metapb::Region,
        into_region: metapb::Region,
    },
    CommitMerge {
        new: metapb::Region,
        old: metapb::Region,
        to_shutdown: metapb::Region,
    },
    ShutdownRegion {
        region: metapb::Region,
        peer: metapb::Peer,
    },
}

// When we apply commands in handing ready, we should also need a way to
// let outer store do something after handing ready over.
// We can save these intermediate results in ready result.
// We only need to care administration commands now.
pub struct ReadyResult {
    // We can execute multi commands like 1, conf change, 2 split region, ...
    // in one ready, and outer store should handle these results sequentially too.
    pub exec_results: Vec<ExecResult>,
    // apply_snap_result is set after snapshot applied.
    pub apply_snap_result: Option<ApplySnapResult>,
}

#[derive(Default)]
struct PendingCmdQueue {
    normals: VecDeque<PendingCmd>,
    conf_change: Option<PendingCmd>,
    uuids: HashSet<Uuid>,
}

impl PendingCmdQueue {
    pub fn contains(&self, uuid: &Uuid) -> bool {
        self.uuids.contains(uuid)
    }

    fn remove(&mut self, cmd: &Option<PendingCmd>) {
        if let Some(ref cmd) = *cmd {
            self.uuids.remove(&cmd.uuid);
        }
    }

    fn pop_normal(&mut self, term: u64) -> Option<PendingCmd> {
        self.normals.pop_front().and_then(|cmd| {
            if cmd.term > term {
                self.normals.push_front(cmd);
                return None;
            }
            let res = Some(cmd);
            self.remove(&res);
            res
        })
    }

    fn append_normal(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        let cmd = self.conf_change.take();
        self.remove(&cmd);
        cmd
    }

    fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.conf_change = Some(cmd);
    }
}

/// Call the callback of `cmd` that the region is removed.
fn notify_region_removed(region_id: u64, peer_id: u64, mut cmd: PendingCmd) {
    let region_not_found = Error::RegionNotFound(region_id);
    let mut resp = cmd_resp::new_error(region_not_found);
    cmd_resp::bind_uuid(&mut resp, cmd.uuid);
    debug!("[region {}] {} is removed, notify {}.",
           region_id,
           peer_id,
           cmd.uuid);
    cmd.call(resp);
}

pub struct Peer {
    engine: Arc<DB>,
    peer_cache: Rc<RefCell<HashMap<u64, metapb::Peer>>>,
    // if we remove ourself in ChangePeer remove, we should set this flag, then
    // any following committed logs in same Ready should be applied failed.
    pending_remove: bool,
    pub peer: metapb::Peer,
    region_id: u64,
    pub raft_group: RawNode<PeerStorage>,
    pending_cmds: PendingCmdQueue,
    // Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: HashMap<u64, Instant>,
    coprocessor_host: CoprocessorHost,
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: u64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    leader_missing_time: Option<Instant>,

    pub delete_count: u64,
    merge_state: MergeState,
    start_merge_time: Option<Instant>,
    start_merge_index: u64,

    pub tag: String,

    pub last_compacted_idx: u64,
}

impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                             region: &metapb::Region)
                                             -> Result<Peer> {
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
    pub fn replicate<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                                region_id: u64,
                                                peer_id: u64)
                                                -> Result<Peer> {
        // We will remove tombstone key when apply snapshot
        info!("[region {}] replicate peer with id {}", region_id, peer_id);

        let mut region = metapb::Region::new();
        region.set_id(region_id);
        Peer::new(store, &region, peer_id)
    }

    fn new<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                      region: &metapb::Region,
                                      peer_id: u64)
                                      -> Result<Peer> {
        if peer_id == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }

        let cfg = store.config();

        let store_id = store.store_id();
        let sched = store.snap_scheduler();
        let tag = format!("[region {}] {}", region.get_id(), peer_id);

        let ps = try!(PeerStorage::new(store.engine(), &region, sched, tag.clone()));

        let region_local_state = try!(store.engine()
            .get_msg::<RegionLocalState>(&keys::region_state_key(region.get_id())));
        let merge_state = match region_local_state {
            Some(s) => s.get_merge_state().get_state(),
            None => MergeState::NoMerge,
        };

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
            pending_cmds: Default::default(),
            peer_cache: store.peer_cache(),
            peer_heartbeats: HashMap::new(),
            coprocessor_host: CoprocessorHost::new(),
            size_diff_hint: 0,
            delete_keys_hint: 0,
            pending_remove: false,
            leader_missing_time: Some(Instant::now()),
            delete_count: 0,
            merge_state: merge_state,
            start_merge_time: Some(Instant::now()),
            start_merge_index: 0,
            tag: tag,
            last_compacted_idx: 0,
        };

        peer.load_all_coprocessors();

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    pub fn destroy(&mut self) -> Result<()> {
        let t = Instant::now();

        let region = self.get_store().get_region().clone();
        info!("{} begin to destroy", self.tag);

        // If pending_remove, meta was destroyed when applying removal.
        if !self.pending_remove {
            // First set Tombstone state explicitly, and clear raft meta.
            let wb = WriteBatch::new();
            try!(self.get_store().clear_meta(&wb));
            try!(write_peer_state(&wb, &region, PeerState::Tombstone));
            try!(self.engine.write(wb));
        }

        // TODO: figure out a way to unit test this.
        let peer_id = self.peer_id();
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region_id, peer_id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region_id, peer_id, cmd);
        }

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

    pub fn is_applying(&self) -> bool {
        self.get_store().is_applying()
    }

    pub fn is_in_region_merge(&self) -> bool {
        self.merge_state != MergeState::NoMerge
    }

    fn add_ready_metric(&self, ready: &Ready, metrics: &mut RaftReadyMetrics) {
        if !ready.messages.is_empty() {
            metrics.message += ready.messages.len() as u64;
        }

        if !ready.committed_entries.is_empty() {
            metrics.commit += ready.committed_entries.len() as u64;
        }

        if !ready.entries.is_empty() {
            metrics.append += ready.entries.len() as u64;
        }

        if !raft::is_empty_snap(&ready.snapshot) {
            metrics.snapshot += 1;
        }
    }

    #[inline]
    fn send<T, I>(&mut self, trans: &T, msgs: I, metrics: &mut RaftMessageMetrics) -> Result<()>
        where T: Transport,
              I: Iterator<Item = eraftpb::Message>
    {
        for msg in msgs {
            match msg.get_msg_type() {
                MessageType::MsgAppend => metrics.append += 1,
                MessageType::MsgAppendResponse => metrics.append_resp += 1,
                MessageType::MsgRequestVote => metrics.vote += 1,
                MessageType::MsgRequestVoteResponse => metrics.vote_resp += 1,
                MessageType::MsgSnapshot => metrics.snapshot += 1,
                MessageType::MsgHeartbeat => metrics.heartbeat += 1,
                MessageType::MsgHeartbeatResponse => metrics.heartbeat_resp += 1,
                MessageType::MsgTransferLeader => metrics.transfer_leader += 1,
                MessageType::MsgTimeoutNow => metrics.timeout_now += 1,
                _ => {}
            }

            try!(self.send_raft_message(msg, trans));
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

    pub fn rollback_region_merge(&mut self) -> Result<()> {
        if self.merge_state == MergeState::Merging {
            info!("{} rollback region merge", self.tag);
            let state_key = keys::region_state_key(self.region().get_id());
            let mut region_local_state = try!(self.engine.get_msg::<RegionLocalState>(&state_key))
                .unwrap();
            let mut merge_state = RegionMergeState::new();
            merge_state.set_state(MergeState::NoMerge);
            region_local_state.set_merge_state(merge_state);
            if let Err(e) = self.engine.put_msg(&state_key, &region_local_state) {
                panic!("{} failed to persist rollbacked region merge state to db, err {:?}",
                       self.tag,
                       e);
            }
            self.merge_state = MergeState::NoMerge;
            self.start_merge_time = None;
            self.start_merge_index = 0;
        }
        Ok(())
    }

    pub fn maybe_region_merge_timeout(&mut self,
                                      d: Duration)
                                      -> Option<(metapb::Region, metapb::Region)> {
        if self.merge_state == MergeState::Merging {
            let t = self.start_merge_time.unwrap();
            if t.elapsed() >= d {
                // Reset `start_merging_time` on retry region merge.
                self.start_merge_time = Some(Instant::now());
                // Read `from_region` from local region merge state.
                let state_key = keys::region_state_key(self.region().get_id());
                let region_local_state =
                    self.engine.get_msg::<RegionLocalState>(&state_key).unwrap().unwrap();
                let from_region = region_local_state.get_merge_state().get_from_region().clone();
                return Some((from_region, self.region().clone()));
            }
        }
        None
    }

    pub fn is_ready_to_suspend_region(&self) -> bool {
        // If all peers in `into_region` have replicated to the state `Merging`, start to send
        // suspend region command to `from_region`. Otherwise, retry later in a specified timeout.
        self.raft_group.raft.all_replicated_to(self.start_merge_index)
    }

    pub fn handle_raft_ready<T: Transport>(&mut self,
                                           trans: &T,
                                           metrics: &mut RaftMetrics)
                                           -> Result<Option<ReadyResult>> {
        if self.mut_store().check_applying_snap() {
            // If we continue to handle all the messages, it may cause too many messages because
            // leader will send all the remaining messages to this follower, which can lead
            // to full message queue under high load.
            debug!("{} still applying snapshot, skip further handling.",
                   self.tag);
            return Ok(None);
        }

        if !self.raft_group.has_ready() {
            return Ok(None);
        }

        debug!("{} handle raft ready", self.tag);

        let mut ready = self.raft_group.ready();

        let t = SlowTimer::new();

        self.add_ready_metric(&ready, &mut metrics.ready);

        // The leader can write to disk and replicate to the followers concurrently
        // For more details, check raft thesis 10.2.1
        if self.is_leader() {
            try!(self.send(trans, ready.messages.drain(..), &mut metrics.message));
        }

        let apply_result = match self.mut_store().handle_raft_ready(&ready) {
            Ok(r) => r,
            Err(e) => {
                // Leader has to panic when failed to persist raft entries,
                // because all the MsgAppend messages have been sent out already.
                // Suppose there are 2 followers. If any follower sends back a
                // MsgAppendResponse, the leader will commit the non-persistent
                // entries.
                if self.is_leader() {
                    panic!("{} failed to handle raft ready: {:?}", self.tag, e);
                }
                return Err(e);
            }
        };

        if !self.is_leader() {
            try!(self.send(trans, ready.messages.drain(..), &mut metrics.message));
        }

        // Call `handle_raft_commit_entries` directly here may lead to inconsistency.
        // In some cases, there will be some pending committed entries when applying a
        // snapshot. If we call `handle_raft_commit_entries` directly, these updates
        // will be written to disk. Because we apply snapshot asynchronously, so these
        // updates will soon be removed. But the soft state of raft is still be updated
        // in memory. Hence when handle ready next time, these updates won't be included
        // in `ready.committed_entries` again, which will lead to inconsistency.
        let exec_results = if self.is_applying() {
            if let Some(ref mut hs) = ready.hs {
                // Snapshot's metadata has been applied.
                hs.set_commit(self.get_store().truncated_index());
            }
            vec![]
        } else {
            try!(self.handle_raft_commit_entries(&ready.committed_entries))
        };

        slow_log!(t,
                  "{} handle ready, entries {}, committed entries {}, messages \
                   {}, snapshot {}, hard state changed {}",
                  self.tag,
                  ready.entries.len(),
                  ready.committed_entries.len(),
                  ready.messages.len(),
                  apply_result.is_some(),
                  ready.hs.is_some());

        self.raft_group.advance(ready);
        Ok(Some(ReadyResult {
            apply_snap_result: apply_result,
            exec_results: exec_results,
        }))
    }

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
    pub fn propose(&mut self,
                   mut cmd: PendingCmd,
                   req: RaftCmdRequest,
                   mut err_resp: RaftCmdResponse)
                   -> bool {
        if self.merge_state == MergeState::Suspended &&
           (req.get_requests().len() != 0 || get_split_cmd(&req).is_some() ||
            get_merge_cmd(&req).is_some() || get_change_peer_cmd(&req).is_some()) {
            // When in `MergeState::Suspended` state, reject
            // 1. data read/write requests from client.
            // 2. region split, region merge, member change commands
            debug!("{} suspended region rejects data read/write requests and region split, \
                    region merge, member change commands",
                   self.tag);
            cmd_resp::bind_error(&mut err_resp, box_err!("region suspended"));
            cmd.call(err_resp);
            return false;
        } else if self.merge_state == MergeState::Merging &&
                  (get_split_cmd(&req).is_some() || get_merge_cmd(&req).is_some() ||
                   get_change_peer_cmd(&req).is_some()) {
            // When in `MergeState::Merging` state, reject
            // 1. region split, region merge, member change commands
            debug!("{} merging region rejects region split, region merge, member change commands",
                   self.tag);
            cmd_resp::bind_error(&mut err_resp, box_err!("region merging"));
            cmd.call(err_resp);
            return false;
        }

        if self.pending_cmds.contains(&cmd.uuid) {
            cmd_resp::bind_error(&mut err_resp, box_err!("duplicated uuid {:?}", cmd.uuid));
            cmd.call(err_resp);
            return false;
        }

        debug!("{} propose command with uuid {:?}", self.tag, cmd.uuid);
        PEER_PROPOSAL_COUNTER_VEC.with_label_values(&["all"]).inc();

        let local_read = self.is_local_read(&req);
        if local_read {
            PEER_PROPOSAL_COUNTER_VEC.with_label_values(&["local_read"]).inc();

            // for read-only, if we don't care stale read, we can
            // execute these commands immediately in leader.
            let mut ctx = ExecContext::new(self, 0, 0, &req);
            let (mut resp, _) = self.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
                error!("{} execute raft command err: {:?}", self.tag, e);
                (cmd_resp::new_error(e), None)
            });

            cmd_resp::bind_uuid(&mut resp, cmd.uuid);
            cmd_resp::bind_term(&mut resp, self.term());
            cmd.call(resp);
            return false;
        } else if get_transfer_leader_cmd(&req).is_some() {
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
            cmd.call(make_transfer_leader_response());
            return false;
        } else if get_change_peer_cmd(&req).is_some() {
            if self.raft_group.raft.pending_conf {
                info!("{} there is a pending conf change, try later", self.tag);
                cmd_resp::bind_error(&mut err_resp,
                                     box_err!("{} there is a pending conf change, try later",
                                              self.tag));
                cmd.call(err_resp);
                return false;
            }
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                // if it loses leadership before conf change is replicated, there may be
                // a stale pending conf change before next conf change is applied. If it
                // becomes leader again with the stale pending conf change, will enter
                // this block, so we notify leadership may have changed.
                self.notify_not_leader(cmd);
            }

            if let Err(e) = self.propose_conf_change(req) {
                cmd_resp::bind_error(&mut err_resp, e);
                cmd.call(err_resp);
                return false;
            }

            self.pending_cmds.set_conf_change(cmd);
        } else if let Err(e) = self.propose_normal(req) {
            cmd_resp::bind_error(&mut err_resp, e);
            cmd.call(err_resp);
            return false;
        } else {
            self.pending_cmds.append_normal(cmd);
        }

        true
    }

    fn is_local_read(&self, req: &RaftCmdRequest) -> bool {
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

        true
    }

    /// Call the callback of `cmd` that leadership may have been changed.
    ///
    /// Please note that, `NotLeader` here doesn't mean that currently this
    /// peer is not leader.
    fn notify_not_leader(&self, mut cmd: PendingCmd) {
        let leader = self.get_peer_from_cache(self.leader_id());
        let not_leader = Error::NotLeader(self.region_id, leader);
        let resp = cmd_resp::err_resp(not_leader, cmd.uuid, self.term());
        info!("{} command {} is stale, skip", self.tag, cmd.uuid);
        cmd.call(resp);
    }

    fn propose_normal(&mut self, mut cmd: RaftCmdRequest) -> Result<()> {
        PEER_PROPOSAL_COUNTER_VEC.with_label_values(&["normal"]).inc();

        // TODO: validate request for unexpected changes.
        try!(self.coprocessor_host.pre_propose(&self.raft_group.get_store(), &mut cmd));
        let data = try!(cmd.write_to_bytes());

        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        try!(self.raft_group.propose(data));
        Ok(())
    }

    fn transfer_leader(&mut self, peer: &metapb::Peer) {
        PEER_PROPOSAL_COUNTER_VEC.with_label_values(&["transfer_leader"]).inc();

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

    fn propose_conf_change(&mut self, cmd: RaftCmdRequest) -> Result<()> {
        PEER_PROPOSAL_COUNTER_VEC.with_label_values(&["conf_change"]).inc();

        let data = try!(cmd.write_to_bytes());

        PEER_PROPOSE_LOG_SIZE_HISTOGRAM.observe(data.len() as f64);

        let change_peer = get_change_peer_cmd(&cmd).unwrap();

        let mut cc = eraftpb::ConfChange::new();
        cc.set_change_type(change_peer.get_change_type());
        cc.set_node_id(change_peer.get_peer().get_id());
        cc.set_context(data);

        info!("{} propose conf change {:?} peer {:?}",
              self.tag,
              cc.get_change_type(),
              cc.get_node_id());

        self.raft_group.propose_conf_change(cc).map_err(From::from)
    }

    pub fn check_epoch(&self, req: &RaftCmdRequest) -> Result<()> {
        let (mut check_ver, mut check_conf_ver) = (false, false);
        if req.has_admin_request() {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::CompactLog |
                AdminCmdType::InvalidAdmin => {}
                AdminCmdType::Split => check_ver = true,
                AdminCmdType::ChangePeer => check_conf_ver = true,
                AdminCmdType::TransferLeader |
                AdminCmdType::Merge |
                AdminCmdType::SuspendRegion |
                AdminCmdType::CommitMerge |
                AdminCmdType::ShutdownRegion => {
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
        let latest_region = self.region();
        let latest_epoch = latest_region.get_region_epoch();

        // should we use not equal here?
        if (check_conf_ver && from_epoch.get_conf_ver() < latest_epoch.get_conf_ver()) ||
           (check_ver && from_epoch.get_version() < latest_epoch.get_version()) {
            debug!("{} received stale epoch {:?}, mime: {:?}",
                   self.tag,
                   from_epoch,
                   latest_epoch);
            return Err(Error::StaleEpoch(format!("latest_epoch of region {} is {:?}, but you \
                                                  sent {:?}",
                                                 self.region_id,
                                                 latest_epoch,
                                                 from_epoch),
                                         vec![self.region().to_owned()]));
        }

        Ok(())
    }

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

    fn send_raft_message<T: Transport>(&mut self, msg: eraftpb::Message, trans: &T) -> Result<()> {
        let mut send_msg = RaftMessage::new();
        send_msg.set_region_id(self.region_id);
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        let mut unreachable = false;

        let from_peer = match self.get_peer_from_cache(msg.get_from()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to lookup sender peer {} in region {}",
                                    msg.get_from(),
                                    self.region_id))
            }
        };

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

            unreachable = true;
        }

        if unreachable {
            self.raft_group.report_unreachable(to_peer_id);

            if msg_type == eraftpb::MessageType::MsgSnapshot {
                self.raft_group.report_snapshot(to_peer_id, SnapshotStatus::Failure);
            }
        }

        Ok(())
    }

    fn handle_raft_commit_entries(&mut self,
                                  committed_entries: &[eraftpb::Entry])
                                  -> Result<Vec<ExecResult>> {
        // We can't apply committed entries when this peer is still applying snapshot.
        assert!(!self.is_applying());
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let t = SlowTimer::new();
        let mut results = vec![];
        let committed_count = committed_entries.len();
        for entry in committed_entries {
            if self.pending_remove {
                // This peer is about to be destroyed, skip everything.
                break;
            }

            let expect_index = self.get_store().applied_index() + 1;
            if expect_index != entry.get_index() {
                panic!("{} expect index {}, but got {}",
                       self.tag,
                       expect_index,
                       entry.get_index());
            }

            let res = try!(match entry.get_entry_type() {
                eraftpb::EntryType::EntryNormal => self.handle_raft_entry_normal(entry),
                eraftpb::EntryType::EntryConfChange => self.handle_raft_entry_conf_change(entry),
            });

            if let Some(res) = res {
                results.push(res);
            }
        }

        slow_log!(t,
                  "{} handle {} committed entries",
                  self.tag,
                  committed_count);
        Ok(results)
    }

    fn handle_raft_entry_normal(&mut self, entry: &eraftpb::Entry) -> Result<Option<ExecResult>> {
        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if data.is_empty() {
            // when a peer become leader, it will send an empty entry.
            let wb = WriteBatch::new();
            let mut state = self.get_store().apply_state.clone();
            state.set_applied_index(index);
            let engine = self.engine.clone();
            let raft_cf = try!(rocksdb::get_cf_handle(engine.as_ref(), CF_RAFT));
            try!(wb.put_msg_cf(raft_cf, &keys::apply_state_key(self.region_id), &state));
            try!(self.engine.write(wb));
            self.mut_store().apply_state = state;
            self.mut_store().applied_index_term = term;
            assert!(term > 0);
            while let Some(cmd) = self.pending_cmds.pop_normal(term - 1) {
                // apprently, all the callbacks whose term is less than entry's term are stale.
                self.notify_not_leader(cmd);
            }
            return Ok(None);
        }

        let cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
        Ok(self.process_raft_cmd(index, term, cmd))
    }

    fn handle_raft_entry_conf_change(&mut self,
                                     entry: &eraftpb::Entry)
                                     -> Result<Option<ExecResult>> {
        let index = entry.get_index();
        let term = entry.get_term();
        let conf_change = try!(protobuf::parse_from_bytes::<eraftpb::ConfChange>(entry.get_data()));
        let cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(conf_change.get_context()));
        let (res, cc) = match self.process_raft_cmd(index, term, cmd) {
            res @ Some(_) => (res, conf_change),
            // If failed, tell raft that the config change was aborted.
            None => (None, eraftpb::ConfChange::new()),
        };
        self.raft_group.apply_conf_change(cc);

        Ok(res)
    }

    fn find_cb(&mut self, uuid: Uuid, term: u64, cmd: &RaftCmdRequest) -> Option<Callback> {
        if get_change_peer_cmd(cmd).is_some() {
            if let Some(mut cmd) = self.pending_cmds.take_conf_change() {
                if cmd.uuid == uuid {
                    return Some(cmd.cb.take().unwrap());
                } else {
                    self.notify_not_leader(cmd);
                }
            }
            return None;
        }
        while let Some(mut head) = self.pending_cmds.pop_normal(term) {
            if head.uuid == uuid {
                return Some(head.cb.take().unwrap());
            }
            // because of the lack of original RaftCmdRequest, we skip calling
            // coprocessor here.
            // TODO: call coprocessor with uuid instead.
            self.notify_not_leader(head);
        }
        None
    }

    fn process_raft_cmd(&mut self,
                        index: u64,
                        term: u64,
                        cmd: RaftCmdRequest)
                        -> Option<ExecResult> {
        if index == 0 {
            panic!("{} processing raft command needs a none zero index",
                   self.tag);
        }

        let uuid = util::get_uuid_from_req(&cmd).unwrap();
        let cb = self.find_cb(uuid, term, &cmd);
        let timer = PEER_APPLY_LOG_HISTOGRAM.start_timer();
        let (mut resp, exec_result) = self.apply_raft_cmd(index, term, &cmd);
        timer.observe_duration();

        debug!("{} applied command with uuid {:?} at log index {}",
               self.tag,
               uuid,
               index);

        if cb.is_none() {
            return exec_result;
        }

        let cb = cb.unwrap();
        self.coprocessor_host.post_apply(self.raft_group.get_store(), &cmd, &mut resp);
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        // Bind uuid here.
        cmd_resp::bind_uuid(&mut resp, uuid);
        cmd_resp::bind_term(&mut resp, self.term());
        cb.call_box((resp,));

        exec_result
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    // apply operation can fail as following situation:
    //   1. encouter an error that will occur on all store, it can continue
    // applying next entry safely, like stale epoch for example;
    //   2. encouter an error that may not occur on all store, in this case
    // we should try to apply the entry again or panic. Considering that this
    // usually due to disk operation fail, which is rare, so just panic is ok.
    fn apply_raft_cmd(&mut self,
                      index: u64,
                      term: u64,
                      req: &RaftCmdRequest)
                      -> (RaftCmdResponse, Option<ExecResult>) {
        // if pending remove, apply should be aborted already.
        assert!(!self.pending_remove);

        let mut ctx = ExecContext::new(self, index, term, req);
        let (resp, exec_result) = self.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
            error!("{} execute raft command err: {:?}", self.tag, e);
            (cmd_resp::new_error(e), None)
        });

        ctx.apply_state.set_applied_index(index);
        if !self.pending_remove {
            ctx.save(self.region_id)
                .unwrap_or_else(|e| panic!("{} failed to save apply context: {:?}", self.tag, e));
        }

        // Commit write and change storage fields atomically.
        self.mut_store()
            .engine
            .write(ctx.wb)
            .unwrap_or_else(|e| panic!("{} failed to commit apply result: {:?}", self.tag, e));

        self.mut_store().apply_state = ctx.apply_state;
        self.mut_store().applied_index_term = term;

        if let Some(ref exec_result) = exec_result {
            match *exec_result {
                ExecResult::ChangePeer { ref region, .. } => {
                    self.mut_store().region = region.clone();
                }
                ExecResult::CompactLog { .. } |
                ExecResult::ShutdownRegion { .. } => {}
                ExecResult::SplitRegion { ref left, .. } => {
                    self.mut_store().region = left.clone();
                }
                ExecResult::StartMerge { .. } => {
                    self.merge_state = MergeState::Merging;
                    self.start_merge_time = Some(Instant::now());
                    self.start_merge_index = index;
                }
                ExecResult::SuspendRegion { .. } => {
                    self.merge_state = MergeState::Suspended;
                }
                ExecResult::CommitMerge { ref new, .. } => {
                    self.mut_store().region = new.clone();
                    self.merge_state = MergeState::NoMerge;
                    self.start_merge_time = None;
                    self.start_merge_index = 0;
                }
            }
        }

        (resp, exec_result)
    }

    /// Clear all the pending commands.
    ///
    /// Please note that all the pending callbacks will be lost.
    /// Should not do this when dropping a peer in case of possible leak.
    pub fn clear_pending_commands(&mut self) {
        if !self.pending_cmds.normals.is_empty() {
            info!("{} clear {} commands",
                  self.tag,
                  self.pending_cmds.normals.len());
            while let Some(mut cmd) = self.pending_cmds.normals.pop_front() {
                cmd.cb.take();
            }
        }
        if let Some(mut cmd) = self.pending_cmds.conf_change.take() {
            info!("{} clear pending conf change", self.tag);
            cmd.cb.take();
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

fn get_change_peer_cmd(msg: &RaftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}

fn get_split_cmd(msg: &RaftCmdRequest) -> Option<&SplitRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_split() {
        return None;
    }

    Some(req.get_split())
}

fn get_merge_cmd(msg: &RaftCmdRequest) -> Option<&MergeRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_merge() {
        return None;
    }

    Some(req.get_merge())
}

struct ExecContext<'a> {
    pub snap: Snapshot,
    pub apply_state: RaftApplyState,
    pub wb: WriteBatch,
    pub req: &'a RaftCmdRequest,
    pub index: u64,
    pub term: u64,
}

impl<'a> ExecContext<'a> {
    fn new<'b>(peer: &'b Peer, index: u64, term: u64, req: &'a RaftCmdRequest) -> ExecContext<'a> {
        ExecContext {
            snap: Snapshot::new(peer.engine.clone()),
            apply_state: peer.get_store().apply_state.clone(),
            wb: WriteBatch::new(),
            req: req,
            index: index,
            term: term,
        }
    }

    fn save(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(self.snap.cf_handle(CF_RAFT));
        try!(self.wb.put_msg_cf(raft_cf,
                                &keys::apply_state_key(region_id),
                                &self.apply_state));
        Ok(())
    }
}

// Here we implement all commands.
impl Peer {
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(&mut self,
                     ctx: &mut ExecContext)
                     -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        try!(self.check_epoch(ctx.req));
        if ctx.req.has_admin_request() {
            self.exec_admin_cmd(ctx)
        } else {
            // Now we don't care write command outer, so use None.
            self.exec_write_cmd(ctx).and_then(|v| Ok((v, None)))
        }
    }

    fn exec_admin_cmd(&mut self,
                      ctx: &mut ExecContext)
                      -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        let request = ctx.req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        info!("{} execute admin command {:?} at [term: {}, index: {}]",
              self.tag,
              request,
              ctx.term,
              ctx.index);

        let (mut response, exec_result) = try!(match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::Split => self.exec_split(ctx, request),
            AdminCmdType::Merge => self.exec_merge(ctx, request),
            AdminCmdType::SuspendRegion => self.exec_suspend_region(ctx, request),
            AdminCmdType::CommitMerge => self.exec_commit_merge(ctx, request),
            AdminCmdType::ShutdownRegion => self.exec_shutdown_region(ctx, request),
            AdminCmdType::CompactLog => self.exec_compact_log(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_change_peer(&mut self,
                        ctx: &ExecContext,
                        request: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region().clone();

        info!("{} exec ConfChange {:?}, epoch: {:?}",
              self.tag,
              util::conf_change_type_str(&change_type),
              region.get_region_epoch());

        // Skip applying change peer command when this region is in region merge procedure.
        if self.is_in_region_merge() {
            info!("{} ignore change peer request: {:?}, since it's in region merge procedure",
                  self.tag,
                  request);
            return Err(box_err!("ignore change peer request: {:?} when region is in region merge",
                                request));
        }

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let exists = util::find_peer(&region, store_id).is_some();
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;

        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            eraftpb::ConfChangeType::AddNode => {
                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["add_peer", "all"]).inc();

                if exists {
                    error!("{} can't add duplicated peer {:?} to region {:?}",
                           self.tag,
                           peer,
                           self.region());
                    return Err(box_err!("can't add duplicated peer {:?} to region {:?}",
                                        peer,
                                        self.region()));
                }
                // TODO: Do we allow adding peer in same node?

                // Add this peer to cache.
                self.peer_cache.borrow_mut().insert(peer.get_id(), peer.clone());
                self.peer_heartbeats.insert(peer.get_id(), Instant::now());
                region.mut_peers().push(peer.clone());

                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["add_peer", "success"]).inc();

                info!("{} add peer {:?} to region {:?}",
                      self.tag,
                      peer,
                      self.region());
            }
            eraftpb::ConfChangeType::RemoveNode => {
                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["remove_peer", "all"]).inc();

                if !exists {
                    error!("{} remove missing peer {:?} from region {:?}",
                           self.tag,
                           peer,
                           self.region());
                    return Err(box_err!("remove missing peer {:?} from region {:?}",
                                        peer,
                                        self.region()));
                }

                if self.peer_id() == peer.get_id() {
                    // Remove ourself, we will destroy all region data later.
                    // So we need not to apply following logs.
                    self.pending_remove = true;
                }

                // Remove this peer from cache.
                self.peer_cache.borrow_mut().remove(&peer.get_id());
                self.peer_heartbeats.remove(&peer.get_id());
                util::remove_peer(&mut region, store_id).unwrap();

                PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["remove_peer", "success"]).inc();

                info!("{} remove {} from region:{:?}",
                      self.tag,
                      peer.get_id(),
                      self.region());
            }
        }

        if self.pending_remove {
            self.get_store()
                .clear_meta(&ctx.wb)
                .and_then(|_| write_peer_state(&ctx.wb, &region, PeerState::Tombstone))
                .unwrap_or_else(|e| panic!("{} failed to remove self: {:?}", self.tag, e));
        } else {
            write_peer_state(&ctx.wb, &region, PeerState::Normal)
                .unwrap_or_else(|e| panic!("{} failed to update region state: {:?}", self.tag, e));
        }

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        Ok((resp,
            Some(ExecResult::ChangePeer {
            change_type: change_type,
            peer: peer.clone(),
            region: region,
        })))
    }

    fn exec_split(&mut self,
                  ctx: &ExecContext,
                  req: &AdminRequest)
                  -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["split", "all"]).inc();

        let split_req = req.get_split();
        if !split_req.has_split_key() {
            return Err(box_err!("missing split key"));
        }

        let split_key = split_req.get_split_key();
        let mut region = self.region().clone();
        if split_key <= region.get_start_key() {
            return Err(box_err!("invalid split request: {:?}", split_req));
        }

        try!(util::check_key_in_region(split_key, &region));

        info!("{} split at key: {}, region: {:?}",
              self.tag,
              escape(split_key),
              region);

        // Skip applying region split command when this region is in region merge procedure.
        if self.is_in_region_merge() {
            info!("{} ignore region split request at key: {}, since it's in region merge \
                   procedure",
                  self.tag,
                  escape(split_key));
            return Err(box_err!("ignore split request: {:?} when region is in region merge",
                                split_req));
        }

        // TODO: check new region id validation.
        let new_region_id = split_req.get_new_region_id();

        // After split, the origin region key range is [start_key, split_key),
        // the new split region is [split_key, end).
        let mut new_region = region.clone();
        region.set_end_key(split_key.to_vec());

        new_region.set_start_key(split_key.to_vec());
        new_region.set_id(new_region_id);

        // Update new region peer ids.
        let new_peer_ids = split_req.get_new_peer_ids();
        if new_peer_ids.len() != new_region.get_peers().len() {
            return Err(box_err!("invalid new peer id count, need {}, but got {}",
                                new_region.get_peers().len(),
                                new_peer_ids.len()));
        }

        for (index, peer) in new_region.mut_peers().iter_mut().enumerate() {
            let peer_id = new_peer_ids[index];
            peer.set_id(peer_id);

            // Add this peer to cache.
            self.peer_cache.borrow_mut().insert(peer_id, peer.clone());
        }

        // update region version
        let region_ver = region.get_region_epoch().get_version() + 1;
        region.mut_region_epoch().set_version(region_ver);
        new_region.mut_region_epoch().set_version(region_ver);
        write_peer_state(&ctx.wb, &region, PeerState::Normal)
            .and_then(|_| write_peer_state(&ctx.wb, &new_region, PeerState::Normal))
            .and_then(|_| write_initial_state(self.engine.as_ref(), &ctx.wb, new_region.get_id()))
            .unwrap_or_else(|e| {
                panic!("{} failed to save split region {:?}: {:?}",
                       self.tag,
                       new_region,
                       e)
            });

        let mut resp = AdminResponse::new();
        resp.mut_split().set_left(region.clone());
        resp.mut_split().set_right(new_region.clone());

        self.size_diff_hint = 0;
        self.delete_keys_hint = 0;

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["split", "success"]).inc();

        Ok((resp,
            Some(ExecResult::SplitRegion {
            left: region,
            right: new_region,
        })))
    }

    fn set_peer_merge_state<T: Mutable>(&mut self, w: &T, state: RegionMergeState) -> Result<()> {
        let state_key = keys::region_state_key(self.region().get_id());
        let mut region_local_state = try!(self.engine.get_msg::<RegionLocalState>(&state_key))
            .unwrap();

        region_local_state.set_merge_state(state);
        try!(w.put_msg(&state_key, &region_local_state));
        Ok(())
    }

    fn exec_merge(&mut self,
                  ctx: &ExecContext,
                  req: &AdminRequest)
                  -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["start merge", "all"]).inc();

        let merge_req = req.get_merge();
        info!("{} merge from region {:?} into region {:?}",
              self.tag,
              merge_req.get_from_region(),
              self.region());

        let mut resp = AdminResponse::new();
        resp.set_merge(MergeResponse::new());

        {
            // Ensure the into_region epoch is not outdated.
            let epoch = merge_req.get_into_region().get_region_epoch();
            let latest_epoch = self.region().get_region_epoch();
            if util::is_epoch_stale(epoch, latest_epoch) {
                // The region epoch in this merge request is outdated, because
                // a pending region split or member change is applied and
                // the region epoch has changed.
                // Ignore this merge request.
                return Ok((resp, None));
            }
        }

        if self.merge_state != MergeState::NoMerge {
            // Just ignore this region merge command since this peer is already
            // in `Merging` or `Suspended` state.
            info!("{} already in merge state {:?}, ignore region merge request {:?}",
                  self.tag,
                  self.merge_state,
                  merge_req);
            Ok((resp, None))
        } else {
            // Update peer local state.
            let mut merge_state = RegionMergeState::new();
            merge_state.set_state(MergeState::Merging);
            merge_state.set_from_region(merge_req.get_from_region().clone());
            self.set_peer_merge_state(&ctx.wb, merge_state.clone()).unwrap_or_else(|e| {
                panic!("{} failed to set peer merge state {:?}: {:?}",
                       self.tag,
                       merge_state,
                       e);
            });

            PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["start merge", "success"]).inc();

            Ok((resp,
                Some(ExecResult::StartMerge {
                from_region: merge_req.get_from_region().clone(),
                into_region: self.region().clone(),
            })))
        }
    }

    fn exec_suspend_region(&mut self,
                           ctx: &ExecContext,
                           req: &AdminRequest)
                           -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["suspend", "all"]).inc();

        let suspend_region = req.get_suspend_region();

        info!("{} suspend from region {:?} by into region {:?}",
              self.tag,
              suspend_region.get_from_region(),
              suspend_region.get_into_region());

        let resp = AdminResponse::new();

        {
            // Ensure the region epoch in suspend command is not outdated.
            let epoch = suspend_region.get_from_region().get_region_epoch();
            let latest_epoch = self.region().get_region_epoch();
            if util::is_epoch_stale(epoch, latest_epoch) {
                // The region epoch in this request is outdated, because
                // a pending region split, member change or region merge is applied and
                // the region epoch has changed.
                // The `into_region` will notice this region epoch change and
                // rollback the region merge, when it scans the `from_region` info periodically.
                return Ok((resp, None));
            }
        }

        if self.merge_state != MergeState::Suspended {
            // If the following `ExecResult::SuspendRegion` is lost due to a node crash,
            // this suspend region command will be retried by the leader of `into_region`
            // in a region merge procedure.
            // Skip to write the `Suspended` state to hard driver when the command is a retry.
            let mut merge_state = RegionMergeState::new();
            merge_state.set_state(MergeState::Suspended);
            self.set_peer_merge_state(&ctx.wb, merge_state.clone()).unwrap_or_else(|e| {
                panic!("{} failed to set peer merge state {:?}: {:?}",
                       self.tag,
                       merge_state,
                       e);
            });
        }

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["suspend", "success"]).inc();

        Ok((resp,
            Some(ExecResult::SuspendRegion {
            from_region: suspend_region.get_from_region().clone(),
            into_region: suspend_region.get_into_region().clone(),
        })))
    }

    fn exec_commit_merge(&mut self,
                         ctx: &ExecContext,
                         req: &AdminRequest)
                         -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["commit merge", "all"]).inc();

        let commit_merge = req.get_commit_merge();

        info!("{} commit region merge from region {:?} into region {:?}",
              self.tag,
              commit_merge.get_from_region(),
              self.region());

        let resp = AdminResponse::new();

        if self.merge_state != MergeState::Merging {
            // Just ignore this command since it's a retry of previous region merge command.
            info!("{} not in region merge Merging state, ignore commit merge {:?}",
                  self.tag,
                  commit_merge);
            Ok((resp, None))
        } else {
            let state_key = keys::region_state_key(self.region().get_id());
            let mut region_local_state = self.engine
                .get_msg::<RegionLocalState>(&state_key)
                .unwrap()
                .unwrap();
            let mut merge_state = RegionMergeState::new();
            // Update region's merge state
            merge_state.set_state(MergeState::NoMerge);
            region_local_state.set_merge_state(merge_state);
            // Merge region range
            let to_shutdown = commit_merge.get_from_region().clone();
            let old_region = self.region().clone();
            let mut new_region = old_region.clone();
            // The ranges in r1, r2 should be adjacent.
            // TODO add checking for that relationship
            if to_shutdown.get_start_key() < old_region.get_start_key() {
                // The range of r1 is ahead of the range of r2
                new_region.set_start_key(to_shutdown.get_start_key().to_vec());
                new_region.set_end_key(old_region.get_end_key().to_vec());
            } else {
                // The range of r2 is ahead of the range of r1
                new_region.set_start_key(old_region.get_start_key().to_vec());
                new_region.set_end_key(to_shutdown.get_end_key().to_vec());
            }
            // Update region epoch version.
            let region_version = old_region.get_region_epoch().get_version() + 1;
            new_region.mut_region_epoch().set_version(region_version);
            region_local_state.set_region(new_region.clone());
            // Persist peer local state
            ctx.wb.put_msg(&state_key, &region_local_state.clone()).unwrap_or_else(|e| {
                panic!("{} failed to set peer state {:?}: {:?}",
                       self.tag,
                       region_local_state,
                       e);
            });

            PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["commit merge", "success"]).inc();

            Ok((resp,
                Some(ExecResult::CommitMerge {
                new: new_region,
                old: old_region,
                to_shutdown: commit_merge.get_from_region().clone(),
            })))
        }
    }

    fn exec_shutdown_region(&mut self,
                            _ctx: &ExecContext,
                            req: &AdminRequest)
                            -> Result<(AdminResponse, Option<ExecResult>)> {
        let shutdown_region = req.get_shutdown_region();
        let region = shutdown_region.get_region();

        info!("{} shutdown region {:?}", self.tag, region);

        // TODO check region == self.region()

        let resp = AdminResponse::new();

        // If this peer is in the region which is asked to shutdown, it will destroy itself.
        let peer_id = self.peer_id();
        if region.get_peers().iter().any(|p| p.get_id() == peer_id) {
            Ok((resp,
                Some(ExecResult::ShutdownRegion {
                region: self.region().clone(),
                peer: self.peer.clone(),
            })))
        } else {
            Ok((resp, None))
        }
    }

    fn exec_compact_log(&mut self,
                        ctx: &mut ExecContext,
                        req: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["compact", "all"]).inc();

        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::new();

        let first_index = self.get_store().first_index();
        if compact_index <= first_index {
            debug!("{} compact index {} <= first index {}, no need to compact",
                   self.tag,
                   compact_index,
                   first_index);
            return Ok((resp, None));
        }

        // compact failure is safe to be omitted, no need to assert.
        try!(self.get_store().compact(&mut ctx.apply_state, compact_index));

        PEER_ADMIN_CMD_COUNTER_VEC.with_label_values(&["compact", "success"]).inc();

        Ok((resp,
            Some(ExecResult::CompactLog { state: ctx.apply_state.get_truncated_state().clone() })))
    }

    fn exec_write_cmd(&mut self, ctx: &ExecContext) -> Result<RaftCmdResponse> {
        let requests = ctx.req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = try!(match cmd_type {
                CmdType::Get => self.do_get(ctx, req),
                CmdType::Put => self.do_put(ctx, req),
                CmdType::Delete => self.do_delete(ctx, req),
                CmdType::Snap => self.do_snap(ctx, req),
                CmdType::Invalid => Err(box_err!("invalid cmd type, message maybe currupted")),
            });

            resp.set_cmd_type(cmd_type);

            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
    }

    fn check_data_key(&self, key: &[u8]) -> Result<()> {
        // region key range has no data prefix, so we must use origin key to check.
        try!(util::check_key_in_region(key, self.get_store().get_region()));

        Ok(())
    }

    fn do_get(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        // TODO: the get_get looks wried, maybe we should figure out a better name later.
        let key = req.get_get().get_key();
        try!(self.check_data_key(key));

        let mut resp = Response::new();
        let res = if req.get_get().has_cf() {
            let cf = req.get_get().get_cf();
            // TODO: check whether cf exists or not.
            ctx.snap.get_value_cf(cf, &keys::data_key(key)).unwrap_or_else(|e| {
                panic!("{} failed to get {} with cf {}: {:?}",
                       self.tag,
                       escape(key),
                       cf,
                       e)
            })
        } else {
            ctx.snap
                .get_value(&keys::data_key(key))
                .unwrap_or_else(|e| panic!("{} failed to get {}: {:?}", self.tag, escape(key), e))
        };
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    fn do_put(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        try!(self.check_data_key(key));

        let resp = Response::new();
        let key = keys::data_key(key);
        if let Some(diff) = self.size_diff_hint.checked_add(key.len() as u64) {
            self.size_diff_hint = diff;
        }
        if let Some(diff) = self.size_diff_hint.checked_add(value.len() as u64) {
            self.size_diff_hint = diff;
        }
        self.size_diff_hint += key.len() as u64;
        self.size_diff_hint += value.len() as u64;
        if req.get_put().has_cf() {
            let cf = req.get_put().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&self.engine, cf)
                .and_then(|handle| ctx.wb.put_cf(handle, &key, value))
                .unwrap_or_else(|e| {
                    panic!("{} failed to write ({}, {}) to cf {}: {:?}",
                           self.tag,
                           escape(&key),
                           escape(value),
                           cf,
                           e)
                });
        } else {
            ctx.wb.put(&key, value).unwrap_or_else(|e| {
                panic!("{} failed to write ({}, {}): {:?}",
                       self.tag,
                       escape(&key),
                       escape(value),
                       e);
            });
        }
        Ok(resp)
    }

    fn do_delete(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        try!(self.check_data_key(key));

        let key = keys::data_key(key);
        // since size_diff_hint is not accurate, so we just skip calculate the value size.
        let klen = key.len() as u64;
        if self.size_diff_hint > klen {
            self.size_diff_hint -= klen;
        } else {
            self.size_diff_hint = 0;
        }
        self.delete_count += 1;
        let resp = Response::new();
        if req.get_delete().has_cf() {
            let cf = req.get_delete().get_cf();
            // TODO: check whether cf exists or not.
            rocksdb::get_cf_handle(&self.engine, cf)
                .and_then(|handle| ctx.wb.delete_cf(handle, &key))
                .unwrap_or_else(|e| {
                    panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
                });
            // lock cf is compact periodically.
            if cf != CF_LOCK {
                self.delete_keys_hint += 1;
            }
        } else {
            ctx.wb.delete(&key).unwrap_or_else(|e| {
                panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
            });
            self.delete_keys_hint += 1;
        }

        Ok(resp)
    }

    fn do_snap(&mut self, _: &ExecContext, _: &Request) -> Result<Response> {
        let mut resp = Response::new();
        resp.mut_snap().set_region(self.get_store().get_region().clone());
        Ok(resp)
    }
}

fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::new();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::new());
    let mut resp = RaftCmdResponse::new();
    resp.set_admin_response(response);
    resp
}
