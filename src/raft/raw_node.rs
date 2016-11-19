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

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


use raft::errors::{Result, Error};
use raft::Storage;
use protobuf::{self, RepeatedField};
use kvproto::eraftpb::{HardState, Entry, EntryType, Message, Snapshot, MessageType, ConfChange,
                       ConfChangeType, ConfState};
use raft::raft::{Config, Raft, SoftState, INVALID_ID};
use raft::Status;
use raft::read_only::ReadState;

#[derive(Debug, Default)]
pub struct Peer {
    pub id: u64,
    pub context: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SnapshotStatus {
    Finish,
    Failure,
}

fn is_local_msg(t: MessageType) -> bool {
    match t {
        MessageType::MsgHup |
        MessageType::MsgBeat |
        MessageType::MsgUnreachable |
        MessageType::MsgSnapStatus |
        MessageType::MsgCheckQuorum => true,
        _ => false,
    }
}

fn is_response_msg(t: MessageType) -> bool {
    match t {
        MessageType::MsgAppendResponse |
        MessageType::MsgRequestVoteResponse |
        MessageType::MsgHeartbeatResponse |
        MessageType::MsgUnreachable => true,
        _ => false,
    }
}

pub fn vote_resp_msg_type(t: MessageType) -> MessageType {
    match t {
        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
        _ => panic!("Not a vote message: {:?}", t),
    }
}

pub fn is_empty_snap(s: &Snapshot) -> bool {
    s.get_metadata().get_index() == 0
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
#[derive(Default, Debug, PartialEq)]
pub struct Ready {
    // The current volatile state of a Node.
    // SoftState will be nil if there is no update.
    // It is not required to consume or store SoftState.
    pub ss: Option<SoftState>,

    // The current state of a Node to be saved to stable storage BEFORE
    // Messages are sent.
    // HardState will be equal to empty state if there is no update.
    pub hs: Option<HardState>,

    // read_states states can be used for node to serve linearizable read requests locally
    // when its applied index is greater than the index in ReadState.
    // Note that the read_state will be returned when raft receives MsgReadIndex.
    // The returned is only valid for the request that requested to read.
    pub read_states: Vec<ReadState>,

    // Entries specifies entries to be saved to stable storage BEFORE
    // Messages are sent.
    pub entries: Vec<Entry>,

    // Snapshot specifies the snapshot to be saved to stable storage.
    pub snapshot: Snapshot,

    // CommittedEntries specifies entries to be committed to a
    // store/state-machine. These have previously been committed to stable
    // store.
    pub committed_entries: Vec<Entry>,

    // Messages specifies outbound messages to be sent AFTER Entries are
    // committed to stable storage.
    // If it contains a MsgSnap message, the application MUST report back to raft
    // when the snapshot has been received or has failed by calling ReportSnapshot.
    pub messages: Vec<Message>,
}

impl Ready {
    fn new<T: Storage>(raft: &mut Raft<T>, prev_ss: &SoftState, prev_hs: &HardState) -> Ready {
        let mut rd = Ready {
            entries: raft.raft_log.unstable_entries().unwrap_or(&[]).to_vec(),
            committed_entries: raft.raft_log.next_entries().unwrap_or_else(Vec::new),
            messages: raft.msgs.drain(..).collect(),
            ..Default::default()
        };
        let ss = raft.soft_state();
        if &ss != prev_ss {
            rd.ss = Some(ss);
        }
        let hs = raft.hard_state();
        if &hs != prev_hs {
            rd.hs = Some(hs);
        }
        if raft.raft_log.get_unstable().snapshot.is_some() {
            rd.snapshot = raft.raft_log.get_unstable().snapshot.clone().unwrap();
        }
        if !raft.read_states.is_empty() {
            rd.read_states = raft.read_states.clone();
        }
        rd
    }
}

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.
pub struct RawNode<T: Storage> {
    pub raft: Raft<T>,
    prev_ss: SoftState,
    prev_hs: HardState,
}

impl<T: Storage> RawNode<T> {
    // NewRawNode returns a new RawNode given configuration and a list of raft peers.
    pub fn new(config: &Config, store: T, peers: &[Peer]) -> Result<RawNode<T>> {
        assert!(config.id != 0, "config.id must not be zero");
        let r = Raft::new(config, store);
        let mut rn = RawNode {
            raft: r,
            prev_hs: Default::default(),
            prev_ss: Default::default(),
        };
        let last_index = rn.raft.get_store().last_index().expect("");
        if last_index == 0 {
            rn.raft.become_follower(1, INVALID_ID);
            let mut ents = Vec::with_capacity(peers.len());
            for (i, peer) in peers.iter().enumerate() {
                let mut cc = ConfChange::new();
                cc.set_change_type(ConfChangeType::AddNode);
                cc.set_node_id(peer.id);
                if peer.context.is_some() {
                    cc.set_context(peer.context.as_ref().unwrap().clone());
                }
                let data = protobuf::Message::write_to_bytes(&cc)
                    .expect("unexpected marshal error");
                let mut e = Entry::new();
                e.set_entry_type(EntryType::EntryConfChange);
                e.set_term(1);
                e.set_index(i as u64 + 1);
                e.set_data(data);
                ents.push(e);
            }
            rn.raft.raft_log.append(&ents);
            rn.raft.raft_log.committed = ents.len() as u64;
            for peer in peers {
                rn.raft.add_node(peer.id);
            }
        }
        rn.prev_ss = rn.raft.soft_state();
        if last_index == 0 {
            rn.prev_hs = Default::default();
        } else {
            rn.prev_hs = rn.raft.hard_state();
        }
        Ok(rn)
    }

    fn commit_ready(&mut self, rd: Ready) {
        if rd.ss.is_some() {
            self.prev_ss = rd.ss.unwrap();
        }
        if let Some(e) = rd.hs {
            if e != HardState::new() {
                self.prev_hs = e;
            }
        }
        if self.prev_hs.get_commit() != 0 {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            self.raft.raft_log.applied_to(self.prev_hs.get_commit());
        }
        if !rd.entries.is_empty() {
            let e = rd.entries.last().unwrap();
            self.raft.raft_log.stable_to(e.get_index(), e.get_term());
        }
        if rd.snapshot != Snapshot::new() {
            self.raft.raft_log.stable_snap_to(rd.snapshot.get_metadata().get_index());
        }
        if !rd.read_states.is_empty() {
            self.raft.read_states.clear();
        }
    }

    // Tick advances the internal logical clock by a single tick.
    pub fn tick(&mut self) {
        self.raft.tick();
    }

    // Campaign causes this RawNode to transition to candidate state.
    pub fn campaign(&mut self) -> Result<()> {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgHup);
        self.raft.step(m)
    }

    // Propose proposes data be appended to the raft log.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<()> {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgPropose);
        m.set_from(self.raft.id);
        let mut e = Entry::new();
        e.set_data(data);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m)
    }

    // ProposeConfChange proposes a config change.
    pub fn propose_conf_change(&mut self, cc: ConfChange) -> Result<()> {
        let data = box_try!(protobuf::Message::write_to_bytes(&cc));
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgPropose);
        let mut e = Entry::new();
        e.set_entry_type(EntryType::EntryConfChange);
        e.set_data(data);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m)
    }

    pub fn apply_conf_change(&mut self, cc: ConfChange) -> ConfState {
        if cc.get_node_id() == INVALID_ID {
            self.raft.reset_pending_conf();
            let mut cs = ConfState::new();
            cs.set_nodes(self.raft.nodes());
            return cs;
        }
        let nid = cc.get_node_id();
        assert!(cc.has_change_type(), "unexpected conf type");
        match cc.get_change_type() {
            ConfChangeType::AddNode => self.raft.add_node(nid),
            ConfChangeType::RemoveNode => self.raft.remove_node(nid),
        }
        let mut cs = ConfState::new();
        cs.set_nodes(self.raft.nodes());
        cs
    }

    // Step advances the state machine using the given message.
    pub fn step(&mut self, m: Message) -> Result<()> {
        // ignore unexpected local messages receiving over network
        if is_local_msg(m.get_msg_type()) {
            return Err(Error::StepLocalMsg);
        }
        if self.raft.prs.contains_key(&m.get_from()) || !is_response_msg(m.get_msg_type()) {
            return self.raft.step(m);
        }
        Err(Error::StepPeerNotFound)
    }

    // Ready returns the current point-in-time state of this RawNode.
    pub fn ready(&mut self) -> Ready {
        Ready::new(&mut self.raft, &self.prev_ss, &self.prev_hs)
    }

    // HasReady called when RawNode user need to check if any Ready pending.
    // Checking logic in this method should be consistent with Ready.containsUpdates().
    pub fn has_ready(&self) -> bool {
        let raft = &self.raft;
        if raft.soft_state() != self.prev_ss {
            return true;
        }
        let hs = raft.hard_state();
        if hs != HardState::new() && hs != self.prev_hs {
            return true;
        }
        if self.get_snap().map_or(false, |s| !is_empty_snap(s)) {
            return true;
        }
        if !raft.msgs.is_empty() || raft.raft_log.unstable_entries().is_some() ||
           raft.raft_log.has_next_entries() {
            return true;
        }
        if !raft.read_states.is_empty() {
            return true;
        }
        false
    }

    #[inline]
    pub fn get_snap(&self) -> Option<&Snapshot> {
        self.raft.get_snap()
    }

    // Advance notifies the RawNode that the application has applied and saved progress in the
    // last Ready results.
    pub fn advance(&mut self, rd: Ready) {
        self.commit_ready(rd);
    }

    // Status returns the current status of the given group.
    pub fn status(&self) -> Status {
        Status::new(&self.raft)
    }

    // ReportUnreachable reports the given node is not reachable for the last send.
    pub fn report_unreachable(&mut self, id: u64) {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgUnreachable);
        m.set_from(id);
        // we don't care if it is ok actually
        self.raft.step(m).is_ok();
    }

    // ReportSnapshot reports the status of the sent snapshot.
    pub fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let rej = status == SnapshotStatus::Failure;
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgSnapStatus);
        m.set_from(id);
        m.set_reject(rej);
        // we don't care if it is ok actually
        self.raft.step(m).is_ok();
    }

    // TransferLeader tries to transfer leadership to the given transferee.
    pub fn transfer_leader(&mut self, transferee: u64) {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgTransferLeader);
        m.set_from(transferee);
        self.raft.step(m).is_ok();
    }

    // ReadIndex requests a read state. The read state will be set in ready.
    // Read State has a read index. Once the application advances further than the read
    // index, any linearizable read requests issued before the read request can be
    // processed safely. The read state will have the same rctx attched.
    pub fn read_index(&mut self, rctx: Vec<u8>) {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut e = Entry::new();
        e.set_data(rctx);
        m.set_entries(RepeatedField::from_vec(vec![e]));
        self.raft.step(m).is_ok();
    }

    #[inline]
    pub fn get_store(&self) -> &T {
        self.raft.get_store()
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        self.raft.mut_store()
    }
}

#[cfg(test)]
mod test {
    use kvproto::eraftpb::MessageType;
    use super::is_local_msg;

    #[test]
    fn test_is_local_msg() {
        let tests = vec![(MessageType::MsgHup, true),
                         (MessageType::MsgBeat, true),
                         (MessageType::MsgUnreachable, true),
                         (MessageType::MsgSnapStatus, true),
                         (MessageType::MsgCheckQuorum, true),
                         (MessageType::MsgPropose, false),
                         (MessageType::MsgAppend, false),
                         (MessageType::MsgAppendResponse, false),
                         (MessageType::MsgRequestVote, false),
                         (MessageType::MsgRequestVoteResponse, false),
                         (MessageType::MsgSnapshot, false),
                         (MessageType::MsgHeartbeat, false),
                         (MessageType::MsgHeartbeatResponse, false),
                         (MessageType::MsgTransferLeader, false),
                         (MessageType::MsgTimeoutNow, false),
                         (MessageType::MsgReadIndex, false),
                         (MessageType::MsgReadIndexResp, false),
                         (MessageType::MsgRequestPreVote, false),
                         (MessageType::MsgRequestPreVoteResponse, false)];
        for (msg_type, result) in tests {
            assert_eq!(is_local_msg(msg_type), result);
        }
    }
}
