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

// Copyright 2015 CoreOS, Inc.
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

use std::cell::RefCell;
use std::rc::Rc;
use kvproto::eraftpb::*;
use protobuf::{self, ProtobufEnum};
use tikv::raft::*;
use tikv::raft::storage::MemStorage;
use super::test_raft::*;
use super::test_raft_paper::*;

fn new_peer(id: u64) -> Peer {
    Peer { id: id, ..Default::default() }
}

fn entry(t: EntryType, term: u64, i: u64, data: Option<Vec<u8>>) -> Entry {
    let mut e = Entry::new();
    e.set_index(i);
    e.set_term(term);
    if let Some(d) = data {
        e.set_data(d);
    }
    e.set_entry_type(t);
    e
}

fn conf_change(t: ConfChangeType, node_id: u64) -> ConfChange {
    let mut cc = ConfChange::new();
    cc.set_change_type(t);
    cc.set_node_id(node_id);
    cc
}

fn new_ready(ss: Option<SoftState>,
             hs: Option<HardState>,
             entries: Vec<Entry>,
             committed_entries: Vec<Entry>)
             -> Ready {
    Ready {
        ss: ss,
        hs: hs,
        entries: entries,
        committed_entries: Some(committed_entries),
        ..Default::default()
    }
}

fn new_raw_node(id: u64,
                peers: Vec<u64>,
                election: usize,
                heartbeat: usize,
                storage: MemStorage,
                peer_nodes: Vec<Peer>)
                -> RawNode<MemStorage> {
    RawNode::new(&new_test_config(id, peers, election, heartbeat),
                 storage,
                 &peer_nodes)
        .unwrap()
}

// test_raw_node_step ensures that RawNode.Step ignore local message.
#[test]
fn test_raw_node_step() {
    for msg_t in MessageType::values() {
        let mut raw_node = new_raw_node(1, vec![], 10, 1, new_storage(), vec![new_peer(1)]);
        let res = raw_node.step(new_message(0, 0, *msg_t, 0));
        // local msg should be ignored.
        if vec![MessageType::MsgBeat,
                MessageType::MsgHup,
                MessageType::MsgUnreachable,
                MessageType::MsgSnapStatus]
            .contains(msg_t) {
            assert_eq!(res, Err(Error::StepLocalMsg));
        }
    }
}


// test_raw_node_read_index_to_old_leader ensures that MsgReadIndex to old leader gets
// forward to the new leader and 'send' method does not attach its term
#[test]
fn test_raw_node_read_index_to_old_leader() {
    let r1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let r2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let r3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(r1), Some(r2), Some(r3)]);

    // elect r1 as leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    let mut test_entries = Entry::new();
    test_entries.set_data(b"testdata".to_vec());

    // send readindex request to r2(follower)
    let _ =
        nt.peers.get_mut(&2).unwrap().step(new_message_with_entries(2,
                                                                    2,
                                                                    MessageType::MsgReadIndex,
                                                                    vec![test_entries.clone()]));

    // verify r2(follower) forwards this message to r1(leader) with term not set
    assert_eq!(nt.peers[&2].msgs.len(), 1);
    let read_index_msg1 =
        new_message_with_entries(2, 1, MessageType::MsgReadIndex, vec![test_entries.clone()]);
    assert_eq!(read_index_msg1, nt.peers[&2].msgs[0]);

    // send readindex request to r3(follower)
    let _ =
        nt.peers.get_mut(&3).unwrap().step(new_message_with_entries(3,
                                                                    3,
                                                                    MessageType::MsgReadIndex,
                                                                    vec![test_entries.clone()]));

    // verify r3(follower) forwards this message to r1(leader) with term not set as well.
    assert_eq!(nt.peers[&3].msgs.len(), 1);

    let read_index_msg2 =
        new_message_with_entries(3, 1, MessageType::MsgReadIndex, vec![test_entries.clone()]);
    assert_eq!(nt.peers[&3].msgs[0], read_index_msg2);

    // now elect r3 as leader
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // let r1 steps the two messages previously we got from r2, r3
    let _ = nt.peers.get_mut(&1).unwrap().step(read_index_msg1);
    let _ = nt.peers.get_mut(&1).unwrap().step(read_index_msg2);

    // verify r1(follower) forwards these messages again to r3(new leader)
    assert_eq!(nt.peers[&1].msgs.len(), 2);

    let read_index_msg3 =
        new_message_with_entries(1, 3, MessageType::MsgReadIndex, vec![test_entries.clone()]);

    assert_eq!(nt.peers[&1].msgs[0], read_index_msg3);
    assert_eq!(nt.peers[&1].msgs[1], read_index_msg3);
}

// test_raw_node_propose_and_conf_change ensures that RawNode.propose and
// RawNode.propose_conf_change send the given proposal and ConfChange to the underlying raft.
#[test]
fn test_raw_node_propose_and_conf_change() {
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![], 10, 1, s.clone(), vec![new_peer(1)]);
    let rd = raw_node.ready();
    s.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);
    raw_node.campaign().expect("");
    let mut proposed = false;
    let mut last_index;
    let mut ccdata = vec![];
    loop {
        let rd = raw_node.ready();
        s.wl().append(&rd.entries).expect("");
        // Once we are the leader, propose a command and a ConfChange.
        if !proposed && rd.ss.is_some() && rd.ss.as_ref().unwrap().leader_id == raw_node.raft.id {
            raw_node.propose(b"somedata".to_vec()).expect("");

            let cc = conf_change(ConfChangeType::AddNode, 1);
            ccdata = protobuf::Message::write_to_bytes(&cc).unwrap();
            raw_node.propose_conf_change(cc).expect("");

            proposed = true;
        }
        raw_node.advance(rd);

        // Exit when we have four entries: one ConfChange, one no-op for the election,
        // our proposed command and proposed ConfChange.
        last_index = s.last_index().unwrap();
        if last_index >= 4 {
            break;
        }
    }

    let entries = s.entries(last_index - 1, last_index + 1, NO_LIMIT).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].get_data(), "somedata".as_bytes());
    assert_eq!(entries[1].get_entry_type(), EntryType::EntryConfChange);
    assert_eq!(entries[1].get_data(), &*ccdata);
}

// test_raw_node_propose_add_duplicate_node ensures that two proposes to add the same node should
// not affect the later propose to add new node.
#[test]
fn test_raw_node_propose_add_duplicate_node() {
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![], 10, 1, s.clone(), vec![new_peer(1)]);
    let rd = raw_node.ready();
    s.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);

    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(&rd.entries).expect("");
        if rd.ss.is_some() && rd.ss.as_ref().unwrap().leader_id == raw_node.raft.id {
            raw_node.advance(rd);
            break;
        }
        raw_node.advance(rd);
    }

    let mut propose_conf_change_and_apply = |cc| {
        raw_node.propose_conf_change(cc).expect("");
        let rd = raw_node.ready();
        s.wl().append(&rd.entries).expect("");
        for e in rd.committed_entries.as_ref().unwrap() {
            if e.get_entry_type() == EntryType::EntryConfChange {
                let conf_change = protobuf::parse_from_bytes(e.get_data()).unwrap();
                raw_node.apply_conf_change(&conf_change);
            }
        }
        raw_node.advance(rd);
    };

    let cc1 = conf_change(ConfChangeType::AddNode, 1);
    let ccdata1 = protobuf::Message::write_to_bytes(&cc1).unwrap();
    propose_conf_change_and_apply(cc1.clone());

    // try to add the same node again
    propose_conf_change_and_apply(cc1);

    // the new node join should be ok
    let cc2 = conf_change(ConfChangeType::AddNode, 2);
    let ccdata2 = protobuf::Message::write_to_bytes(&cc2).unwrap();
    propose_conf_change_and_apply(cc2);

    let last_index = s.last_index().unwrap();

    // the last three entries should be: ConfChange cc1, cc1, cc2
    let mut entries = s.entries(last_index - 2, last_index + 1, NO_LIMIT).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].take_data(), ccdata1);
    assert_eq!(entries[2].take_data(), ccdata2);
}

// test_raw_node_read_index ensures that RawNode.read_index sends the MsgReadIndex message
// to the underlying raft. It also ensures that ReadState can be read out.
#[test]
fn test_raw_node_read_index() {
    let a = Rc::new(RefCell::new(Vec::new()));
    let b = a.clone();
    let before_step_state = Box::new(move |m: &Message| {
        b.borrow_mut().push(m.clone());
        true
    });
    let wrequest_ctx = b"somedata".to_vec();
    let wrs = vec![ReadState {
                       index: 1u64,
                       request_ctx: wrequest_ctx.clone(),
                   }];

    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![], 10, 1, s.clone(), vec![new_peer(1)]);
    raw_node.raft.read_states = wrs.clone();
    // ensure the read_states can be read out
    assert!(raw_node.has_ready());
    let rd = raw_node.ready();
    assert_eq!(rd.read_states, wrs);
    s.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);
    // ensure raft.read_states is reset after advance
    assert!(raw_node.raft.read_states.is_empty());

    let wrequest_ctx = b"somedata2".to_vec();
    raw_node.campaign().expect("");
    loop {
        let rd = raw_node.ready();
        s.wl().append(&rd.entries).expect("");
        if rd.ss.as_ref().map_or(false, |ss| ss.leader_id == raw_node.raft.id) {
            raw_node.advance(rd);

            // Once we are the leader, issue a read index request
            raw_node.raft.before_step_state = Some(before_step_state);
            raw_node.read_index(wrequest_ctx.clone());
            break;
        }
        raw_node.advance(rd);
    }
    // ensure that MsgReadIndex message is sent to the underlying raft
    let msgs = a.borrow();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgReadIndex);
    assert_eq!(wrequest_ctx, msgs[0].get_entries()[0].get_data());
}


// test_raw_node_start ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
#[test]
fn test_raw_node_start() {
    let cc = conf_change(ConfChangeType::AddNode, 1);
    let ccdata = protobuf::Message::write_to_bytes(&cc).unwrap();
    let wants =
        vec![new_ready(None,
                       Some(hard_state(1, 1, 0)),
                       vec![entry(EntryType::EntryConfChange, 1, 1, Some(ccdata.clone()))],
                       vec![entry(EntryType::EntryConfChange, 1, 1, Some(ccdata.clone()))]),
             new_ready(None,
                       Some(hard_state(2, 3, 1)),
                       vec![new_entry(2, 3, Some("foo"))],
                       vec![new_entry(2, 3, Some("foo"))])];

    let store = new_storage();
    let mut raw_node = new_raw_node(1, vec![], 10, 1, store.clone(), vec![new_peer(1)]);
    let rd = raw_node.ready();
    info!("rd {:?}", &rd);
    assert_eq!(rd, wants[0]);
    store.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);

    let rd = raw_node.ready();
    store.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);

    raw_node.campaign().expect("");
    let rd = raw_node.ready();
    store.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);

    raw_node.propose(b"foo".to_vec()).expect("");
    let rd = raw_node.ready();
    assert_eq!(rd, wants[1]);
    store.wl().append(&rd.entries).expect("");
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart() {
    let entries = vec![empty_entry(1, 1), new_entry(1, 2, Some("foo"))];
    let st = hard_state(1, 1, 0);

    let want = new_ready(None, None, vec![], entries[..1].to_vec());

    let store = new_storage();
    store.wl().set_hardstate(st);
    store.wl().append(&entries).expect("");
    let mut raw_node = new_raw_node(1, vec![], 10, 1, store, vec![]);
    let rd = raw_node.ready();
    assert_eq!(rd, want);
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}

#[test]
fn test_raw_node_restart_from_snapshot() {
    let snap = new_snapshot(2, 1, vec![1, 2]);
    let entries = vec![new_entry(1, 3, Some("foo"))];
    let st = hard_state(1, 3, 0);

    let want = new_ready(None, None, vec![], entries.clone());

    let s = new_storage();
    s.wl().set_hardstate(st);
    s.wl().apply_snapshot(snap).expect("");
    s.wl().append(&entries).expect("");
    let mut raw_node = new_raw_node(1, vec![], 10, 1, s, vec![]);
    let rd = raw_node.ready();
    assert_eq!(rd, want);
    raw_node.advance(rd);
    assert!(!raw_node.has_ready());
}
