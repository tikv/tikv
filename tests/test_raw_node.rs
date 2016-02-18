use tikv::proto::raftpb::*;
use protobuf::{self, ProtobufEnum};
use tikv::raft::*;
use tikv::raft::storage::MemStorage;
use std::sync::Arc;
use test_raft::*;
use test_raft_paper::*;

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

fn soft_state(lead: u64, state: StateRole) -> SoftState {
    SoftState {
        lead: lead,
        raft_state: state,
    }
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
        committed_entries: committed_entries,
        ..Default::default()
    }
}

fn new_raw_node(id: u64,
                peers: Vec<u64>,
                election: usize,
                heartbeat: usize,
                storage: Arc<MemStorage>,
                peer_nodes: Vec<Peer>)
                -> RawNode<MemStorage> {
    RawNode::new(&new_test_config(id, peers, election, heartbeat, storage),
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

// test_raw_node_propose_and_conf_change ensures that RawNode.propose and
// RawNode.propose_conf_change send the given proposal and ConfChange to the underlying raft.
#[test]
fn test_raw_node_propose_and_conf_change() {
    let s = new_storage();
    let mut raw_node = new_raw_node(1, vec![], 10, 1, s.clone(), vec![new_peer(1)]);
    raw_node.campaign().expect("");
    let mut proposed = false;
    let mut last_index;
    let mut ccdata = vec![];
    loop {
        let rd = raw_node.ready();
        s.wl().append(&rd.entries).expect("");
        // Once we are the leader, propose a command and a ConfChange.
        if !proposed && rd.ss.is_some() && rd.ss.as_ref().unwrap().lead == raw_node.raft.id {
            raw_node.propose(b"somedata".to_vec()).expect("");

            let cc = conf_change(ConfChangeType::ConfChangeAddNode, 1);
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

// test_raw_node_start ensures that a node can be started correctly. The node should
// start with correct configuration change entries, and can accept and commit
// proposals.
#[test]
fn test_raw_node_start() {
    let cc = conf_change(ConfChangeType::ConfChangeAddNode, 1);
    let ccdata = protobuf::Message::write_to_bytes(&cc).unwrap();
    let wants = vec![new_ready(Some(soft_state(1, StateRole::Leader)),
                               Some(hard_state(2, 2, 1)),
                               vec![
                entry(EntryType::EntryConfChange, 1, 1, Some(ccdata.clone())),
                empty_entry(2, 2),
            ],
                               vec![
                entry(EntryType::EntryConfChange, 1, 1, Some(ccdata.clone())),
                empty_entry(2, 2),
            ]),
                     new_ready(None,
                               Some(hard_state(2, 3, 1)),
                               vec![new_entry(2, 3, Some("foo"))],
                               vec![new_entry(2, 3, Some("foo"))])];

    let store = new_storage();
    let mut raw_node = new_raw_node(1, vec![], 10, 1, store.clone(), vec![new_peer(1)]);
    raw_node.campaign().expect("");
    let rd = raw_node.ready();
    info!("rd {:?}", &rd);
    assert_eq!(rd, wants[0]);
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
    let entries = vec![
        empty_entry(1, 1),
        new_entry(1, 2, Some("foo")),
    ];
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
