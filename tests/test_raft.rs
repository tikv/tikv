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

use tikv::raft::*;
use tikv::raft::storage::MemStorage;
use std::collections::HashMap;
use protobuf::{self, RepeatedField};
use std::ops::Deref;
use std::ops::DerefMut;
use std::cmp;
use kvproto::eraftpb::{Entry, Message, MessageType, HardState, Snapshot, ConfState, EntryType,
                       ConfChange, ConfChangeType};
use rand;

pub fn ltoa(raft_log: &RaftLog<MemStorage>) -> String {
    let mut s = format!("committed: {}\n", raft_log.committed);
    s = s + &format!("applied: {}\n", raft_log.applied);
    for (i, e) in raft_log.all_entries().iter().enumerate() {
        s = s + &format!("#{}: {:?}\n", i, e);
    }
    s
}

pub fn new_storage() -> MemStorage {
    MemStorage::new()
}

fn new_progress(state: ProgressState,
                matched: u64,
                next_idx: u64,
                pending_snapshot: u64,
                ins_size: usize)
                -> Progress {
    Progress {
        state: state,
        matched: matched,
        next_idx: next_idx,
        pending_snapshot: pending_snapshot,
        ins: Inflights::new(ins_size),
        ..Default::default()
    }
}

pub fn new_test_config(id: u64, peers: Vec<u64>, election: usize, heartbeat: usize) -> Config {
    Config {
        id: id,
        peers: peers,
        election_tick: election,
        heartbeat_tick: heartbeat,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

pub fn new_test_raft(id: u64,
                     peers: Vec<u64>,
                     election: usize,
                     heartbeat: usize,
                     storage: MemStorage)
                     -> Interface {
    Interface::new(Raft::new(&new_test_config(id, peers, election, heartbeat), storage))
}

fn read_messages<T: Storage>(raft: &mut Raft<T>) -> Vec<Message> {
    raft.msgs.drain(..).collect()
}

fn ents(terms: Vec<u64>) -> Interface {
    let store = MemStorage::new();
    for (i, term) in terms.iter().enumerate() {
        let mut e = Entry::new();
        e.set_index(i as u64 + 1);
        e.set_term(*term);
        store.wl().append(&[e]).expect("");
    }
    let mut raft = new_test_raft(1, vec![], 5, 1, store);
    raft.reset(terms[terms.len() - 1]);
    raft
}

fn next_ents(r: &mut Raft<MemStorage>, s: &MemStorage) -> Vec<Entry> {
    s.wl().append(r.raft_log.unstable_entries().unwrap_or(&[])).expect("");
    let (last_idx, last_term) = (r.raft_log.last_index(), r.raft_log.last_term());
    r.raft_log.stable_to(last_idx, last_term);
    let ents = r.raft_log.next_entries();
    let committed = r.raft_log.committed;
    r.raft_log.applied_to(committed);
    ents.unwrap_or_else(Vec::new)
}

#[derive(Default, Debug, PartialEq, Eq, Hash)]
struct Connem {
    from: u64,
    to: u64,
}

/// Compare to upstream, we use struct instead of trait here.
/// Because to be able to cast Interface later, we have to make
/// Raft derive Any, which will require a lot of dependencies to derive Any.
/// That's not worthy for just testing purpose.
pub struct Interface {
    raft: Option<Raft<MemStorage>>,
}

impl Interface {
    fn new(r: Raft<MemStorage>) -> Interface {
        Interface { raft: Some(r) }
    }

    pub fn step(&mut self, m: Message) -> Result<()> {
        match self.raft {
            Some(_) => Raft::step(self, m),
            None => Ok(()),
        }
    }

    pub fn read_messages(&mut self) -> Vec<Message> {
        match self.raft {
            Some(_) => self.msgs.drain(..).collect(),
            None => vec![],
        }
    }

    fn initial(&mut self, id: u64, ids: &[u64], pre_vote: bool) {
        if self.raft.is_some() {
            self.id = id;
            self.prs = HashMap::with_capacity(ids.len());
            for id in ids {
                self.prs.insert(*id, Progress { ..Default::default() });
            }
            let term = self.term;
            self.reset(term);
            self.pre_vote = pre_vote;
        }
    }
}

impl Deref for Interface {
    type Target = Raft<MemStorage>;
    fn deref(&self) -> &Raft<MemStorage> {
        self.raft.as_ref().unwrap()
    }
}

impl DerefMut for Interface {
    fn deref_mut(&mut self) -> &mut Raft<MemStorage> {
        self.raft.as_mut().unwrap()
    }
}

pub const NOP_STEPPER: Option<Interface> = Some(Interface { raft: None });

pub const SOME_DATA: Option<&'static str> = Some("somedata");

pub fn new_message_with_entries(from: u64, to: u64, t: MessageType, ents: Vec<Entry>) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(t);
    if !ents.is_empty() {
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

pub fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
    let mut m = new_message_with_entries(from, to, t, vec![]);
    if n > 0 {
        let mut ents = Vec::with_capacity(n);
        for _ in 0..n {
            ents.push(new_entry(0, 0, SOME_DATA));
        }
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

pub fn empty_entry(term: u64, index: u64) -> Entry {
    new_entry(term, index, None)
}

pub fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
    let mut e = Entry::new();
    e.set_index(index);
    e.set_term(term);
    if let Some(d) = data {
        e.set_data(d.as_bytes().to_vec());
    }
    e
}

fn new_raft_log(ents: Vec<Entry>, offset: u64, committed: u64) -> RaftLog<MemStorage> {
    let store = MemStorage::new();
    store.wl().append(&ents).expect("");
    RaftLog {
        store: store,
        unstable: Unstable { offset: offset, ..Default::default() },
        committed: committed,
        ..Default::default()
    }
}

fn new_raft_log_with_storage(s: MemStorage) -> RaftLog<MemStorage> {
    RaftLog::new(s, String::from(""))
}

pub fn new_snapshot(index: u64, term: u64, nodes: Vec<u64>) -> Snapshot {
    let mut s = Snapshot::new();
    s.mut_metadata().set_index(index);
    s.mut_metadata().set_term(term);
    s.mut_metadata().mut_conf_state().set_nodes(nodes);
    s
}

#[derive(Default)]
pub struct Network {
    pub peers: HashMap<u64, Interface>,
    storage: HashMap<u64, MemStorage>,
    dropm: HashMap<Connem, f64>,
    ignorem: HashMap<MessageType, bool>,
}

impl Network {
    // initializes a network from peers.
    // A nil node will be replaced with a new *stateMachine.
    // A *stateMachine will get its k, id.
    // When using stateMachine, the address list is always [1, n].
    pub fn new(peers: Vec<Option<Interface>>) -> Network {
        Network::new_with_config(peers, false)
    }

    // new_with_config is like new but sets the configuration pre_vote explicitly
    // for any state machines it creates.
    pub fn new_with_config(mut peers: Vec<Option<Interface>>, pre_vote: bool) -> Network {
        let size = peers.len();
        let peer_addrs: Vec<u64> = (1..size as u64 + 1).collect();
        let mut nstorage = HashMap::new();
        let mut npeers = HashMap::new();
        for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
            match p {
                None => {
                    nstorage.insert(id, new_storage());
                    let mut r = new_test_raft(id, peer_addrs.clone(), 10, 1, nstorage[&id].clone());
                    r.pre_vote = pre_vote;
                    npeers.insert(id, r);
                }
                Some(mut p) => {
                    p.initial(id, &peer_addrs, pre_vote);
                    npeers.insert(id, p);
                }
            }
        }
        Network {
            peers: npeers,
            storage: nstorage,
            ..Default::default()
        }
    }

    fn ignore(&mut self, t: MessageType) {
        self.ignorem.insert(t, true);
    }

    fn filter(&self, mut msgs: Vec<Message>) -> Vec<Message> {
        msgs.drain(..)
            .filter(|m| {
                if self.ignorem.get(&m.get_msg_type()).cloned().unwrap_or(false) {
                    return false;
                }
                // hups never go over the network, so don't drop them but panic
                assert!(m.get_msg_type() != MessageType::MsgHup, "unexpected msgHup");
                let perc = self.dropm
                    .get(&Connem {
                        from: m.get_from(),
                        to: m.get_to(),
                    })
                    .cloned()
                    .unwrap_or(0f64);
                rand::random::<f64>() >= perc
            })
            .collect()
    }

    pub fn send(&mut self, msgs: Vec<Message>) {
        let mut msgs = msgs;
        while !msgs.is_empty() {
            let mut new_msgs = vec![];
            for m in msgs.drain(..) {
                let resp = {
                    let p = self.peers.get_mut(&m.get_to()).unwrap();
                    p.step(m).expect("");
                    p.read_messages()
                };
                new_msgs.append(&mut self.filter(resp));
            }
            msgs.append(&mut new_msgs);
        }
    }

    fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert(Connem {
                              from: from,
                              to: to,
                          },
                          perc);
    }

    fn cut(&mut self, one: u64, other: u64) {
        self.drop(one, other, 1f64);
        self.drop(other, one, 1f64);
    }

    fn isolate(&mut self, id: u64) {
        for i in 0..self.peers.len() as u64 {
            let nid = i + 1;
            if nid != id {
                self.drop(id, nid, 1.0);
                self.drop(nid, id, 1.0);
            }
        }
    }

    fn recover(&mut self) {
        self.dropm = HashMap::new();
        self.ignorem = HashMap::new();
    }
}

#[test]
fn test_progress_become_probe() {
    let matched = 1u64;
    let mut tests = vec![
        (new_progress(ProgressState::Replicate, matched, 5, 0, 256), 2),
        // snapshot finish
        (new_progress(ProgressState::Snapshot, matched, 5, 10, 256), 11),
        // snapshot failure
        (new_progress(ProgressState::Snapshot, matched, 5, 0, 256), 2),
    ];
    for (i, &mut (ref mut p, wnext)) in tests.iter_mut().enumerate() {
        p.become_probe();
        if p.state != ProgressState::Probe {
            panic!("#{}: state = {:?}, want {:?}",
                   i,
                   p.state,
                   ProgressState::Probe);
        }
        if p.matched != matched {
            panic!("#{}: match = {:?}, want {:?}", i, p.matched, matched);
        }
        if p.next_idx != wnext {
            panic!("#{}: next = {}, want {}", i, p.next_idx, wnext);
        }
    }
}

#[test]
fn test_progress_become_replicate() {
    let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
    p.become_replicate();

    assert_eq!(p.state, ProgressState::Replicate);
    assert_eq!(p.matched, 1);
    assert_eq!(p.matched + 1, p.next_idx);
}

#[test]
fn test_progress_become_snapshot() {
    let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
    p.become_snapshot(10);
    assert_eq!(p.state, ProgressState::Snapshot);
    assert_eq!(p.matched, 1);
    assert_eq!(p.pending_snapshot, 10);
}

#[test]
fn test_progress_update() {
    let (prev_m, prev_n) = (3u64, 5u64);
    let tests = vec![
        (prev_m - 1, prev_m, prev_n, false),
        (prev_m, prev_m, prev_n, false),
        (prev_m + 1, prev_m + 1, prev_n, true),
        (prev_m + 2, prev_m + 2, prev_n + 1, true),
    ];
    for (i, &(update, wm, wn, wok)) in tests.iter().enumerate() {
        let mut p = Progress {
            matched: prev_m,
            next_idx: prev_n,
            ..Default::default()
        };
        let ok = p.maybe_update(update);
        if ok != wok {
            panic!("#{}: ok= {}, want {}", i, ok, wok);
        }
        if p.matched != wm {
            panic!("#{}: match= {}, want {}", i, p.matched, wm);
        }
        if p.next_idx != wn {
            panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
        }
    }
}

#[test]
fn test_progress_maybe_decr() {
    let tests = vec![
        // state replicate and rejected is not greater than match
        (ProgressState::Replicate, 5, 10, 5, 5, false, 10),
        // state replicate and rejected is not greater than match
        (ProgressState::Replicate, 5, 10, 4, 4, false, 10),
        // state replicate and rejected is greater than match
        // directly decrease to match+1
        (ProgressState::Replicate, 5, 10, 9, 9, true, 6),
        // next-1 != rejected is always false
        (ProgressState::Probe, 0, 0, 0, 0, false, 0),
        // next-1 != rejected is always false
        (ProgressState::Probe, 0, 10, 5, 5, false, 10),
        // next>1 = decremented by 1
        (ProgressState::Probe, 0, 10, 9, 9, true, 9),
        // next>1 = decremented by 1
        (ProgressState::Probe, 0, 2, 1, 1, true, 1),
        // next<=1 = reset to 1
        (ProgressState::Probe, 0, 1, 0, 0, true, 1),
        // decrease to min(rejected, last+1)
        (ProgressState::Probe, 0, 10, 9, 2, true, 3),
        // rejected < 1, reset to 1
        (ProgressState::Probe, 0, 10, 9, 0, true, 1),
    ];
    for (i, &(state, m, n, rejected, last, w, wn)) in tests.iter().enumerate() {
        let mut p = new_progress(state, m, n, 0, 0);
        if p.maybe_decr_to(rejected, last) != w {
            panic!("#{}: maybeDecrTo= {}, want {}", i, !w, w);
        }
        if p.matched != m {
            panic!("#{}: match= {}, want {}", i, p.matched, m);
        }
        if p.next_idx != wn {
            panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
        }
    }
}

#[test]
fn test_progress_is_paused() {
    let tests = vec![
        (ProgressState::Probe, false, false),
        (ProgressState::Probe, true, true),
        (ProgressState::Replicate, false, false),
        (ProgressState::Replicate, true, false),
        (ProgressState::Snapshot, false, true),
        (ProgressState::Snapshot, true, true),
    ];
    for (i, &(state, paused, w)) in tests.iter().enumerate() {
        let p = Progress {
            state: state,
            paused: paused,
            ins: Inflights::new(256),
            ..Default::default()
        };
        if p.is_paused() != w {
            panic!("#{}: shouldwait = {}, want {}", i, p.is_paused(), w)
        }
    }
}

// test_progress_resume ensures that progress.maybeUpdate and progress.maybeDecrTo
// will reset progress.paused.
#[test]
fn test_progress_resume() {
    let mut p = Progress {
        next_idx: 2,
        paused: true,
        ..Default::default()
    };
    p.maybe_decr_to(1, 1);
    assert!(!p.paused, "paused= true, want false");
    p.paused = true;
    p.maybe_update(2);
    assert!(!p.paused, "paused= true, want false");
}

// test_progress_resume_by_heartbeat ensures raft.heartbeat reset progress.paused by heartbeat.
#[test]
fn test_progress_resume_by_heartbeat() {
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    raft.become_candidate();
    raft.become_leader();
    raft.prs.get_mut(&2).unwrap().paused = true;
    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgBeat);
    raft.step(m).expect("");
    assert!(!raft.prs[&2].paused, "paused = true, want false");
}

#[test]
fn test_progress_paused() {
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    raft.become_candidate();
    raft.become_leader();
    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgPropose);
    let mut e = Entry::new();
    e.set_data(b"some_data".to_vec());
    m.set_entries(RepeatedField::from_vec(vec![e]));
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    let ms = read_messages(&mut raft);
    assert_eq!(ms.len(), 1);
}

#[test]
fn test_leader_election_no_pre_vote() {
    test_leader_election(false);
}

#[test]
fn test_leader_election_pre_vote() {
    test_leader_election(true);
}

fn test_leader_election(pre_vote: bool) {
    let mut tests = vec![
        (Network::new_with_config(vec![None, None, None], pre_vote), StateRole::Leader, 1),
        (Network::new_with_config(vec![None, None, NOP_STEPPER], pre_vote), StateRole::Leader, 1),
        (Network::new_with_config(vec![None,
                                       NOP_STEPPER,
                                       NOP_STEPPER], pre_vote), StateRole::Candidate, 1),
        (Network::new_with_config(vec![None,
                                       NOP_STEPPER,
                                       NOP_STEPPER,
                                       None], pre_vote), StateRole::Candidate, 1),
        (Network::new_with_config(vec![None,
                                       NOP_STEPPER,
                                       NOP_STEPPER,
                                       None,
                                       None], pre_vote), StateRole::Leader, 1),

        // three logs futher along than 0, but in the same term so rejection
        // are returned instead of the votes being ignored.
        (Network::new_with_config(vec![None,
                                       Some(ents(vec![1])),
                                       Some(ents(vec![1])),
                                       Some(ents(vec![1,1])),
                                       None], pre_vote), StateRole::Follower, 1),

        // logs converge
        (Network::new_with_config(vec![Some(ents(vec![1])),
                                       None,
                                       Some(ents(vec![2])),
                                       Some(ents(vec![1])),
                                       None], pre_vote), StateRole::Leader, 2),
    ];

    for (i, &mut (ref mut network, state, term)) in tests.iter_mut().enumerate() {
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgHup);
        network.send(vec![m]);
        let raft = network.peers.get(&1).unwrap();
        let (exp_state, exp_term) = if state == StateRole::Candidate && pre_vote {
            // In pre-vote mode, an election that fails to complete
            // leaves the node in pre-candidate state without advancing
            // the term.
            (StateRole::PreCandidate, 0)
        } else {
            (state, term)
        };
        if raft.state != exp_state {
            panic!("#{}: state = {:?}, want {:?}", i, raft.state, exp_state);
        }
        if raft.term != exp_term {
            panic!("#{}: term = {}, want {}", i, raft.term, exp_term)
        }
    }
}

#[test]
fn test_leader_cycle_no_pre_vote() {
    test_leader_cycle(false)
}

#[test]
fn test_leader_cycle_pre_vote() {
    test_leader_cycle(true)
}

// test_leader_cycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean state (as they do in
// test_leader_election)
fn test_leader_cycle(pre_vote: bool) {
    let mut network = Network::new_with_config(vec![None, None, None], pre_vote);
    for campaigner_id in 1..4 {
        network.send(vec![new_message(campaigner_id, campaigner_id, MessageType::MsgHup, 0)]);

        for sm in network.peers.values() {
            if sm.id == campaigner_id && sm.state != StateRole::Leader {
                panic!("preVote={}: campaigning node {} state = {:?}, want Leader",
                       pre_vote,
                       sm.id,
                       sm.state);
            } else if sm.id != campaigner_id && sm.state != StateRole::Follower {
                panic!("preVote={}: after campaign of node {}, node {} had state = {:?}, want \
                        Follower",
                       pre_vote,
                       campaigner_id,
                       sm.id,
                       sm.state);
            }
        }
    }
}

#[test]
fn test_vote_from_any_state() {
    test_vote_from_any_state_for_type(MessageType::MsgRequestVote);
}

#[test]
fn test_prevote_from_any_state() {
    test_vote_from_any_state_for_type(MessageType::MsgRequestPreVote);
}

fn test_vote_from_any_state_for_type(vt: MessageType) {
    let all_states =
        vec![StateRole::Follower, StateRole::Candidate, StateRole::PreCandidate, StateRole::Leader];
    for state in all_states {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        r.term = 1;
        match state {
            StateRole::Follower => {
                let term = r.term;
                r.become_follower(term, 3);
            }
            StateRole::PreCandidate => r.become_pre_candidate(),
            StateRole::Candidate => r.become_candidate(),
            StateRole::Leader => {
                r.become_candidate();
                r.become_leader();
            }
        }
        // Note that setting our state above may have advanced r.term
        // past its initial value.
        let orig_term = r.term;
        let new_term = r.term + 1;

        let mut msg = new_message(2, 1, vt, 0);
        msg.set_term(new_term);
        msg.set_log_term(new_term);
        msg.set_index(42);
        r.step(msg).expect(&format!("{:?},{:?}: step failed", vt, state));
        assert_eq!(r.msgs.len(),
                   1,
                   "{:?},{:?}: {} response messages, want 1: {:?}",
                   vt,
                   state,
                   r.msgs.len(),
                   r.msgs);
        let resp = &r.msgs[0];
        assert_eq!(resp.get_msg_type(),
                   vote_resp_msg_type(vt),
                   "{:?},{:?}: response message is {:?}, want {:?}",
                   vt,
                   state,
                   resp.get_msg_type(),
                   vote_resp_msg_type(vt));
        assert!(!resp.get_reject(),
                "{:?},{:?}: unexpected rejection",
                vt,
                state);

        // If this was a real vote, we reset our state and term.
        if vt == MessageType::MsgRequestVote {
            assert_eq!(r.state,
                       StateRole::Follower,
                       "{:?},{:?}, state {:?}, want {:?}",
                       vt,
                       state,
                       r.state,
                       StateRole::Follower);
            assert_eq!(r.term,
                       new_term,
                       "{:?},{:?}, term {}, want {}",
                       vt,
                       state,
                       r.term,
                       new_term);
            assert_eq!(r.vote, 2, "{:?},{:?}, vote {}, want 2", vt, state, r.vote);
        } else {
            // In a pre-vote, nothing changes.
            assert_eq!(r.state,
                       state,
                       "{:?},{:?}, state {:?}, want {:?}",
                       vt,
                       state,
                       r.state,
                       state);
            assert_eq!(r.term,
                       orig_term,
                       "{:?},{:?}, term {}, want {}",
                       vt,
                       state,
                       r.term,
                       orig_term);
            // If state == Follower or PreCandidate, r hasn't voted yet.
            // In Candidate or Leader, it's voted for itself.
            assert!(r.vote == INVALID_ID || r.vote == 1,
                    "{:?},{:?}, vote {}, want {:?} or 1",
                    vt,
                    state,
                    r.vote,
                    INVALID_ID);
        }
    }
}

#[test]
fn test_log_replicatioin() {
    let mut tests = vec![
        (Network::new(vec![None, None, None]),
            vec![new_message(1, 1, MessageType::MsgPropose, 1)],
            2),

        (Network::new(vec![None, None, None]),
            vec![new_message(1, 1, MessageType::MsgPropose, 1),
                new_message(1, 2, MessageType::MsgHup, 0),
                new_message(1, 2, MessageType::MsgPropose, 1)],
            4),
    ];

    for (i, &mut (ref mut network, ref msgs, wcommitted)) in tests.iter_mut().enumerate() {
        network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        for m in msgs {
            network.send(vec![m.clone()]);
        }

        for (j, x) in &mut network.peers {
            if x.raft_log.committed != wcommitted {
                panic!("#{}.{}: committed = {}, want {}",
                       i,
                       j,
                       x.raft_log.committed,
                       wcommitted);
            }

            let mut ents = next_ents(x, &network.storage[j]);
            let ents: Vec<Entry> = ents.drain(..).filter(|e| e.has_data()).collect();
            for (k, m) in msgs.iter()
                .filter(|m| m.get_msg_type() == MessageType::MsgPropose)
                .enumerate() {
                if ents[k].get_data() != m.get_entries()[0].get_data() {
                    panic!("#{}.{}: data = {:?}, want {:?}",
                           i,
                           j,
                           ents[k].get_data(),
                           m.get_entries()[0].get_data());
                }
            }
        }
    }
}

#[test]
fn test_single_node_commit() {
    let mut tt = Network::new(vec![None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 3);
}

// test_cannot_commit_without_new_term_entry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
#[test]
fn test_cannot_commit_without_new_term_entry() {
    let mut tt = Network::new(vec![None, None, None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 0 cannot reach 2, 3, 4
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 1);

    // network recovery
    tt.recover();
    // avoid committing ChangeTerm proposal
    tt.ignore(MessageType::MsgAppend);

    // elect 2 as the new leader with term 2
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // no log entries from previous term should be committed
    assert_eq!(tt.peers[&2].raft_log.committed, 1);

    tt.recover();
    // send heartbeat; reset wait
    tt.send(vec![new_message(2, 2, MessageType::MsgBeat, 0)]);
    // append an entry at current term
    tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
    // expect the committed to be advanced
    assert_eq!(tt.peers[&2].raft_log.committed, 5);
}

// test_commit_without_new_term_entry tests the entries could be committed
// when leader changes, no new proposal comes in.
#[test]
fn test_commit_without_new_term_entry() {
    let mut tt = Network::new(vec![None, None, None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 0 cannot reach 2, 3, 4
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 1);

    // network recovery
    tt.recover();

    // elect 1 as the new leader with term 2
    // after append a ChangeTerm entry from the current term, all entries
    // should be committed
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 4);
}

#[test]
fn test_dueling_candidates() {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);
    nt.cut(1, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    nt.recover();

    // Candidate 3 now increases its term and tries to vote again, we except it to
    // disrupt the leader 1 since it has a higher term, 3 will be follower again
    // since both 1 and 2 rejects its vote request since 3 does not have a long
    // enough log.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    let wlog = new_raft_log(vec![empty_entry(1, 1)], 2, 1);
    let wlog2 = new_raft_log_with_storage(new_storage());
    let tests = vec![
        (StateRole::Follower, 2, &wlog),
        (StateRole::Follower, 2, &wlog),
        (StateRole::Follower, 2, &wlog2),
    ];

    for (i, &(state, term, raft_log)) in tests.iter().enumerate() {
        let id = i as u64 + 1;
        if nt.peers[&id].state != state {
            panic!("#{}: state = {:?}, want {:?}",
                   i,
                   nt.peers[&id].state,
                   state);
        }
        if nt.peers[&id].term != term {
            panic!("#{}: term = {}, want {}", i, nt.peers[&id].term, term);
        }
        let base = ltoa(raft_log);
        let l = ltoa(&nt.peers[&(1 + i as u64)].raft_log);
        if base != l {
            panic!("#{}: raft_log:\n {}, want:\n {}", i, l, base);
        }
    }
}

#[test]
fn test_dueling_pre_candidates() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    a.pre_vote = true;
    b.pre_vote = true;
    c.pre_vote = true;

    let mut nt = Network::new_with_config(vec![Some(a), Some(b), Some(c)], true);
    nt.cut(1, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // 3 campaigns then reverts to follower when its pre_vote is rejected
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.recover();

    // Candidate 3 now increases its term and tries to vote again.
    // With pre-vote, it does not disrupt the leader.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    let wlog = new_raft_log(vec![empty_entry(1, 1)], 2, 1);
    let wlog2 = new_raft_log_with_storage(new_storage());
    let tests = vec![
        (StateRole::Leader, 1, &wlog),
        (StateRole::Follower, 1, &wlog),
        (StateRole::Follower, 1, &wlog2),
    ];
    for (i, &(state, term, raft_log)) in tests.iter().enumerate() {
        let id = i as u64 + 1;
        if nt.peers[&id].state != state {
            panic!("#{}: state = {:?}, want {:?}",
                   i,
                   nt.peers[&id].state,
                   state);
        }
        if nt.peers[&id].term != term {
            panic!("#{}: term = {}, want {}", i, nt.peers[&id].term, term);
        }
        let base = ltoa(raft_log);
        let l = ltoa(&nt.peers[&(1 + i as u64)].raft_log);
        if base != l {
            panic!("#{}: raft_log:\n {}, want:\n {}", i, l, base);
        }
    }
}

#[test]
fn test_candidate_concede() {
    let mut tt = Network::new(vec![None, None, None]);
    tt.isolate(1);

    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // heal the partition
    tt.recover();
    // send heartbeat; reset wait
    tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

    // send a proposal to 3 to flush out a MsgAppend to 1
    let data = "force follower";
    let mut m = new_message(3, 3, MessageType::MsgPropose, 0);
    m.set_entries(RepeatedField::from_vec(vec![new_entry(0, 0, Some(data))]));
    tt.send(vec![m]);
    // send heartbeat; flush out commit
    tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Follower);
    assert_eq!(tt.peers[&1].term, 1);

    let ents = vec![empty_entry(1, 1), new_entry(1, 2, Some(data))];
    let want_log = ltoa(&new_raft_log(ents, 3, 2));
    for (id, p) in &tt.peers {
        let l = ltoa(&p.raft_log);
        if l != want_log {
            panic!("#{}: raft_log: {}, want: {}", id, l, want_log);
        }
    }
}

#[test]
fn test_single_node_candidate() {
    let mut tt = Network::new(vec![None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Leader);
}

#[test]
fn test_sinle_node_pre_candidate() {
    let mut tt = Network::new_with_config(vec![None], true);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Leader);
}

#[test]
fn test_old_messages() {
    let mut tt = Network::new(vec![None, None, None]);
    // make 0 leader @ term 3
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
    let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
    m.set_term(2);
    m.set_entries(RepeatedField::from_vec(vec![empty_entry(2, 3)]));
    tt.send(vec![m]);
    // commit a new entry
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    let ents =
        vec![empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3), new_entry(3, 4, SOME_DATA)];
    let ilog = new_raft_log(ents, 5, 4);
    let base = ltoa(&ilog);
    for (id, p) in &tt.peers {
        let l = ltoa(&p.raft_log);
        if l != base {
            panic!("#{}: raft_log: {}, want: {}", id, l, base);
        }
    }
}

// test_old_messages_reply - optimization - reply with new term.

#[test]
fn test_proposal() {
    let mut tests = vec![
        (Network::new(vec![None, None, None]), true),
        (Network::new(vec![None, None, NOP_STEPPER]), true),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER]), false),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None]), false),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None, None]), true),
    ];

    for (j, (mut nw, success)) in tests.drain(..).enumerate() {
        let send = |nw: &mut Network, m| {
            let res = recover_safe!(|| nw.send(vec![m]));
            assert!(res.is_ok() || !success);
        };

        // promote 0 the leader
        send(&mut nw, new_message(1, 1, MessageType::MsgHup, 0));
        send(&mut nw, new_message(1, 1, MessageType::MsgPropose, 1));

        let want_log = if success {
            new_raft_log(vec![empty_entry(1, 1), new_entry(1, 2, SOME_DATA)], 3, 2)
        } else {
            new_raft_log_with_storage(new_storage())
        };
        let base = ltoa(&want_log);
        for (id, p) in &nw.peers {
            if p.raft.is_some() {
                let l = ltoa(&p.raft_log);
                if l != base {
                    panic!("#{}: raft_log: {}, want {}", id, l, base);
                }
            }
        }
        if nw.peers[&1].term != 1 {
            panic!("#{}: term = {}, want: {}", j, nw.peers[&1].term, 1);
        }
    }
}

#[test]
fn test_proposal_by_proxy() {
    let mut tests = vec![
        Network::new(vec![None, None, None]),
        Network::new(vec![None, None, NOP_STEPPER]),
    ];
    for (j, tt) in tests.iter_mut().enumerate() {
        // promote 0 the leader
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        // propose via follower
        tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);

        let want_log = new_raft_log(vec![empty_entry(1, 1), new_entry(1, 2, SOME_DATA)], 3, 2);
        let base = ltoa(&want_log);
        for (id, p) in &tt.peers {
            if p.raft.is_none() {
                continue;
            }
            let l = ltoa(&p.raft_log);
            if l != base {
                panic!("#{}: raft_log: {}, want: {}", id, l, base);
            }
        }
        if tt.peers[&1].term != 1 {
            panic!("#{}: term = {}, want {}", j, tt.peers[&1].term, 1);
        }
    }
}

#[test]
fn test_commit() {
    let mut tests = vec![
        // single
        (vec![1u64], vec![empty_entry(1, 1)], 1u64, 1u64),
        (vec![1], vec![empty_entry(1, 1)], 2, 0),
        (vec![2], vec![empty_entry(1, 1), empty_entry(2, 2)], 2, 2),
        (vec![1], vec![empty_entry(2, 1)], 2, 1),

        // odd
        (vec![2, 1, 1], vec![empty_entry(1, 1), empty_entry(2, 2)], 1, 1),
        (vec![2, 1, 1], vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 0),
        (vec![2, 1, 2], vec![empty_entry(1, 1), empty_entry(2, 2)], 2, 2),
        (vec![2, 1, 2], vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 0),

        // even
        (vec![2, 1, 1, 1], vec![empty_entry(1, 1), empty_entry(2, 2)], 1, 1),
        (vec![2, 1, 1, 1], vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 0),
        (vec![2, 1, 1, 2], vec![empty_entry(1, 1), empty_entry(2, 2)], 1, 1),
        (vec![2, 1, 1, 2], vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 0),
        (vec![2, 1, 2, 2], vec![empty_entry(1, 1), empty_entry(2, 2)], 2, 2),
        (vec![2, 1, 2, 2], vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 0),
    ];

    for (i, (matches, logs, sm_term, w)) in tests.drain(..).enumerate() {
        let store = MemStorage::new();
        store.wl().append(&logs).expect("");
        let mut hs = HardState::new();
        hs.set_term(sm_term);
        store.wl().set_hardstate(hs);

        let mut sm = new_test_raft(1, vec![1], 5, 1, store);
        for (j, &v) in matches.iter().enumerate() {
            sm.set_progress(j as u64 + 1, v, v + 1);
        }
        sm.maybe_commit();
        if sm.raft_log.committed != w {
            panic!("#{}: committed = {}, want {}", i, sm.raft_log.committed, w);
        }
    }
}

#[test]
fn test_pass_election_timeout() {
    let tests = vec![
        (5, 0f64, false),
        (10, 0.1, true),
        (13, 0.4, true),
        (15, 0.6, true),
        (18, 0.9, true),
        (20, 1.0, false),
    ];

    for (i, &(elapse, wprobability, round)) in tests.iter().enumerate() {
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.election_elapsed = elapse;
        let mut c = 0;
        for _ in 0..10000 {
            sm.reset_randomized_election_timeout();
            if sm.pass_election_timeout() {
                c += 1;
            }
        }
        let mut got = c as f64 / 10000.0;
        if round {
            got = (got * 10.0 + 0.5).floor() / 10.0;
        }
        if (got - wprobability).abs() > 0.000001 {
            panic!("#{}: probability = {}, want {}", i, got, wprobability);
        }
    }
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
#[test]
fn test_step_ignore_old_term_msg() {
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    let panic_before_step_state =
        Box::new(|_: &Message| panic!("before step state function hook called unexpectedly"));
    sm.before_step_state = Some(panic_before_step_state);
    sm.term = 2;
    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(1);
    sm.step(m).expect("");
}

// test_handle_msg_append ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the
//    log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
#[test]
fn test_handle_msg_append() {
    let nm = |term, log_term, index, commit, ents: Option<Vec<(u64, u64)>>| {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgAppend);
        m.set_term(term);
        m.set_log_term(log_term);
        m.set_index(index);
        m.set_commit(commit);
        if let Some(ets) = ents {
            m.set_entries(RepeatedField::from_vec(ets.iter()
                .map(|&(i, t)| empty_entry(t, i))
                .collect()));
        }
        m
    };
    let mut tests = vec![
        // Ensure 1
        (nm(2, 3, 2, 3, None), 2, 0, true), // previous log mismatch
        (nm(2, 3, 3, 3, None), 2, 0, true), // previous log non-exist

        // Ensure 2
        (nm(2, 1, 1, 1, None), 2, 1, false),
        (nm(2, 0, 0, 1, Some(vec![(1, 2)])), 1, 1, false),
        (nm(2, 2, 2, 3, Some(vec![(3, 2), (4, 2)])), 4, 3, false),
        (nm(2, 2, 2, 4, Some(vec![(3, 2)])), 3, 3, false),
        (nm(2, 1, 1, 4, Some(vec![(2, 2)])), 2, 2, false),

        // Ensure 3
        (nm(1, 1, 1, 3, None), 2, 1, false), // match entry 1, commit up to last new entry 1
        (nm(1, 1, 1, 3, Some(vec![(2, 2)])), 2, 2, false), // match entry 1, commit up to last new
                                                           // entry 2
        (nm(2, 2, 2, 3, None), 2, 2, false), // match entry 2, commit up to last new entry 2
        (nm(2, 2, 2, 4, None), 2, 2, false), // commit up to log.last()
    ];

    for (j, (m, w_index, w_commit, w_reject)) in tests.drain(..).enumerate() {
        let store = new_storage();
        store.wl().append(&[empty_entry(1, 1), empty_entry(2, 2)]).expect("");
        let mut sm = new_test_raft(1, vec![1], 10, 1, store);
        sm.become_follower(2, INVALID_ID);

        sm.handle_append_entries(m);
        if sm.raft_log.last_index() != w_index {
            panic!("#{}: last_index = {}, want {}",
                   j,
                   sm.raft_log.last_index(),
                   w_index);
        }
        if sm.raft_log.committed != w_commit {
            panic!("#{}: committed = {}, want {}",
                   j,
                   sm.raft_log.committed,
                   w_commit);
        }
        let m = sm.read_messages();
        if m.len() != 1 {
            panic!("#{}: msg count = {}, want 1", j, m.len());
        }
        if m[0].get_reject() != w_reject {
            panic!("#{}: reject = {}, want {}", j, m[0].get_reject(), w_reject);
        }
    }
}

// test_handle_heartbeat ensures that the follower commits to the commit in the message.
#[test]
fn test_handle_heartbeat() {
    let commit = 2u64;
    let nw = |f, to, term, commit| {
        let mut m = new_message(f, to, MessageType::MsgHeartbeat, 0);
        m.set_term(term);
        m.set_commit(commit);
        m
    };
    let mut tests = vec![
        (nw(2, 1, 2, commit + 1), commit + 1),
        (nw(2, 1, 2, commit - 1), commit), // do not decrease commit
    ];
    for (i, (m, w_commit)) in tests.drain(..).enumerate() {
        let store = new_storage();
        store.wl()
            .append(&[empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)])
            .expect("");
        let mut sm = new_test_raft(1, vec![1, 2], 5, 1, store);
        sm.become_follower(2, 2);
        sm.raft_log.commit_to(commit);
        sm.handle_heartbeat(m);
        if sm.raft_log.committed != w_commit {
            panic!("#{}: committed = {}, want = {}",
                   i,
                   sm.raft_log.committed,
                   w_commit);
        }
        let m = sm.read_messages();
        if m.len() != 1 {
            panic!("#{}: msg count = {}, want 1", i, m.len());
        }
        if m[0].get_msg_type() != MessageType::MsgHeartbeatResponse {
            panic!("#{}: type = {:?}, want MsgHeartbeatResponse",
                   i,
                   m[0].get_msg_type());
        }
    }
}

// test_handle_heartbeat_resp ensures that we re-send log entries when we get a heartbeat response.
#[test]
fn test_handle_heartbeat_resp() {
    let store = new_storage();
    store.wl()
        .append(&[empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)])
        .expect("");
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, store);
    sm.become_candidate();
    sm.become_leader();
    let last_index = sm.raft_log.last_index();
    sm.raft_log.commit_to(last_index);

    // A heartbeat response from a node that is behind; re-send MsgApp
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0)).expect("");
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);

    // A second heartbeat response with no AppResp does not re-send because we are in the wait
    // state.
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0)).expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 0);

    // Send a heartbeat to reset the wait state; next heartbeat will re-send MsgApp.
    sm.bcast_heartbeat();
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0)).expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgHeartbeat);
    assert_eq!(msgs[1].get_msg_type(), MessageType::MsgAppend);

    // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(msgs[1].get_index() + msgs[1].get_entries().len() as u64);
    sm.step(m).expect("");
    // Consume the message sent in response to MsgAppResp
    sm.read_messages();

    sm.bcast_heartbeat(); // reset wait state
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0)).expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgHeartbeat);
}

// test_msg_append_response_wait_reset verifies the waitReset behavior of a leader
// MsgAppResp.
#[test]
fn test_msg_append_response_wait_reset() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage());
    sm.become_candidate();
    sm.become_leader();

    // The new leader has just emitted a new Term 4 entry; consume those messages
    // from the outgoing queue.
    sm.bcast_append();
    sm.read_messages();

    // Node 2 acks the first entry, making it committed.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(1);
    sm.step(m).expect("");
    assert_eq!(sm.raft_log.committed, 1);
    // Also consume the MsgApp messages that update Commit on the followers.
    sm.read_messages();

    // A new command is now proposed on node 1.
    m = new_message(1, 0, MessageType::MsgPropose, 0);
    m.set_entries(RepeatedField::from_vec(vec![empty_entry(0, 0)]));
    sm.step(m).expect("");

    // The command is broadcast to all nodes not in the wait state.
    // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msgs[0].get_to(), 2);
    assert_eq!(msgs[0].get_entries().len(), 1);
    assert_eq!(msgs[0].get_entries()[0].get_index(), 2);

    // Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
    m = new_message(3, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(1);
    sm.step(m).expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msgs[0].get_to(), 3);
    assert_eq!(msgs[0].get_entries().len(), 1);
    assert_eq!(msgs[0].get_entries()[0].get_index(), 2);
}

#[test]
fn test_recv_msg_request_vote() {
    test_recv_msg_request_vote_for_type(MessageType::MsgRequestVote);
}

#[test]
fn test_recv_msg_request_prevote() {
    test_recv_msg_request_vote_for_type(MessageType::MsgRequestPreVote);
}

fn test_recv_msg_request_vote_for_type(msg_type: MessageType) {
    let mut tests = vec![
        (StateRole::Follower, 0, 0, INVALID_ID, true),
        (StateRole::Follower, 0, 1, INVALID_ID, true),
        (StateRole::Follower, 0, 2, INVALID_ID, true),
        (StateRole::Follower, 0, 3, INVALID_ID, false),

        (StateRole::Follower, 1, 0, INVALID_ID, true),
        (StateRole::Follower, 1, 1, INVALID_ID, true),
        (StateRole::Follower, 1, 2, INVALID_ID, true),
        (StateRole::Follower, 1, 3, INVALID_ID, false),

        (StateRole::Follower, 2, 0, INVALID_ID, true),
        (StateRole::Follower, 2, 1, INVALID_ID, true),
        (StateRole::Follower, 2, 2, INVALID_ID, false),
        (StateRole::Follower, 2, 3, INVALID_ID, false),

        (StateRole::Follower, 3, 0, INVALID_ID, true),
        (StateRole::Follower, 3, 1, INVALID_ID, true),
        (StateRole::Follower, 3, 2, INVALID_ID, false),
        (StateRole::Follower, 3, 3, INVALID_ID, false),

        (StateRole::Follower, 3, 2, 2, false),
        (StateRole::Follower, 3, 2, 1, true),

        (StateRole::Leader, 3, 3, 1, true),
        (StateRole::PreCandidate, 3, 3, 1, true),
        (StateRole::Candidate, 3, 3, 1, true),
    ];

    for (j, (state, i, term, vote_for, w_reject)) in tests.drain(..).enumerate() {
        let raft_log = new_raft_log(vec![empty_entry(2, 1), empty_entry(2, 2)], 3, 0);
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
        if msg_type == MessageType::MsgRequestPreVote {
            sm.pre_vote = true;
        }
        sm.state = state;
        sm.vote = vote_for;
        sm.raft_log = raft_log;
        let mut m = new_message(2, 0, msg_type, 0);
        m.set_index(i);
        m.set_log_term(term);
        sm.step(m).expect("");

        let msgs = sm.read_messages();
        if msgs.len() != 1 {
            panic!("#{}: msgs count = {}, want 1", j, msgs.len());
        }
        if msgs[0].get_msg_type() != vote_resp_msg_type(msg_type) {
            panic!("#{}: m.type = {:?}, want {:?}",
                   i,
                   msgs[0].get_msg_type(),
                   vote_resp_msg_type(msg_type));
        }
        if msgs[0].get_reject() != w_reject {
            panic!("#{}: m.get_reject = {}, want {}",
                   j,
                   msgs[0].get_reject(),
                   w_reject);
        }
    }
}

#[test]
fn test_state_transition() {
    let mut tests = vec![
        (StateRole::Follower, StateRole::Follower, true, 1, INVALID_ID),
        (StateRole::Follower, StateRole::PreCandidate, true, 0, INVALID_ID),
        (StateRole::Follower, StateRole::Candidate, true, 1, INVALID_ID),
        (StateRole::Follower, StateRole::Leader, false, 0, INVALID_ID),

        (StateRole::PreCandidate, StateRole::Follower, true, 0, INVALID_ID),
        (StateRole::PreCandidate, StateRole::PreCandidate, true, 0, INVALID_ID),
        (StateRole::PreCandidate, StateRole::Candidate, true, 1, INVALID_ID),
        (StateRole::PreCandidate, StateRole::Leader, true, 0, 1),

        (StateRole::Candidate, StateRole::Follower, true, 0, INVALID_ID),
        (StateRole::Candidate, StateRole::PreCandidate, true, 0, INVALID_ID),
        (StateRole::Candidate, StateRole::Candidate, true, 1, INVALID_ID),
        (StateRole::Candidate, StateRole::Leader, true, 0, 1),

        (StateRole::Leader, StateRole::Follower, true, 1, INVALID_ID),
        (StateRole::Leader, StateRole::PreCandidate, false, 0, INVALID_ID),
        (StateRole::Leader, StateRole::Candidate, false, 1, INVALID_ID),
        (StateRole::Leader, StateRole::Leader, true, 0, 1),
    ];
    for (i, (from, to, wallow, wterm, wlead)) in tests.drain(..).enumerate() {
        let mut sm: &mut Raft<MemStorage> = &mut new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.state = from;

        let res = recover_safe!(|| {
            match to {
                StateRole::Follower => sm.become_follower(wterm, wlead),
                StateRole::PreCandidate => sm.become_pre_candidate(),
                StateRole::Candidate => sm.become_candidate(),
                StateRole::Leader => sm.become_leader(),
            }
        });
        if res.is_ok() ^ wallow {
            panic!("#{}: allow = {}, want {}", i, res.is_ok(), wallow);
        }
        if res.is_err() {
            continue;
        }

        if sm.term != wterm {
            panic!("#{}: term = {}, want {}", i, sm.term, wterm);
        }
        if sm.leader_id != wlead {
            panic!("#{}: lead = {}, want {}", i, sm.leader_id, wlead);
        }
    }
}

#[test]
fn test_all_server_stepdown() {
    let mut tests = vec![
        (StateRole::Follower, StateRole::Follower, 3, 0),
        (StateRole::PreCandidate, StateRole::Follower, 3, 0),
        (StateRole::Candidate, StateRole::Follower, 3, 0),
        (StateRole::Leader, StateRole::Follower, 3, 1),
    ];

    let tmsg_types = vec![MessageType::MsgRequestVote, MessageType::MsgAppend];
    let tterm = 3u64;

    for (i, (state, wstate, wterm, windex)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        match state {
            StateRole::Follower => sm.become_follower(1, INVALID_ID),
            StateRole::PreCandidate => sm.become_pre_candidate(),
            StateRole::Candidate => sm.become_candidate(),
            StateRole::Leader => {
                sm.become_candidate();
                sm.become_leader();
            }
        }

        for (j, &msg_type) in tmsg_types.iter().enumerate() {
            let mut m = new_message(2, 0, msg_type, 0);
            m.set_term(tterm);
            m.set_log_term(tterm);
            sm.step(m).expect("");

            if sm.state != wstate {
                panic!("{}.{} state = {:?}, want {:?}", i, j, sm.state, wstate);
            }
            if sm.term != wterm {
                panic!("{}.{} term = {}, want {}", i, j, sm.term, wterm);
            }
            if sm.raft_log.last_index() != windex {
                panic!("{}.{} index = {}, want {}",
                       i,
                       j,
                       sm.raft_log.last_index(),
                       windex);
            }
            let entry_count = sm.raft_log.all_entries().len() as u64;
            if entry_count != windex {
                panic!("{}.{} ents count = {}, want {}", i, j, entry_count, windex);
            }
            let wlead = if msg_type == MessageType::MsgRequestVote {
                INVALID_ID
            } else {
                2
            };
            if sm.leader_id != wlead {
                panic!("{}, sm.lead = {}, want {}", i, sm.leader_id, INVALID_ID);
            }
        }
    }
}

#[test]
fn test_leader_stepdown_when_quorum_active() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage());
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();

    for _ in 0..(sm.get_election_timeout() + 1) {
        let mut m = new_message(2, 0, MessageType::MsgHeartbeatResponse, 0);
        m.set_term(sm.term);
        sm.step(m).expect("");
        sm.tick();
    }

    assert_eq!(sm.state, StateRole::Leader);
}

#[test]
fn test_leader_stepdown_when_quorum_lost() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage());

    sm.check_quorum = true;

    sm.become_candidate();
    sm.become_leader();

    for _ in 0..(sm.get_election_timeout() + 1) {
        sm.tick();
    }

    assert_eq!(sm.state, StateRole::Follower);
}

#[test]
fn test_leader_superseding_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    let b_election_timeout = nt.peers[&2].get_election_timeout();

    // prevent campaigning from b
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 1);
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // Letting b's electionElapsed reach to electionTimeout
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

#[test]
fn test_leader_election_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let a_election_timeout = nt.peers[&1].get_election_timeout();
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&1).unwrap().set_randomized_election_timeout(a_election_timeout + 1);
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 2);

    // Immediately after creation, votes are cast regardless of the election timeout

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // need to reset randomizedElectionTimeout larger than electionTimeout again,
    // because the value might be reset to electionTimeout since the last state changes
    let a_election_timeout = nt.peers[&1].get_election_timeout();
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&1).unwrap().set_randomized_election_timeout(a_election_timeout + 1);
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 2);

    for _ in 0..a_election_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

// test_free_stuck_candidate_with_check_quorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
#[test]
fn test_free_stuck_candidate_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.isolate(1);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);
    assert_eq!(nt.peers[&3].term, &nt.peers[&2].term + 1);

    // Vote again for safety
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);
    assert_eq!(nt.peers[&3].term, &nt.peers[&2].term + 2);

    nt.recover();
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.set_term(nt.peers[&1].term);
    nt.send(vec![msg]);

    // Disrupt the leader so that the stuck peer is freed
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].term, nt.peers[&1].term);
}

#[test]
fn test_non_promotable_voter_wich_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b)]);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 1);

    // Need to remove 2 again to make it a non-promotable node since newNetwork
    // overwritten some internal states
    nt.peers.get_mut(&2).unwrap().prs.remove(&2).unwrap();

    assert_eq!(nt.peers[&2].promotable(), false);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].leader_id, 1);
}

#[test]
fn test_read_only_option_safe() {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut tests = vec![
        (1, 10, 11, "ctx1"),
        (2, 10, 21, "ctx2"),
        (3, 10, 31, "ctx3"),
        (1, 10, 41, "ctx4"),
        (2, 10, 51, "ctx5"),
        (3, 10, 61, "ctx6"),
    ];

    for (i, (id, proposals, wri, wctx)) in tests.drain(..).enumerate() {
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let e = new_entry(0, 0, Some(wctx));
        nt.send(vec![new_message_with_entries(id, id, MessageType::MsgReadIndex, vec![e])]);

        let read_states: Vec<ReadState> =
            nt.peers.get_mut(&id).unwrap().read_states.drain(..).collect();
        if read_states.is_empty() {
            panic!("#{}: read_states is empty, want non-empty", i);
        }
        let rs = &read_states[0];
        if rs.index != wri {
            panic!("#{}: read_index = {}, want {}", i, rs.index, wri)
        }
        let vec_wctx = wctx.as_bytes().to_vec();
        if rs.request_ctx != vec_wctx {
            panic!("#{}: request_ctx = {:?}, want {:?}",
                   i,
                   rs.request_ctx,
                   vec_wctx)
        }
    }
}

#[test]
fn test_read_only_option_lease() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;
    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut tests = vec![
        (1, 10, 11, "ctx1"),
        (2, 10, 21, "ctx2"),
        (3, 10, 31, "ctx3"),
        (1, 10, 41, "ctx4"),
        (2, 10, 51, "ctx5"),
        (3, 10, 61, "ctx6"),
    ];

    for (i, (id, proposals, wri, wctx)) in tests.drain(..).enumerate() {
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let e = new_entry(0, 0, Some(wctx));
        nt.send(vec![new_message_with_entries(id, id, MessageType::MsgReadIndex, vec![e])]);

        let read_states: Vec<ReadState> =
            nt.peers.get_mut(&id).unwrap().read_states.drain(..).collect();
        if read_states.is_empty() {
            panic!("#{}: read_states is empty, want non-empty", i);
        }
        let rs = &read_states[0];
        if rs.index != wri {
            panic!("#{}: read_index = {}, want {}", i, rs.index, wri);
        }
        let vec_wctx = wctx.as_bytes().to_vec();
        if rs.request_ctx != vec_wctx {
            panic!("#{}: request_ctx = {:?}, want {:?}",
                   i,
                   rs.request_ctx,
                   vec_wctx);
        }
    }
}

#[test]
fn test_read_only_option_lease_without_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let ctx = "ctx1";
    let e = new_entry(0, 0, Some(ctx));
    nt.send(vec![new_message_with_entries(2, 2, MessageType::MsgReadIndex, vec![e])]);

    let read_states = &nt.peers[&2].read_states;
    assert!(!read_states.is_empty());
    let rs = &read_states[0];
    assert_eq!(rs.index, INVALID_ID);
    let vec_ctx = ctx.as_bytes().to_vec();
    assert_eq!(rs.request_ctx, vec_ctx);
}

#[test]
fn test_leader_append_response() {
    // initial progress: match = 0; next = 3
    let mut tests = vec![
        (3, true, 0, 3, 0, 0, 0), // stale resp; no replies
        (2, true, 0, 2, 1, 1, 0), // denied resp; leader does not commit; descrease next and send
                                  // probing msg
        (2, false, 2, 4, 2, 2, 2), // accept resp; leader commits; broadcast with commit index
        (0, false, 0, 3, 0, 0, 0),
    ];

    for (i, (index, reject, wmatch, wnext, wmsg_num, windex, wcommitted)) in tests.drain(..)
        .enumerate() {
        // sm term is 1 after it becomes the leader.
        // thus the last log term must be 1 to be committed.
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        sm.raft_log = new_raft_log(vec![empty_entry(0, 1), empty_entry(1, 2)], 3, 0);
        sm.become_candidate();
        sm.become_leader();
        sm.read_messages();
        let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
        m.set_index(index);
        m.set_term(sm.term);
        m.set_reject(reject);
        m.set_reject_hint(index);
        sm.step(m).expect("");

        if sm.prs[&2].matched != wmatch {
            panic!("#{}: match = {}, want {}", i, sm.prs[&2].matched, wmatch);
        }
        if sm.prs[&2].next_idx != wnext {
            panic!("#{}: next = {}, want {}", i, sm.prs[&2].next_idx, wnext);
        }

        let mut msgs = sm.read_messages();
        if msgs.len() != wmsg_num {
            panic!("#{} msg_num = {}, want {}", i, msgs.len(), wmsg_num);
        }
        for (j, msg) in msgs.drain(..).enumerate() {
            if msg.get_index() != windex {
                panic!("#{}.{} index = {}, want {}", i, j, msg.get_index(), windex);
            }
            if msg.get_commit() != wcommitted {
                panic!("#{}.{} commit = {}, want {}",
                       i,
                       j,
                       msg.get_commit(),
                       wcommitted);
            }
        }
    }
}

// When the leader receives a heartbeat tick, it should
// send a MsgApp with m.Index = 0, m.LogTerm=0 and empty entries.
#[test]
fn test_bcast_beat() {
    let offset = 1000u64;
    // make a state machine with log.offset = 1000
    let s = new_snapshot(offset, 1, vec![1, 2, 3]);
    let store = new_storage();
    store.wl().apply_snapshot(s).expect("");
    let mut sm = new_test_raft(1, vec![], 10, 1, store);
    sm.term = 1;

    sm.become_candidate();
    sm.become_leader();
    for i in 0..10 {
        sm.append_entry(&mut [empty_entry(0, i as u64 + 1)]);
    }
    // slow follower
    let mut_pr = |sm: &mut Interface, n, matched, next_idx| {
        let m = sm.prs.get_mut(&n).unwrap();
        m.matched = matched;
        m.next_idx = next_idx;
    };
    // slow follower
    mut_pr(&mut sm, 2, 5, 6);
    // normal follower
    let last_index = sm.raft_log.last_index();
    mut_pr(&mut sm, 3, last_index, last_index + 1);

    sm.step(new_message(0, 0, MessageType::MsgBeat, 0)).expect("");
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 2);
    let mut want_commit_map = HashMap::new();
    want_commit_map.insert(2, cmp::min(sm.raft_log.committed, sm.prs[&2].matched));
    want_commit_map.insert(3, cmp::min(sm.raft_log.committed, sm.prs[&3].matched));
    for (i, m) in msgs.drain(..).enumerate() {
        if m.get_msg_type() != MessageType::MsgHeartbeat {
            panic!("#{}: type = {:?}, want = {:?}",
                   i,
                   m.get_msg_type(),
                   MessageType::MsgHeartbeat);
        }
        if m.get_index() != 0 {
            panic!("#{}: prev_index = {}, want {}", i, m.get_index(), 0);
        }
        if m.get_log_term() != 0 {
            panic!("#{}: prev_term = {}, want {}", i, m.get_log_term(), 0);
        }
        if want_commit_map[&m.get_to()] == 0 {
            panic!("#{}: unexpected to {}", i, m.get_to())
        } else {
            if m.get_commit() != want_commit_map[&m.get_to()] {
                panic!("#{}: commit = {}, want {}",
                       i,
                       m.get_commit(),
                       want_commit_map[&m.get_to()]);
            }
            want_commit_map.remove(&m.get_to());
        }
        if m.get_entries().len() != 0 {
            panic!("#{}: entries count = {}, want 0", i, m.get_entries().len());
        }
    }
}

// tests the output of the statemachine when receiving MsgBeat
#[test]
fn test_recv_msg_beat() {
    let mut tests = vec![
        (StateRole::Leader, 2),
        // candidate and follower should ignore MsgBeat
        (StateRole::Candidate, 0),
        (StateRole::Follower, 0),
    ];

    for (i, (state, w_msg)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        sm.raft_log = new_raft_log(vec![empty_entry(0, 1), empty_entry(1, 2)], 0, 0);
        sm.term = 1;
        sm.state = state;
        sm.step(new_message(1, 1, MessageType::MsgBeat, 0)).expect("");

        let msgs = sm.read_messages();
        if msgs.len() != w_msg {
            panic!("#{}: msg count = {}, want {}", i, msgs.len(), w_msg);
        }
        for m in msgs {
            if m.get_msg_type() != MessageType::MsgHeartbeat {
                panic!("#{}: msg.type = {:?}, want {:?}",
                       i,
                       m.get_msg_type(),
                       MessageType::MsgHeartbeat);
            }
        }
    }
}

#[test]
fn test_leader_increase_next() {
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let mut tests = vec![
        // state replicate; optimistically increase next
        // previous entries + noop entry + propose + 1
        (ProgressState::Replicate, 2, previous_ents.len() as u64 + 1 + 1 + 1),
        // state probe, not optimistically increase next
        (ProgressState::Probe, 2, 2),
    ];
    for (i, (state, next_idx, wnext)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
        sm.raft_log.append(&previous_ents);
        sm.become_candidate();
        sm.become_leader();
        sm.prs.get_mut(&2).unwrap().state = state;
        sm.prs.get_mut(&2).unwrap().next_idx = next_idx;
        sm.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

        if sm.prs[&2].next_idx != wnext {
            panic!("#{}: next = {}, want {}", i, sm.prs[&2].next_idx, wnext);
        }
    }
}

#[test]
fn test_send_append_for_progress_probe() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.prs.get_mut(&2).unwrap().become_probe();

    // each round is a heartbeat
    for _ in 0..3 {
        // we expect that raft will only send out one msgAPP per heartbeat timeout
        r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        let mut msg = r.read_messages();
        assert_eq!(msg.len(), 1);
        assert_eq!(msg[0].get_index(), 0);

        assert!(r.prs[&2].paused);
        for _ in 0..10 {
            r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
            r.send_append(2);
            assert_eq!(r.read_messages().len(), 0);
        }

        // do a heartbeat
        for _ in 0..r.get_heartbeat_timeout() {
            r.step(new_message(1, 1, MessageType::MsgBeat, 0)).expect("");
        }
        // consume the heartbeat
        msg = r.read_messages();
        assert_eq!(msg.len(), 1);
        assert_eq!(msg[0].get_msg_type(), MessageType::MsgHeartbeat);
    }
}

#[test]
fn test_send_append_for_progress_replicate() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.prs.get_mut(&2).unwrap().become_replicate();

    for _ in 0..10 {
        r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        assert_eq!(r.read_messages().len(), 1);
    }
}

#[test]
fn test_send_append_for_progress_snapshot() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.prs.get_mut(&2).unwrap().become_snapshot(10);

    for _ in 0..10 {
        r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        assert_eq!(r.read_messages().len(), 0);
    }
}

#[test]
fn test_recv_msg_unreachable() {
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let s = new_storage();
    s.wl().append(&previous_ents).expect("");
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, s);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    // set node 2 to state replicate
    r.prs.get_mut(&2).unwrap().matched = 3;
    r.prs.get_mut(&2).unwrap().become_replicate();
    r.prs.get_mut(&2).unwrap().optimistic_update(5);

    r.step(new_message(2, 1, MessageType::MsgUnreachable, 0)).expect("");

    assert_eq!(r.prs[&2].state, ProgressState::Probe);
    assert_eq!(r.prs[&2].matched + 1, r.prs[&2].next_idx);
}

#[test]
fn test_restore() {
    // magic number
    let s = new_snapshot(11, 11, vec![1, 2, 3]);

    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    assert!(sm.restore(s.clone()));
    assert_eq!(sm.raft_log.last_index(), s.get_metadata().get_index());
    assert_eq!(sm.raft_log.term(s.get_metadata().get_index()).unwrap(),
               s.get_metadata().get_term());
    assert_eq!(sm.nodes(), s.get_metadata().get_conf_state().get_nodes());
    assert!(!sm.restore(s));
}

#[test]
fn test_restore_ignore_snapshot() {
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let commit = 1u64;
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.raft_log.append(&previous_ents);
    sm.raft_log.commit_to(commit);

    let mut s = new_snapshot(commit, 1, vec![1, 2]);

    // ingore snapshot
    assert!(!sm.restore(s.clone()));
    assert_eq!(sm.raft_log.committed, commit);

    // ignore snapshot and fast forward commit
    s.mut_metadata().set_index(commit + 1);
    assert!(!sm.restore(s));
    assert_eq!(sm.raft_log.committed, commit + 1);
}

#[test]
fn test_provide_snap() {
    // restore the state machine from a snapshot so it has a compacted log and a snapshot
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number

    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.restore(s);

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 2, so that node 2 needs a snapshot
    sm.prs.get_mut(&2).unwrap().next_idx = sm.raft_log.first_index();
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(sm.prs[&2].next_idx - 1);
    m.set_reject(true);
    sm.step(m).expect("");

    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgSnapshot);
}

#[test]
fn test_ignore_providing_snapshot() {
    // restore the state machine from a snapshot so it has a compacted log and a snapshot
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.restore(s);

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 2, so that node 2 needs a snapshot
    // change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
    sm.prs.get_mut(&2).unwrap().next_idx = sm.raft_log.first_index() - 1;
    sm.prs.get_mut(&2).unwrap().recent_active = false;

    sm.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

    assert_eq!(sm.read_messages().len(), 0);
}

#[test]
fn test_restore_from_snap_msg() {
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number
    let mut sm = new_test_raft(2, vec![1, 2], 10, 1, new_storage());
    let mut m = new_message(1, 0, MessageType::MsgSnapshot, 0);
    m.set_term(2);
    m.set_snapshot(s);

    sm.step(m).expect("");

    assert_eq!(sm.leader_id, 1);

    // TODO: port the remaining if upstream completed this test.
}

#[test]
fn test_slow_node_restore() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);
    for _ in 0..100 {
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    }
    next_ents(&mut nt.peers.get_mut(&1).unwrap(), &nt.storage[&1]);
    let mut cs = ConfState::new();
    cs.set_nodes(nt.peers[&1].nodes());
    nt.storage[&1]
        .wl()
        .create_snapshot(nt.peers[&1].raft_log.applied, Some(cs), vec![])
        .expect("");
    nt.storage[&1].wl().compact(nt.peers[&1].raft_log.applied).expect("");

    nt.recover();
    // send heartbeats so that the leader can learn everyone is active.
    // node 3 will only be considered as active when node 1 receives a reply from it.
    loop {
        nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
        if nt.peers[&1].prs[&3].recent_active {
            break;
        }
    }

    // trigger a snapshot
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    // trigger a commit
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(nt.peers[&3].raft_log.committed,
               nt.peers[&1].raft_log.committed);
}

// test_step_config tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
#[test]
fn test_step_config() {
    // a raft that cannot make progress
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    let index = r.raft_log.last_index();
    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    m.mut_entries().push(e);
    r.step(m).expect("");
    assert_eq!(r.raft_log.last_index(), index + 1);
    assert!(r.pending_conf);
}

// test_step_ignore_config tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
#[test]
fn test_step_ignore_config() {
    // a raft that cannot make progress
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    m.mut_entries().push(e);
    r.step(m.clone()).expect("");
    let index = r.raft_log.last_index();
    let pending_conf = r.pending_conf;
    r.step(m.clone()).expect("");
    let mut we = empty_entry(1, 3);
    we.set_entry_type(EntryType::EntryNormal);
    let wents = vec![we];
    let entries = r.raft_log.entries(index + 1, NO_LIMIT).expect("");
    assert_eq!(entries, wents);
    assert_eq!(r.pending_conf, pending_conf);
}

// test_recover_pending_config tests that new leader recovers its pendingConf flag
// based on uncommitted entries.
#[test]
fn test_recover_pending_config() {
    let mut tests = vec![
        (EntryType::EntryNormal, false),
        (EntryType::EntryConfChange, true),
    ];
    for (i, (ent_type, wpending)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
        let mut e = Entry::new();
        e.set_entry_type(ent_type);
        r.append_entry(&mut [e]);
        r.become_candidate();
        r.become_leader();
        if r.pending_conf != wpending {
            panic!("#{}: pending_conf = {}, want {}",
                   i,
                   r.pending_conf,
                   wpending);
        }
    }
}

// test_recover_double_pending_config tests that new leader will panic if
// there exist two uncommitted config entries.
#[test]
fn test_recover_double_pending_config() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    r.append_entry(&mut [e.clone()]);
    r.append_entry(&mut [e]);
    r.become_candidate();
    assert!(recover_safe!(|| r.become_leader()).is_err());
}

// test_add_node tests that addNode could update pendingConf and nodes correctly.
#[test]
fn test_add_node() {
    let mut r = new_test_raft(1, vec![1], 10, 1, new_storage());
    r.pending_conf = true;
    r.add_node(2);
    assert!(!r.pending_conf);
    assert_eq!(r.nodes(), vec![1, 2]);
}

// test_remove_node tests that removeNode could update pendingConf, nodes and
// and removed list correctly.
#[test]
fn test_remove_node() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.pending_conf = true;
    r.remove_node(2);
    assert!(!r.pending_conf);
    assert_eq!(r.nodes(), vec![1]);

    // remove all nodes from cluster
    r.remove_node(1);
    assert_eq!(r.nodes(), vec![]);
}

#[test]
fn test_promotable() {
    let id = 1u64;
    let mut tests = vec![
        (vec![1], true),
        (vec![1, 2, 3], true),
        (vec![], false),
        (vec![2, 3], false),
    ];
    for (i, (peers, wp)) in tests.drain(..).enumerate() {
        let r = new_test_raft(id, peers, 5, 1, new_storage());
        if r.promotable() != wp {
            panic!("#{}: promotable = {}, want {}", i, r.promotable(), wp);
        }
    }
}

#[test]
fn test_raft_nodes() {
    let mut tests = vec![
        (vec![1, 2, 3], vec![1, 2, 3]),
        (vec![3, 2, 1], vec![1, 2, 3]),
    ];
    for (i, (ids, wids)) in tests.drain(..).enumerate() {
        let r = new_test_raft(1, ids, 10, 1, new_storage());
        if r.nodes() != wids {
            panic!("#{}: nodes = {:?}, want {:?}", i, r.nodes(), wids);
        }
    }
}

#[test]
fn test_campaign_while_leader() {
    test_campaign_while_leader_with_pre_vote(false);
}

#[test]
fn test_pre_campaign_while_leader() {
    test_campaign_while_leader_with_pre_vote(true);
}


fn test_campaign_while_leader_with_pre_vote(pre_vote: bool) {
    let mut r = new_test_raft(1, vec![1], 5, 1, new_storage());
    r.pre_vote = pre_vote;
    assert_eq!(r.state, StateRole::Follower);
    // We don't call campaign() directly because it comes after the check
    // for our current state.
    r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
    assert_eq!(r.state, StateRole::Leader);
    let term = r.term;
    r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
    assert_eq!(r.state, StateRole::Leader);
    assert_eq!(r.term, term);
}

// test_commit_after_remove_node verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
#[test]
fn test_commit_after_remove_node() {
    // Create a cluster with two nodes.
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, s.clone());
    r.become_candidate();
    r.become_leader();

    // Begin to remove the second node.
    let mut m = new_message(0, 0, MessageType::MsgPropose, 0);
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    let mut cc = ConfChange::new();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.set_node_id(2);
    e.set_data(protobuf::Message::write_to_bytes(&cc).unwrap());
    m.mut_entries().push(e);
    r.step(m).expect("");
    // Stabilize the log and make sure nothing is committed yet.
    assert_eq!(next_ents(&mut r, &s).len(), 0);
    let cc_index = r.raft_log.last_index();

    // While the config change is pending, make another proposal.
    let mut m = new_message(0, 0, MessageType::MsgPropose, 0);
    let mut e = new_entry(0, 0, Some("hello"));
    e.set_entry_type(EntryType::EntryNormal);
    m.mut_entries().push(e);
    r.step(m).expect("");

    // Node 2 acknowledges the config change, committing it.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(cc_index);
    r.step(m).expect("");
    let ents = next_ents(&mut r, &s);
    assert_eq!(ents.len(), 2);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert!(!ents[0].has_data());
    assert_eq!(ents[1].get_entry_type(), EntryType::EntryConfChange);

    // Apply the config change. This reduces quorum requirements so the
    // pending command can now commit.
    r.remove_node(2);
    let ents = next_ents(&mut r, &s);
    assert_eq!(ents.len(), 1);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert_eq!(ents[0].get_data(), "hello".as_bytes());
}

// test_leader_transfer_to_uptodate_node verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
#[test]
fn test_leader_transfer_to_uptodate_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let lead_id = nt.peers[&1].leader_id;
    assert_eq!(lead_id, 1);

    // Transfer leadership to peer 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to peer 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// test_leader_transfer_to_uptodate_node_from_follower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like test_leader_transfer_to_uptodate_node, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
#[test]
fn test_leader_transfer_to_uptodate_node_from_follower() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let lead_id = nt.peers[&1].leader_id;
    assert_eq!(lead_id, 1);

    // transfer leadership to peer 2.
    nt.send(vec![new_message(2, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to peer 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
#[test]
fn test_leader_transfer_with_check_quorum() {
    let mut nt = Network::new(vec![None, None, None]);
    for i in 1..4 {
        let r = &mut nt.peers.get_mut(&i).unwrap();
        r.check_quorum = true;
        let election_timeout = r.get_election_timeout();
        r.set_randomized_election_timeout(election_timeout + i as usize);
    }

    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers.get_mut(&2).unwrap().set_randomized_election_timeout(b_election_timeout + 1);

    // Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].leader_id, 1);

    // Transfer leadership to 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_to_slow_follower() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.recover();
    assert_eq!(nt.peers[&1].prs[&3].matched, 1);

    // Transfer leadership to 3 when node 3 is lack of log.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 3);
}

#[test]
fn test_leader_transfer_after_snapshot() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    next_ents(&mut nt.peers.get_mut(&1).unwrap(), &nt.storage[&1]);
    let mut cs = ConfState::new();
    cs.set_nodes(nt.peers[&1].nodes());
    nt.storage[&1]
        .wl()
        .create_snapshot(nt.peers[&1].raft_log.applied, Some(cs), vec![])
        .expect("");
    nt.storage[&1].wl().compact(nt.peers[&1].raft_log.applied).expect("");

    nt.recover();
    assert_eq!(nt.peers[&1].prs[&3].matched, 1);

    // Transfer leadership to 3 when node 3 is lack of snapshot.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    // Send pb.MsgHeartbeatResp to leader to trigger a snapshot for node 3.
    nt.send(vec![new_message(3, 1, MessageType::MsgHeartbeatResponse, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 3);
}

#[test]
fn test_leader_transfer_to_self() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to self, there will be noop.
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_to_non_existing_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to non-existing node, there will be noop.
    nt.send(vec![new_message(4, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_timeout() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node, wait for timeout.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);
    let heartbeat_timeout = nt.peers[&1].get_heartbeat_timeout();
    let election_timeout = nt.peers[&1].get_election_timeout();
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_ignore_proposal() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node to let transfer pending, then send proposal.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(nt.peers[&1].prs[&1].matched, 1);
}

#[test]
fn test_leader_transfer_receive_higher_term_vote() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node to let transfer pending.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.send(vec![new_message_with_entries(2, 2, MessageType::MsgHup, vec![new_entry(1, 2, None)])]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);
}

#[test]
fn test_leader_transfer_remove_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.ignore(MessageType::MsgTimeoutNow);

    // The lead_transferee is removed when leadship transferring.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.peers.get_mut(&1).unwrap().remove_node(3);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// test_leader_transfer_back verifies leadership can transfer
// back to self when last transfer is pending.
#[test]
fn test_leader_transfer_back() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    // Transfer leadership back to self.
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

// test_leader_transfer_second_transfer_to_another_node verifies leader can transfer to another node
// when last transfer is pending.
#[test]
fn test_leader_transfer_second_transfer_to_another_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    // Transfer leadership to another node.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);
}

// test_leader_transfer_second_transfer_to_same_node verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
#[test]
fn test_leader_transfer_second_transfer_to_same_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    let heartbeat_timeout = nt.peers[&1].get_heartbeat_timeout();
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    // Second transfer leadership request to the same node.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    let election_timeout = nt.peers[&1].get_election_timeout();
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

fn check_leader_transfer_state(r: &Raft<MemStorage>, state: StateRole, lead: u64) {
    if r.state != state || r.leader_id != lead {
        panic!("after transferring, node has state {:?} lead {}, want state {:?} lead {}",
               r.state,
               r.leader_id,
               state,
               lead);
    }
    assert_eq!(r.lead_transferee, None);
}
