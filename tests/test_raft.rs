use tikv::raft::*;
use tikv::raft::storage::MemStorage;
use std::sync::Arc;
use std::collections::HashMap;
use protobuf::RepeatedField;
use std::ops::Deref;
use std::ops::DerefMut;
use std::panic;
use std::cmp;
use tikv::proto::raftpb::{Entry, Message, MessageType, HardState, Snapshot};
use rand;

fn ltoa(raft_log: &RaftLog<MemStorage>) -> String {
    let mut s = format!("committed: {}\n", raft_log.committed);
    s = s + &format!("applied: {}\n", raft_log.applied);
    for (i, e) in raft_log.all_entries().iter().enumerate() {
        s = s + &format!("#{}: {:?}\n", i, e);
    }
    s
}

fn new_storage() -> Arc<MemStorage> {
    Arc::new(MemStorage::new())
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

fn new_test_raft(id: u64,
                 peers: Vec<u64>,
                 election: usize,
                 heartbeat: usize,
                 storage: Arc<MemStorage>)
                 -> Interface {
    Interface::new(Raft::new(&Config {
        id: id,
        peers: peers,
        election_tick: election,
        heartbeat_tick: heartbeat,
        storage: storage,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }))
}

fn read_messages<T: Storage + Default>(raft: &mut Raft<T>) -> Vec<Message> {
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
    let mut raft = new_test_raft(1, vec![], 5, 1, Arc::new(store));
    raft.reset(0);
    raft
}

fn next_ents(r: &mut Raft<MemStorage>, s: &MemStorage) -> Vec<Entry> {
    s.wl().append(&r.raft_log.unstable_entries().unwrap()).expect("");
    let (last_idx, last_term) = (r.raft_log.last_index(), r.raft_log.last_term());
    r.raft_log.stable_to(last_idx, last_term);
    let ents = r.raft_log.next_entries();
    let committed = r.raft_log.committed;
    r.raft_log.applied_to(committed);
    ents.unwrap()
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
struct Interface {
    raft: Option<Raft<MemStorage>>,
}

impl Interface {
    fn new(r: Raft<MemStorage>) -> Interface {
        Interface { raft: Some(r) }
    }

    fn step(&mut self, m: Message) -> Result<()> {
        match self.raft {
            Some(_) => Raft::step(self, m),
            None => Ok(()),
        }
    }

    fn read_messages(&mut self) -> Vec<Message> {
        match self.raft {
            Some(_) => self.msgs.drain(..).collect(),
            None => vec![],
        }
    }

    fn initial(&mut self, id: u64, ids: &Vec<u64>) {
        if self.raft.is_some() {
            self.id = id;
            self.prs = HashMap::with_capacity(ids.len());
            for id in ids {
                self.prs.insert(*id, Progress { ..Default::default() });
            }
            self.reset(0);
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

const NOP_STEPPER: Option<Interface> = Some(Interface { raft: None });

const SOME_DATA: Option<&'static str> = Some("somedata");

fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(t);
    if n > 0 {
        let mut ents = Vec::with_capacity(n);
        for _ in 0..n {
            ents.push(new_entry(0, 0, SOME_DATA));
        }
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

fn empty_entry(term: u64, index: u64) -> Entry {
    new_entry(term, index, None)
}

fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
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
        store: Arc::new(store),
        unstable: Unstable { offset: offset, ..Default::default() },
        committed: committed,
        ..Default::default()
    }
}

fn new_snapshot(index: u64, term: u64, nodes: Vec<u64>) -> Snapshot {
    let mut s = Snapshot::new();
    s.mut_metadata().set_index(index);
    s.mut_metadata().set_term(term);
    s.mut_metadata().mut_conf_state().set_nodes(nodes);
    s
}

#[derive(Default)]
struct Network {
    peers: HashMap<u64, Interface>,
    storage: HashMap<u64, Arc<MemStorage>>,
    dropm: HashMap<Connem, f64>,
    ignorem: HashMap<MessageType, bool>,
}

impl Network {
    // initializes a network from peers.
    // A nil node will be replaced with a new *stateMachine.
    // A *stateMachine will get its k, id.
    // When using stateMachine, the address list is always [1, n].
    fn new(peers: Vec<Option<Interface>>) -> Network {
        let size = peers.len();
        let peer_addrs: Vec<u64> = (1..size as u64 + 1).collect();
        let mut nstorage: HashMap<u64, Arc<MemStorage>> = HashMap::new();
        let mut npeers: HashMap<u64, Interface> = HashMap::new();
        let mut peers = peers;
        for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
            match p {
                None => {
                    nstorage.insert(id, new_storage());
                    let r = new_test_raft(id, peer_addrs.clone(), 10, 1, nstorage[&id].clone());
                    npeers.insert(id, r);
                }
                Some(p) => {
                    let mut p = p;
                    p.initial(id, &peer_addrs);
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

    fn filter(&self, msgs: Vec<Message>) -> Vec<Message> {
        let mut msgs = msgs;
        let msgs: Vec<Message> =
            msgs.drain(..)
                .filter(|m| {
                    if self.ignorem.get(&m.get_msg_type()).map(|x| *x).unwrap_or(false) {
                        return false;
                    }
                    // hups never go over the network, so don't drop them but panic
                    assert!(m.get_msg_type() != MessageType::MsgHup, "unexpected msgHup");
                    let perc = self.dropm
                                   .get(&Connem {
                                       from: m.get_from(),
                                       to: m.get_to(),
                                   })
                                   .map(|x| *x)
                                   .unwrap_or(0f64);
                    rand::random::<f64>() >= perc
                })
                .collect();
        msgs
    }

    fn send(&mut self, msgs: Vec<Message>) {
        let mut msgs = msgs;
        while msgs.len() > 0 {
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
    e.set_data("some_data".as_bytes().to_vec());
    m.set_entries(RepeatedField::from_vec(vec![e]));
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    let ms = read_messages(&mut raft);
    assert_eq!(ms.len(), 1);
}

#[test]
fn test_leader_election() {
    let mut tests = vec![
        (Network::new(vec![None, None, None]), StateRole::Leader),
        (Network::new(vec![None, None, NOP_STEPPER]), StateRole::Leader),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER]), StateRole::Candidate),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None]), StateRole::Candidate),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None, None]), StateRole::Leader),
        
        // three logs further along than 0
        (Network::new(vec![None, Some(ents(vec![1])), Some(ents(vec![2])), Some(ents(vec![1, 3])), None]), StateRole::Follower),
        
        // logs converge
        (Network::new(vec![Some(ents(vec![1])), None, Some(ents(vec![2])), Some(ents(vec![1])), None]), StateRole::Leader),
    ];

    for (i, &mut (ref mut network, state)) in tests.iter_mut().enumerate() {
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgHup);
        network.send(vec![m]);
        let raft = network.peers.get(&1).unwrap();
        if raft.state != state {
            panic!("#{}: state = {:?}, want {:?}", i, raft.state, state);
        }
        if raft.term != 1 {
            panic!("#{}: term = {}, want {}", i, raft.term, 1)
        }
    }
}

#[test]
fn test_log_replicatioin() {
    let mut tests = vec![
        (Network::new(vec![None, None, None]), vec![new_message(1, 1, MessageType::MsgPropose, 1)], 2),
        (Network::new(vec![None, None, None]), vec![new_message(1, 1, MessageType::MsgPropose, 1), new_message(1, 2, MessageType::MsgHup, 0), new_message(1, 2, MessageType::MsgPropose, 1)], 4),
    ];

    for (i, &mut (ref mut network, ref msgs, wcommitted)) in tests.iter_mut().enumerate() {
        network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        for m in msgs {
            network.send(vec![m.clone()]);
        }

        for (j, x) in network.peers.iter_mut() {
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

    nt.recover();
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    let wlog = new_raft_log(vec![new_entry(1, 1, None)], 2, 1);
    let wlog2 = RaftLog::new(new_storage());
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

    let ents = vec![new_entry(1, 1, None), new_entry(1, 2, Some(data))];
    let want_log = ltoa(&new_raft_log(ents, 3, 2));
    for (id, p) in tt.peers.iter() {
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
fn test_old_messages() {
    let mut tt = Network::new(vec![None, None, None]);
    // make 0 leader @ term 3
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
    let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
    m.set_term(2);
    m.set_entries(RepeatedField::from_vec(vec![new_entry(2, 3, None)]));
    tt.send(vec![m]);
    // commit a new entry
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    let ents = vec![new_entry(1, 1, None),
                    new_entry(2, 2, None),
                    new_entry(3, 3, None),
                    new_entry(3, 4, SOME_DATA)];
    let ilog = new_raft_log(ents, 5, 4);
    let base = ltoa(&ilog);
    for (id, p) in tt.peers.iter() {
        let l = ltoa(&p.raft_log);
        if l != base {
            panic!("#{}: raft_log: {}, want: {}", id, l, base);
        }
    }
}

// TestOldMessagesReply - optimization - reply with new term.

#[test]
fn test_proposal() {
    let mut tests = vec![
        (Network::new(vec![None, None, None]), true),
        (Network::new(vec![None, None, NOP_STEPPER]), true),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER]), false),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None]), false),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None, None]), true),
    ];

    for (j, (network, success)) in tests.drain(..).enumerate() {
        let mut nw = network;
        let send = |nw: &mut Network, m| {
            let mut network_wrapper = panic::AssertRecoverSafe::new(nw);
            let res = panic::recover(move || network_wrapper.send(vec![m]));
            assert!(res.is_ok() || !success);
        };

        // promote 0 the leader
        send(&mut nw, new_message(1, 1, MessageType::MsgHup, 0));
        send(&mut nw, new_message(1, 1, MessageType::MsgPropose, 1));

        let want_log = if success {
            new_raft_log(vec![new_entry(1, 1, None), new_entry(1, 2, SOME_DATA)],
                         3,
                         2)
        } else {
            RaftLog::new(new_storage())
        };
        let base = ltoa(&want_log);
        for (id, p) in nw.peers.iter() {
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

        let want_log = new_raft_log(vec![new_entry(1, 1, None), new_entry(1, 2, SOME_DATA)],
                                    3,
                                    2);
        let base = ltoa(&want_log);
        for (id, p) in tt.peers.iter() {
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

        let mut sm = new_test_raft(1, vec![1], 5, 1, Arc::new(store));
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
fn test_is_election_timeout() {
    let tests = vec![
        (5, 0f64, false),
        (13, 0.3, true),
        (15, 0.5, true),
        (18, 0.8, true),
        (20, 1.0, false),
    ];

    for (i, &(elapse, wprobability, round)) in tests.iter().enumerate() {
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.election_elapsed = elapse;
        let mut c = 0;
        for _ in 0..10000 {
            if sm.is_election_timeout() {
                c += 1;
            }
        }
        let mut got = c as f64 / 10000.0;
        if round {
            got = (got * 10.0 + 0.5).floor() / 10.0;
        }
        if got != wprobability {
            panic!("#{}: possibility = {}, want {}", i, got, wprobability);
        }
    }
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
#[test]
fn test_step_ignore_old_term_msg() {
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.skip_step = Some(Box::new(move || {
        panic!("step function should not be called.");
    }));
    sm.term = 2;
    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(1);
    sm.step(m).expect("");
}

// TestHandleMsgApp ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the log.
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
                                                     .map(|&(i, t)| new_entry(t, i, None))
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
        (nm(1, 1, 1, 3, Some(vec![(2, 2)])), 2, 2, false), // match entry 1, commit up to last new entry 2
        (nm(2, 2, 2, 3, None), 2, 2, false), // match entry 2, commit up to last new entry 2
        (nm(2, 2, 2, 4, None), 2, 2, false), // commit up to log.last()
    ];

    for (j, (m, w_index, w_commit, w_reject)) in tests.drain(..).enumerate() {
        let store = new_storage();
        store.wl().append(&[new_entry(1, 1, None), new_entry(2, 2, None)]).expect("");
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

// TestHandleHeartbeat ensures that the follower commits to the commit in the message.
#[test]
fn test_handle_heartbeat() {
    let commit = 2u64;
    let nw = |f, to, term, commit| {
        let mut m = new_message(f, to, MessageType::MsgAppend, 0);
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
             .append(&[new_entry(1, 1, None), new_entry(2, 2, None), new_entry(3, 3, None)])
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
         .append(&[new_entry(1, 1, None), new_entry(2, 2, None), new_entry(3, 3, None)])
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

    // A second heartbeat response with no AppResp does not re-send because we are in the wait state.
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

// TestMsgAppRespWaitReset verifies the waitReset behavior of a leader
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
    m.set_entries(RepeatedField::from_vec(vec![new_entry(0, 0, None)]));
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
        (StateRole::Candidate, 3, 3, 1, true),
    ];

    for (j, (state, i, term, vote_for, w_reject)) in tests.drain(..).enumerate() {
        let raft_log = new_raft_log(vec![new_entry(2, 1, None), new_entry(2, 2, None)], 3, 0);
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.state = state;
        sm.vote = vote_for;
        sm.raft_log = raft_log;
        let mut m = new_message(2, 0, MessageType::MsgRequestVote, 0);
        m.set_index(i);
        m.set_log_term(term);
        sm.step(m).expect("");

        let msgs = sm.read_messages();
        if msgs.len() != 1 {
            panic!("#{}: msgs count = {}, want 1", j, msgs.len());
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
        (StateRole::Follower, StateRole::Candidate, true, 1, INVALID_ID),
        (StateRole::Follower, StateRole::Leader, false, 0, INVALID_ID),
        
        (StateRole::Candidate, StateRole::Follower, true, 0, INVALID_ID),
        (StateRole::Candidate, StateRole::Candidate, true, 1, INVALID_ID),
        (StateRole::Candidate, StateRole::Leader, true, 0, 1),
        
        (StateRole::Leader, StateRole::Follower, true, 1, INVALID_ID),
        (StateRole::Leader, StateRole::Candidate, false, 1, INVALID_ID),
        (StateRole::Leader, StateRole::Leader, true, 0, 1),
    ];
    for (i, (from, to, wallow, wterm, wlead)) in tests.drain(..).enumerate() {
        let mut sm: &mut Raft<MemStorage> = &mut new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.state = from;

        let res = {
            let mut sm_wrapper = panic::AssertRecoverSafe::new(&mut sm);
            panic::recover(move || {
                match to {
                    StateRole::Follower => sm_wrapper.become_follower(wterm, wlead),
                    StateRole::Candidate => sm_wrapper.become_candidate(),
                    StateRole::Leader => sm_wrapper.become_leader(),
                }
            })
        };
        if res.is_ok() ^ wallow {
            panic!("#{}: allow = {}, want {}", i, res.is_ok(), wallow);
        }
        if res.is_err() {
            continue;
        }

        if sm.term != wterm {
            panic!("#{}: term = {}, want {}", i, sm.term, wterm);
        }
        if sm.lead != wlead {
            panic!("#{}: lead = {}, want {}", i, sm.lead, wlead);
        }
    }
}

#[test]
fn test_all_server_stepdown() {
    let mut tests = vec![
        (StateRole::Follower, StateRole::Follower, 3, 0),
        (StateRole::Candidate, StateRole::Follower, 3, 0),
        (StateRole::Leader, StateRole::Follower, 3, 1),
    ];

    let tmsg_types = vec![MessageType::MsgRequestVote, MessageType::MsgAppend];
    let tterm = 3u64;

    for (i, (state, wstate, wterm, windex)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        match state {
            StateRole::Follower => sm.become_follower(1, INVALID_ID),
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
            if sm.lead != wlead {
                panic!("{}, sm.lead = {}, want {}", i, sm.lead, INVALID_ID);
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
fn test_leader_append_response() {
    // initial progress: match = 0; next = 3
    let mut tests = vec![
        (3, true, 0, 3, 0, 0, 0), // stale resp; no replies
        (2, true, 0, 2, 1, 1, 0), // denied resp; leader does not commit; descrease next and send probing msg
        (2, false, 2, 4, 2, 2, 2), // accept resp; leader commits; broadcast with commit index
        (0, false, 0, 3, 0, 0, 0),
    ];

    for (i,
         (index, reject, wmatch, wnext, wmsg_num, windex, wcomitted)) in tests.drain(..)
                                                                              .enumerate() {
        // sm term is 1 after it becomes the leader.
        // thus the last log term must be 1 to be committed.
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        sm.raft_log = new_raft_log(vec![new_entry(0, 1, None), new_entry(1, 2, None)], 3, 0);
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
            if msg.get_commit() != wcomitted {
                panic!("#{}.{} commit = {}, want {}",
                       i,
                       j,
                       msg.get_commit(),
                       wcomitted);
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
        sm.append_entry(&mut vec![new_entry(0, i as u64 + 1, None)]);
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
        sm.raft_log = new_raft_log(vec![new_entry(0, 1, None), new_entry(1, 2, None)], 0, 0);
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
    let previous_ents = vec![new_entry(1, 1, None), new_entry(1, 2, None), new_entry(1, 3, None)];
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
        r.append_entry(&mut vec![new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        let mut msg = r.read_messages();
        assert_eq!(msg.len(), 1);
        assert_eq!(msg[0].get_index(), 0);

        assert!(r.prs[&2].paused);
        for _ in 0..10 {
            r.append_entry(&mut vec![new_entry(0, 0, SOME_DATA)]);
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
        r.append_entry(&mut vec![new_entry(0, 0, SOME_DATA)]);
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
        r.append_entry(&mut vec![new_entry(0, 0, SOME_DATA)]);
        r.send_append(2);
        assert_eq!(r.read_messages().len(), 0);
    }
}

#[test]
fn test_recv_msg_unreachable() {
    let previous_ents = vec![new_entry(1, 1, None), new_entry(1, 2, None), new_entry(1, 3, None)];
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
    let previous_ents = vec![new_entry(1, 1, None), new_entry(1, 2, None), new_entry(1, 3, None)];
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

    // TODO: port the remaining if upstream completed this test.
}
