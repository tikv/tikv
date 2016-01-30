use tikv::raft::*;
use tikv::raft::storage::MemStorage;
use std::sync::Arc;
use std::collections::HashMap;
use protobuf::RepeatedField;
use std::ops::Deref;
use std::ops::DerefMut;
use tikv::proto::raftpb::{Entry, Message, MessageType};
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

fn new_test_raft<T: Storage + Default>(id: u64,
                                        peers: Vec<u64>,
                                        election: usize,
                                        heartbeat: usize,
                                        storage: Arc<T>)
                                        -> Raft<T> {
    Raft::new(&Config {
        id: id,
        peers: peers,
        election_tick: election,
        heartbeat_tick: heartbeat,
        storage: storage,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    })
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
    Interface::new(raft)
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

fn nop_stepper() -> Interface {
    Interface { raft: None }
}

fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(t);
    if n > 0 {
        let mut ents = Vec::with_capacity(n);
        for _ in 0..n {
            ents.push(new_entry(0, 0, Some("somedata")));
        }
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
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

#[derive(Default)]
struct Network {
    peers: HashMap<u64, Interface>,
    storage: HashMap<u64, Arc<MemStorage>>,
    dropm: HashMap<Connem, f64>,
    ignorem: HashMap<MessageType, bool>,
}

impl Network {
    // newNetwork initializes a network from peers.
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
                    npeers.insert(id, Interface::new(r));
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
        self.dropm.insert(Connem {from: from, to: to}, perc);
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
            panic!("#{}: state = {:?}, want {:?}", i, p.state, ProgressState::Probe);
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

// TestProgressResume ensures that progress.maybeUpdate and progress.maybeDecrTo
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

// TestProgressResumeByHeartbeat ensures raft.heartbeat reset progress.paused by heartbeat.
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
        (Network::new(vec![None, None, Some(nop_stepper())]), StateRole::Leader),
        (Network::new(vec![None, Some(nop_stepper()), Some(nop_stepper())]), StateRole::Candidate),
        (Network::new(vec![None, Some(nop_stepper()), Some(nop_stepper()), None]), StateRole::Candidate),
        (Network::new(vec![None, Some(nop_stepper()), Some(nop_stepper()), None, None]), StateRole::Leader),
        
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
                panic!("#{}.{}: committed = {}, want {}", i, j, x.raft_log.committed, wcommitted);
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

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
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

// TestCommitWithoutNewTermEntry tests the entries could be committed
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

    let mut nt = Network::new(vec![Some(Interface::new(a)),
                                    Some(Interface::new(b)),
                                    Some(Interface::new(c))]);
    nt.cut(1, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    nt.recover();
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    let store = MemStorage::new();
    let mut e = Entry::new();
    e.set_term(1);
    e.set_index(1);
    store.wl().append(&[new_entry(1, 1, None)]).expect("");
    let wlog = RaftLog {
        store: Arc::new(store),
        committed: 1,
        unstable: Unstable { offset: 2, ..Default::default() },
        ..Default::default()
    };
    let wlog2 = RaftLog::new(new_storage());
    let tests = vec![
        (StateRole::Follower, 2, &wlog),
        (StateRole::Follower, 2, &wlog),
        (StateRole::Follower, 2, &wlog2),
    ];

    for (i, &(state, term, raft_log)) in tests.iter().enumerate() {
        let id = i as u64 + 1;
        if nt.peers[&id].state != state {
            panic!("#{}: state = {:?}, want {:?}", i, nt.peers[&id].state, state);
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

    let store = MemStorage::new();
    store.wl().append(&[new_entry(1, 1, None), new_entry(1, 2, Some(data))]).expect("");
    let want_log = ltoa(&RaftLog {
        store: Arc::new(store),
        unstable: Unstable { offset: 3, ..Default::default() },
        committed: 2,
        ..Default::default()
    });
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
