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

use super::test_raft::*;
use kvproto::eraftpb::*;
use tikv::raft::*;
use tikv::raft::storage::MemStorage;
use protobuf::RepeatedField;

pub fn hard_state(t: u64, c: u64, v: u64) -> HardState {
    let mut hs = HardState::new();
    hs.set_term(t);
    hs.set_commit(c);
    hs.set_vote(v);
    hs
}

fn commit_noop_entry(r: &mut Interface, s: &MemStorage) {
    assert_eq!(r.state, StateRole::Leader);
    r.bcast_append();
    // simulate the response of MsgAppend
    let msgs = r.read_messages();
    for m in msgs {
        assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
        assert_eq!(m.get_entries().len(), 1);
        assert!(!m.get_entries()[0].has_data());
        r.step(accept_and_reply(m)).expect("");
    }
    // ignore further messages to refresh followers' commit index
    r.read_messages();
    s.wl().append(r.raft_log.unstable_entries().unwrap_or(&[])).expect("");
    let committed = r.raft_log.committed;
    r.raft_log.applied_to(committed);
    let (last_index, last_term) = (r.raft_log.last_index(), r.raft_log.last_term());
    r.raft_log.stable_to(last_index, last_term);
}

fn accept_and_reply(m: Message) -> Message {
    assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
    let mut reply = new_message(m.get_to(), m.get_from(), MessageType::MsgAppendResponse, 0);
    reply.set_term(m.get_term());
    reply.set_index(m.get_index() + m.get_entries().len() as u64);
    reply
}

#[test]
fn test_follower_update_term_from_message() {
    test_update_term_from_message(StateRole::Follower);
}

#[test]
fn test_candidate_update_term_from_message() {
    test_update_term_from_message(StateRole::Candidate);
}

#[test]
fn test_leader_update_term_from_message() {
    test_update_term_from_message(StateRole::Leader);
}

// test_update_term_from_message tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
fn test_update_term_from_message(state: StateRole) {
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    match state {
        StateRole::Follower => r.become_follower(1, 2),
        StateRole::PreCandidate => r.become_pre_candidate(),
        StateRole::Candidate => r.become_candidate(),
        StateRole::Leader => {
            r.become_candidate();
            r.become_leader();
        }
    }

    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(2);
    r.step(m).expect("");

    assert_eq!(r.term, 2);
    assert_eq!(r.state, StateRole::Follower);
}

// test_reject_stale_term_message tests that if a server receives a request with
// a stale term number, it rejects the request.
// Our implementation ignores the request instead.
// Reference: section 5.1
#[test]
fn test_reject_stale_term_message() {
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let panic_before_step_state =
        Box::new(|_: &Message| panic!("before step state function hook called unexpectedly"));
    r.before_step_state = Some(panic_before_step_state);
    r.load_state(hard_state(2, 0, 0));

    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(r.term - 1);
    r.step(m).expect("");
}

// test_start_as_follower tests that when servers start up, they begin as followers.
// Reference: section 5.2
#[test]
fn test_start_as_follower() {
    let r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    assert_eq!(r.state, StateRole::Follower);
}

// test_leader_bcast_beat tests that if the leader receives a heartbeat tick,
// it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
// heartbeat to all followers.
// Reference: section 5.2
#[test]
fn test_leader_bcast_beat() {
    // heartbeat interval
    let hi = 1;
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, hi, new_storage());
    r.become_candidate();
    r.become_leader();
    for i in 0..10 {
        r.append_entry(&mut [empty_entry(0, i as u64 + 1)]);
    }

    for _ in 0..hi {
        r.tick();
    }

    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));

    let new_message_ext = |f, to| {
        let mut m = new_message(f, to, MessageType::MsgHeartbeat, 0);
        m.set_term(1);
        m.set_commit(0);
        m
    };

    let expect_msgs = vec![new_message_ext(1, 2), new_message_ext(1, 3)];
    assert_eq!(msgs, expect_msgs);
}

#[test]
fn test_follower_start_election() {
    test_nonleader_start_election(StateRole::Follower);
}

#[test]
fn test_candidate_start_new_election() {
    test_nonleader_start_election(StateRole::Candidate);
}

// test_nonleader_start_election tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
fn test_nonleader_start_election(state: StateRole) {
    // election timeout
    let et = 10;
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, new_storage());
    match state {
        StateRole::Follower => r.become_follower(1, 2),
        StateRole::Candidate => r.become_candidate(),
        _ => panic!("Only non-leader role is accepted."),
    }

    for _ in 1..2 * et {
        r.tick();
    }

    assert_eq!(r.term, 2);
    assert_eq!(r.state, StateRole::Candidate);
    assert!(r.votes[&r.id]);
    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));
    let new_message_ext = |f, to| {
        let mut m = new_message(f, to, MessageType::MsgRequestVote, 0);
        m.set_term(2);
        m.set_log_term(0);
        m.set_index(0);
        m
    };
    let expect_msgs = vec![new_message_ext(1, 2), new_message_ext(1, 3)];
    assert_eq!(msgs, expect_msgs);
}

// test_leader_election_in_one_round_rpc tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
#[test]
fn test_leader_election_in_one_round_rpc() {
    let mut tests =
        vec![// win the election when receiving votes from a majority of the servers
             (1, map!(), StateRole::Leader),
             (3, map!(2 => true, 3 => true), StateRole::Leader),
             (3, map!(2 => true), StateRole::Leader),
             (5, map!(2 => true, 3 => true, 4 => true, 5 => true), StateRole::Leader),
             (5, map!(2 => true, 3 => true, 4 => true), StateRole::Leader),
             (5, map!(2 => true, 3 => true), StateRole::Leader),

             // return to follower state if it receives vote denial from a majority
             (3, map!(2 => false, 3 => false), StateRole::Follower),
             (5, map!(2 => false, 3 => false, 4 => false, 5 => false), StateRole::Follower),
             (5, map!(2 => true, 3 => false, 4 => false, 5 => false), StateRole::Follower),

             // stay in candidate if it does not obtain the majority
             (3, map!(), StateRole::Candidate),
             (5, map!(2 => true), StateRole::Candidate),
             (5, map!(2 => false, 3 => false), StateRole::Candidate),
             (5, map!(), StateRole::Candidate)];

    for (i, (size, votes, state)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, (1..size as u64 + 1).collect(), 10, 1, new_storage());

        r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
        for (id, vote) in votes {
            let mut m = new_message(id, 1, MessageType::MsgRequestVoteResponse, 0);
            m.set_reject(!vote);
            r.step(m).expect("");
        }

        if r.state != state {
            panic!("#{}: state = {:?}, want {:?}", i, r.state, state);
        }
        if r.term != 1 {
            panic!("#{}: term = {}, want {}", i, r.term, 1);
        }
    }
}

// test_follower_vote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
#[test]
fn test_follower_vote() {
    let mut tests = vec![(INVALID_ID, 1, false),
                         (INVALID_ID, 2, false),
                         (1, 1, false),
                         (2, 2, false),
                         (1, 2, true),
                         (2, 1, true)];

    for (i, (vote, nvote, wreject)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        r.load_state(hard_state(1, 0, vote));

        let mut m = new_message(nvote, 1, MessageType::MsgRequestVote, 0);
        m.set_term(1);
        r.step(m).expect("");

        let msgs = r.read_messages();
        let mut m = new_message(1, nvote, MessageType::MsgRequestVoteResponse, 0);
        m.set_term(1);
        m.set_reject(wreject);
        let expect_msgs = vec![m];
        if msgs != expect_msgs {
            panic!("#{}: msgs = {:?}, want {:?}", i, msgs, expect_msgs);
        }
    }
}

// test_candidate_fallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
#[test]
fn test_candidate_fallback() {
    let new_message_ext = |f, to, term| {
        let mut m = new_message(f, to, MessageType::MsgAppend, 0);
        m.set_term(term);
        m
    };
    let mut tests = vec![new_message_ext(2, 1, 1), new_message_ext(2, 1, 2)];
    for (i, m) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
        assert_eq!(r.state, StateRole::Candidate);

        let term = m.get_term();
        r.step(m).expect("");

        if r.state != StateRole::Follower {
            panic!("#{}: state = {:?}, want {:?}",
                   i,
                   r.state,
                   StateRole::Follower);
        }
        if r.term != term {
            panic!("#{}: term = {}, want {}", i, r.term, term);
        }
    }
}

#[test]
fn test_follower_election_timeout_randomized() {
    test_non_leader_election_timeout_randomized(StateRole::Follower);
}

#[test]
fn test_candidate_election_timeout_randomized() {
    test_non_leader_election_timeout_randomized(StateRole::Candidate);
}

// test_non_leader_election_timeout_randomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
fn test_non_leader_election_timeout_randomized(state: StateRole) {
    let et = 10;
    let mut r = new_test_raft(1, vec![1, 2, 3], et, 1, new_storage());
    let mut timeouts = map!();
    for _ in 0..1000 * et {
        let term = r.term;
        match state {
            StateRole::Follower => r.become_follower(term + 1, 2),
            StateRole::Candidate => r.become_candidate(),
            _ => panic!("only non leader state is accepted!"),
        }

        let mut time = 0;
        while r.read_messages().is_empty() {
            r.tick();
            time += 1;
        }
        timeouts.insert(time, true);
    }

    assert!(timeouts.len() <= et && timeouts.len() >= et - 1);
    for d in et + 1..2 * et {
        assert!(timeouts[&d]);
    }
}

#[test]
fn test_follower_election_timeout_nonconflict() {
    test_nonleaders_election_timeout_nonconfict(StateRole::Follower);
}

#[test]
fn test_acandidates_election_timeout_nonconf() {
    test_nonleaders_election_timeout_nonconfict(StateRole::Candidate);
}

// test_nonleaders_election_timeout_nonconfict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
fn test_nonleaders_election_timeout_nonconfict(state: StateRole) {
    let et = 10;
    let size = 5;
    let mut rs = Vec::with_capacity(size);
    let ids: Vec<u64> = (1..size as u64 + 1).collect();
    for id in ids.iter().take(size) {
        rs.push(new_test_raft(*id, ids.clone(), et, 1, new_storage()));
    }
    let mut conflicts = 0;
    for _ in 0..1000 {
        for r in &mut rs {
            let term = r.term;
            match state {
                StateRole::Follower => r.become_follower(term + 1, INVALID_ID),
                StateRole::Candidate => r.become_candidate(),
                _ => panic!("non leader state is expect!"),
            }
        }

        let mut timeout_num = 0;
        while timeout_num == 0 {
            for r in &mut rs {
                r.tick();
                if !r.read_messages().is_empty() {
                    timeout_num += 1;
                }
            }
        }
        // several rafts time out at the same tick
        if timeout_num > 1 {
            conflicts += 1;
        }
    }

    assert!(conflicts as f64 / 1000.0 <= 0.3);
}

// test_leader_start_replication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
#[test]
fn test_leader_start_replication() {
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s.clone());
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r, &s);
    let li = r.raft_log.last_index();

    r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

    assert_eq!(r.raft_log.last_index(), li + 1);
    assert_eq!(r.raft_log.committed, li);
    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));
    let wents = vec![new_entry(1, li + 1, SOME_DATA)];
    let new_message_ext = |f, to, ents| {
        let mut m = new_message(f, to, MessageType::MsgAppend, 0);
        m.set_term(1);
        m.set_index(li);
        m.set_log_term(1);
        m.set_commit(li);
        m.set_entries(RepeatedField::from_vec(ents));
        m
    };
    let expect_msgs = vec![new_message_ext(1, 2, wents.clone()),
                           new_message_ext(1, 3, wents.clone())];
    assert_eq!(msgs, expect_msgs);
    assert_eq!(r.raft_log.unstable_entries(), Some(&*wents));
}

// test_leader_commit_entry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
#[test]
fn test_leader_commit_entry() {
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s.clone());
    r.become_candidate();
    r.become_leader();
    commit_noop_entry(&mut r, &s);
    let li = r.raft_log.last_index();
    r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

    for m in r.read_messages() {
        r.step(accept_and_reply(m)).expect("");
    }

    assert_eq!(r.raft_log.committed, li + 1);
    let wents = vec![new_entry(1, li + 1, SOME_DATA)];
    assert_eq!(r.raft_log.next_entries(), Some(wents));
    let mut msgs = r.read_messages();
    msgs.sort_by_key(|m| format!("{:?}", m));
    for (i, m) in msgs.drain(..).enumerate() {
        assert_eq!(i as u64 + 2, m.get_to());
        assert_eq!(m.get_msg_type(), MessageType::MsgAppend);
        assert_eq!(m.get_commit(), li + 1);
    }
}

// test_leader_acknowledge_commit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
#[test]
fn test_leader_acknowledge_commit() {
    let mut tests = vec![(1, map!(), true),
                         (3, map!(), false),
                         (3, map!(2 => true), true),
                         (3, map!(2 => true, 3 => true), true),
                         (5, map!(), false),
                         (5, map!(2 => true), false),
                         (5, map!(2 => true, 3 => true), true),
                         (5, map!(2 => true, 3 => true, 4 => true), true),
                         (5, map!(2 => true, 3 => true, 4 => true, 5 => true), true)];
    for (i, (size, acceptors, wack)) in tests.drain(..).enumerate() {
        let s = new_storage();
        let mut r = new_test_raft(1, (1..size + 1).collect(), 10, 1, s.clone());
        r.become_candidate();
        r.become_leader();
        commit_noop_entry(&mut r, &s);
        let li = r.raft_log.last_index();
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

        for m in r.read_messages() {
            if acceptors.contains_key(&m.get_to()) && acceptors[&m.get_to()] {
                r.step(accept_and_reply(m)).expect("");
            }
        }

        let g = r.raft_log.committed > li;
        if g ^ wack {
            panic!("#{}: ack commit = {}, want {}", i, g, wack);
        }
    }
}

// test_leader_commit_preceding_entries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
#[test]
fn test_leader_commit_preceding_entries() {
    let mut tests = vec![vec![],
                         vec![empty_entry(2, 1)],
                         vec![empty_entry(1, 1), empty_entry(2, 2)],
                         vec![empty_entry(1, 1)]];

    for (i, mut tt) in tests.drain(..).enumerate() {
        let s = new_storage();
        s.wl().append(&tt).expect("");
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        r.load_state(hard_state(2, 0, 0));
        r.become_candidate();
        r.become_leader();
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

        for m in r.read_messages() {
            r.step(accept_and_reply(m)).expect("");
        }

        let li = tt.len() as u64;
        tt.append(&mut vec![empty_entry(3, li + 1), new_entry(3, li + 2, SOME_DATA)]);
        let g = r.raft_log.next_entries();
        let wg = Some(tt);
        if g != wg {
            panic!("#{}: ents = {:?}, want {:?}", i, g, wg);
        }
    }
}

// test_follower_commit_entry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
#[test]
fn test_follower_commit_entry() {
    let mut tests = vec![(vec![new_entry(1, 1, SOME_DATA)], 1),
                         (vec![new_entry(1, 1, SOME_DATA), new_entry(1, 2, Some("somedata2"))], 2),
                         (vec![new_entry(1, 1, Some("somedata2")), new_entry(1, 2, SOME_DATA)], 2),
                         (vec![new_entry(1, 1, SOME_DATA), new_entry(1, 2, Some("somedata2"))], 1)];

    for (i, (ents, commit)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        r.become_follower(1, 2);

        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.set_term(1);
        m.set_commit(commit);
        m.set_entries(RepeatedField::from_vec(ents.clone()));
        r.step(m).expect("");

        if r.raft_log.committed != commit {
            panic!("#{}: committed = {}, want {}",
                   i,
                   r.raft_log.committed,
                   commit);
        }
        let wents = Some(ents[..commit as usize].to_vec());
        let g = r.raft_log.next_entries();
        if g != wents {
            panic!("#{}: next_ents = {:?}, want {:?}", i, g, wents);
        }
    }
}

// test_follower_check_msg_append tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
#[test]
fn test_follower_check_msg_append() {
    let ents = vec![empty_entry(1, 1), empty_entry(2, 2)];
    let mut tests =
        vec![// match with committed entries
             (0, 0, 1, false, 0),
             (ents[0].get_term(), ents[0].get_index(), 1, false, 0),
             // match with uncommitted entries
             (ents[1].get_term(), ents[1].get_index(), 2, false, 0),

             // unmatch with existing entry
             (ents[0].get_term(), ents[1].get_index(), ents[1].get_index(), true, 2),
             // unexisting entry
             (ents[1].get_term() + 1, ents[1].get_index() + 1, ents[1].get_index() + 1, true, 2)];
    for (i, (term, index, windex, wreject, wreject_hint)) in tests.drain(..).enumerate() {
        let s = new_storage();
        s.wl().append(&ents).expect("");
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        r.load_state(hard_state(0, 1, 0));
        r.become_follower(2, 2);

        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.set_term(2);
        m.set_log_term(term);
        m.set_index(index);
        r.step(m).expect("");

        let msgs = r.read_messages();
        let mut wm = new_message(1, 2, MessageType::MsgAppendResponse, 0);
        wm.set_term(2);
        wm.set_index(windex);
        if wreject {
            wm.set_reject(wreject);
            wm.set_reject_hint(wreject_hint);
        }
        let expect_msgs = vec![wm];
        if msgs != expect_msgs {
            panic!("#{}: msgs = {:?}, want {:?}", i, msgs, expect_msgs);
        }
    }
}

// test_follower_append_entries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
#[test]
fn test_follower_append_entries() {
    let mut tests =
        vec![(2,
              2,
              vec![empty_entry(3, 3)],
              vec![empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)],
              vec![empty_entry(3, 3)]),

             (1,
              1,
              vec![empty_entry(3, 2), empty_entry(4, 3)],
              vec![empty_entry(1, 1), empty_entry(3, 2), empty_entry(4, 3)],
              vec![empty_entry(3, 2), empty_entry(4, 3)]),

             (0, 0, vec![empty_entry(1, 1)], vec![empty_entry(1, 1), empty_entry(2, 2)], vec![]),
             (0, 0, vec![empty_entry(3, 1)], vec![empty_entry(3, 1)], vec![empty_entry(3, 1)])];
    for (i, (index, term, ents, wents, wunstable)) in tests.drain(..).enumerate() {
        let s = new_storage();
        s.wl().append(&[empty_entry(1, 1), empty_entry(2, 2)]).expect("");
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, s);
        r.become_follower(2, 2);

        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.set_term(2);
        m.set_log_term(term);
        m.set_index(index);
        m.set_entries(RepeatedField::from_vec(ents));
        r.step(m).expect("");

        let g = r.raft_log.all_entries();
        if g != wents {
            panic!("#{}: ents = {:?}, want {:?}", i, g, wents);
        }
        let g = r.raft_log.unstable_entries();
        let wunstable = if wunstable.is_empty() {
            None
        } else {
            Some(&*wunstable)
        };
        if g != wunstable {
            panic!("#{}: unstable_entries = {:?}, want {:?}", i, g, wunstable);
        }
    }
}

// test_leader_sync_follower_log tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
#[test]
fn test_leader_sync_follower_log() {
    let ents = vec![empty_entry(0, 0),
                    empty_entry(1, 1),
                    empty_entry(1, 2),
                    empty_entry(1, 3),
                    empty_entry(4, 4),
                    empty_entry(4, 5),
                    empty_entry(5, 6),
                    empty_entry(5, 7),
                    empty_entry(6, 8),
                    empty_entry(6, 9),
                    empty_entry(6, 10)];
    let term = 8u64;
    let mut tests = vec![vec![empty_entry(0, 0),
                              empty_entry(1, 1),
                              empty_entry(1, 2),
                              empty_entry(1, 3),
                              empty_entry(4, 4),
                              empty_entry(4, 5),
                              empty_entry(5, 6),
                              empty_entry(5, 7),
                              empty_entry(6, 8),
                              empty_entry(6, 9)],
                         vec![empty_entry(0, 0),
                              empty_entry(1, 1),
                              empty_entry(1, 2),
                              empty_entry(1, 3),
                              empty_entry(4, 4)],
                         vec![empty_entry(0, 0),
                              empty_entry(1, 1),
                              empty_entry(1, 2),
                              empty_entry(1, 3),
                              empty_entry(4, 4),
                              empty_entry(4, 5),
                              empty_entry(5, 6),
                              empty_entry(5, 7),
                              empty_entry(6, 8),
                              empty_entry(6, 9),
                              empty_entry(6, 10),
                              empty_entry(6, 11)],
                         vec![empty_entry(0, 0),
                              empty_entry(1, 1),
                              empty_entry(1, 2),
                              empty_entry(1, 3),
                              empty_entry(4, 4),
                              empty_entry(4, 5),
                              empty_entry(5, 6),
                              empty_entry(5, 7),
                              empty_entry(6, 8),
                              empty_entry(6, 9),
                              empty_entry(6, 10),
                              empty_entry(7, 11),
                              empty_entry(7, 12)],
                         vec![empty_entry(0, 0),
                              empty_entry(1, 1),
                              empty_entry(1, 2),
                              empty_entry(1, 3),
                              empty_entry(4, 4),
                              empty_entry(4, 5),
                              empty_entry(4, 6),
                              empty_entry(4, 7)],
                         vec![empty_entry(0, 0),
                              empty_entry(1, 1),
                              empty_entry(1, 2),
                              empty_entry(1, 3),
                              empty_entry(2, 4),
                              empty_entry(2, 5),
                              empty_entry(2, 6),
                              empty_entry(3, 7),
                              empty_entry(3, 8),
                              empty_entry(3, 9),
                              empty_entry(3, 10),
                              empty_entry(3, 11)]];
    for (i, tt) in tests.drain(..).enumerate() {
        let lead_store = new_storage();
        lead_store.wl().append(&ents).expect("");
        let mut lead = new_test_raft(1, vec![1, 2, 3], 10, 1, lead_store);
        let last_index = lead.raft_log.last_index();
        lead.load_state(hard_state(term, last_index, 0));
        let follower_store = new_storage();
        follower_store.wl().append(&tt).expect("");
        let mut follower = new_test_raft(2, vec![1, 2, 3], 10, 1, follower_store);
        follower.load_state(hard_state(term - 1, 0, 0));
        // It is necessary to have a three-node cluster.
        // The second may have more up-to-date log than the first one, so the
        // first node needs the vote from the third node to become the leader.
        let mut n = Network::new(vec![Some(lead), Some(follower), NOP_STEPPER]);
        n.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        // The election occurs in the term after the one we loaded with
        // lead.load_state above.
        let mut m = new_message(3, 1, MessageType::MsgRequestVoteResponse, 0);
        m.set_term(term + 1);
        n.send(vec![m]);

        let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
        m.set_entries(RepeatedField::from_vec(vec![Entry::new()]));
        n.send(vec![m]);
        let lead_str = ltoa(&n.peers[&1].raft_log);
        let follower_str = ltoa(&n.peers[&2].raft_log);
        if lead_str != follower_str {
            panic!("#{}: lead str: {}, follower_str: {}",
                   i,
                   lead_str,
                   follower_str);
        }
    }
}

// test_vote_request tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
#[test]
fn test_vote_request() {
    let mut tests = vec![(vec![empty_entry(1, 1)], 2),
                         (vec![empty_entry(1, 1), empty_entry(2, 2)], 3)];
    for (j, (ents, wterm)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
        m.set_term(wterm - 1);
        m.set_log_term(0);
        m.set_index(0);
        m.set_entries(RepeatedField::from_vec(ents.clone()));
        r.step(m).expect("");
        r.read_messages();

        for _ in 1..r.get_election_timeout() * 2 {
            r.tick_election();
        }

        let mut msgs = r.read_messages();
        msgs.sort_by_key(|m| format!("{:?}", m));
        if msgs.len() != 2 {
            panic!("#{}: msg count = {}, want 2", j, msgs.len());
        }
        for (i, m) in msgs.iter().enumerate() {
            if m.get_msg_type() != MessageType::MsgRequestVote {
                panic!("#{}.{}: msg_type = {:?}, want {:?}",
                       j,
                       i,
                       m.get_msg_type(),
                       MessageType::MsgRequestVote);
            }
            if m.get_to() != i as u64 + 2 {
                panic!("#{}.{}: to = {}, want {}", j, i, m.get_to(), i + 2);
            }
            if m.get_term() != wterm {
                panic!("#{}.{}: term = {}, want {}", j, i, m.get_term(), wterm);
            }
            let windex = ents.last().unwrap().get_index();
            let wlogterm = ents.last().unwrap().get_term();
            if m.get_index() != windex {
                panic!("#{}.{}: index = {}, want {}", j, i, m.get_index(), windex);
            }
            if m.get_log_term() != wlogterm {
                panic!("#{}.{}: log_term = {}, want {}",
                       j,
                       i,
                       m.get_log_term(),
                       wlogterm);
            }
        }
    }
}

// test_voter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
#[test]
fn test_voter() {
    let mut tests = vec![// same logterm
                         (vec![empty_entry(1, 1)], 1, 1, false),
                         (vec![empty_entry(1, 1)], 1, 2, false),
                         (vec![empty_entry(1, 1), empty_entry(1, 2)], 1, 1, true),
                         // candidate higher logterm
                         (vec![empty_entry(1, 1)], 2, 1, false),
                         (vec![empty_entry(1, 1)], 2, 2, false),
                         (vec![empty_entry(1, 1), empty_entry(1, 2)], 2, 1, false),
                         // voter higher logterm
                         (vec![empty_entry(2, 1)], 1, 1, true),
                         (vec![empty_entry(2, 1)], 1, 2, true),
                         (vec![empty_entry(2, 1), empty_entry(1, 2)], 1, 1, true)];
    for (i, (ents, log_term, index, wreject)) in tests.drain(..).enumerate() {
        let s = new_storage();
        s.wl().append(&ents).expect("");
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, s);

        let mut m = new_message(2, 1, MessageType::MsgRequestVote, 0);
        m.set_term(3);
        m.set_log_term(log_term);
        m.set_index(index);
        r.step(m).expect("");

        let msgs = r.read_messages();
        if msgs.len() != 1 {
            panic!("#{}: msg count = {}, want {}", i, msgs.len(), 1);
        }
        if msgs[0].get_msg_type() != MessageType::MsgRequestVoteResponse {
            panic!("#{}: msg_type = {:?}, want {:?}",
                   i,
                   msgs[0].get_msg_type(),
                   MessageType::MsgRequestVoteResponse);
        }
        if msgs[0].get_reject() != wreject {
            panic!("#{}: reject = {}, want {}",
                   i,
                   msgs[0].get_reject(),
                   wreject);
        }
    }
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
#[test]
fn test_leader_only_commits_log_from_current_term() {
    let ents = vec![empty_entry(1, 1), empty_entry(2, 2)];
    let mut tests = vec![// do not commit log entries in previous terms
                         (1, 0),
                         (2, 0),
                         // commit log in current term
                         (3, 3)];
    for (i, (index, wcommit)) in tests.drain(..).enumerate() {
        let store = new_storage();
        store.wl().append(&ents).expect("");
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, store);
        r.load_state(hard_state(2, 0, 0));
        // become leader at term 3
        r.become_candidate();
        r.become_leader();
        r.read_messages();
        // propose a entry to current term
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");

        let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
        m.set_term(r.term);
        m.set_index(index);
        r.step(m).expect("");
        if r.raft_log.committed != wcommit {
            panic!("#{}: commit = {}, want {}",
                   i,
                   r.raft_log.committed,
                   wcommit);
        }
    }
}
