
#![allow(dead_code)]
use std::cmp;
use raft::storage::{Storage, ThreadSafeStorage};
use proto::raftpb::{HardState, ConfState, Entry, Snapshot, Message, MessageType};
use protobuf::repeated::RepeatedField;
use raft::progress::{Progress, Inflights, ProgressState};
use raft::errors::{Result, Error, other, StorageError};
use std::collections::HashMap;
use raft::raft_log::RaftLog;
use std::sync::Arc;
use std::fmt;


#[derive(Debug, PartialEq, Clone, Copy)]
enum StateRole {
    Follower,
    Candidate,
    Leader,
    Invalid,
}

impl Default for StateRole {
    fn default() -> StateRole {
        StateRole::Invalid
    }
}

const INVALID_ID: u64 = 0;

/// Config contains the parameters to start a raft.
#[derive(Default)]
pub struct Config<T: Storage + Sync> {
    /// id is the identity of the local raft. ID cannot be 0.
    id: u64,

    /// peers contains the IDs of all nodes (including self) in
    /// the raft cluster. It should only be set when starting a new
    /// raft cluster.
    /// Restarting raft from previous configuration will panic if
    /// peers is set.
    /// peer is private and only used for testing right now.
    peers: Vec<u64>,

    /// ElectionTick is the election timeout. If a follower does not
    /// receive any message from the leader of current term during
    /// ElectionTick, it will become candidate and start an election.
    /// ElectionTick must be greater than HeartbeatTick. We suggest
    /// to use ElectionTick = 10 * HeartbeatTick to avoid unnecessary
    /// leader switching.
    election_tick: usize,
    /// HeartbeatTick is the heartbeat usizeerval. A leader sends heartbeat
    /// message to mausizeain the leadership every heartbeat usizeerval.
    heartbeat_tick: usize,

    /// Storage is the storage for raft. raft generates entires and
    /// states to be stored in storage. raft reads the persisted entires
    /// and states out of Storage when it needs. raft reads out the previous
    /// state and configuration out of storage when restarting.
    storage: Option<Arc<ThreadSafeStorage<T>>>,
    /// Applied is the last applied index. It should only be set when restarting
    /// raft. raft will not return entries to the application smaller or equal to Applied.
    /// If Applied is unset when restarting, raft might return previous applied entries.
    /// This is a very application dependent configuration.
    applied: u64,

    /// MaxSizePerMsg limits the max size of each append message. Smaller value lowers
    /// the raft recovery cost(initial probing and message lost during normal operation).
    /// On the other side, it might affect the throughput during normal replication.
    /// Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    max_size_per_msg: u64,
    /// max_inflight_msgs limits the max number of in-flight append messages during optimistic
    /// replication phase. The application transportation layer usually has its own sending
    /// buffer over TCP/UDP. Setting MaxInflightMsgs to avoid overflowing that sending buffer.
    /// TODO (xiangli): feedback to application to limit the proposal rate?
    max_inflight_msgs: usize,

    /// check_quorum specifies if the leader should check quorum activity. Leader steps down when
    /// quorum is not active for an electionTimeout.
    check_quorum: bool,
}

impl<T: Storage + Sync> Config<T> {
    pub fn validate(&self) -> Result<()> {
        if self.id == INVALID_ID {
            return Err(other("invalid node id"));
        }

        if self.heartbeat_tick <= 0 {
            return Err(other("heartbeat tick must greater than 0"));
        }

        if self.election_tick <= self.heartbeat_tick {
            return Err(other("election tick must be greater than heartbeat tick"));
        }

        if self.storage.is_none() {
            return Err(other("storage should be specified"));
        }

        if self.max_inflight_msgs <= 0 {
            return Err(other("max inflight messages must be greater than 0"));
        }

        Ok(())
    }
}

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq)]
struct SoftState {
    lead: u64,
    raft_state: StateRole,
}

struct Raft<T: Storage + Sync> {
    hs: HardState,

    id: u64,

    /// the log
    raft_log: RaftLog<T>,

    max_inflight: usize,
    max_msg_size: u64,
    prs: HashMap<u64, Progress>,

    state: StateRole,

    votes: HashMap<u64, bool>,

    msgs: Vec<Message>,

    /// the leader id
    lead: u64,

    /// New configuration is ignored if there exists unapplied configuration.
    pending_conf: bool,

    /// number of ticks since it reached last electionTimeout when it is leader
    /// or candidate.
    /// number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    election_elapsed: usize,

    /// number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,

    check_quorum: bool,

    heartbeat_timeout: usize,
    election_timeout: usize,
}

impl<T: Storage + Sync> Raft<T> {
    fn new(c: &Config<T>) -> Raft<T> {
        c.validate().expect("configuration is invalid");
        let store = c.storage.as_ref().unwrap().clone();
        let rs = (**store).initial_state().expect("");
        let mut raft_log = RaftLog::new(store);
        let mut peers = &*c.peers;
        if rs.conf_state.get_nodes().len() > 0 {
            if peers.len() > 0 {
                // TODO(bdarnell): the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snap
                panic!("cannot specify both new(peers) and ConfState.Nodes")
            }
            peers = rs.conf_state.get_nodes();
        }
        let mut r = Raft {
            hs: HardState::new(),
            id: c.id,
            raft_log: raft_log,
            max_inflight: c.max_inflight_msgs,
            max_msg_size: c.max_size_per_msg,
            prs: HashMap::with_capacity(peers.len()),
            state: StateRole::Invalid,
            votes: HashMap::new(),
            msgs: vec![],
            lead: INVALID_ID,
            pending_conf: false,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            check_quorum: c.check_quorum,
            heartbeat_timeout: c.heartbeat_tick,
            election_timeout: c.election_tick,
        };
        for p in peers {
            r.prs.insert(*p,
                         Progress {
                             next_idx: 1,
                             ins: Inflights::new(r.max_inflight),
                             ..Default::default()
                         });
        }
        if rs.hard_state != HardState::new() {
            r.load_state(rs.hard_state);
        }
        if c.applied > 0 {
            r.raft_log.applied_to(c.applied);
        }
        r.become_follower(r.hs.get_term(), INVALID_ID);

        let mut nodes_str = String::new();
        for n in r.nodes() {
            nodes_str = nodes_str + &format!("{:?}", n);
            nodes_str = nodes_str + ",";
        }
        let nodes_str_len = nodes_str.len();
        nodes_str.remove(nodes_str_len - 1);
        info!("newRaft {:x} [peers: [{}], term: {:?}, commit: {}, applied: {}, last_index: {}, \
               last_term: {}]",
              r.id,
              nodes_str,
              r.hs.get_term(),
              r.raft_log.get_committed(),
              r.raft_log.get_applied(),
              r.raft_log.last_index(),
              r.raft_log.last_term());
        r
    }

    fn has_leader(&self) -> bool {
        self.lead != INVALID_ID
    }

    fn soft_state(&self) -> SoftState {
        SoftState {
            lead: self.lead,
            raft_state: self.state,
        }
    }

    fn quorum(&self) -> usize {
        self.prs.len() / 2 + 1
    }

    fn nodes(&self) -> Vec<u64> {
        let mut nodes = Vec::with_capacity(self.prs.len());
        nodes.extend(self.prs.keys());
        nodes.sort();
        nodes
    }

    // send persists state to stable storage and then sends to its mailbox.
    fn send(&mut self, m: Message) {
        let mut m = m;
        m.set_from(self.id);
        // do not attach term to MsgPropose
        // proposals are a way to forward to the leader and
        // should be treated as local message.
        if m.get_field_type() != MessageType::MsgPropose {
            m.set_term(self.hs.get_term());
        }
        self.msgs.push(m);
    }

    fn prepare_send_snapshot(&mut self, m: &mut Message, to: u64) {
        let pr = self.prs.get_mut(&to).unwrap();
        if !pr.recent_active {
            debug!("ignore sending snapshot to {:x} since it is not recently active",
                   to);
            return;
        }

        m.set_field_type(MessageType::MsgSnapshot);
        let snapshot_r = self.raft_log.snapshot();
        if let Err(e) = snapshot_r {
            if e == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!("{:x} failed to send snapshot to {:x} because snapshot is termporarily \
                        unavailable",
                       self.id,
                       to);
                return;
            }
            panic!(e);
        }
        let snapshot = snapshot_r.unwrap();
        if snapshot.get_metadata().get_index() == 0 {
            panic!("need non-empty snapshot");
        }
        let (sindex, sterm) = (snapshot.get_metadata().get_index(),
                               snapshot.get_metadata().get_term());
        m.set_snapshot(snapshot.clone());
        debug!("{:x} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {:x} \
                [{:?}]",
               self.id,
               self.raft_log.first_index(),
               self.hs.get_commit(),
               sindex,
               sterm,
               to,
               pr);
        pr.become_snapshot(sindex);
        debug!("{:x} paused sending replication messages to {:x} [{:?}]",
               self.id,
               to,
               pr);
    }

    fn prepare_send_entries(&mut self, m: &mut Message, to: u64, term: u64, ents: Vec<Entry>) {
        let pr = self.prs.get_mut(&to).unwrap();
        m.set_field_type(MessageType::MsgAppend);
        m.set_index(pr.next_idx - 1);
        m.set_logTerm(term);
        m.set_entries(RepeatedField::from_vec(ents));
        m.set_commit(self.raft_log.get_committed());
        if m.get_entries().len() != 0 {
            match pr.state {
                ProgressState::Replicate => {
                    let last = m.get_entries().last().unwrap().get_Index();
                    pr.optimistic_update(last);
                    pr.ins.add(last);
                }
                ProgressState::Probe => pr.pause(),
                _ => {
                    panic!("{:x} is sending append in unhandled state {:?}",
                           self.id,
                           pr.state)
                }
            }
        }
    }

    // send_append sends RPC, with entries to the given peer.
    fn send_append(&mut self, to: u64) {
        let (term, ents) = {
            let pr = self.prs.get(&to).unwrap();
            if pr.is_paused() {
                return;
            }
            (self.raft_log.term(pr.next_idx - 1),
             self.raft_log.entries(pr.next_idx, self.max_msg_size))
        };
        let mut m = Message::new();
        m.set_to(to);
        if term.is_err() || ents.is_err() {
            // send snapshot if we failed to get term or entries
            self.prepare_send_snapshot(&mut m, to);
        } else {
            self.prepare_send_entries(&mut m, to, term.unwrap(), ents.unwrap());
        }
        self.send(m);
    }

    // sendHeartbeat sends an empty MsgApp
    fn send_heartbeat(&mut self, to: u64) {
        // Attach the commit as min(to.matched, r.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        let commit = cmp::min(self.prs.get(&to).unwrap().matched,
                              self.raft_log.get_committed());
        let mut m = Message::new();
        m.set_to(to);
        m.set_field_type(MessageType::MsgHeartbeat);
        m.set_commit(commit);
        self.send(m);
    }

    // bcastAppend sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    fn bcast_append(&mut self) {
        let keys: Vec<u64> = self.prs.keys().map(|x| *x).collect();
        for id in keys {
            if id == self.id {
                continue;
            }
            self.send_append(id);
        }
    }

    fn become_follower(&self, term: u64, lead: u64) {}

    fn load_state(&self, hs: HardState) {}
}
