
#![allow(dead_code)]

use std::cmp;
use raft::storage::Storage;
use proto::raftpb::{HardState, ConfState, Entry, Snapshot, Message};
use raft::progress::{Progress, Inflights};
use raft::errors::{Result, Error, other};
use std::collections::HashMap;
use raft::raft_log::RaftLog;


#[derive(Debug, PartialEq)]
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

// Config contains the parameters to start a raft.
#[derive(Debug, Default)]
pub struct Config<T>
    where T: Storage
{
    // id is the identity of the local raft. ID cannot be 0.
    id: u64,

    // peers contains the IDs of all nodes (including self) in
    // the raft cluster. It should only be set when starting a new
    // raft cluster.
    // Restarting raft from previous configuration will panic if
    // peers is set.
    // peer is private and only used for testing right now.
    peers: Vec<u64>,

    // ElectionTick is the election timeout. If a follower does not
    // receive any message from the leader of current term during
    // ElectionTick, it will become candidate and start an election.
    // ElectionTick must be greater than HeartbeatTick. We suggest
    // to use ElectionTick = 10 * HeartbeatTick to avoid unnecessary
    // leader switching.
    election_tick: usize,
    // HeartbeatTick is the heartbeat usizeerval. A leader sends heartbeat
    // message to mausizeain the leadership every heartbeat usizeerval.
    heartbeat_tick: usize,

    // Storage is the storage for raft. raft generates entires and
    // states to be stored in storage. raft reads the persisted entires
    // and states out of Storage when it needs. raft reads out the previous
    // state and configuration out of storage when restarting.
    storage: T,
    // Applied is the last applied index. It should only be set when restarting
    // raft. raft will not return entries to the application smaller or equal to Applied.
    // If Applied is unset when restarting, raft might return previous applied entries.
    // This is a very application dependent configuration.
    applied: u64,

    // MaxSizePerMsg limits the max size of each append message. Smaller value lowers
    // the raft recovery cost(initial probing and message lost during normal operation).
    // On the other side, it might affect the throughput during normal replication.
    // Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    max_size_per_msg: u64,
    // max_inflights_msgs limits the max number of in-flight append messages during optimistic
    // replication phase. The application transportation layer usually has its own sending
    // buffer over TCP/UDP. Setting MaxInflightMsgs to avoid overflowing that sending buffer.
    // TODO (xiangli): feedback to application to limit the proposal rate?
    max_inflights_msgs: usize,

    // check_quorum specifies if the leader should check quorum activity. Leader steps down when
    // quorum is not active for an electionTimeout.
    check_quorum: bool,
}

impl<T> Config<T> where T: Storage
{
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

        if self.max_inflights_msgs <= 0 {
            return Err(other("max inflight messages must be greater than 0"));
        }

        Ok(())
    }
}



#[derive(Default)]
struct Raft {
    hs: HardState,

    id: u64,

    max_inflight: usize,
    max_msg_size: u64,
    prs: HashMap<u64, Progress>,

    state: StateRole,

    votes: HashMap<u64, bool>,

    msgs: Vec<Message>,

    // the leader id
    lead: u64,

    // New configuration is ignored if there exists unapplied configuration.
    pending_conf: bool,

    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    election_elapsed: usize,

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    heartbeat_elapsed: usize,

    check_quorum: bool,

    heartbeat_timeout: usize,
    election_timeout: usize,
    tick: Vec<Box<FnMut()>>,
    step: Vec<Box<FnMut(&Message)>>,
}

impl Raft {
    fn newRaft<T>(c: &Config<T>) -> Raft
        where T: Storage
    {
        c.validate().map_err(|e| panic!(e));
        let mut raftlog = RaftLog::new(c.storage);
        let state = &c.storage.initial_state();
        state.map_err(|e| panic!(e));
        let hs = &state.unwrap().hard_state;
        let cs = &state.unwrap().conf_state;
        let mut peers = c.peers.into_iter().collect::<Vec<u64>>();
        if cs.get_nodes().len() > 0 {
            if peers.len() > 0 {
                panic!("cannot specify both newRaft(peers) and conf_state.nodes")
            }
            peers = cs.get_nodes().into_iter().map(|&x| x).collect::<Vec<u64>>();
        }

        let mut tmp: HashMap<u64, Progress> = HashMap::new();
        for id in peers {
            tmp.insert(id,
                       Progress {
                           next_idx: 1,
                           ins: Inflights::new(c.max_inflights_msgs),
                           ..Default::default()
                       });
        }
        // empty state
        if hs == &HardState::new() {

        }
        Raft {
            id: c.id,
            lead: INVALID_ID,
            max_msg_size: c.max_size_per_msg,
            max_inflight: c.max_inflights_msgs,
            election_timeout: c.election_tick,
            heartbeat_timeout: c.heartbeat_tick,
            check_quorum: c.check_quorum,
            prs: tmp,
            tick: Vec::new(),
            step: Vec::new(),
            ..Default::default()
        }
    }

    pub fn do_nothing(&self) {}
}
