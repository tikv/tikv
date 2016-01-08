use raft::raftpb::*;
use raft::errors::Result;


#[derive(Debug, Clone)]
pub struct RaftState {
    hard_state: HardState,
    conf_state: ConfState,
}

pub trait Storage {
    fn initial_state() -> Result<RaftState>;
    fn entries(low: u64, high: u64, max_size: u64) -> Vec<Entry>;
    fn term(idx: u64) -> Result<u64>;
    fn first_index() -> Result<u64>;
    fn last_index() -> Result<u64>;
    fn snapshot() -> Result<Snapshot>;
}

pub struct MemStorage {
    hard_state: HardState,
    snapshot: Snapshot,
    entries: Vec<Entry>,
}

// pub fn new() -> MemStorage {
// MemStorage { entries: Vec::new() }
// }
//

impl Storage for MemStorage {
    fn initial_state() -> Result<RaftState> {
        None
    }

    fn entries(low: u64, high: u64, max_size: u64) -> Vec<Entry> {
        None
    }

    fn term(idx: u64) -> Result<u64> {
        None
    }

    fn first_index() -> Result<u64> {
        None
    }

    fn last_index() -> Result<u64> {
        None
    }

    fn snapshot() -> Result<Snapshot> {
        None
    }
}
