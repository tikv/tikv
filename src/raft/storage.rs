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

impl MemStorage {
    pub fn new() -> MemStorage {
	MemStorage { 
	    entries: vec![Entry::new()], 
	    hard_state:HardState::new(),
	    snapshot:Snapshot::new(),
	}
    }
}


impl Storage for MemStorage {
    fn initial_state() -> Result<RaftState> {
	unimplemented!()
    }

    fn entries(low: u64, high: u64, max_size: u64) -> Vec<Entry> {
	unimplemented!()
    }

    fn term(idx: u64) -> Result<u64> {
	unimplemented!()
    }

    fn first_index() -> Result<u64> {
	unimplemented!()
    }

    fn last_index() -> Result<u64> {
	unimplemented!()
    }

    fn snapshot() -> Result<Snapshot> {
	unimplemented!()
    }
}
