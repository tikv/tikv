use raft::raftpb::*;
use std::{result, error};
use std::{io, fmt, ops};

pub type Result<T> = result::Result<T, error::Error>;

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
    fn snapshot() -> Box<Snapshot>;
}
