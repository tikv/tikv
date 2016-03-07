#![allow(dead_code)]
use proto::raftpb::HardState;
use raft::raft::{Raft, SoftState, StateRole};
use raft::storage::Storage;
use raft::progress::Progress;
use std::collections::HashMap;

#[derive(Default)]
pub struct Status {
    id: u64,
    pub hs: HardState,
    ss: SoftState,
    applied: u64,
    progress: HashMap<u64, Progress>,
}

impl Status {
    // new gets a copy of the current raft status.
    pub fn new<T: Storage>(raft: &Raft<T>) -> Status {
        let mut s = Status { id: raft.id, ..Default::default() };
        s.hs = raft.hard_state();
        s.ss = raft.soft_state();
        s.applied = raft.raft_log.get_applied();
        if s.ss.raft_state == StateRole::Leader {
            s.progress = raft.prs.clone();
        }
        s
    }
}
