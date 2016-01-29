#![allow(dead_code)]
use proto::raftpb::HardState;
use raft::raft::{Raft, SoftState, StateRole};
use raft::storage::Storage;
use raft::progress::Progress;
use std::collections::HashMap;

#[derive(Default)]
pub struct Status {
    id: u64,
    hs: HardState,
    ss: SoftState,
    applied: u64,
    progress: HashMap<u64, Progress>,
}

impl Status {
    // new gets a copy of the current raft status.
    pub fn new<T: Storage + Default>(raft: &Raft<T>) -> Status {
        let mut s = Status { id: raft.id, ..Default::default() };
        s.hs = raft.hs.clone();
        s.ss = raft.soft_state();
        s.applied = raft.raft_log.get_applied();
        if s.ss.raft_state == StateRole::Leader {
            s.progress = raft.prs.clone();
        }
        s
    }
}
