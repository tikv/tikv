// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;

use collections::HashMap;
use kvproto::{metapb::Region, raft_serverpb::RegionLocalState};
use raftstore::store::fsm::apply::NewSplitPeer;

use crate::operation::CommittedEntries;

#[derive(Debug)]
pub enum ApplyTask {
    CommittedEntries(CommittedEntries),
}

#[derive(Debug, Default)]
pub struct ApplyRes {
    pub applied_index: u64,
    pub applied_term: u64,
    pub region_state: Option<RegionLocalState>,
    pub exec_res: VecDeque<ExecResult>,
}

#[derive(Debug)]
pub enum ExecResult {
    SplitRegion {
        regions: Vec<Region>,
        derived: Region,
        new_split_regions: HashMap<u64, NewSplitPeer>,
    },
}
