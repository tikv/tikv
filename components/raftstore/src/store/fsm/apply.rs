use engine::rocks::Snapshot;
use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb::{Peer as PeerMeta, Region};
use kvproto::raft_serverpb::{
    MergeState, RaftApplyState, RaftTruncatedState,
};
use raft::eraftpb::ConfChange;
use std::collections::VecDeque;

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub conf_change: ConfChange,
    pub peer: PeerMeta,
    pub region: Region,
}

#[derive(Debug)]
pub struct Range {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Range {
    pub fn new(cf: String, start_key: Vec<u8>, end_key: Vec<u8>) -> Range {
        Range {
            cf,
            start_key,
            end_key,
        }
    }
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer(ChangePeer),
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
    },
    SplitRegion {
        regions: Vec<Region>,
        derived: Region,
    },
    PrepareMerge {
        region: Region,
        state: MergeState,
    },
    CommitMerge {
        region: Region,
        source: Region,
    },
    RollbackMerge {
        region: Region,
        commit: u64,
    },
    ComputeHash {
        region: Region,
        index: u64,
        snap: Snapshot,
    },
    VerifyHash {
        index: u64,
        hash: Vec<u8>,
    },
    DeleteRange {
        ranges: Vec<Range>,
    },
    IngestSST {
        ssts: Vec<SSTMeta>,
    },
}

#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: i64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub written_bytes: u64,
    pub written_keys: u64,
    pub lock_cf_written_bytes: u64,
}

#[derive(Debug)]
pub struct ApplyRes {
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub exec_res: VecDeque<ExecResult>,
    pub metrics: ApplyMetrics,
    pub merged: bool,
}

#[derive(Debug)]
pub enum TaskRes {
    Apply(ApplyRes),
    Destroy {
        // ID of region that has been destroyed.
        region_id: u64,
        // ID of peer that has been destroyed.
        peer_id: u64,
    },
}

