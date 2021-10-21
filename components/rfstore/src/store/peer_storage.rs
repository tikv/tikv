// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::*;
use kvproto::*;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use super::keys::raft_state_key;
use super::*;
use bytes::Buf;
use futures::channel::mpsc::UnboundedSender;
use kvenginepb::Snapshot;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftLocalState;
use raft_proto::eraftpb;
use rfengine;
use tokio::sync::mpsc;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCELLING: usize = 2;
pub const JOB_STATUS_CANCELLED: usize = 3;
pub const JOB_STATUS_FINISHED: usize = 4;
pub const JOB_STATUS_FAILED: usize = 5;

/// Possible status returned by `check_applying_snap`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CheckApplyingSnapStatus {
    /// A snapshot is just applied.
    Success,
    /// A snapshot is being applied.
    Applying,
    /// No snapshot is being applied at all or the snapshot is canceled
    Idle,
}

#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating {
        canceled: Arc<AtomicBool>,
        index: Arc<AtomicU64>,
        receiver: Receiver<Snapshot>,
    },
    Applying(Arc<AtomicUsize>),
    ApplyAborted,
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax)
            | (&SnapState::ApplyAborted, &SnapState::ApplyAborted)
            | (&SnapState::Generating { .. }, &SnapState::Generating { .. }) => true,
            (&SnapState::Applying(ref b1), &SnapState::Applying(ref b2)) => {
                b1.load(Ordering::Relaxed) == b2.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }
}

// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub(crate) const RAFT_INIT_LOG_TERM: u64 = 5;
pub(crate) const RAFT_INIT_LOG_INDEX: u64 = 5;

// compact_raft_log discards all log entries prior to compact_index. We must guarantee
// that the compact_index is not greater than applied index.
pub(crate) fn compact_raft_log(
    state: RaftApplyState,
    compact_index: u64,
    compact_term: u64,
) -> Result<()> {
    todo!()
}

pub(crate) struct ReadyApplySnapshot {
    // prev_region is the region before snapshot applied
    pub(crate) prev_region: metapb::Region,
    pub(crate) region: metapb::Region,
    pub(crate) snap_data: SnapData,
}

pub(crate) struct InvokeContext {
    pub(crate) region: metapb::Region,
    pub(crate) raft_state: RaftState,
    pub(crate) apply_state: RaftApplyState,
    pub(crate) last_term: u64,
    pub(crate) snap_data: SnapData,
}

pub(crate) struct PeerStorage {
    pub(crate) engines: Engines,

    pub(crate) peer: metapb::Peer,
    region: metapb::Region,
    raft_state: RaftState,
    apply_state: RaftApplyState,
    last_term: u64,

    snap_state: RefCell<SnapState>,
    region_sched: mpsc::Sender<RegionTask>,
    snap_tried_cnt: usize,

    // The ApplyState that is persisted to L0 file.
    stable_apply_state: RaftApplyState,
    split_stage: kvenginepb::SplitStage,
    initial_flushed: bool,
    shard_meta: kvengine::ShardMeta,

    pub tag: String,
}

impl raft::Storage for PeerStorage {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        todo!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<eraftpb::Entry>> {
        todo!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        todo!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<eraftpb::Snapshot> {
        todo!()
    }
}

impl PeerStorage {
    pub(crate) fn new(
        engines: Engines,
        region: &metapb::Region,
        region_sched: UnboundedSender<RegionTask>,
        peer_id: u64,
        tag: String,
    ) -> Result<PeerStorage> {
        todo!()
    }

    pub(crate) fn clear_meta(&self, rwb: &mut rfengine::WriteBatch) {
        clear_meta(&self.engines.raft, rwb, &self.region);
    }

    pub(crate) fn get_region_id(&self) -> u64 {
        self.region.get_id()
    }

    pub(crate) fn is_initialized(&self) -> bool {
        todo!()
    }

    pub(crate) fn clear_data(&self) -> Result<()> {
        todo!()
    }

    #[inline]
    pub fn first_index(&self) -> u64 {
        self.apply_state.truncated_index + 1
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_state.last_index
    }

    #[inline]
    pub fn last_term(&self) -> u64 {
        self.last_term
    }

    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.applied_index
    }

    #[inline]
    pub fn set_applied_state(&mut self, apply_state: RaftApplyState) {
        self.apply_state = apply_state;
    }

    #[inline]
    pub fn set_applied_term(&mut self, applied_index_term: u64) {
        self.apply_state.applied_index_term = applied_index_term;
    }

    #[inline]
    pub fn apply_state(&self) -> RaftApplyState {
        self.apply_state
    }

    #[inline]
    pub fn applied_index_term(&self) -> u64 {
        self.apply_state.applied_index_term
    }

    #[inline]
    pub fn commit_index(&self) -> u64 {
        self.raft_state.commit
    }

    #[inline]
    pub fn set_commit_index(&mut self, commit: u64) {
        assert!(commit >= self.commit_index());
        self.raft_state.commit = commit;
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.apply_state.truncated_index
    }

    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.apply_state.truncated_index_term
    }

    pub fn region(&self) -> &metapb::Region {
        &self.region
    }

    pub fn set_region(&mut self, region: metapb::Region) {
        self.region = region;
    }

    #[inline]
    pub fn is_applying_snapshot(&self) -> bool {
        matches!(*self.snap_state.borrow(), SnapState::Applying(_))
    }

    /// Check if the storage is applying a snapshot.
    #[inline]
    pub fn check_applying_snap(&mut self) -> CheckApplyingSnapStatus {
        let mut res = CheckApplyingSnapStatus::Idle;
        let new_state = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                let s = status.load(Ordering::Relaxed);
                if s == JOB_STATUS_FINISHED {
                    res = CheckApplyingSnapStatus::Success;
                    SnapState::Relax
                } else if s == JOB_STATUS_CANCELLED {
                    SnapState::ApplyAborted
                } else if s == JOB_STATUS_FAILED {
                    // TODO: cleanup region and treat it as tombstone.
                    panic!("{} applying snapshot failed", self.get_region_id());
                } else {
                    return CheckApplyingSnapStatus::Applying;
                }
            }
            _ => return res,
        };
        *self.snap_state.borrow_mut() = new_state;
        res
    }

    /// Cancel applying snapshot, return true if the job can be considered not be run again.
    pub fn cancel_applying_snap(&mut self) -> bool {
        let is_canceled = match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                if status
                    .compare_exchange(
                        JOB_STATUS_PENDING,
                        JOB_STATUS_CANCELLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    true
                } else if status
                    .compare_exchange(
                        JOB_STATUS_RUNNING,
                        JOB_STATUS_CANCELLING,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    return false;
                } else {
                    false
                }
            }
            _ => return false,
        };
        if is_canceled {
            *self.snap_state.borrow_mut() = SnapState::ApplyAborted;
            return true;
        }
        // now status can only be JOB_STATUS_CANCELLING, JOB_STATUS_CANCELLED,
        // JOB_STATUS_FAILED and JOB_STATUS_FINISHED.
        self.check_applying_snap() != CheckApplyingSnapStatus::Applying
    }
}

fn init_raft_state(
    raft_engine: &mut rfengine::RFEngine,
    region: &metapb::Region,
) -> Result<RaftState> {
    let mut rs = RaftState::default();
    let rs_key = raft_state_key(region.id);
    let rs_val = raft_engine.get_state(region.id, rs_key.chunk());
    if let Some(val) = rs_val {
        if region.peers.len() > 0 {
            // new split region.
            rs.last_index = RAFT_INIT_LOG_INDEX;
            rs.term = RAFT_INIT_LOG_TERM;
            rs.commit = RAFT_INIT_LOG_INDEX;
            let mut wb = rfengine::WriteBatch::new();
            wb.set_state(region.id, rs_key.chunk(), rs.marshal().chunk());
            raft_engine.write(&wb)?;
        }
    }
    Ok(rs)
}

/// Delete all meta belong to the region. Results are stored in `wb`.
pub fn clear_meta(
    raft: &rfengine::RFEngine,
    raft_wb: &mut rfengine::WriteBatch,
    region: &metapb::Region,
) {
    todo!()
}

pub fn write_peer_state(
    raft_wb: &mut rfengine::WriteBatch,
    region: &metapb::Region,
    state: raft_serverpb::PeerState,
    merge_state: Option<raft_serverpb::MergeState>,
) {
    todo!()
}

pub(crate) struct SnapData {}
