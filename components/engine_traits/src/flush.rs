// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A helper class to detect flush event and trace apply index.
//!
//! The whole idea is when all CFs have flushed to disk, then the apply index
//! should be able to be advanced to the latest. The implementations depends on
//! the assumption that memtable/write buffer is frozen one by one and flushed
//! one by one.
//!
//! Because apply index can be arbitrary value after restart, so apply related
//! states like `RaftApplyState` and `RegionLocalState` are mapped to index.
//! Once apply index is confirmed, the latest states before apply index should
//! be used as the start state.

use std::{
    mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState};
use tikv_util::Either;

use crate::{RaftEngine, RaftLogBatch};

#[derive(Debug)]
enum StateChange {
    ApplyState(RaftApplyState),
    RegionState(RegionLocalState),
}

/// States that is related to apply progress.
#[derive(Default, Debug)]
struct StateChanges {
    /// apply index, state change
    changes: Vec<(u64, StateChange)>,
}

struct FlushProgress {
    cf: String,
    id: u64,
    apply_index: u64,
    state_changes: StateChanges,
}

/// A share state between raftstore and underlying engine.
///
/// raftstore will update state changes and corresponding apply index, when
/// flush, `PersistenceListener` will query states related to the memtable
/// and persist the relation to raft engine.
#[derive(Default, Debug)]
pub struct FlushState {
    applied_index: AtomicU64,
    changes: Mutex<StateChanges>,
}

impl FlushState {
    /// Set the latest applied index.
    #[inline]
    pub fn set_applied_index(&self, index: u64) {
        self.applied_index.store(index, Ordering::Release);
    }

    /// Query the applied index.
    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Record an apply state change.
    ///
    /// This can be triggered by admin command like compact log. General log
    /// apply will not trigger the change, instead they are recorded by
    /// `set_applied_index`.
    #[inline]
    pub fn update_apply_state(&self, index: u64, state: RaftApplyState) {
        self.changes
            .lock()
            .unwrap()
            .changes
            .push((index, StateChange::ApplyState(state)));
    }

    /// Record a region state change.
    ///
    /// This can be triggered by admin command like split/merge.
    #[inline]
    pub fn update_region_state(&self, index: u64, state: RegionLocalState) {
        self.changes
            .lock()
            .unwrap()
            .changes
            .push((index, StateChange::RegionState(state)));
    }

    /// Check if there is any state change.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.changes.lock().unwrap().changes.is_empty()
    }

    /// Get the last changed state.
    #[inline]
    pub fn last_state(&self) -> Option<(u64, Either<RaftApplyState, RegionLocalState>)> {
        let changes = self.changes.lock().unwrap();
        let (index, state) = changes.changes.last()?;
        let state = match state {
            StateChange::ApplyState(state) => Either::Left(state.clone()),
            StateChange::RegionState(state) => Either::Right(state.clone()),
        };
        Some((*index, state))
    }
}

/// A flush listener that maps memtable to apply index and persist the relation
/// to raft engine.
pub struct PersistenceListener<ER> {
    region_id: u64,
    tablet_index: u64,
    state: Arc<FlushState>,
    progress: Mutex<Vec<FlushProgress>>,
    raft: ER,
}

impl<ER: RaftEngine> PersistenceListener<ER> {
    pub fn new(region_id: u64, tablet_index: u64, state: Arc<FlushState>, raft: ER) -> Self {
        Self {
            region_id,
            tablet_index,
            state,
            progress: Mutex::new(Vec::new()),
            raft,
        }
    }
}

impl<ER: RaftEngine> PersistenceListener<ER> {
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.state
    }

    /// Called when memtable is frozen.
    ///
    /// `id` should be unique between memtables, which is used to identify
    /// memtable in the flushed event.
    pub fn on_memtable_sealed(&self, cf: String, id: u64) {
        // The correctness relies on the assumption that there will be only one
        // thread writting to the DB and increasing apply index.
        let mut state_changes = self.state.changes.lock().unwrap();
        // Query within lock so it's correct even in manually flush.
        let apply_index = self.state.applied_index.load(Ordering::SeqCst);
        let changes = mem::take(&mut *state_changes);
        drop(state_changes);
        self.progress.lock().unwrap().push(FlushProgress {
            cf,
            id,
            apply_index,
            state_changes: changes,
        });
    }

    /// Called a memtable finished flushing.
    pub fn on_flush_completed(&self, cf: &str, id: u64) {
        // Maybe we should hook the compaction to avoid the file is compacted before
        // being recorded.
        let pr = {
            let mut prs = self.progress.lock().unwrap();
            let pos = prs
                .iter()
                .position(|pr| pr.cf == cf && pr.id == id)
                .unwrap();
            prs.swap_remove(pos)
        };
        let mut batch = self.raft.log_batch(1);
        // TODO: It's possible that flush succeeds but fails to call
        // `on_flush_completed` before exit. In this case the flushed data will
        // be replayed again after restarted. To solve the problem, we need to
        // (1) persist flushed file numbers in `on_flush_begin` and (2) check
        // the file number in `on_compaction_begin`. After restart, (3) check if the
        // file exists. If (1) && ((2) || (3)), then we don't need to replay the data.
        for (index, change) in pr.state_changes.changes {
            match &change {
                StateChange::ApplyState(state) => {
                    batch.put_apply_state(self.region_id, index, state).unwrap();
                }
                StateChange::RegionState(state) => {
                    batch
                        .put_region_state(self.region_id, index, state)
                        .unwrap();
                }
            }
        }
        if pr.apply_index != 0 {
            batch
                .put_flushed_index(self.region_id, cf, self.tablet_index, pr.apply_index)
                .unwrap();
        }
        self.raft.consume(&mut batch, true).unwrap();
    }
}
