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
    collections::LinkedList,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::{RaftEngine, RaftLogBatch};

#[derive(Debug)]
pub struct FlushProgress {
    cf: String,
    apply_index: u64,
    earliest_seqno: u64,
}

impl FlushProgress {
    fn merge(&mut self, pr: FlushProgress) {
        debug_assert_eq!(self.cf, pr.cf);
        debug_assert!(self.apply_index <= pr.apply_index);
        self.apply_index = pr.apply_index;
    }

    pub fn applied_index(&self) -> u64 {
        self.apply_index
    }

    pub fn cf(&self) -> &str {
        &self.cf
    }
}

/// A share state between raftstore and underlying engine.
///
/// raftstore will update state changes and corresponding apply index, when
/// flush, `PersistenceListener` will query states related to the memtable
/// and persist the relation to raft engine.
#[derive(Default, Debug)]
pub struct FlushState {
    applied_index: AtomicU64,
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
}

/// A helper trait to avoid exposing `RaftEngine` to `TabletFactory`.
pub trait StateStorage: Sync + Send {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: FlushProgress);
}

/// A flush listener that maps memtable to apply index and persist the relation
/// to raft engine.
pub struct PersistenceListener {
    region_id: u64,
    tablet_index: u64,
    state: Arc<FlushState>,
    progress: Mutex<LinkedList<FlushProgress>>,
    storage: Arc<dyn StateStorage>,
}

impl PersistenceListener {
    pub fn new(
        region_id: u64,
        tablet_index: u64,
        state: Arc<FlushState>,
        storage: Arc<dyn StateStorage>,
    ) -> Self {
        Self {
            region_id,
            tablet_index,
            state,
            progress: Mutex::new(LinkedList::new()),
            storage,
        }
    }
}

impl PersistenceListener {
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.state
    }

    /// Called when memtable is frozen.
    ///
    /// `earliest_seqno` should be the smallest seqno of the memtable.
    pub fn on_memtable_sealed(&self, cf: String, earliest_seqno: u64) {
        // The correctness relies on the assumption that there will be only one
        // thread writting to the DB and increasing apply index.
        // Apply index will be set within DB lock, so it's correct even with manual
        // flush.
        let apply_index = self.state.applied_index.load(Ordering::SeqCst);
        self.progress.lock().unwrap().push_back(FlushProgress {
            cf,
            apply_index,
            earliest_seqno,
        });
    }

    /// Called a memtable finished flushing.
    ///
    /// `largest_seqno` should be the largest seqno of the generated file.
    pub fn on_flush_completed(&self, cf: &str, largest_seqno: u64) {
        // Maybe we should hook the compaction to avoid the file is compacted before
        // being recorded.
        let pr = {
            let mut prs = self.progress.lock().unwrap();
            let mut cursor = prs.cursor_front_mut();
            let mut flushed_pr = None;
            while let Some(pr) = cursor.current() {
                if pr.cf != cf {
                    cursor.move_next();
                    continue;
                }
                // Note flushed largest_seqno equals to earliest_seqno of next memtable.
                if pr.earliest_seqno < largest_seqno {
                    match &mut flushed_pr {
                        None => flushed_pr = cursor.remove_current(),
                        Some(flushed_pr) => {
                            flushed_pr.merge(cursor.remove_current().unwrap());
                        }
                    }
                    continue;
                }
                break;
            }
            match flushed_pr {
                Some(pr) => pr,
                None => panic!("{} not found in {:?}", cf, prs),
            }
        };
        self.storage
            .persist_progress(self.region_id, self.tablet_index, pr);
    }
}

impl<R: RaftEngine> StateStorage for R {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: FlushProgress) {
        if pr.apply_index == 0 {
            return;
        }
        let mut batch = self.log_batch(1);
        // TODO: It's possible that flush succeeds but fails to call
        // `on_flush_completed` before exit. In this case the flushed data will
        // be replayed again after restarted. To solve the problem, we need to
        // (1) persist flushed file numbers in `on_flush_begin` and (2) check
        // the file number in `on_compaction_begin`. After restart, (3) check if the
        // file exists. If (1) && ((2) || (3)), then we don't need to replay the data.
        batch
            .put_flushed_index(region_id, &pr.cf, tablet_index, pr.apply_index)
            .unwrap();
        self.consume(&mut batch, true).unwrap();
    }
}
