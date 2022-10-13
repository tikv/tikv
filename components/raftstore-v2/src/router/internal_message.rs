// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Arc,
    },
};

use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState};
use raft::eraftpb::Snapshot as RaftSnapshot;
use raftstore::store::RaftlogFetchTask as RegionTask;
use tikv_util::{box_try, worker::Scheduler};

use crate::{operation::CommittedEntries, Result};

pub struct GenSnapTask {
    pub(crate) region_id: u64,
    pub tablet_index: u64,
    // Fill it when you are going to generate the snapshot.
    // index used to check if the gen task should be canceled.
    pub index: Arc<AtomicU64>,
    // Fetch it to cancel the task if necessary.
    pub canceled: Arc<AtomicBool>,
    snap_notifier: SyncSender<RaftSnapshot>,
    // indicates whether the snapshot is triggered due to load balance
    for_balance: bool,
}

impl GenSnapTask {
    pub fn new(
        region_id: u64,
        tablet_index: u64,
        index: Arc<AtomicU64>,
        canceled: Arc<AtomicBool>,
        snap_notifier: SyncSender<RaftSnapshot>,
    ) -> GenSnapTask {
        GenSnapTask {
            region_id,
            tablet_index,
            index,
            canceled,
            snap_notifier,
            for_balance: false,
        }
    }

    pub fn set_for_balance(&mut self) {
        self.for_balance = true;
    }

    pub fn generate_and_schedule_snapshot(
        self,
        region_state: RegionLocalState,
        last_applied_term: u64,
        last_applied_index: u64,
        region_sched: &Scheduler<RegionTask>,
    ) -> Result<()> {
        self.index.store(last_applied_index, Ordering::SeqCst);
        let snapshot = RegionTask::GenTabletSnapshot {
            region_id: self.region_id,
            tablet_index: self.tablet_index,
            region_state,
            last_applied_term,
            last_applied_index,
            notifier: self.snap_notifier,
            for_balance: self.for_balance,
            canceled: self.canceled,
        };
        box_try!(region_sched.schedule(snapshot));
        Ok(())
    }
}

impl Debug for GenSnapTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenSnapTask")
            .field("region_id", &self.region_id)
            .finish()
    }
}

#[derive(Debug)]
pub enum ApplyTask {
    CommittedEntries(CommittedEntries),
    Snapshot(GenSnapTask),
}

#[derive(Debug, Default)]
pub struct ApplyRes {
    pub applied_index: u64,
    pub applied_term: u64,
    pub region_state: Option<RegionLocalState>,
}
