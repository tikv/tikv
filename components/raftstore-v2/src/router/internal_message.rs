// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Arc,
    },
};

use kvproto::raft_serverpb::RaftApplyState;
use raft::eraftpb::Snapshot as RaftSnapshot;
use tikv_util::{box_try, worker::Scheduler};

use crate::{worker::RegionTask, Result};

pub struct GenSnapTask {
    pub(crate) region_id: u64,
    pub tablet_index: u64,
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
        canceled: Arc<AtomicBool>,
        snap_notifier: SyncSender<RaftSnapshot>,
    ) -> GenSnapTask {
        GenSnapTask {
            region_id,
            tablet_index,
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
        region_sched: &Scheduler<RegionTask>,
    ) -> Result<()> {
        let snapshot = RegionTask::Gen {
            region_id: self.region_id,
            tablet_index: self.tablet_index,
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

pub enum ApplyTask {}

#[derive(Debug)]
pub enum ApplyRes {}
