// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use txn_types::TimeStamp;
use causal_ts::TsTracker;
use crate::endpoint::Task;
use tikv_util::{warn, worker::Scheduler};
pub struct RawKvTsTracker {
    sched: Scheduler<Task>,
}

impl RawKvTsTracker {
    pub fn new(sched: Scheduler<Task>) -> Self {
        RawKvTsTracker {
            sched,
        }
    }
}

impl TsTracker for RawKvTsTracker {
    fn track_ts(&self, region_id: u64, key: Vec<u8>, ts: TimeStamp) {
        if let Err(e) = self.sched.schedule(Task::RawTrackTs { region_id, key, ts }) {
            warn!("cdc schedule task failed"; "error" => ?e);
        }
    }
}