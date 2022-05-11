// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use causal_ts::TsTracker;
use collections::HashMap;
use tikv_util::warn;
use tikv_util::worker::Scheduler;
use txn_types::TimeStamp;

use crate::endpoint::Task;

#[derive(Clone)]
pub struct CdcTsTracker {
    sched: Scheduler<Task>,
    subscribed_regions: Arc<RwLock<HashMap<u64, bool>>>,
}

impl CdcTsTracker {
    pub fn new(sched: Scheduler<Task>) -> CdcTsTracker {
        CdcTsTracker {
            sched,
            subscribed_regions: Arc::default(),
        }
    }

    pub fn subscribe_region(&self, region_id: u64) {
        self.subscribed_regions
            .write()
            .unwrap()
            .insert(region_id, true);
    }

    pub fn unsubscribe_region(&self, region_id: u64) {
        self.subscribed_regions.write().unwrap().remove(&region_id);
    }

    pub fn is_subscribed(&self, region_id: u64) -> bool {
        self.subscribed_regions
            .read()
            .unwrap()
            .get(&region_id)
            .is_some()
    }
}

impl TsTracker for CdcTsTracker {
    fn track_ts(&self, region_id: u64, ts: TimeStamp) {
        if self.is_subscribed(region_id) {
            if let Err(e) = self.sched.schedule(Task::TrackTs { region_id, ts }) {
                warn!("cdc schedule task failed"; "error" => ?e);
            }
        }
    }
}
