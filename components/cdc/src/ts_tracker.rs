// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use causal_ts::TsTracker;
use collections::HashMap;
use tikv_util::{warn, worker::Scheduler};
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_subscribe_unsubscribe() {
        let region_id1 = 1;
        let region_id2 = 2;
        let region_id3 = 3;

        let (scheduler, mut rx) = tikv_util::worker::dummy_scheduler();
        let ts_tracker = CdcTsTracker::new(scheduler);

        // subscribe region 1, 2
        ts_tracker.subscribe_region(region_id1);
        ts_tracker.subscribe_region(region_id2);
        ts_tracker.track_ts(region_id1, 10.into());
        ts_tracker.track_ts(region_id2, 11.into());
        ts_tracker.track_ts(region_id3, 12.into());
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 1);
            }
            _ => panic!("unexpected task"),
        };
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 2);
            }
            _ => panic!("unexpected task"),
        };

        // subscribe region 1, 2, 3
        ts_tracker.subscribe_region(region_id3);
        ts_tracker.track_ts(region_id1, 10.into());
        ts_tracker.track_ts(region_id2, 11.into());
        ts_tracker.track_ts(region_id3, 12.into());
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 1);
            }
            _ => panic!("unexpected task"),
        };
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 2);
            }
            _ => panic!("unexpected task"),
        };
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 3);
            }
            _ => panic!("unexpected task"),
        };

        // subscribe region 1, 3
        ts_tracker.subscribe_region(region_id3);
        ts_tracker.unsubscribe_region(region_id2);
        ts_tracker.track_ts(region_id1, 10.into());
        ts_tracker.track_ts(region_id2, 11.into());
        ts_tracker.track_ts(region_id3, 12.into());
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 1);
            }
            _ => panic!("unexpected task"),
        };
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::TrackTs { region_id, .. } => {
                assert_eq!(region_id, 3);
            }
            _ => panic!("unexpected task"),
        };
    }
}
