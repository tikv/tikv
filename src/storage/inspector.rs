// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};

use super::metrics::*;
use raft::StateRole;

use crate::raftstore::coprocessor::{
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RoleObserver,
};
use crate::util::collections::HashMap;
use crate::util::worker::Scheduler;
use crate::util::HandyRwLock;

//const KEY_BUCKET_SIZE: usize = 1024;

#[derive(Debug, Clone)]
enum LeaderChangeEvent {
    Update { region_id: u64, version: u64 },
    Remove { region_id: u64 },
}

impl Display for LeaderChangeEvent {
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        Debug::fmt(self, fmt)
    }
}

#[derive(Clone)]
struct LeaderChangeObserver {
    scheduler: Scheduler<LeaderChangeEvent>,
}

impl LeaderChangeObserver {
    //    pub fn new(scheduler: Scheduler<LeaderChangeEvent>) -> Self {
    //        Self { scheduler }
    //    }
}

impl Coprocessor for LeaderChangeObserver {}

impl RoleObserver for LeaderChangeObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext, role: StateRole) {
        if role == StateRole::Leader {
            self.scheduler
                .schedule(LeaderChangeEvent::Update {
                    region_id: ctx.region().get_id(),
                    version: ctx.region().get_region_epoch().get_version(),
                })
                .unwrap();
        } else {
            self.scheduler
                .schedule(LeaderChangeEvent::Remove {
                    region_id: ctx.region().get_id(),
                })
                .unwrap();
        }
    }
}

impl RegionChangeObserver for LeaderChangeObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        if role == StateRole::Leader {
            let event = match event {
                RegionChangeEvent::Create | RegionChangeEvent::Update => {
                    LeaderChangeEvent::Update {
                        region_id: ctx.region().get_id(),
                        version: ctx.region().get_region_epoch().get_version(),
                    }
                }
                RegionChangeEvent::Destroy => LeaderChangeEvent::Remove {
                    region_id: ctx.region().get_id(),
                },
            };
            self.scheduler.schedule(event).unwrap();
        }
    }
}

// region_id -> (epoch_version, max_read_ts)
type TsMap = HashMap<(u64, u64), Arc<AtomicU64>>;

#[derive(Clone)]
pub struct MvccInspector {
    max_read_ts_map: Arc<RwLock<TsMap>>,
    //    update_worker: Arc<Mutex<Worker<LeaderChangeEvent>>>,
    //    refs: Arc<AtomicUsize>,
}

impl MvccInspector {
    pub fn new() -> Self {
        let max_read_ts_map = Arc::new(RwLock::new(TsMap::default()));

        //        let update_runner = MvccInspectorUpdateRunner {
        //            max_read_ts_map: max_read_ts_map.clone(),
        //        };
        //
        //        let mut worker = WorkerBuilder::new("mvcc-inspector-update-runner").create();
        //        worker.start(update_runner).unwrap();

        Self {
            max_read_ts_map,
            //            update_worker: Arc::new(Mutex::new(worker)),
            //            refs: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn register_observer(&self, _host: &mut CoprocessorHost) {
        //        let scheduler = self.update_worker.lock().unwrap().scheduler();
        //        let observer = LeaderChangeObserver::new(scheduler);
        //
        //        host.registry
        //            .register_role_observer(1, box observer.clone());
        //        host.registry
        //            .register_region_change_observer(1, box observer);
    }

    //    fn stop(&self) {
    //        self.update_worker
    //            .lock()
    //            .unwrap()
    //            .stop()
    //            .unwrap()
    //            .join()
    //            .unwrap();
    //    }

    pub fn report_read_ts(&self, region_id: u64, version: u64, ts: u64, from: &str) {
        if ts == u64::max_value() {
            return;
        }
        let mut times = 0;
        let prev = self
            .lock_entry(region_id, version)
            .fetch_update(
                |saved_ts| {
                    times += 1;
                    Some(::std::cmp::max(saved_ts, ts))
                },
                atomic::Ordering::SeqCst,
                atomic::Ordering::SeqCst,
            )
            .unwrap();
        KV_MAX_READ_TS_UPDATE_COUNTER
            .with_label_values(&[from, if prev >= ts { "fail" } else { "success" }])
            .inc();
        KV_MAX_READ_TS_FETCH_UPDATE_RETRY_HISTOGRAM.observe(times as f64);
    }

    pub fn get_max_read_ts(&self, region_id: u64, version: u64) -> u64 {
        let entry = self.lock_entry(region_id, version);
        let res = entry.load(atomic::Ordering::SeqCst);
        KV_PREWRITE_MAX_READ_TS_RES_COUNTER
            .with_label_values(&[if res == 0 { "invalid" } else { "valid" }])
            .inc();
        res
    }

    fn lock_entry(&self, region_id: u64, version: u64) -> Arc<AtomicU64> {
        if let Some(entry) = self.max_read_ts_map.rl().get(&(region_id, version)) {
            return entry.clone();
        }
        let mut map = self.max_read_ts_map.wl();
        map.entry((region_id, version)).or_default().clone()
    }
}

//impl Clone for MvccInspector {
//    fn clone(&self) -> Self {
//        self.refs.fetch_add(1, atomic::Ordering::SeqCst);
//
//        Self {
//            max_read_ts_map: self.max_read_ts_map.clone(),
//            update_worker: self.update_worker.clone(),
//            refs: self.refs.clone(),
//        }
//    }
//}

//impl Drop for MvccInspector {
//    fn drop(&mut self) {
//        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);
//        if refs == 1 {
//            self.stop();
//        }
//    }
//}

//#[derive(Clone)]
//struct MvccInspectorUpdateRunner {
//    max_read_ts_map: Arc<RwLock<TsMap>>,
//}
//
//impl Runnable<LeaderChangeEvent> for MvccInspectorUpdateRunner {
//    fn run(&mut self, event: LeaderChangeEvent) {}
//}
