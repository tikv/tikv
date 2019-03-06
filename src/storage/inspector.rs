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
use std::sync::atomic::{self, AtomicUsize};
use std::sync::{Arc, Mutex, RwLock};

//use super::metrics::*;
use futures::Future;
//use raft::StateRole;

use crate::pd::PdClient;
use crate::raftstore::coprocessor::CoprocessorHost;
use crate::util::collections::HashMap;
use crate::util::worker::{Builder as WorkerBuilder, Runnable, Scheduler, Worker};
use crate::util::HandyRwLock;

//const KEY_BUCKET_SIZE: usize = 1024;

#[derive(Debug, Clone)]
enum InspectorRunnerTask {
    //    UpdateRegion { region_id: u64, version: u64 },
    // RemoveRegion { region_id: u64 },
    UpdateTsForRegion { region_id: u64, version: u64 },
}

impl Display for InspectorRunnerTask {
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        Debug::fmt(self, fmt)
    }
}

//#[derive(Clone)]
//struct LeaderChangeObserver {
//    scheduler: Scheduler<InspectorRunnerTask>,
//}
//
//impl LeaderChangeObserver {
//    //    pub fn new(scheduler: Scheduler<LeaderChangeEvent>) -> Self {
//    //        Self { scheduler }
//    //    }
//}
//
//impl Coprocessor for LeaderChangeObserver {}
//
//impl RoleObserver for LeaderChangeObserver {
//    fn on_role_change(&self, ctx: &mut ObserverContext, role: StateRole) {
//        if role == StateRole::Leader {
//            self.scheduler
//                .schedule(InspectorRunnerTask::UpdateRegion {
//                    region_id: ctx.region().get_id(),
//                    version: ctx.region().get_region_epoch().get_version(),
//                })
//                .unwrap();
//        } else {
//            self.scheduler
//                .schedule(InspectorRunnerTask::RemoveRegion {
//                    region_id: ctx.region().get_id(),
//                })
//                .unwrap();
//        }
//    }
//}

//impl RegionChangeObserver for LeaderChangeObserver {
//    fn on_region_changed(
//        &self,
//        ctx: &mut ObserverContext,
//        event: RegionChangeEvent,
//        role: StateRole,
//    ) {
//        if role == StateRole::Leader {
//            let event = match event {
//                RegionChangeEvent::Create | RegionChangeEvent::Update => {
//                    LeaderChangeEvent::UpdateRegion {
//                        region_id: ctx.region().get_id(),
//                        version: ctx.region().get_region_epoch().get_version(),
//                    }
//                }
//                RegionChangeEvent::Destroy => LeaderChangeEvent::Remove {
//                    region_id: ctx.region().get_id(),
//                },
//            };
//            self.scheduler.schedule(event).unwrap();
//        }
//    }
//}

#[derive(Default)]
struct RegionMaxTsRecord {
    is_ready: bool,
    version: u64,
    max_read_ts: u64,
}

// region_id -> (is_ready, epoch_version, max_read_ts)
type TsMap = HashMap<u64, Arc<RwLock<RegionMaxTsRecord>>>;

pub struct MvccInspector {
    max_read_ts_map: Arc<RwLock<TsMap>>,
    worker: Arc<Mutex<Worker<InspectorRunnerTask>>>,
    scheduler: Scheduler<InspectorRunnerTask>,
    refs: Arc<AtomicUsize>,
}

impl MvccInspector {
    pub fn new<C: PdClient + 'static>(pd_client: Arc<C>) -> Self {
        let max_read_ts_map = Arc::new(RwLock::new(TsMap::default()));

        let update_runner = MvccInspectorUpdateRunner {
            max_read_ts_map: max_read_ts_map.clone(),
            pd_client,
        };

        let mut worker = WorkerBuilder::new("mvcc-inspector-update-runner").create();
        worker.start(update_runner).unwrap();
        let scheduler = worker.scheduler();

        Self {
            max_read_ts_map,
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
            refs: Arc::new(AtomicUsize::new(1)),
        }
    }

    // Quickly make the code buildable
    pub fn new_mock() -> Self {
        let worker = WorkerBuilder::new("mvcc-inspector-update-runner").create();
        let scheduler = worker.scheduler();

        Self {
            max_read_ts_map: Default::default(),
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
            refs: Arc::new(AtomicUsize::new(1)),
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

    fn stop(&self) {
        self.worker.lock().unwrap().stop().unwrap().join().unwrap();
    }

    pub fn report_read_ts(&self, region_id: u64, version: u64, ts: u64, _from: &str) {
        if ts == u64::max_value() {
            return;
        }
        let entry = self.get_entry(region_id, version);
        let mut lock = entry.wl();
        if lock.version < version {
            lock.is_ready = false;
            lock.version = version;
            self.scheduler
                .schedule(InspectorRunnerTask::UpdateTsForRegion { region_id, version })
                .unwrap();
        }
        if lock.max_read_ts < ts {
            lock.max_read_ts = ts;
        }
    }

    pub fn get_max_read_ts(&self, region_id: u64, version: u64) -> u64 {
        let entry = self.get_entry(region_id, version);
        let lock = entry.rl();
        if lock.is_ready && lock.version == version {
            return lock.max_read_ts;
        }
        0
    }

    fn get_entry(&self, region_id: u64, version: u64) -> Arc<RwLock<RegionMaxTsRecord>> {
        if let Some(entry) = self.max_read_ts_map.rl().get(&region_id) {
            return entry.clone();
        }
        let mut map = self.max_read_ts_map.wl();
        let mut not_exist = false;
        let entry = map.entry(region_id).or_insert_with(|| {
            not_exist = true;
            Arc::new(RwLock::new(RegionMaxTsRecord {
                is_ready: false,
                version,
                max_read_ts: 0,
            }))
        });
        self.scheduler
            .schedule(InspectorRunnerTask::UpdateTsForRegion { region_id, version })
            .unwrap();
        entry.clone()
    }
}

impl Clone for MvccInspector {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, atomic::Ordering::SeqCst);

        Self {
            max_read_ts_map: self.max_read_ts_map.clone(),
            worker: self.worker.clone(),
            scheduler: self.scheduler.clone(),
            refs: self.refs.clone(),
        }
    }
}

impl Drop for MvccInspector {
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, atomic::Ordering::SeqCst);
        if refs == 1 {
            self.stop();
        }
    }
}

#[derive(Clone)]
struct MvccInspectorUpdateRunner<C: PdClient> {
    max_read_ts_map: Arc<RwLock<TsMap>>,
    pd_client: Arc<C>,
}

impl<C: PdClient> MvccInspectorUpdateRunner<C> {
    fn update_ts_for_region(&self, region_id: u64, version: u64) {
        let ts = self.pd_client.get_timestamp().wait().unwrap();
        if let Some(mut entry) = self.max_read_ts_map.rl().get(&region_id).map(|e| e.wl()) {
            if entry.version != version {
                return;
            }
            if entry.max_read_ts < ts {
                entry.max_read_ts = ts;
            }
            entry.is_ready = true;
            info!("max_read_ts is ready"; "region_id" => region_id, "version" => version, "max_read_ts" => ts);
        }
    }
}

impl<C: PdClient> Runnable<InspectorRunnerTask> for MvccInspectorUpdateRunner<C> {
    fn run(&mut self, event: InspectorRunnerTask) {
        match event {
            InspectorRunnerTask::UpdateTsForRegion { region_id, version } => {
                self.update_ts_for_region(region_id, version)
            }
        }
    }
}
