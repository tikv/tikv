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

use std::sync::{Arc, RwLock};

//use super::metrics::*;
use futures::{Future, Async, Poll, Stream};
use futures::sync::mpsc;
use tokio_threadpool::{ThreadPool, Builder as ThreadPoolBuilder};
//use raft::StateRole;

use crate::pd::PdClient;
use crate::raftstore::coprocessor::CoprocessorHost;
use crate::util::collections::HashMap;
use crate::util::HandyRwLock;

//const KEY_BUCKET_SIZE: usize = 1024;
const INSPECTOR_NAME_PREFIX: &str = "mvcc-inspector";

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

#[derive(Debug, Clone, Copy)]
struct UpdateTsTask { 
    region_id: u64,
    version: u64,
}

#[derive(Default)]
struct RegionMaxTsRecord {
    is_ready: bool,
    version: u64,
    max_read_ts: u64,
}

// region_id -> (is_ready, epoch_version, max_read_ts)
type TsMap = HashMap<u64, Arc<RwLock<RegionMaxTsRecord>>>;

#[derive(Clone)]
pub struct MvccInspector {
    inner: Arc<Inner>,
}

pub struct Inner {
    max_read_ts_map: RwLock<TsMap>,
    worker: ThreadPool,
    sender: mpsc::UnboundedSender<UpdateTsTask>,
}

impl MvccInspector {
    pub fn new<C: PdClient + 'static>(pd_client: Arc<C>) -> Self {
        let worker = ThreadPoolBuilder::new()
            .pool_size(1)
            .name_prefix(INSPECTOR_NAME_PREFIX)
            .build();
        let (tx, rx) = mpsc::unbounded();
        let inner = Arc::new(Inner {
            max_read_ts_map: Default::default(),
            worker,
            sender: tx,
        });

        let inner1 = Arc::clone(&inner);
        inner.worker.spawn(TaskCollector(rx).for_each(move |map|{
            let inner2 = Arc::clone(&inner1);
            pd_client.get_timestamp()
                .map_err(|e| error!("get timestamp fail"; "err" => ?e))
                .map(move |tso| (tso, map))
                .map(move |(tso, map)| {
                    for (region_id, version) in map {
                        let global_map = inner2.max_read_ts_map.rl();  
                        let e = Arc::clone(global_map.get(&region_id).unwrap());
                        drop(global_map);
                        let mut e = e.wl();
                        if e.version != version {
                            continue;
                        }
                        e.max_read_ts = tso;
                        e.is_ready = true;
                        info!("max_read_ts is ready"; "region_id" => region_id, "version" => version, "max_read_ts" => tso);
                    }
                })
        }));
        Self { inner }
    }

    // Quickly make the code buildable
    pub fn new_mock() -> Self {
        let worker = ThreadPoolBuilder::new()
            .pool_size(1)
            .name_prefix(INSPECTOR_NAME_PREFIX)
            .build();
        let (tx, _) = mpsc::unbounded(); // TODO: real mock.
        let inner = Arc::new(Inner {
            max_read_ts_map: Default::default(),
            worker,
            sender: tx,
        });
        Self { inner }
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

    pub fn report_read_ts(&self, region_id: u64, version: u64, ts: u64, _from: &str) {
        if ts == u64::max_value() {
            return;
        }
        let entry = self.get_entry(region_id, version);
        let mut lock = entry.wl();
        if lock.version < version {
            lock.is_ready = false;
            lock.version = version;
            self.inner.sender
                .unbounded_send(UpdateTsTask { region_id, version })
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
        if let Some(entry) = self.inner.max_read_ts_map.rl().get(&region_id) {
            return entry.clone();
        }
        let mut map = self.inner.max_read_ts_map.wl();
        let mut not_exist = false;
        let entry = map.entry(region_id).or_insert_with(|| {
            not_exist = true;
            Arc::new(RwLock::new(RegionMaxTsRecord {
                is_ready: false,
                version,
                max_read_ts: 0,
            }))
        });
        self.inner.sender
            .unbounded_send(UpdateTsTask { region_id, version })
            .unwrap();
        entry.clone()
    }
}

struct TaskCollector(mpsc::UnboundedReceiver<UpdateTsTask>);
impl Stream for TaskCollector {
    type Error = ();
    type Item = HashMap<u64, u64>;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut items = HashMap::new();
        loop {
            match self.0.poll() {
                Ok(Async::Ready(Some(UpdateTsTask {region_id, version}))) => {
                    if items.get(&region_id).unwrap_or(&0) < &version {
                        items.insert(region_id, version);
                    }
                },
                Ok(Async::NotReady) => break,
                _ => unreachable!(),
            }
        }
        if items.is_empty() {
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(Some(items)))
    }
}
