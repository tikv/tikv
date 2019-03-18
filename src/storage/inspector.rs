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
use std::time::Instant;

use futures::sync::mpsc;
use futures::{lazy, Async, Future, Poll, Stream};
use raft::StateRole;
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};

use crate::pd::PdClient;
use crate::raftstore::coprocessor::{
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RoleObserver,
};
use crate::storage::metrics::*;
use crate::util::collections::HashMap;
use crate::util::time::duration_to_sec;
use crate::util::HandyRwLock;

const INSPECTOR_NAME_PREFIX: &str = "mvcc-inspector";
const GLOBAL_REGION_SLOTS: usize = 128;

#[derive(Clone)]
struct LeaderChangeObserver {
    inner: Arc<Inner>,
}

impl LeaderChangeObserver {
    pub fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    fn update_region(&self, region_id: u64, version: u64) {
        let timer = Instant::now();
        let inner = Arc::clone(&self.inner);
        self.inner.thread_pool.spawn(lazy(move || {
            let slot = region_id as usize % GLOBAL_REGION_SLOTS;
            let mut map = inner.max_read_ts_map[slot].wl();
            let entry = Arc::clone(map.entry(region_id).or_default());
            let mut entry = entry.wl();
            drop(map);

            // If inserted with `or_default`, the version must be 0
            if entry.version < version {
                entry.is_ready = false;
                entry.version = version;
                inner
                    .sender
                    .unbounded_send(UpdateTsTask { region_id, version })
                    .unwrap();
            }
            MVCC_INSPECTOR_UPDATE_DURATION.observe(duration_to_sec(timer.elapsed()));
            Ok(())
        }));
    }

    fn remove_region(&self, region_id: u64, version: u64) {
        let timer = Instant::now();
        let inner = Arc::clone(&self.inner);
        self.inner.thread_pool.spawn(lazy(move || {
            let slot = region_id as usize % GLOBAL_REGION_SLOTS;
            let mut map = inner.max_read_ts_map[slot].wl();
            if let Some(entry) = map.get(&region_id).cloned() {
                let entry = entry.wl();
                if entry.version <= version {
                    map.remove(&region_id);
                }
            }
            MVCC_INSPECTOR_UPDATE_DURATION.observe(duration_to_sec(timer.elapsed()));
            Ok(())
        }));
    }
}

impl Coprocessor for LeaderChangeObserver {}

impl RoleObserver for LeaderChangeObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext, role: StateRole) {
        if role == StateRole::Leader {
            self.update_region(
                ctx.region().get_id(),
                ctx.region().get_region_epoch().get_version(),
            );
        } else {
            self.remove_region(
                ctx.region().get_id(),
                ctx.region().get_region_epoch().get_version(),
            );
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
            match event {
                RegionChangeEvent::Create | RegionChangeEvent::Update => {
                    self.update_region(
                        ctx.region().get_id(),
                        ctx.region().get_region_epoch().get_version(),
                    );
                }
                RegionChangeEvent::Destroy => {
                    self.remove_region(
                        ctx.region().get_id(),
                        ctx.region().get_region_epoch().get_version(),
                    );
                }
            };
        }
    }
}

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
    max_read_ts_map: Vec<RwLock<TsMap>>,
    thread_pool: ThreadPool,
    sender: mpsc::UnboundedSender<UpdateTsTask>,
}

impl MvccInspector {
    pub fn new<C: PdClient + 'static>(pd_client: Arc<C>) -> Self {
        let maps = (0..GLOBAL_REGION_SLOTS)
            .map(|_| Default::default())
            .collect();
        let worker = ThreadPoolBuilder::new()
            .pool_size(1)
            .name_prefix(INSPECTOR_NAME_PREFIX)
            .build();
        let (tx, rx) = mpsc::unbounded();
        let inner = Arc::new(Inner {
            max_read_ts_map: maps,
            thread_pool: worker,
            sender: tx,
        });

        let inner1 = Arc::clone(&inner);
        inner.thread_pool.spawn(TaskCollector(rx).for_each(move |map|{
            let inner2 = Arc::clone(&inner1);
            pd_client.get_timestamp()
                .map_err(|e| error!("get timestamp fail"; "err" => ?e))
                .map(move |tso| (tso, map))
                .map(move |(tso, map)| {
                    for (region_id, version) in map {
                        let slot = region_id as usize % GLOBAL_REGION_SLOTS;
                        let global_map = inner2.max_read_ts_map[slot].rl();
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

    pub fn new_mock() -> Self {
        let maps = (0..GLOBAL_REGION_SLOTS)
            .map(|_| Default::default())
            .collect();
        let worker = ThreadPoolBuilder::new()
            .pool_size(1)
            .name_prefix(INSPECTOR_NAME_PREFIX)
            .build();
        let (tx, _) = mpsc::unbounded(); // TODO: real mock.
        let inner = Arc::new(Inner {
            max_read_ts_map: maps,
            thread_pool: worker,
            sender: tx,
        });
        Self { inner }
    }

    pub fn register_observer(&self, host: &mut CoprocessorHost) {
        let observer = LeaderChangeObserver::new(Arc::clone(&self.inner));
        host.registry
            .register_role_observer(1, box observer.clone());
        host.registry
            .register_region_change_observer(1, box observer);
    }

    pub fn report_read_ts(&self, region_id: u64, version: u64, ts: u64, _from: &str) {
        let timer = Instant::now();
        if ts == u64::max_value() {
            return;
        }
        if let Some(entry) = self.get_entry(region_id) {
            let mut lock = entry.wl();
            if lock.version < version {
                lock.is_ready = false;
                lock.version = version;
                self.inner
                    .sender
                    .unbounded_send(UpdateTsTask { region_id, version })
                    .unwrap();
            }
            if lock.max_read_ts < ts {
                lock.max_read_ts = ts;
            }
        }
        MVCC_INSPECTOR_TS_REPORT_DURATION.observe(duration_to_sec(timer.elapsed()));
    }

    pub fn get_max_read_ts(&self, region_id: u64, version: u64) -> u64 {
        if let Some(entry) = self.get_entry(region_id) {
            let lock = entry.rl();
            if lock.is_ready && lock.version == version {
                return lock.max_read_ts;
            }
        }
        0
    }

    fn get_entry(&self, region_id: u64) -> Option<Arc<RwLock<RegionMaxTsRecord>>> {
        let slot = region_id as usize % GLOBAL_REGION_SLOTS;
        self.inner.max_read_ts_map[slot]
            .rl()
            .get(&region_id)
            .cloned()
    }
}

struct TaskCollector(mpsc::UnboundedReceiver<UpdateTsTask>);
impl Stream for TaskCollector {
    type Item = HashMap<u64, u64>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut items = HashMap::new();
        loop {
            match self.0.poll() {
                Ok(Async::Ready(Some(UpdateTsTask { region_id, version }))) => {
                    if *items.get(&region_id).unwrap_or(&0) < version {
                        items.insert(region_id, version);
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use test::{black_box, Bencher};

    use std::sync::atomic::{self, AtomicBool};
    use std::thread;

    fn bench_report_ts_impl(threads: usize, b: &mut Bencher) {
        // This benchmark's result shows the ops of only one of those threads, so we can see the
        // performance regression of a single thread when there are other threads accessing it.
        let inspector = MvccInspector::new_mock();
        let region_id = 1;
        inspector.inner.max_read_ts_map[region_id as usize % GLOBAL_REGION_SLOTS]
            .wl()
            .insert(
                region_id,
                Arc::new(RwLock::new(RegionMaxTsRecord {
                    max_read_ts: 0,
                    is_ready: true,
                    version: 1,
                })),
            );

        let is_stopped = Arc::new(AtomicBool::new(false));

        let handles = (0..threads - 1)
            .map(|i| {
                let inspector1 = inspector.clone();
                let is_stopped1 = Arc::clone(&is_stopped);
                thread::spawn(move || {
                    let mut ts = i as u64;
                    loop {
                        if is_stopped1.load(atomic::Ordering::Acquire) {
                            break;
                        }
                        ts += 100;
                        inspector1.report_read_ts(region_id, 1, ts, "");
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut ts = 0;
        let inspector1 = inspector.clone();
        b.iter(move || {
            ts += 100;
            inspector1.report_read_ts(region_id, 1, ts, "");
        });

        black_box(inspector);
        is_stopped.store(true, atomic::Ordering::Release);
        handles.into_iter().for_each(|h| h.join().unwrap());
    }

    #[bench]
    fn bench_report_ts_1(b: &mut Bencher) {
        bench_report_ts_impl(1, b);
    }

    #[bench]
    fn bench_report_ts_2(b: &mut Bencher) {
        bench_report_ts_impl(2, b);
    }

    #[bench]
    fn bench_report_ts_4(b: &mut Bencher) {
        bench_report_ts_impl(4, b);
    }

    #[bench]
    fn bench_report_ts_8(b: &mut Bencher) {
        bench_report_ts_impl(8, b);
    }

    #[bench]
    fn bench_report_ts_16(b: &mut Bencher) {
        bench_report_ts_impl(16, b);
    }

    #[bench]
    fn bench_report_ts_32(b: &mut Bencher) {
        bench_report_ts_impl(32, b);
    }
}
