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
use std::sync::mpsc as std_mpsc;
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

const READ_TS_CACHE_NAME_PREFIX: &str = "read-ts-cache";
const GLOBAL_REGION_SLOTS: usize = 128;

/// Hash the `region_id` to the slot index.
#[inline]
fn region_slot(region_id: u64) -> usize {
    region_id as usize % GLOBAL_REGION_SLOTS
}

/// Collects changes of leaders on this store and update it to the collection.
#[derive(Clone)]
struct LeaderChangeObserver {
    inner: Arc<Inner>,
    /// A message will be sent every time a event is handled. For test purpose only. It's used to
    /// wait for the async event handling logic to finish.
    sync_signal_tx: Option<std_mpsc::SyncSender<()>>,
}

impl LeaderChangeObserver {
    pub fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            sync_signal_tx: None,
        }
    }

    /// Handles adding or updating a leader on the store.
    fn update(&self, region_id: u64, version: u64) {
        let timer = Instant::now();

        let inner = Arc::clone(&self.inner);
        let sync_signal_tx = self.sync_signal_tx.clone();
        self.inner.thread_pool.spawn(lazy(move || {
            let slot = region_slot(region_id);
            let mut map = inner.max_read_ts_map[slot].wl();
            let entry = Arc::clone(map.entry(region_id).or_default());
            let mut entry = entry.wl();
            // Releases the outer lock.
            drop(map);

            // If inserted with `or_default`, the version must be 0.
            if entry.version < version {
                // If the leader is just created on this store or the version just changed, we
                // should get a timestamp from PD, and set its `max_read_ts` to
                // `max(max_read_ts, ts_from_pd)`. `is_ready` will be set after getting timestamp
                // from PD.
                entry.is_ready = false;
                entry.version = version;
                inner
                    .sender
                    .unbounded_send(UpdateTsTask { region_id, version })
                    .unwrap();
            }
            if let Some(tx) = sync_signal_tx {
                tx.send(()).unwrap();
            }
            READ_TS_CACHE_UPDATE_DURATION.observe(duration_to_sec(timer.elapsed()));
            Ok(())
        }));
    }

    /// Handles removing a leader from the store.
    fn remove(&self, region_id: u64, version: u64) {
        let timer = Instant::now();

        let inner = Arc::clone(&self.inner);
        let sync_signal_tx = self.sync_signal_tx.clone();
        self.inner.thread_pool.spawn(lazy(move || {
            let slot = region_slot(region_id);
            let mut map = inner.max_read_ts_map[slot].wl();
            if let Some(entry) = map.get(&region_id).cloned() {
                let entry = entry.wl();
                if entry.version <= version {
                    map.remove(&region_id);
                }
            }
            if let Some(tx) = sync_signal_tx {
                tx.send(()).unwrap();
            }
            READ_TS_CACHE_UPDATE_DURATION.observe(duration_to_sec(timer.elapsed()));
            Ok(())
        }));
    }
}

impl Coprocessor for LeaderChangeObserver {}

impl RoleObserver for LeaderChangeObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if role == StateRole::Leader {
            self.update(
                ctx.region().get_id(),
                ctx.region().get_region_epoch().get_version(),
            );
        } else {
            self.remove(
                ctx.region().get_id(),
                ctx.region().get_region_epoch().get_version(),
            );
        }
    }
}

impl RegionChangeObserver for LeaderChangeObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        if role == StateRole::Leader {
            match event {
                RegionChangeEvent::Create | RegionChangeEvent::Update => {
                    self.update(
                        ctx.region().get_id(),
                        ctx.region().get_region_epoch().get_version(),
                    );
                }
                RegionChangeEvent::Destroy => {
                    self.remove(
                        ctx.region().get_id(),
                        ctx.region().get_region_epoch().get_version(),
                    );
                }
            };
        }
    }
}

/// Represents the task of allocating a timestamp from PD and assigning it to a region.
#[derive(Debug, Clone, Copy)]
struct UpdateTsTask {
    region_id: u64,
    version: u64,
}

#[derive(Default, PartialEq, Debug, Clone)]
struct RegionMaxTsRecord {
    /// The `max_read_ts` is not safe to use immediately after creation or updating of the region.
    /// When the `max_read_ts` is ready, `is_ready` will be set.
    is_ready: bool,

    /// The version of the region's epoch. The `max_read_ts` can only be used when `version` matches
    /// the request's version.
    version: u64,

    /// The maximum timestamp of transactional reads on this region. Note that in order to ensure
    /// the `max_read_ts` is no less than reads on the region before the region's leader goes to
    /// the current store, A timestamp will be fetched from PD after creating or updating of the
    /// region.
    max_read_ts: u64,
}

/// Keeps `max_read_ts` of each region. Transactional read operations update it before getting
/// snapshots, and prewrite operations fetch it after finishing writing and return it to the client.
/// Clients may use this value to calculate a valid commit_ts instead of allocating it from PD to
/// reduce latency.
#[derive(Clone)]
pub struct ReadTsCache {
    inner: Arc<Inner>,
}

// region_id -> (is_ready, epoch_version, max_read_ts)
type TsMap = HashMap<u64, Arc<RwLock<RegionMaxTsRecord>>>;

struct Inner {
    /// The maps from `region_id` to the region's `RegionMaxTsRecord`. The regions are hashed to
    /// several buckets to reduce lock contention.
    max_read_ts_map: Vec<RwLock<TsMap>>,
    thread_pool: ThreadPool,
    sender: mpsc::UnboundedSender<UpdateTsTask>,
}

impl ReadTsCache {
    pub fn new<C: PdClient + 'static>(pd_client: Arc<C>) -> Self {
        let maps = (0..GLOBAL_REGION_SLOTS)
            .map(|_| Default::default())
            .collect();
        let worker = ThreadPoolBuilder::new()
            .pool_size(1)
            .name_prefix(READ_TS_CACHE_NAME_PREFIX)
            .build();
        let (tx, rx) = mpsc::unbounded();
        let inner = Arc::new(Inner {
            max_read_ts_map: maps,
            thread_pool: worker,
            sender: tx,
        });

        let inner1 = Arc::clone(&inner);
        inner
            .thread_pool
            .spawn(TaskCollector(rx).for_each(move |map| {
                let inner2 = Arc::clone(&inner1);
                pd_client
                    .get_timestamp()
                    .map_err(|e| error!("get timestamp fail"; "err" => ?e))
                    .map(move |tso| (tso, map))
                    .map(move |(tso, map)| {
                        for (region_id, version) in map {
                            Self::make_ready(&inner2, region_id, version, tso);
                        }
                    })
            }));
        Self { inner }
    }

    pub fn new_mock() -> (Self, std_mpsc::Receiver<(u64, u64)>) {
        let maps = (0..GLOBAL_REGION_SLOTS)
            .map(|_| Default::default())
            .collect();
        let worker = ThreadPoolBuilder::new()
            .pool_size(1)
            .name_prefix(READ_TS_CACHE_NAME_PREFIX)
            .build();
        let (tx, rx) = mpsc::unbounded(); // TODO: real mock.
        let inner = Arc::new(Inner {
            max_read_ts_map: maps,
            thread_pool: worker,
            sender: tx,
        });
        let (std_tx, std_rx) = std_mpsc::channel();
        inner.thread_pool.spawn(rx.for_each(move |task| {
            let _ = std_tx.send((task.region_id, task.version));
            Ok(())
        }));
        (Self { inner }, std_rx)
    }

    /// Registers the observer needed by `ReadTsCache` to the given `CoprocessorHost`.
    pub fn register_observer(&self, host: &mut CoprocessorHost) {
        let observer = LeaderChangeObserver::new(Arc::clone(&self.inner));
        host.registry
            .register_role_observer(1, Box::new(observer.clone()));
        host.registry
            .register_region_change_observer(1, Box::new(observer));
    }

    /// Updates the region's max read ts to `ts` if `ts` is greater.
    pub fn report_read_ts(&self, region_id: u64, ts: u64) {
        let timer = Instant::now();
        if ts == u64::max_value() {
            return;
        }
        if let Some(entry) = self.get_entry(region_id) {
            let mut lock = entry.wl();
            // Doesn't care whether the version matches here.
            if lock.max_read_ts < ts {
                lock.max_read_ts = ts;
            }
        }
        READ_TS_CACHE_TS_REPORT_DURATION.observe(duration_to_sec(timer.elapsed()));
    }

    /// Gets the region's max read ts. Returns 0 if not ready, region doesn't exist or versions
    /// doesn't match.
    pub fn get_max_read_ts(&self, region_id: u64, version: u64) -> u64 {
        if let Some(entry) = self.get_entry(region_id) {
            let lock = entry.rl();
            if lock.is_ready && lock.version == version {
                return lock.max_read_ts;
            }
        }
        0
    }

    fn make_ready(inner: &Inner, region_id: u64, version: u64, tso: u64) {
        let slot = region_slot(region_id);
        let map = inner.max_read_ts_map[slot].rl();
        let e = match map.get(&region_id).map(Arc::clone) {
            Some(e) => e,
            None => {
                info!(
                    "get tso finished but the region doesn't exist in the map";
                    "region_id" => region_id
                );
                return;
            }
        };

        drop(map);
        let mut e = e.wl();
        if e.version != version {
            info!(
                "get tso finished but the region version doesn't match";
                "region_id" => region_id,
                "old_version" => e.version,
                "version" => version
            );
            return;
        }
        if tso > e.max_read_ts {
            e.max_read_ts = tso;
        }
        e.is_ready = true;
        info!("max_read_ts is ready"; "region_id" => region_id, "version" => version, "max_read_ts" => tso);
    }

    /// Gets the entry of the specified region from the maps.
    fn get_entry(&self, region_id: u64) -> Option<Arc<RwLock<RegionMaxTsRecord>>> {
        let slot = region_slot(region_id);
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
    use std::time::Duration;

    use kvproto::metapb::Region;

    fn bench_report_ts_impl(threads: usize, b: &mut Bencher) {
        // This benchmark's result shows the ops of only one of those threads, so we can see the
        // performance regression of a single thread when there are other threads accessing it.
        let c = ReadTsCache::new_mock().0;
        let region_id = 1;
        c.inner.max_read_ts_map[region_slot(region_id)].wl().insert(
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
                let c1 = c.clone();
                let is_stopped1 = Arc::clone(&is_stopped);
                thread::spawn(move || {
                    let mut ts = i as u64;
                    loop {
                        if is_stopped1.load(atomic::Ordering::Acquire) {
                            break;
                        }
                        ts += 100;
                        c1.report_read_ts(region_id, ts);
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut ts = 0;
        let c1 = c.clone();
        b.iter(move || {
            ts += 100;
            c1.report_read_ts(region_id, ts);
        });

        black_box(c);
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

    fn new_region(region_id: u64, version: u64) -> Region {
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_region_epoch().set_version(version);
        region
    }

    fn role_change(observer: &LeaderChangeObserver, region_id: u64, version: u64, role: StateRole) {
        observer.on_role_change(
            &mut ObserverContext::new(&new_region(region_id, version)),
            role,
        );
    }

    fn create_region(
        observer: &LeaderChangeObserver,
        region_id: u64,
        version: u64,
        role: StateRole,
    ) {
        observer.on_region_changed(
            &mut ObserverContext::new(&new_region(region_id, version)),
            RegionChangeEvent::Create,
            role,
        );
    }

    fn update_region(
        observer: &LeaderChangeObserver,
        region_id: u64,
        version: u64,
        role: StateRole,
    ) {
        observer.on_region_changed(
            &mut ObserverContext::new(&new_region(region_id, version)),
            RegionChangeEvent::Update,
            role,
        );
    }

    fn remove_region(
        observer: &LeaderChangeObserver,
        region_id: u64,
        version: u64,
        role: StateRole,
    ) {
        observer.on_region_changed(
            &mut ObserverContext::new(&new_region(region_id, version)),
            RegionChangeEvent::Destroy,
            role,
        );
    }

    fn get_entry(c: &ReadTsCache, region_id: u64) -> Option<RegionMaxTsRecord> {
        c.get_entry(region_id).map(|entry| entry.rl().clone())
    }

    fn region_max_ts_record(is_ready: bool, version: u64, max_read_ts: u64) -> RegionMaxTsRecord {
        RegionMaxTsRecord {
            is_ready,
            version,
            max_read_ts,
        }
    }

    #[test]
    fn test_read_ts_cache() {
        let (c, update_task_rx) = ReadTsCache::new_mock();
        let mut observer = LeaderChangeObserver::new(Arc::clone(&c.inner));

        let wait_timeout = Duration::from_secs(1);

        let (tx, sync_rx) = std_mpsc::sync_channel(100);
        observer.sync_signal_tx = Some(tx);

        // Creates region 1.
        role_change(&observer, 1, 2, StateRole::Leader);
        sync_rx.recv_timeout(wait_timeout).unwrap();
        assert_eq!(get_entry(&c, 1).unwrap(), region_max_ts_record(false, 2, 0));
        assert_eq!(update_task_rx.recv_timeout(wait_timeout).unwrap(), (1, 2));

        // Updates ts to 100.
        c.report_read_ts(1, 100);
        assert_eq!(
            get_entry(&c, 1).unwrap(),
            region_max_ts_record(false, 2, 100)
        );
        // Returns 0 if not ready.
        assert_eq!(c.get_max_read_ts(1, 2), 0);

        // Simulates finishing getting TSO.
        ReadTsCache::make_ready(&c.inner, 1, 2, 150);
        assert_eq!(
            get_entry(&c, 1).unwrap(),
            region_max_ts_record(true, 2, 150)
        );
        assert_eq!(c.get_max_read_ts(1, 2), 150);
        // Returns 0 if region doesn't exist.
        assert_eq!(c.get_max_read_ts(2, 2), 0);
        // Returns 0 if version doesn't match.
        assert_eq!(c.get_max_read_ts(1, 3), 0);
        assert_eq!(c.get_max_read_ts(1, 1), 0);

        // Updates region version.
        update_region(&observer, 1, 3, StateRole::Leader);
        sync_rx.recv_timeout(wait_timeout).unwrap();
        assert_eq!(
            get_entry(&c, 1).unwrap(),
            region_max_ts_record(false, 3, 150)
        );
        assert_eq!(update_task_rx.recv_timeout(wait_timeout).unwrap(), (1, 3));
        assert_eq!(c.get_max_read_ts(1, 3), 0);

        c.report_read_ts(1, 200);
        ReadTsCache::make_ready(&c.inner, 1, 3, 180);
        assert_eq!(
            get_entry(&c, 1).unwrap(),
            region_max_ts_record(true, 3, 200)
        );

        // Removes the region due to it's no longer a leader.
        role_change(&observer, 1, 3, StateRole::Follower);
        sync_rx.recv_timeout(wait_timeout).unwrap();
        assert!(get_entry(&c, 1).is_none());

        create_region(&observer, 3, 3, StateRole::Leader);
        sync_rx.recv_timeout(wait_timeout).unwrap();
        assert_eq!(get_entry(&c, 3).unwrap(), region_max_ts_record(false, 3, 0));
        assert_eq!(update_task_rx.recv_timeout(wait_timeout).unwrap(), (3, 3));

        remove_region(&observer, 3, 2, StateRole::Leader);
        sync_rx.recv_timeout(wait_timeout).unwrap();
        // Not removed due to version not match.
        assert_eq!(get_entry(&c, 3).unwrap(), region_max_ts_record(false, 3, 0));

        remove_region(&observer, 3, 3, StateRole::Leader);
        sync_rx.recv_timeout(wait_timeout).unwrap();
        assert!(get_entry(&c, 3).is_none());

        // Non-leader regions' changing are ignored.
        create_region(&observer, 4, 1, StateRole::Follower);
        update_region(&observer, 4, 1, StateRole::Follower);
        remove_region(&observer, 4, 1, StateRole::Follower);

        drop(observer);
        drop(c);
        assert_eq!(
            sync_rx.recv_timeout(wait_timeout).unwrap_err(),
            std_mpsc::RecvTimeoutError::Disconnected
        );
        assert_eq!(
            update_task_rx.recv_timeout(wait_timeout).unwrap_err(),
            std_mpsc::RecvTimeoutError::Disconnected
        );
    }
}
