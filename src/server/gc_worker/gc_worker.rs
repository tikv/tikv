// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use engine_rocks::RocksEngine;
use kvproto::kvrpcpb::{Context, LockInfo};
use pd_client::ClusterVersion;
use raftstore::coprocessor::{CoprocessorHost, RegionInfoProvider};
use raftstore::router::RaftStoreRouter;
use tikv_util::config::VersionTrack;
use tikv_util::worker::{LazyWorker, ScheduleError, Scheduler};
use txn_types::{Key, TimeStamp};

use crate::server::metrics::*;
use crate::storage::kv::Engine;

use super::applied_lock_collector::{AppliedLockCollector, Callback as LockCollectorCallback};
use super::config::{GcConfig, GcWorkerConfigManager};
use super::gc_runner::{AutoGcConfig, GcExecutor, GcRunner, GcSafePointProvider, GcTask};
use super::{Callback, CompactionFilterInitializer, Error, ErrorInner, Result};

pub const GC_MAX_EXECUTING_TASKS: usize = 10;

/// When we failed to schedule a `GcTask` to `GcRunner`, use this to handle the `ScheduleError`.
fn handle_gc_task_schedule_error(e: ScheduleError<GcTask>) -> Result<()> {
    error!("failed to schedule gc task"; "err" => %e);
    Err(box_err!("failed to schedule gc task: {:?}", e))
}

/// Used to schedule GC operations.
pub struct GcWorker {
    config_manager: GcWorkerConfigManager,
    /// How many requests are scheduled from outside and unfinished.
    scheduled_tasks: Arc<AtomicUsize>,
    /// How many strong references. The worker will be stopped
    /// once there are no more references.
    refs: Arc<AtomicUsize>,
    worker: Arc<Mutex<LazyWorker<GcTask>>>,
    worker_scheduler: Scheduler<GcTask>,
    stop: Arc<AtomicBool>,

    applied_lock_collector: Option<Arc<AppliedLockCollector>>,
    cluster_version: ClusterVersion,
}

impl Clone for GcWorker {
    #[inline]
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::SeqCst);

        Self {
            config_manager: self.config_manager.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
            refs: self.refs.clone(),
            worker: self.worker.clone(),
            worker_scheduler: self.worker_scheduler.clone(),
            stop: self.stop.clone(),
            applied_lock_collector: self.applied_lock_collector.clone(),
            cluster_version: self.cluster_version.clone(),
        }
    }
}

impl Drop for GcWorker {
    #[inline]
    fn drop(&mut self) {
        let refs = self.refs.fetch_sub(1, Ordering::SeqCst);

        if refs != 1 {
            return;
        }

        let r = self.stop();
        if let Err(e) = r {
            error!(?e; "Failed to stop gc_worker");
        }
    }
}

impl GcWorker {
    pub fn new(cfg: GcConfig, cluster_version: ClusterVersion) -> GcWorker {
        let worker = LazyWorker::new("gc-worker");
        let worker_scheduler = worker.scheduler();
        GcWorker {
            config_manager: GcWorkerConfigManager(Arc::new(VersionTrack::new(cfg))),
            scheduled_tasks: Arc::new(AtomicUsize::new(0)),
            refs: Arc::new(AtomicUsize::new(1)),
            worker: Arc::new(Mutex::new(worker)),
            worker_scheduler,
            stop: Arc::new(AtomicBool::new(false)),
            applied_lock_collector: None,
            cluster_version,
        }
    }

    pub fn start_auto_gc<E, S, R, RR>(
        &self,
        cfg: AutoGcConfig,
        safe_point_provider: S,
        region_info_provider: R,
        engine: E,
        raft_store_router: RR,
    ) -> Result<Arc<AtomicU64>>
    where
        S: GcSafePointProvider,
        R: RegionInfoProvider,
        E: Engine,
        RR: RaftStoreRouter<RocksEngine> + 'static,
    {
        let safe_point = Arc::new(AtomicU64::new(0));

        let kvdb = engine.kv_engine();
        let cfg_mgr = self.config_manager.clone();
        let cluster_version = self.cluster_version.clone();
        kvdb.init_compaction_filter(safe_point.clone(), cfg_mgr, cluster_version);

        let executor = GcExecutor::new(
            engine,
            raft_store_router,
            self.config_manager.0.clone().tracker("gc-woker".to_owned()),
            self.config_manager.value().clone(),
        );
        let mut mgr = GcRunner::new(
            cfg,
            safe_point_provider,
            region_info_provider,
            safe_point.clone(),
            self.config_manager.clone(),
            self.cluster_version.clone(),
            self.stop.clone(),
            Box::new(executor),
        );
        mgr.start();
        self.worker.lock().unwrap().start_with_timer(mgr);
        Ok(safe_point)
    }

    pub fn start_observe_lock_apply(
        &mut self,
        coprocessor_host: &mut CoprocessorHost<RocksEngine>,
    ) -> Result<()> {
        assert!(self.applied_lock_collector.is_none());
        let collector = Arc::new(AppliedLockCollector::new(coprocessor_host)?);
        self.applied_lock_collector = Some(collector);
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        // Stop self.
        self.worker.lock().unwrap().stop();
        Ok(())
    }

    pub fn scheduler(&self) -> Scheduler<GcTask> {
        self.worker_scheduler.clone()
    }

    /// Check whether GCWorker is busy. If busy, callback will be invoked with an error that
    /// indicates GCWorker is busy; otherwise, return a new callback that invokes the original
    /// callback as well as decrease the scheduled task counter.
    fn check_is_busy<T: 'static>(&self, callback: Callback<T>) -> Option<Callback<T>> {
        if self.scheduled_tasks.fetch_add(1, Ordering::SeqCst) >= GC_MAX_EXECUTING_TASKS {
            self.scheduled_tasks.fetch_sub(1, Ordering::SeqCst);
            callback(Err(Error::from(ErrorInner::GcWorkerTooBusy)));
            return None;
        }
        let scheduled_tasks = Arc::clone(&self.scheduled_tasks);
        Some(Box::new(move |r| {
            scheduled_tasks.fetch_sub(1, Ordering::SeqCst);
            callback(r);
        }))
    }

    /// Only for tests.
    pub fn gc(&self, safe_point: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            let start_key = vec![];
            let end_key = vec![];
            self.worker_scheduler
                .schedule(GcTask::Gc {
                    region_id: 0,
                    start_key,
                    end_key,
                    safe_point,
                    callback: Some(callback),
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    /// Cleans up all keys in a range and quickly free the disk space. The range might span over
    /// multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
    /// on RocksDB, bypassing the Raft layer. User must promise that, after calling `destroy_range`,
    /// the range will never be accessed any more. However, `destroy_range` is allowed to be called
    /// multiple times on an single range.
    pub fn unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.unsafe_destroy_range.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::UnsafeDestroyRange {
                    ctx,
                    start_key,
                    end_key,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn get_config_manager(&self) -> GcWorkerConfigManager {
        self.config_manager.clone()
    }

    pub fn physical_scan_lock(
        &self,
        ctx: Context,
        max_ts: TimeStamp,
        start_key: Key,
        limit: usize,
        callback: Callback<Vec<LockInfo>>,
    ) -> Result<()> {
        GC_COMMAND_COUNTER_VEC_STATIC.physical_scan_lock.inc();
        self.check_is_busy(callback).map_or(Ok(()), |callback| {
            self.worker_scheduler
                .schedule(GcTask::PhysicalScanLock {
                    ctx,
                    max_ts,
                    start_key,
                    limit,
                    callback,
                })
                .or_else(handle_gc_task_schedule_error)
        })
    }

    pub fn start_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.start_collecting(max_ts, callback))
    }

    pub fn get_collected_locks(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<(Vec<LockInfo>, bool)>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.get_collected_locks(max_ts, callback))
    }

    pub fn stop_collecting(
        &self,
        max_ts: TimeStamp,
        callback: LockCollectorCallback<()>,
    ) -> Result<()> {
        self.applied_lock_collector
            .as_ref()
            .ok_or_else(|| box_err!("applied_lock_collector not supported"))
            .and_then(move |c| c.stop_collecting(max_ts, callback))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::mpsc::channel;

    use engine_rocks::RocksSnapshot;
    use engine_traits::KvEngine;
    use futures::executor::block_on;
    use kvproto::{kvrpcpb::Op, metapb};
    use raftstore::router::RaftStoreBlackHole;
    use raftstore::store::RegionSnapshot;
    use tikv_util::codec::number::NumberEncoder;
    use tikv_util::future::paired_future_callback;
    use txn_types::Mutation;

    use crate::server::gc_worker::gc_runner::make_mock_auto_gc_cfg;
    use crate::server::gc_worker::gc_runner::{MockRegionInfoProvider, MockSafePointProvider};
    use crate::storage::kv::{
        self, write_modifies, Callback as EngineCallback, Modify, Result as EngineResult,
        SnapContext, TestEngineBuilder, WriteData,
    };
    use crate::storage::lock_manager::DummyLockManager;
    use crate::storage::{txn::commands, Engine, Storage, TestStorageBuilder};

    use super::*;

    /// A wrapper of engine that adds the 'z' prefix to keys internally.
    /// For test engines, they writes keys into db directly, but in production a 'z' prefix will be
    /// added to keys by raftstore layer before writing to db. Some functionalities of `GCWorker`
    /// bypasses Raft layer, so they needs to know how data is actually represented in db. This
    /// wrapper allows test engines write 'z'-prefixed keys to db.
    #[derive(Clone)]
    struct PrefixedEngine(kv::RocksEngine);

    impl Engine for PrefixedEngine {
        // Use RegionSnapshot which can remove the z prefix internally.
        type Snap = RegionSnapshot<RocksSnapshot>;
        type Local = RocksEngine;

        fn kv_engine(&self) -> RocksEngine {
            self.0.kv_engine()
        }

        fn snapshot_on_kv_engine(
            &self,
            start_key: &[u8],
            end_key: &[u8],
        ) -> kv::Result<Self::Snap> {
            let mut region = metapb::Region::default();
            region.set_start_key(start_key.to_owned());
            region.set_end_key(end_key.to_owned());
            // Use a fake peer to avoid panic.
            region.mut_peers().push(Default::default());
            Ok(RegionSnapshot::from_snapshot(
                Arc::new(self.kv_engine().snapshot()),
                Arc::new(region),
            ))
        }

        fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
            for modify in &mut modifies {
                match modify {
                    Modify::Delete(_, ref mut key) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::Put(_, ref mut key, _) => {
                        let bytes = keys::data_key(key.as_encoded());
                        *key = Key::from_encoded(bytes);
                    }
                    Modify::DeleteRange(_, ref mut key1, ref mut key2, _) => {
                        let bytes = keys::data_key(key1.as_encoded());
                        *key1 = Key::from_encoded(bytes);
                        let bytes = keys::data_end_key(key2.as_encoded());
                        *key2 = Key::from_encoded(bytes);
                    }
                }
            }
            write_modifies(&self.kv_engine(), modifies)
        }

        fn async_write(
            &self,
            ctx: &Context,
            mut batch: WriteData,
            callback: EngineCallback<()>,
        ) -> EngineResult<()> {
            batch.modifies.iter_mut().for_each(|modify| match modify {
                Modify::Delete(_, ref mut key) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::Put(_, ref mut key, _) => {
                    *key = Key::from_encoded(keys::data_key(key.as_encoded()));
                }
                Modify::DeleteRange(_, ref mut start_key, ref mut end_key, _) => {
                    *start_key = Key::from_encoded(keys::data_key(start_key.as_encoded()));
                    *end_key = Key::from_encoded(keys::data_end_key(end_key.as_encoded()));
                }
            });
            self.0.async_write(ctx, batch, callback)
        }

        fn async_snapshot(
            &self,
            ctx: SnapContext<'_>,
            callback: EngineCallback<Self::Snap>,
        ) -> EngineResult<()> {
            self.0.async_snapshot(
                ctx,
                Box::new(move |(cb_ctx, r)| {
                    callback((
                        cb_ctx,
                        r.map(|snap| {
                            let mut region = metapb::Region::default();
                            // Add a peer to pass initialized check.
                            region.mut_peers().push(metapb::Peer::default());
                            RegionSnapshot::from_snapshot(snap, Arc::new(region))
                        }),
                    ))
                }),
            )
        }
    }

    /// Assert the data in `storage` is the same as `expected_data`. Keys in `expected_data` should
    /// be encoded form without ts.
    fn check_data<E: Engine>(
        storage: &Storage<E, DummyLockManager>,
        expected_data: &BTreeMap<Vec<u8>, Vec<u8>>,
    ) {
        let scan_res = block_on(storage.scan(
            Context::default(),
            Key::from_encoded_slice(b""),
            None,
            expected_data.len() + 1,
            0,
            1.into(),
            false,
            false,
        ))
        .unwrap();

        let all_equal = scan_res
            .into_iter()
            .map(|res| res.unwrap())
            .zip(expected_data.iter())
            .all(|((k1, v1), (k2, v2))| &k1 == k2 && &v1 == v2);
        assert!(all_equal);
    }

    fn test_destroy_range_impl(
        init_keys: &[Vec<u8>],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.

        let engine = TestEngineBuilder::new().build().unwrap();
        let storage =
            TestStorageBuilder::from_engine_and_lock_mgr(engine.clone(), DummyLockManager {})
                .build()
                .unwrap();

        let gc_worker = GcWorker::new(
            GcConfig::default(),
            ClusterVersion::new(semver::Version::new(5, 0, 0)),
        );
        let (cfg, provider) = make_mock_auto_gc_cfg();
        gc_worker
            .start_auto_gc(
                cfg,
                provider,
                MockRegionInfoProvider::default(),
                engine,
                RaftStoreBlackHole,
            )
            .unwrap();
        // Convert keys to key value pairs, where the value is "value-{key}".
        let data: BTreeMap<_, _> = init_keys
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extend_from_slice(key);
                (Key::from_raw(key).into_encoded(), value)
            })
            .collect();

        // Generate `Mutation`s from these keys.
        let mutations: Vec<_> = init_keys
            .iter()
            .map(|key| {
                let mut value = b"value-".to_vec();
                value.extend_from_slice(key);
                Mutation::Put((Key::from_raw(key), value))
            })
            .collect();
        let primary = init_keys[0].clone();

        let start_ts = start_ts.into();

        // Write these data to the storage.
        wait_op!(|cb| storage.sched_txn_command(
            commands::Prewrite::with_defaults(mutations, primary, start_ts),
            cb,
        ))
        .unwrap()
        .unwrap();

        // Commit.
        let keys: Vec<_> = init_keys.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| storage.sched_txn_command(
            commands::Commit::new(keys, start_ts, commit_ts.into(), Context::default()),
            cb
        ))
        .unwrap()
        .unwrap();

        // Assert these data is successfully written to the storage.
        check_data(&storage, &data);

        let start_key = Key::from_raw(start_key);
        let end_key = Key::from_raw(end_key);

        // Calculate expected data set after deleting the range.
        let data: BTreeMap<_, _> = data
            .into_iter()
            .filter(|(k, _)| k < start_key.as_encoded() || k >= end_key.as_encoded())
            .collect();

        // Invoke unsafe destroy range.
        wait_op!(|cb| gc_worker.unsafe_destroy_range(Context::default(), start_key, end_key, cb))
            .unwrap()
            .unwrap();

        // Check remaining data is as expected.
        check_data(&storage, &data);

        Ok(())
    }

    #[test]
    fn test_destroy_range() {
        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2",
            b"key4",
        )
        .unwrap();

        test_destroy_range_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
                b"key6".to_vec(),
                b"key7".to_vec(),
            ],
            5,
            10,
            b"key1",
            b"key9",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ],
            5,
            10,
            b"key2\x00",
            b"key4",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00\x00",
        )
        .unwrap();

        test_destroy_range_impl(
            &[
                b"key1".to_vec(),
                b"key1\x00".to_vec(),
                b"key1\x00\x00".to_vec(),
                b"key1\x00\x00\x00".to_vec(),
            ],
            5,
            10,
            b"key1\x00",
            b"key1\x00",
        )
        .unwrap();
    }

    #[test]
    fn test_physical_scan_lock() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let prefixed_engine = PrefixedEngine(engine);
        let storage = TestStorageBuilder::<_, DummyLockManager>::from_engine_and_lock_mgr(
            prefixed_engine.clone(),
            DummyLockManager {},
        )
        .build()
        .unwrap();
        let gc_worker = GcWorker::new(GcConfig::default(), ClusterVersion::default());

        let (cfg, provider) = make_mock_auto_gc_cfg();
        gc_worker
            .start_auto_gc(
                cfg,
                provider,
                MockRegionInfoProvider::default(),
                prefixed_engine,
                RaftStoreBlackHole,
            )
            .unwrap();

        let physical_scan_lock = |max_ts: u64, start_key, limit| {
            let (cb, f) = paired_future_callback();
            gc_worker
                .physical_scan_lock(Context::default(), max_ts.into(), start_key, limit, cb)
                .unwrap();
            block_on(f).unwrap()
        };

        let mut expected_lock_info = Vec::new();

        // Put locks into the storage.
        for i in 0..50 {
            let mut k = vec![];
            k.encode_u64(i).unwrap();
            let v = k.clone();

            let mutation = Mutation::Put((Key::from_raw(&k), v));

            let lock_ts = 10 + i % 3;

            // Collect all locks with ts <= 11 to check the result of physical_scan_lock.
            if lock_ts <= 11 {
                let mut info = LockInfo::default();
                info.set_primary_lock(k.clone());
                info.set_lock_version(lock_ts);
                info.set_key(k.clone());
                info.set_lock_type(Op::Put);
                expected_lock_info.push(info)
            }

            let (tx, rx) = channel();
            storage
                .sched_txn_command(
                    commands::Prewrite::with_defaults(vec![mutation], k, lock_ts.into()),
                    Box::new(move |res| tx.send(res).unwrap()),
                )
                .unwrap();
            rx.recv()
                .unwrap()
                .unwrap()
                .locks
                .into_iter()
                .for_each(|r| r.unwrap());
        }

        let res = physical_scan_lock(11, Key::from_raw(b""), 50).unwrap();
        assert_eq!(res, expected_lock_info);

        let res = physical_scan_lock(11, Key::from_raw(b""), 5).unwrap();
        assert_eq!(res[..], expected_lock_info[..5]);

        let mut start_key = vec![];
        start_key.encode_u64(4).unwrap();
        let res = physical_scan_lock(11, Key::from_raw(&start_key), 6).unwrap();
        // expected_locks[3] is the key 4.
        assert_eq!(res[..], expected_lock_info[3..9]);
    }
}
