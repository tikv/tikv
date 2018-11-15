// Copyright 2018 PingCAP, Inc.
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

use super::engine::{Engine, Error as EngineError, ScanMode, StatisticsSummary};
use super::metrics::*;
use super::mvcc::{MvccReader, MvccTxn};
use super::{Callback, Error, Key, Result, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::Context;
use raftstore::store::keys;
use raftstore::store::msg::Msg as RaftStoreMsg;
use raftstore::store::util::delete_all_in_range_cf;
use rocksdb::rocksdb::DB;
use server::transport::{RaftStoreRouter, ServerRaftStoreRouter};
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use util::rocksdb::get_cf_handle;
use util::time::{duration_to_sec, SlowTimer};
use util::worker::{self, Builder, Runnable, ScheduleError, Worker};

// TODO: make it configurable.
pub const GC_BATCH_SIZE: usize = 512;

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

pub const GC_MAX_PENDING_TASKS: usize = 2;
const GC_SNAPSHOT_TIMEOUT_SECS: u64 = 10;
const GC_TASK_SLOW_SECONDS: u64 = 30;

enum GCTask {
    GC {
        ctx: Context,
        safe_point: u64,
        callback: Callback<()>,
    },
    UnsafeDestroyRange {
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    },
}

impl GCTask {
    pub fn take_callback(self) -> Callback<()> {
        match self {
            GCTask::GC { callback, .. } => callback,
            GCTask::UnsafeDestroyRange { callback, .. } => callback,
        }
    }

    pub fn get_label(&self) -> &'static str {
        match self {
            GCTask::GC { .. } => "gc",
            GCTask::UnsafeDestroyRange { .. } => "unsafe_destroy_range",
        }
    }
}

impl Display for GCTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            GCTask::GC {
                ctx, safe_point, ..
            } => {
                let epoch = format!("{:?}", ctx.region_epoch.as_ref());
                f.debug_struct("GC")
                    .field("region", &ctx.get_region_id())
                    .field("epoch", &epoch)
                    .field("safe_point", safe_point)
                    .finish()
            }
            GCTask::UnsafeDestroyRange {
                start_key, end_key, ..
            } => f
                .debug_struct("UnsafeDestroyRange")
                .field("start_key", &format!("{}", start_key))
                .field("end_key", &format!("{}", end_key))
                .finish(),
        }
    }
}

/// `GCRunner` is used to perform GC on the engine
struct GCRunner<E: Engine> {
    engine: E,
    local_storage: Option<Arc<DB>>,
    raft_store_router: Option<ServerRaftStoreRouter>,

    ratio_threshold: f64,

    stats: StatisticsSummary,
}

impl<E: Engine> GCRunner<E> {
    pub fn new(
        engine: E,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
        ratio_threshold: f64,
    ) -> Self {
        Self {
            engine,
            local_storage,
            raft_store_router,
            ratio_threshold,
            stats: StatisticsSummary::default(),
        }
    }

    fn get_snapshot(&self, ctx: &mut Context) -> Result<E::Snap> {
        let timeout = Duration::from_secs(GC_SNAPSHOT_TIMEOUT_SECS);
        match wait_op!(|cb| self.engine.async_snapshot(ctx, cb), timeout) {
            Some((cb_ctx, Ok(snapshot))) => {
                if let Some(term) = cb_ctx.term {
                    ctx.set_term(term);
                }
                Ok(snapshot)
            }
            Some((_, Err(e))) => Err(e),
            None => Err(EngineError::Timeout(timeout)),
        }.map_err(Error::from)
    }

    /// Scan keys in the region. Returns scanned keys if any, and a key indicating scan progress
    fn scan_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: u64,
        from: Option<Key>,
        limit: usize,
    ) -> Result<(Vec<Key>, Option<Key>)> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            None,
            None,
            ctx.get_isolation_level(),
        );

        let is_range_start = from.is_none();

        // range start gc with from == None, and this is an optimization to
        // skip gc before scanning all data.
        let skip_gc = is_range_start && !reader.need_gc(safe_point, self.ratio_threshold);
        let res = if skip_gc {
            KV_GC_SKIPPED_COUNTER.inc();
            Ok((vec![], None))
        } else {
            reader
                .scan_keys(from, limit)
                .map_err(Error::from)
                .and_then(|(keys, next)| {
                    if keys.is_empty() {
                        assert!(next.is_none());
                        if is_range_start {
                            KV_GC_EMPTY_RANGE_COUNTER.inc();
                        }
                    }
                    Ok((keys, next))
                })
        };
        self.stats.add_statistics(reader.get_statistics());
        res
    }

    /// Clean up outdated data.
    fn gc_keys(
        &mut self,
        ctx: &mut Context,
        safe_point: u64,
        keys: Vec<Key>,
        mut next_scan_key: Option<Key>,
    ) -> Result<Option<Key>> {
        let snapshot = self.get_snapshot(ctx)?;
        let mut txn = MvccTxn::new(snapshot, 0, !ctx.get_not_fill_cache()).unwrap();
        for k in keys {
            let gc_info = txn.gc(k.clone(), safe_point)?;

            if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                info!(
                    "[region {}] GC found at least {} versions for key {}",
                    ctx.get_region_id(),
                    gc_info.found_versions,
                    k
                );
            }
            // TODO: we may delete only part of the versions in a batch, which may not beyond
            // the logging threshold `GC_LOG_DELETED_VERSION_THRESHOLD`.
            if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                info!(
                    "[region {}] GC deleted {} versions for key {}",
                    ctx.get_region_id(),
                    gc_info.deleted_versions,
                    k
                );
            }

            if !gc_info.is_completed {
                next_scan_key = Some(k);
                break;
            }
        }
        self.stats.add_statistics(&txn.take_statistics());

        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            self.engine.write(ctx, modifies)?;
        }
        Ok(next_scan_key)
    }

    fn gc(&mut self, ctx: &mut Context, safe_point: u64) -> Result<()> {
        debug!(
            "doing gc on region {}, safe_point {}",
            ctx.get_region_id(),
            safe_point
        );

        let mut next_key = None;
        loop {
            let (keys, next) = self
                .scan_keys(ctx, safe_point, next_key, GC_BATCH_SIZE)
                .map_err(|e| {
                    warn!("gc scan_keys failed on region {}: {:?}", safe_point, &e);
                    e
                })?;
            if keys.is_empty() {
                break;
            }

            next_key = self.gc_keys(ctx, safe_point, keys, next).map_err(|e| {
                warn!("gc_keys failed on region {}: {:?}", safe_point, &e);
                e
            })?;
            if next_key.is_none() {
                break;
            }
        }

        debug!(
            "gc on region {}, safe_point {} has finished",
            ctx.get_region_id(),
            safe_point
        );
        Ok(())
    }

    fn unsafe_destroy_range(&self, _: &Context, start_key: &Key, end_key: &Key) -> Result<()> {
        info!(
            "unsafe destroy range start_key: {}, end_key: {} started.",
            start_key, end_key
        );

        // TODO: Refine usage of errors

        let local_storage = self.local_storage.as_ref().ok_or_else(|| {
            let e: Error = box_err!("unsafe destroy range not supported: local_storage not set");
            warn!("unsafe destroy range failed: {:?}", &e);
            e
        })?;

        // Convert keys to RocksDB layer form
        // TODO: Logic coupled with raftstore's implementation. Maybe better design is to do it in
        // somewhere of the same layer with apply_worker.
        let start_data_key = keys::data_key(start_key.as_encoded());
        let end_data_key = keys::data_end_key(end_key.as_encoded());

        let cfs = &[CF_LOCK, CF_DEFAULT, CF_WRITE];

        // First, call delete_files_in_range to free as much disk space as possible
        let delete_files_start_time = Instant::now();
        for cf in cfs {
            let cf_handle = get_cf_handle(local_storage, cf).unwrap();
            local_storage
                .delete_files_in_range_cf(cf_handle, &start_data_key, &end_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!(
                        "unsafe destroy range failed at delete_files_in_range_cf: {:?}",
                        e
                    );
                    e
                })?;
        }

        info!(
            "unsafe destroy range start_key: {}, end_key: {} finished deleting files in range, cost time: {:?}",
            start_key, end_key, delete_files_start_time.elapsed(),
        );

        // Then, delete all remaining keys in the range.
        let cleanup_all_start_time = Instant::now();
        for cf in cfs {
            // TODO: set use_delete_range with config here.
            delete_all_in_range_cf(local_storage, cf, &start_data_key, &end_data_key, false)
                .map_err(|e| {
                    let e: Error = box_err!(e);
                    warn!(
                        "unsafe destroy range failed at delete_all_in_range_cf: {:?}",
                        e
                    );
                    e
                })?;
        }

        let cleanup_all_time_cost = cleanup_all_start_time.elapsed();

        if let Some(router) = self.raft_store_router.as_ref() {
            router
                .send(RaftStoreMsg::ClearRegionSizeInRange {
                    start_key: start_key.as_encoded().to_vec(),
                    end_key: end_key.as_encoded().to_vec(),
                })
                .unwrap_or_else(|e| {
                    // Warn and ignore it.
                    warn!(
                        "unsafe destroy range: failed sending ClearRegionSizeInRange: {:?}",
                        e
                    );
                });
        } else {
            warn!("unsafe destroy range: can't clear region size information: raft_store_router not set");
        }

        info!(
            "unsafe destroy range start_key: {}, end_key: {} finished cleaning up all, cost time {:?}",
            start_key, end_key, cleanup_all_time_cost,
        );
        Ok(())
    }

    fn handle_gc_worker_task(&mut self, mut task: GCTask) {
        let label = task.get_label();
        GC_GCTASK_COUNTER_VEC.with_label_values(&[label]).inc();

        let timer = SlowTimer::from_secs(GC_TASK_SLOW_SECONDS);

        let result = match &mut task {
            GCTask::GC {
                ctx, safe_point, ..
            } => self.gc(ctx, *safe_point),
            GCTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                ..
            } => self.unsafe_destroy_range(ctx, start_key, end_key),
        };

        GC_TASK_DURATION_HISTOGRAM_VEC
            .with_label_values(&[label])
            .observe(duration_to_sec(timer.elapsed()));
        slow_log!(timer, "{}", task);

        if result.is_err() {
            GC_GCTASK_FAIL_COUNTER_VEC.with_label_values(&[label]).inc();
        }
        (task.take_callback())(result);
    }
}

impl<E: Engine> Runnable<GCTask> for GCRunner<E> {
    #[inline]
    fn run(&mut self, task: GCTask) {
        self.handle_gc_worker_task(task);
    }

    // The default implementation of `run_batch` prints a warning to log when it takes over 1 second
    // to handle a task. It's not proper here, so override it to remove the log.
    #[inline]
    fn run_batch(&mut self, tasks: &mut Vec<GCTask>) {
        for task in tasks.drain(..) {
            self.run(task);
        }
    }

    fn on_tick(&mut self) {
        let stats = mem::replace(&mut self.stats, StatisticsSummary::default());
        for (cf, details) in stats.stat.details() {
            for (tag, count) in details {
                GC_KEYS_COUNTER_VEC
                    .with_label_values(&[cf, tag])
                    .inc_by(count as i64);
            }
        }
    }
}

/// `GCWorker` is used to schedule GC operations
#[derive(Clone)]
pub struct GCWorker<E: Engine> {
    engine: E,
    /// `local_storage` represent the underlying RocksDB of the `engine`.
    local_storage: Option<Arc<DB>>,
    /// `raft_store_router` is useful to signal raftstore clean region size informations.
    raft_store_router: Option<ServerRaftStoreRouter>,

    ratio_threshold: f64,

    worker: Arc<Mutex<Worker<GCTask>>>,
    worker_scheduler: worker::Scheduler<GCTask>,
}

impl<E: Engine> GCWorker<E> {
    pub fn new(
        engine: E,
        local_storage: Option<Arc<DB>>,
        raft_store_router: Option<ServerRaftStoreRouter>,
        ratio_threshold: f64,
    ) -> GCWorker<E> {
        let worker = Arc::new(Mutex::new(
            Builder::new("gc-worker")
                .pending_capacity(GC_MAX_PENDING_TASKS)
                .create(),
        ));
        let worker_scheduler = worker.lock().unwrap().scheduler();
        GCWorker {
            engine,
            local_storage,
            raft_store_router,
            ratio_threshold,
            worker,
            worker_scheduler,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        let runner = GCRunner::new(
            self.engine.clone(),
            self.local_storage.take(),
            self.raft_store_router.take(),
            self.ratio_threshold,
        );
        self.worker
            .lock()
            .unwrap()
            .start(runner)
            .map_err(|e| box_err!("failed to start gc_worker, err: {:?}", e))
    }

    pub fn stop(&self) -> Result<()> {
        let h = self.worker.lock().unwrap().stop().unwrap();
        if let Err(e) = h.join() {
            Err(box_err!("failed to join gc_worker handle, err: {:?}", e))
        } else {
            Ok(())
        }
    }

    fn handle_schedule_error(e: ScheduleError<GCTask>) -> Result<()> {
        match e {
            ScheduleError::Full(task) => {
                GC_TOO_BUSY_COUNTER.inc();
                (task.take_callback())(Err(Error::GCWorkerTooBusy));
                Ok(())
            }
            _ => Err(box_err!("failed to schedule gc task: {:?}", e)),
        }
    }

    pub fn async_gc(&self, ctx: Context, safe_point: u64, callback: Callback<()>) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask::GC {
                ctx,
                safe_point,
                callback,
            })
            .or_else(Self::handle_schedule_error)
    }

    /// Clean up all keys in a range and quickly free the disk space. The range might span over
    /// multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
    /// on RocksDB, bypassing the Raft layer. User must promise that, after calling `destroy_range`,
    /// the range will never be accessed any more. However, `destroy_range` is allowed to be called
    /// multiple times on an single range.
    pub fn async_unsafe_destroy_range(
        &self,
        ctx: Context,
        start_key: Key,
        end_key: Key,
        callback: Callback<()>,
    ) -> Result<()> {
        self.worker_scheduler
            .schedule(GCTask::UnsafeDestroyRange {
                ctx,
                start_key,
                end_key,
                callback,
            })
            .or_else(Self::handle_schedule_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;
    use std::collections::BTreeMap;
    use storage::{Mutation, Options, Storage};
    use storage::{TestEngineBuilder, TestStorageBuilder};

    /// Assert the data in `storage` is the same as `expected_data`. Keys in `expected_data` should
    /// be encoded form without ts.
    fn check_data<E: Engine>(storage: &Storage<E>, expected_data: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let scan_res = storage
            .async_scan(
                Context::default(),
                Key::from_encoded_slice(b""),
                None,
                expected_data.len() + 1,
                1,
                Options::default(),
            )
            .wait()
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
        start_ts: u64,
        commit_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<()> {
        // Return Result from this function so we can use the `wait_op` macro here.

        let engine = TestEngineBuilder::new().build().unwrap();
        let db = engine.get_rocksdb();
        let storage = TestStorageBuilder::from_engine(engine)
            .local_storage(db)
            .build()
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

        // Write these data to the storage.
        wait_op!(|cb| storage.async_prewrite(
            Context::default(),
            mutations,
            primary,
            start_ts,
            Options::default(),
            cb
        )).unwrap()
            .unwrap();

        // Commit.
        let keys: Vec<_> = init_keys.iter().map(|k| Key::from_raw(k)).collect();
        wait_op!(|cb| storage.async_commit(Context::default(), keys, start_ts, commit_ts, cb))
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
        wait_op!(|cb| storage.async_unsafe_destroy_range(
            Context::default(),
            start_key,
            end_key,
            cb
        )).unwrap()
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
        ).unwrap();

        test_destroy_range_impl(
            &[b"key1".to_vec(), b"key9".to_vec()],
            5,
            10,
            b"key3",
            b"key7",
        ).unwrap();

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
        ).unwrap();

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
        ).unwrap();

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
        ).unwrap();

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
        ).unwrap();
    }
}
