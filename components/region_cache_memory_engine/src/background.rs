// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeSet, fmt::Display, sync::Arc, thread::JoinHandle, time::Duration};

use bytes::Bytes;
use crossbeam::{
    channel::{bounded, tick, Sender},
    epoch, select,
};
use engine_rocks::RocksSnapshot;
use engine_traits::{CacheRange, IterOptions, Iterable, Iterator, CF_DEFAULT, CF_WRITE, DATA_CFS};
use parking_lot::RwLock;
use slog_global::{error, info, warn};
use tikv_util::{
    keybuilder::KeyBuilder,
    worker::{Builder, Runnable, ScheduleError, Scheduler, Worker},
};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};
use yatp::Remote;

use crate::{
    engine::{RangeCacheMemoryEngineCore, SkiplistHandle},
    keys::{decode_key, encode_key, encoding_for_filter, InternalBytes, InternalKey, ValueType},
    memory_controller::MemoryController,
    metrics::GC_FILTERED_STATIC,
};

/// Try to extract the key and `u64` timestamp from `encoded_key`.
///
/// See also: [`txn_types::Key::split_on_ts_for`]
fn split_ts(key: &[u8]) -> Result<(&[u8], u64), String> {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => Ok((key, ts.into_inner())),
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

fn parse_write(value: &[u8]) -> Result<WriteRef<'_>, String> {
    match WriteRef::parse(value) {
        Ok(write) => Ok(write),
        Err(_) => Err(format!(
            "invalid write cf value: {}",
            log_wrappers::Value(value)
        )),
    }
}

#[derive(Debug)]
pub enum BackgroundTask {
    Gc(GcTask),
    LoadRange,
    MemoryCheckAndEvict,
    DeleteRange(Vec<CacheRange>),
}

impl Display for BackgroundTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackgroundTask::Gc(ref t) => t.fmt(f),
            BackgroundTask::LoadRange => f.debug_struct("LoadTask").finish(),
            BackgroundTask::MemoryCheckAndEvict => f.debug_struct("MemoryCheckAndEvict").finish(),
            BackgroundTask::DeleteRange(ref r) => {
                f.debug_struct("DeleteRange").field("range", r).finish()
            }
        }
    }
}

#[derive(Debug)]
pub struct GcTask {
    pub safe_point: u64,
}

impl Display for GcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcTask")
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

// BgWorkManager managers the worker inits, stops, and task schedules. When
// created, it starts a worker which receives tasks such as gc task, range
// delete task, range snapshot load and so on, and starts a thread for
// periodically schedule gc tasks.
pub struct BgWorkManager {
    worker: Worker,
    scheduler: Scheduler<BackgroundTask>,
    tick_stopper: Option<(JoinHandle<()>, Sender<bool>)>,
}

impl Drop for BgWorkManager {
    fn drop(&mut self) {
        let (h, tx) = self.tick_stopper.take().unwrap();
        let _ = tx.send(true);
        let _ = h.join();
        self.worker.stop();
    }
}

impl BgWorkManager {
    pub fn new(
        core: Arc<RwLock<RangeCacheMemoryEngineCore>>,
        gc_interval: Duration,
        memory_controller: Arc<MemoryController>,
    ) -> Self {
        let worker = Worker::new("range-cache-background-worker");
        let runner = BackgroundRunner::new(core.clone(), memory_controller);
        let scheduler = worker.start("range-cache-engine-background", runner);

        let scheduler_clone = scheduler.clone();

        let (handle, tx) = BgWorkManager::start_tick(scheduler_clone, gc_interval);

        Self {
            worker,
            scheduler,
            tick_stopper: Some((handle, tx)),
        }
    }

    pub fn schedule_task(&self, task: BackgroundTask) -> Result<(), ScheduleError<BackgroundTask>> {
        self.scheduler.schedule_force(task)
    }

    fn start_tick(
        scheduler: Scheduler<BackgroundTask>,
        gc_interval: Duration,
    ) -> (JoinHandle<()>, Sender<bool>) {
        let (tx, rx) = bounded(0);
        let h = std::thread::spawn(move || {
            let ticker = tick(gc_interval);
            loop {
                select! {
                    recv(ticker) -> _ => {
                        let safe_point = TimeStamp::physical_now() - gc_interval.as_millis() as u64;
                        let safe_point = TimeStamp::compose(safe_point, 0).into_inner();
                        if let Err(e) = scheduler.schedule(BackgroundTask::Gc(GcTask {safe_point})) {
                            error!(
                                "schedule range cache engine gc failed";
                                "err" => ?e,
                            );
                        }
                    },
                    recv(rx) -> r => {
                        if let Err(e) = r {
                            error!(
                                "receive error in range cache engien gc ticker";
                                "err" => ?e,
                            );
                        }
                        return;
                    },
                }
            }
        });
        (h, tx)
    }
}

#[derive(Clone)]
struct BackgroundRunnerCore {
    engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
    memory_controller: Arc<MemoryController>,
}

impl BackgroundRunnerCore {
    /// Returns the ranges that are eligible for garbage collection.
    ///
    /// Returns `None` if there are no ranges cached or the previous gc is not
    /// finished.
    fn ranges_for_gc(&self) -> Option<BTreeSet<CacheRange>> {
        let ranges: BTreeSet<CacheRange> = {
            let core = self.engine.read();
            if core.range_manager().has_ranges_in_gc() {
                return None;
            }
            core.range_manager().ranges().keys().cloned().collect()
        };
        let ranges_clone = ranges.clone();
        if ranges_clone.is_empty() {
            return None;
        }
        {
            let mut core = self.engine.write();
            core.mut_range_manager().set_ranges_in_gc(ranges_clone);
        }
        Some(ranges)
    }

    fn gc_range(&self, range: &CacheRange, safe_point: u64) -> FilterMetrics {
        let (skiplist_engine, safe_ts) = {
            let mut core = self.engine.write();
            let Some(range_meta) = core.mut_range_manager().mut_range_meta(range) else {
                return FilterMetrics::default();
            };
            let min_snapshot = range_meta
                .range_snapshot_list()
                .min_snapshot_ts()
                .unwrap_or(u64::MAX);
            let safe_point = u64::min(safe_point, min_snapshot);

            if safe_point <= range_meta.safe_point() {
                info!(
                    "safe point not large enough";
                    "prev" => range_meta.safe_point(),
                    "current" => safe_point,
                );
                return FilterMetrics::default();
            }

            // todo: change it to debug!
            info!(
                "safe point update";
                "prev" => range_meta.safe_point(),
                "current" => safe_point,
                "range" => ?range,
            );
            range_meta.set_safe_point(safe_point);
            (core.engine(), safe_point)
        };

        let write_cf_handle = skiplist_engine.cf_handle(CF_WRITE);
        let default_cf_handle = skiplist_engine.cf_handle(CF_DEFAULT);
        let mut filter = Filter::new(safe_ts, default_cf_handle, write_cf_handle.clone());

        let mut iter = write_cf_handle.iterator();
        let guard = &epoch::pin();
        iter.seek_to_first(guard);
        while iter.valid() {
            let k = iter.key();
            let v = iter.value();
            if let Err(e) = filter.filter(k.as_bytes(), v.as_bytes()) {
                warn!(
                    "Something Wrong in memory engine GC";
                    "error" => ?e,
                );
            }
            iter.next(guard);
        }

        info!(
            "range gc complete";
            "range" => ?range,
            "total_version" => filter.metrics.total,
            "filtered_version" => filter.metrics.filtered,
            "below_safe_point_unique_keys" => filter.metrics.unique_key,
            "below_safe_point_version" => filter.metrics.versions,
            "below_safe_point_delete_version" => filter.metrics.delete_versions,
        );

        std::mem::take(&mut filter.metrics)
    }

    fn on_gc_finished(&mut self, ranges: BTreeSet<CacheRange>) {
        let mut core = self.engine.write();
        core.mut_range_manager().on_gc_finished(ranges);
    }

    /// Returns the first range to load with RocksDB snapshot. The `bool`
    /// returned indicates whether the task has been canceled due to memory
    /// issue.
    ///
    /// Returns `None` if there are no ranges to load.
    fn get_range_to_load(&self) -> Option<(CacheRange, Arc<RocksSnapshot>, bool)> {
        let core = self.engine.read();
        core.range_manager()
            .pending_ranges_loading_data
            .front()
            .cloned()
    }

    fn on_snapshot_load_finished(&mut self, range: CacheRange) {
        fail::fail_point!("on_snapshot_load_finished");
        loop {
            // Consume the cached write batch after the snapshot is acquired.
            let mut core = self.engine.write();
            // We still need to check whether the snapshot is canceled during the load
            let canceled = core
                .range_manager()
                .pending_ranges_loading_data
                .front()
                .unwrap()
                .2;
            if canceled {
                let (r, ..) = core
                    .mut_range_manager()
                    .pending_ranges_loading_data
                    .pop_front()
                    .unwrap();
                assert_eq!(r, range);
                drop(core);
                // Clear the range directly here to quickly free the memory.
                self.delete_ranges(&[r]);
                return;
            }

            if core.has_cached_write_batch(&range) {
                let (cache_batch, skiplist_engine) = {
                    (
                        core.take_cache_write_batch(&range).unwrap(),
                        core.engine().clone(),
                    )
                };
                drop(core);
                let guard = &epoch::pin();
                for (seq, entry) in cache_batch {
                    entry
                        .write_to_memory(
                            seq,
                            &skiplist_engine,
                            self.memory_controller.clone(),
                            guard,
                        )
                        .unwrap();
                }
                fail::fail_point!("on_cached_write_batch_consumed");
            } else {
                RangeCacheMemoryEngineCore::pending_range_completes_loading(&mut core, &range);
                break;
            }
        }
    }

    fn on_snapshot_load_canceled(&mut self, range: CacheRange) {
        let mut core = self.engine.write();
        let (r, ..) = core
            .mut_range_manager()
            .pending_ranges_loading_data
            .pop_front()
            .unwrap();
        assert_eq!(r, range);
    }

    fn delete_ranges(&mut self, ranges: &[CacheRange]) {
        let skiplist_engine = self.engine.read().engine();
        for r in ranges {
            skiplist_engine.delete_range(r);
        }
        self.engine
            .write()
            .mut_range_manager()
            .on_delete_ranges(ranges);
    }
}

pub struct BackgroundRunner {
    core: BackgroundRunnerCore,

    // We have following three separate workers so that each type of task would not block each
    // others
    range_load_remote: Remote<yatp::task::future::TaskCell>,
    range_load_worker: Worker,

    delete_range_remote: Remote<yatp::task::future::TaskCell>,
    delete_range_worker: Worker,

    gc_range_remote: Remote<yatp::task::future::TaskCell>,
    gc_range_worker: Worker,
}

impl Drop for BackgroundRunner {
    fn drop(&mut self) {
        self.range_load_worker.stop();
        self.delete_range_worker.stop();
        self.gc_range_worker.stop();
    }
}

impl BackgroundRunner {
    pub fn new(
        engine: Arc<RwLock<RangeCacheMemoryEngineCore>>,
        memory_controller: Arc<MemoryController>,
    ) -> Self {
        let range_load_worker = Builder::new("background-range-load-worker")
            // Range load now is implemented sequentially, so we must use exactly one thread to handle it.
            // todo(SpadeA): if the load speed is a bottleneck, we may consider to use multiple threads to load ranges.
            .thread_count(1)
            .create();
        let range_load_remote = range_load_worker.remote();

        let delete_range_worker = Worker::new("background-delete-range_worker");
        let delete_range_remote = delete_range_worker.remote();

        let gc_range_worker = Builder::new("background-range-load-worker")
            // Gc must also use exactly one thread to handle it.
            .thread_count(1)
            .create();
        let gc_range_remote = delete_range_worker.remote();
        Self {
            core: BackgroundRunnerCore {
                engine,
                memory_controller,
            },
            range_load_worker,
            range_load_remote,
            delete_range_worker,
            delete_range_remote,
            gc_range_worker,
            gc_range_remote,
        }
    }
}

impl Runnable for BackgroundRunner {
    type Task = BackgroundTask;

    fn run(&mut self, task: Self::Task) {
        match task {
            BackgroundTask::Gc(t) => {
                let mut core = self.core.clone();
                if let Some(ranges) = core.ranges_for_gc() {
                    let f = async move {
                        let mut metrics = FilterMetrics::default();
                        for range in &ranges {
                            let m = core.gc_range(range, t.safe_point);
                            metrics.merge(&m);
                        }
                        core.on_gc_finished(ranges);
                        metrics.flush();
                    };
                    self.gc_range_remote.spawn(f);
                }
            }
            BackgroundTask::LoadRange => {
                let mut core = self.core.clone();
                let f = async move {
                    let skiplist_engine = {
                        let core = core.engine.read();
                        core.engine().clone()
                    };
                    while let Some((range, snap, mut canceled)) = core.get_range_to_load() {
                        let iter_opt = IterOptions::new(
                            Some(KeyBuilder::from_vec(range.start.clone(), 0, 0)),
                            Some(KeyBuilder::from_vec(range.end.clone(), 0, 0)),
                            false,
                        );
                        if core.memory_controller.reached_soft_limit() {
                            // We are running out of memory, so cancel the load.
                            canceled = true;
                        }

                        if canceled {
                            core.on_snapshot_load_canceled(range);
                            continue;
                        }
                        for &cf in DATA_CFS {
                            let guard = &epoch::pin();
                            let handle = skiplist_engine.cf_handle(cf);
                            match snap.iterator_opt(cf, iter_opt.clone()) {
                                Ok(mut iter) => {
                                    iter.seek_to_first().unwrap();
                                    while iter.valid().unwrap() {
                                        // use 0 sequence number here as the kv is clearly
                                        // visible
                                        let mut encoded_key =
                                            encode_key(iter.key(), 0, ValueType::Value);
                                        let mut val =
                                            InternalBytes::from_vec(iter.value().to_vec());
                                        encoded_key
                                            .set_memory_controller(core.memory_controller.clone());
                                        val.set_memory_controller(core.memory_controller.clone());
                                        handle.insert(encoded_key, val, guard);
                                        iter.next().unwrap();
                                    }
                                }
                                Err(e) => {
                                    error!("creating rocksdb iterator failed"; "cf" => cf, "err" => %e);
                                }
                            }
                        }
                        core.on_snapshot_load_finished(range);
                    }
                };
                self.range_load_remote.spawn(f);
            }
            BackgroundTask::MemoryCheckAndEvict => {
                let mem_usage = self.core.memory_controller.mem_usage();
                if mem_usage > self.core.memory_controller.soft_limit_threshold() {
                    // todo: select ranges to evict
                }
                self.core.memory_controller.set_memory_checking(false);
            }
            BackgroundTask::DeleteRange(ranges) => {
                let mut core = self.core.clone();
                let f = async move { core.delete_ranges(&ranges) };
                self.delete_range_remote.spawn(f);
            }
        }
    }
}

#[derive(Default)]
struct FilterMetrics {
    total: usize,
    versions: usize,
    delete_versions: usize,
    filtered: usize,
    unique_key: usize,
    mvcc_rollback_and_locks: usize,
}

impl FilterMetrics {
    fn merge(&mut self, other: &FilterMetrics) {
        self.total += other.total;
        self.versions += other.versions;
        self.delete_versions += other.delete_versions;
        self.filtered += other.filtered;
        self.unique_key += other.unique_key;
        self.mvcc_rollback_and_locks += other.mvcc_rollback_and_locks;
    }

    fn flush(&self) {
        GC_FILTERED_STATIC.total.inc_by(self.total as u64);
        GC_FILTERED_STATIC
            .below_safe_point_total
            .inc_by(self.versions as u64);
        GC_FILTERED_STATIC.filtered.inc_by(self.filtered as u64);
        GC_FILTERED_STATIC
            .below_safe_point_unique
            .inc_by(self.unique_key as u64);
    }
}

struct Filter {
    safe_point: u64,
    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    default_cf_handle: SkiplistHandle,
    write_cf_handle: SkiplistHandle,

    // When deleting some keys, the latest one should be deleted at last to avoid the older
    // version appears.
    cached_delete_key: Option<Vec<u8>>,

    metrics: FilterMetrics,
}

impl Drop for Filter {
    fn drop(&mut self) {
        if let Some(cached_delete_key) = self.cached_delete_key.take() {
            let guard = &epoch::pin();
            self.write_cf_handle
                .remove(&InternalBytes::from_vec(cached_delete_key), guard);
        }
    }
}

impl Filter {
    fn new(
        safe_point: u64,
        default_cf_handle: SkiplistHandle,
        write_cf_handle: SkiplistHandle,
    ) -> Self {
        Self {
            safe_point,
            default_cf_handle,
            write_cf_handle,
            mvcc_key_prefix: vec![],
            cached_delete_key: None,
            remove_older: false,
            metrics: FilterMetrics::default(),
        }
    }

    fn filter(&mut self, key: &Bytes, value: &Bytes) -> Result<(), String> {
        self.metrics.total += 1;
        let InternalKey { user_key, .. } = decode_key(key);

        let (mvcc_key_prefix, commit_ts) = split_ts(user_key)?;
        if commit_ts > self.safe_point {
            return Ok(());
        }

        let guard = &epoch::pin();
        self.metrics.versions += 1;
        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.metrics.unique_key += 1;
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            self.remove_older = false;
            if let Some(cached_delete_key) = self.cached_delete_key.take() {
                self.write_cf_handle
                    .remove(&InternalBytes::from_vec(cached_delete_key), guard);
            }
        }

        let mut filtered = self.remove_older;
        let write = parse_write(value)?;
        if !self.remove_older {
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => {
                    self.metrics.mvcc_rollback_and_locks += 1;
                    filtered = true;
                }
                WriteType::Put => self.remove_older = true,
                WriteType::Delete => {
                    self.metrics.delete_versions += 1;
                    self.remove_older = true;

                    // The first mvcc type below safe point is the mvcc delete. We should delay to
                    // remove it until all the followings with the same user key have been deleted
                    // to avoid older version apper.
                    self.cached_delete_key = Some(key.to_vec());
                }
            }
        }

        if !filtered {
            return Ok(());
        }
        self.metrics.filtered += 1;
        self.write_cf_handle
            .remove(&InternalBytes::from_bytes(key.clone()), guard);
        self.handle_filtered_write(write, guard)?;

        Ok(())
    }

    fn handle_filtered_write(
        &mut self,
        write: WriteRef<'_>,
        guard: &epoch::Guard,
    ) -> std::result::Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            // todo(SpadeA): We don't know the sequence number of the key in the skiplist so
            // we cannot delete it directly. So we encoding a key with MAX sequence number
            // so we can find the mvcc key with sequence number in the skiplist by using
            // get_with_key and delete it with the result key. It involes more than one
            // seek(both get and remove invovle seek). Maybe we can provide the API to
            // delete the mvcc keys with all sequence numbers.
            let default_key = encoding_for_filter(&self.mvcc_key_prefix, write.start_ts);
            let mut iter = self.default_cf_handle.iterator();
            iter.seek(&default_key, guard);
            while iter.valid() && iter.key().same_user_key_with(&default_key) {
                self.default_cf_handle.remove(iter.key(), guard);
                iter.next(guard);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use std::{sync::Arc, time::Duration};

    use crossbeam::epoch;
    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRange, RangeCacheEngine, SyncMutable, CF_DEFAULT, CF_WRITE, DATA_CFS,
    };
    use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
    use tempfile::Builder;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::Filter;
    use crate::{
        background::BackgroundRunner,
        engine::{SkiplistEngine, SkiplistHandle},
        keys::{
            construct_key, construct_value, encode_key, encode_seek_key, encoding_for_filter,
            InternalBytes, ValueType,
        },
        memory_controller::MemoryController,
        EngineConfig, RangeCacheMemoryEngine,
    };

    fn put_data(
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        commit_ts: u64,
        seq_num: u64,
        short_value: bool,
        default_cf: &SkiplistHandle,
        write_cf: &SkiplistHandle,
        mem_controller: Arc<MemoryController>,
    ) {
        let write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(commit_ts))
            .into_encoded();
        let mut write_k = encode_key(&write_k, seq_num, ValueType::Value);
        write_k.set_memory_controller(mem_controller.clone());
        let write_v = Write::new(
            WriteType::Put,
            TimeStamp::new(start_ts),
            if short_value {
                Some(value.to_vec())
            } else {
                None
            },
        );
        let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
        val.set_memory_controller(mem_controller.clone());
        let guard = &epoch::pin();
        write_cf.insert(write_k, val, guard);

        if !short_value {
            let default_k = Key::from_raw(key)
                .append_ts(TimeStamp::new(start_ts))
                .into_encoded();
            let mut default_k = encode_key(&default_k, seq_num + 1, ValueType::Value);
            default_k.set_memory_controller(mem_controller.clone());
            let mut val = InternalBytes::from_vec(value.to_vec());
            val.set_memory_controller(mem_controller);
            default_cf.insert(default_k, val, guard);
        }
    }

    fn delete_data(
        key: &[u8],
        ts: u64,
        seq_num: u64,
        write_cf: &SkiplistHandle,
        mem_controller: Arc<MemoryController>,
    ) {
        let write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .into_encoded();
        let mut write_k = encode_key(&write_k, seq_num, ValueType::Value);
        write_k.set_memory_controller(mem_controller.clone());
        let write_v = Write::new(WriteType::Delete, TimeStamp::new(ts), None);
        let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
        val.set_memory_controller(mem_controller);
        let guard = &epoch::pin();
        write_cf.insert(write_k, val, guard);
    }

    fn rollback_data(
        key: &[u8],
        ts: u64,
        seq_num: u64,
        write_cf: &SkiplistHandle,
        mem_controller: Arc<MemoryController>,
    ) {
        let write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .into_encoded();
        let mut write_k = encode_key(&write_k, seq_num, ValueType::Value);
        write_k.set_memory_controller(mem_controller.clone());
        let write_v = Write::new(WriteType::Rollback, TimeStamp::new(ts), None);
        let mut val = InternalBytes::from_vec(write_v.as_ref().to_bytes());
        val.set_memory_controller(mem_controller);
        let guard = &epoch::pin();
        write_cf.insert(write_k, val, guard);
    }

    fn element_count(sklist: &SkiplistHandle) -> u64 {
        let guard = &epoch::pin();
        let mut count = 0;
        let mut iter = sklist.iterator();
        iter.seek_to_first(guard);
        while iter.valid() {
            count += 1;
            iter.next(guard);
        }
        count
    }

    // We should not use skiplist.get directly as we only cares keys without
    // sequence number suffix
    fn key_exist(sl: &SkiplistHandle, key: &InternalBytes, guard: &epoch::Guard) -> bool {
        let mut iter = sl.iterator();
        iter.seek(key, guard);
        if iter.valid() && iter.key().same_user_key_with(key) {
            return true;
        }
        false
    }

    // We should not use skiplist.get directly as we only cares keys without
    // sequence number suffix
    fn get_value(
        sl: &SkiplistHandle,
        key: &InternalBytes,
        guard: &epoch::Guard,
    ) -> Option<Vec<u8>> {
        let mut iter = sl.iterator();
        iter.seek(key, guard);
        if iter.valid() && iter.key().same_user_key_with(key) {
            return Some(iter.value().as_slice().to_vec());
        }
        None
    }

    fn dummy_controller(skip_engine: SkiplistEngine) -> Arc<MemoryController> {
        Arc::new(MemoryController::new(usize::MAX, usize::MAX, skip_engine))
    }

    #[test]
    fn test_filter() {
        let skiplist_engine = SkiplistEngine::new();
        let write = skiplist_engine.cf_handle(CF_WRITE);
        let default = skiplist_engine.cf_handle(CF_DEFAULT);

        let memory_controller = dummy_controller(skiplist_engine.clone());

        put_data(
            b"key1",
            b"value1",
            10,
            15,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value21",
            10,
            15,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value22",
            20,
            25,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        // mock repeate apply
        put_data(
            b"key2",
            b"value22",
            20,
            25,
            15,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value23",
            30,
            35,
            16,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value31",
            20,
            25,
            18,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value32",
            30,
            35,
            20,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        delete_data(b"key3", 40, 22, &write, memory_controller.clone());
        assert_eq!(7, element_count(&default));
        assert_eq!(8, element_count(&write));

        let mut filter = Filter::new(50, default.clone(), write.clone());
        let mut count = 0;
        let mut iter = write.iterator();
        let guard = &epoch::pin();
        iter.seek_to_first(guard);
        while iter.valid() {
            let k = iter.key();
            let v = iter.value();
            filter.filter(k.as_bytes(), v.as_bytes()).unwrap();
            count += 1;
            iter.next(guard);
        }
        assert_eq!(count, 8);
        drop(filter);

        assert_eq!(2, element_count(&write));
        assert_eq!(2, element_count(&default));

        let encode_key = |key, ts| {
            let key = Key::from_raw(key);
            encoding_for_filter(key.as_encoded(), ts)
        };

        let key = encode_key(b"key1", TimeStamp::new(15));
        assert!(key_exist(&write, &key, guard));

        let key = encode_key(b"key2", TimeStamp::new(35));
        assert!(key_exist(&write, &key, guard));

        let key = encode_key(b"key3", TimeStamp::new(35));
        assert!(!key_exist(&write, &key, guard));

        let key = encode_key(b"key1", TimeStamp::new(10));
        assert!(key_exist(&default, &key, guard));

        let key = encode_key(b"key2", TimeStamp::new(30));
        assert!(key_exist(&default, &key, guard));

        let key = encode_key(b"key3", TimeStamp::new(30));
        assert!(!key_exist(&default, &key, guard));
    }

    #[test]
    fn test_gc() {
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let memory_controller = engine.memory_controller();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let skiplist_engine = engine.core().write().engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        let encode_key = |key, ts| {
            let key = Key::from_raw(key);
            encoding_for_filter(key.as_encoded(), ts)
        };

        put_data(
            b"key1",
            b"value1",
            10,
            11,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key1",
            b"value2",
            12,
            13,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key1",
            b"value3",
            14,
            15,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));

        let worker = BackgroundRunner::new(engine.core.clone(), memory_controller.clone());

        // gc will not remove the latest mvcc put below safe point
        worker.core.gc_range(&range, 14);
        assert_eq!(2, element_count(&default));
        assert_eq!(2, element_count(&write));

        worker.core.gc_range(&range, 16);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));

        // rollback will not make the first older version be filtered
        rollback_data(b"key1", 17, 16, &write, memory_controller.clone());
        worker.core.gc_range(&range, 17);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));
        let key = encode_key(b"key1", TimeStamp::new(15));
        let guard = &epoch::pin();
        assert!(key_exist(&write, &key, guard));
        let key = encode_key(b"key1", TimeStamp::new(14));
        assert!(key_exist(&default, &key, guard));

        // unlike in WriteCompactionFilter, the latest mvcc delete below safe point will
        // be filtered
        delete_data(b"key1", 19, 18, &write, memory_controller.clone());
        worker.core.gc_range(&range, 19);
        assert_eq!(0, element_count(&write));
        assert_eq!(0, element_count(&default));
    }

    #[test]
    fn test_snapshot_block_gc() {
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let memory_controller = engine.memory_controller();
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let skiplist_engine = engine.core().write().engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        put_data(
            b"key1",
            b"value1",
            10,
            11,
            10,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value21",
            10,
            11,
            12,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value22",
            15,
            16,
            14,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key2",
            b"value23",
            20,
            21,
            16,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value31",
            5,
            6,
            18,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        put_data(
            b"key3",
            b"value32",
            10,
            11,
            20,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        let worker = BackgroundRunner::new(engine.core.clone(), memory_controller);
        let s1 = engine.snapshot(range.clone(), 10, u64::MAX);
        let s2 = engine.snapshot(range.clone(), 11, u64::MAX);
        let s3 = engine.snapshot(range.clone(), 20, u64::MAX);

        // nothing will be removed due to snapshot 5
        worker.core.gc_range(&range, 30);
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        drop(s1);
        worker.core.gc_range(&range, 30);
        assert_eq!(5, element_count(&default));
        assert_eq!(5, element_count(&write));

        drop(s2);
        worker.core.gc_range(&range, 30);
        assert_eq!(4, element_count(&default));
        assert_eq!(4, element_count(&write));

        drop(s3);
        worker.core.gc_range(&range, 30);
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));
    }

    #[test]
    fn test_gc_worker() {
        let mut config = EngineConfig::config_for_test();
        config.gc_interval = Duration::from_secs(1);
        let engine = RangeCacheMemoryEngine::new(config);
        let memory_controller = engine.memory_controller();
        let (write, default) = {
            let mut core = engine.core.write();
            core.mut_range_manager()
                .new_range(CacheRange::new(b"".to_vec(), b"z".to_vec()));
            let engine = core.engine();
            (engine.cf_handle(CF_WRITE), engine.cf_handle(CF_DEFAULT))
        };

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(10).as_millis() as u64;
        let commit_ts1 = TimeStamp::physical_now() - Duration::from_secs(9).as_millis() as u64;
        put_data(
            b"k",
            b"v1",
            start_ts,
            commit_ts1,
            100,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(8).as_millis() as u64;
        let commit_ts2 = TimeStamp::physical_now() - Duration::from_secs(7).as_millis() as u64;
        put_data(
            b"k",
            b"v2",
            start_ts,
            commit_ts2,
            110,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(6).as_millis() as u64;
        let commit_ts3 = TimeStamp::physical_now() - Duration::from_secs(5).as_millis() as u64;
        put_data(
            b"k",
            b"v3",
            start_ts,
            commit_ts3,
            110,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(4).as_millis() as u64;
        let commit_ts4 = TimeStamp::physical_now() - Duration::from_secs(3).as_millis() as u64;
        put_data(
            b"k",
            b"v4",
            start_ts,
            commit_ts4,
            110,
            false,
            &default,
            &write,
            memory_controller.clone(),
        );

        let guard = &epoch::pin();
        for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
            let key = Key::from_raw(b"k");
            let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));

            assert!(key_exist(&write, &key, guard));
        }

        std::thread::sleep(Duration::from_secs_f32(1.5));

        let key = Key::from_raw(b"k");
        // now, the outdated mvcc versions should be gone
        for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
            let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));
            assert!(!key_exist(&write, &key, guard));
        }

        let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(commit_ts4));
        assert!(key_exist(&write, &key, guard));
    }

    #[test]
    fn test_background_worker_load() {
        let mut engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let path = Builder::new().prefix("test_load").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();
        engine.set_disk_engine(rocks_engine.clone());

        for i in 10..20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            rocks_engine
                .put_cf(CF_DEFAULT, &key, value.as_bytes())
                .unwrap();
            rocks_engine
                .put_cf(CF_WRITE, &key, value.as_bytes())
                .unwrap();
        }

        let k = format!("zk{:08}", 15).into_bytes();
        let r1 = CacheRange::new(DATA_MIN_KEY.to_vec(), k.clone());
        let r2 = CacheRange::new(k, DATA_MAX_KEY.to_vec());
        {
            let mut core = engine.core.write();
            core.mut_range_manager().pending_ranges.push(r1.clone());
            core.mut_range_manager().pending_ranges.push(r2.clone());
        }
        engine.prepare_for_apply(&r1);
        engine.prepare_for_apply(&r2);

        // concurrent write to rocksdb, but the key will not be loaded in the memory
        // engine
        let key = construct_key(20, 1);
        let key20 = data_key(&key);
        let value = construct_value(20, 20);
        rocks_engine
            .put_cf(CF_DEFAULT, &key20, value.as_bytes())
            .unwrap();
        rocks_engine
            .put_cf(CF_WRITE, &key20, value.as_bytes())
            .unwrap();

        let (write, default) = {
            let core = engine.core().write();
            let skiplist_engine = core.engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        // wait for background load
        std::thread::sleep(Duration::from_secs(1));

        let _ = engine.snapshot(r1, u64::MAX, u64::MAX).unwrap();
        let _ = engine.snapshot(r2, u64::MAX, u64::MAX).unwrap();

        let guard = &epoch::pin();
        for i in 10..20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            let key = encode_seek_key(&key, u64::MAX);
            assert_eq!(
                get_value(&write, &key, guard).unwrap().as_slice(),
                value.as_bytes()
            );
            assert_eq!(
                get_value(&default, &key, guard).unwrap().as_slice(),
                value.as_bytes()
            );
        }

        let key20 = encode_seek_key(&key20, u64::MAX);
        assert!(!key_exist(&write, &key20, guard));
        assert!(!key_exist(&default, &key20, guard));
    }

    #[test]
    fn test_ranges_for_gc() {
        let engine = RangeCacheMemoryEngine::new(EngineConfig::config_for_test());
        let memory_controller = engine.memory_controller();
        let r1 = CacheRange::new(b"a".to_vec(), b"b".to_vec());
        let r2 = CacheRange::new(b"b".to_vec(), b"c".to_vec());
        engine.new_range(r1);
        engine.new_range(r2);

        let mut runner = BackgroundRunner::new(engine.core.clone(), memory_controller);
        let ranges = runner.core.ranges_for_gc().unwrap();
        assert_eq!(2, ranges.len());

        // until the previous gc finished, node ranges will be returned
        assert!(runner.core.ranges_for_gc().is_none());
        runner.core.on_gc_finished(ranges);

        let ranges = runner.core.ranges_for_gc().unwrap();
        assert_eq!(2, ranges.len());
    }
}
