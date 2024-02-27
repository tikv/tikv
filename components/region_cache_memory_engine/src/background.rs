// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{collections::BTreeSet, fmt::Display, sync::Arc, thread::JoinHandle, time::Duration};

use crossbeam::{
    channel::{bounded, tick, Sender},
    select,
    sync::ShardedLock,
};
use engine_rocks::RocksSnapshot;
use engine_traits::{CacheRange, IterOptions, Iterable, Iterator, CF_DEFAULT, CF_WRITE, DATA_CFS};
use skiplist_rs::Skiplist;
use slog_global::{error, info, warn};
use tikv_util::{
    keybuilder::KeyBuilder,
    worker::{Runnable, ScheduleError, Scheduler, Worker},
};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};
use yatp::Remote;

use crate::{
    engine::RangeCacheMemoryEngineCore,
    keys::{
        decode_key, encode_key, encoding_for_filter, InternalKey, InternalKeyComparator, ValueType,
    },
    memory_limiter::GlobalMemoryLimiter,
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
    pub fn new(core: Arc<ShardedLock<RangeCacheMemoryEngineCore>>, gc_interval: Duration) -> Self {
        let worker = Worker::new("range-cache-background-worker");
        let runner = BackgroundRunner::new(core.clone());
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
            loop {
                select! {
                    recv(tick(gc_interval)) -> _ => {
                        if scheduler.is_busy() {
                            info!(
                                "range cache engine gc worker is busy, jump to next gc duration";
                            );
                            continue;
                        }

                        let safe_point = TimeStamp::physical_now() - gc_interval.as_millis() as u64;
                        let safe_point = TimeStamp::compose(safe_point, 0).into_inner();
                        if let Err(e) = scheduler.schedule(BackgroundTask::GcTask(GcTask {safe_point})) {
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

#[derive(Debug)]
pub enum BackgroundTask {
    GcTask(GcTask),
    LoadTask,
}

#[derive(Debug)]
pub struct GcTask {
    pub safe_point: u64,
}

impl Display for BackgroundTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackgroundTask::GcTask(ref t) => t.fmt(f),
            BackgroundTask::LoadTask => f.debug_struct("LoadTask").finish(),
        }
    }
}

impl Display for GcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcTask")
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

#[derive(Clone)]
struct BackgroundRunnerCore {
    engine: Arc<ShardedLock<RangeCacheMemoryEngineCore>>,
}

impl BackgroundRunnerCore {
    fn ranges_for_gc(&self) -> BTreeSet<CacheRange> {
        let ranges: BTreeSet<CacheRange> = {
            let core = self.engine.read().unwrap();
            core.range_manager().ranges().keys().cloned().collect()
        };
        let ranges_clone = ranges.clone();
        {
            let mut core = self.engine.write().unwrap();
            core.mut_range_manager().set_ranges_in_gc(ranges_clone);
        }
        ranges
    }

    fn gc_range(&self, range: &CacheRange, safe_point: u64) {
        let (skiplist_engine, safe_ts) = {
            let mut core = self.engine.write().unwrap();
            let Some(range_meta) = core.mut_range_manager().mut_range_meta(range) else {
                return;
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
                return;
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

        let mut iter = write_cf_handle.iter();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            let k = iter.key();
            let v = iter.value();
            if let Err(e) = filter.filter(k, v) {
                warn!(
                    "Something Wrong in memory engine GC";
                    "error" => ?e,
                );
            }
            iter.next();
            count += 1;
        }

        info!(
            "range gc complete";
            "range" => ?range,
            "total_version" => count,
            "unique_keys" => filter.unique_key,
            "outdated_version" => filter.versions,
            "outdated_delete_version" => filter.delete_versions,
            "filtered_version" => filter.filtered,
        );
    }

    fn gc_finished(&mut self) {
        let mut core = self.engine.write().unwrap();
        core.mut_range_manager().clear_ranges_in_gc();
    }

    // return the first range to load with RocksDB snapshot
    fn get_range_to_load(&self) -> Option<(CacheRange, Arc<RocksSnapshot>)> {
        let core = self.engine.read().unwrap();
        core.range_manager()
            .ranges_loading_snapshot
            .front()
            .cloned()
    }

    fn on_snapshot_loaded(&mut self, range: CacheRange) -> engine_traits::Result<()> {
        fail::fail_point!("on_snapshot_loaded");
        let has_cache_batch = {
            let core = self.engine.read().unwrap();
            core.has_cached_write_batch(&range)
        };
        if has_cache_batch {
            let (cache_batch, skiplist_engine) = {
                let mut core = self.engine.write().unwrap();
                (
                    core.take_cache_write_batch(&range).unwrap(),
                    core.engine().clone(),
                )
            };
            for (seq, entry) in cache_batch {
                entry.write_to_memory(&skiplist_engine, seq)?;
            }
        }
        fail::fail_point!("on_snapshot_loaded_finish_before_status_change");
        {
            let mut core = self.engine.write().unwrap();
            let range_manager = core.mut_range_manager();
            assert_eq!(
                range_manager.ranges_loading_snapshot.pop_front().unwrap().0,
                range
            );
            range_manager.ranges_loading_cached_write.push(range);
        }
        Ok(())
    }
}

pub struct BackgroundRunner {
    core: BackgroundRunnerCore,
    range_load_remote: Remote<yatp::task::future::TaskCell>,
    range_load_worker: Worker,
}

impl Drop for BackgroundRunner {
    fn drop(&mut self) {
        self.range_load_worker.stop();
    }
}

impl BackgroundRunner {
    pub fn new(engine: Arc<ShardedLock<RangeCacheMemoryEngineCore>>) -> Self {
        let range_load_worker = Worker::new("background-range-load-worker");
        let range_load_remote = range_load_worker.remote();
        Self {
            core: BackgroundRunnerCore { engine },
            range_load_worker,
            range_load_remote,
        }
    }
}

impl Runnable for BackgroundRunner {
    type Task = BackgroundTask;

    fn run(&mut self, task: Self::Task) {
        match task {
            BackgroundTask::GcTask(t) => {
                let ranges = self.core.ranges_for_gc();
                for range in ranges {
                    self.core.gc_range(&range, t.safe_point);
                }
                self.core.gc_finished();
            }
            BackgroundTask::LoadTask => {
                let mut core = self.core.clone();
                let f = async move {
                    let skiplist_engine = {
                        let core = core.engine.read().unwrap();
                        core.engine().clone()
                    };
                    while let Some((range, snap)) = core.get_range_to_load() {
                        let iter_opt = IterOptions::new(
                            Some(KeyBuilder::from_vec(range.start.clone(), 0, 0)),
                            Some(KeyBuilder::from_vec(range.end.clone(), 0, 0)),
                            false,
                        );
                        for &cf in DATA_CFS {
                            let handle = skiplist_engine.cf_handle(cf);
                            match snap.iterator_opt(cf, iter_opt.clone()) {
                                Ok(mut iter) => {
                                    iter.seek_to_first().unwrap();
                                    while iter.valid().unwrap() {
                                        // use 0 sequence number here as the kv is clearly visible
                                        let encoded_key =
                                            encode_key(iter.key(), 0, ValueType::Value);
                                        handle.put(encoded_key, iter.value().to_vec());
                                        iter.next().unwrap();
                                    }
                                }
                                Err(e) => {
                                    error!("creating rocksdb iterator failed"; "cf" => cf, "err" => %e);
                                }
                            }
                        }
                        core.on_snapshot_loaded(range).unwrap();
                    }
                };
                self.range_load_remote.spawn(f);
            }
        }
    }
}

struct Filter {
    safe_point: u64,
    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    default_cf_handle: Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
    write_cf_handle: Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,

    // the total size of the keys buffered, when it exceeds the limit, all keys in the buffer will
    // be removed
    filtered_write_key_size: usize,
    filtered_write_key_buffer: Vec<Vec<u8>>,
    cached_delete_key: Option<Vec<u8>>,

    versions: usize,
    delete_versions: usize,
    filtered: usize,
    unique_key: usize,
    mvcc_rollback_and_locks: usize,
}

impl Drop for Filter {
    fn drop(&mut self) {
        if let Some(cached_delete_key) = self.cached_delete_key.take() {
            self.write_cf_handle.remove(cached_delete_key.as_slice());
        }
    }
}

impl Filter {
    fn new(
        safe_point: u64,
        default_cf_handle: Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
        write_cf_handle: Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
    ) -> Self {
        Self {
            safe_point,
            default_cf_handle,
            write_cf_handle,
            unique_key: 0,
            filtered_write_key_size: 0,
            filtered_write_key_buffer: Vec::with_capacity(100),
            mvcc_key_prefix: vec![],
            delete_versions: 0,
            versions: 0,
            filtered: 0,
            cached_delete_key: None,
            mvcc_rollback_and_locks: 0,
            remove_older: false,
        }
    }

    fn filter(&mut self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let InternalKey { user_key, .. } = decode_key(key);

        let (mvcc_key_prefix, commit_ts) = split_ts(user_key)?;
        if commit_ts > self.safe_point {
            return Ok(());
        }

        self.versions += 1;
        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.unique_key += 1;
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            self.remove_older = false;
            if let Some(cached_delete_key) = self.cached_delete_key.take() {
                self.write_cf_handle.remove(&cached_delete_key);
            }
        }

        let mut filtered = self.remove_older;
        let write = parse_write(value)?;
        if !self.remove_older {
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => {
                    self.mvcc_rollback_and_locks += 1;
                    filtered = true;
                }
                WriteType::Put => self.remove_older = true,
                WriteType::Delete => {
                    self.delete_versions += 1;
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
        self.filtered += 1;
        self.write_cf_handle.remove(key);
        self.handle_filtered_write(write)?;

        Ok(())
    }

    fn handle_filtered_write(&mut self, write: WriteRef<'_>) -> std::result::Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            // todo(SpadeA): We don't know the sequence number of the key in the skiplist so
            // we cannot delete it directly. So we encoding a key with MAX sequence number
            // so we can find the mvcc key with sequence number in the skiplist by using
            // get_with_key and delete it with the result key. It involes more than one
            // seek(both get and remove invovle seek). Maybe we can provide the API to
            // delete the mvcc keys with all sequence numbers.
            let default_key = encoding_for_filter(&self.mvcc_key_prefix, write.start_ts);
            while let Some(entry) = self.default_cf_handle.get(&default_key) {
                self.default_cf_handle.remove(entry.key().as_slice());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use core::slice::SlicePattern;
    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;
    use engine_rocks::util::new_engine;
    use engine_traits::{
        CacheRange, RangeCacheEngine, SyncMutable, CF_DEFAULT, CF_WRITE, DATA_CFS,
    };
    use keys::{data_key, DATA_MAX_KEY, DATA_MIN_KEY};
    use skiplist_rs::Skiplist;
    use tempfile::Builder;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::Filter;
    use crate::{
        background::BackgroundRunner,
        engine::SkiplistEngine,
        keys::{
            construct_key, construct_value, encode_key, encode_seek_key, encoding_for_filter,
            InternalKeyComparator, ValueType, VALUE_TYPE_FOR_SEEK,
        },
        memory_limiter::GlobalMemoryLimiter,
        RangeCacheMemoryEngine,
    };

    fn put_data(
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        commit_ts: u64,
        seq_num: u64,
        short_value: bool,
        default_cf: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
        write_cf: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
    ) {
        let write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(commit_ts))
            .into_encoded();
        let write_k = encode_key(&write_k, seq_num, ValueType::Value);
        let write_v = Write::new(
            WriteType::Put,
            TimeStamp::new(start_ts),
            if short_value {
                Some(value.to_vec())
            } else {
                None
            },
        );
        write_cf.put(write_k, Bytes::from(write_v.as_ref().to_bytes()));

        if !short_value {
            let default_k = Key::from_raw(key)
                .append_ts(TimeStamp::new(start_ts))
                .into_encoded();
            let default_k = encode_key(&default_k, seq_num + 1, ValueType::Value);
            default_cf.put(default_k, Bytes::from(value.to_vec()));
        }
    }

    fn delete_data(
        key: &[u8],
        ts: u64,
        seq_num: u64,
        write_cf: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
    ) {
        let write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .into_encoded();
        let write_k = encode_key(&write_k, seq_num, ValueType::Value);
        let write_v = Write::new(WriteType::Delete, TimeStamp::new(ts), None);
        write_cf.put(write_k, Bytes::from(write_v.as_ref().to_bytes()));
    }

    fn rollback_data(
        key: &[u8],
        ts: u64,
        seq_num: u64,
        write_cf: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>,
    ) {
        let write_k = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .into_encoded();
        let write_k = encode_key(&write_k, seq_num, ValueType::Value);
        let write_v = Write::new(WriteType::Rollback, TimeStamp::new(ts), None);
        write_cf.put(write_k, Bytes::from(write_v.as_ref().to_bytes()));
    }

    fn element_count(sklist: &Arc<Skiplist<InternalKeyComparator, GlobalMemoryLimiter>>) -> u64 {
        let mut count = 0;
        let mut iter = sklist.iter();
        iter.seek_to_first();
        while iter.valid() {
            count += 1;
            iter.next();
        }
        count
    }

    #[test]
    fn test_filter() {
        let skiplist_engine = SkiplistEngine::new(Arc::default());
        let write = skiplist_engine.cf_handle(CF_WRITE);
        let default = skiplist_engine.cf_handle(CF_DEFAULT);

        put_data(b"key1", b"value1", 10, 15, 10, false, &default, &write);
        put_data(b"key2", b"value21", 10, 15, 12, false, &default, &write);
        put_data(b"key2", b"value22", 20, 25, 14, false, &default, &write);
        // mock repeate apply
        put_data(b"key2", b"value22", 20, 25, 15, false, &default, &write);
        put_data(b"key2", b"value23", 30, 35, 16, false, &default, &write);
        put_data(b"key3", b"value31", 20, 25, 18, false, &default, &write);
        put_data(b"key3", b"value32", 30, 35, 20, false, &default, &write);
        delete_data(b"key3", 40, 22, &write);
        assert_eq!(7, element_count(&default));
        assert_eq!(8, element_count(&write));

        let mut filter = Filter::new(50, default.clone(), write.clone());
        let mut count = 0;
        let mut iter = write.iter();
        iter.seek_to_first();
        while iter.valid() {
            let k = iter.key();
            let v = iter.value();
            filter.filter(k.as_slice(), v.as_slice()).unwrap();
            count += 1;
            iter.next();
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
        assert!(write.get(&key).is_some());

        let key = encode_key(b"key2", TimeStamp::new(35));
        assert!(write.get(&key).is_some());

        let key = encode_key(b"key3", TimeStamp::new(35));
        assert!(write.get(&key).is_none());

        let key = encode_key(b"key1", TimeStamp::new(10));
        assert!(default.get(&key).is_some());

        let key = encode_key(b"key2", TimeStamp::new(30));
        assert!(default.get(&key).is_some());

        let key = encode_key(b"key3", TimeStamp::new(30));
        assert!(default.get(&key).is_none());
    }

    #[test]
    fn test_gc() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1));
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let mut core = engine.core().write().unwrap();
            let skiplist_engine = core.engine();
            core.mut_range_manager().set_range_readable(&range, true);
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        let encode_key = |key, ts| {
            let key = Key::from_raw(key);
            encoding_for_filter(key.as_encoded(), ts)
        };

        put_data(b"key1", b"value1", 10, 11, 10, false, &default, &write);
        put_data(b"key1", b"value2", 12, 13, 12, false, &default, &write);
        put_data(b"key1", b"value3", 14, 15, 14, false, &default, &write);
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));

        let worker = BackgroundRunner::new(engine.core.clone());

        // gc will not remove the latest mvcc put below safe point
        worker.core.gc_range(&range, 14);
        assert_eq!(2, element_count(&default));
        assert_eq!(2, element_count(&write));

        worker.core.gc_range(&range, 16);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));

        // rollback will not make the first older version be filtered
        rollback_data(b"key1", 17, 16, &write);
        worker.core.gc_range(&range, 17);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));
        let key = encode_key(b"key1", TimeStamp::new(15));
        assert!(write.get(&key).is_some());
        let key = encode_key(b"key1", TimeStamp::new(14));
        assert!(default.get(&key).is_some());

        // unlike in WriteCompactionFilter, the latest mvcc delete below safe point will
        // be filtered
        delete_data(b"key1", 19, 18, &write);
        worker.core.gc_range(&range, 19);
        assert_eq!(0, element_count(&write));
        assert_eq!(0, element_count(&default));
    }

    #[test]
    fn test_snapshot_block_gc() {
        let engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1));
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let mut core = engine.core().write().unwrap();
            let skiplist_engine = core.engine();
            core.mut_range_manager().set_range_readable(&range, true);
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        put_data(b"key1", b"value1", 10, 11, 10, false, &default, &write);
        put_data(b"key2", b"value21", 10, 11, 12, false, &default, &write);
        put_data(b"key2", b"value22", 15, 16, 14, false, &default, &write);
        put_data(b"key2", b"value23", 20, 21, 16, false, &default, &write);
        put_data(b"key3", b"value31", 5, 6, 18, false, &default, &write);
        put_data(b"key3", b"value32", 10, 11, 20, false, &default, &write);
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        let worker = BackgroundRunner::new(engine.core.clone());
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
        let engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1));
        let (write, default) = {
            let mut core = engine.core.write().unwrap();
            core.mut_range_manager()
                .new_range(CacheRange::new(b"".to_vec(), b"z".to_vec()));
            let engine = core.engine();
            (engine.cf_handle(CF_WRITE), engine.cf_handle(CF_DEFAULT))
        };

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(10).as_millis() as u64;
        let commit_ts1 = TimeStamp::physical_now() - Duration::from_secs(9).as_millis() as u64;
        put_data(
            b"k", b"v1", start_ts, commit_ts1, 100, false, &default, &write,
        );

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(8).as_millis() as u64;
        let commit_ts2 = TimeStamp::physical_now() - Duration::from_secs(7).as_millis() as u64;
        put_data(
            b"k", b"v2", start_ts, commit_ts2, 110, false, &default, &write,
        );

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(6).as_millis() as u64;
        let commit_ts3 = TimeStamp::physical_now() - Duration::from_secs(5).as_millis() as u64;
        put_data(
            b"k", b"v3", start_ts, commit_ts3, 110, false, &default, &write,
        );

        let start_ts = TimeStamp::physical_now() - Duration::from_secs(4).as_millis() as u64;
        let commit_ts4 = TimeStamp::physical_now() - Duration::from_secs(3).as_millis() as u64;
        put_data(
            b"k", b"v4", start_ts, commit_ts4, 110, false, &default, &write,
        );

        for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
            let key = Key::from_raw(b"k");
            let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));

            assert!(write.get(&key).is_some());
        }

        std::thread::sleep(Duration::from_secs_f32(1.5));

        let key = Key::from_raw(b"k");
        // now, the outdated mvcc versions should be gone
        for &ts in &[commit_ts1, commit_ts2, commit_ts3] {
            let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(ts));
            assert!(write.get(&key).is_none());
        }

        let key = encoding_for_filter(key.as_encoded(), TimeStamp::new(commit_ts4));
        assert!(write.get(&key).is_some());
    }

    #[test]
    fn test_background_worker_load() {
        let mut engine = RangeCacheMemoryEngine::new(Arc::default(), Duration::from_secs(1000));
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
            let mut core = engine.core.write().unwrap();
            core.mut_range_manager().pending_ranges.push(r1.clone());
            core.mut_range_manager().pending_ranges.push(r2.clone());
        }
        engine.handle_pending_load();

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
            let core = engine.core().write().unwrap();
            let skiplist_engine = core.engine();
            (
                skiplist_engine.cf_handle(CF_WRITE),
                skiplist_engine.cf_handle(CF_DEFAULT),
            )
        };

        // wait for background load
        std::thread::sleep(Duration::from_secs(1));

        for i in 10..20 {
            let key = construct_key(i, 1);
            let key = data_key(&key);
            let value = construct_value(i, i);
            let key = encode_seek_key(&key, u64::MAX, VALUE_TYPE_FOR_SEEK);
            assert_eq!(
                write.get(&key).unwrap().value().as_slice(),
                value.as_bytes()
            );
            assert_eq!(
                default.get(&key).unwrap().value().as_slice(),
                value.as_bytes()
            );
        }

        let key20 = encode_seek_key(&key20, u64::MAX, VALUE_TYPE_FOR_SEEK);
        assert!(write.get(&key20).is_none());
        assert!(default.get(&key20).is_none());
    }
}
