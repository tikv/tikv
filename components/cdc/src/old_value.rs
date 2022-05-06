// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Deref;

use engine_traits::{ReadOptions, CF_DEFAULT, CF_WRITE};
use getset::CopyGetters;
use tikv::storage::{
    mvcc::near_load_data_by_write, Cursor, CursorBuilder, ScanMode, Snapshot as EngineSnapshot,
    Statistics,
};
use tikv_kv::Iterator;
use tikv_util::{
    config::ReadableSize,
    lru::{LruCache, SizePolicy},
    time::Instant,
    Either,
};
use txn_types::{Key, MutationType, OldValue, TimeStamp, Value, WriteRef, WriteType};

use crate::{metrics::*, Result};

pub(crate) type OldValueCallback = Box<
    dyn Fn(Key, TimeStamp, &mut OldValueCache, &mut Statistics) -> Result<Option<Vec<u8>>> + Send,
>;

#[derive(Default)]
pub struct OldValueCacheSizePolicy(usize);

impl SizePolicy<Key, (OldValue, Option<MutationType>)> for OldValueCacheSizePolicy {
    fn current(&self) -> usize {
        self.0
    }

    fn on_insert(&mut self, key: &Key, value: &(OldValue, Option<MutationType>)) {
        self.0 +=
            key.as_encoded().len() + value.0.size() + std::mem::size_of::<Option<MutationType>>();
    }

    fn on_remove(&mut self, key: &Key, value: &(OldValue, Option<MutationType>)) {
        self.0 -=
            key.as_encoded().len() + value.0.size() + std::mem::size_of::<Option<MutationType>>();
    }

    fn on_reset(&mut self, val: usize) {
        self.0 = val;
    }
}

#[derive(CopyGetters)]
pub struct OldValueCache {
    cache: LruCache<Key, (OldValue, Option<MutationType>), OldValueCacheSizePolicy>,
    #[getset(get_copy = "pub")]
    access_count: usize,
    #[getset(get_copy = "pub")]
    miss_count: usize,
    #[getset(get_copy = "pub")]
    miss_none_count: usize,
    #[getset(get_copy = "pub")]
    update_count: usize,
}

impl OldValueCache {
    pub fn new(capacity: ReadableSize) -> OldValueCache {
        CDC_OLD_VALUE_CACHE_MEMORY_QUOTA.set(capacity.0 as i64);
        OldValueCache {
            cache: LruCache::with_capacity_sample_and_trace(
                capacity.0 as usize,
                0,
                OldValueCacheSizePolicy(0),
            ),
            access_count: 0,
            miss_count: 0,
            miss_none_count: 0,
            update_count: 0,
        }
    }

    pub fn insert(&mut self, key: Key, old_value: (OldValue, Option<MutationType>)) {
        self.cache.insert(key, old_value);
        self.update_count += 1;
    }

    pub fn resize(&mut self, new_capacity: ReadableSize) {
        CDC_OLD_VALUE_CACHE_MEMORY_QUOTA.set(new_capacity.0 as i64);
        self.cache.resize(new_capacity.0 as usize);
    }

    pub fn flush_metrics(&mut self) {
        fail::fail_point!("cdc_flush_old_value_metrics", |_| {});
        CDC_OLD_VALUE_CACHE_BYTES.set(self.cache.size() as i64);
        CDC_OLD_VALUE_CACHE_LEN.set(self.cache.len() as i64);
        CDC_OLD_VALUE_CACHE_ACCESS.add(self.access_count as i64);
        CDC_OLD_VALUE_CACHE_MISS.add(self.miss_count as i64);
        CDC_OLD_VALUE_CACHE_MISS_NONE.add(self.miss_none_count as i64);
        self.access_count = 0;
        self.miss_count = 0;
        self.miss_none_count = 0;
        self.update_count = 0;
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.cache.capacity()
    }
}

/// Fetch old value for `key`. If it can't be found in `old_value_cache`, seek and retrieve it with
/// `query_ts` from `snapshot`.
pub fn get_old_value<S: EngineSnapshot>(
    snapshot: &S,
    key: Key,
    query_ts: TimeStamp,
    old_value_cache: &mut OldValueCache,
    statistics: &mut Statistics,
) -> Result<Option<Vec<u8>>> {
    let start = Instant::now();
    tikv_util::defer!(
        CDC_OLD_VALUE_DURATION_HISTOGRAM
            .with_label_values(&["all"])
            .observe(start.saturating_elapsed().as_secs_f64())
    );

    old_value_cache.access_count += 1;
    if let Some((old_value, mutation_type)) = old_value_cache.cache.remove(&key) {
        return match mutation_type {
            // Old value of an Insert is guaranteed to be None.
            Some(MutationType::Insert) => {
                assert_eq!(old_value, OldValue::None);
                Ok(None)
            }
            // For Put, Delete or a mutation type we do not know,
            // we read old value from the cache.
            Some(MutationType::Put) | Some(MutationType::Delete) | None => {
                match old_value {
                    OldValue::None => Ok(None),
                    OldValue::Value { value } => Ok(Some(value)),
                    OldValue::ValueTimeStamp { start_ts } => {
                        let prev_key = key.truncate_ts().unwrap().append_ts(start_ts);
                        let value = get_value_default(snapshot, &prev_key, statistics);
                        Ok(value)
                    }
                    // Unspecified and SeekWrite should not be added into cache.
                    OldValue::Unspecified | OldValue::SeekWrite(_) => unreachable!(),
                }
            }
            _ => unreachable!(),
        };
    }

    // Cannot get old value from cache, seek for it in engine.
    old_value_cache.miss_count += 1;
    let key = key.truncate_ts().unwrap().append_ts(query_ts);
    let mut cursor = new_write_cursor_on_key(snapshot, &key);
    let value = near_seek_old_value(&key, &mut cursor, Either::Left(snapshot), statistics)?;
    if value.is_none() {
        old_value_cache.miss_none_count += 1;
    }
    Ok(value)
}

pub fn new_old_value_cursor<S: EngineSnapshot>(snapshot: &S, cf: &'static str) -> Cursor<S::Iter> {
    let lower = snapshot.lower_bound().map(Key::from_encoded_slice);
    let upper = snapshot.upper_bound().map(Key::from_encoded_slice);
    CursorBuilder::new(snapshot, cf)
        .fill_cache(false)
        .scan_mode(ScanMode::Mixed)
        .range(lower, upper)
        .build()
        .unwrap()
}

/// Gets the latest value to the key with an older or equal version.
///
/// The key passed in should be a key with a timestamp. This function will returns
/// the latest value of the entry if the user key is the same to the given key and
/// the timestamp is older than or equal to the timestamp in the given key.
///
/// `load_from_cf_data` indicates how to get value from `CF_DEFAULT`.
pub fn near_seek_old_value<S: EngineSnapshot>(
    key: &Key,
    write_cursor: &mut Cursor<S::Iter>,
    load_from_cf_data: Either<&S, &mut Cursor<S::Iter>>,
    statistics: &mut Statistics,
) -> Result<Option<Value>> {
    let start = Instant::now();
    tikv_util::defer!(
        CDC_OLD_VALUE_DURATION_HISTOGRAM
            .with_label_values(&["seek"])
            .observe(start.saturating_elapsed().as_secs_f64())
    );

    let (user_key, seek_ts) = Key::split_on_ts_for(key.as_encoded()).unwrap();
    if write_cursor.near_seek(key, &mut statistics.write)?
        && Key::is_user_key_eq(write_cursor.key(&mut statistics.write), user_key)
    {
        let mut old_value = None;
        while Key::is_user_key_eq(write_cursor.key(&mut statistics.write), user_key) {
            let write = WriteRef::parse(write_cursor.value(&mut statistics.write)).unwrap();
            old_value = match write.write_type {
                WriteType::Put if write.check_gc_fence_as_latest_version(seek_ts) => {
                    match write.short_value {
                        Some(short_value) => Some(short_value.to_vec()),
                        None => {
                            let mut key = key.clone().truncate_ts().unwrap();
                            match load_from_cf_data {
                                Either::Left(snapshot) => {
                                    key = key.append_ts(write.start_ts);
                                    get_value_default(snapshot, &key, statistics)
                                }
                                Either::Right(cursor) => Some(near_load_data_by_write(
                                    cursor,
                                    &key,
                                    write.start_ts,
                                    statistics,
                                )?),
                            }
                        }
                    }
                }
                WriteType::Delete | WriteType::Put => None,
                WriteType::Rollback | WriteType::Lock => {
                    if !write_cursor.next(&mut statistics.write) {
                        None
                    } else {
                        continue;
                    }
                }
            };
            break;
        }
        Ok(old_value)
    } else {
        Ok(None)
    }
}

pub struct OldValueCursors<I: Iterator> {
    pub write: Cursor<I>,
    pub default: Cursor<I>,
}

impl<I: Iterator> OldValueCursors<I> {
    pub fn new(write: Cursor<I>, default: Cursor<I>) -> Self {
        OldValueCursors { write, default }
    }
}

// Create a write cursor for fetching an old value for `key`.
fn new_write_cursor_on_key<S: EngineSnapshot>(snapshot: &S, key: &Key) -> Cursor<S::Iter> {
    let ts = Key::decode_ts_from(key.as_encoded()).unwrap();
    let upper = {
        let user_key = key.clone().truncate_ts().unwrap();
        Some(user_key.append_ts(TimeStamp::zero()))
    };

    CursorBuilder::new(snapshot, CF_WRITE)
        .fill_cache(false)
        .scan_mode(ScanMode::Mixed)
        // Set the range explicitly to avoid region boundaries are used incorrectly.
        .range(Some(key.clone()), upper)
        // Use bloom filter to speed up seeking on a given prefix.
        .prefix_seek(true)
        .hint_max_ts(Some(ts))
        .build()
        .unwrap()
}

fn get_value_default<S: EngineSnapshot>(
    snapshot: &S,
    key: &Key,
    statistics: &mut Statistics,
) -> Option<Value> {
    let start = Instant::now();
    tikv_util::defer!(
        CDC_OLD_VALUE_DURATION_HISTOGRAM
            .with_label_values(&["get"])
            .observe(start.saturating_elapsed().as_secs_f64())
    );

    statistics.data.get += 1;
    let mut opts = ReadOptions::new();
    opts.set_fill_cache(false);
    snapshot
        .get_cf_opt(opts, CF_DEFAULT, key)
        .unwrap()
        .map(|v| v.deref().to_vec())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_rocks::{ReadPerfInstant, RocksEngine};
    use engine_traits::{KvEngine, MiscExt};
    use tikv::{
        config::DbConfig,
        storage::{kv::TestEngineBuilder, txn::tests::*},
    };

    use super::*;

    fn must_get_eq(
        kv_engine: &RocksEngine,
        key: &Key,
        ts: u64,
        value: Option<Value>,
    ) -> Statistics {
        let key = key.clone().append_ts(ts.into());
        let snapshot = Arc::new(kv_engine.snapshot());
        let mut cursor = new_write_cursor_on_key(&snapshot, &key);
        let load_default = Either::Left(&snapshot);
        let mut stats = Statistics::default();
        let v = near_seek_old_value(&key, &mut cursor, load_default, &mut stats).unwrap();
        assert_eq!(v, value);
        stats
    }

    #[test]
    fn test_old_value_resize() {
        let capacity = 1024;

        let mut old_value_cache = OldValueCache::new(ReadableSize(capacity));
        let value = (
            OldValue::Value {
                value: b"value".to_vec(),
            },
            None,
        );

        // The size of each insert.
        let mut size_calc = OldValueCacheSizePolicy::default();
        size_calc.on_insert(&Key::from_raw(&0_usize.to_be_bytes()), &value);
        let size = size_calc.current();

        // Insert ten values.
        let cases = 10_usize;
        for i in 0..cases {
            let key = Key::from_raw(&i.to_be_bytes());
            old_value_cache.cache.insert(key, value.clone());
        }

        assert_eq!(old_value_cache.cache.size(), size * cases as usize);
        assert_eq!(old_value_cache.cache.len(), cases as usize);
        assert_eq!(old_value_cache.capacity(), capacity as usize);

        // Reduces capacity.
        let new_capacity = 256;
        // The memory usage that needs to be removed because of the capacity reduction.
        let dropped = old_value_cache.cache.size() - new_capacity;
        let dropped_count = if dropped % size != 0 {
            (dropped / size) + 1
        } else {
            dropped / size
        };
        // The remaining values count.
        let remaining_count = old_value_cache.cache.len() - dropped_count;
        old_value_cache.resize(ReadableSize(new_capacity as u64));

        assert_eq!(old_value_cache.cache.size(), size * remaining_count);
        assert_eq!(old_value_cache.cache.len(), remaining_count);
        assert_eq!(old_value_cache.capacity(), new_capacity as usize);
        for i in dropped_count..cases {
            let key = Key::from_raw(&i.to_be_bytes());
            assert_eq!(old_value_cache.cache.get(&key).is_some(), true);
        }

        // Increases the capacity again.
        let new_capacity = 1024;
        old_value_cache.resize(ReadableSize(new_capacity));

        assert_eq!(old_value_cache.cache.size(), size * remaining_count);
        assert_eq!(old_value_cache.cache.len(), remaining_count);
        assert_eq!(old_value_cache.capacity(), new_capacity as usize);
        for i in dropped_count..cases {
            let key = Key::from_raw(&i.to_be_bytes());
            assert_eq!(old_value_cache.cache.get(&key).is_some(), true);
        }
    }

    #[test]
    fn test_old_value_reader() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();
        let k = b"k";
        let key = Key::from_raw(k);

        must_prewrite_put(&engine, k, b"v1", k, 1);
        must_get_eq(&kv_engine, &key, 2, None);
        must_get_eq(&kv_engine, &key, 1, None);
        must_commit(&engine, k, 1, 1);
        must_get_eq(&kv_engine, &key, 1, Some(b"v1".to_vec()));

        must_prewrite_put(&engine, k, b"v2", k, 2);
        must_get_eq(&kv_engine, &key, 2, Some(b"v1".to_vec()));
        must_rollback(&engine, k, 2, false);

        must_prewrite_put(&engine, k, b"v3", k, 3);
        must_get_eq(&kv_engine, &key, 3, Some(b"v1".to_vec()));
        must_commit(&engine, k, 3, 3);

        must_prewrite_delete(&engine, k, k, 4);
        must_get_eq(&kv_engine, &key, 4, Some(b"v3".to_vec()));
        must_commit(&engine, k, 4, 4);

        must_prewrite_put(&engine, k, vec![b'v'; 5120].as_slice(), k, 5);
        must_get_eq(&kv_engine, &key, 5, None);
        must_commit(&engine, k, 5, 5);

        must_prewrite_delete(&engine, k, k, 6);
        must_get_eq(&kv_engine, &key, 6, Some(vec![b'v'; 5120]));
        must_rollback(&engine, k, 6, false);

        must_prewrite_put(&engine, k, b"v4", k, 7);
        must_commit(&engine, k, 7, 9);

        must_acquire_pessimistic_lock(&engine, k, k, 8, 10);
        must_pessimistic_prewrite_put(&engine, k, b"v5", k, 8, 10, true);
        must_get_eq(&kv_engine, &key, 10, Some(b"v4".to_vec()));
        must_commit(&engine, k, 8, 11);
    }

    #[test]
    fn test_old_value_reader_check_gc_fence() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();

        // PUT,      Read
        //  `--------------^
        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 10);
        must_commit(&engine, b"k1", 10, 20);
        must_cleanup_with_gc_fence(&engine, b"k1", 20, 0, 50, true);

        // PUT,      Read
        //  `---------^
        must_prewrite_put(&engine, b"k2", b"v2", b"k2", 11);
        must_commit(&engine, b"k2", 11, 20);
        must_cleanup_with_gc_fence(&engine, b"k2", 20, 0, 40, true);

        // PUT,      Read
        //  `-----^
        must_prewrite_put(&engine, b"k3", b"v3", b"k3", 12);
        must_commit(&engine, b"k3", 12, 20);
        must_cleanup_with_gc_fence(&engine, b"k3", 20, 0, 30, true);

        // PUT,   PUT,       Read
        //  `-----^ `----^
        must_prewrite_put(&engine, b"k4", b"v4", b"k4", 13);
        must_commit(&engine, b"k4", 13, 14);
        must_prewrite_put(&engine, b"k4", b"v4x", b"k4", 15);
        must_commit(&engine, b"k4", 15, 20);
        must_cleanup_with_gc_fence(&engine, b"k4", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&engine, b"k4", 20, 0, 30, true);

        // PUT,   DEL,       Read
        //  `-----^ `----^
        must_prewrite_put(&engine, b"k5", b"v5", b"k5", 13);
        must_commit(&engine, b"k5", 13, 14);
        must_prewrite_delete(&engine, b"k5", b"v5", 15);
        must_commit(&engine, b"k5", 15, 20);
        must_cleanup_with_gc_fence(&engine, b"k5", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&engine, b"k5", 20, 0, 30, true);

        // PUT, LOCK, LOCK,   Read
        //  `------------------------^
        must_prewrite_put(&engine, b"k6", b"v6", b"k6", 16);
        must_commit(&engine, b"k6", 16, 20);
        must_prewrite_lock(&engine, b"k6", b"k6", 25);
        must_commit(&engine, b"k6", 25, 26);
        must_prewrite_lock(&engine, b"k6", b"k6", 28);
        must_commit(&engine, b"k6", 28, 29);
        must_cleanup_with_gc_fence(&engine, b"k6", 20, 0, 50, true);

        // PUT, LOCK,   LOCK,   Read
        //  `---------^
        must_prewrite_put(&engine, b"k7", b"v7", b"k7", 16);
        must_commit(&engine, b"k7", 16, 20);
        must_prewrite_lock(&engine, b"k7", b"k7", 25);
        must_commit(&engine, b"k7", 25, 26);
        must_cleanup_with_gc_fence(&engine, b"k7", 20, 0, 27, true);
        must_prewrite_lock(&engine, b"k7", b"k7", 28);
        must_commit(&engine, b"k7", 28, 29);

        // PUT,  Read
        //  * (GC fence ts is 0)
        must_prewrite_put(&engine, b"k8", b"v8", b"k8", 17);
        must_commit(&engine, b"k8", 17, 30);
        must_cleanup_with_gc_fence(&engine, b"k8", 30, 0, 0, true);

        // PUT, LOCK,     Read
        // `-----------^
        must_prewrite_put(&engine, b"k9", b"v9", b"k9", 18);
        must_commit(&engine, b"k9", 18, 20);
        must_prewrite_lock(&engine, b"k9", b"k9", 25);
        must_commit(&engine, b"k9", 25, 26);
        must_cleanup_with_gc_fence(&engine, b"k9", 20, 0, 27, true);

        let expected_results = vec![
            (b"k1", Some(b"v1")),
            (b"k2", None),
            (b"k3", None),
            (b"k4", None),
            (b"k5", None),
            (b"k6", Some(b"v6")),
            (b"k7", None),
            (b"k8", Some(b"v8")),
            (b"k9", None),
        ];

        for (k, v) in expected_results {
            must_get_eq(&kv_engine, &Key::from_raw(k), 40, v.map(|v| v.to_vec()));
        }
    }

    #[test]
    fn test_old_value_reuse_cursor() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();
        let value = || vec![b'v'; 1024];

        for i in 0..100 {
            let key = format!("key-{:0>3}", i).into_bytes();
            must_prewrite_put(&engine, &key, &value(), &key, 100);
            must_commit(&engine, &key, 100, 101);
            must_prewrite_put(&engine, &key, &value(), &key, 200);
            must_commit(&engine, &key, 200, 201);
        }

        let snapshot = Arc::new(kv_engine.snapshot());
        let mut cursor = new_old_value_cursor(&snapshot, CF_WRITE);
        let mut default_cursor = new_old_value_cursor(&snapshot, CF_DEFAULT);
        let mut load_default = |use_default_cursor: bool| {
            if use_default_cursor {
                let x = unsafe { std::mem::transmute::<_, &'static mut _>(&mut default_cursor) };
                Either::Right(x)
            } else {
                Either::Left(&snapshot)
            }
        };

        for &use_default_cursor in &[true, false] {
            let mut stats = Default::default();
            for i in 0..30 {
                let raw_key = format!("key-{:0>3}", i).into_bytes();
                let key = Key::from_raw(&raw_key).append_ts(150.into());
                let ld = load_default(use_default_cursor);
                let v = near_seek_old_value(&key, &mut cursor, ld, &mut stats).unwrap();
                assert!(v.map_or(false, |x| x == value()));
            }
            assert_eq!(stats.write.seek, 1);
            assert_eq!(stats.write.next, 58);
            if use_default_cursor {
                assert_eq!(stats.data.seek, 1);
                assert_eq!(stats.data.next, 58);
                assert_eq!(stats.data.get, 0);
            } else {
                assert_eq!(stats.data.seek, 0);
                assert_eq!(stats.data.next, 0);
                assert_eq!(stats.data.get, 30);
            }

            for i in 60..100 {
                let raw_key = format!("key-{:0>3}", i).into_bytes();
                let key = Key::from_raw(&raw_key).append_ts(150.into());
                let ld = load_default(use_default_cursor);
                let v = near_seek_old_value(&key, &mut cursor, ld, &mut stats).unwrap();
                assert!(v.map_or(false, |x| x == value()));
            }
            assert_eq!(stats.write.seek, 2);
            assert_eq!(stats.write.next, 144);
            if use_default_cursor {
                assert_eq!(stats.data.seek, 2);
                assert_eq!(stats.data.next, 144);
                assert_eq!(stats.data.get, 0);
            } else {
                assert_eq!(stats.data.seek, 0);
                assert_eq!(stats.data.next, 0);
                assert_eq!(stats.data.get, 70);
            }
        }
    }

    #[test]
    fn test_get_old_value_with_prefix_seek() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        cfg.writecf.pin_l0_filter_and_index_blocks = false;
        let engine = TestEngineBuilder::new().build_with_cfg(&cfg).unwrap();
        let kv_engine = engine.get_rocksdb();

        // Key must start with `z` to pass `TsFilter`'s check.
        for i in 0..4 {
            let key = format!("zkey-{:0>3}", i).into_bytes();
            must_prewrite_put(&engine, &key, b"value", &key, 100);
            must_commit(&engine, &key, 100, 101);
            kv_engine.flush_cf(CF_WRITE, true).unwrap();
        }

        let key = format!("zkey-{:0>3}", 0).into_bytes();
        let snapshot = Arc::new(kv_engine.snapshot());
        let perf_instant = ReadPerfInstant::new();
        let value = get_old_value(
            &snapshot,
            Key::from_raw(&key).append_ts(100.into()),
            102.into(),
            &mut OldValueCache::new(ReadableSize(0)),
            &mut Statistics::default(),
        )
        .unwrap();
        assert_eq!(value.unwrap(), b"value");

        // block read count should be 1 instead of 4 because some of them
        // are filtered by `prefix_seek`.
        let perf_delta = perf_instant.delta();
        assert_eq!(perf_delta.block_read_count, 1);
    }
}
