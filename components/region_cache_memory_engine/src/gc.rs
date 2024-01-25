// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{fmt::Display, sync::Arc};

use engine_traits::{CacheRange, CF_DEFAULT, CF_WRITE};
use skiplist_rs::Skiplist;
use slog_global::{info, warn};
use txn_types::{Key, WriteRef, WriteType};

use crate::{
    keys::{decode_key, encoding_for_filter, InternalKey, InternalKeyComparator},
    memory_limiter::GlobalMemoryLimiter,
    RangeCacheMemoryEngine,
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

pub struct GcRunner {
    memory_engine: RangeCacheMemoryEngine,
}

impl GcRunner {
    pub fn new(memory_engine: RangeCacheMemoryEngine) -> Self {
        Self { memory_engine }
    }

    fn gc_range(&mut self, range: &CacheRange, safe_point: u64) {
        let (skiplist_engine, safe_ts) = {
            let mut core = self.memory_engine.core().lock().unwrap();
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
            while let Some((key, val)) = self.default_cf_handle.get_with_key(&default_key) {
                self.default_cf_handle.remove(key.as_slice());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use core::slice::SlicePattern;
    use std::sync::Arc;

    use bytes::Bytes;
    use engine_traits::{CacheRange, RangeCacheEngine, CF_DEFAULT, CF_WRITE};
    use skiplist_rs::Skiplist;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::Filter;
    use crate::{
        engine::SkiplistEngine,
        gc::GcRunner,
        keys::{encode_key, encoding_for_filter, InternalKeyComparator, ValueType},
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
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let mut core = engine.core().lock().unwrap();
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

        let mut worker = GcRunner::new(engine);

        // gc will not remove the latest mvcc put below safe point
        worker.gc_range(&range, 14);
        assert_eq!(2, element_count(&default));
        assert_eq!(2, element_count(&write));

        worker.gc_range(&range, 16);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));

        // rollback will not make the first older version be filtered
        rollback_data(b"key1", 17, 16, &write);
        worker.gc_range(&range, 17);
        assert_eq!(1, element_count(&default));
        assert_eq!(1, element_count(&write));
        let key = encode_key(b"key1", TimeStamp::new(15));
        assert!(write.get(&key).is_some());
        let key = encode_key(b"key1", TimeStamp::new(14));
        assert!(default.get(&key).is_some());

        // unlike in WriteCompactionFilter, the latest mvcc delete below safe point will
        // be filtered
        delete_data(b"key1", 19, 18, &write);
        worker.gc_range(&range, 19);
        assert_eq!(0, element_count(&write));
        assert_eq!(0, element_count(&default));
    }

    #[test]
    fn test_snapshot_block_gc() {
        let engine = RangeCacheMemoryEngine::new(Arc::default());
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());
        let (write, default) = {
            let mut core = engine.core().lock().unwrap();
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

        let mut worker = GcRunner::new(engine.clone());
        let s1 = engine.snapshot(range.clone(), 10, u64::MAX);
        let s2 = engine.snapshot(range.clone(), 11, u64::MAX);
        let s3 = engine.snapshot(range.clone(), 20, u64::MAX);

        // nothing will be removed due to snapshot 5
        worker.gc_range(&range, 30);
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        drop(s1);
        worker.gc_range(&range, 30);
        assert_eq!(5, element_count(&default));
        assert_eq!(5, element_count(&write));

        drop(s2);
        worker.gc_range(&range, 30);
        assert_eq!(4, element_count(&default));
        assert_eq!(4, element_count(&write));

        drop(s3);
        worker.gc_range(&range, 30);
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));
    }
}
