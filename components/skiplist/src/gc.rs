// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, sync::Arc};

use engine_traits::{CF_DEFAULT, CF_WRITE};
use slog_global::{info, warn};
use tikv_util::worker::Runnable;
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use crate::{
    memory_engine::{cf_to_id, LruMemoryEngine},
    ByteWiseComparator, Skiplist,
};

pub fn parse_write(value: &[u8]) -> Result<WriteRef<'_>, String> {
    match WriteRef::parse(value) {
        Ok(write) => Ok(write),
        Err(_) => Err(format!(
            "invalid write cf value: {}",
            log_wrappers::Value(value)
        )),
    }
}

pub fn split_ts(key: &[u8]) -> Result<(&[u8], u64), String> {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => Ok((key, ts.into_inner())),
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

#[derive(Debug)]
pub struct GcTask {
    pub safe_point: TimeStamp,
}

impl Display for GcTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcTask")
            .field("safe_point", &self.safe_point)
            .finish()
    }
}

pub struct GcRunner {
    lru_memory_engine: LruMemoryEngine,
}

impl GcRunner {
    pub fn new(engine: LruMemoryEngine) -> Self {
        Self {
            lru_memory_engine: engine,
        }
    }

    // 1. lock
    // 2. get snapshot list
    // 3. use min of snapshot and safe_point to be the new safepoint (should be
    // larger than the prev one)
    // 4. unlock -- as the safe point is updated, so any txn start ts less than it
    // will not use this memory engine
    // 5. do gc
    fn gc_region(&mut self, region_id: u64, safe_point: TimeStamp) {
        let (region_m_engine, safet_point) = {
            let safe_point = safe_point.into_inner();
            let mut core = self.lru_memory_engine.core.lock().unwrap();
            let min_snapshot = if let Some(snaphots) = core.snapshots.get(&region_id) {
                snaphots
                    .first_key_value()
                    .map(|(ts, _)| *ts)
                    .unwrap_or(u64::MAX)
            } else {
                u64::MAX
            };

            if let Some(memory_engine) = core.engine.get_mut(&region_id) {
                let safe_point = u64::min(safe_point, min_snapshot);
                if memory_engine.safe_point >= safe_point {
                    info!(
                        "safe point not large enough";
                        "prev" => memory_engine.safe_point,
                        "current" => safe_point,
                    );
                    return;
                }

                info!(
                    "safe point update";
                    "prev" => memory_engine.safe_point,
                    "current" => safe_point,
                    "region_id" => region_id,
                );
                // Only here can modify this.
                memory_engine.safe_point = safe_point;

                (memory_engine.clone(), safe_point)
            } else {
                return;
            }
        };

        let write_cf_sklist = &region_m_engine.data[cf_to_id(CF_WRITE) as usize];
        let default_cf_sklist = &region_m_engine.data[cf_to_id(CF_DEFAULT) as usize];
        let mut filter = Filter::new(
            safet_point,
            default_cf_sklist.clone(),
            write_cf_sklist.clone(),
        );

        let mut iter = write_cf_sklist.iter();
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

        let mut count2 = 0;
        if filter.filtered > 0 {
            let mut iter = write_cf_sklist.iter();
            iter.seek_to_first();
            while iter.valid() {
                let k = iter.key();
                let v = iter.value();
                iter.next();
                count2 += 1;
            }
        }

        info!(
            "region gc complete";
            "region_id" => region_id,
            "total_version" => count,
            "total_version_again" => count2,
            "unique_keys" => filter.unique_key,
            "outdated_version" => filter.versions,
            "outdated_delete_version" => filter.delete_versions,
            "filtered_version" => filter.filtered,
            "max_duplicate_version" => filter.max_duplicate_version,
        );
    }
}

impl Runnable for GcRunner {
    type Task = GcTask;

    fn run(&mut self, task: Self::Task) {
        let regions: Vec<u64> = {
            let core = self.lru_memory_engine.core.lock().unwrap();
            core.engine.keys().into_iter().map(|id| *id).collect()
        };

        for region_id in regions {
            self.gc_region(region_id, task.safe_point);
        }
    }
}

struct Filter {
    safe_point: u64,
    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    default_cf: Arc<Skiplist<ByteWiseComparator>>,
    write_cf: Arc<Skiplist<ByteWiseComparator>>,

    cached_delete_key: Option<Vec<u8>>,

    versions: usize,
    delete_versions: usize,
    filtered: usize,
    unique_key: usize,
    max_duplicate_version: usize,
    cur_duplicate_version: usize,
    mvcc_rollback_and_locks: usize,
}

impl Drop for Filter {
    fn drop(&mut self) {
        if let Some(cached_delete_key) = self.cached_delete_key.take() {
            self.write_cf.remove(cached_delete_key.as_slice());
        }
    }
}

impl Filter {
    fn new(
        safe_point: u64,
        default_cf: Arc<Skiplist<ByteWiseComparator>>,
        write_cf: Arc<Skiplist<ByteWiseComparator>>,
    ) -> Self {
        Self {
            safe_point,
            default_cf,
            write_cf,
            unique_key: 0,
            mvcc_key_prefix: vec![],
            delete_versions: 0,
            versions: 0,
            filtered: 0,
            cached_delete_key: None,
            mvcc_rollback_and_locks: 0,
            max_duplicate_version: 0,
            cur_duplicate_version: 0,
            remove_older: false,
        }
    }

    fn filter(&mut self, key: &[u8], value: &[u8]) -> std::result::Result<(), String> {
        let (mvcc_key_prefix, commit_ts) = split_ts(key)?;
        if commit_ts > self.safe_point {
            return Ok(());
        }

        self.versions += 1;
        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.cur_duplicate_version = 1;
            self.unique_key += 1;
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            self.remove_older = false;
            if let Some(cached_delete_key) = self.cached_delete_key.take() {
                self.write_cf.remove(cached_delete_key.as_slice());
            }
        } else {
            self.cur_duplicate_version += 1;
            self.max_duplicate_version =
                usize::max(self.max_duplicate_version, self.cur_duplicate_version);
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
                    
                    // need to delete at last to avoid order versions appear
                    self.cached_delete_key = Some(key.to_vec());
                }
            }
        }

        if !filtered {
            return Ok(());
        }
        self.filtered += 1;
        self.handle_filtered_write(write)?;
        self.write_cf.remove(key);
        Ok(())
    }

    fn handle_filtered_write(&mut self, write: WriteRef<'_>) -> std::result::Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            let key =
                Key::from_encoded_slice(self.mvcc_key_prefix.as_slice()).append_ts(write.start_ts);
            self.default_cf.remove(key.as_encoded());
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use core::slice::SlicePattern;
    use std::sync::Arc;

    use bytes::Bytes;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::GcRunner;
    use crate::{
        gc::Filter,
        memory_engine::{cf_to_id, LruMemoryEngine, RegionMemoryEngine},
        ByteWiseComparator, Skiplist,
    };

    fn put_data(
        key: &[u8],
        value: &[u8],
        start_ts: u64,
        commit_ts: u64,
        short_value: bool,
        default_cf: &Arc<Skiplist<ByteWiseComparator>>,
        write_cf: &Arc<Skiplist<ByteWiseComparator>>,
    ) {
        let write_k = Key::from_raw(key).append_ts(TimeStamp(commit_ts));
        let write_v = Write::new(
            WriteType::Put,
            TimeStamp(start_ts),
            if short_value {
                Some(value.to_vec())
            } else {
                None
            },
        );
        write_cf.put(
            Bytes::from(write_k.into_encoded()),
            Bytes::from(write_v.as_ref().to_bytes()),
        );

        if !short_value {
            let default_k = Key::from_raw(key).append_ts(TimeStamp(start_ts));
            default_cf.put(
                Bytes::from(default_k.into_encoded()),
                Bytes::from(value.to_vec()),
            );
        }
    }

    fn element_count(sklist: &Arc<Skiplist<ByteWiseComparator>>) -> u64 {
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
        let region_m_engine = RegionMemoryEngine::default();
        let write = region_m_engine.data[cf_to_id("write") as usize].clone();
        let default = region_m_engine.data[cf_to_id("default") as usize].clone();

        put_data(b"key1", b"value1", 10, 15, false, &default, &write);
        put_data(b"key2", b"value21", 10, 15, false, &default, &write);
        put_data(b"key2", b"value22", 20, 25, false, &default, &write);
        put_data(b"key2", b"value23", 30, 35, false, &default, &write);
        put_data(b"key3", b"value31", 20, 25, false, &default, &write);
        put_data(b"key3", b"value32", 30, 35, false, &default, &write);

        let mut filter = Filter::new(50, default.clone(), write.clone());

        assert_eq!(6, element_count(&default));
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
        assert_eq!(count, 6);

        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));

        assert!(
            write
                .get(Key::from_raw(b"key1").append_ts(TimeStamp(15)).as_encoded())
                .is_some()
        );
        assert!(
            write
                .get(Key::from_raw(b"key2").append_ts(TimeStamp(35)).as_encoded())
                .is_some()
        );
        assert!(
            write
                .get(Key::from_raw(b"key3").append_ts(TimeStamp(35)).as_encoded())
                .is_some()
        );
        assert!(
            default
                .get(Key::from_raw(b"key1").append_ts(TimeStamp(10)).as_encoded())
                .is_some()
        );
        assert!(
            default
                .get(Key::from_raw(b"key2").append_ts(TimeStamp(30)).as_encoded())
                .is_some()
        );
        assert!(
            default
                .get(Key::from_raw(b"key3").append_ts(TimeStamp(30)).as_encoded())
                .is_some()
        );
    }

    #[test]
    fn test_snapshot_block_gc() {
        let lru_engine = LruMemoryEngine::new();
        lru_engine.new_region(1);
        let region_m_engine = lru_engine
            .core
            .lock()
            .unwrap()
            .engine
            .get(&1)
            .unwrap()
            .clone();

        let write = region_m_engine.data[cf_to_id("write") as usize].clone();
        let default = region_m_engine.data[cf_to_id("default") as usize].clone();
        put_data(b"key1", b"value1", 10, 11, false, &default, &write);
        put_data(b"key2", b"value21", 10, 11, false, &default, &write);
        put_data(b"key2", b"value22", 15, 16, false, &default, &write);
        put_data(b"key2", b"value23", 20, 21, false, &default, &write);
        put_data(b"key3", b"value31", 5, 6, false, &default, &write);
        put_data(b"key3", b"value32", 10, 11, false, &default, &write);
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        let mut worker = GcRunner::new(lru_engine.clone());
        let s1 = lru_engine.new_snapshot(1, 10).unwrap();
        let s2 = lru_engine.new_snapshot(1, 11).unwrap();
        let s3 = lru_engine.new_snapshot(1, 20).unwrap();

        // nothing will be removed due to snapshot 5
        worker.gc_region(1, TimeStamp(30));
        assert_eq!(6, element_count(&default));
        assert_eq!(6, element_count(&write));

        drop(s1);
        worker.gc_region(1, TimeStamp(30));
        assert_eq!(5, element_count(&default));
        assert_eq!(5, element_count(&write));

        drop(s2);
        worker.gc_region(1, TimeStamp(30));
        assert_eq!(4, element_count(&default));
        assert_eq!(4, element_count(&write));

        drop(s3);
        worker.gc_region(1, TimeStamp(30));
        assert_eq!(3, element_count(&default));
        assert_eq!(3, element_count(&write));
    }
}
