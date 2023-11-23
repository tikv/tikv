// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::Ordering, Arc};

use futures_executor::block_on;
use keys::data_key;
use kvproto::metapb::Region;
use nom::AsBytes;
use skiplist::{
    memory_engine::{LruMemoryEngine, RegionMemoryEngine},
    ByteWiseComparator, Skiplist,
};
use tidb_query_datatype::codec::mysql::Res;
use tikv_kv::{Engine, ScanMode, SnapContext};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use super::{gc_worker::init_snap_ctx, Result};
use crate::{
    server::gc_worker::compaction_filter::{parse_write, split_ts},
    storage::mvcc::MvccReader,
};

pub struct GcWorker<E: Engine> {
    store_id: u64,
    lru_memory_engine: LruMemoryEngine,
    engine: E,
}

impl<E: Engine> GcWorker<E> {
    // 1. lock
    // 2. get snapshot list
    // 3. use min of snapshot and safe_point to be the new safepoint (should be
    // larger than the prev one)
    // 4. unlock -- as the safe point is updated, so any txn start ts less than it
    // will not use this memory engine
    // 5. do gc
    fn gc_region(&mut self, region: Region, safe_point: TimeStamp) -> Result<()> {
        let region_id = region.get_id();
        let region_m_engine = {
            let safe_point = safe_point.into_inner();
            let m_engine = self.lru_memory_engine.core.lock().unwrap();
            let min_snapshot = m_engine
                .snapshot_list
                .get(&region_id)
                .map(|list| list[0])
                .or_else(|| Some(u64::MAX))
                .unwrap();
            if let Some(memory_engine) = m_engine.engine.get(&region_id) {
                let safe_point = u64::min(safe_point, min_snapshot);
                if memory_engine.safe_point.load(Ordering::SeqCst) >= safe_point {
                    return Ok(());
                }

                // Only here can modify this.
                memory_engine.safe_point.store(safe_point, Ordering::SeqCst);

                memory_engine.clone()
            } else {
                return Ok(());
            }
        };

        let mut reader = self.create_reader(&region)?;
        Ok(())
    }

    fn create_reader(&mut self, region: &Region) -> Result<MvccReader<E::Snap>> {
        let region_start = Key::from_encoded(data_key(region.get_start_key()));
        let region_end = Key::from_encoded(data_key(region.get_end_key()));

        let mut reader = {
            let snapshot = self.get_snapshot(self.store_id, region)?;
            MvccReader::new(snapshot, Some(ScanMode::Forward), false)
        };
        reader.set_range(Some(region_start), Some(region_end));
        Ok(reader)
    }

    fn get_snapshot(&mut self, store_id: u64, region: &Region) -> Result<<E as Engine>::Snap> {
        let ctx = init_snap_ctx(store_id, region);
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };

        Ok(block_on(async {
            tikv_kv::snapshot(&mut self.engine, snap_ctx).await
        })?)
    }
}

pub enum FilterDecision {
    /// The record will be kept instead of filtered.
    Keep,
    /// The record will be filtered, and a tombstone will be left.
    Remove,
}

struct Filter {
    safe_point: u64,
    mvcc_key_prefix: Vec<u8>,
    remove_older: bool,

    default_cf: Arc<Skiplist<ByteWiseComparator>>,
    write_cf: Arc<Skiplist<ByteWiseComparator>>,

    versions: usize,
    filtered: usize,
    total_versions: usize,
    total_filtered: usize,
    mvcc_rollback_and_locks: usize,
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
            mvcc_key_prefix: vec![],
            versions: 0,
            filtered: 0,
            total_filtered: 0,
            total_versions: 0,
            mvcc_rollback_and_locks: 0,
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
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            self.remove_older = false;
        }

        let mut filtered = self.remove_older;
        let write = parse_write(value)?;
        if !self.remove_older {
            match write.write_type {
                WriteType::Rollback | WriteType::Lock => {
                    self.mvcc_rollback_and_locks += 1;
                    filtered = true;
                }
                WriteType::Put | WriteType::Delete => self.remove_older = true,
            }
        }

        if !filtered {
            return Ok(());
        }
        self.filtered += 1;
        self.handle_filtered_write(write)?;
        self.default_cf.remove(key);
        Ok(())
    }

    fn handle_filtered_write(&mut self, write: WriteRef<'_>) -> std::result::Result<(), String> {
        if write.short_value.is_none() && write.write_type == WriteType::Put {
            let key =
                Key::from_encoded_slice(self.mvcc_key_prefix.as_bytes()).append_ts(write.start_ts);
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
    use skiplist::{
        memory_engine::{cf_to_id, RegionMemoryEngine},
        ByteWiseComparator, Skiplist,
    };
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use crate::server::gc_worker::m_engine_gc_worker::Filter;

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

    #[test]
    fn test_filter() {
        let region_m_engine = RegionMemoryEngine::default();
        let write = region_m_engine.data[cf_to_id("write") as usize].clone();
        let default = region_m_engine.data[cf_to_id("default") as usize].clone();

        put_data(b"key1", b"value1", 10, 15, false, &default, &write);
        put_data(b"key2", b"value21", 10, 15, true, &default, &write);
        put_data(b"key2", b"value22", 20, 25, false, &default, &write);
        put_data(b"key2", b"value23", 30, 35, false, &default, &write);
        put_data(b"key3", b"value31", 20, 25, false, &default, &write);
        put_data(b"key3", b"value32", 30, 35, true, &default, &write);

        let mut filter = Filter::new(10, default.clone(), write.clone());

        let mut iter = write.iter();
        iter.seek_to_first();
        while iter.valid() {
            let k = iter.key();
            let v = iter.value();
            filter.filter(k.as_slice(), v.as_slice()).unwrap();
            iter.next();
        }
    }
}
