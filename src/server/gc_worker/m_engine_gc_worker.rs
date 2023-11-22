// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;

use futures_executor::block_on;
use kvproto::metapb::Region;
use keys::data_key;
use skiplist::memory_engine::LruMemoryEngine;
use tidb_query_datatype::codec::mysql::Res;
use tikv_kv::{Engine, ScanMode, SnapContext};
use txn_types::{TimeStamp, Key, WriteType, WriteRef};
use crate::server::gc_worker::compaction_filter::{parse_write, split_ts};

use super::{gc_worker::init_snap_ctx, Result};
use crate::storage::mvcc::MvccReader;

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
    fn gc_regions(&mut self, region: Region, safe_point: TimeStamp) -> Result<()> {
        let region_id = region.get_id();
        let region_m_engine = {
            let safe_point = safe_point.into_inner();
            let m_engine = self.lru_memory_engine.core.lock().unwrap();
            let min_snapshot = m_engine.snapshot_list.get(&region_id).map(|list| list[0]);
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
    }

    fn create_reader(
        &mut self,
        region: &Region,
    ) -> Result<MvccReader<E::Snap>> {
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
    mvcc_deletion_overlaps: Option<usize>,
    remove_older: bool,

    versions: usize,
    filtered: usize,
    total_versions: usize,
    total_filtered: usize,
    mvcc_rollback_and_locks: usize,
}

impl Filter {
    fn filter(&mut self, key: &[u8], value: &[u8]) ->std::result::Result<FilterDecision, String> {
        let (mvcc_key_prefix, commit_ts) = split_ts(key)?;
        if commit_ts > self.safe_point {
            return Ok(FilterDecision::Keep);
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
                WriteType::Put|WriteType::Delete => self.remove_older = true,
            }
        }

        if !filtered {
            return Ok(FilterDecision::Keep);
        }
        self.filtered += 1;
        self.handle_filtered_write(write)?;
        Ok(FilterDecision::Remove)
    }

    fn handle_filtered_write(&mut self, write: WriteRef<'_>) -> Result<(), String> {
        unimplemented!()
    }
}