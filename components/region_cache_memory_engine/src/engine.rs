// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    ops::Deref,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_traits::{
    CfNamesExt, DbVector, IterOptions, Iterable, Iterator, Mutable, Peekable, ReadOptions,
    RegionCacheEngine, Result, Snapshot, SnapshotMiscExt, WriteBatch, WriteBatchExt, WriteOptions,
};
use skiplist_rs::{ByteWiseComparator, IterRef, Skiplist};

/// RegionMemoryEngine stores data for a specific cached region
///
/// todo: The skiplist used here currently is for test purpose. Replace it
/// with a formal implementation.
#[derive(Clone)]
pub struct RegionMemoryEngine {
    data: [Arc<Skiplist<ByteWiseComparator>>; 3],
}

impl Debug for RegionMemoryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

type SnapshotList = BTreeMap<u64, u64>;

#[derive(Default)]
pub struct RegionMemoryMeta {
    // It records the snapshots that have been granted previsously with specific snapshot_ts. We
    // should guarantee that the data visible to any one of the snapshot in it will not be removed.
    snapshots: SnapshotList,
    // It indicates whether the region is readable. False means integrity of the data in this
    // cached region is not satisfied due to being evicted for instance.
    can_read: bool,
    // Request with read_ts below it is not eligible for granting snapshot.
    // Note: different region can have different safe_ts.
    safe_ts: u64,
}

#[derive(Default)]
pub struct RegionCacheMemoryEngineCore {
    engine: HashMap<u64, RegionMemoryEngine>,
    region_metats: HashMap<u64, RegionMemoryMeta>,
}

/// The RegionCacheMemoryEngine serves as a region cache, storing hot regions in
/// the leaders' store. Incoming writes that are written to disk engine (now,
/// RocksDB) are also written to the RegionCacheMemoryEngine, leading to a
/// mirrored data set in the cached regions with the disk engine.
///
/// A load/evict unit manages the memory, deciding which regions should be
/// evicted when the memory used by the RegionCacheMemoryEngine reaches a
/// certain limit, and determining which regions should be loaded when there is
/// spare memory capacity.
///
/// The safe point lifetime differs between RegionCacheMemoryEngine and the disk
/// engine, often being much shorter in RegionCacheMemoryEngine. This means that
/// RegionCacheMemoryEngine may filter out some keys that still exist in the
/// disk engine, thereby improving read performance as fewer duplicated keys
/// will be read. If there's a need to read keys that may have been filtered by
/// RegionCacheMemoryEngine (as indicated by read_ts and safe_point of the
/// cached region), we resort to using a the disk engine's snapshot instead.
#[derive(Clone, Default)]
pub struct RegionCacheMemoryEngine {
    core: Arc<Mutex<RegionCacheMemoryEngineCore>>,
}

impl Debug for RegionCacheMemoryEngine {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

impl RegionCacheMemoryEngine {
    pub fn new() -> Self {
        RegionCacheMemoryEngine::default()
    }
}

impl RegionCacheEngine for RegionCacheMemoryEngine {
    type Snapshot = RegionCacheSnapshot;

    fn snapshot(&self, region_id: u64, read_ts: u64) -> Option<Self::Snapshot> {
        unimplemented!()
    }
}

// todo: fill fields needed
pub struct RegionCacheWriteBatch;

impl WriteBatchExt for RegionCacheMemoryEngine {
    type WriteBatch = RegionCacheWriteBatch;
    // todo: adjust it
    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn write_batch(&self) -> Self::WriteBatch {
        RegionCacheWriteBatch {}
    }

    fn write_batch_with_cap(&self, _: usize) -> Self::WriteBatch {
        RegionCacheWriteBatch {}
    }
}

pub struct RegionCacheIterator {
    valid: bool,
    prefix_same_as_start: bool,
    prefix: Option<Vec<u8>>,
    iter: IterRef<Skiplist<ByteWiseComparator>, ByteWiseComparator>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

impl Iterable for RegionCacheMemoryEngine {
    type Iterator = RegionCacheIterator;

    fn iterator(&self, cf: &str) -> Result<Self::Iterator> {
        unimplemented!()
    }

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}

impl Iterator for RegionCacheIterator {
    fn key(&self) -> &[u8] {
        unimplemented!()
    }

    fn value(&self) -> &[u8] {
        unimplemented!()
    }

    fn next(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn prev(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        unimplemented!()
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        unimplemented!()
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        unimplemented!()
    }

    fn valid(&self) -> Result<bool> {
        unimplemented!()
    }
}

impl WriteBatch for RegionCacheWriteBatch {
    fn write_opt(&mut self, _: &WriteOptions) -> Result<u64> {
        unimplemented!()
    }

    fn data_size(&self) -> usize {
        unimplemented!()
    }

    fn count(&self) -> usize {
        unimplemented!()
    }

    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    fn should_write_to_engine(&self) -> bool {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn set_save_point(&mut self) {
        unimplemented!()
    }

    fn pop_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn merge(&mut self, _: Self) -> Result<()> {
        unimplemented!()
    }
}

impl Mutable for RegionCacheWriteBatch {
    fn put(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn put_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete(&mut self, _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_cf(&mut self, _: &str, _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }

    fn delete_range_cf(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<()> {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub struct RegionCacheSnapshot {
    region_id: u64,
    snapshot_ts: u64,
    engine: RegionMemoryEngine,
}

impl Snapshot for RegionCacheSnapshot {}

impl Iterable for RegionCacheSnapshot {
    type Iterator = RegionCacheIterator;

    fn iterator(&self, cf: &str) -> Result<Self::Iterator> {
        unimplemented!()
    }

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!()
    }
}

impl Peekable for RegionCacheSnapshot {
    type DbVector = RegionCacheDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DbVector>> {
        unimplemented!()
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        unimplemented!()
    }
}

impl CfNamesExt for RegionCacheSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        unimplemented!()
    }
}

impl SnapshotMiscExt for RegionCacheSnapshot {
    fn sequence_number(&self) -> u64 {
        self.snapshot_ts
    }
}

// todo: fill fields needed
#[derive(Debug)]
pub struct RegionCacheDbVector;

impl Deref for RegionCacheDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unimplemented!()
    }
}

impl DbVector for RegionCacheDbVector {}

impl<'a> PartialEq<&'a [u8]> for RegionCacheDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        unimplemented!()
    }
}
