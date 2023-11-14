// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use collections::HashMap;
use crossbeam_skiplist::{map::Entry, SkipMap};
use engine_traits::{CfNamesExt, SnapshotMiscExt, CF_DEFAULT, CF_LOCK, CF_WRITE};
use slog_global::info;
use txn_types::Key;

use crate::{key::ByteWiseComparator, IterRef, Skiplist};

pub type MemoryBatch = HashMap<u64, [Vec<(Bytes, Bytes)>; 3]>;

pub fn cf_to_id(cf: &str) -> u8 {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        _ => panic!("unrecognized cf {}", cf),
    }
}

#[derive(Clone)]
pub struct RegionMemoryEngine {
    pub data: [Arc<Skiplist<ByteWiseComparator>>; 3],
}

impl Default for RegionMemoryEngine {
    fn default() -> Self {
        RegionMemoryEngine {
            data: [
                Arc::new(Skiplist::with_capacity(
                    ByteWiseComparator::default(),
                    1 << 20,
                    true,
                )),
                Arc::new(Skiplist::with_capacity(
                    ByteWiseComparator::default(),
                    1 << 20,
                    true,
                )),
                Arc::new(Skiplist::with_capacity(
                    ByteWiseComparator::default(),
                    1 << 20,
                    true,
                )),
            ],
        }
    }
}

#[derive(Clone)]
pub struct LruMemoryEngine {
    pub core: Arc<Mutex<LruMemoryEngineCore>>,
}

impl Default for LruMemoryEngine {
    fn default() -> Self {
        LruMemoryEngine::new()
    }
}

pub struct LruMemoryEngineCore {
    pub engine: HashMap<u64, RegionMemoryEngine>,
    // todo: replace it
    snapshot_list: Vec<u64>,
    max_version: Arc<AtomicU64>,
}

impl LruMemoryEngine {
    pub fn new() -> Self {
        LruMemoryEngine {
            core: Arc::new(Mutex::new(LruMemoryEngineCore {
                engine: HashMap::default(),
                snapshot_list: vec![],
                max_version: Arc::new(AtomicU64::new(0)),
            })),
        }
    }

    pub fn new_region(&self, region_id: u64) {
        self.core
            .lock()
            .unwrap()
            .engine
            .insert(region_id, RegionMemoryEngine::default());
    }

    pub fn consume_batch(&self, batch: MemoryBatch) {
        for (id, batch) in batch.into_iter() {
            let (max_version, regional_engine) = {
                let mut core = self.core.lock().unwrap();
                let max_version = core.max_version.clone();
                if core.engine.get(&id).is_none() {
                    info!(
                        "create region memory engine";
                        "region_id" => id,
                    );
                }
                let regional_engine = core.engine.entry(id).or_default();
                (max_version, regional_engine.data.clone())
            };
            let mut max_ts = max_version.load(Ordering::Acquire);
            batch
                .into_iter()
                .zip(regional_engine.into_iter())
                .for_each(|(kvs, engine)| {
                    kvs.into_iter().for_each(|(k, v)| {
                        let ts = Key::decode_ts_from(k.as_slice()).unwrap().0;
                        max_ts = u64::max(ts, max_ts);
                        engine.put(k, v);
                    });
                });
            max_version.store(max_ts, Ordering::Release);
        }
    }
}

// impl Snapshot for LruMemoryEngine {}

unsafe impl Send for LruMemoryEngine {}
unsafe impl Sync for LruMemoryEngine {}

impl LruMemoryEngine {
    pub fn new_snapshot(&self) -> MemoryEngineSnapshot {
        let snapshot = {
            let mut core = self.core.lock().unwrap();
            let snapshot = core.max_version.load(Ordering::Relaxed);
            core.snapshot_list.push(snapshot);
            snapshot
        };

        MemoryEngineSnapshot {
            engine: self.clone(),
            snapshot,
        }
    }
}

// todo(SpadeA): clone and snapshot
#[derive(Clone)]
pub struct MemoryEngineSnapshot {
    engine: LruMemoryEngine,
    snapshot: u64,
}

impl Debug for MemoryEngineSnapshot {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Drop for MemoryEngineSnapshot {
    fn drop(&mut self) {
        let _core = self.engine.core.lock().unwrap();
        // core.snapshot_list.remove(self.snapshot);
        // todo: more cleanup works including gc
    }
}

impl MemoryEngineSnapshot {
    pub fn iterator_opt(
        &self,
        cf: &str,
        opts: engine_traits::IterOptions,
    ) -> Option<MemoryEngineIterator> {
        if opts.region_id().is_none() {
            return None;
        }
        let regional_engine = self
            .engine
            .core
            .lock()
            .unwrap()
            .engine
            .get(&opts.region_id().unwrap())?
            .data[cf_to_id(cf) as usize]
            .clone();
        let (lower_bound, upper_bound) = opts.build_bounds();
        Some(MemoryEngineIterator {
            cf: String::from(cf),
            valid: false,
            lower_bound,
            upper_bound,
            iter: regional_engine.iter(),
        })
    }
}

pub struct MemoryEngineIterator {
    cf: String,
    valid: bool,
    iter: IterRef<Skiplist<ByteWiseComparator>, ByteWiseComparator>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

unsafe impl Send for MemoryEngineIterator {}

// use engine_traits::Iterator;
impl MemoryEngineIterator {
    pub fn key(&self) -> &[u8] {
        assert!(self.valid);
        self.iter.key().as_slice()
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid);
        self.iter.value().as_slice()
    }

    pub fn next(&mut self) -> engine_traits::Result<bool> {
        self.iter.next();
        self.valid = self.iter.valid();
        Ok(self.valid)
    }

    pub fn prev(&mut self) -> engine_traits::Result<bool> {
        self.iter.prev();
        self.valid = self.iter.valid();
        Ok(self.valid)
    }

    pub fn seek(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        let start = if let Some(ref lower_bound) = self.lower_bound && key < lower_bound.as_slice() {
            &lower_bound
        } else {
            key
        };
        self.iter.seek(start);
        self.valid = self.iter.valid();
        Ok(self.valid)
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        let end = if let Some(ref upper_bound) = self.upper_bound && key > upper_bound.as_slice() {
            &upper_bound
        } else {
            key
        };
        self.iter.seek_for_prev(end);
        self.valid = self.iter.valid();
        Ok(self.valid)
    }

    pub fn seek_to_first(&mut self) -> engine_traits::Result<bool> {
        if let Some(ref lower_bound) = self.lower_bound {
            self.iter.seek(lower_bound.as_slice());
        } else {
            self.iter.seek_to_first()
        }
        self.valid = self.iter.valid();
        Ok(self.valid)
    }

    pub fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        if let Some(ref upper_bound) = self.upper_bound {
            self.iter.seek(upper_bound.as_slice());
        } else {
            self.iter.seek_to_last();
        }
        self.valid = self.iter.valid();
        Ok(self.valid)
    }

    pub fn valid(&self) -> engine_traits::Result<bool> {
        Ok(self.valid)
    }
}

mod test {
    use super::*;

    fn key_with_ts(key: &[u8], ts: u64) -> Bytes {
        let mut key = Key::from_raw(key);
        let key = key.append_ts((ts).into());
        Bytes::from(key.as_encoded().to_vec())
    }

    #[test]
    fn test_x() {
        let lru = LruMemoryEngine::new();
        lru.new_region(1);
        let mut a = MemoryBatch::default();
        a.insert(
            1,
            [
                vec![
                    (key_with_ts(b"zkkkkk1", 1), Bytes::from(b"val1".to_vec())),
                    (key_with_ts(b"zkkkkk1", 2), Bytes::from(b"val2".to_vec())),
                    (key_with_ts(b"zkkkkk2", 3), Bytes::from(b"val3".to_vec())),
                    (key_with_ts(b"zkkkkk3", 4), Bytes::from(b"val4".to_vec())),
                ],
                vec![],
                vec![],
            ],
        );

        lru.consume_batch(a);

        let snapshot = lru.new_snapshot();
        let mut opts = engine_traits::IterOptions::default();
        opts.set_region_id(1);
        let mut iter = snapshot.iterator_opt(CF_DEFAULT, opts).unwrap();
        let _ = iter.seek(b"kkkkk1");
        while iter.valid().unwrap() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            let _ = iter.next();
        }
    }
}
