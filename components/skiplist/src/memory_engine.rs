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
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use slog_global::info;

use crate::{key::ByteWiseComparator, IterRef, Skiplist};

pub enum ValueType {
    Put(Bytes),
    Delete,
}

pub type MemoryBatch = HashMap<u64, [Vec<(Bytes, ValueType)>; 3]>;

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
    pub safe_point: AtomicU64,
}

impl RegionMemoryEngine {
    // todo: concurrency between split and gc?
    pub fn split(&self, split_key: Vec<Vec<u8>>) -> Vec<RegionMemoryEngine> {
        let cf1 = self.data[0].split(&split_key);
        let cf2 = self.data[1].split(&split_key);
        let cf3 = self.data[2].split(&split_key);

        let mut res = Vec::with_capacity(split_key.len());
        for ((s1, s2), s3) in cf1.into_iter().zip(cf2.into_iter()).zip(cf3.into_iter()) {
            res.push(RegionMemoryEngine {
                data: [Arc::new(s1), Arc::new(s2), Arc::new(s3)],
                safe_point: AtomicU64::new(self.safe_point.load(Ordering::SeqCst)),
            });
        }

        res
    }
}

impl Debug for RegionMemoryEngine {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Default for RegionMemoryEngine {
    fn default() -> Self {
        RegionMemoryEngine {
            data: [
                Arc::new(Skiplist::with_capacity(
                    ByteWiseComparator::default(),
                    5 << 30,
                    true,
                )),
                Arc::new(Skiplist::with_capacity(
                    ByteWiseComparator::default(),
                    5 << 30,
                    true,
                )),
                Arc::new(Skiplist::with_capacity(
                    ByteWiseComparator::default(),
                    5 << 30,
                    true,
                )),
            ],
            safe_point: AtomicU64::new(0),
        }
    }
}

#[derive(Clone)]
pub struct LruMemoryEngine {
    pub core: Arc<Mutex<LruMemoryEngineCore>>,
}

impl Debug for LruMemoryEngine {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Default for LruMemoryEngine {
    fn default() -> Self {
        LruMemoryEngine::new()
    }
}

pub struct LruMemoryEngineCore {
    pub engine: HashMap<u64, RegionMemoryEngine>,
    // todo: replace it
    pub snapshot_list: HashMap<u64, Vec<u64>>,
}

impl LruMemoryEngine {
    pub fn new() -> Self {
        LruMemoryEngine {
            core: Arc::new(Mutex::new(LruMemoryEngineCore {
                engine: HashMap::default(),
                snapshot_list: HashMap::default(),
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
            let regional_engine = {
                let mut core = self.core.lock().unwrap();
                if core.engine.get(&id).is_none() {
                    info!(
                        "create region memory engine";
                        "region_id" => id,
                    );
                }
                let regional_engine = core.engine.entry(id).or_default();
                (regional_engine.data.clone())
            };
            batch
                .into_iter()
                .zip(regional_engine.into_iter())
                .for_each(|(kvs, engine)| {
                    kvs.into_iter().for_each(|(k, v)| match v {
                        ValueType::Put(v) => {
                            engine.put(k, v);
                        }
                        ValueType::Delete => {
                            let _ = engine.remove(k).is_some();
                        }
                    });
                });
        }
    }
}

// impl Snapshot for LruMemoryEngine {}

unsafe impl Send for LruMemoryEngine {}
unsafe impl Sync for LruMemoryEngine {}

impl LruMemoryEngine {
    pub fn new_snapshot(&self) -> MemoryEngineSnapshot {
        let _snapshot = {
            let mut core = self.core.lock().unwrap();
            let snapshot = core.max_version.load(Ordering::Relaxed);
            core.snapshot_list.push(snapshot);
            snapshot
        };

        MemoryEngineSnapshot {
            engine: self.clone(),
            _snapshot,
        }
    }
}

// todo(SpadeA): clone and snapshot
#[derive(Clone)]
pub struct MemoryEngineSnapshot {
    engine: LruMemoryEngine,
    _snapshot: u64,
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
        let prefix_same_as_start = opts.prefix_same_as_start();
        let (lower_bound, upper_bound) = opts.build_bounds();
        Some(MemoryEngineIterator {
            _cf: String::from(cf),
            valid: false,
            prefix_same_as_start,
            prefix: None,
            lower_bound,
            upper_bound,
            iter: regional_engine.iter(),
        })
    }
}

pub struct MemoryEngineIterator {
    _cf: String,
    valid: bool,
    prefix_same_as_start: bool,
    prefix: Option<Vec<u8>>,
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
        if let Some(ref upper) = self.upper_bound && self.valid {
            self.valid = self.key() < upper.as_slice();
        }
        if self.valid && self.prefix_same_as_start {
            let prefix = self.prefix.as_ref().unwrap();
            let cur_key = self.key();
            if &cur_key[..cur_key.len() - 8] != prefix.as_slice() {
                self.valid = false;
            }
        }
        Ok(self.valid)
    }

    pub fn prev(&mut self) -> engine_traits::Result<bool> {
        self.iter.prev();
        self.valid = self.iter.valid();
        if let Some(ref lower) = self.lower_bound && self.valid {
            self.valid = self.key() >= lower.as_slice();
        }
        if self.valid && self.prefix_same_as_start {
            let prefix = self.prefix.as_ref().unwrap();
            let cur_key = self.key();
            if &cur_key[..cur_key.len() - 8] != prefix.as_slice() {
                self.valid = false;
            }
        }
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
        if let Some(ref upper)  = self.upper_bound && self.valid {
            self.valid = self.key() < upper.as_slice();
        }

        if self.valid && self.prefix_same_as_start {
            self.prefix = Some(key[..key.len() - 8].to_vec());
            let cur_key = self.key();
            if &cur_key[..cur_key.len() - 8] != self.prefix.as_ref().unwrap().as_slice() {
                self.valid = false;
            }
        }

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
        if let Some(ref lower) = self.lower_bound && self.valid {
            self.valid = self.key() >= lower.as_slice();
        }

        if self.valid && self.prefix_same_as_start {
            self.prefix = Some(key[..key.len() - 8].to_vec());
            let cur_key = self.key();
            if &cur_key[..cur_key.len() - 8] != self.prefix.as_ref().unwrap().as_slice() {
                self.valid = false;
            }
        }

        Ok(self.valid)
    }

    pub fn seek_to_first(&mut self) -> engine_traits::Result<bool> {
        if let Some(lower_bound) = self.lower_bound.clone() {
            return self.seek(lower_bound.as_slice());
        } else {
            self.iter.seek_to_first()
        }
        self.valid = self.iter.valid();
        if let Some(ref upper) = self.upper_bound && self.valid {
            self.valid = self.key() < upper.as_slice();
        }

        if self.valid && self.prefix_same_as_start {
            let cur_key = self.key();
            self.prefix = Some(cur_key[..cur_key.len() - 8].to_vec());
        }

        Ok(self.valid)
    }

    pub fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        if let Some(upper_bound) = self.upper_bound.clone() {
            return self.seek(upper_bound.as_slice());
        } else {
            self.iter.seek_to_last();
        }
        self.valid = self.iter.valid();
        if let Some(ref upper) = self.upper_bound && self.valid {
            self.valid = self.key() < upper.as_slice();
        }

        if self.valid && self.prefix_same_as_start {
            let cur_key = self.key();
            self.prefix = Some(cur_key[..cur_key.len() - 8].to_vec());
        }

        Ok(self.valid)
    }

    pub fn valid(&self) -> engine_traits::Result<bool> {
        Ok(self.valid)
    }
}

mod test {

    #[test]
    fn test_x() {
        let lru = LruMemoryEngine::new();
        lru.new_region(1);
        let mut a = MemoryBatch::default();
        a.insert(
            1,
            [
                vec![
                    (
                        key_with_ts(b"zkkkkk1", 1),
                        ValueType::Put(Bytes::from(b"val1".to_vec())),
                    ),
                    (
                        key_with_ts(b"zkkkkk1", 2),
                        ValueType::Put(Bytes::from(b"val2".to_vec())),
                    ),
                    (
                        key_with_ts(b"zkkkkk2", 3),
                        ValueType::Put(Bytes::from(b"val3".to_vec())),
                    ),
                    (
                        key_with_ts(b"zkkkkk3", 4),
                        ValueType::Put(Bytes::from(b"val4".to_vec())),
                    ),
                    (key_with_ts(b"zkkkkk2", 3), ValueType::Delete),
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
