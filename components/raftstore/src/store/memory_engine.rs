use std::{
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use collections::HashMap;
use crossbeam_skiplist::{
    map::{Entry, Range as SkipListRange},
    SkipMap,
};
use engine_panic::PanicDbVector;
use engine_traits::{CfNamesExt, Peekable, SnapshotMiscExt, CF_DEFAULT, CF_LOCK, CF_WRITE};

pub type MemoryBatch = HashMap<u64, [Vec<(Vec<u8>, Vec<u8>)>; 3]>;

fn cf_to_id(cf: &str) -> u8 {
    match cf {
        CF_DEFAULT => 0,
        CF_LOCK => 1,
        CF_WRITE => 2,
        _ => panic!("unrecognized cf {}", cf),
    }
}

#[derive(Clone, Debug)]
pub struct RegionMemoryEngine {
    data: [Arc<SkipMap<Vec<u8>, Vec<u8>>>; 3],
}

#[derive(Clone, Debug)]
pub struct LRUMemoryEngine {
    core: Arc<Mutex<LRUMemoryEngineCore>>,
}

#[derive(Debug)]
struct LRUMemoryEngineCore {
    engine: HashMap<u64, RegionMemoryEngine>,
    // todo: replace it
    snapshot_list: Vec<u64>,
    max_version: Arc<AtomicU64>,
}

impl LRUMemoryEngine {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn consume_batch(&self, batch: MemoryBatch) {
        for (id, batch) in batch.into_iter() {
            let (max_version, regional_engine) = {
                let mut core = self.core.lock().unwrap();
                let regional_engine = core.engine.get(&id).unwrap();
                (core.max_version.clone(), regional_engine.data.clone())
            };
            batch
                .into_iter()
                .zip(regional_engine.into_iter())
                .for_each(|(kvs, engine)| {
                    kvs.into_iter().for_each(|(k, v)| {
                        engine.insert(k, v);
                        // todo: update max_version
                    });
                });
        }
    }
}

// impl Snapshot for LRUMemoryEngine {}

unsafe impl Send for LRUMemoryEngine {}
unsafe impl Sync for LRUMemoryEngine {}

impl Peekable for LRUMemoryEngine {
    // todo: modify it
    type DbVector = PanicDbVector;

    fn get_msg<M: protobuf::Message + Default>(
        &self,
        key: &[u8],
    ) -> engine_traits::Result<Option<M>> {
        unimplemented!();
    }

    fn get_msg_cf<M: protobuf::Message + Default>(
        &self,
        cf: &str,
        key: &[u8],
    ) -> engine_traits::Result<Option<M>> {
        unimplemented!();
    }

    fn get_value(&self, key: &[u8]) -> engine_traits::Result<Option<Self::DbVector>> {
        unimplemented!();
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> engine_traits::Result<Option<Self::DbVector>> {
        unimplemented!();
    }

    fn get_value_cf_opt(
        &self,
        opts: &engine_traits::ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> engine_traits::Result<Option<Self::DbVector>> {
        unimplemented!();
    }

    fn get_value_opt(
        &self,
        opts: &engine_traits::ReadOptions,
        key: &[u8],
    ) -> engine_traits::Result<Option<Self::DbVector>> {
        unimplemented!();
    }
}

impl SnapshotMiscExt for LRUMemoryEngine {
    fn sequence_number(&self) -> u64 {
        unimplemented!();
    }
}

impl CfNamesExt for LRUMemoryEngine {
    fn cf_names(&self) -> Vec<&str> {
        unimplemented!()
    }
}

impl LRUMemoryEngine {
    fn new_snapshot(&self) -> MemoryEngineSnapshot {
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

pub struct MemoryEngineSnapshot {
    engine: LRUMemoryEngine,
    snapshot: u64,
}

impl Drop for MemoryEngineSnapshot {
    fn drop(&mut self) {
        let core = self.engine.core.lock().unwrap();
        // core.snapshot_list.remove(self.snapshot);
        // todo: more cleanup works including gc
    }
}

impl MemoryEngineSnapshot {
    fn iterator_opt(&self, cf: &str, opts: engine_traits::IterOptions) -> MemoryEngineIterator {
        let regional_engine = self
            .engine
            .core
            .lock()
            .unwrap()
            .engine
            .get(&opts.region_id().unwrap())
            .unwrap()
            .clone();
        let (lower_bound, upper_bound) = match opts.build_bounds() {
            (Some(lower), Some(upper)) => (lower, upper),
            (Some(lower), None) => (lower, keys::DATA_MAX_KEY.to_vec()),
            (None, Some(upper)) => (keys::DATA_MIN_KEY.to_vec(), upper),
            (None, None) => (keys::DATA_MIN_KEY.to_vec(), keys::DATA_MAX_KEY.to_vec()),
        };
        MemoryEngineIterator {
            cf: String::from(cf),
            valid: false,
            current: None,
            lower_bound,
            upper_bound,
            engine: regional_engine.data[cf_to_id(cf) as usize].clone(),
            entry: None,
        }
    }
}

pub struct MemoryEngineIterator<'a, 'b> {
    cf: String,
    valid: bool,
    engine: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    current: Option<SkipListRange<'a, Vec<u8>, Range<Vec<u8>>, Vec<u8>, Vec<u8>>>,

    entry: Option<Entry<'b, Vec<u8>, Vec<u8>>>,
}

use engine_traits::Iterator;
impl<'a, 'b> Iterator for MemoryEngineIterator<'a, 'b>
where
    'a: 'b,
{
    fn key(&self) -> &[u8] {
        assert!(self.valid);
        self.entry.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid);
        self.entry.as_ref().unwrap().value()
    }

    fn next(&mut self) -> engine_traits::Result<bool> {
        self.entry = self.current.as_mut().unwrap().next();
        self.valid = self.entry.is_some();
        Ok(self.valid)
    }

    fn prev(&mut self) -> engine_traits::Result<bool> {
        unimplemented!();
    }

    fn seek(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        let start = if key < self.lower_bound.as_slice() {
            &self.lower_bound
        } else {
            key
        };

        self.current = Some(self.engine.range(start.to_vec()..self.upper_bound.clone()));
        self.entry = self.current.as_mut().unwrap().next();
        self.valid = self.entry.is_some();
        Ok(self.valid)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> engine_traits::Result<bool> {
        unimplemented!();
    }

    fn seek_to_first(&'c mut self) -> engine_traits::Result<bool> {
        self.current = Some(
            self.engine
                .range(self.lower_bound.clone()..self.upper_bound.clone()),
        );
        self.entry = self.current.as_mut().unwrap().next();
        self.valid = self.entry.is_some();
        Ok(self.valid)
    }

    fn seek_to_last(&mut self) -> engine_traits::Result<bool> {
        unimplemented!();
    }

    fn valid(&self) -> engine_traits::Result<bool> {
        unimplemented!();
    }
}

#[test]
fn test_x() {}
