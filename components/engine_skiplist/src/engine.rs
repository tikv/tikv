// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_handle::SkiplistCFHandle;
use crate::db_vector::SkiplistDBVector;
use crate::snapshot::SkiplistSnapshot;
use crate::write_batch::SkiplistWriteBatch;

use crossbeam_skiplist::map::{Entry as SkipEntry, Iter as SkipIter, Range as SkipRange, SkipMap};
use engine_traits::{
    CfName, Error, IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, Result,
    SeekKey, SyncMutable, WriteOptions, CF_DEFAULT,
};
use std::ops::{Bound, Range};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use tikv_util::collections::HashMap;

static ENGINE_SEQ_NO_ALLOC: AtomicUsize = AtomicUsize::new(0);

pub struct SkiplistEngineBuilder {
    cf_names: Vec<CfName>,
}

impl SkiplistEngineBuilder {
    pub fn new() -> Self {
        Self { cf_names: vec![] }
    }

    pub fn cf_names(mut self, names: &[CfName]) -> Self {
        self.cf_names = names.to_vec();
        self
    }

    pub fn build(self) -> SkiplistEngine {
        let mut engines = HashMap::default();
        let mut cf_handles = HashMap::default();
        if self.cf_names.is_empty() {
            let default_engine = Arc::new(SkipMap::new());
            let default_handle = SkiplistCFHandle {
                seq_no: ENGINE_SEQ_NO_ALLOC.fetch_add(1, Ordering::Relaxed),
                cf_name: CF_DEFAULT,
            };
            engines.insert(default_handle.clone(), default_engine);
            cf_handles.insert(CF_DEFAULT, default_handle);
        } else {
            for cf_name in self.cf_names {
                let engine = Arc::new(SkipMap::new());
                let cf_handle = SkiplistCFHandle {
                    cf_name,
                    seq_no: ENGINE_SEQ_NO_ALLOC.fetch_add(1, Ordering::Relaxed),
                };
                engines.insert(cf_handle.clone(), engine);
                cf_handles.insert(cf_name, cf_handle);
            }
        }
        SkiplistEngine {
            engines,
            cf_handles,
            total_bytes: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SkiplistEngine {
    pub total_bytes: Arc<AtomicUsize>,
    pub(crate) engines: HashMap<SkiplistCFHandle, Arc<SkipMap<Vec<u8>, Vec<u8>>>>,
    pub(crate) cf_handles: HashMap<CfName, SkiplistCFHandle>,
}

impl SkiplistEngine {
    fn get_cf_engine(&self, cf: &str) -> Result<&Arc<SkipMap<Vec<u8>, Vec<u8>>>> {
        let handle = self
            .cf_handles
            .get(cf)
            .ok_or_else(|| Error::CFName(cf.to_owned()))?;
        Ok(self.engines.get(handle).unwrap())
    }
}

impl KvEngine for SkiplistEngine {
    type Snapshot = SkiplistSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        SkiplistSnapshot::new(self.clone())
    }
    fn sync(&self) -> Result<()> {
        Ok(())
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
    }
}

impl Peekable for SkiplistEngine {
    type DBVector = SkiplistDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        let engine = self.get_cf_engine(cf)?;
        Ok(engine
            .get(key)
            .map(|e| SkiplistDBVector(e.value().to_vec())))
    }
}

impl SyncMutable for SkiplistEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.total_bytes.fetch_add(key.len(), Ordering::Relaxed);
        self.total_bytes.fetch_add(value.len(), Ordering::Relaxed);
        let engine = self.get_cf_engine(cf)?;
        engine.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let engine = self.get_cf_engine(cf)?;
        if let Some(e) = engine.remove(key) {
            self.total_bytes.fetch_sub(e.key().len(), Ordering::Relaxed);
            self.total_bytes
                .fetch_sub(e.value().len(), Ordering::Relaxed);
        }
        Ok(())
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let range = Range {
            start: begin_key.to_vec(),
            end: end_key.to_vec(),
        };
        let engine = self.get_cf_engine(cf)?;
        engine.range(range).for_each(|e| {
            e.remove();
            self.total_bytes.fetch_sub(e.key().len(), Ordering::Relaxed);
            self.total_bytes
                .fetch_sub(e.value().len(), Ordering::Relaxed);
        });
        Ok(())
    }
}

impl Iterable for SkiplistEngine {
    type Iterator = SkiplistEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        self.iterator_cf_opt(CF_DEFAULT, opts)
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let engine = self.get_cf_engine(cf)?.clone();
        let lower_bound = Range {
            start: opts.lower_bound().map(|e| e.to_vec()).unwrap_or_default(),
            end: opts.upper_bound().map(|e| e.to_vec()).unwrap_or_else(|| {
                engine
                    .back()
                    .map(|e| e.value().to_owned())
                    .unwrap_or_default()
            }),
        };
        let lower_bound = opts
            .lower_bound()
            .map(|e| Bound::Included(e))
            .unwrap_or_else(|| Bound::Unbounded);
        let upper_bound = opts
            .upper_bound()
            .map(|e| Bound::Included(e))
            .unwrap_or_else(|| Bound::Unbounded);
        Ok(SkiplistEngineIterator::new(
            engine,
            lower_bound,
            upper_bound,
        ))
    }
}

pub struct SkiplistEngineIterator {
    engine: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    lower_bound: SkipEntry<'static, Vec<u8>, Vec<u8>>,
    upper_bound: SkipEntry<'static, Vec<u8>, Vec<u8>>,
    cursor: SkipEntry<'static, Vec<u8>, Vec<u8>>,
    valid: bool,
}

impl SkiplistEngineIterator {
    fn new(
        engine: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
        lower_bound: Bound<&[u8]>,
        upper_bound: Bound<&[u8]>,
    ) -> Self {
        Self {
            lower_bound: unsafe {
                (*Arc::downgrade(&engine).as_ptr())
                    .lower_bound(lower_bound)
                    .unwrap()
            },
            upper_bound: unsafe {
                (*Arc::downgrade(&engine).as_ptr())
                    .upper_bound(upper_bound)
                    .unwrap()
            },
            cursor: unsafe {
                (*Arc::downgrade(&engine).as_ptr())
                    .lower_bound(lower_bound)
                    .unwrap()
            },
            engine,
            valid: true,
        }
    }
}

impl Iterator for SkiplistEngineIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => self.cursor = self.lower_bound.clone(),
            SeekKey::End => self.cursor = self.upper_bound.clone(),
            SeekKey::Key(key) => {
                if key < self.cursor.key().as_slice() {
                    while key < self.cursor.key().as_slice() {
                        if !self.cursor.move_next() {
                            break;
                        }
                    }
                } else if key > self.cursor.key().as_slice() {
                    while let Some(e) = self.cursor.prev() {
                        if e.key().as_slice() >= key {
                            self.cursor.move_prev();
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Ok(true)
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => self.cursor = self.lower_bound.clone(),
            SeekKey::End => self.cursor = self.upper_bound.clone(),
            SeekKey::Key(key) => {
                if key < self.cursor.key().as_slice() {
                    while let Some(e) = self.cursor.next() {
                        if e.key().as_slice() <= key {
                            self.cursor.move_next();
                        } else {
                            break;
                        }
                    }
                } else if key > self.cursor.key().as_slice() {
                    while key > self.cursor.key().as_slice() {
                        if !self.cursor.move_prev() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(true)
    }

    fn prev(&mut self) -> Result<bool> {
        self.valid = self.cursor.move_prev();
        Ok(self.valid)
    }
    fn next(&mut self) -> Result<bool> {
        self.valid = self.cursor.move_next();
        Ok(self.valid)
    }

    fn key(&self) -> &[u8] {
        self.cursor.key()
    }
    fn value(&self) -> &[u8] {
        self.cursor.value()
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.valid)
    }
}
