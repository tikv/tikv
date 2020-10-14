// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Bound, Range};
use std::sync::Arc;

use crossbeam_skiplist::map::{Entry as SkipEntry, Range as SkipRange, SkipMap};
use engine_traits::{
    CFHandleExt, CfName, Error, IterOptions, Iterable, Iterator, KvEngine, MvccPropertiesExt,
    Peekable, ReadOptions, Result, SeekKey, SyncMutable, WriteOptions, CF_DEFAULT,
};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tikv_util::collections::HashMap;

use crate::cf_handle::SkiplistCFHandle;
use crate::db_vector::SkiplistDBVector;
use crate::metrics::*;
use crate::snapshot::SkiplistSnapshot;
use crate::write_batch::SkiplistWriteBatch;

static ENGINE_SEQ_NO_ALLOC: AtomicUsize = AtomicUsize::new(0);

pub struct SkiplistEngineBuilder {
    name: &'static str,
    cf_names: Vec<CfName>,
}

impl SkiplistEngineBuilder {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            cf_names: vec![],
        }
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
            name: self.name,
            engines,
            cf_handles,
            total_bytes: Arc::new(AtomicUsize::new(0)),
            current_version: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub type Key = (Vec<u8>, u64);
pub type Value = Vec<u8>;

#[derive(Clone, Debug)]
pub struct SkiplistEngine {
    pub name: &'static str,
    pub total_bytes: Arc<AtomicUsize>,
    pub(crate) engines: HashMap<SkiplistCFHandle, Arc<SkipMap<Key, Value>>>,
    pub(crate) cf_handles: HashMap<CfName, SkiplistCFHandle>,
    pub(crate) current_version: Arc<AtomicU64>,
}

impl SkiplistEngine {
    pub fn get_cf_engine(&self, cf: &str) -> Result<&Arc<SkipMap<Key, Value>>> {
        let handle = self
            .cf_handles
            .get(cf)
            .ok_or_else(|| Error::CFName(cf.to_owned()))?;
        self.engines
            .get(handle)
            .ok_or_else(|| Error::Engine("cannot get engine by handle".to_string()))
    }

    pub fn put_version(&self, cf: &str, key: &[u8], value: &[u8], version: u64) -> Result<()> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "put"])
            .start_coarse_timer();
        self.total_bytes.fetch_add(key.len(), Ordering::Relaxed);
        self.total_bytes.fetch_add(value.len(), Ordering::Relaxed);
        let engine = self.get_cf_engine(cf)?;
        engine.insert((key.to_vec(), version), value.to_vec());
        Ok(())
    }

    pub fn get_version(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
        version: Option<u64>,
    ) -> Result<Option<(Value, u64)>> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "get"])
            .start_coarse_timer();
        let engine = self.get_cf_engine(cf)?;
        let version = version.unwrap_or(std::u64::MAX);
        if let Some(e) = engine.upper_bound(Bound::Excluded(&(key.to_vec(), version))) {
            if e.key().0 == key {
                return Ok(Some((e.value().to_vec(), e.key().1)));
            }
        };
        Ok(None)
    }
}

impl KvEngine for SkiplistEngine {
    type Snapshot = SkiplistSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        let version = self.current_version.load(Ordering::Acquire);
        SkiplistSnapshot::new(self.clone(), version)
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
        let value = self.get_version(opts, cf, key, None)?;
        Ok(value.map(|(v, _)| SkiplistDBVector(v)))
    }
}

impl SyncMutable for SkiplistEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_version(
            cf,
            key,
            value,
            self.current_version.fetch_add(1, Ordering::AcqRel),
        );
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "delete"])
            .start_coarse_timer();
        let range = Range {
            start: (key.to_vec(), 0),
            end: (key.to_vec(), std::u64::MAX),
        };
        let engine = self.get_cf_engine(cf)?;
        engine.range(range).for_each(|e| {
            e.remove();
            self.total_bytes
                .fetch_sub(e.key().0.len(), Ordering::Relaxed);
            self.total_bytes
                .fetch_sub(e.value().len(), Ordering::Relaxed);
        });
        Ok(())
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "delete_range"])
            .start_coarse_timer();
        let range = Range {
            start: (begin_key.to_vec(), 0),
            end: (end_key.to_vec(), 0),
        };
        let engine = self.get_cf_engine(cf)?;
        engine.range(range).for_each(|e| {
            e.remove();
            self.total_bytes
                .fetch_sub(e.key().0.len(), Ordering::Relaxed);
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
        let lower_bound = opts.lower_bound().map(|e| e.to_vec());
        let upper_bound = opts.upper_bound().map(|e| e.to_vec());
        let version = self.current_version.load(Ordering::Acquire);
        Ok(SkiplistEngineIterator::new(
            self.name,
            engine,
            lower_bound,
            upper_bound,
            version,
        ))
    }
}

static ITERATOR_ID: AtomicUsize = AtomicUsize::new(0);

pub struct SkiplistEngineIterator {
    name: &'static str,
    engine: Arc<SkipMap<Key, Value>>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
    last_kv: Option<(Key, Value)>,
    version: u64,
}

impl SkiplistEngineIterator {
    pub fn new(
        name: &'static str,
        engine: Arc<SkipMap<Key, Value>>,
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
        version: u64,
    ) -> Self {
        let engine_clone = engine.clone();
        let mut iter = Self {
            name,
            lower_bound,
            upper_bound,
            engine,
            version,
            last_kv: None,
        };
        let start = iter.seek_start();
        iter.last_kv = start;
        iter
    }

    fn seek_start(&self) -> Option<(Key, Value)> {
        let lower_bound = self.lower_bound.as_ref().map(|v| (v.clone(), 0));
        if let Some(mut e) = self.engine.lower_bound(
            lower_bound
                .as_ref()
                .map(|v| Bound::Included(v))
                .unwrap_or(Bound::Unbounded),
        ) {
            while !check_version(e.key(), self.version) {
                if !e.move_next() {
                    return None;
                }
            }
            if check_in_range(
                e.key(),
                self.upper_bound.as_ref(),
                self.lower_bound.as_ref(),
            ) {
                loop {
                    if let Some(n) = e.next() {
                        if n.key().0 == e.key().0 && check_version(n.key(), self.version) {
                            e.move_next();
                            continue;
                        }
                    }
                    break;
                }
                return Some((e.key().clone(), e.value().clone()));
            }
        }
        None
    }

    fn seek_end(&self) -> Option<(Key, Value)> {
        let upper_bound = self.upper_bound.as_ref().map(|v| (v.clone(), 0));
        if let Some(mut e) = self.engine.upper_bound(
            upper_bound
                .as_ref()
                .map(|v| Bound::Excluded(v))
                .unwrap_or(Bound::Unbounded),
        ) {
            while !check_version(e.key(), self.version) {
                if !e.move_prev() {
                    return None;
                }
            }
            if check_in_range(
                e.key(),
                self.upper_bound.as_ref(),
                self.lower_bound.as_ref(),
            ) {
                return Some((e.key().clone(), e.value().clone()));
            }
        }
        None
    }
}

fn check_in_range(key: &Key, upper_bound: Option<&Vec<u8>>, lower_bound: Option<&Vec<u8>>) -> bool {
    let (key, _) = key;
    if let Some(upper) = upper_bound {
        if upper <= key {
            return false;
        }
    }
    if let Some(lower) = lower_bound {
        if lower > key {
            return false;
        }
    }
    true
}

fn check_version(key: &Key, version: u64) -> bool {
    key.1 < version
}

impl Iterator for SkiplistEngineIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "seek"])
            .start_coarse_timer();

        self.last_kv = match key {
            SeekKey::Start => self.seek_start(),
            SeekKey::End => self.seek_end(),
            SeekKey::Key(key) => {
                // Check lower bound, seek to start if key is smaller than lower bound.
                if let Some(l) = self.lower_bound.as_deref() {
                    if key < l {
                        return self.seek(SeekKey::Start);
                    }
                }
                // Check upper bound, set `last_kv` to None and return false if key is bigger
                // than upper bound.
                if let Some(u) = self.upper_bound.as_deref() {
                    if key > u {
                        self.last_kv = None;
                        return Ok(false);
                    }
                }
                // Use `Skiplist::lower_bound` to seek key, `Bound::Included` indicates to return the first key >= `key`.
                if let Some(mut e) = self.engine.lower_bound(Bound::Included(&(key.to_vec(), 0))) {
                    while !check_version(e.key(), self.version) && e.move_next() {}
                    if check_in_range(
                        e.key(),
                        self.upper_bound.as_ref(),
                        self.lower_bound.as_ref(),
                    ) && check_version(e.key(), self.version)
                    {
                        loop {
                            if let Some(n) = e.next() {
                                if n.key().0 == e.key().0 && check_version(n.key(), self.version) {
                                    e.move_next();
                                    continue;
                                }
                            }
                            break;
                        }
                        Some((e.key().clone(), e.value().clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };
        Ok(self.last_kv.is_some())
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "seek_for_prev"])
            .start_coarse_timer();

        match key {
            SeekKey::Start | SeekKey::End => self.seek(key),
            SeekKey::Key(key) => {
                // Check lower bound, set `last_kv` to None and return false if key is smaller
                // then lower bound.
                if let Some(l) = self.lower_bound.as_deref() {
                    if key < l {
                        self.last_kv = None;
                        return Ok(false);
                    }
                }
                // Check upper bound, seek to end if key is bigger than upper bound.
                if let Some(u) = self.upper_bound.as_deref() {
                    if key > u {
                        return self.seek(SeekKey::End);
                    }
                }
                // Use `Skiplist::upper_bound` to seek key, `Bound::Excluded` indicates to return the last key < `key`.
                self.last_kv = if let Some(mut e) =
                    self.engine.upper_bound(Bound::Excluded(&(key.to_vec(), 0)))
                {
                    while !check_version(e.key(), self.version) && e.move_prev() {}
                    if check_in_range(
                        e.key(),
                        self.upper_bound.as_ref(),
                        self.lower_bound.as_ref(),
                    ) {
                        Some((e.key().clone(), e.value().clone()))
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(self.last_kv.is_some())
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "prev"])
            .start_coarse_timer();
        if self.last_kv.is_none() {
            return Ok(false);
        }
        let ((last_key, _), _) = self.last_kv.as_ref().unwrap();
        self.last_kv = if let Some(mut e) = self
            .engine
            .upper_bound(Bound::Excluded(&(last_key.clone(), 0)))
        {
            while !check_version(e.key(), self.version) && e.move_prev() {}
            if check_in_range(
                e.key(),
                self.upper_bound.as_ref(),
                self.lower_bound.as_ref(),
            ) && check_version(e.key(), self.version)
            {
                Some((e.key().clone(), e.value().clone()))
            } else {
                None
            }
        } else {
            None
        };
        Ok(self.last_kv.is_some())
    }
    fn next(&mut self) -> Result<bool> {
        let _timer = SKIPLIST_ACTION_HISTOGRAM_VEC
            .with_label_values(&[self.name, "next"])
            .start_coarse_timer();
        if self.last_kv.is_none() {
            return Ok(false);
        }
        let ((last_key, _), _) = self.last_kv.as_ref().unwrap();
        self.last_kv = if let Some(mut e) = self
            .engine
            .lower_bound(Bound::Excluded(&(last_key.clone(), std::u64::MAX)))
        {
            while !check_version(e.key(), self.version) && e.move_next() {}
            if check_in_range(
                e.key(),
                self.upper_bound.as_ref(),
                self.lower_bound.as_ref(),
            ) {
                loop {
                    if let Some(n) = e.next() {
                        if n.key().0 == e.key().0 && check_version(n.key(), self.version) {
                            e.move_next();
                            continue;
                        }
                    }
                    break;
                }
                Some((e.key().clone(), e.value().clone()))
            } else {
                None
            }
        } else {
            None
        };
        Ok(self.last_kv.is_some())
    }

    fn key(&self) -> &[u8] {
        let ((key, _), _) = self.last_kv.as_ref().unwrap();
        key.as_slice()
    }
    fn value(&self) -> &[u8] {
        let (_, value) = self.last_kv.as_ref().unwrap();
        value.as_slice()
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.last_kv.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::{ALL_CFS, CF_WRITE};
    use tikv_util::keybuilder::KeyBuilder;

    #[test]
    fn test_skiplist_seek() {
        let engine = SkiplistEngineBuilder::new("test").cf_names(ALL_CFS).build();
        let _ = engine.get_cf_engine(CF_WRITE);
        let data = vec![
            // k0
            (b"k0", b"w1"),
            (b"k0", b"w2"),
            (b"k0", b"v0"),
            // k1
            (b"k1", b"w1"),
            (b"k1", b"w2"),
            (b"k1", b"v1"),
            // k3
            (b"k3", b"w1"),
            (b"k3", b"w2"),
            (b"k3", b"w3"),
            (b"k3", b"w4"),
            (b"k3", b"v3"),
            // k5
            (b"k5", b"w1"),
            (b"k5", b"v5"),
            // k7
            (b"k7", b"w1"),
            (b"k7", b"w2"),
            (b"k7", b"v7"),
            // k9
            (b"k9", b"w1"),
            (b"k9", b"v9"),
        ];
        for (k, v) in data {
            engine.put_cf(CF_WRITE, k, v).unwrap();
        }
        let start = KeyBuilder::from_slice(b"k1", 2, 0);
        let end = KeyBuilder::from_slice(b"k9", 2, 0);
        let opts = IterOptions::new(Some(start), Some(end), false);
        let snapshot = engine.snapshot();
        let mut iter = snapshot.iterator_cf_opt(CF_WRITE, opts).unwrap();
        assert_eq!(iter.key(), b"k1");
        assert_eq!(iter.value(), b"v1");

        let data = vec![
            (b"k1", b"w3"),
            (b"k3", b"w5"),
            (b"k4", b"w1"),
            (b"k7", b"w3"),
            (b"k8", b"w1"),
        ];
        for (k, v) in data {
            engine.put_cf(CF_WRITE, k, v).unwrap();
        }
        let v1 = snapshot.get_value_cf(CF_WRITE, b"k1").unwrap();
        assert_eq!(v1.unwrap().as_ref(), b"v1");
        assert!(iter.seek(SeekKey::End).unwrap());
        assert_eq!(iter.key(), b"k7");
        assert_eq!(iter.value(), b"v7");

        assert!(iter.seek(SeekKey::Start).unwrap());
        assert_eq!(iter.key(), b"k1");
        assert_eq!(iter.value(), b"v1");

        assert!(!iter.seek(SeekKey::Key(b"k9")).unwrap());

        assert!(iter.seek(SeekKey::Key(b"k0")).unwrap());
        assert_eq!(iter.key(), b"k1");
        assert!(!iter.prev().unwrap());

        assert!(iter.seek(SeekKey::Key(b"k7")).unwrap());
        assert_eq!(iter.key(), b"k7");
        assert_eq!(iter.value(), b"v7");

        assert!(!iter.next().unwrap(), "{:?}", iter.key());

        assert!(iter.seek(SeekKey::Key(b"k2")).unwrap());
        assert_eq!(iter.key(), b"k3");
        assert_eq!(iter.value(), b"v3");

        assert!(iter.seek(SeekKey::Key(b"k6")).unwrap());
        assert_eq!(iter.key(), b"k7");
        assert_eq!(iter.value(), b"v7");

        let expected = vec![
            (b"k1", b"v1"),
            (b"k3", b"v3"),
            (b"k5", b"v5"),
            (b"k7", b"v7"),
        ];
        let mut i = 0;
        assert!(iter.seek(SeekKey::Start).unwrap());
        assert_eq!(iter.key(), b"k1");
        assert_eq!(iter.value(), b"v1");
        while iter.valid().unwrap() {
            assert_eq!(iter.key(), expected[i].0);
            assert_eq!(iter.value(), expected[i].1);
            i += 1;
            iter.next().unwrap();
        }

        let mut i = expected.len();
        assert!(iter.seek(SeekKey::End).unwrap());
        assert_eq!(iter.key(), b"k7");
        assert_eq!(iter.value(), b"v7");
        while iter.valid().unwrap() {
            assert_eq!(iter.key(), expected[i - 1].0);
            assert_eq!(iter.value(), expected[i - 1].1);
            i -= 1;
            iter.prev().unwrap();
        }
    }

    #[test]
    fn test_skiplist_seek_for_prev() {
        let engine = SkiplistEngineBuilder::new("test").cf_names(ALL_CFS).build();
        let _ = engine.get_cf_engine(CF_WRITE);
        let data = vec![
            // k0
            (b"k0", b"w1"),
            (b"k0", b"w2"),
            (b"k0", b"v0"),
            // k1
            (b"k1", b"w1"),
            (b"k1", b"w2"),
            (b"k1", b"v1"),
            // k3
            (b"k3", b"w1"),
            (b"k3", b"w2"),
            (b"k3", b"w3"),
            (b"k3", b"w4"),
            (b"k3", b"v3"),
            // k5
            (b"k5", b"w1"),
            (b"k5", b"v5"),
            // k7
            (b"k7", b"w1"),
            (b"k7", b"w2"),
            (b"k7", b"v7"),
            // k8
            (b"k8", b"w1"),
            (b"k8", b"v8"),
        ];
        for (k, v) in data {
            engine.put_cf(CF_WRITE, k, v).unwrap();
        }
        let start = KeyBuilder::from_slice(b"k1", 2, 0);
        let end = KeyBuilder::from_slice(b"k8", 2, 0);
        let opts = IterOptions::new(Some(start), Some(end), false);
        let mut iter = engine.iterator_cf_opt(CF_WRITE, opts).unwrap();
        assert_eq!(iter.key(), b"k1");
        assert_eq!(iter.value(), b"v1");

        let data = vec![
            (b"k1", b"w3"),
            (b"k3", b"w5"),
            (b"k4", b"w1"),
            (b"k7", b"w3"),
            (b"k8", b"w1"),
        ];
        for (k, v) in data {
            engine.put_cf(CF_WRITE, k, v).unwrap();
        }

        assert!(iter.seek_for_prev(SeekKey::Key(b"k9")).unwrap());
        assert_eq!(iter.key(), b"k7");
        assert_eq!(iter.value(), b"v7");

        assert!(iter.seek_for_prev(SeekKey::Key(b"k8")).unwrap());
        assert_eq!(iter.key(), b"k7");
        assert_eq!(iter.value(), b"v7");

        assert!(!iter.seek_for_prev(SeekKey::Key(b"k0")).unwrap());

        assert!(iter.seek_for_prev(SeekKey::Key(b"k7")).unwrap());
        assert_eq!(iter.key(), b"k5");
        assert_eq!(iter.value(), b"v5");

        assert!(iter.seek_for_prev(SeekKey::Key(b"k2")).unwrap());
        assert_eq!(iter.key(), b"k1");
        assert_eq!(iter.value(), b"v1");

        assert!(iter.seek_for_prev(SeekKey::Key(b"k6")).unwrap());
        assert_eq!(iter.key(), b"k5");
        assert_eq!(iter.value(), b"v5");
    }
}
