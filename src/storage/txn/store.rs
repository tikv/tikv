// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::IsolationLevel;

use super::{Error, ErrorInner, Result};
use crate::storage::kv::{Snapshot, Statistics};
use crate::storage::metrics::*;
use crate::storage::mvcc::{
    EntryScanner, Error as MvccError, ErrorInner as MvccErrorInner, NewerTsCheckState, PointGetter,
    PointGetterBuilder, Scanner as MvccScanner, ScannerBuilder,
};
use txn_types::{Key, KvPair, TimeStamp, TsSet, Value, WriteRef};

pub trait Store: Send {
    /// The scanner type returned by `scanner()`.
    type Scanner: Scanner;

    /// Fetch the provided key.
    fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>>;

    /// Re-use last cursor to incrementally (if possible) fetch the provided key.
    fn incremental_get(&mut self, key: &Key) -> Result<Option<Value>>;

    /// Take the statistics. Currently only available for `incremental_get`.
    fn incremental_get_take_statistics(&mut self) -> Statistics;

    /// Whether there was data > ts during previous incremental gets.
    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState;

    /// Fetch the provided set of keys.
    fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Statistics,
    ) -> Result<Vec<Result<Option<Value>>>>;

    /// Retrieve a scanner over the bounds.
    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        check_has_newer_ts_data: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner>;
}

/// [`Scanner`]s allow retrieving items or batches from a scan result.
///
/// Commonly they are obtained as a result of a [`scanner`](Store::scanner) operation.
pub trait Scanner: Send {
    /// Get the next [`KvPair`](KvPair) if it exists.
    fn next(&mut self) -> Result<Option<(Key, Value)>>;

    /// Get the next [`KvPair`](KvPair)s up to `limit` if they exist.
    fn scan(&mut self, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = Vec::with_capacity(limit);
        while results.len() < limit {
            match self.next() {
                Ok(Some((k, v))) => {
                    results.push(Ok((k.to_raw()?, v)));
                }
                Ok(None) => break,
                Err(
                    e
                    @
                    Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked {
                        ..
                    }))),
                ) => {
                    results.push(Err(e));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    /// Whether there was data > ts during previous scans.
    fn met_newer_ts_data(&self) -> NewerTsCheckState;

    /// Take statistics.
    fn take_statistics(&mut self) -> Statistics;
}

pub trait TxnEntryStore: Send {
    /// The scanner type returned by `scanner()`.
    type Scanner: TxnEntryScanner;

    /// Retrieve a scanner over the bounds.
    fn entry_scanner(
        &self,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        after_ts: TimeStamp,
        output_delete: bool,
    ) -> Result<Self::Scanner>;
}

/// [`TxnEntryScanner`] allows retrieving items or batches from a scan result.
///
/// Commonly they are obtained as a result of a
/// [`entry_scanner`](TxnEntryStore::entry_scanner) operation.
pub trait TxnEntryScanner: Send {
    fn next_entry(&mut self) -> Result<Option<TxnEntry>>;

    fn scan_entries(&mut self, batch: &mut EntryBatch) -> Result<()> {
        while batch.entries.len() < batch.entries.capacity() {
            match self.next_entry() {
                Ok(Some(entry)) => {
                    batch.entries.push(entry);
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Take statistics.
    fn take_statistics(&mut self) -> Statistics;
}

/// A transaction entry in underlying storage.
#[derive(PartialEq, Debug)]
pub enum TxnEntry {
    Prewrite { default: KvPair, lock: KvPair },
    Commit { default: KvPair, write: KvPair },
    // TOOD: Add more entry if needed.
}

impl TxnEntry {
    /// This method will return a kv pair whose
    /// content and encode are same as a kv pair
    /// reture by ```StoreScanner::next```
    pub fn into_kvpair(self) -> Result<(Vec<u8>, Vec<u8>)> {
        match self {
            TxnEntry::Commit { default, write } => {
                if !default.0.is_empty() {
                    let k = Key::from_encoded(default.0).truncate_ts()?;
                    let k = k.into_raw()?;
                    Ok((k, default.1))
                } else {
                    let k = Key::from_encoded(write.0).truncate_ts()?;
                    let k = k.into_raw()?;
                    let v = WriteRef::parse(&write.1)
                        .map_err(MvccError::from)?
                        .to_owned();
                    let v = v.short_value.unwrap_or_else(Vec::default);
                    Ok((k, v))
                }
            }
            // Prewrite are not support
            _ => unreachable!(),
        }
    }
}

/// A batch of transaction entries.
pub struct EntryBatch {
    entries: Vec<TxnEntry>,
}

impl EntryBatch {
    pub fn with_capacity(cap: usize) -> EntryBatch {
        EntryBatch {
            entries: Vec::with_capacity(cap),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &TxnEntry> {
        self.entries.iter()
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, TxnEntry> {
        self.entries.drain(..)
    }
}

pub struct SnapshotStore<S: Snapshot> {
    snapshot: S,
    start_ts: TimeStamp,
    isolation_level: IsolationLevel,
    fill_cache: bool,
    bypass_locks: TsSet,
    check_has_newer_ts_data: bool,

    point_getter_cache: Option<PointGetter<S>>,
}

impl<S: Snapshot> Store for SnapshotStore<S> {
    type Scanner = MvccScanner<S>;

    fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let mut point_getter = PointGetterBuilder::new(self.snapshot.clone(), self.start_ts)
            .fill_cache(self.fill_cache)
            .isolation_level(self.isolation_level)
            .multi(false)
            .bypass_locks(self.bypass_locks.clone())
            .build()?;
        let v = point_getter.get(key)?;
        statistics.add(&point_getter.take_statistics());
        Ok(v)
    }

    fn incremental_get(&mut self, key: &Key) -> Result<Option<Value>> {
        if self.point_getter_cache.is_none() {
            self.point_getter_cache = Some(
                PointGetterBuilder::new(self.snapshot.clone(), self.start_ts)
                    .fill_cache(self.fill_cache)
                    .isolation_level(self.isolation_level)
                    .multi(true)
                    .bypass_locks(self.bypass_locks.clone())
                    .check_has_newer_ts_data(self.check_has_newer_ts_data)
                    .build()?,
            );
        }
        Ok(self.point_getter_cache.as_mut().unwrap().get(key)?)
    }

    #[inline]
    fn incremental_get_take_statistics(&mut self) -> Statistics {
        if self.point_getter_cache.is_none() {
            Statistics::default()
        } else {
            self.point_getter_cache.as_mut().unwrap().take_statistics()
        }
    }

    #[inline]
    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState {
        if self.point_getter_cache.is_none() {
            NewerTsCheckState::Unknown
        } else {
            self.point_getter_cache
                .as_ref()
                .unwrap()
                .met_newer_ts_data()
        }
    }

    fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Statistics,
    ) -> Result<Vec<Result<Option<Value>>>> {
        use std::mem::{self, MaybeUninit};
        type Element = Result<Option<Value>>;

        if keys.len() == 1 {
            return Ok(vec![self.get(&keys[0], statistics)]);
        }

        let mut order_and_keys: Vec<_> = keys.iter().enumerate().collect();
        order_and_keys.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

        let mut point_getter = PointGetterBuilder::new(self.snapshot.clone(), self.start_ts)
            .fill_cache(self.fill_cache)
            .isolation_level(self.isolation_level)
            .multi(true)
            .bypass_locks(self.bypass_locks.clone())
            .build()?;

        let mut values: Vec<MaybeUninit<Element>> = Vec::with_capacity(keys.len());
        for _ in 0..keys.len() {
            values.push(MaybeUninit::uninit());
        }
        for (original_order, key) in order_and_keys {
            let value = point_getter.get(key).map_err(Error::from);
            unsafe {
                values[original_order].as_mut_ptr().write(value);
            }
        }

        statistics.add(&point_getter.take_statistics());

        let values = unsafe { mem::transmute::<Vec<MaybeUninit<Element>>, Vec<Element>>(values) };
        Ok(values)
    }

    #[inline]
    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        check_has_newer_ts_data: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<MvccScanner<S>> {
        // Check request bounds with physical bound
        self.verify_range(&lower_bound, &upper_bound)?;
        let scanner = ScannerBuilder::new(self.snapshot.clone(), self.start_ts, desc)
            .range(lower_bound, upper_bound)
            .omit_value(key_only)
            .fill_cache(self.fill_cache)
            .isolation_level(self.isolation_level)
            .bypass_locks(self.bypass_locks.clone())
            .check_has_newer_ts_data(check_has_newer_ts_data)
            .build()?;

        Ok(scanner)
    }
}

impl<S: Snapshot> TxnEntryStore for SnapshotStore<S> {
    type Scanner = EntryScanner<S>;
    fn entry_scanner(
        &self,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
        after_ts: TimeStamp,
        output_delete: bool,
    ) -> Result<EntryScanner<S>> {
        // Check request bounds with physical bound
        self.verify_range(&lower_bound, &upper_bound)?;
        let scanner =
            ScannerBuilder::new(self.snapshot.clone(), self.start_ts, false /* desc */)
                .range(lower_bound, upper_bound)
                .omit_value(false)
                .fill_cache(self.fill_cache)
                .isolation_level(self.isolation_level)
                .bypass_locks(self.bypass_locks.clone())
                .hint_min_ts(Some(after_ts.next()))
                .hint_max_ts(Some(self.start_ts))
                .build_entry_scanner(after_ts, output_delete)?;

        Ok(scanner)
    }
}

impl<S: Snapshot> SnapshotStore<S> {
    pub fn new(
        snapshot: S,
        start_ts: TimeStamp,
        isolation_level: IsolationLevel,
        fill_cache: bool,
        bypass_locks: TsSet,
        check_has_newer_ts_data: bool,
    ) -> Self {
        SnapshotStore {
            snapshot,
            start_ts,
            isolation_level,
            fill_cache,
            bypass_locks,
            check_has_newer_ts_data,

            point_getter_cache: None,
        }
    }

    #[inline]
    pub fn set_start_ts(&mut self, start_ts: TimeStamp) {
        self.start_ts = start_ts;
    }

    #[inline]
    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.isolation_level = isolation_level;
    }

    #[inline]
    pub fn set_bypass_locks(&mut self, locks: TsSet) {
        self.bypass_locks = locks;
    }

    fn verify_range(&self, lower_bound: &Option<Key>, upper_bound: &Option<Key>) -> Result<()> {
        if let Some(ref l) = lower_bound {
            if let Some(b) = self.snapshot.lower_bound() {
                if !b.is_empty() && l.as_encoded().as_slice() < b {
                    REQUEST_EXCEED_BOUND.inc();
                    return Err(Error::from(ErrorInner::InvalidReqRange {
                        start: Some(l.as_encoded().clone()),
                        end: upper_bound.as_ref().map(|ref b| b.as_encoded().clone()),
                        lower_bound: Some(b.to_vec()),
                        upper_bound: self.snapshot.upper_bound().map(|b| b.to_vec()),
                    }));
                }
            }
        }
        if let Some(ref u) = upper_bound {
            if let Some(b) = self.snapshot.upper_bound() {
                if !b.is_empty() && (u.as_encoded().as_slice() > b || u.as_encoded().is_empty()) {
                    REQUEST_EXCEED_BOUND.inc();
                    return Err(Error::from(ErrorInner::InvalidReqRange {
                        start: lower_bound.as_ref().map(|ref b| b.as_encoded().clone()),
                        end: Some(u.as_encoded().clone()),
                        lower_bound: self.snapshot.lower_bound().map(|b| b.to_vec()),
                        upper_bound: Some(b.to_vec()),
                    }));
                }
            }
        }

        Ok(())
    }
}

/// A Store that reads on fixtures.
pub struct FixtureStore {
    data: std::collections::BTreeMap<Key, Result<Vec<u8>>>,
}

impl Clone for FixtureStore {
    fn clone(&self) -> Self {
        let data = self
            .data
            .iter()
            .map(|(k, v)| {
                let owned_k = k.clone();
                let owned_v = match v {
                    Ok(v) => Ok(v.clone()),
                    Err(e) => Err(e.maybe_clone().unwrap()),
                };
                (owned_k, owned_v)
            })
            .collect();
        Self { data }
    }
}

impl FixtureStore {
    pub fn new(data: std::collections::BTreeMap<Key, Result<Vec<u8>>>) -> Self {
        FixtureStore { data }
    }
}

impl Store for FixtureStore {
    type Scanner = FixtureStoreScanner;

    #[inline]
    fn get(&self, key: &Key, _statistics: &mut Statistics) -> Result<Option<Vec<u8>>> {
        let r = self.data.get(key);
        match r {
            None => Ok(None),
            Some(Ok(v)) => Ok(Some(v.clone())),
            Some(Err(e)) => Err(e.maybe_clone().unwrap()),
        }
    }

    #[inline]
    fn incremental_get(&mut self, key: &Key) -> Result<Option<Vec<u8>>> {
        let mut s = Statistics::default();
        self.get(key, &mut s)
    }

    #[inline]
    fn incremental_get_take_statistics(&mut self) -> Statistics {
        Statistics::default()
    }

    #[inline]
    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    #[inline]
    fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Statistics,
    ) -> Result<Vec<Result<Option<Vec<u8>>>>> {
        Ok(keys.iter().map(|key| self.get(key, statistics)).collect())
    }

    #[inline]
    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        _: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<FixtureStoreScanner> {
        use std::ops::Bound;

        let lower = lower_bound.as_ref().map_or(Bound::Unbounded, |v| {
            if !desc {
                Bound::Included(v)
            } else {
                Bound::Excluded(v)
            }
        });
        let upper = upper_bound.as_ref().map_or(Bound::Unbounded, |v| {
            if desc {
                Bound::Included(v)
            } else {
                Bound::Excluded(v)
            }
        });

        let mut vec: Vec<_> = self
            .data
            .range((lower, upper))
            .map(|(k, v)| {
                let owned_k = k.clone();
                let owned_v = if key_only {
                    match v {
                        Ok(_v) => Ok(vec![]),
                        Err(e) => Err(e.maybe_clone().unwrap()),
                    }
                } else {
                    match v {
                        Ok(v) => Ok(v.clone()),
                        Err(e) => Err(e.maybe_clone().unwrap()),
                    }
                };
                (owned_k, owned_v)
            })
            .collect();

        if desc {
            vec.reverse();
        }

        Ok(FixtureStoreScanner {
            // TODO: Remove clone when GATs is available. See rust-lang/rfcs#1598.
            data: vec.into_iter(),
        })
    }
}

/// A Scanner that scans on fixtures.
pub struct FixtureStoreScanner {
    data: std::vec::IntoIter<(Key, Result<Vec<u8>>)>,
}

impl Scanner for FixtureStoreScanner {
    #[inline]
    fn next(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
        let value = self.data.next();
        match value {
            None => Ok(None),
            Some((k, Ok(v))) => Ok(Some((k, v))),
            Some((_k, Err(e))) => Err(e),
        }
    }

    #[inline]
    fn met_newer_ts_data(&self) -> NewerTsCheckState {
        NewerTsCheckState::Unknown
    }

    #[inline]
    fn take_statistics(&mut self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{
        Cursor, Engine, Iterator, Result as EngineResult, RocksEngine, RocksSnapshot, ScanMode,
        TestEngineBuilder,
    };
    use crate::storage::mvcc::{Mutation, MvccTxn};
    use engine_traits::CfName;
    use engine_traits::IterOptions;
    use kvproto::kvrpcpb::Context;

    const KEY_PREFIX: &str = "key_prefix";
    const START_TS: TimeStamp = TimeStamp::new(10);
    const COMMIT_TS: TimeStamp = TimeStamp::new(20);
    const START_ID: u64 = 1000;

    struct TestStore {
        keys: Vec<String>,
        snapshot: RocksSnapshot,
        ctx: Context,
        engine: RocksEngine,
    }

    impl TestStore {
        fn new(key_num: u64) -> TestStore {
            let engine = TestEngineBuilder::new().build().unwrap();
            let keys: Vec<String> = (START_ID..START_ID + key_num)
                .map(|i| format!("{}{}", KEY_PREFIX, i))
                .collect();
            let ctx = Context::default();
            let snapshot = engine.snapshot(&ctx).unwrap();
            let mut store = TestStore {
                keys,
                snapshot,
                ctx,
                engine,
            };
            store.init_data();
            store
        }

        #[inline]
        fn init_data(&mut self) {
            let primary_key = format!("{}{}", KEY_PREFIX, START_ID);
            let pk = primary_key.as_bytes();
            // do prewrite.
            {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true);
                for key in &self.keys {
                    let key = key.as_bytes();
                    txn.prewrite(
                        Mutation::Put((Key::from_raw(key), key.to_vec())),
                        pk,
                        false,
                        0,
                        0,
                        TimeStamp::default(),
                    )
                    .unwrap();
                }
                self.engine.write(&self.ctx, txn.into_modifies()).unwrap();
            }
            self.refresh_snapshot();
            // do commit
            {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true);
                for key in &self.keys {
                    let key = key.as_bytes();
                    txn.commit(Key::from_raw(key), COMMIT_TS).unwrap();
                }
                self.engine.write(&self.ctx, txn.into_modifies()).unwrap();
            }
            self.refresh_snapshot();
        }

        #[inline]
        fn refresh_snapshot(&mut self) {
            self.snapshot = self.engine.snapshot(&self.ctx).unwrap()
        }

        fn store(&self) -> SnapshotStore<RocksSnapshot> {
            SnapshotStore::new(
                self.snapshot.clone(),
                COMMIT_TS.next(),
                IsolationLevel::Si,
                true,
                Default::default(),
                false,
            )
        }
    }

    // Snapshot with bound
    #[derive(Clone)]
    struct MockRangeSnapshot {
        start: Vec<u8>,
        end: Vec<u8>,
    }

    #[derive(Default)]
    struct MockRangeSnapshotIter {}

    impl Iterator for MockRangeSnapshotIter {
        fn next(&mut self) -> EngineResult<bool> {
            Ok(true)
        }
        fn prev(&mut self) -> EngineResult<bool> {
            Ok(true)
        }
        fn seek(&mut self, _: &Key) -> EngineResult<bool> {
            Ok(true)
        }
        fn seek_for_prev(&mut self, _: &Key) -> EngineResult<bool> {
            Ok(true)
        }
        fn seek_to_first(&mut self) -> EngineResult<bool> {
            Ok(true)
        }
        fn seek_to_last(&mut self) -> EngineResult<bool> {
            Ok(true)
        }
        fn valid(&self) -> EngineResult<bool> {
            Ok(true)
        }
        fn validate_key(&self, _: &Key) -> EngineResult<()> {
            Ok(())
        }
        fn key(&self) -> &[u8] {
            b""
        }
        fn value(&self) -> &[u8] {
            b""
        }
    }

    impl MockRangeSnapshot {
        fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
            Self { start, end }
        }
    }

    impl Snapshot for MockRangeSnapshot {
        type Iter = MockRangeSnapshotIter;

        fn get(&self, _: &Key) -> EngineResult<Option<Value>> {
            Ok(None)
        }
        fn get_cf(&self, _: CfName, _: &Key) -> EngineResult<Option<Value>> {
            Ok(None)
        }
        fn iter(&self, _: IterOptions, _: ScanMode) -> EngineResult<Cursor<Self::Iter>> {
            Ok(Cursor::new(
                MockRangeSnapshotIter::default(),
                ScanMode::Forward,
            ))
        }
        fn iter_cf(
            &self,
            _: CfName,
            _: IterOptions,
            _: ScanMode,
        ) -> EngineResult<Cursor<Self::Iter>> {
            Ok(Cursor::new(
                MockRangeSnapshotIter::default(),
                ScanMode::Forward,
            ))
        }
        fn lower_bound(&self) -> Option<&[u8]> {
            Some(self.start.as_slice())
        }
        fn upper_bound(&self) -> Option<&[u8]> {
            Some(self.end.as_slice())
        }
    }

    #[test]
    fn test_snapshot_store_get() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();
        let mut statistics = Statistics::default();
        for key in &store.keys {
            let key = key.as_bytes();
            let data = snapshot_store
                .get(&Key::from_raw(key), &mut statistics)
                .unwrap();
            assert!(data.is_some(), "{:?} expect some, but got none", key);
        }
    }

    #[test]
    fn test_snapshot_store_batch_get() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();
        let mut statistics = Statistics::default();
        let mut keys_list = Vec::new();
        for key in &store.keys {
            keys_list.push(Key::from_raw(key.as_bytes()));
        }
        let data = snapshot_store
            .batch_get(&keys_list, &mut statistics)
            .unwrap();
        for item in data {
            let item = item.unwrap();
            assert!(item.is_some(), "item expect some while get none");
        }
    }

    #[test]
    fn test_snapshot_store_scan() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();
        let key = format!("{}{}", KEY_PREFIX, START_ID);
        let start_key = Key::from_raw(key.as_bytes());
        let mut scanner = snapshot_store
            .scanner(false, false, false, Some(start_key), None)
            .unwrap();

        let half = (key_num / 2) as usize;
        let expect = &store.keys[0..half];
        let result = scanner.scan(half).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> = expect
            .iter()
            .map(|k| Some((k.clone().into_bytes(), k.clone().into_bytes())))
            .collect();
        assert_eq!(result, expect, "expect {:?}, but got {:?}", expect, result);
    }

    #[test]
    fn test_snapshot_store_reverse_scan() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();

        let half = (key_num / 2) as usize;
        let key = format!("{}{}", KEY_PREFIX, START_ID + (half as u64) - 1);
        let start_key = Key::from_raw(key.as_bytes());
        let expect = &store.keys[0..half - 1];
        let mut scanner = snapshot_store
            .scanner(true, false, false, None, Some(start_key))
            .unwrap();

        let result = scanner.scan(half).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();

        let mut expect: Vec<Option<KvPair>> = expect
            .iter()
            .map(|k| Some((k.clone().into_bytes(), k.clone().into_bytes())))
            .collect();
        expect.reverse();

        assert_eq!(result, expect, "expect {:?}, but got {:?}", expect, result);
    }

    #[test]
    fn test_scan_with_bound() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();

        let lower_bound = Key::from_raw(format!("{}{}", KEY_PREFIX, START_ID + 10).as_bytes());
        let upper_bound = Key::from_raw(format!("{}{}", KEY_PREFIX, START_ID + 20).as_bytes());

        let expected: Vec<_> = (10..20)
            .map(|i| Key::from_raw(format!("{}{}", KEY_PREFIX, START_ID + i).as_bytes()))
            .collect();

        let mut scanner = snapshot_store
            .scanner(
                false,
                false,
                false,
                Some(lower_bound.clone()),
                Some(upper_bound.clone()),
            )
            .unwrap();

        // Collect all scanned keys
        let mut result = Vec::new();
        while let Some((k, _)) = scanner.next().unwrap() {
            result.push(k);
        }
        assert_eq!(result, expected);

        let mut scanner = snapshot_store
            .scanner(true, false, false, Some(lower_bound), Some(upper_bound))
            .unwrap();

        // Collect all scanned keys
        let mut result = Vec::new();
        while let Some((k, _)) = scanner.next().unwrap() {
            result.push(k);
        }
        assert_eq!(result, expected.into_iter().rev().collect::<Vec<_>>());
    }

    #[test]
    fn test_scanner_verify_bound() {
        // Store with a limited range
        let snap = MockRangeSnapshot::new(b"b".to_vec(), b"c".to_vec());
        let store = SnapshotStore::new(
            snap,
            TimeStamp::zero(),
            IsolationLevel::Si,
            true,
            Default::default(),
            false,
        );
        let bound_a = Key::from_encoded(b"a".to_vec());
        let bound_b = Key::from_encoded(b"b".to_vec());
        let bound_c = Key::from_encoded(b"c".to_vec());
        let bound_d = Key::from_encoded(b"d".to_vec());
        assert!(store.scanner(false, false, false, None, None).is_ok());
        assert!(store
            .scanner(
                false,
                false,
                false,
                Some(bound_b.clone()),
                Some(bound_c.clone())
            )
            .is_ok());
        assert!(store
            .scanner(
                false,
                false,
                false,
                Some(bound_a.clone()),
                Some(bound_c.clone())
            )
            .is_err());
        assert!(store
            .scanner(
                false,
                false,
                false,
                Some(bound_b.clone()),
                Some(bound_d.clone())
            )
            .is_err());
        assert!(store
            .scanner(false, false, false, Some(bound_a.clone()), Some(bound_d))
            .is_err());

        // Store with whole range
        let snap2 = MockRangeSnapshot::new(b"".to_vec(), b"".to_vec());
        let store2 = SnapshotStore::new(
            snap2,
            TimeStamp::zero(),
            IsolationLevel::Si,
            true,
            Default::default(),
            false,
        );
        assert!(store2.scanner(false, false, false, None, None).is_ok());
        assert!(store2
            .scanner(false, false, false, Some(bound_a.clone()), None)
            .is_ok());
        assert!(store2
            .scanner(false, false, false, Some(bound_a), Some(bound_b))
            .is_ok());
        assert!(store2
            .scanner(false, false, false, None, Some(bound_c))
            .is_ok());
    }

    fn gen_fixture_store() -> FixtureStore {
        use std::collections::BTreeMap;

        let mut data = BTreeMap::default();
        data.insert(Key::from_raw(b"abc"), Ok(b"foo".to_vec()));
        data.insert(Key::from_raw(b"ab"), Ok(b"bar".to_vec()));
        data.insert(Key::from_raw(b"abcd"), Ok(b"box".to_vec()));
        data.insert(Key::from_raw(b"b"), Ok(b"alpha".to_vec()));
        data.insert(Key::from_raw(b"bb"), Ok(b"alphaalpha".to_vec()));
        data.insert(
            Key::from_raw(b"bba"),
            Err(Error::from(ErrorInner::Mvcc(MvccError::from(
                MvccErrorInner::KeyIsLocked(kvproto::kvrpcpb::LockInfo::default()),
            )))),
        );
        data.insert(Key::from_raw(b"z"), Ok(b"beta".to_vec()));
        data.insert(Key::from_raw(b"ca"), Ok(b"hello".to_vec()));
        data.insert(
            Key::from_raw(b"zz"),
            Err(Error::from(ErrorInner::Mvcc(MvccError::from(
                txn_types::Error::from(txn_types::ErrorInner::BadFormatLock),
            )))),
        );

        FixtureStore::new(data)
    }

    #[test]
    fn test_fixture_get() {
        let store = gen_fixture_store();
        let mut statistics = Statistics::default();
        assert_eq!(
            store
                .get(&Key::from_raw(b"not exist"), &mut statistics)
                .unwrap(),
            None
        );
        assert_eq!(
            store.get(&Key::from_raw(b"c"), &mut statistics).unwrap(),
            None
        );
        assert_eq!(
            store.get(&Key::from_raw(b"ab"), &mut statistics).unwrap(),
            Some(b"bar".to_vec())
        );
        assert_eq!(
            store.get(&Key::from_raw(b"caa"), &mut statistics).unwrap(),
            None
        );
        assert_eq!(
            store.get(&Key::from_raw(b"ca"), &mut statistics).unwrap(),
            Some(b"hello".to_vec())
        );
        assert!(store.get(&Key::from_raw(b"bba"), &mut statistics).is_err());
        assert_eq!(
            store.get(&Key::from_raw(b"bbaa"), &mut statistics).unwrap(),
            None
        );
        assert_eq!(
            store.get(&Key::from_raw(b"abcd"), &mut statistics).unwrap(),
            Some(b"box".to_vec())
        );
        assert_eq!(
            store
                .get(&Key::from_raw(b"abcd\x00"), &mut statistics)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .get(&Key::from_raw(b"\x00abcd"), &mut statistics)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .get(&Key::from_raw(b"ab\x00cd"), &mut statistics)
                .unwrap(),
            None
        );
        assert_eq!(
            store.get(&Key::from_raw(b"ab"), &mut statistics).unwrap(),
            Some(b"bar".to_vec())
        );
        assert!(store.get(&Key::from_raw(b"zz"), &mut statistics).is_err());
        assert_eq!(
            store.get(&Key::from_raw(b"z"), &mut statistics).unwrap(),
            Some(b"beta".to_vec())
        );
    }

    #[test]
    fn test_fixture_scanner() {
        let store = gen_fixture_store();

        let mut scanner = store.scanner(false, false, false, None, None).unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"ab"), b"bar".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abc"), b"foo".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abcd"), b"box".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"alpha".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"bb"), b"alphaalpha".to_vec()))
        );
        assert!(scanner.next().is_err());
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"ca"), b"hello".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"z"), b"beta".to_vec()))
        );
        assert!(scanner.next().is_err());
        // note: mvcc impl does not guarantee to work any more after meeting a non lock error
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store.scanner(true, false, false, None, None).unwrap();
        assert!(scanner.next().is_err());
        // note: mvcc impl does not guarantee to work any more after meeting a non lock error
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"z"), b"beta".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"ca"), b"hello".to_vec()))
        );
        assert!(scanner.next().is_err());
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"bb"), b"alphaalpha".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"b"), b"alpha".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abcd"), b"box".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abc"), b"foo".to_vec()))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"ab"), b"bar".to_vec()))
        );
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store.scanner(false, true, false, None, None).unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"ab"), vec![]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abc"), vec![]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abcd"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), Some((Key::from_raw(b"b"), vec![])));
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"bb"), vec![]))
        );
        assert!(scanner.next().is_err());
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"ca"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), Some((Key::from_raw(b"z"), vec![])));
        assert!(scanner.next().is_err());
        // note: mvcc impl does not guarantee to work any more after meeting a non lock error
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                false,
                true,
                false,
                Some(Key::from_raw(b"abc")),
                Some(Key::from_raw(b"abcd")),
            )
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abc"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                false,
                true,
                false,
                Some(Key::from_raw(b"abc")),
                Some(Key::from_raw(b"bba")),
            )
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abc"), vec![]))
        );
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abcd"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), Some((Key::from_raw(b"b"), vec![])));
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"bb"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                false,
                true,
                false,
                Some(Key::from_raw(b"b")),
                Some(Key::from_raw(b"c")),
            )
            .unwrap();
        assert_eq!(scanner.next().unwrap(), Some((Key::from_raw(b"b"), vec![])));
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"bb"), vec![]))
        );
        assert!(scanner.next().is_err());
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                false,
                true,
                false,
                Some(Key::from_raw(b"b")),
                Some(Key::from_raw(b"b")),
            )
            .unwrap();
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                true,
                true,
                false,
                Some(Key::from_raw(b"abc")),
                Some(Key::from_raw(b"abcd")),
            )
            .unwrap();
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abcd"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                true,
                true,
                false,
                Some(Key::from_raw(b"abc")),
                Some(Key::from_raw(b"bba")),
            )
            .unwrap();
        assert!(scanner.next().is_err());
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"bb"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), Some((Key::from_raw(b"b"), vec![])));
        assert_eq!(
            scanner.next().unwrap(),
            Some((Key::from_raw(b"abcd"), vec![]))
        );
        assert_eq!(scanner.next().unwrap(), None);
    }
}

#[cfg(test)]
mod benches {
    use super::*;
    use crate::test;
    use rand::RngCore;
    use std::collections::BTreeMap;

    fn gen_payload(n: usize) -> Vec<u8> {
        let mut data = vec![0; n];
        rand::thread_rng().fill_bytes(&mut data);
        data
    }

    #[bench]
    fn bench_fixture_get(b: &mut test::Bencher) {
        let user_key = gen_payload(64);
        let mut data = BTreeMap::default();
        for i in 0..100 {
            let mut key = user_key.clone();
            key.push(i);
            data.insert(Key::from_raw(&key), Ok(gen_payload(100)));
        }
        let store = FixtureStore::new(data);
        let mut query_user_key = user_key;
        query_user_key.push(10);
        let query_key = Key::from_raw(&query_user_key);
        b.iter(|| {
            let store = test::black_box(&store);
            let mut statistics = Statistics::default();
            let value = store
                .get(test::black_box(&query_key), &mut statistics)
                .unwrap();
            test::black_box(value);
        })
    }

    #[bench]
    fn bench_fixture_batch_get(b: &mut test::Bencher) {
        let mut batch_get_keys = vec![];
        let mut data = BTreeMap::default();
        for _ in 0..100 {
            let user_key = gen_payload(64);
            let key = Key::from_raw(&user_key);
            batch_get_keys.push(key.clone());
            data.insert(key, Ok(gen_payload(100)));
        }
        let store = FixtureStore::new(data);
        b.iter(|| {
            let store = test::black_box(&store);
            let mut statistics = Statistics::default();
            let value = store.batch_get(test::black_box(&batch_get_keys), &mut statistics);
            test::black_box(value.unwrap());
        })
    }

    #[bench]
    fn bench_fixture_scanner(b: &mut test::Bencher) {
        let mut data = BTreeMap::default();
        for _ in 0..2000 {
            let user_key = gen_payload(64);
            data.insert(Key::from_raw(&user_key), Ok(gen_payload(100)));
        }
        let store = FixtureStore::new(data);
        b.iter(|| {
            let store = test::black_box(&store);
            let scanner = store
                .scanner(
                    test::black_box(true),
                    test::black_box(false),
                    test::black_box(false),
                    test::black_box(None),
                    test::black_box(None),
                )
                .unwrap();
            test::black_box(scanner);
        })
    }

    #[bench]
    fn bench_fixture_scanner_next(b: &mut test::Bencher) {
        let mut data = BTreeMap::default();
        for _ in 0..2000 {
            let user_key = gen_payload(64);
            data.insert(Key::from_raw(&user_key), Ok(gen_payload(100)));
        }
        let store = FixtureStore::new(data);
        b.iter(|| {
            let store = test::black_box(&store);
            let mut scanner = store
                .scanner(
                    test::black_box(true),
                    test::black_box(false),
                    test::black_box(false),
                    test::black_box(None),
                    test::black_box(None),
                )
                .unwrap();
            for _ in 0..1000 {
                let v = scanner.next().unwrap();
                test::black_box(v);
            }
        })
    }

    #[bench]
    fn bench_fixture_scanner_scan(b: &mut test::Bencher) {
        let mut data = BTreeMap::default();
        for _ in 0..2000 {
            let user_key = gen_payload(64);
            data.insert(Key::from_raw(&user_key), Ok(gen_payload(100)));
        }
        let store = FixtureStore::new(data);
        b.iter(|| {
            let store = test::black_box(&store);
            let mut scanner = store
                .scanner(
                    test::black_box(true),
                    test::black_box(false),
                    test::black_box(false),
                    test::black_box(None),
                    test::black_box(None),
                )
                .unwrap();
            test::black_box(scanner.scan(1000).unwrap());
        })
    }
}
