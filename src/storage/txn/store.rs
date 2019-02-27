// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{Error, Result};
use kvproto::kvrpcpb::IsolationLevel;
use storage::mvcc::{
    BackwardScanner, BackwardScannerBuilder, ForwardScanner, ForwardScannerBuilder,
};
use storage::mvcc::{Error as MvccError, MvccReader};
use storage::{Key, KvPair, ScanMode, Snapshot, Statistics, Value};

pub struct SnapshotStore<S: Snapshot> {
    snapshot: S,
    start_ts: u64,
    isolation_level: IsolationLevel,
    fill_cache: bool,
}

impl<S: Snapshot> SnapshotStore<S> {
    pub fn new(
        snapshot: S,
        start_ts: u64,
        isolation_level: IsolationLevel,
        fill_cache: bool,
    ) -> Self {
        SnapshotStore {
            snapshot,
            start_ts,
            isolation_level,
            fill_cache,
        }
    }

    pub fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>> {
        let mut reader = MvccReader::new(
            self.snapshot.clone(),
            None,
            self.fill_cache,
            None,
            None,
            self.isolation_level,
        );
        let v = reader.get(key, self.start_ts)?;
        statistics.add(reader.get_statistics());
        Ok(v)
    }

    pub fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Statistics,
    ) -> Result<Vec<Result<Option<Value>>>> {
        // TODO: sort the keys and use ScanMode::Forward
        let mut reader = MvccReader::new(
            self.snapshot.clone(),
            None,
            self.fill_cache,
            None,
            None,
            self.isolation_level,
        );
        let mut results = Vec::with_capacity(keys.len());
        for k in keys {
            results.push(reader.get(k, self.start_ts).map_err(Error::from));
        }
        statistics.add(reader.get_statistics());
        Ok(results)
    }

    /// Create a scanner.
    /// when key_only is true, all the returned value will be empty.
    pub fn scanner(
        &self,
        mode: ScanMode,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<StoreScanner<S>> {
        // Check request bounds with physical bound
        self.verify_range(&lower_bound, &upper_bound)?;

        let (forward_scanner, backward_scanner) = match mode {
            ScanMode::Forward => {
                let forward_scanner =
                    ForwardScannerBuilder::new(self.snapshot.clone(), self.start_ts)
                        .range(lower_bound, upper_bound)
                        .omit_value(key_only)
                        .fill_cache(self.fill_cache)
                        .isolation_level(self.isolation_level)
                        .build()?;
                (Some(forward_scanner), None)
            }
            ScanMode::Backward => {
                let backward_scanner =
                    BackwardScannerBuilder::new(self.snapshot.clone(), self.start_ts)
                        .range(lower_bound, upper_bound)
                        .omit_value(key_only)
                        .fill_cache(self.fill_cache)
                        .isolation_level(self.isolation_level)
                        .build()?;
                (None, Some(backward_scanner))
            }
            _ => unreachable!(),
        };
        Ok(StoreScanner {
            forward_scanner,
            backward_scanner,
        })
    }

    fn verify_range(&self, lower_bound: &Option<Key>, upper_bound: &Option<Key>) -> Result<()> {
        if let Some(ref l) = lower_bound {
            if let Some(b) = self.snapshot.lower_bound() {
                if !b.is_empty() && l.as_encoded().as_slice() < b {
                    return Err(Error::InvalidReqRange {
                        start: Some(l.as_encoded().clone()),
                        end: upper_bound.as_ref().map(|ref b| b.as_encoded().clone()),
                        lower_bound: Some(b.to_vec()),
                        upper_bound: self.snapshot.upper_bound().map(|b| b.to_vec()),
                    });
                }
            }
        }
        if let Some(ref u) = upper_bound {
            if let Some(b) = self.snapshot.upper_bound() {
                if !b.is_empty() && (u.as_encoded().as_slice() > b || u.as_encoded().is_empty()) {
                    return Err(Error::InvalidReqRange {
                        start: lower_bound.as_ref().map(|ref b| b.as_encoded().clone()),
                        end: Some(u.as_encoded().clone()),
                        lower_bound: self.snapshot.lower_bound().map(|b| b.to_vec()),
                        upper_bound: Some(b.to_vec()),
                    });
                }
            }
        }

        Ok(())
    }
}

pub struct StoreScanner<S: Snapshot> {
    forward_scanner: Option<ForwardScanner<S>>,
    backward_scanner: Option<BackwardScanner<S>>,
}

impl<S: Snapshot> StoreScanner<S> {
    #[inline]
    pub fn next(&mut self) -> Result<Option<(Key, Value)>> {
        if let Some(scanner) = self.forward_scanner.as_mut() {
            Ok(scanner.read_next()?)
        } else {
            Ok(self.backward_scanner.as_mut().unwrap().read_next()?)
        }
    }

    pub fn scan(&mut self, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = Vec::with_capacity(limit);
        while results.len() < limit {
            match self.next() {
                Ok(Some((k, v))) => {
                    results.push(Ok((k.to_raw()?, v)));
                }
                Ok(None) => break,
                Err(e @ Error::Mvcc(MvccError::KeyIsLocked { .. })) => {
                    results.push(Err(e));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    pub fn take_statistics(&mut self) -> Statistics {
        if self.forward_scanner.is_some() {
            return self.forward_scanner.as_mut().unwrap().take_statistics();
        }
        if self.backward_scanner.is_some() {
            return self.backward_scanner.as_mut().unwrap().take_statistics();
        }
        unreachable!();
    }
}

#[cfg(test)]
mod test {
    use super::SnapshotStore;
    use kvproto::kvrpcpb::{Context, IsolationLevel};
    use raftstore::store::engine::IterOption;
    use storage::engine::{
        self, Engine, Result as EngineResult, RocksEngine, RocksSnapshot, ScanMode, TEMP_DIR,
    };
    use storage::mvcc::MvccTxn;
    use storage::{
        CfName, Cursor, Iterator, Key, KvPair, Mutation, Options, Snapshot, Statistics, Value,
        ALL_CFS,
    };

    const KEY_PREFIX: &str = "key_prefix";
    const START_TS: u64 = 10;
    const COMMIT_TS: u64 = 20;
    const START_ID: u64 = 1000;

    struct TestStore {
        keys: Vec<String>,
        snapshot: RocksSnapshot,
        ctx: Context,
        engine: RocksEngine,
    }

    impl TestStore {
        fn new(key_num: u64) -> TestStore {
            let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
            let keys: Vec<String> = (START_ID..START_ID + key_num)
                .map(|i| format!("{}{}", KEY_PREFIX, i))
                .collect();
            let ctx = Context::new();
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
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true).unwrap();
                for key in &self.keys {
                    let key = key.as_bytes();
                    txn.prewrite(
                        Mutation::Put((Key::from_raw(key), key.to_vec())),
                        pk,
                        &Options::default(),
                    ).unwrap();
                }
                self.engine.write(&self.ctx, txn.into_modifies()).unwrap();
            }
            self.refresh_snapshot();
            // do commit
            {
                let mut txn = MvccTxn::new(self.snapshot.clone(), START_TS, true).unwrap();
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
                COMMIT_TS + 1,
                IsolationLevel::SI,
                true,
            )
        }
    }

    // Snapshot with bound
    #[derive(Clone, Debug)]
    struct MockRangeSnapshot {
        start: Vec<u8>,
        end: Vec<u8>,
    }

    #[derive(Default)]
    struct MockRangeSnapshotIter {}

    impl Iterator for MockRangeSnapshotIter {
        fn next(&mut self) -> bool {
            true
        }
        fn prev(&mut self) -> bool {
            true
        }
        fn seek(&mut self, _: &Key) -> EngineResult<bool> {
            Ok(true)
        }
        fn seek_for_prev(&mut self, _: &Key) -> EngineResult<bool> {
            Ok(true)
        }
        fn seek_to_first(&mut self) -> bool {
            true
        }
        fn seek_to_last(&mut self) -> bool {
            true
        }
        fn valid(&self) -> bool {
            true
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
        fn iter(&self, _: IterOption, _: ScanMode) -> EngineResult<Cursor<Self::Iter>> {
            Ok(Cursor::new(
                MockRangeSnapshotIter::default(),
                ScanMode::Forward,
            ))
        }
        fn iter_cf(
            &self,
            _: CfName,
            _: IterOption,
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
        let data = snapshot_store.batch_get(&keys_list, &mut statistics);
        assert!(data.is_ok(), "expect ok,while got {:?}", data.unwrap_err());
        for item in data.unwrap() {
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
            .scanner(ScanMode::Forward, false, Some(start_key), None)
            .unwrap();

        let half = (key_num / 2) as usize;
        let expect = &store.keys[0..half];
        let result = scanner.scan(half).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();
        let expect: Vec<Option<KvPair>> = expect
            .into_iter()
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
            .scanner(ScanMode::Backward, false, None, Some(start_key))
            .unwrap();

        let result = scanner.scan(half).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();

        let mut expect: Vec<Option<KvPair>> = expect
            .into_iter()
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
                ScanMode::Forward,
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
            .scanner(
                ScanMode::Backward,
                false,
                Some(lower_bound),
                Some(upper_bound),
            )
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
        let store = SnapshotStore::new(snap, 0, IsolationLevel::SI, true);
        let bound_a = Key::from_encoded(b"a".to_vec());
        let bound_b = Key::from_encoded(b"b".to_vec());
        let bound_c = Key::from_encoded(b"c".to_vec());
        let bound_d = Key::from_encoded(b"d".to_vec());
        assert!(store.scanner(ScanMode::Forward, false, None, None).is_ok());
        assert!(
            store
                .scanner(
                    ScanMode::Forward,
                    false,
                    Some(bound_b.clone()),
                    Some(bound_c.clone())
                )
                .is_ok()
        );
        assert!(
            store
                .scanner(
                    ScanMode::Forward,
                    false,
                    Some(bound_a.clone()),
                    Some(bound_c.clone())
                )
                .is_err()
        );
        assert!(
            store
                .scanner(
                    ScanMode::Forward,
                    false,
                    Some(bound_b.clone()),
                    Some(bound_d.clone())
                )
                .is_err()
        );
        assert!(
            store
                .scanner(
                    ScanMode::Forward,
                    false,
                    Some(bound_a.clone()),
                    Some(bound_d.clone())
                )
                .is_err()
        );

        // Store with whole range
        let snap2 = MockRangeSnapshot::new(b"".to_vec(), b"".to_vec());
        let store2 = SnapshotStore::new(snap2, 0, IsolationLevel::SI, true);
        assert!(store2.scanner(ScanMode::Forward, false, None, None).is_ok());
        assert!(
            store2
                .scanner(ScanMode::Forward, false, Some(bound_a.clone()), None)
                .is_ok()
        );
        assert!(
            store2
                .scanner(
                    ScanMode::Forward,
                    false,
                    Some(bound_a.clone()),
                    Some(bound_b.clone())
                )
                .is_ok()
        );
        assert!(
            store2
                .scanner(ScanMode::Forward, false, None, Some(bound_c.clone()))
                .is_ok()
        );
    }
}
