// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod fixture_store;
mod snapshot_store;

pub use self::fixture_store::{FixtureStore, FixtureStoreScanner};
pub use self::snapshot_store::SnapshotStore;

use crate::storage::mvcc::Error as MvccError;
use crate::storage::{Key, KvPair, Statistics, Value};

use super::{Error, Result};

pub trait Store: Send {
    type Scanner: Scanner;

    fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>>;

    fn batch_get(&self, keys: &[Key], statistics: &mut Statistics) -> Vec<Result<Option<Value>>>;

    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner>;
}

pub trait Scanner: Send {
    fn next(&mut self) -> Result<Option<(Key, Value)>>;

    fn scan(&mut self, limit: usize) -> Result<Vec<Result<KvPair>>> {
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

    fn take_statistics(&mut self) -> Statistics;
}

#[cfg(test)]
mod tests {
    use super::Error;
    use super::{FixtureStore, Scanner, SnapshotStore, Store};

    use crate::storage::kv::{
        Engine, Result as EngineResult, RocksEngine, RocksSnapshot, ScanMode,
    };
    use crate::storage::mvcc::Error as MvccError;
    use crate::storage::mvcc::MvccTxn;
    use crate::storage::{
        CfName, Cursor, Iterator, Key, KvPair, Mutation, Options, Snapshot, Statistics,
        TestEngineBuilder, Value,
    };
    use engine::IterOption;
    use kvproto::kvrpcpb::{Context, IsolationLevel};

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
            let engine = TestEngineBuilder::new().build().unwrap();
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
                    )
                    .unwrap();
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
    #[derive(Clone)]
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
        fn status(&self) -> EngineResult<()> {
            Ok(())
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
            .scanner(false, false, Some(start_key), None)
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
            .scanner(true, false, None, Some(start_key))
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
            .scanner(true, false, Some(lower_bound), Some(upper_bound))
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
        assert!(store.scanner(false, false, None, None).is_ok());
        assert!(store
            .scanner(false, false, Some(bound_b.clone()), Some(bound_c.clone()))
            .is_ok());
        assert!(store
            .scanner(false, false, Some(bound_a.clone()), Some(bound_c.clone()))
            .is_err());
        assert!(store
            .scanner(false, false, Some(bound_b.clone()), Some(bound_d.clone()))
            .is_err());
        assert!(store
            .scanner(false, false, Some(bound_a.clone()), Some(bound_d.clone()))
            .is_err());

        // Store with whole range
        let snap2 = MockRangeSnapshot::new(b"".to_vec(), b"".to_vec());
        let store2 = SnapshotStore::new(snap2, 0, IsolationLevel::SI, true);
        assert!(store2.scanner(false, false, None, None).is_ok());
        assert!(store2
            .scanner(false, false, Some(bound_a.clone()), None)
            .is_ok());
        assert!(store2
            .scanner(false, false, Some(bound_a.clone()), Some(bound_b.clone()))
            .is_ok());
        assert!(store2
            .scanner(false, false, None, Some(bound_c.clone()))
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
            Err(Error::Mvcc(MvccError::KeyIsLocked {
                // We won't check error detail in tests, so we can just fill fields casually
                key: vec![],
                primary: vec![],
                ts: 1,
                ttl: 2,
                txn_size: 0,
            })),
        );
        data.insert(Key::from_raw(b"z"), Ok(b"beta".to_vec()));
        data.insert(Key::from_raw(b"ca"), Ok(b"hello".to_vec()));
        data.insert(
            Key::from_raw(b"zz"),
            Err(Error::Mvcc(MvccError::BadFormatLock)),
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

        let mut scanner = store.scanner(false, false, None, None).unwrap();
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

        let mut scanner = store.scanner(true, false, None, None).unwrap();
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

        let mut scanner = store.scanner(false, true, None, None).unwrap();
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
                Some(Key::from_raw(b"b")),
                Some(Key::from_raw(b"b")),
            )
            .unwrap();
        assert_eq!(scanner.next().unwrap(), None);

        let mut scanner = store
            .scanner(
                true,
                true,
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
    use crate::test;

    use rand::RngCore;
    use std::collections::BTreeMap;

    use super::{FixtureStore, Scanner, Store};
    use crate::storage::{Key, Statistics};

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
        let mut query_user_key = user_key.clone();
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
            test::black_box(value);
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
                    test::black_box(None),
                    test::black_box(None),
                )
                .unwrap();
            test::black_box(scanner.scan(1000).unwrap());
        })
    }
}
