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
        lower_bound: Option<Vec<u8>>,
        upper_bound: Option<Vec<u8>>,
    ) -> Result<StoreScanner<S>> {
        let mut reader = MvccReader::new(
            self.snapshot.clone(),
            Some(mode),
            self.fill_cache,
            lower_bound,
            upper_bound,
            self.isolation_level,
        );
        reader.set_key_only(key_only);
        Ok(StoreScanner {
            reader,
            start_ts: self.start_ts,
        })
    }
}

pub struct StoreScanner<S: Snapshot> {
    reader: MvccReader<S>,
    start_ts: u64,
}

#[inline]
fn handle_mvcc_err(e: MvccError, result: &mut Vec<Result<KvPair>>) -> Result<Key> {
    let key = if let MvccError::KeyIsLocked { key: ref k, .. } = e {
        Some(Key::from_raw(k))
    } else {
        None
    };
    match key {
        Some(k) => {
            result.push(Err(e.into()));
            Ok(k)
        }
        None => Err(e.into()),
    }
}

impl<S: Snapshot> StoreScanner<S> {
    pub fn seek(&mut self, key: Key) -> Result<Option<(Key, Value)>> {
        Ok(self.reader.seek(key, self.start_ts)?)
    }

    pub fn reverse_seek(&mut self, key: Key) -> Result<Option<(Key, Value)>> {
        Ok(self.reader.reverse_seek(key, self.start_ts)?)
    }

    pub fn scan(&mut self, mut key: Key, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        while results.len() < limit {
            match self.seek(key) {
                Ok(Some((k, v))) => {
                    results.push(Ok((k.raw()?, v)));
                    key = k;
                }
                Ok(None) => break,
                Err(Error::Mvcc(e)) => key = handle_mvcc_err(e, &mut results)?,
                Err(e) => return Err(e),
            }
            key = key.append_ts(0);
        }
        Ok(results)
    }

    pub fn reverse_scan(&mut self, mut key: Key, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = vec![];
        while results.len() < limit {
            match self.reverse_seek(key) {
                Ok(Some((k, v))) => {
                    results.push(Ok((k.raw()?, v)));
                    key = k;
                }
                Ok(None) => break,
                Err(Error::Mvcc(e)) => key = handle_mvcc_err(e, &mut results)?,
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    pub fn get_statistics(&self) -> &Statistics {
        self.reader.get_statistics()
    }

    pub fn collect_statistics_into(&mut self, stats: &mut Statistics) {
        self.reader.collect_statistics_into(stats);
    }
}

#[cfg(test)]
mod test {
    use super::SnapshotStore;
    use kvproto::kvrpcpb::{Context, IsolationLevel};
    use storage::engine::{self, Engine, RocksEngine, RocksSnapshot, TEMP_DIR};
    use storage::mvcc::MvccTxn;
    use storage::{Key, KvPair, Mutation, Options, ScanMode, Statistics, Value, ALL_CFS};

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
                let mut txn = MvccTxn::new(
                    self.snapshot.clone(),
                    START_TS,
                    None,
                    IsolationLevel::SI,
                    true,
                );
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
                let mut txn = MvccTxn::new(
                    self.snapshot.clone(),
                    START_TS,
                    None,
                    IsolationLevel::SI,
                    true,
                );
                for key in &self.keys {
                    let key = key.as_bytes();
                    txn.commit(&Key::from_raw(key), COMMIT_TS).unwrap();
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
        let mut scanner = snapshot_store
            .scanner(ScanMode::Forward, false, None, None)
            .unwrap();

        let key = format!("{}{}", KEY_PREFIX, START_ID);
        let start_key = Key::from_raw(key.as_bytes());
        let half = (key_num / 2) as usize;
        let expect = &store.keys[0..half];
        let result = scanner.scan(start_key, half).unwrap();
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
        let mut scanner = snapshot_store
            .scanner(ScanMode::Backward, false, None, None)
            .unwrap();

        let half = (key_num / 2) as usize;
        let key = format!("{}{}", KEY_PREFIX, START_ID + (half as u64) - 1);
        let start_key = Key::from_raw(key.as_bytes());
        let expect = &store.keys[0..half - 1];
        let result = scanner.reverse_scan(start_key, half).unwrap();
        let result: Vec<Option<KvPair>> = result.into_iter().map(Result::ok).collect();

        let mut expect: Vec<Option<KvPair>> = expect
            .into_iter()
            .map(|k| Some((k.clone().into_bytes(), k.clone().into_bytes())))
            .collect();
        expect.reverse();

        assert_eq!(result, expect, "expect {:?}, but got {:?}", expect, result);
    }

    #[test]
    fn test_snapshot_store_seek() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();
        let mut scanner = snapshot_store
            .scanner(ScanMode::Forward, false, None, None)
            .unwrap();

        let key = format!("{}{}aaa", KEY_PREFIX, START_ID);
        let start_key = Key::from_raw(key.as_bytes());
        let result = scanner.seek(start_key).unwrap();
        let expect_key = format!("{}{}", KEY_PREFIX, START_ID + 1);
        let expect_value = expect_key.clone().into_bytes();
        let expect = Some((Key::from_raw(expect_key.as_bytes()), expect_value as Value));
        assert_eq!(result, expect, "expect {:?}, but got {:?}", expect, result);
    }

    #[test]
    fn test_snapshot_store_reverse_seek() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();

        let mut scanner = snapshot_store
            .scanner(ScanMode::Backward, false, None, None)
            .unwrap();

        let key = format!("{}{}aaa", KEY_PREFIX, START_ID);
        let start_key = Key::from_raw(key.as_bytes());
        let result = scanner.reverse_seek(start_key).unwrap();
        let expect_key = format!("{}{}", KEY_PREFIX, START_ID);
        let expect_value = expect_key.clone().into_bytes();
        let expect = Some((Key::from_raw(expect_key.as_bytes()), expect_value as Value));
        assert_eq!(result, expect, "expect {:?}, but got {:?}", expect, result);
    }

    #[test]
    fn test_seek_with_bound() {
        let key_num = 100;
        let store = TestStore::new(key_num);
        let snapshot_store = store.store();

        let lower_bound = format!("{}{}", KEY_PREFIX, START_ID).into_bytes();
        let upper_bound = format!("{}{}", KEY_PREFIX, START_ID + 10).into_bytes();

        let mut scanner = snapshot_store
            .scanner(
                ScanMode::Forward,
                false,
                Some(lower_bound.clone()),
                Some(upper_bound.clone()),
            )
            .unwrap();

        // Seek with upper bound should returns None.
        let result = scanner.seek(Key::from_raw(&upper_bound)).unwrap();
        assert!(result.is_none());

        let mut scanner = snapshot_store
            .scanner(
                ScanMode::Backward,
                false,
                Some(lower_bound.clone()),
                Some(upper_bound.clone()),
            )
            .unwrap();

        // Reverse seek with lower bound should returns None.
        let result = scanner.reverse_seek(Key::from_raw(&lower_bound)).unwrap();
        assert!(result.is_none());
    }
}
