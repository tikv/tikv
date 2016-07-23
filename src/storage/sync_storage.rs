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

use storage::{Storage, Key, Value, KvPair, Mutation, Result};
use storage::config::Config;
use kvproto::kvrpcpb::Context;

/// `SyncStorage` wraps `Storage` with sync API, usually used for testing.
pub struct SyncStorage(Storage);

impl SyncStorage {
    pub fn new(config: &Config) -> SyncStorage {
        let mut storage = Storage::new(&config).unwrap();
        storage.start(&config).unwrap();
        SyncStorage(storage)
    }

    pub fn get(&self, ctx: Context, key: &Key, start_ts: u64) -> Result<Option<Value>> {
        let e = async!(_cb,
                       try!(self.0.async_get(ctx, key.to_owned(), start_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn batch_get(&self,
                     ctx: Context,
                     keys: &[Key],
                     start_ts: u64)
                     -> Result<Vec<Result<KvPair>>> {
        let e = async!(_cb,
                       try!(self.0.async_batch_get(ctx, keys.to_owned(), start_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn scan(&self,
                ctx: Context,
                key: Key,
                limit: usize,
                start_ts: u64)
                -> Result<Vec<Result<KvPair>>> {
        let e = async!(_cb, try!(self.0.async_scan(ctx, key, limit, start_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn prewrite(&self,
                    ctx: Context,
                    mutations: Vec<Mutation>,
                    primary: Vec<u8>,
                    start_ts: u64)
                    -> Result<Vec<Result<()>>> {
        let e = async!(_cb,
                       try!(self.0.async_prewrite(ctx, mutations, primary, start_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn commit(&self,
                  ctx: Context,
                  keys: Vec<Key>,
                  start_ts: u64,
                  commit_ts: u64)
                  -> Result<()> {
        let e = async!(_cb,
                       try!(self.0.async_commit(ctx, keys, start_ts, commit_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn commit_then_get(&self,
                           ctx: Context,
                           key: Key,
                           lock_ts: u64,
                           commit_ts: u64,
                           get_ts: u64)
                           -> Result<Option<Value>> {
        let e = async!(_cb,
                       try!(self.0
                           .async_commit_then_get(ctx, key, lock_ts, commit_ts, get_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn cleanup(&self, ctx: Context, key: Key, start_ts: u64) -> Result<()> {
        let e = async!(_cb, try!(self.0.async_cleanup(ctx, key, start_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn rollback(&self, ctx: Context, keys: Vec<Key>, start_ts: u64) -> Result<()> {
        let e = async!(_cb, try!(self.0.async_rollback(ctx, keys, start_ts, _cb)));
        await!(e).unwrap().into()
    }

    pub fn rollback_then_get(&self, ctx: Context, key: Key, lock_ts: u64) -> Result<Option<Value>> {
        let e = async!(_cb,
                       try!(self.0.async_rollback_then_get(ctx, key, lock_ts, _cb)));
        await!(e).unwrap().into()
    }
}

impl Drop for SyncStorage {
    fn drop(&mut self) {
        self.0.stop().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use std::thread;
    use rand::random;
    use kvproto::kvrpcpb::Context;
    use storage::{Mutation, Key, KvPair, make_key};
    use storage::mvcc::TEST_TS_BASE;

    struct AssertionStorage(SyncStorage);

    impl AssertionStorage {
        fn get_none(&self, key: &[u8], ts: u64) {
            let key = make_key(key);
            assert_eq!(self.0.get(Context::new(), &key, ts).unwrap(), None);
        }

        fn get_err(&self, key: &[u8], ts: u64) {
            let key = make_key(key);
            assert!(self.0.get(Context::new(), &key, ts).is_err());
        }

        fn get_ok(&self, key: &[u8], ts: u64, expect: &[u8]) {
            let key = make_key(key);
            assert_eq!(self.0.get(Context::new(), &key, ts).unwrap().unwrap(),
                       expect);
        }

        fn put_ok(&self, key: &[u8], value: &[u8], start_ts: u64, commit_ts: u64) {
            self.0
                .prewrite(Context::new(),
                          vec![Mutation::Put((make_key(key), value.to_vec()))],
                          key.to_vec(),
                          start_ts)
                .unwrap();
            self.0.commit(Context::new(), vec![make_key(key)], start_ts, commit_ts).unwrap();
        }

        fn delete_ok(&self, key: &[u8], start_ts: u64, commit_ts: u64) {
            self.0
                .prewrite(Context::new(),
                          vec![Mutation::Delete(make_key(key))],
                          key.to_vec(),
                          start_ts)
                .unwrap();
            self.0.commit(Context::new(), vec![make_key(key)], start_ts, commit_ts).unwrap();
        }

        fn scan_ok(&self,
                   start_key: &[u8],
                   limit: usize,
                   ts: u64,
                   expect: Vec<Option<(&[u8], &[u8])>>) {
            let key_address = make_key(start_key);
            let result = self.0.scan(Context::new(), key_address, limit, ts).unwrap();
            let result: Vec<Option<KvPair>> = result.into_iter()
                .map(Result::ok)
                .collect();
            let expect: Vec<Option<KvPair>> = expect.into_iter()
                .map(|x| x.map(|(k, v)| (k.to_vec(), v.to_vec())))
                .collect();
            assert_eq!(result, expect);
        }

        #[allow(unused_variables)]
        fn reverse_scan_ok(&self,
                           start_key: &[u8],
                           limit: usize,
                           ts: u64,
                           expect: Vec<Option<(&[u8], &[u8])>>) {
            // let key_address = make_key(start_key);
            // let result = self.0.reverse_scan(Context::new(), key_address, limit, ts).unwrap();
            // let result: Vec<Option<KvPair>> = result.into_iter()
            //     .map(Result::ok)
            //     .collect();
            // let expect: Vec<Option<KvPair>> = expect.into_iter()
            //     .map(|x| x.map(|(k, v)| (k.to_vec(), v.to_vec())))
            //     .collect();
            // assert_eq!(result, expect);
        }

        fn prewrite_ok(&self, mutations: Vec<Mutation>, primary: &[u8], start_ts: u64) {
            self.0.prewrite(Context::new(), mutations, primary.to_vec(), start_ts).unwrap();
        }

        fn commit_ok(&self, keys: Vec<&[u8]>, start_ts: u64, commit_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            self.0.commit(Context::new(), keys, start_ts, commit_ts).unwrap();
        }

        fn rollback_ok(&self, keys: Vec<&[u8]>, start_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            self.0.rollback(Context::new(), keys, start_ts).unwrap();
        }

        fn rollback_err(&self, keys: Vec<&[u8]>, start_ts: u64) {
            let keys: Vec<Key> = keys.iter().map(|x| make_key(x)).collect();
            assert!(self.0.rollback(Context::new(), keys, start_ts).is_err());
        }

        fn commit_then_get_ok(&self,
                              key: &[u8],
                              lock_ts: u64,
                              commit_ts: u64,
                              get_ts: u64,
                              expect: &[u8]) {
            assert_eq!(self.0
                           .commit_then_get(Context::new(),
                                            make_key(key),
                                            lock_ts,
                                            commit_ts,
                                            get_ts)
                           .unwrap()
                           .unwrap(),
                       expect);
        }

        fn rollback_then_get_ok(&self, key: &[u8], lock_ts: u64, expect: &[u8]) {
            assert_eq!(self.0
                           .rollback_then_get(Context::new(), make_key(key), lock_ts)
                           .unwrap()
                           .unwrap(),
                       expect);
        }
    }

    fn new_assertion_storage() -> AssertionStorage {
        AssertionStorage(SyncStorage::new(&Default::default()))
    }

    #[test]
    fn test_txn_store_get() {
        let store = new_assertion_storage();
        // not exist
        store.get_none(b"x", 10);
        // after put
        store.put_ok(b"x", b"x", 5, 10);
        store.get_none(b"x", 9);
        store.get_ok(b"x", 10, b"x");
        store.get_ok(b"x", 11, b"x");
    }

    #[test]
    fn test_txn_store_delete() {
        let store = new_assertion_storage();
        store.put_ok(b"x", b"x5-10", 5, 10);
        store.delete_ok(b"x", 15, 20);
        store.get_none(b"x", 5);
        store.get_none(b"x", 9);
        store.get_ok(b"x", 10, b"x5-10");
        store.get_ok(b"x", 19, b"x5-10");
        store.get_none(b"x", 20);
        store.get_none(b"x", 21);
    }

    #[test]
    fn test_txn_store_cleanup_rollback() {
        let store = new_assertion_storage();
        store.put_ok(b"secondary", b"s-0", 1, 2);
        store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                               Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                          b"primary",
                          5);
        store.get_err(b"secondary", 10);
        store.rollback_ok(vec![b"primary"], 5);
        store.rollback_then_get_ok(b"secondary", 5, b"s-0");
        store.rollback_then_get_ok(b"secondary", 5, b"s-0");
    }

    #[test]
    fn test_txn_store_cleanup_commit() {
        let store = new_assertion_storage();
        store.put_ok(b"secondary", b"s-0", 1, 2);
        store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                               Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                          b"primary",
                          5);
        store.get_err(b"secondary", 8);
        store.get_err(b"secondary", 12);
        store.commit_ok(vec![b"primary"], 5, 10);
        store.rollback_err(vec![b"primary"], 5);
        store.commit_then_get_ok(b"secondary", 5, 10, 8, b"s-0");
        store.commit_then_get_ok(b"secondary", 5, 10, 12, b"s-5");
        store.commit_then_get_ok(b"secondary", 5, 10, 8, b"s-0");
        store.commit_then_get_ok(b"secondary", 5, 10, 12, b"s-5");
    }

    #[test]
    fn test_txn_store_scan() {
        let store = new_assertion_storage();

        // ver10: A(10) - B(_) - C(10) - D(_) - E(10)
        store.put_ok(b"A", b"A10", 5, 10);
        store.put_ok(b"C", b"C10", 5, 10);
        store.put_ok(b"E", b"E10", 5, 10);

        let check_v10 = || {
            store.scan_ok(b"", 0, 10, vec![]);
            store.scan_ok(b"", 1, 10, vec![Some((b"A", b"A10"))]);
            store.scan_ok(b"", 2, 10, vec![Some((b"A", b"A10")), Some((b"C", b"C10"))]);
            store.scan_ok(b"",
                          3,
                          10,
                          vec![Some((b"A", b"A10")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"",
                          4,
                          10,
                          vec![Some((b"A", b"A10")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"A",
                          3,
                          10,
                          vec![Some((b"A", b"A10")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"A\x00",
                          3,
                          10,
                          vec![Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"C",
                          4,
                          10,
                          vec![Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"F", 1, 10, vec![]);

            store.reverse_scan_ok(b"F", 0, 10, vec![]);
            store.reverse_scan_ok(b"F", 1, 10, vec![Some((b"E", b"E10"))]);
            store.reverse_scan_ok(b"F",
                                  2,
                                  10,
                                  vec![Some((b"E", b"E10")), Some((b"C", b"C10"))]);
            store.reverse_scan_ok(b"F",
                                  3,
                                  10,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"C", b"C10")),
                                       Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"F",
                                  4,
                                  10,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"C", b"C10")),
                                       Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"F",
                                  3,
                                  10,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"C", b"C10")),
                                       Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"D",
                                  3,
                                  10,
                                  vec![Some((b"C", b"C10")), Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"C", 4, 10, vec![Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"0", 1, 10, vec![]);
        };
        check_v10();

        // ver20: A(10) - B(20) - C(10) - D(20) - E(10)
        store.put_ok(b"B", b"B20", 15, 20);
        store.put_ok(b"D", b"D20", 15, 20);

        let check_v20 = || {
            store.scan_ok(b"",
                          5,
                          20,
                          vec![Some((b"A", b"A10")),
                               Some((b"B", b"B20")),
                               Some((b"C", b"C10")),
                               Some((b"D", b"D20")),
                               Some((b"E", b"E10"))]);
            store.scan_ok(b"C",
                          5,
                          20,
                          vec![Some((b"C", b"C10")), Some((b"D", b"D20")), Some((b"E", b"E10"))]);
            store.scan_ok(b"D\x00", 1, 20, vec![Some((b"E", b"E10"))]);

            store.reverse_scan_ok(b"F",
                                  5,
                                  20,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"D", b"D20")),
                                       Some((b"C", b"C10")),
                                       Some((b"B", b"B20")),
                                       Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"C\x00",
                                  5,
                                  20,
                                  vec![Some((b"C", b"C10")),
                                       Some((b"B", b"B20")),
                                       Some((b"A", b"A10"))]);
            store.reverse_scan_ok(b"AAA", 1, 20, vec![Some((b"A", b"A10"))]);
        };
        check_v10();
        check_v20();

        // ver30: A(_) - B(20) - C(10) - D(_) - E(10)
        store.delete_ok(b"A", 25, 30);
        store.delete_ok(b"D", 25, 30);

        let check_v30 = || {
            store.scan_ok(b"",
                          5,
                          30,
                          vec![Some((b"B", b"B20")), Some((b"C", b"C10")), Some((b"E", b"E10"))]);
            store.scan_ok(b"A", 1, 30, vec![Some((b"B", b"B20"))]);
            store.scan_ok(b"C\x00", 5, 30, vec![Some((b"E", b"E10"))]);

            store.reverse_scan_ok(b"F",
                                  5,
                                  30,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"C", b"C10")),
                                       Some((b"B", b"B20"))]);
            store.reverse_scan_ok(b"D\x00", 1, 30, vec![Some((b"C", b"C10"))]);
            store.reverse_scan_ok(b"D\x00",
                                  5,
                                  30,
                                  vec![Some((b"C", b"C10")), Some((b"B", b"B20"))]);
        };
        check_v10();
        check_v20();
        check_v30();

        // ver40: A(_) - B(_) - C(40) - D(40) - E(10)
        store.delete_ok(b"B", 35, 40);
        store.put_ok(b"C", b"C40", 35, 40);
        store.put_ok(b"D", b"D40", 35, 40);

        let check_v40 = || {
            store.scan_ok(b"",
                          5,
                          40,
                          vec![Some((b"C", b"C40")), Some((b"D", b"D40")), Some((b"E", b"E10"))]);
            store.scan_ok(b"",
                          5,
                          100,
                          vec![Some((b"C", b"C40")), Some((b"D", b"D40")), Some((b"E", b"E10"))]);
            store.reverse_scan_ok(b"F",
                                  5,
                                  40,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"D", b"D40")),
                                       Some((b"C", b"C40"))]);
            store.reverse_scan_ok(b"F",
                                  5,
                                  100,
                                  vec![Some((b"E", b"E10")),
                                       Some((b"D", b"D40")),
                                       Some((b"C", b"C40"))]);
        };
        check_v10();
        check_v20();
        check_v30();
        check_v40();
    }

    struct Oracle {
        ts: AtomicUsize,
    }

    impl Oracle {
        fn new() -> Oracle {
            Oracle { ts: AtomicUsize::new(TEST_TS_BASE as usize) }
        }

        fn get_ts(&self) -> u64 {
            self.ts.fetch_add(1, Ordering::Relaxed) as u64
        }
    }

    const INC_MAX_RETRY: usize = 100;

    fn inc(store: &SyncStorage, oracle: &Oracle, key: &[u8]) -> Result<i32, ()> {
        let key_address = make_key(key);
        for i in 0..INC_MAX_RETRY {
            let start_ts = oracle.get_ts();
            let number: i32 = match store.get(Context::new(), &key_address, start_ts) {
                Ok(Some(x)) => String::from_utf8(x).unwrap().parse().unwrap(),
                Ok(None) => 0,
                Err(_) => {
                    backoff(i);
                    continue;
                }
            };
            let next = number + 1;
            if let Err(_) = store.prewrite(Context::new(),
                                           vec![Mutation::Put((make_key(key),
                                                               next.to_string().into_bytes()))],
                                           key.to_vec(),
                                           start_ts) {
                backoff(i);
                continue;
            }
            let commit_ts = oracle.get_ts();
            if let Err(_) = store.commit(Context::new(),
                                         vec![key_address.clone()],
                                         start_ts,
                                         commit_ts) {
                backoff(i);
                continue;
            }
            return Ok(number);
        }
        Err(())
    }

    #[test]
    fn test_isolation_inc() {
        const THREAD_NUM: usize = 4;
        const INC_PER_THREAD: usize = 100;

        let store = Arc::new(new_assertion_storage());
        let oracle = Arc::new(Oracle::new());
        let punch_card = Arc::new(Mutex::new(vec![false; THREAD_NUM * INC_PER_THREAD]));

        let mut threads = vec![];
        for _ in 0..THREAD_NUM {
            let (punch_card, store, oracle) = (punch_card.clone(), store.clone(), oracle.clone());
            threads.push(thread::spawn(move || {
                for _ in 0..INC_PER_THREAD {
                    let number = inc(&store.0, &oracle, b"key").unwrap() as usize;
                    let mut punch = punch_card.lock().unwrap();
                    assert_eq!(punch[number], false);
                    punch[number] = true;
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(inc(&store.0, &oracle, b"key").unwrap() as usize,
                   THREAD_NUM * INC_PER_THREAD);
    }

    fn format_key(x: usize) -> Vec<u8> {
        format!("k{}", x).into_bytes()
    }

    fn inc_multi(store: &SyncStorage, oracle: &Oracle, n: usize) -> bool {
        'retry: for i in 0..INC_MAX_RETRY {
            let start_ts = oracle.get_ts();
            let keys: Vec<Key> = (0..n).map(format_key).map(|x| make_key(&x)).collect();
            let mut mutations = vec![];
            for key in keys.iter().take(n) {
                let number = match store.get(Context::new(), key, start_ts) {
                    Ok(Some(n)) => String::from_utf8(n).unwrap().parse().unwrap(),
                    Ok(None) => 0,
                    Err(_) => {
                        backoff(i);
                        continue 'retry;
                    }
                };
                let next = number + 1;
                mutations.push(Mutation::Put((key.clone(), next.to_string().into_bytes())));
            }
            if let Err(_) = store.prewrite(Context::new(), mutations, b"k0".to_vec(), start_ts) {
                backoff(i);
                continue;
            }
            let commit_ts = oracle.get_ts();
            if let Err(_) = store.commit(Context::new(), keys, start_ts, commit_ts) {
                backoff(i);
                continue;
            }
            return true;
        }
        false
    }

    const BACK_OFF_CAP: u64 = 100;

    // Implements exponential backoff with full jitter.
    // See: http://www.awsarchitectureblog.com/2015/03/backoff.html.
    fn backoff(attempts: usize) {
        let upper_ms = match attempts {
            0...6 => 2u64.pow(attempts as u32),
            _ => BACK_OFF_CAP,
        };
        thread::sleep(Duration::from_millis(random::<u64>() % upper_ms))
    }

    #[test]
    fn test_isolation_multi_inc() {
        const THREAD_NUM: usize = 4;
        const KEY_NUM: usize = 4;
        const INC_PER_THREAD: usize = 100;

        let store = Arc::new(new_assertion_storage());
        let oracle = Arc::new(Oracle::new());
        let mut threads = vec![];
        for _ in 0..THREAD_NUM {
            let (store, oracle) = (store.clone(), oracle.clone());
            threads.push(thread::spawn(move || {
                for _ in 0..INC_PER_THREAD {
                    assert!(inc_multi(&store.0, &oracle, KEY_NUM));
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        for n in 0..KEY_NUM {
            assert_eq!(inc(&store.0, &oracle, &format_key(n)).unwrap() as usize,
                       THREAD_NUM * INC_PER_THREAD);
        }
    }

    use test::Bencher;

    #[bench]
    fn bench_txn_store_rocksdb_inc(b: &mut Bencher) {
        let store = new_assertion_storage();
        let oracle = Oracle::new();

        b.iter(|| {
            inc(&store.0, &oracle, b"key").unwrap();
        });
    }

    #[bench]
    fn bench_txn_store_rocksdb_inc_x100(b: &mut Bencher) {
        let store = new_assertion_storage();
        let oracle = Oracle::new();

        b.iter(|| {
            inc_multi(&store.0, &oracle, 100);
        });
    }

    #[bench]
    fn bench_txn_store_rocksdb_put_x100(b: &mut Bencher) {
        let store = new_assertion_storage();
        let oracle = Oracle::new();

        b.iter(|| {
            for _ in 0..100 {
                store.put_ok(b"key", b"value", oracle.get_ts(), oracle.get_ts());
            }
        });
    }
}