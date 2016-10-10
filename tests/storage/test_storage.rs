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

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::thread;
use rand::random;
use super::sync_storage::SyncStorage;
use kvproto::kvrpcpb::{Context, LockInfo};
use tikv::storage::{Mutation, Key, KvPair, make_key};
use tikv::storage::txn::GC_BATCH_SIZE;
use tikv::storage::config::Config as StorageConfig;

#[derive(Clone)]
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

    fn batch_get_ok(&self, keys: &[&[u8]], ts: u64, expect: Vec<&[u8]>) {
        let keys: Vec<Key> = keys.into_iter().map(|x| make_key(x)).collect();
        let result: Vec<Vec<u8>> = self.0
            .batch_get(Context::new(), &keys, ts)
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap().1)
            .collect();
        let expect: Vec<Vec<u8>> = expect.into_iter().map(|x| x.to_vec()).collect();
        assert_eq!(result, expect);
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
        // TODO: uncomment it after Storage support reverse_scan.
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

    fn scan_lock_ok(&self, max_ts: u64, expect: Vec<LockInfo>) {
        assert_eq!(self.0.scan_lock(Context::new(), max_ts).unwrap(), expect);
    }

    fn resolve_lock_ok(&self, start_ts: u64, commit_ts: Option<u64>) {
        self.0.resolve_lock(Context::new(), start_ts, commit_ts).unwrap();
    }

    fn gc_ok(&self, safe_point: u64) {
        self.0.gc(Context::new(), safe_point).unwrap();
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
}

#[test]
fn test_txn_store_batch_get() {
    let store = new_assertion_storage();
    store.put_ok(b"x", b"x1", 5, 10);
    store.put_ok(b"y", b"y1", 15, 20);
    store.put_ok(b"z", b"z1", 25, 30);
    store.batch_get_ok(&[b"x", b"y", b"z", b"w"], 15, vec![b"x1"]);
    store.batch_get_ok(&[b"x", b"y", b"z", b"w"], 16, vec![b"x1"]);
    store.batch_get_ok(&[b"x", b"y", b"z", b"w"], 19, vec![b"x1"]);
    store.batch_get_ok(&[b"x", b"y", b"z", b"w"], 20, vec![b"x1", b"y1"]);
    store.batch_get_ok(&[b"x", b"y", b"z", b"w"], 21, vec![b"x1", b"y1"]);
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

fn lock(key: &[u8], primary: &[u8], ts: u64) -> LockInfo {
    let mut lock = LockInfo::new();
    lock.set_key(key.to_vec());
    lock.set_primary_lock(primary.to_vec());
    lock.set_lock_version(ts);
    lock
}

#[test]
fn test_txn_store_scan_lock() {
    let store = new_assertion_storage();

    store.put_ok(b"k1", b"v1", 1, 2);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"p1"), b"v5".to_vec())),
                           Mutation::Put((make_key(b"s1"), b"v5".to_vec()))],
                      b"p1",
                      5);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"p2"), b"v10".to_vec())),
                           Mutation::Put((make_key(b"s2"), b"v10".to_vec()))],
                      b"p2",
                      10);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"p3"), b"v20".to_vec())),
                           Mutation::Put((make_key(b"s3"), b"v20".to_vec()))],
                      b"p3",
                      20);
    // scan should return locks.
    store.scan_ok(b"",
                  10,
                  15,
                  vec![Some((b"k1", b"v1")), None, None, None, None]);
    store.scan_lock_ok(10,
                       vec![lock(b"p1", b"p1", 5),
                            lock(b"p2", b"p2", 10),
                            lock(b"s1", b"p1", 5),
                            lock(b"s2", b"p2", 10)]);
}

#[test]
fn test_txn_store_resolve_lock() {
    let store = new_assertion_storage();

    store.prewrite_ok(vec![Mutation::Put((make_key(b"p1"), b"v5".to_vec())),
                           Mutation::Put((make_key(b"s1"), b"v5".to_vec()))],
                      b"p1",
                      5);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"p2"), b"v10".to_vec())),
                           Mutation::Put((make_key(b"s2"), b"v10".to_vec()))],
                      b"p2",
                      10);
    store.resolve_lock_ok(5, None);
    store.resolve_lock_ok(10, Some(20));
    store.get_none(b"p1", 20);
    store.get_none(b"s1", 30);
    store.get_ok(b"p2", 20, b"v10");
    store.get_ok(b"s2", 30, b"v10");
    store.scan_lock_ok(30, vec![]);
}

#[test]
fn test_txn_store_gc() {
    let mut cfg = StorageConfig::new();
    cfg.sched_pro_exec_gc = false;
    let store = AssertionStorage(SyncStorage::new(&cfg));
    store.put_ok(b"k", b"v1", 5, 10);
    store.put_ok(b"k", b"v2", 15, 20);
    store.gc_ok(30);
    store.get_none(b"k", 15);
    store.get_ok(b"k", 25, b"v2");
}

fn test_txn_store_gc_multiple_keys(n: usize) {
    let store = new_assertion_storage();
    for i in 0..n {
        let key = format!("k{}", i);
        store.put_ok(key.as_bytes(), b"value", 5, 10);
    }
    store.gc_ok(20);
}

#[test]
fn test_txn_store_gc2() {
    for &i in &[0, 1, GC_BATCH_SIZE - 1, GC_BATCH_SIZE, GC_BATCH_SIZE + 1, GC_BATCH_SIZE * 2] {
        test_txn_store_gc_multiple_keys(i);
    }
}

struct Oracle {
    ts: AtomicUsize,
}

impl Oracle {
    fn new() -> Oracle {
        Oracle { ts: AtomicUsize::new(1 as usize) }
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

    let store = new_assertion_storage();
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

    let store = new_assertion_storage();
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
