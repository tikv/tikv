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
use std::sync::mpsc::channel;
use std::time::Duration;
use std::thread;
use rand::random;
use super::sync_storage::SyncStorage;
use kvproto::kvrpcpb::{Context, LockInfo};
use tikv::storage::{self, Mutation, Key, make_key, ALL_CFS, Storage};
use tikv::storage::engine::{self, TEMP_DIR, Engine};
use tikv::storage::txn::{GC_BATCH_SIZE, RESOLVE_LOCK_BATCH_SIZE};
use tikv::storage::mvcc::MAX_TXN_WRITE_SIZE;
use tikv::storage::config::Config;

use super::util::new_raft_engine;
use super::assert_storage::AssertionStorage;
use storage::util;
use std::collections::HashSet;
use std::u64;

#[test]
fn test_txn_store_get() {
    let store = AssertionStorage::default();
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
    let store = AssertionStorage::default();
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
    let store = AssertionStorage::default();
    store.put_ok(b"secondary", b"s-0", 1, 2);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                           Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                      b"primary",
                      5);
    store.get_err(b"secondary", 10);
    store.rollback_ok(vec![b"primary"], 5);
    store.cleanup_ok(b"primary", 5);
}

#[test]
fn test_txn_store_cleanup_commit() {
    let store = AssertionStorage::default();
    store.put_ok(b"secondary", b"s-0", 1, 2);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                           Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                      b"primary",
                      5);
    store.get_err(b"secondary", 8);
    store.get_err(b"secondary", 12);
    store.commit_ok(vec![b"primary"], 5, 10);
    store.cleanup_err(b"primary", 5);
    store.rollback_err(vec![b"primary"], 5);
}

#[test]
fn test_txn_store_for_point_get_with_pk() {
    let store = AssertionStorage::default();

    store.put_ok(b"b", b"v2", 1, 2);
    store.put_ok(b"primary", b"v1", 2, 3);
    store.put_ok(b"secondary", b"v3", 3, 4);
    store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"v3".to_vec())),
                           Mutation::Put((make_key(b"secondary"), b"s-5".to_vec())),
                           Mutation::Put((make_key(b"new_key"), b"new_key".to_vec()))],
                      b"primary",
                      5);
    store.get_ok(b"primary", 4, b"v1");
    store.get_ok(b"primary", u64::MAX, b"v1");
    store.get_err(b"primary", 6);

    store.get_ok(b"secondary", 4, b"v3");
    store.get_err(b"secondary", 6);
    store.get_err(b"secondary", u64::MAX);

    store.get_err(b"new_key", 6);
    store.get_ok(b"b", 6, b"v2");

}

#[test]
fn test_txn_store_batch_get() {
    let store = AssertionStorage::default();
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
    let store = AssertionStorage::default();

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
    };
    check_v10();
    check_v20();
    check_v30();
    check_v40();
}

#[test]
fn test_txn_store_scan_key_only() {
    let store = AssertionStorage::default();
    store.put_ok(b"A", b"A", 5, 10);
    store.put_ok(b"B", b"B", 5, 10);
    store.put_ok(b"C", b"C", 5, 10);
    store.scan_key_only_ok(b"AA", 2, 10, vec![Some(b"B"), Some(b"C")]);
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
    let store = AssertionStorage::default();

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
    let store = AssertionStorage::default();

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

fn test_txn_store_resolve_lock_batch(key_prefix_len: usize, n: usize) {
    let prefix = String::from_utf8(vec![b'k'; key_prefix_len]).unwrap();
    let keys: Vec<String> = (0..n).map(|i| format!("{}{}", prefix, i)).collect();

    let store = AssertionStorage::default();
    for k in &keys {
        store.prewrite_ok(vec![Mutation::Put((make_key(k.as_bytes()), b"v".to_vec()))],
                          b"k1",
                          5);
    }
    store.resolve_lock_ok(5, Some(10));
    for k in &keys {
        store.get_ok(k.as_bytes(), 30, b"v");
        store.get_none(k.as_bytes(), 8);
    }
}

#[test]
fn test_txn_store_resolve_lock2() {
    for &i in &[0,
                1,
                RESOLVE_LOCK_BATCH_SIZE - 1,
                RESOLVE_LOCK_BATCH_SIZE,
                RESOLVE_LOCK_BATCH_SIZE + 1,
                RESOLVE_LOCK_BATCH_SIZE * 2] {
        test_txn_store_resolve_lock_batch(1, i);
    }

    for &i in &[1, MAX_TXN_WRITE_SIZE / 2, MAX_TXN_WRITE_SIZE + 1] {
        test_txn_store_resolve_lock_batch(i, 3);
    }
}

#[test]
fn test_txn_store_commit_illegal_tso() {
    let store = AssertionStorage::default();
    let commit_ts = 4;
    let start_ts = 5;
    store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                           Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                      b"primary",
                      start_ts);

    store.commit_with_illegal_tso(vec![b"primary"], start_ts, commit_ts);
}

#[test]
fn test_store_resolve_with_illegal_tso() {
    let store = AssertionStorage::default();
    let commit_ts = Some(4);
    let start_ts = 5;
    store.prewrite_ok(vec![Mutation::Put((make_key(b"primary"), b"p-5".to_vec())),
                           Mutation::Put((make_key(b"secondary"), b"s-5".to_vec()))],
                      b"primary",
                      start_ts);
    store.resolve_lock_with_illegal_tso(start_ts, commit_ts);
}

#[test]
fn test_txn_store_gc() {
    let key = "k";
    let store = AssertionStorage::default();
    let (_cluster, raft_store) = AssertionStorage::new_raft_storage_with_store_count(3, key);
    store.test_txn_store_gc(key);
    raft_store.test_txn_store_gc(key);
}

fn test_txn_store_gc_multiple_keys(key_prefix_len: usize, n: usize) {
    let prefix = String::from_utf8(vec![b'k'; key_prefix_len]).unwrap();
    test_txn_store_gc_multiple_keys_cluster_storage(n, prefix.clone());
    test_txn_store_gc_multiple_keys_single_storage(n, prefix.clone());
}

pub fn test_txn_store_gc_multiple_keys_single_storage(n: usize, prefix: String) {
    let store = AssertionStorage::default();
    let keys: Vec<String> = (0..n).map(|i| format!("{}{}", prefix, i)).collect();
    for k in &keys {
        store.put_ok(k.as_bytes(), b"v1", 5, 10);
        store.put_ok(k.as_bytes(), b"v2", 15, 20);
    }
    store.gc_ok(30);
    for k in &keys {
        store.get_none(k.as_bytes(), 15);
    }
}

pub fn test_txn_store_gc_multiple_keys_cluster_storage(n: usize, prefix: String) {
    let (mut cluster, mut store) =
        AssertionStorage::new_raft_storage_with_store_count(3, prefix.clone().as_str());
    let keys: Vec<String> = (0..n).map(|i| format!("{}{}", prefix, i)).collect();
    let mut stores: HashSet<u64> = HashSet::new();
    for k in &keys {
        store.put_ok_for_cluster(&mut cluster, k.as_bytes(), b"v1", 5, 10);
        store.put_ok_for_cluster(&mut cluster, k.as_bytes(), b"v2", 15, 20);
        let store_id = store.ctx.get_peer().get_store_id();
        if !stores.contains(&store_id) {
            stores.insert(store_id);
        }
    }
    for k in &keys {
        store.update_with_key(&mut cluster, k);
        let store_id = store.ctx.get_peer().get_store_id();
        if stores.remove(&store_id) {
            store.gc_ok(30); // clear data which commit_ts < 30
        }
    }

    for k in &keys {
        store.get_none_from_cluster(&mut cluster, k.as_bytes(), 15);
    }
}

#[test]
fn test_txn_store_gc2_without_key() {
    test_txn_store_gc_multiple_keys(1, 0);
}

#[test]
fn test_txn_store_gc2_with_less_keys() {
    test_txn_store_gc_multiple_keys(1, 3);
}

#[test]
fn test_txn_store_gc2_with_many_keys() {
    test_txn_store_gc_multiple_keys(1, GC_BATCH_SIZE + 1);
}

#[test]
fn test_txn_store_gc2_with_long_key_prefix() {
    test_txn_store_gc_multiple_keys(MAX_TXN_WRITE_SIZE + 1, 3);
}

#[test]
fn test_txn_store_gc3() {
    let key = "k";
    let store = AssertionStorage::default();
    store.test_txn_store_gc3(key.as_bytes()[0]);
    let (_cluster, raft_store) = AssertionStorage::new_raft_storage_with_store_count(3, key);
    raft_store.test_txn_store_gc3(key.as_bytes()[0]);
}

#[test]
fn test_txn_store_rawkv() {
    let store = AssertionStorage::default();
    store.raw_get_ok(b"key".to_vec(), None);
    store.raw_put_ok(b"key".to_vec(), b"value".to_vec());
    store.raw_get_ok(b"key".to_vec(), Some(b"value".to_vec()));
    store.raw_put_ok(b"key".to_vec(), b"v2".to_vec());
    store.raw_get_ok(b"key".to_vec(), Some(b"v2".to_vec()));
    store.raw_delete_ok(b"key".to_vec());
    store.raw_get_ok(b"key".to_vec(), None);
}

#[test]
fn test_txn_store_lock_primary() {
    let store = AssertionStorage::default();
    // txn1 locks "p" then aborts.
    store.prewrite_ok(vec![Mutation::Put((make_key(b"p"), b"p1".to_vec()))],
                      b"p",
                      1);

    // txn2 wants to write "p", "s".
    store.prewrite_locked(vec![Mutation::Put((make_key(b"p"), b"p2".to_vec())),
                               Mutation::Put((make_key(b"s"), b"s2".to_vec()))],
                          b"p",
                          2,
                          vec![(b"p", b"p", 1)]);
    // txn2 cleanups txn1's lock.
    store.rollback_ok(vec![b"p"], 1);
    store.resolve_lock_ok(1, None);

    // txn3 wants to write "p", "s", neither of them should be locked.
    store.prewrite_ok(vec![Mutation::Put((make_key(b"p"), b"p3".to_vec())),
                           Mutation::Put((make_key(b"s"), b"s3".to_vec()))],
                      b"p",
                      3);
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
        if store.prewrite(Context::new(),
                      vec![Mutation::Put((make_key(key), next.to_string().into_bytes()))],
                      key.to_vec(),
                      start_ts)
            .is_err() {
            backoff(i);
            continue;
        }
        let commit_ts = oracle.get_ts();
        if store.commit(Context::new(),
                    vec![key_address.clone()],
                    start_ts,
                    commit_ts)
            .is_err() {
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

    let store = AssertionStorage::default();
    let oracle = Arc::new(Oracle::new());
    let punch_card = Arc::new(Mutex::new(vec![false; THREAD_NUM * INC_PER_THREAD]));

    let mut threads = vec![];
    for _ in 0..THREAD_NUM {
        let (punch_card, store, oracle) = (punch_card.clone(), store.clone(), oracle.clone());
        threads.push(thread::spawn(move || {
            for _ in 0..INC_PER_THREAD {
                let number = inc(&store.store, &oracle, b"key").unwrap() as usize;
                let mut punch = punch_card.lock().unwrap();
                assert_eq!(punch[number], false);
                punch[number] = true;
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(inc(&store.store, &oracle, b"key").unwrap() as usize,
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
        if store.prewrite(Context::new(), mutations, b"k0".to_vec(), start_ts).is_err() {
            backoff(i);
            continue;
        }
        let commit_ts = oracle.get_ts();
        if store.commit(Context::new(), keys, start_ts, commit_ts).is_err() {
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

    let store = AssertionStorage::default();
    let oracle = Arc::new(Oracle::new());
    let mut threads = vec![];
    for _ in 0..THREAD_NUM {
        let (store, oracle) = (store.clone(), oracle.clone());
        threads.push(thread::spawn(move || {
            for _ in 0..INC_PER_THREAD {
                assert!(inc_multi(&store.store, &oracle, KEY_NUM));
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }
    for n in 0..KEY_NUM {
        assert_eq!(inc(&store.store, &oracle, &format_key(n)).unwrap() as usize,
                   THREAD_NUM * INC_PER_THREAD);
    }
}

use test::Bencher;

#[bench]
fn bench_txn_store_rocksdb_inc(b: &mut Bencher) {
    let store = AssertionStorage::default();
    let oracle = Oracle::new();

    b.iter(|| {
        inc(&store.store, &oracle, b"key").unwrap();
    });
}

#[bench]
fn bench_txn_store_rocksdb_inc_x100(b: &mut Bencher) {
    let store = AssertionStorage::default();
    let oracle = Oracle::new();

    b.iter(|| {
        inc_multi(&store.store, &oracle, 100);
    });
}

#[bench]
fn bench_txn_store_rocksdb_put_x100(b: &mut Bencher) {
    let store = AssertionStorage::default();
    let oracle = Oracle::new();

    b.iter(|| {
        for _ in 0..100 {
            store.put_ok(b"key", b"value", oracle.get_ts(), oracle.get_ts());
        }
    });
}

fn test_storage_1gc_with_engine(engine: Box<Engine>, ctx: Context) {
    let mut engine = util::BlockEngine::new(engine);
    let config = Config::default();
    let mut storage = Storage::from_engine(engine.clone(), &config).unwrap();
    storage.start(&config).unwrap();
    let (stx, srx) = channel();
    engine.block_snapshot(stx);
    let (tx1, rx1) = channel();
    storage.async_gc(ctx.clone(),
                  1,
                  box move |res: storage::Result<()>| {
                      assert!(res.is_ok());
                      tx1.send(1).unwrap();
                  })
        .unwrap();

    // Old GC command is blocked at snapshot stage, the other one will get ServerIsBusy error.
    let (tx2, rx2) = channel();
    storage.async_gc(Context::new(),
                  1,
                  box move |res: storage::Result<()>| {
            match res {
                Err(storage::Error::SchedTooBusy) => {}
                _ => panic!("expect too busy"),
            }
            tx2.send(1).unwrap();
        })
        .unwrap();

    srx.recv_timeout(Duration::from_secs(2)).unwrap();
    rx2.recv().unwrap();
    engine.unblock_snapshot();
    rx1.recv().unwrap();
}
#[test]
fn test_storage_1gc() {
    let engine = engine::new_local_engine(TEMP_DIR, ALL_CFS).unwrap();
    test_storage_1gc_with_engine(engine, Context::new());
    let (_cluster, raft_engine, ctx) = new_raft_engine(3, "");
    test_storage_1gc_with_engine(raft_engine, ctx);
}
