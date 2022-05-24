// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    iter::repeat,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use engine_traits::{CF_DEFAULT, CF_LOCK};
use kvproto::kvrpcpb::{ApiVersion, Context, KeyRange, LockInfo};
use rand::random;
use test_storage::*;
use tikv::{
    coprocessor::checksum_crc64_xor,
    server::gc_worker::DEFAULT_GC_BATCH_KEYS,
    storage::{mvcc::MAX_TXN_WRITE_SIZE, txn::RESOLVE_LOCK_BATCH_SIZE, Engine},
};
use txn_types::{Key, Mutation, TimeStamp};

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
fn test_txn_store_get_with_type_lock() {
    let store = AssertionStorage::default();
    store.put_ok(b"k1", b"v1", 1, 2);
    store.prewrite_ok(vec![Mutation::make_lock(Key::from_raw(b"k1"))], b"k1", 5);
    store.get_ok(b"k1", 20, b"v1");
}

#[test]
fn test_txn_store_batch_get_command() {
    let store = AssertionStorage::default();
    // not exist
    store.get_none(b"a", 10);
    store.get_none(b"b", 10);
    // after put
    store.put_ok(b"a", b"x", 5, 10);
    store.put_ok(b"b", b"x", 5, 10);
    store.batch_get_command_ok(&[b"a", b"b", b"c"], 10, vec![b"x", b"x", b""]);
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
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"primary"), b"p-5".to_vec()),
            Mutation::make_put(Key::from_raw(b"secondary"), b"s-5".to_vec()),
        ],
        b"primary",
        5,
    );
    store.get_err(b"secondary", 10);
    store.rollback_ok(vec![b"primary"], 5);
    store.cleanup_ok(b"primary", 5, 0);
}

#[test]
fn test_txn_store_cleanup_commit() {
    let store = AssertionStorage::default();
    store.put_ok(b"secondary", b"s-0", 1, 2);
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"primary"), b"p-5".to_vec()),
            Mutation::make_put(Key::from_raw(b"secondary"), b"s-5".to_vec()),
        ],
        b"primary",
        5,
    );
    store.get_err(b"secondary", 8);
    store.get_err(b"secondary", 12);
    store.commit_ok(vec![b"primary"], 5, 10, 10);
    store.cleanup_err(b"primary", 5, 0);
    store.rollback_err(vec![b"primary"], 5);
}

#[test]
fn test_txn_store_for_point_get_with_pk() {
    let store = AssertionStorage::default();

    store.put_ok(b"b", b"v2", 1, 2);
    store.put_ok(b"primary", b"v1", 2, 3);
    store.put_ok(b"secondary", b"v3", 3, 4);
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"primary"), b"v3".to_vec()),
            Mutation::make_put(Key::from_raw(b"secondary"), b"s-5".to_vec()),
            Mutation::make_put(Key::from_raw(b"new_key"), b"new_key".to_vec()),
        ],
        b"primary",
        5,
    );
    store.get_ok(b"primary", 4, b"v1");
    store.get_ok(b"primary", TimeStamp::max(), b"v1");
    store.get_err(b"primary", 6);

    store.get_ok(b"secondary", 4, b"v3");
    store.get_err(b"secondary", 6);
    store.get_err(b"secondary", TimeStamp::max());

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
        store.scan_ok(b"", None, 0, 10, vec![]);
        store.scan_ok(b"", None, 1, 10, vec![Some((b"A", b"A10"))]);
        store.scan_ok(
            b"",
            None,
            2,
            10,
            vec![Some((b"A", b"A10")), Some((b"C", b"C10"))],
        );
        store.scan_ok(
            b"",
            None,
            3,
            10,
            vec![
                Some((b"A", b"A10")),
                Some((b"C", b"C10")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(
            b"",
            None,
            4,
            10,
            vec![
                Some((b"A", b"A10")),
                Some((b"C", b"C10")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(
            b"A",
            None,
            3,
            10,
            vec![
                Some((b"A", b"A10")),
                Some((b"C", b"C10")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(
            b"A\x00",
            None,
            3,
            10,
            vec![Some((b"C", b"C10")), Some((b"E", b"E10"))],
        );
        store.scan_ok(
            b"C",
            None,
            4,
            10,
            vec![Some((b"C", b"C10")), Some((b"E", b"E10"))],
        );
        store.scan_ok(b"F", None, 1, 10, vec![]);
    };
    check_v10();

    // ver20: A(10) - B(20) - C(10) - D(20) - E(10)
    store.put_ok(b"B", b"B20", 15, 20);
    store.put_ok(b"D", b"D20", 15, 20);

    let check_v20 = || {
        store.scan_ok(
            b"",
            None,
            5,
            20,
            vec![
                Some((b"A", b"A10")),
                Some((b"B", b"B20")),
                Some((b"C", b"C10")),
                Some((b"D", b"D20")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(
            b"C",
            None,
            5,
            20,
            vec![
                Some((b"C", b"C10")),
                Some((b"D", b"D20")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(b"D\x00", None, 1, 20, vec![Some((b"E", b"E10"))]);
    };
    check_v10();
    check_v20();

    // ver30: A(_) - B(20) - C(10) - D(_) - E(10)
    store.delete_ok(b"A", 25, 30);
    store.delete_ok(b"D", 25, 30);

    let check_v30 = || {
        store.scan_ok(
            b"",
            None,
            5,
            30,
            vec![
                Some((b"B", b"B20")),
                Some((b"C", b"C10")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(b"A", None, 1, 30, vec![Some((b"B", b"B20"))]);
        store.scan_ok(b"C\x00", None, 5, 30, vec![Some((b"E", b"E10"))]);
    };
    check_v10();
    check_v20();
    check_v30();

    // ver40: A(_) - B(_) - C(40) - D(40) - E(10)
    store.delete_ok(b"B", 35, 40);
    store.put_ok(b"C", b"C40", 35, 40);
    store.put_ok(b"D", b"D40", 35, 40);

    let check_v40 = || {
        store.scan_ok(
            b"",
            None,
            5,
            40,
            vec![
                Some((b"C", b"C40")),
                Some((b"D", b"D40")),
                Some((b"E", b"E10")),
            ],
        );
        store.scan_ok(
            b"",
            None,
            5,
            100,
            vec![
                Some((b"C", b"C40")),
                Some((b"D", b"D40")),
                Some((b"E", b"E10")),
            ],
        );
    };
    check_v10();
    check_v20();
    check_v30();
    check_v40();
}

#[test]
fn test_txn_store_reverse_scan() {
    let store = AssertionStorage::default();

    // ver10: A(10) - B(_) - C(10) - D(_) - E(10)
    store.put_ok(b"A", b"A10", 5, 10);
    store.put_ok(b"C", b"C10", 5, 10);
    store.put_ok(b"E", b"E10", 5, 10);

    let check_v10 = || {
        store.reverse_scan_ok(b"Z", None, 0, 10, vec![]);
        store.reverse_scan_ok(b"Z", None, 1, 10, vec![Some((b"E", b"E10"))]);
        store.reverse_scan_ok(
            b"Z",
            None,
            2,
            10,
            vec![Some((b"E", b"E10")), Some((b"C", b"C10"))],
        );
        store.reverse_scan_ok(
            b"Z",
            None,
            3,
            10,
            vec![
                Some((b"E", b"E10")),
                Some((b"C", b"C10")),
                Some((b"A", b"A10")),
            ],
        );
        store.reverse_scan_ok(
            b"Z",
            None,
            4,
            10,
            vec![
                Some((b"E", b"E10")),
                Some((b"C", b"C10")),
                Some((b"A", b"A10")),
            ],
        );
        store.reverse_scan_ok(
            b"E\x00",
            None,
            3,
            10,
            vec![
                Some((b"E", b"E10")),
                Some((b"C", b"C10")),
                Some((b"A", b"A10")),
            ],
        );
        store.reverse_scan_ok(
            b"E",
            None,
            3,
            10,
            vec![Some((b"C", b"C10")), Some((b"A", b"A10"))],
        );
        store.reverse_scan_ok(b"", None, 1, 10, vec![]);
    };
    check_v10();

    // ver20: A(10) - B(20) - C(10) - D(20) - E(10)
    store.put_ok(b"B", b"B20", 15, 20);
    store.put_ok(b"D", b"D20", 15, 20);

    let check_v20 = || {
        store.reverse_scan_ok(
            b"Z",
            None,
            5,
            20,
            vec![
                Some((b"E", b"E10")),
                Some((b"D", b"D20")),
                Some((b"C", b"C10")),
                Some((b"B", b"B20")),
                Some((b"A", b"A10")),
            ],
        );
        store.reverse_scan_ok(
            b"C",
            None,
            5,
            20,
            vec![Some((b"B", b"B20")), Some((b"A", b"A10"))],
        );
        store.reverse_scan_ok(b"D\x00", None, 1, 20, vec![Some((b"D", b"D20"))]);
    };
    check_v10();
    check_v20();

    // ver30: A(_) - B(20) - C(10) - D(_) - E(10)
    store.delete_ok(b"A", 25, 30);
    store.delete_ok(b"D", 25, 30);

    let check_v30 = || {
        store.reverse_scan_ok(
            b"Z",
            None,
            5,
            30,
            vec![
                Some((b"E", b"E10")),
                Some((b"C", b"C10")),
                Some((b"B", b"B20")),
            ],
        );
        store.reverse_scan_ok(b"E", None, 1, 30, vec![Some((b"C", b"C10"))]);
        store.reverse_scan_ok(b"B\x00", None, 5, 30, vec![Some((b"B", b"B20"))]);
    };
    check_v10();
    check_v20();
    check_v30();

    // ver40: A(_) - B(_) - C(40) - D(40) - E(10)
    store.delete_ok(b"B", 35, 40);
    store.put_ok(b"C", b"C40", 35, 40);
    store.put_ok(b"D", b"D40", 35, 40);

    let check_v40 = || {
        store.reverse_scan_ok(
            b"Z",
            None,
            5,
            40,
            vec![
                Some((b"E", b"E10")),
                Some((b"D", b"D40")),
                Some((b"C", b"C40")),
            ],
        );
        store.reverse_scan_ok(
            b"E",
            None,
            5,
            100,
            vec![Some((b"D", b"D40")), Some((b"C", b"C40"))],
        );
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
    store.scan_key_only_ok(b"AA", None, 2, 10, vec![Some(b"B"), Some(b"C")]);
}

fn lock(key: &[u8], primary: &[u8], ts: u64) -> LockInfo {
    let mut lock = LockInfo::default();
    lock.set_key(key.to_vec());
    lock.set_primary_lock(primary.to_vec());
    lock.set_lock_version(ts);
    lock
}

#[test]
fn test_txn_store_scan_lock() {
    let store = AssertionStorage::default();

    store.put_ok(b"k1", b"v1", 1, 2);
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p1"), b"v5".to_vec()),
            Mutation::make_put(Key::from_raw(b"s1"), b"v5".to_vec()),
        ],
        b"p1",
        5,
    );
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p2"), b"v10".to_vec()),
            Mutation::make_put(Key::from_raw(b"s2"), b"v10".to_vec()),
        ],
        b"p2",
        10,
    );
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p3"), b"v20".to_vec()),
            Mutation::make_put(Key::from_raw(b"s3"), b"v20".to_vec()),
        ],
        b"p3",
        20,
    );
    // scan should return locks.
    store.scan_ok(
        b"",
        None,
        10,
        15,
        vec![Some((b"k1", b"v1")), None, None, None, None],
    );

    store.scan_locks_ok(10, b"", b"", 1, vec![lock(b"p1", b"p1", 5)]);

    store.scan_locks_ok(
        10,
        b"s",
        b"",
        2,
        vec![lock(b"s1", b"p1", 5), lock(b"s2", b"p2", 10)],
    );

    store.scan_locks_ok(
        10,
        b"",
        b"",
        0,
        vec![
            lock(b"p1", b"p1", 5),
            lock(b"p2", b"p2", 10),
            lock(b"s1", b"p1", 5),
            lock(b"s2", b"p2", 10),
        ],
    );

    store.scan_locks_ok(
        10,
        b"",
        b"",
        100,
        vec![
            lock(b"p1", b"p1", 5),
            lock(b"p2", b"p2", 10),
            lock(b"s1", b"p1", 5),
            lock(b"s2", b"p2", 10),
        ],
    );
}

#[test]
fn test_txn_store_resolve_lock() {
    let store = AssertionStorage::default();

    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p1"), b"v5".to_vec()),
            Mutation::make_put(Key::from_raw(b"s1"), b"v5".to_vec()),
        ],
        b"p1",
        5,
    );
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p2"), b"v10".to_vec()),
            Mutation::make_put(Key::from_raw(b"s2"), b"v10".to_vec()),
        ],
        b"p2",
        10,
    );
    store.resolve_lock_ok(5, None::<TimeStamp>);
    store.resolve_lock_ok(10, Some(20));
    store.get_none(b"p1", 20);
    store.get_none(b"s1", 30);
    store.get_ok(b"p2", 20, b"v10");
    store.get_ok(b"s2", 30, b"v10");
    store.scan_locks_ok(30, b"", b"", 100, vec![]);
}

fn test_txn_store_resolve_lock_batch(key_prefix_len: usize, n: usize) {
    let prefix = String::from_utf8(vec![b'k'; key_prefix_len]).unwrap();
    let keys: Vec<String> = (0..n).map(|i| format!("{}{}", prefix, i)).collect();

    let store = AssertionStorage::default();
    for k in &keys {
        store.prewrite_ok(
            vec![Mutation::make_put(
                Key::from_raw(k.as_bytes()),
                b"v".to_vec(),
            )],
            b"k1",
            5,
        );
    }
    store.resolve_lock_ok(5, Some(10));
    for k in &keys {
        store.get_ok(k.as_bytes(), 30, b"v");
        store.get_none(k.as_bytes(), 8);
    }
}

#[test]
fn test_txn_store_resolve_lock_in_a_batch() {
    let store = AssertionStorage::default();

    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p1"), b"v5".to_vec()),
            Mutation::make_put(Key::from_raw(b"s1"), b"v5".to_vec()),
        ],
        b"p1",
        5,
    );
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p2"), b"v10".to_vec()),
            Mutation::make_put(Key::from_raw(b"s2"), b"v10".to_vec()),
        ],
        b"p2",
        10,
    );
    store.resolve_lock_batch_ok(5, 0, 10, 20);
    store.get_none(b"p1", 30);
    store.get_none(b"s1", 30);
    store.get_ok(b"p2", 30, b"v10");
    store.get_ok(b"s2", 30, b"v10");
    store.scan_locks_ok(30, b"", b"", 100, vec![]);
}

#[test]
fn test_txn_store_resolve_lock2() {
    for &i in &[
        0,
        1,
        RESOLVE_LOCK_BATCH_SIZE - 1,
        RESOLVE_LOCK_BATCH_SIZE,
        RESOLVE_LOCK_BATCH_SIZE + 1,
        RESOLVE_LOCK_BATCH_SIZE * 2,
    ] {
        test_txn_store_resolve_lock_batch(1, i);
    }

    for &i in &[1, 512, 1024] {
        test_txn_store_resolve_lock_batch(i, 50);
    }
}

#[test]
fn test_txn_store_commit_illegal_tso() {
    let store = AssertionStorage::default();
    let commit_ts = 4;
    let start_ts = 5;
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"primary"), b"p-5".to_vec()),
            Mutation::make_put(Key::from_raw(b"secondary"), b"s-5".to_vec()),
        ],
        b"primary",
        start_ts,
    );

    store.commit_with_illegal_tso(vec![b"primary"], start_ts, commit_ts);
}

#[test]
fn test_store_resolve_with_illegal_tso() {
    let store = AssertionStorage::default();
    let commit_ts = Some(4);
    let start_ts = 5;
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"primary"), b"p-5".to_vec()),
            Mutation::make_put(Key::from_raw(b"secondary"), b"s-5".to_vec()),
        ],
        b"primary",
        start_ts,
    );
    store.resolve_lock_with_illegal_tso(start_ts, commit_ts);
}

#[test]
fn test_txn_store_gc() {
    let key = "k";
    let store = AssertionStorage::default();
    let (_cluster, raft_store) = AssertionStorageApiV1::new_raft_storage_with_store_count(3, key);
    store.test_txn_store_gc(key);
    raft_store.test_txn_store_gc(key);
}

fn test_txn_store_gc_multiple_keys(key_prefix_len: usize, n: usize) {
    let prefix = String::from_utf8(vec![b'k'; key_prefix_len]).unwrap();
    test_txn_store_gc_multiple_keys_cluster_storage(n, prefix.clone());
    test_txn_store_gc_multiple_keys_single_storage(n, prefix);
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
        AssertionStorageApiV1::new_raft_storage_with_store_count(3, prefix.as_str());
    let keys: Vec<String> = (0..n).map(|i| format!("{}{}", prefix, i)).collect();
    if !keys.is_empty() {
        store.batch_put_ok_for_cluster(&mut cluster, &keys, repeat(b"v1" as &[u8]), 5, 10);
        store.batch_put_ok_for_cluster(&mut cluster, &keys, repeat(b"v2" as &[u8]), 15, 20);
    }

    let mut last_region = cluster.get_region(b"");
    store.gc_ok_for_cluster(&mut cluster, b"", 30);
    for k in &keys {
        // clear data whose commit_ts < 30
        let region = cluster.get_region(k.as_bytes());
        if last_region != region {
            store.gc_ok_for_cluster(&mut cluster, k.as_bytes(), 30);
            last_region = region;
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
    test_txn_store_gc_multiple_keys(1, DEFAULT_GC_BATCH_KEYS + 1);
}

#[test]
fn test_txn_store_gc2_with_long_key_prefix() {
    test_txn_store_gc_multiple_keys(1024, MAX_TXN_WRITE_SIZE / 1024 * 3);
}

#[test]
fn test_txn_store_gc3() {
    let key = "k";
    let store = AssertionStorage::default();
    store.test_txn_store_gc3(key.as_bytes()[0]);
    let (mut cluster, mut raft_store) =
        AssertionStorageApiV1::new_raft_storage_with_store_count(3, key);
    raft_store.test_txn_store_gc3_for_cluster(&mut cluster, key.as_bytes()[0]);
}

#[test]
fn test_txn_store_rawkv() {
    let store = AssertionStorage::default();
    store.raw_get_ok("".to_string(), b"key".to_vec(), None);
    store.raw_put_ok("".to_string(), b"key".to_vec(), b"value".to_vec());
    store.raw_get_ok("".to_string(), b"key".to_vec(), Some(b"value".to_vec()));
    store.raw_put_ok("".to_string(), b"key".to_vec(), b"v2".to_vec());
    store.raw_get_ok("".to_string(), b"key".to_vec(), Some(b"v2".to_vec()));
    store.raw_delete_ok("".to_string(), b"key".to_vec());
    store.raw_get_ok("".to_string(), b"key".to_vec(), None);

    store.raw_put_ok("".to_string(), b"k1".to_vec(), b"v1".to_vec());
    store.raw_put_ok("".to_string(), b"k2".to_vec(), b"v2".to_vec());
    store.raw_put_ok("".to_string(), b"k3".to_vec(), b"v3".to_vec());
    store.raw_scan_ok("".to_string(), b"".to_vec(), None, 1, vec![(b"k1", b"v1")]);
    store.raw_scan_ok(
        "".to_string(),
        b"k1".to_vec(),
        None,
        1,
        vec![(b"k1", b"v1")],
    );
    store.raw_scan_ok(
        "".to_string(),
        b"k10".to_vec(),
        None,
        1,
        vec![(b"k2", b"v2")],
    );
    store.raw_scan_ok(
        "".to_string(),
        b"".to_vec(),
        None,
        2,
        vec![(b"k1", b"v1"), (b"k2", b"v2")],
    );
    store.raw_scan_ok(
        "".to_string(),
        b"k1".to_vec(),
        None,
        5,
        vec![(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")],
    );
    store.raw_scan_ok("".to_string(), b"".to_vec(), None, 0, vec![]);
    store.raw_scan_ok("".to_string(), b"k5".to_vec(), None, 1, vec![]);
}

#[test]
fn test_txn_store_rawkv_cf() {
    let store = AssertionStorage::default();
    store.raw_put_ok(CF_DEFAULT.to_string(), b"k1".to_vec(), b"v1".to_vec());
    store.raw_get_ok(CF_DEFAULT.to_string(), b"k1".to_vec(), Some(b"v1".to_vec()));
    store.raw_get_ok("".to_string(), b"k1".to_vec(), Some(b"v1".to_vec()));
    store.raw_get_ok(CF_LOCK.to_string(), b"k1".to_vec(), None);

    store.raw_put_ok("".to_string(), b"k2".to_vec(), b"v2".to_vec());
    store.raw_put_ok(CF_LOCK.to_string(), b"k3".to_vec(), b"v3".to_vec());
    store.raw_get_ok(CF_DEFAULT.to_string(), b"k2".to_vec(), Some(b"v2".to_vec()));
    store.raw_get_ok(CF_LOCK.to_string(), b"k3".to_vec(), Some(b"v3".to_vec()));
    store.raw_get_ok(CF_DEFAULT.to_string(), b"k3".to_vec(), None);
    store.raw_scan_ok(
        CF_DEFAULT.to_string(),
        b"".to_vec(),
        None,
        3,
        vec![(b"k1", b"v1"), (b"k2", b"v2")],
    );

    store.raw_put_err("foobar".to_string(), b"key".to_vec(), b"value".to_vec());
}

#[test]
fn test_txn_storage_keysize() {
    let store = AssertionStorage::default();
    let long_key = vec![b'x'; 10240];
    store.raw_put_ok("".to_string(), b"short_key".to_vec(), b"v".to_vec());
    store.raw_put_err("".to_string(), long_key.clone(), b"v".to_vec());
    store.raw_delete_ok("".to_string(), b"short_key".to_vec());
    store.raw_delete_err("".to_string(), long_key.clone());
    store.prewrite_ok(
        vec![Mutation::make_put(
            Key::from_raw(b"short_key"),
            b"v".to_vec(),
        )],
        b"short_key",
        1,
    );
    store.prewrite_err(
        vec![Mutation::make_put(Key::from_raw(&long_key), b"v".to_vec())],
        b"short_key",
        1,
    );
}

#[test]
fn test_txn_store_lock_primary() {
    let store = AssertionStorage::default();
    // txn1 locks "p" then aborts.
    store.prewrite_ok(
        vec![Mutation::make_put(Key::from_raw(b"p"), b"p1".to_vec())],
        b"p",
        1,
    );

    // txn2 wants to write "p", "s".
    store.prewrite_locked(
        vec![
            Mutation::make_put(Key::from_raw(b"p"), b"p2".to_vec()),
            Mutation::make_put(Key::from_raw(b"s"), b"s2".to_vec()),
        ],
        b"p",
        2,
        vec![(b"p", b"p", 1.into())],
    );
    // txn2 cleanups txn1's lock.
    store.rollback_ok(vec![b"p"], 1);
    store.resolve_lock_ok(1, None::<TimeStamp>);

    // txn3 wants to write "p", "s", neither of them should be locked.
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"p"), b"p3".to_vec()),
            Mutation::make_put(Key::from_raw(b"s"), b"s3".to_vec()),
        ],
        b"p",
        3,
    );
}

#[test]
fn test_txn_store_write_conflict() {
    let store = AssertionStorage::default();
    let key = b"key";
    let primary = b"key";
    let conflict_start_ts = 5;
    let conflict_commit_ts = 10;
    store.put_ok(key, primary, conflict_start_ts, conflict_commit_ts);
    let start_ts2 = 6;
    store.prewrite_conflict(
        vec![Mutation::make_put(Key::from_raw(key), primary.to_vec())],
        primary,
        start_ts2,
        key,
        conflict_start_ts,
    );
}

const TIDB_KEY_CASE: &[u8] = b"t_a";
const TXN_KEY_CASE: &[u8] = b"x\0_a";
const RAW_KEY_CASE: &[u8] = b"r\0_a";

// Test API version verification for txnkv requests.
// See the following for detail:
//   * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
//   * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto, enum APIVersion.
#[test]
fn test_txn_store_txnkv_api_version() {
    let test_data = vec![
        // storage api_version = V1|V1ttl, for backward compatible.
        (ApiVersion::V1, ApiVersion::V1, TIDB_KEY_CASE, true),
        (ApiVersion::V1, ApiVersion::V1, TXN_KEY_CASE, true),
        (ApiVersion::V1, ApiVersion::V1, RAW_KEY_CASE, true),
        // storage api_version = V1ttl, allow RawKV request only.
        (ApiVersion::V1ttl, ApiVersion::V1, TXN_KEY_CASE, false),
        // storage api_version = V1, reject V2 request.
        (ApiVersion::V1, ApiVersion::V2, TIDB_KEY_CASE, false),
        // storage api_version = V2.
        // backward compatible for TiDB request, and TiDB request only.
        (ApiVersion::V2, ApiVersion::V1, TIDB_KEY_CASE, true),
        (ApiVersion::V2, ApiVersion::V1, TXN_KEY_CASE, false),
        (ApiVersion::V2, ApiVersion::V1, RAW_KEY_CASE, false),
        // V2 api validation.
        (ApiVersion::V2, ApiVersion::V2, TXN_KEY_CASE, true),
        (ApiVersion::V2, ApiVersion::V2, RAW_KEY_CASE, false),
        (ApiVersion::V2, ApiVersion::V2, TIDB_KEY_CASE, false),
    ];

    for (storage_api_version, req_api_version, key, is_legal) in test_data.into_iter() {
        dispatch_api_version!(storage_api_version, {
            let mut store = AssertionStorage::<_, API>::new();
            store.ctx.set_api_version(req_api_version);

            let mut end_key = key.to_vec();
            if let Some(end_key) = end_key.last_mut() {
                *end_key = 0xff;
            }

            if is_legal {
                store.get_none(key, 10);
                store.put_ok(key, b"x", 5, 10);
                store.get_none(key, 9);
                store.get_ok(key, 10, b"x");

                store.batch_get_ok(&[key, key, key], 10, vec![b"x", b"x", b"x"]);
                store.batch_get_command_ok(&[key, key, key], 10, vec![b"x", b"x", b"x"]);

                store.scan_ok(key, Some(&end_key), 100, 10, vec![Some((key, b"x"))]);
                store.scan_locks_ok(20, key, &end_key, 10, vec![]);

                store.delete_range_ok(key, key);
            } else {
                store.get_err(key, 10);
                // check api version at service tier: store.put_err(key, b"x", 5, 10);
                store.batch_get_err(&[key, key, key], 10);
                store.batch_get_command_err(&[key, key, key], 10);

                store.scan_err(key, None, 100, 10);

                // To compatible with TiDB gc-worker, we remove check_api_version_ranges in scan_lock
                store.scan_locks_ok(20, key, &end_key, 10, vec![]);

                store.delete_range_err(key, key);
            }
        });
    }
}

// Test API version verification for rawkv requests.
// See the following for detail:
//   * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
//   * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto, enum APIVersion.
#[test]
fn test_txn_store_rawkv_api_version() {
    let test_data = vec![
        // storage api_version = V1|V1ttl, for backward compatible.
        (ApiVersion::V1, ApiVersion::V1, TIDB_KEY_CASE, true),
        (ApiVersion::V1, ApiVersion::V1, TXN_KEY_CASE, true),
        (ApiVersion::V1, ApiVersion::V1, RAW_KEY_CASE, true),
        (ApiVersion::V1ttl, ApiVersion::V1, RAW_KEY_CASE, true),
        // storage api_version = V1, reject V2 request.
        (ApiVersion::V1, ApiVersion::V2, RAW_KEY_CASE, false),
        // storage api_version = V2.
        // backward compatible for TiDB request, and TiDB (txnkv) request only.
        (ApiVersion::V2, ApiVersion::V1, TIDB_KEY_CASE, false),
        (ApiVersion::V2, ApiVersion::V1, TXN_KEY_CASE, false),
        (ApiVersion::V2, ApiVersion::V1, RAW_KEY_CASE, false),
        // V2 api validation.
        (ApiVersion::V2, ApiVersion::V2, TXN_KEY_CASE, false),
        (ApiVersion::V2, ApiVersion::V2, RAW_KEY_CASE, true),
        (ApiVersion::V2, ApiVersion::V2, TIDB_KEY_CASE, false),
    ];

    let cf = "";

    for (storage_api_version, req_api_version, key, is_legal) in test_data.into_iter() {
        dispatch_api_version!(storage_api_version, {
            let mut store = AssertionStorage::<_, API>::new();
            store.ctx.set_api_version(req_api_version);

            let mut end_key = key.to_vec();
            if let Some(last_byte) = end_key.last_mut() {
                *last_byte = 0xff;
            }

            let mut range = KeyRange::default();
            range.set_start_key(key.to_vec());

            let mut range_bounded = KeyRange::default();
            range_bounded.set_start_key(key.to_vec());
            range_bounded.set_end_key(end_key.clone());

            if is_legal {
                store.raw_get_ok(cf.to_owned(), key.to_vec(), None);
                store.raw_put_ok(cf.to_owned(), key.to_vec(), b"value".to_vec());
                store.raw_get_ok(cf.to_owned(), key.to_vec(), Some(b"value".to_vec()));
                if !matches!(storage_api_version, ApiVersion::V1) {
                    store.raw_get_key_ttl_ok(cf.to_owned(), key.to_vec(), Some(0));
                }

                store.raw_batch_get_ok(
                    cf.to_owned(),
                    vec![key.to_vec(), key.to_vec()],
                    vec![(key, b"value"), (key, b"value")],
                );
                store.raw_batch_get_command_ok(
                    cf.to_owned(),
                    vec![key.to_vec(), key.to_vec()],
                    vec![b"value", b"value"],
                );

                store.raw_delete_ok(cf.to_owned(), key.to_vec());
                store.raw_delete_range_ok(cf.to_owned(), key.to_vec(), key.to_vec());
                store.raw_batch_delete_ok(cf.to_owned(), vec![key.to_vec()]);

                store.raw_batch_put_ok(cf.to_owned(), vec![(key.to_vec(), b"value".to_vec())]);

                store.raw_scan_ok(
                    cf.to_owned(),
                    key.to_vec(),
                    Some(end_key.clone()),
                    100,
                    vec![(key, b"value")],
                );
                store.raw_batch_scan_ok(
                    cf.to_owned(),
                    vec![range_bounded.clone()],
                    100,
                    vec![(key, b"value")],
                );
                {
                    // unbounded end key
                    match storage_api_version {
                        ApiVersion::V1 | ApiVersion::V1ttl => {
                            store.raw_scan_ok(
                                cf.to_owned(),
                                key.to_vec(),
                                None,
                                100,
                                vec![(key, b"value")],
                            );
                            store.raw_batch_scan_ok(
                                cf.to_owned(),
                                vec![range.clone()],
                                100,
                                vec![(key, b"value")],
                            );
                        }
                        ApiVersion::V2 => {
                            // unbounded key is prohibitted in V2.
                            store.raw_scan_err(cf.to_owned(), key.to_vec(), None, 100);
                            store.raw_batch_scan_err(cf.to_owned(), vec![range.clone()], 100);
                        }
                    }
                }

                store.raw_compare_and_swap_atomic_ok(
                    cf.to_owned(),
                    key.to_vec(),
                    Some(b"value".to_vec()),
                    b"new_value".to_vec(),
                    (Some(b"value".to_vec()), true),
                );

                store.raw_batch_delete_atomic_ok(cf.to_owned(), vec![key.to_vec()]);
                store.raw_batch_put_atomic_ok(
                    cf.to_owned(),
                    vec![(key.to_vec(), b"value".to_vec())],
                );

                let digest = crc64fast::Digest::new();
                let checksum = checksum_crc64_xor(0, digest.clone(), key, b"value");
                store.raw_checksum_ok(
                    vec![range_bounded.clone()],
                    (checksum, 1, (key.len() + b"value".len()) as u64),
                );
            } else {
                store.raw_get_err(cf.to_owned(), key.to_vec());
                if !matches!(storage_api_version, ApiVersion::V1) {
                    store.raw_get_key_ttl_err(cf.to_owned(), key.to_vec());
                }
                store.raw_put_err(cf.to_owned(), key.to_vec(), b"value".to_vec());

                store.raw_batch_get_err(cf.to_owned(), vec![key.to_vec(), key.to_vec()]);
                store.raw_batch_get_command_err(cf.to_owned(), vec![key.to_vec(), key.to_vec()]);

                store.raw_delete_err(cf.to_owned(), key.to_vec());
                store.raw_delete_range_err(cf.to_owned(), key.to_vec(), key.to_vec());
                store.raw_batch_delete_err(cf.to_owned(), vec![key.to_vec()]);

                store.raw_batch_put_err(cf.to_owned(), vec![(key.to_vec(), b"value".to_vec())]);

                store.raw_scan_err(cf.to_owned(), key.to_vec(), Some(end_key.clone()), 100);
                store.raw_batch_scan_err(cf.to_owned(), vec![range_bounded.clone()], 100);

                store.raw_compare_and_swap_atomic_err(
                    cf.to_owned(),
                    key.to_vec(),
                    None,
                    b"value".to_vec(),
                );

                store.raw_batch_delete_atomic_err(cf.to_owned(), vec![key.to_vec()]);
                store.raw_batch_put_atomic_err(
                    cf.to_owned(),
                    vec![(key.to_vec(), b"value".to_vec())],
                );

                store.raw_checksum_err(vec![range_bounded.clone()]);
            }
        });
    }
}

struct Oracle {
    ts: AtomicUsize,
}

impl Oracle {
    fn new() -> Oracle {
        Oracle {
            ts: AtomicUsize::new(1_usize),
        }
    }

    fn get_ts(&self) -> TimeStamp {
        (self.ts.fetch_add(1, Ordering::Relaxed) as u64).into()
    }
}

const INC_MAX_RETRY: usize = 100;

fn inc<E: Engine, F: KvFormat>(
    store: &SyncTestStorage<E, F>,
    oracle: &Oracle,
    key: &[u8],
) -> Result<i32, ()> {
    let key_address = Key::from_raw(key);
    for i in 0..INC_MAX_RETRY {
        let start_ts = oracle.get_ts();
        let number: i32 = match store.get(Context::default(), &key_address, start_ts) {
            Ok((Some(x), ..)) => String::from_utf8(x).unwrap().parse().unwrap(),
            Ok((None, ..)) => 0,
            Err(_) => {
                backoff(i);
                continue;
            }
        };
        let next = number + 1;
        if store
            .prewrite(
                Context::default(),
                vec![Mutation::make_put(
                    Key::from_raw(key),
                    next.to_string().into_bytes(),
                )],
                key.to_vec(),
                start_ts,
            )
            .is_err()
        {
            backoff(i);
            continue;
        }
        let commit_ts = oracle.get_ts();
        if store
            .commit(
                Context::default(),
                vec![key_address.clone()],
                start_ts,
                commit_ts,
            )
            .is_err()
        {
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
        let (punch_card, store, oracle) =
            (Arc::clone(&punch_card), store.clone(), Arc::clone(&oracle));
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
    assert_eq!(
        inc(&store.store, &oracle, b"key").unwrap() as usize,
        THREAD_NUM * INC_PER_THREAD
    );
}

fn format_key(x: usize) -> Vec<u8> {
    format!("k{}", x).into_bytes()
}

fn inc_multi<E: Engine, F: KvFormat>(
    store: &SyncTestStorage<E, F>,
    oracle: &Oracle,
    n: usize,
) -> bool {
    'retry: for i in 0..INC_MAX_RETRY {
        let start_ts = oracle.get_ts();
        let keys: Vec<Key> = (0..n).map(format_key).map(|x| Key::from_raw(&x)).collect();
        let mut mutations = vec![];
        for key in keys.iter().take(n) {
            let number = match store.get(Context::default(), key, start_ts) {
                Ok((Some(n), ..)) => String::from_utf8(n).unwrap().parse().unwrap(),
                Ok((None, ..)) => 0,
                Err(_) => {
                    backoff(i);
                    continue 'retry;
                }
            };
            let next = number + 1;
            mutations.push(Mutation::make_put(
                key.clone(),
                next.to_string().into_bytes(),
            ));
        }
        if store
            .prewrite(Context::default(), mutations, b"k0".to_vec(), start_ts)
            .is_err()
        {
            backoff(i);
            continue;
        }
        let commit_ts = oracle.get_ts();
        if store
            .commit(Context::default(), keys, start_ts, commit_ts)
            .is_err()
        {
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
        0..=6 => 2u64.pow(attempts as u32),
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
        let (store, oracle) = (store.clone(), Arc::clone(&oracle));
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
        assert_eq!(
            inc(&store.store, &oracle, &format_key(n)).unwrap() as usize,
            THREAD_NUM * INC_PER_THREAD
        );
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
