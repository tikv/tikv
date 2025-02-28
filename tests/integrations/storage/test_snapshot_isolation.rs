// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use test_storage::*;
use txn_types::{Key, Mutation};

/// Tests that a transaction cannot read values that were written after its start timestamp.
/// This verifies the snapshot isolation property that each transaction reads from a consistent
/// snapshot taken at its start time.
#[test]
fn test_snapshot_isolation_read_consistency() {
    let store = AssertionStorage::default();
    
    // Initial value
    store.put_ok(b"key", b"value1", 1, 2);

    // T1: Read at ts=5
    store.get_ok(b"key", 5, b"value1");

    // T2: Write new value
    store.put_ok(b"key", b"value2", 6, 7);

    // T1: Should still read old value from its snapshot
    store.get_ok(b"key", 5, b"value1");
}

/// Tests that concurrent transactions writing to the same key will have one commit
/// and one abort due to write conflict detection.
#[test]
fn test_snapshot_isolation_write_conflict() {
    let store = AssertionStorage::default();
    
    // Initial value
    store.put_ok(b"key", b"value1", 1, 2);

    // T1: Start write transaction
    store.prewrite_ok(
        vec![Mutation::make_put(Key::from_raw(b"key"), b"value2".to_vec())],
        b"key",
        5,
    );

    // T2: Try to write same key - should get a lock error
    store.prewrite_locked(
        vec![Mutation::make_put(Key::from_raw(b"key"), b"value3".to_vec())],
        b"key",
        6,
        vec![(b"key", b"key", 5.into())],
    );

    // T1: Commit should succeed
    store.commit_ok(vec![b"key"], 5, 7, 7);

    // Verify final value
    store.get_ok(b"key", 8, b"value2");
}

/// Tests that a transaction reading a key will still see a consistent snapshot
/// even if concurrent transactions are modifying other keys
#[test]
fn test_snapshot_isolation_with_multiple_keys() {
    let store = AssertionStorage::default();
    
    // Initial values
    store.put_ok(b"key1", b"value1", 1, 2);
    store.put_ok(b"key2", b"value2", 1, 2);

    // T1: Start read transaction
    let read_ts = 5;
    store.get_ok(b"key1", read_ts, b"value1");

    // T2: Update key2
    store.put_ok(b"key2", b"value2-new", 6, 7);

    // T1: Should still read original values
    store.get_ok(b"key1", read_ts, b"value1");
    store.get_ok(b"key2", read_ts, b"value2");

    // Verify final values at a later timestamp
    store.get_ok(b"key1", 8, b"value1");
    store.get_ok(b"key2", 8, b"value2-new");
}

/// Tests that write skew anomaly is prevented under snapshot isolation.
/// Write skew could occur if two transactions read overlapping data,
/// make disjoint updates, and commit in a way that violates a constraint
/// that was true at the start of both transactions
#[test]
fn test_snapshot_isolation_prevent_write_skew() {
    let store = AssertionStorage::default();
    
    // Initial values - constraint: sum must be >= 100
    store.put_ok(b"x", b"60", 1, 2);
    store.put_ok(b"y", b"60", 1, 2);

    // T1: Read both values and update x
    store.prewrite_ok(
        vec![
            Mutation::make_put(Key::from_raw(b"x"), b"0".to_vec()),
            // Lock y to prevent write skew
            Mutation::make_lock(Key::from_raw(b"y")),
        ],
        b"x",
        5,
    );

    // T2: Try to update y - should fail due to T1's lock
    store.prewrite_locked(
        vec![Mutation::make_put(Key::from_raw(b"y"), b"0".to_vec())],
        b"y",
        6,
        vec![(b"y", b"x", 5.into())],
    );

    // T1: Commit
    store.commit_ok(vec![b"x", b"y"], 5, 7, 7);

    // Verify final values - x was updated, y keeps original value
    store.get_ok(b"x", 8, b"0");
    store.get_ok(b"y", 8, b"60");
}
