// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::{Context, IsolationLevel};
use tidb_query_common::storage::{IntervalRange, Storage};
use tikv::{
    coprocessor::dag::TikvStorage,
    storage::{Engine, Scanner, Snapshot, SnapshotStore, Statistics, Store, TestEngineBuilder},
};
use tikv_kv::{Modify, WriteData};
use txn_types::{Key, Lock, LockType, TimeStamp, ValueEntry, Write, WriteType};

type Row = (Vec<u8>, Vec<u8>, Option<u64>);

fn put(data: &mut Vec<Modify>, key: &[u8], start: u64, commit: u64, value: &[u8]) {
    data.push(Modify::Put(
        CF_DEFAULT,
        Key::from_raw(key).append_ts(start.into()),
        value.to_vec(),
    ));
    data.push(Modify::Put(
        CF_WRITE,
        Key::from_raw(key).append_ts(commit.into()),
        Write::new(WriteType::Put, start.into(), None)
            .as_ref()
            .to_bytes(),
    ));
}

fn lock(data: &mut Vec<Modify>, key: &[u8]) {
    data.push(Modify::Put(
        CF_LOCK,
        Key::from_raw(key),
        Lock::new(
            LockType::Put,
            key.to_vec(),
            3.into(),
            100,
            Some(b"uncommitted".to_vec()),
            TimeStamp::zero(),
            1,
            TimeStamp::zero(),
            false,
        )
        .to_bytes(),
    ));
}

fn store<S: Snapshot>(snapshot: S, check_newer: bool) -> SnapshotStore<S> {
    SnapshotStore::new(
        snapshot,
        10.into(),
        IsolationLevel::Si,
        true,
        Default::default(),
        Default::default(),
        check_newer,
    )
}

fn row(key: Key, value: ValueEntry) -> Row {
    (
        key.into_raw().unwrap(),
        value.value,
        value.commit_ts.map(TimeStamp::into_inner),
    )
}

fn expected<S: Snapshot>(
    snapshot: S,
    lower: &[u8],
    upper: &[u8],
    backward: bool,
    key_only: bool,
    commit_ts: bool,
) -> Vec<Row> {
    let mut scanner = store(snapshot, false)
        .scanner(
            backward,
            key_only,
            false,
            commit_ts,
            Some(Key::from_raw(lower)),
            Some(Key::from_raw(upper)),
        )
        .unwrap();
    let mut rows = Vec::new();
    while let Some((key, value)) = scanner.next_entry().unwrap() {
        rows.push(row(key, value));
    }
    rows
}

#[test]
fn test_scanner_reuse_matches_fresh_bounded_scans_and_statistics() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let mut data = Vec::new();
    put(&mut data, b"b", 1, 2, &[b'b'; 128]);
    put(&mut data, b"b", 19, 20, b"future");
    put(&mut data, b"h", 2, 3, &[b'h'; 256]);
    put(&mut data, b"j", 1, 2, b"deleted");
    data.push(Modify::Put(
        CF_WRITE,
        Key::from_raw(b"j").append_ts(4.into()),
        Write::new(WriteType::Delete, 3.into(), None)
            .as_ref()
            .to_bytes(),
    ));
    data.push(Modify::Put(
        CF_WRITE,
        Key::from_raw(b"n").append_ts(4.into()),
        Write::new(WriteType::Rollback, 4.into(), None)
            .as_ref()
            .to_bytes(),
    ));
    put(&mut data, b"t", 4, 5, &[b't'; 512]);
    lock(&mut data, b"e");
    lock(&mut data, b"x");
    engine
        .write(&Context::default(), WriteData::from_modifies(data))
        .unwrap();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let ranges: &[(&[u8], &[u8])] = &[
        (b"a", b"d"),
        (b"f", b"g"),
        (b"h", b"i"),
        (b"i", b"m"),
        (b"m", b"o"),
        (b"p", b"u"),
        (b"u", b"v"),
    ];
    for backward in [false, true] {
        let mut storage = TikvStorage::new(store(snapshot.clone(), false), false);
        let mut stats = Statistics::default();
        let mut total_size = 0;
        for i in 0..ranges.len() {
            let (lower, upper) = ranges[if backward { ranges.len() - 1 - i } else { i }];
            let wanted = expected(snapshot.clone(), lower, upper, backward, false, true);
            storage
                .begin_scan(
                    backward,
                    false,
                    true,
                    IntervalRange::from((lower.to_vec(), upper.to_vec())),
                )
                .unwrap();
            let mut actual = Vec::new();
            while let Some(entry) = storage.scan_next_entry().unwrap() {
                total_size += Key::from_raw(&entry.key).len() + entry.value.len();
                actual.push((entry.key, entry.value, entry.commit_ts));
            }
            assert_eq!(actual, wanted);
            assert!(storage.scan_next_entry().unwrap().is_none());
            storage.collect_statistics(&mut stats);
            assert_eq!(storage.scanned_bytes(), total_size);
        }
        assert_eq!(stats.write.processed_keys, 3);
        assert_eq!(stats.processed_size, total_size);
        let mut drained = Statistics::default();
        storage.collect_statistics(&mut drained);
        assert_eq!(drained.processed_size, 0);
    }
}

#[test]
fn test_scanner_reuse_resets_order_options_and_partial_scans() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let mut data = Vec::new();
    for key in [b"b", b"h", b"t"] {
        put(&mut data, key, 1, 2, &[key[0]; 128]);
    }
    engine
        .write(&Context::default(), WriteData::from_modifies(data))
        .unwrap();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let mut storage = TikvStorage::new(store(snapshot.clone(), false), false);
    let scans: &[(&[u8], &[u8], bool, bool, bool)] = &[
        (b"a", b"u", false, false, false),
        (b"f", b"i", false, false, false),
        (b"d", b"d", false, false, false),
        (b"a", b"d", false, false, true),
        (b"p", b"u", false, true, true),
        (b"a", b"u", true, true, true),
        (b"a", b"i", true, false, false),
        (b"p", b"u", true, false, false),
    ];
    for &(lower, upper, backward, key_only, commit_ts) in scans {
        let wanted = expected(
            snapshot.clone(),
            lower,
            upper,
            backward,
            key_only,
            commit_ts,
        );
        storage
            .begin_scan(
                backward,
                key_only,
                commit_ts,
                IntervalRange::from((lower.to_vec(), upper.to_vec())),
            )
            .unwrap();
        let mut actual = Vec::new();
        while let Some(entry) = storage.scan_next_entry().unwrap() {
            actual.push((entry.key, entry.value, entry.commit_ts));
        }
        assert_eq!(actual, wanted);
    }
    storage
        .begin_scan(false, false, true, IntervalRange::from(("a", "u")))
        .unwrap();
    assert_eq!(storage.scan_next_entry().unwrap().unwrap().key, b"b");
    storage
        .begin_scan(false, false, true, IntervalRange::from(("a", "d")))
        .unwrap();
    assert_eq!(storage.scan_next_entry().unwrap().unwrap().key, b"b");
    assert!(storage.scan_next_entry().unwrap().is_none());
}

#[test]
fn test_scanner_reuse_cacheability_ignores_gaps_and_remains_sticky() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let mut data = Vec::new();
    put(&mut data, b"b", 1, 2, b"visible");
    put(&mut data, b"z", 19, 20, b"future");
    lock(&mut data, b"m");
    engine
        .write(&Context::default(), WriteData::from_modifies(data))
        .unwrap();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    for backward in [false, true] {
        let mut storage = TikvStorage::new(store(snapshot.clone(), true), true);
        let ranges = if backward {
            [("p", "y"), ("d", "f"), ("a", "d")]
        } else {
            [("a", "d"), ("d", "f"), ("p", "y")]
        };
        for range in ranges {
            storage
                .begin_scan(backward, false, false, IntervalRange::from(range))
                .unwrap();
            while storage.scan_next_entry().unwrap().is_some() {}
            assert_eq!(storage.met_uncacheable_data(), Some(false));
        }
        storage
            .begin_scan(backward, false, false, IntervalRange::from(("y", "zz")))
            .unwrap();
        assert!(storage.scan_next_entry().unwrap().is_none());
        assert_eq!(storage.met_uncacheable_data(), Some(true));
        storage
            .begin_scan(backward, false, false, IntervalRange::from(("f", "g")))
            .unwrap();
        assert!(storage.scan_next_entry().unwrap().is_none());
        assert_eq!(storage.met_uncacheable_data(), Some(true));
    }
}

#[test]
fn test_scanner_reuse_does_not_decode_gap_records() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let mut data = Vec::new();
    for key in [b"b", b"h", b"t"] {
        put(&mut data, key, 1, 2, key);
    }
    data.push(Modify::Put(CF_WRITE, Key::from_encoded(vec![b'e']), vec![]));
    engine
        .write(&Context::default(), WriteData::from_modifies(data))
        .unwrap();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    for backward in [false, true] {
        let mut storage = TikvStorage::new(store(snapshot.clone(), false), false);
        let ranges = if backward {
            [("p", "u"), ("f", "i"), ("a", "d")]
        } else {
            [("a", "d"), ("f", "i"), ("p", "u")]
        };
        for (lower, upper) in ranges {
            let wanted = expected(
                snapshot.clone(),
                lower.as_bytes(),
                upper.as_bytes(),
                backward,
                false,
                false,
            );
            storage
                .begin_scan(backward, false, false, IntervalRange::from((lower, upper)))
                .unwrap();
            let mut actual = Vec::new();
            while let Some(entry) = storage.scan_next_entry().unwrap() {
                actual.push((entry.key, entry.value, entry.commit_ts));
            }
            assert_eq!(actual, wanted);
        }
    }
}

#[test]
fn test_scanner_reuse_enables_newer_timestamp_check() {
    use tikv::storage::mvcc::NewerTsCheckState;
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let mut data = Vec::new();
    put(&mut data, b"b", 1, 2, b"b");
    put(&mut data, b"h", 1, 2, b"h");
    put(&mut data, b"z", 19, 20, b"future");
    engine
        .write(&Context::default(), WriteData::from_modifies(data))
        .unwrap();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    let store = store(snapshot, false);
    let mut previous = None;
    for (lower, upper, check) in [("a", "d", false), ("f", "i", false), ("y", "zz", true)] {
        let mut scanner = store
            .scanner_with_reuse(
                previous.take(),
                false,
                false,
                check,
                false,
                Some(Key::from_raw(lower.as_bytes())),
                Some(Key::from_raw(upper.as_bytes())),
                &mut Statistics::default(),
            )
            .unwrap();
        while scanner.next_entry().unwrap().is_some() {}
        assert_eq!(
            scanner.met_newer_ts_data(),
            if check {
                NewerTsCheckState::Met
            } else {
                NewerTsCheckState::Unknown
            }
        );
        previous = Some(scanner);
    }
}
