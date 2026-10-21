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

fn assert_same_statistics(actual: &Statistics, expected: &Statistics) {
    assert_eq!(actual.processed_size, expected.processed_size);
    for (cf, actual, expected) in [
        (CF_LOCK, &actual.lock, &expected.lock),
        (CF_WRITE, &actual.write, &expected.write),
        (CF_DEFAULT, &actual.data, &expected.data),
    ] {
        assert_eq!(actual.details(), expected.details(), "{cf}");
        assert_eq!(
            actual.flow_stats.read_keys, expected.flow_stats.read_keys,
            "{cf}"
        );
        assert_eq!(
            actual.flow_stats.read_bytes, expected.flow_stats.read_bytes,
            "{cf}"
        );
        assert_eq!(actual.block_read_count, expected.block_read_count, "{cf}");
    }
}

#[test]
fn test_scanner_reuse_region_cache_fallback_preserves_snapshot_bounds_and_statistics() {
    use std::sync::Arc;

    use engine_rocks::{RocksEngine, RocksSnapshot, util::new_engine};
    use engine_traits::{
        CacheRegion, DATA_CFS, IterOptions, Iterable, Iterator, KvEngine, Mutable,
        RegionCacheEngine, Snapshot as EngineSnapshot, SnapshotMiscExt, WriteBatch, WriteBatchExt,
    };
    use hybrid_engine::{HybridEngineSnapshot, observer::RegionCacheWriteBatchObserver};
    use in_memory_engine::{
        InMemoryEngineConfig, InMemoryEngineContext, RegionCacheMemoryEngine, test_util::new_region,
    };
    use raftstore::{
        coprocessor::{WriteBatchObserver, WriteBatchWrapper},
        store::RegionSnapshot,
    };
    use tikv_util::config::VersionTrack;

    let path = tempfile::Builder::new()
        .prefix("scanner-reuse-cache")
        .tempdir()
        .unwrap();
    let disk = new_engine(path.path().to_str().unwrap(), DATA_CFS).unwrap();
    let cache = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(InMemoryEngineConfig::config_for_test()),
    )));
    let region = new_region(
        1,
        Key::from_raw(b"b").into_encoded(),
        Key::from_raw(b"y").into_encoded(),
    );
    cache.new_region(region.clone());
    assert!(cache.core().region_manager().set_safe_point(region.id, 1));
    let cache_region = CacheRegion::from_region(&region);

    // Real disk records outside the region must never become visible through
    // RegionSnapshot, including at its exclusive upper endpoint.
    let mut outside = Vec::new();
    put(&mut outside, b"a", 1, 2, b"before region");
    put(&mut outside, b"y", 1, 2, b"after region");
    let mut batch = disk.write_batch();
    for modify in outside {
        let Modify::Put(cf, key, value) = modify else {
            unreachable!()
        };
        batch
            .put_cf(cf, &keys::data_key(key.as_encoded()), &value)
            .unwrap();
    }
    batch.write().unwrap();

    // Exercise the normal write observer and its RocksDB sequence-number
    // handoff, rather than filling the cache independently from the disk.
    let write = |modifies: Vec<Modify>| {
        let observer = RegionCacheWriteBatchObserver::new(cache.clone());
        let mut batch = WriteBatchWrapper::new(
            disk.write_batch(),
            Some(observer.create_observable_write_batch()),
        );
        batch.prepare_for_region(&region);
        for modify in modifies {
            let Modify::Put(cf, key, value) = modify else {
                unreachable!()
            };
            batch
                .put_cf(cf, &keys::data_key(key.as_encoded()), &value)
                .unwrap();
        }
        batch.write().unwrap();
    };
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
    write(data);

    let disk_snapshot = disk.snapshot();
    let sequence_number = disk_snapshot.sequence_number();
    let cache_snapshot = cache
        .snapshot(cache_region.clone(), 10, sequence_number)
        .unwrap();
    assert_eq!(cache_snapshot.sequence_number(), sequence_number);
    let hybrid = HybridEngineSnapshot::<RocksEngine, RegionCacheMemoryEngine>::new(
        disk_snapshot,
        Some(cache_snapshot),
    );
    assert!(hybrid.region_cache_snapshot_available());
    assert!(hybrid.in_memory_engine_hit());

    // All MVCC CFs use the real cache branch. An unsupported reset must leave
    // its iterator at its old record, even when that record is outside the
    // requested new range.
    for cf in DATA_CFS {
        let mut options = IterOptions::default();
        options.set_lower_bound(&cache_region.start, 0);
        options.set_upper_bound(&cache_region.end, 0);
        let mut iter = hybrid.iterator_opt(cf, options).unwrap();
        assert!(iter.seek_to_first().unwrap(), "{cf}");
        let previous = (iter.key().to_vec(), iter.value().to_vec());
        assert!(
            !iter
                .reset_bounds_and_seek(
                    &keys::data_key(Key::from_raw(b"h").as_encoded()),
                    &keys::data_key(Key::from_raw(b"i").as_encoded()),
                    false,
                )
                .unwrap()
        );
        assert!(iter.valid().unwrap());
        assert_eq!(
            (iter.key(), iter.value()),
            (previous.0.as_slice(), previous.1.as_slice())
        );
    }

    let snapshot = RegionSnapshot::from_snapshot(Arc::new(hybrid), Arc::new(region.clone()));
    let disk_snapshot = RegionSnapshot::<RocksSnapshot>::from_raw(disk.clone(), region.clone());
    assert_eq!(
        disk_snapshot.get_snapshot().sequence_number(),
        sequence_number
    );

    // These commits are below read_ts=10, but above the pinned physical
    // sequence number. Recreating a scanner must retain both snapshot fences.
    let mut later = Vec::new();
    put(&mut later, b"b", 7, 8, b"written after snapshot");
    put(&mut later, b"f", 3, 4, b"inserted after snapshot");
    later.push(Modify::Put(
        CF_WRITE,
        Key::from_raw(b"h").append_ts(9.into()),
        Write::new(WriteType::Delete, 8.into(), None)
            .as_ref()
            .to_bytes(),
    ));
    write(later);
    assert!(disk.snapshot().sequence_number() > sequence_number);
    let current = RegionSnapshot::<RocksSnapshot>::from_raw(disk.clone(), region.clone());
    assert!(expected(current.clone(), b"h", b"i", false, false, true).is_empty());
    assert_eq!(expected(current, b"f", b"g", false, false, true).len(), 1);

    let ranges: &[(&[u8], &[u8])] = &[
        (b"b", b"d"),
        (b"f", b"g"),
        (b"h", b"i"),
        (b"i", b"m"),
        (b"m", b"o"),
        (b"p", b"u"),
        (b"u", b"y"),
    ];
    for backward in [false, true] {
        let mut storage = TikvStorage::new(store(snapshot.clone(), false), false);
        let fresh_store = store(snapshot.clone(), false);
        let mut actual_statistics = Statistics::default();
        let mut fresh_statistics = Statistics::default();
        let mut total_size = 0;
        for i in 0..ranges.len() {
            let (lower, upper) = ranges[if backward { ranges.len() - 1 - i } else { i }];
            let wanted = match lower {
                b"b" => vec![(b"b".to_vec(), vec![b'b'; 128], Some(2))],
                b"h" => vec![(b"h".to_vec(), vec![b'h'; 256], Some(3))],
                b"p" => vec![(b"t".to_vec(), vec![b't'; 512], Some(5))],
                _ => Vec::new(),
            };
            assert_eq!(
                expected(disk_snapshot.clone(), lower, upper, backward, false, true),
                wanted
            );
            let mut fresh = fresh_store
                .scanner(
                    backward,
                    false,
                    false,
                    true,
                    Some(Key::from_raw(lower)),
                    Some(Key::from_raw(upper)),
                )
                .unwrap();
            let mut fresh_rows = Vec::new();
            while let Some((key, value)) = fresh.next_entry().unwrap() {
                fresh_rows.push(row(key, value));
            }
            assert_eq!(fresh_rows, wanted);
            fresh_statistics.add(&fresh.take_statistics());

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
            assert_eq!(storage.scanned_bytes(), total_size);
            // Also leave statistics undrained over range switches: begin_scan
            // must preserve them when the cache refuses cursor reuse.
            if i % 2 == 1 || i + 1 == ranges.len() {
                storage.collect_statistics(&mut actual_statistics);
                assert_same_statistics(&actual_statistics, &fresh_statistics);
            }
        }
        assert_eq!(actual_statistics.write.processed_keys, 3);
        assert_eq!(actual_statistics.processed_size, total_size);
        let mut drained = Statistics::default();
        storage.collect_statistics(&mut drained);
        assert_same_statistics(&drained, &Statistics::default());

        // A region-bound error during a subsequent begin_scan must preserve
        // the partial scan's counters once, then permit a valid fresh scan.
        storage
            .begin_scan(backward, false, true, IntervalRange::from(("h", "i")))
            .unwrap();
        assert_eq!(
            storage.scan_next_entry().unwrap().unwrap().value,
            vec![b'h'; 256]
        );
        let invalid = if backward { ("u", "z") } else { ("a", "d") };
        assert!(
            storage
                .begin_scan(backward, false, true, IntervalRange::from(invalid))
                .is_err()
        );
        let mut partial = fresh_store
            .scanner(
                backward,
                false,
                false,
                true,
                Some(Key::from_raw(b"h")),
                Some(Key::from_raw(b"i")),
            )
            .unwrap();
        assert!(partial.next_entry().unwrap().is_some());
        let mut error_statistics = Statistics::default();
        storage.collect_statistics(&mut error_statistics);
        assert_same_statistics(&error_statistics, &partial.take_statistics());
        let mut drained = Statistics::default();
        storage.collect_statistics(&mut drained);
        assert_same_statistics(&drained, &Statistics::default());

        // Cache fallback must continue to surface a real MVCC lock conflict.
        storage
            .begin_scan(backward, false, true, IntervalRange::from(("b", "d")))
            .unwrap();
        assert!(storage.scan_next_entry().unwrap().is_some());
        storage
            .begin_scan(backward, false, true, IntervalRange::from(("d", "f")))
            .unwrap();
        assert!(storage.scan_next_entry().is_err());
        storage
            .begin_scan(backward, false, true, IntervalRange::from(("h", "i")))
            .unwrap();
        assert_eq!(
            storage.scan_next_entry().unwrap().unwrap().value,
            vec![b'h'; 256]
        );
        assert!(storage.scan_next_entry().unwrap().is_none());
    }
}
