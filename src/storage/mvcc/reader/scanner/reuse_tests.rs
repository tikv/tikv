// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, CfName, IterOptions, MetricsExt, ReadOptions};
use tidb_query_common::storage::{IntervalRange, Storage as QueryStorage};

use super::*;
use crate::{
    coprocessor::dag::TikvStorage,
    storage::{
        kv::{self, BTreeEngine, Engine, TestEngineBuilder},
        txn::{SnapshotStore, Store, tests::*},
    },
};

#[derive(Clone)]
struct CountingSnapshot<S> {
    inner: S,
    iterators: Arc<AtomicUsize>,
    reject_reset_cf: Option<CfName>,
}

impl<S: Snapshot> CountingSnapshot<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            iterators: Arc::new(AtomicUsize::new(0)),
            reject_reset_cf: None,
        }
    }

    fn iterator_count(&self) -> usize {
        self.iterators.load(Ordering::Relaxed)
    }
}

impl<S: Snapshot> Snapshot for CountingSnapshot<S> {
    type Iter = ResetSupportIterator<S::Iter>;
    type Ext<'a>
        = S::Ext<'a>
    where
        Self: 'a;

    fn get(&self, key: &Key) -> kv::Result<Option<Value>> {
        self.inner.get(key)
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        self.inner.get_cf(cf, key)
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        self.inner.get_cf_opt(opts, cf, key)
    }

    fn iter(&self, cf: CfName, opts: IterOptions) -> kv::Result<Self::Iter> {
        self.iterators.fetch_add(1, Ordering::Relaxed);
        Ok(ResetSupportIterator {
            inner: self.inner.iter(cf, opts)?,
            reject_reset: self.reject_reset_cf == Some(cf),
        })
    }

    fn lower_bound(&self) -> Option<&[u8]> {
        self.inner.lower_bound()
    }

    fn upper_bound(&self) -> Option<&[u8]> {
        self.inner.upper_bound()
    }

    fn ext(&self) -> Self::Ext<'_> {
        self.inner.ext()
    }
}

struct ResetSupportIterator<I> {
    inner: I,
    reject_reset: bool,
}

impl<I: Iterator> MetricsExt for ResetSupportIterator<I> {
    type Collector = I::Collector;

    fn metrics_collector(&self) -> Self::Collector {
        self.inner.metrics_collector()
    }
}

impl<I: Iterator> Iterator for ResetSupportIterator<I> {
    fn reset_bounds_and_seek(
        &mut self,
        lower: &Key,
        upper: &Key,
        reverse: bool,
    ) -> kv::Result<bool> {
        if self.reject_reset {
            Ok(false)
        } else {
            self.inner.reset_bounds_and_seek(lower, upper, reverse)
        }
    }

    fn next(&mut self) -> kv::Result<bool> {
        self.inner.next()
    }

    fn prev(&mut self) -> kv::Result<bool> {
        self.inner.prev()
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        self.inner.seek(key)
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
        self.inner.seek_for_prev(key)
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        self.inner.seek_to_first()
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        self.inner.seek_to_last()
    }

    fn valid(&self) -> kv::Result<bool> {
        self.inner.valid()
    }

    fn validate_key(&self, key: &Key) -> kv::Result<()> {
        self.inner.validate_key(key)
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }
}

fn bounds(lower: &[u8], upper: &[u8]) -> (Option<Key>, Option<Key>) {
    (Some(Key::from_raw(lower)), Some(Key::from_raw(upper)))
}

fn collect<S: Snapshot>(
    scanner: &mut Scanner<S>,
) -> Vec<std::result::Result<(Vec<u8>, ValueEntry), String>> {
    let mut result = Vec::new();
    loop {
        match scanner.next_entry() {
            Ok(Some((key, entry))) => result.push(Ok((key.into_raw().unwrap(), entry))),
            Ok(None) => return result,
            Err(err) => result.push(Err(format!("{err:?}"))),
        }
        // A lock error must move the cursor so that the next call progresses.
        assert!(result.len() < 1000);
    }
}

fn snapshot_store<S: Snapshot>(snapshot: S) -> SnapshotStore<S> {
    SnapshotStore::new(
        snapshot,
        30.into(),
        IsolationLevel::Si,
        true,
        TsSet::default(),
        TsSet::default(),
        true,
    )
}

#[test]
fn test_scanner_reuse_physical_iterators_both_directions() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let value = vec![b'v'; 512];
    for key in [b"a", b"c", b"e", b"g"] {
        must_prewrite_put(&mut engine, key, &value, key, 10);
        must_commit(&mut engine, key, 10, 20);
    }

    for desc in [false, true] {
        let snapshot = CountingSnapshot::new(engine.snapshot(Default::default()).unwrap());
        let store = snapshot_store(snapshot.clone());
        let (lower, upper) = bounds(b"a", b"b");
        let mut scanner = store
            .scanner(desc, false, true, true, lower, upper)
            .unwrap();
        assert_eq!(snapshot.iterator_count(), 2);
        let result = collect(&mut scanner);
        assert_eq!(
            result,
            vec![Ok((
                b"a".to_vec(),
                ValueEntry::new(value.clone(), Some(20.into()))
            ))]
        );
        assert_eq!(snapshot.iterator_count(), 3);
        let stats = scanner.take_statistics();
        assert_eq!(stats.write.processed_keys, 1);
        assert_eq!(scanner.take_statistics().write.processed_keys, 0);

        // Includes an exhausted range, empty ranges, reverse movement, and
        // widening a range beyond both original physical iterator bounds.
        for (lower, upper) in [
            (b"e".as_slice(), b"f".as_slice()),
            (b"b", b"c"),
            (b"c", b"c"),
            (b"g", b"h"),
            (b"a", b"h"),
            (b"c", b"f"),
            (b"a", b"b"),
        ] {
            let (lower_bound, upper_bound) = bounds(lower, upper);
            scanner = store
                .scanner_with_reuse(
                    Some(scanner),
                    desc,
                    false,
                    true,
                    true,
                    lower_bound,
                    upper_bound,
                )
                .unwrap();
            assert_eq!(snapshot.iterator_count(), 3, "range {lower:?}..{upper:?}");
            let reused_result = collect(&mut scanner);
            let (lower_bound, upper_bound) = bounds(lower, upper);
            // Build the reference on the uncounted underlying snapshot.
            let mut fresh = snapshot_store(snapshot.inner.clone())
                .scanner(desc, false, true, true, lower_bound, upper_bound)
                .unwrap();
            assert_eq!(reused_result, collect(&mut fresh));
            let expected_len = reused_result.len();
            assert_eq!(scanner.take_statistics().write.processed_keys, expected_len);
        }
    }
}

#[test]
fn test_scanner_reuse_partial_range_and_lazy_default_cursor() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    for (key, value) in [
        (b"a".as_slice(), vec![b'a']),
        (b"c", vec![b'c'; 512]),
        (b"e", vec![b'e'; 512]),
    ] {
        must_prewrite_put(&mut engine, key, &value, key, 10);
        must_commit(&mut engine, key, 10, 20);
    }
    for desc in [false, true] {
        let snapshot = CountingSnapshot::new(engine.snapshot(Default::default()).unwrap());
        let store = snapshot_store(snapshot.clone());
        let (lower, upper) = bounds(b"a", b"b");
        let mut scanner = store
            .scanner(desc, false, false, false, lower, upper)
            .unwrap();
        assert!(scanner.next_entry().unwrap().is_some());
        assert_eq!(snapshot.iterator_count(), 2);

        // Reset before EOF. The lazy default cursor must use the new range.
        for key in [b"c", b"e"] {
            let (lower, upper) = bounds(key, &[key[0] + 1]);
            scanner = store
                .scanner_with_reuse(Some(scanner), desc, false, false, false, lower, upper)
                .unwrap();
            assert_eq!(
                collect(&mut scanner),
                vec![Ok((
                    key.to_vec(),
                    ValueEntry::from_value(vec![key[0]; 512])
                ))]
            );
            assert_eq!(snapshot.iterator_count(), 3);
        }
    }
}

#[test]
fn test_scanner_reuse_mvcc_visibility_locks_and_newer_ts() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    for key in [b"a", b"c", b"e", b"g", b"h"] {
        must_prewrite_put(&mut engine, key, &vec![key[0]; 512], key, 10);
        must_commit(&mut engine, key, 10, 20);
    }
    must_prewrite_put(&mut engine, b"c", &vec![b'n'; 512], b"c", 40);
    must_commit(&mut engine, b"c", 40, 50);
    must_prewrite_delete(&mut engine, b"e", b"e", 25);
    must_commit(&mut engine, b"e", 25, 26);
    must_prewrite_put(&mut engine, b"g", &vec![b'l'; 512], b"g", 25);
    must_prewrite_put(&mut engine, b"h", &vec![b'l'; 512], b"h", 60);

    let snapshot = engine.snapshot(Default::default()).unwrap();
    // Writes after acquiring the snapshot must remain invisible after a reset.
    must_prewrite_put(&mut engine, b"b", b"new key", b"b", 21);
    must_commit(&mut engine, b"b", 21, 22);

    for desc in [false, true] {
        for isolation in [IsolationLevel::Si, IsolationLevel::Rc] {
            for (bypass, access) in [
                (TsSet::default(), TsSet::default()),
                (TsSet::new(vec![TimeStamp::new(25)]), TsSet::default()),
                (TsSet::default(), TsSet::new(vec![TimeStamp::new(25)])),
            ] {
                let store = SnapshotStore::new(
                    snapshot.clone(),
                    30.into(),
                    isolation,
                    true,
                    bypass,
                    access,
                    true,
                );
                let (lower, upper) = bounds(b"a", b"b");
                let mut scanner = store
                    .scanner(desc, false, true, false, lower, upper)
                    .unwrap();
                assert_eq!(collect(&mut scanner).len(), 1);
                assert_eq!(scanner.met_newer_ts_data(), NewerTsCheckState::NotMetYet);
                for (lower, upper) in [
                    (b"c".as_slice(), b"d".as_slice()),
                    (b"e", b"i"),
                    (b"a", b"i"),
                    (b"a", b"b"),
                ] {
                    let (lower, upper) = bounds(lower, upper);
                    scanner = store
                        .scanner_with_reuse(
                            Some(scanner),
                            desc,
                            false,
                            true,
                            false,
                            lower.clone(),
                            upper.clone(),
                        )
                        .unwrap();
                    let mut fresh = store
                        .scanner(desc, false, true, false, lower, upper)
                        .unwrap();
                    assert_eq!(collect(&mut scanner), collect(&mut fresh));
                    assert_eq!(scanner.met_newer_ts_data(), NewerTsCheckState::Met);
                }
            }
        }
    }
}

#[test]
fn test_scanner_reuse_rebuilds_for_changed_scan_options() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    must_prewrite_put(&mut engine, b"a", b"value", b"a", 10);
    must_commit(&mut engine, b"a", 10, 20);
    let snapshot = CountingSnapshot::new(engine.snapshot(Default::default()).unwrap());
    let store = snapshot_store(snapshot.clone());
    let (lower, upper) = bounds(b"a", b"b");
    let mut scanner = store
        .scanner(false, false, false, false, lower, upper)
        .unwrap();
    let mut expected_count = 2;
    for (desc, key_only, load_commit_ts, check_newer) in [
        (true, false, false, false),
        (true, true, false, false),
        (true, true, true, false),
        (true, true, true, true),
    ] {
        let (lower, upper) = bounds(b"a", b"b");
        scanner = store
            .scanner_with_reuse(
                Some(scanner),
                desc,
                key_only,
                check_newer,
                load_commit_ts,
                lower,
                upper,
            )
            .unwrap();
        expected_count += 2;
        assert_eq!(snapshot.iterator_count(), expected_count);
        let entry = scanner.next_entry().unwrap().unwrap().1;
        assert_eq!(
            entry.value,
            if key_only { vec![] } else { b"value".to_vec() }
        );
        assert_eq!(entry.commit_ts, load_commit_ts.then(|| 20.into()));
        assert_eq!(
            scanner.met_newer_ts_data(),
            if check_newer {
                NewerTsCheckState::NotMetYet
            } else {
                NewerTsCheckState::Unknown
            }
        );
    }
}

#[test]
fn test_scanner_reuse_falls_back_for_unsupported_engine() {
    let mut engine = BTreeEngine::new(&[CF_DEFAULT, CF_LOCK, CF_WRITE]);
    let snapshot = CountingSnapshot::new(engine.snapshot(Default::default()).unwrap());
    let store = snapshot_store(snapshot.clone());
    let (lower, upper) = bounds(b"a", b"b");
    let scanner = store
        .scanner(false, false, true, false, lower, upper)
        .unwrap();
    assert_eq!(snapshot.iterator_count(), 2);
    let (lower, upper) = bounds(b"c", b"d");
    let mut scanner = store
        .scanner_with_reuse(Some(scanner), false, false, true, false, lower, upper)
        .unwrap();
    assert_eq!(snapshot.iterator_count(), 4);
    assert!(scanner.next_entry().unwrap().is_none());
}

#[test]
fn test_scanner_reuse_rejects_unbounded_or_inverted_range() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let snapshot = engine.snapshot(Default::default()).unwrap();
    for desc in [false, true] {
        for (lower, upper) in [
            (None, Some(Key::from_raw(b"b"))),
            (Some(Key::from_raw(b"a")), None),
            (Some(Key::from_raw(b"b")), Some(Key::from_raw(b"a"))),
        ] {
            let mut scanner = ScannerBuilder::new(snapshot.clone(), 30.into())
                .desc(desc)
                .range(Some(Key::from_raw(b"a")), Some(Key::from_raw(b"b")))
                .build()
                .unwrap();
            assert!(
                !scanner
                    .reset_range(desc, false, false, lower, upper)
                    .unwrap()
            );
        }
    }
}

#[test]
fn test_scanner_reuse_fallback_retains_partial_reset_statistics() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let value = vec![b'v'; 512];
    for key in [b"a", b"c"] {
        must_prewrite_put(&mut engine, key, &value, key, 10);
        must_commit(&mut engine, key, 10, 20);
    }

    for desc in [false, true] {
        for reject_reset_cf in [CF_LOCK, CF_DEFAULT] {
            let mut snapshot = CountingSnapshot::new(engine.snapshot(Default::default()).unwrap());
            snapshot.reject_reset_cf = Some(reject_reset_cf);
            let store = snapshot_store(snapshot.clone());
            let (lower, upper) = bounds(b"a", b"b");
            let mut scanner = store
                .scanner(desc, false, true, false, lower, upper)
                .unwrap();
            assert_eq!(collect(&mut scanner).len(), 1);
            assert_eq!(snapshot.iterator_count(), 3);
            // Keep the old range's statistics undrained. Successful write-CF
            // reset is followed by rejected lock/default-CF reset and fallback.
            let (lower, upper) = bounds(b"c", b"d");
            scanner = store
                .scanner_with_reuse(Some(scanner), desc, false, true, false, lower, upper)
                .unwrap();
            assert_eq!(collect(&mut scanner).len(), 1);
            assert_eq!(snapshot.iterator_count(), 6);
            let statistics = scanner.take_statistics();
            let seek_count = |cf: &CfStatistics| cf.seek + cf.seek_for_prev;
            assert_eq!(seek_count(&statistics.write), 3);
            assert_eq!(
                seek_count(&statistics.lock),
                if reject_reset_cf == CF_LOCK { 2 } else { 3 }
            );
            assert_eq!(statistics.write.processed_keys, 2);
            assert_eq!(
                statistics.processed_size,
                2 * kv::kv_processed_size(Key::from_raw(b"a").len(), value.len())
            );
            assert_eq!(scanner.take_statistics().write.total_op_count(), 0);

            // TikvStorage drains before reset; the attempted physical work
            // still has to survive in the replacement scanner's statistics.
            let mut storage = TikvStorage::new(store, true);
            for range in [("a", "b"), ("c", "d")] {
                storage
                    .begin_scan(desc, false, false, IntervalRange::from(range))
                    .unwrap();
                assert!(storage.scan_next_entry().unwrap().is_some());
                assert!(storage.scan_next_entry().unwrap().is_none());
            }
            let mut statistics = Statistics::default();
            storage.collect_statistics(&mut statistics);
            assert_eq!(seek_count(&statistics.write), 3);
            assert_eq!(
                seek_count(&statistics.lock),
                if reject_reset_cf == CF_LOCK { 2 } else { 3 }
            );
            assert_eq!(statistics.write.processed_keys, 2);
        }
    }
}

#[test]
fn test_scanner_reuse_coprocessor_statistics_and_cacheability() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let value = vec![b'v'; 512];
    for key in [b"a", b"c", b"e"] {
        must_prewrite_put(&mut engine, key, &value, key, 10);
        must_commit(&mut engine, key, 10, 20);
    }
    must_prewrite_put(&mut engine, b"c", b"future", b"c", 40);
    must_commit(&mut engine, b"c", 40, 50);

    for desc in [false, true] {
        let snapshot = CountingSnapshot::new(engine.snapshot(Default::default()).unwrap());
        let mut storage = TikvStorage::new(snapshot_store(snapshot.clone()), true);
        let mut expected_bytes = 0;
        for (index, (lower, upper)) in [("a", "b"), ("c", "d"), ("e", "f")].into_iter().enumerate()
        {
            storage
                .begin_scan(desc, false, false, IntervalRange::from((lower, upper)))
                .unwrap();
            let entry = storage.scan_next_entry().unwrap().unwrap();
            expected_bytes +=
                kv::kv_processed_size(Key::from_raw(lower.as_bytes()).len(), value.len());
            assert_eq!(entry.key, lower.as_bytes());
            assert_eq!(entry.value, value);
            assert!(storage.scan_next_entry().unwrap().is_none());
            assert_eq!(snapshot.iterator_count(), 3);
            assert_eq!(storage.scanned_bytes(), expected_bytes);
            // The second range sees a newer MVCC version, and later ranges
            // must keep that decision even after statistics are drained.
            assert_eq!(storage.met_uncacheable_data(), Some(index > 0));
            let mut statistics = Statistics::default();
            storage.collect_statistics(&mut statistics);
            assert_eq!(statistics.write.processed_keys, 1);
            assert_eq!(
                statistics.processed_size,
                kv::kv_processed_size(Key::from_raw(lower.as_bytes()).len(), value.len())
            );
            let mut drained = Statistics::default();
            storage.collect_statistics(&mut drained);
            assert_eq!(drained.write.processed_keys, 0);
            assert_eq!(drained.processed_size, 0);
            assert_eq!(storage.scanned_bytes(), expected_bytes);
        }
    }
}
