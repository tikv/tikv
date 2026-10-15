// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! MVCC multi-range scan microbenchmarks for #4239.
//!
//! Build once, then run the same executable twice with `MEASUREMENT=CPU_TIME`:
//! use `TIKV_MULTI_RANGE_FRESH=1` and Criterion's `--save-baseline fresh`, then
//! unset that variable and use `--baseline fresh`. Filter on
//! `multi_range_scan/`. `FreshStore` deliberately uses Store's default
//! scanner_with_reuse method; the other path uses the production SnapshotStore
//! implementation. Neither build flags nor the generated data differ between
//! these runs.
//!
//! A full benchmark build and a short comparison can be run from the repo root:
//! ```text
//! ./scripts/env cargo bench -p tests --bench coprocessor_executors --no-run
//! MEASUREMENT=CPU_TIME TIKV_MULTI_RANGE_FRESH=1 TIKV_MULTI_RANGE_STATS=1 \
//!   ./scripts/env cargo bench -p tests --bench coprocessor_executors -- \
//!   multi_range_scan/ --save-baseline fresh --warm-up-time 1 \
//!   --measurement-time 2 --sample-size 10 --noplot
//! MEASUREMENT=CPU_TIME TIKV_MULTI_RANGE_FRESH=0 TIKV_MULTI_RANGE_STATS=1 \
//!   ./scripts/env cargo bench -p tests --bench coprocessor_executors -- \
//!   multi_range_scan/ --baseline fresh --warm-up-time 1 \
//!   --measurement-time 2 --sample-size 10 --noplot
//! ```
//! Keep the source, build flags, and executable unchanged between both runs.
//! Ten samples are suitable for smoke comparisons; use longer runs and repeat
//! with reversed run order for performance claims. Both paths explicitly use
//! RocksDB's EnableCount perf level, so the printed statistics can expose
//! physical tombstone skips. Each new range uses a hard seek and physical
//! bounds, so gap tombstones are not expected to cause linear cursor movement.
//! Physical-tombstone fixtures put records and subsequent deletes in separate
//! SST generations, pin a snapshot of the original puts, and disable automatic
//! compaction. These are repeated, cache-warmed local CPU scans; they do not
//! measure production cold-cache or full-disk I/O behavior.
//!
//! Fixtures and full key/value/order checks are outside Criterion's timed loop.
//! Each iteration creates one TikvStorage, drains every range, and collects its
//! statistics. Set `TIKV_MULTI_RANGE_STATS=1` to print validation-run
//! statistics. This isolates the local scan path: a smoke run or a partially
//! optimized profile does not establish a production latency or throughput
//! improvement.

use std::{cell::OnceCell, sync::Arc};

use criterion::{black_box, measurement::Measurement};
use engine_rocks::{
    PerfLevel, RocksCfOptions, RocksDbOptions, RocksEngine, RocksSnapshot, set_perf_level,
};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE, KvEngine, MiscExt, SyncMutable};
use kvproto::kvrpcpb::IsolationLevel;
use tempfile::TempDir;
use tidb_query_common::storage::{IntervalRange, Storage};
use tikv::{
    coprocessor::dag::TikvStorage,
    storage::{Result, Statistics, Store, mvcc::NewerTsCheckState, txn::SnapshotStore},
};
use txn_types::{Key, TimeStamp, ValueEntry, Write, WriteType};

const ROWS_PER_RANGE: usize = 2;
const VALUE: [u8; 64] = [b'v'; 64];

/// Forwards everything except scanner_with_reuse, preserving a fresh-scanner
/// control path in the same binary as the optimized implementation.
struct FreshStore<S>(S);

impl<S: Store> Store for FreshStore<S> {
    type Scanner = S::Scanner;

    fn get_entry(
        &self,
        key: &Key,
        load_commit_ts: bool,
        statistics: &mut Statistics,
    ) -> Result<Option<ValueEntry>> {
        self.0.get_entry(key, load_commit_ts, statistics)
    }

    fn incremental_get_entry(
        &mut self,
        key: &Key,
        load_commit_ts: bool,
    ) -> Result<Option<ValueEntry>> {
        self.0.incremental_get_entry(key, load_commit_ts)
    }

    fn incremental_get_take_statistics(&mut self) -> Statistics {
        self.0.incremental_get_take_statistics()
    }

    fn incremental_get_met_newer_ts_data(&self) -> NewerTsCheckState {
        self.0.incremental_get_met_newer_ts_data()
    }

    fn is_check_has_newer_ts_data(&self) -> bool {
        self.0.is_check_has_newer_ts_data()
    }

    fn batch_get(
        &self,
        keys: &[Key],
        load_commit_ts: bool,
        statistics: &mut Vec<Statistics>,
    ) -> Result<Vec<Result<Option<ValueEntry>>>> {
        self.0.batch_get(keys, load_commit_ts, statistics)
    }

    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        check_has_newer_ts_data: bool,
        load_commit_ts: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner> {
        self.0.scanner(
            desc,
            key_only,
            check_has_newer_ts_data,
            load_commit_ts,
            lower_bound,
            upper_bound,
        )
    }
}

#[derive(Clone, Copy)]
enum Layout {
    Dense,
    SparseRanges,
    SparseData,
    PhysicalTombstones,
    WidePhysicalTombstones,
    MvccDeletes,
    MostlyEmpty,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Self::Dense => "dense",
            Self::SparseRanges => "sparse_ranges",
            Self::SparseData => "sparse_data",
            Self::PhysicalTombstones => "physical_tombstones",
            Self::WidePhysicalTombstones => "wide_physical_tombstones",
            Self::MvccDeletes => "mvcc_deletes",
            Self::MostlyEmpty => "mostly_empty",
        }
    }

    fn stride(self) -> usize {
        match self {
            Self::Dense | Self::MostlyEmpty => 4,
            Self::WidePhysicalTombstones => 8194,
            _ => 130,
        }
    }

    fn physical_tombstones(self) -> bool {
        matches!(
            self,
            Self::PhysicalTombstones | Self::WidePhysicalTombstones
        )
    }

    fn populated(self, index: usize) -> bool {
        match self {
            Self::SparseData => index % self.stride() < ROWS_PER_RANGE,
            Self::MostlyEmpty => {
                index % self.stride() < ROWS_PER_RANGE && index / self.stride() % 32 == 0
            }
            _ => true,
        }
    }
}

struct Fixture {
    snapshot: Arc<RocksSnapshot>,
    // Preserve the original puts underneath the physical tombstones, including
    // when a memtable is flushed while the delete batch is being constructed.
    _tombstone_pin: Option<RocksSnapshot>,
    _engine: RocksEngine,
    _directory: TempDir,
    ranges: Vec<IntervalRange>,
    expected_keys: Vec<Vec<u8>>,
    fresh: bool,
}

fn raw_key(index: usize) -> Vec<u8> {
    let mut key = vec![b'k'];
    key.extend_from_slice(&(index as u64).to_be_bytes());
    key
}

fn version_key(index: usize, ts: u64) -> Key {
    Key::from_raw(&raw_key(index)).append_ts(TimeStamp::new(ts))
}

fn put_row(engine: &RocksEngine, index: usize) {
    engine
        .put_cf(CF_DEFAULT, version_key(index, 1).as_encoded(), &VALUE)
        .unwrap();
    let write = Write::new(WriteType::Put, TimeStamp::new(1), None)
        .as_ref()
        .to_bytes();
    engine
        .put_cf(CF_WRITE, version_key(index, 2).as_encoded(), &write)
        .unwrap();
}

impl Fixture {
    fn new(layout: Layout, count: usize) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("multi-range-scan")
            .tempdir()
            .unwrap();
        let cf_options = [CF_DEFAULT, CF_WRITE, CF_LOCK]
            .into_iter()
            .map(|cf| {
                let mut options = RocksCfOptions::default();
                options.set_disable_auto_compactions(true);
                (cf, options)
            })
            .collect();
        let engine = engine_rocks::util::new_engine_opt(
            directory.path().to_str().unwrap(),
            RocksDbOptions::default(),
            cf_options,
        )
        .unwrap();
        let stride = layout.stride();
        let end = (count - 1) * stride + ROWS_PER_RANGE;
        let mut puts = 0_u64;
        for index in 0..end {
            if layout.populated(index) {
                put_row(&engine, index);
                puts += 1;
            }
        }
        engine.flush_cfs(&[CF_DEFAULT, CF_WRITE], true).unwrap();

        let tombstone_pin = layout.physical_tombstones().then(|| engine.snapshot());
        let mut deletes = 0_u64;
        if layout.physical_tombstones() || matches!(layout, Layout::MvccDeletes) {
            for index in 0..end {
                if index % stride < ROWS_PER_RANGE {
                    continue;
                }
                if layout.physical_tombstones() {
                    engine
                        .delete_cf(CF_DEFAULT, version_key(index, 1).as_encoded())
                        .unwrap();
                    engine
                        .delete_cf(CF_WRITE, version_key(index, 2).as_encoded())
                        .unwrap();
                } else {
                    let write = Write::new(WriteType::Delete, TimeStamp::new(3), None)
                        .as_ref()
                        .to_bytes();
                    engine
                        .put_cf(CF_WRITE, version_key(index, 4).as_encoded(), &write)
                        .unwrap();
                }
                deletes += 1;
            }
            engine.flush_cfs(&[CF_DEFAULT, CF_WRITE], true).unwrap();
        }
        if layout.physical_tombstones() {
            // All writes and deletes are unique, both batches were flushed,
            // and compaction is disabled. The extra entries therefore prove
            // the on-disk fixture retains the requested physical tombstones.
            for cf in [CF_DEFAULT, CF_WRITE] {
                let db = engine.as_inner();
                let handle = db.cf_handle(cf).unwrap();
                let tables = db.get_properties_of_all_tables_cf(handle).unwrap();
                let entries: u64 = tables.iter().map(|(_, p)| p.num_entries()).sum();
                assert_eq!(entries, puts + deletes, "{}: {cf}", layout.name());
                assert!(deletes > 0);
            }
        }

        let ranges: Vec<_> = (0..count)
            .map(|i| {
                IntervalRange::from((raw_key(i * stride), raw_key(i * stride + ROWS_PER_RANGE)))
            })
            .collect();
        let expected_keys = (0..count)
            .flat_map(|i| i * stride..i * stride + ROWS_PER_RANGE)
            .filter(|index| layout.populated(*index))
            .map(raw_key)
            .collect();
        Self {
            snapshot: Arc::new(engine.snapshot()),
            _tombstone_pin: tombstone_pin,
            _engine: engine,
            _directory: directory,
            ranges,
            expected_keys,
            // Read only at fixture construction, never in the measured loop.
            fresh: std::env::var("TIKV_MULTI_RANGE_FRESH").is_ok_and(|v| v == "1"),
        }
    }

    fn store(&self) -> SnapshotStore<Arc<RocksSnapshot>> {
        SnapshotStore::new(
            self.snapshot.clone(),
            TimeStamp::new(10),
            IsolationLevel::Si,
            true,
            Default::default(),
            Default::default(),
            false,
        )
    }

    fn scan<S: Store>(&self, store: S, backward: bool, verify: bool) -> Statistics {
        let mut storage = TikvStorage::new(store, false);
        let mut keys = Vec::new();
        for i in 0..self.ranges.len() {
            let index = if backward {
                self.ranges.len() - i - 1
            } else {
                i
            };
            storage
                .begin_scan(backward, false, false, self.ranges[index].clone())
                .unwrap();
            while let Some(entry) = storage.scan_next_entry().unwrap() {
                if verify {
                    assert_eq!(entry.value, VALUE);
                    assert_eq!(entry.commit_ts, None);
                    keys.push(entry.key);
                } else {
                    black_box(entry);
                }
            }
        }
        let mut statistics = Statistics::default();
        storage.collect_statistics(&mut statistics);
        if verify {
            let mut expected = self.expected_keys.clone();
            if backward {
                expected.reverse();
            }
            assert_eq!(keys, expected);
            assert_eq!(statistics.write.processed_keys, expected.len());
            assert_eq!(statistics.data.processed_keys, expected.len());
            let processed_size: usize = expected
                .iter()
                .map(|key| Key::from_raw(key).len() + VALUE.len())
                .sum();
            assert_eq!(statistics.processed_size, processed_size);
        }
        statistics
    }

    fn run(&self, backward: bool, verify: bool) -> Statistics {
        if self.fresh {
            self.scan(FreshStore(self.store()), backward, verify)
        } else {
            self.scan(self.store(), backward, verify)
        }
    }
}

pub fn bench<M: Measurement + 'static>(c: &mut criterion::Criterion<M>) {
    // Metrics collectors sample RocksDB's thread-local perf counters. Enable
    // them consistently for both variants and outside Criterion's timed loop.
    set_perf_level(PerfLevel::EnableCount);
    // Dense has 3 range counts; the remaining 6 layouts have one each.
    // Each runs in both directions: (3 + 6) * 2 = 18 cases.
    let cases = [
        (Layout::Dense, 1),
        (Layout::Dense, 64),
        (Layout::Dense, 512),
        (Layout::SparseRanges, 512),
        (Layout::SparseData, 512),
        (Layout::PhysicalTombstones, 512),
        (Layout::WidePhysicalTombstones, 64),
        (Layout::MvccDeletes, 512),
        (Layout::MostlyEmpty, 512),
    ];
    for (layout, count) in cases {
        // Criterion filters before invoking the closure, so selecting a small
        // case does not construct the much larger tombstone fixtures.
        let fixture = OnceCell::new();
        for backward in [false, true] {
            let direction = if backward { "backward" } else { "forward" };
            let id = format!("multi_range_scan/{}/{count}/{direction}", layout.name());
            c.bench_function(&id, |b| {
                let fixture = fixture.get_or_init(|| Fixture::new(layout, count));
                let statistics = fixture.run(backward, true);
                if std::env::var("TIKV_MULTI_RANGE_STATS").is_ok_and(|v| v == "1") {
                    eprintln!("{id} fresh={} {statistics:?}", fixture.fresh);
                }
                b.iter(|| black_box(fixture.run(backward, false)));
            });
        }
    }
}
