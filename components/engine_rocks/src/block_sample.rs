// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Samples the MVCC rows visible at a timestamp by reading a fraction of the
//! data blocks of the write CF.
//!
//! Every data block of every sorted run (each L0 file and each level) is
//! selected on its own with probability `1 / spacing`, and the memtables are
//! sampled per row with the same probability. A visible row has exactly one
//! visible version, which lives in exactly one block or memtable, so every
//! visible row is returned with probability `1 / spacing` no matter how many
//! versions, deletes or levels the data has. Only the selected blocks are
//! read; a version found there is returned only if no newer sorted run holds a
//! newer version of its row.
//!
//! Only newer runs are probed. This depends on a version that RocksDB wrote
//! later never having a smaller commit ts than a Put or Delete of the same row
//! that it wrote earlier. Percolator keeps that order because a transaction
//! cannot commit a key below a newer commit of the key. If the order breaks,
//! a row can come back twice or with an old value. Locks are not checked:
//! statistics read committed data only.

use std::{
    cell::Cell,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use engine_traits::{
    CF_DEFAULT, CF_WRITE, IterOptions, Iterable, Iterator, MvccBlockSampleOptions,
    MvccBlockSampleStats, Result,
};
use rocksdb::{BlockSampleSink, BlockSampler, DB, rocksdb_options::UnsafeSnap};
use txn_types::{WriteRef, WriteType};

use crate::{RocksSnapshot, r2e, util::get_cf_handle};

const TS_LEN: usize = 8;
// RocksDB value type of a put; other types are deletes or range deletes.
const ROCKS_TYPE_VALUE: u8 = 1;

fn decode_ts(key: &[u8]) -> u64 {
    let mut b = [0u8; TS_LEN];
    b.copy_from_slice(&key[key.len() - TS_LEN..]);
    !u64::from_be_bytes(b)
}

fn with_ts(user_key: &[u8], ts: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(user_key.len() + TS_LEN);
    k.extend_from_slice(user_key);
    k.extend_from_slice(&(!ts).to_be_bytes());
    k
}

/// Splits an internal key into (MVCC key, RocksDB value type).
fn split_internal_key(ikey: &[u8]) -> Option<(&[u8], u8)> {
    if ikey.len() < 8 + TS_LEN + 1 {
        return None;
    }
    Some((&ikey[..ikey.len() - 8], ikey[ikey.len() - 8]))
}

fn key_hash(seed: u64, user_key: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    user_key.hash(&mut h);
    h.finish()
}

struct Candidate {
    user_key: Vec<u8>,
    commit_ts: u64,
    is_put: bool,
    short_value: Option<Vec<u8>>,
    start_ts: u64,
    level: i32,
    file_number: u64,
    // The sampled block of the row; every memtable row gets its own.
    block_id: u64,
    // The visible version is in the next block of the same file.
    needs_forward: bool,
}

/// Picks, in each sampled block, the version that may be visible for every
/// row whose newest entry in this file lives in the block.
struct Collector {
    read_ts: u64,
    spacing: u64,
    seed: u64,
    level: i32,
    file_number: u64,
    prev_index_key: Vec<u8>,
    cur: Vec<u8>,
    decided: bool,
    owned: bool,
    first_entry: bool,
    block_id: u64,
    last_rocks_key: Vec<u8>,
    out: Vec<Candidate>,
}

impl Collector {
    fn finish_group(&mut self) {
        // The memtables hold all their versions of a row, so a memtable row
        // without a visible version has none there.
        if self.level >= 0 && !self.cur.is_empty() && self.owned && !self.decided {
            self.out.push(Candidate {
                user_key: self.cur.clone(),
                commit_ts: 0,
                is_put: false,
                short_value: None,
                start_ts: 0,
                level: self.level,
                file_number: self.file_number,
                block_id: self.block_id,
                needs_forward: true,
            });
        }
    }

    fn push(&mut self, user_key: &[u8], commit_ts: u64, write: &WriteRef<'_>) {
        self.out.push(Candidate {
            user_key: user_key.to_vec(),
            commit_ts,
            is_put: write.write_type == WriteType::Put,
            short_value: write.short_value.map(|v| v.to_vec()),
            start_ts: write.start_ts.into_inner(),
            level: self.level,
            file_number: self.file_number,
            block_id: self.block_id,
            needs_forward: false,
        });
    }

    fn keep_memtable_row(&self, user_key: &[u8]) -> bool {
        key_hash(self.seed, user_key) % self.spacing == 0
    }
}

impl BlockSampleSink for Collector {
    fn begin_block(&mut self, level: i32, file_number: u64, prev: &[u8]) {
        self.level = level;
        self.file_number = file_number;
        self.prev_index_key.clear();
        self.prev_index_key.extend_from_slice(prev);
        self.cur.clear();
        self.last_rocks_key.clear();
        self.decided = false;
        self.owned = false;
        self.first_entry = true;
        self.block_id += 1;
    }

    fn entry(&mut self, ikey: &[u8], value: &[u8]) {
        let Some((key, rocks_type)) = split_internal_key(ikey) else {
            return;
        };
        // Older RocksDB versions of the same MVCC key are hidden.
        if key == self.last_rocks_key.as_slice() {
            return;
        }
        self.last_rocks_key.clear();
        self.last_rocks_key.extend_from_slice(key);
        let user_key = &key[..key.len() - TS_LEN];
        let ts = decode_ts(key);
        if user_key != self.cur.as_slice() {
            // A row that ends inside the block without a visible version has
            // none in this file, so only the last row of a block can continue.
            self.cur.clear();
            self.cur.extend_from_slice(user_key);
            self.decided = false;
            self.owned = if self.level < 0 {
                // The memtables have no blocks: a row is selected by its key
                // hash, and it is a block of its own.
                self.block_id += 1;
                self.keep_memtable_row(user_key)
            } else {
                // The previous block's index key is >= its last key. If it
                // starts with this row's key, the row's newer versions are in
                // the previous block, which owns the row.
                !(self.first_entry && self.prev_index_key.starts_with(user_key))
            };
        }
        self.first_entry = false;
        if !self.owned || self.decided || ts > self.read_ts || rocks_type != ROCKS_TYPE_VALUE {
            return;
        }
        let Ok(write) = WriteRef::parse(value) else {
            return;
        };
        if !matches!(write.write_type, WriteType::Put | WriteType::Delete) {
            return;
        }
        self.decided = true;
        let uk = self.cur.clone();
        self.push(&uk, ts, &write);
    }

    fn end_block(&mut self) {
        self.finish_group();
        self.cur.clear();
    }
}

/// Whether a newer sorted run holds a Put or Delete of the row of `c` with a
/// commit ts in `(c.commit_ts, read_ts]`, or overwrites `c` itself. A newer
/// run is read only when its prefix filter may hold the row.
fn shadowed_by_newer(sampler: &mut BlockSampler, c: &Candidate, read_ts: u64) -> Result<bool> {
    if c.level < 0 {
        // Memtables are the newest run.
        return Ok(false);
    }
    let seek = with_ts(&c.user_key, read_ts);
    let shadowed = Cell::new(false);
    let user_len = c.user_key.len();
    let mut visit = |ikey: &[u8], value: &[u8]| -> bool {
        let Some((key, rocks_type)) = split_internal_key(ikey) else {
            return true;
        };
        if key.len() != user_len + TS_LEN {
            return true;
        }
        let ts = decode_ts(key);
        if ts > read_ts {
            return true;
        }
        if ts < c.commit_ts {
            return false;
        }
        if ts == c.commit_ts {
            // The same MVCC key in a newer run: a RocksDB delete or rewrite.
            shadowed.set(true);
            return false;
        }
        if rocks_type != ROCKS_TYPE_VALUE {
            return true;
        }
        match WriteRef::parse(value) {
            Ok(w) if matches!(w.write_type, WriteType::Put | WriteType::Delete) => {
                shadowed.set(true);
                false
            }
            _ => true,
        }
    };
    sampler
        .probe_newer(c.level, c.file_number, &seek, &c.user_key, &mut visit)
        .map_err(r2e)?;
    Ok(shadowed.get())
}

/// Fills in the visible version of a row whose versions continue into the
/// next block of the same file.
fn resolve_forward(sampler: &mut BlockSampler, c: &mut Candidate, read_ts: u64) -> Result<bool> {
    let seek = with_ts(&c.user_key, read_ts);
    let user_len = c.user_key.len();
    let mut found = None;
    let mut visit = |ikey: &[u8], value: &[u8]| -> bool {
        let Some((key, rocks_type)) = split_internal_key(ikey) else {
            return true;
        };
        if key.len() != user_len + TS_LEN || rocks_type != ROCKS_TYPE_VALUE {
            return true;
        }
        let ts = decode_ts(key);
        if ts > read_ts {
            return true;
        }
        match WriteRef::parse(value) {
            Ok(w) if matches!(w.write_type, WriteType::Put | WriteType::Delete) => {
                found = Some((
                    ts,
                    w.write_type == WriteType::Put,
                    w.short_value.map(|v| v.to_vec()),
                    w.start_ts.into_inner(),
                ));
                false
            }
            _ => true,
        }
    };
    sampler
        .probe_file(c.level, c.file_number, &seek, &c.user_key, &mut visit)
        .map_err(r2e)?;
    match found {
        Some((ts, is_put, short_value, start_ts)) => {
            c.commit_ts = ts;
            c.is_put = is_put;
            c.short_value = short_value;
            c.start_ts = start_ts;
            Ok(true)
        }
        None => Ok(false),
    }
}

pub(crate) fn sample_mvcc_rows(
    snap: &RocksSnapshot,
    db: &DB,
    unsafe_snap: &UnsafeSnap,
    start: &[u8],
    end: &[u8],
    read_ts: u64,
    opts: &MvccBlockSampleOptions,
    f: &mut dyn FnMut(&[u8], &[u8], u64),
) -> Result<Option<MvccBlockSampleStats>> {
    // The perf context counts the block reads of this thread; the sampling
    // runs on one thread without yielding, so the difference is its own I/O.
    let perf_level = rocksdb::get_perf_level();
    if (perf_level as i32) < (rocksdb::PerfLevel::EnableCount as i32) {
        rocksdb::set_perf_level(rocksdb::PerfLevel::EnableCount);
    }
    let perf = rocksdb::PerfContext::get();
    let reads0 = perf.block_read_count();
    let result = sample_mvcc_rows_impl(snap, db, unsafe_snap, start, end, read_ts, opts, f);
    let reads1 = rocksdb::PerfContext::get().block_read_count();
    rocksdb::set_perf_level(perf_level);
    result.map(|stats| {
        stats.map(|mut stats| {
            stats.block_reads = reads1.saturating_sub(reads0);
            stats
        })
    })
}

#[allow(clippy::too_many_arguments)]
fn sample_mvcc_rows_impl(
    snap: &RocksSnapshot,
    db: &DB,
    unsafe_snap: &UnsafeSnap,
    start: &[u8],
    end: &[u8],
    read_ts: u64,
    opts: &MvccBlockSampleOptions,
    f: &mut dyn FnMut(&[u8], &[u8], u64),
) -> Result<Option<MvccBlockSampleStats>> {
    let spacing = opts.spacing.max(1);
    let handle = get_cf_handle(db, CF_WRITE)?;
    let mut sampler = db
        .block_sampler(handle, Some(unsafe_snap))
        .map_err(r2e)?;
    let mut collector = Collector {
        read_ts,
        spacing,
        seed: opts.seed,
        level: 0,
        file_number: 0,
        prev_index_key: vec![],
        cur: vec![],
        decided: false,
        owned: false,
        first_entry: false,
        block_id: 0,
        last_rocks_key: vec![],
        out: vec![],
    };
    let block_stats = sampler
        .sample(Some(start), Some(end), spacing, opts.seed, &mut collector)
        .map_err(r2e)?;
    let mut stats = MvccBlockSampleStats {
        blocks_in_range: block_stats.blocks_in_range,
        blocks_selected: block_stats.blocks_selected,
        memtable_entries: block_stats.memtable_entries,
        candidates: collector.out.len() as u64,
        ..Default::default()
    };
    let mut live = Vec::with_capacity(collector.out.len());
    for mut c in collector.out {
        if c.needs_forward && !resolve_forward(&mut sampler, &mut c, read_ts)? {
            continue;
        }
        if shadowed_by_newer(&mut sampler, &c, read_ts)? {
            stats.shadowed += 1;
            continue;
        }
        if !c.is_put {
            stats.deleted += 1;
            continue;
        }
        live.push(c);
    }
    // Long values are read from CF_DEFAULT in key order with one iterator, so
    // the rows of one sampled block read the few default blocks they share
    // once, as a scan would.
    live.sort_unstable_by(|a, b| a.user_key.cmp(&b.user_key));
    let mut default_iter = None;
    for c in live {
        match c.short_value {
            Some(ref v) => f(&c.user_key, v, c.block_id),
            None => {
                if default_iter.is_none() {
                    let mut iter_opts = IterOptions::default();
                    iter_opts.set_fill_cache(false);
                    default_iter = Some(snap.iterator_opt(CF_DEFAULT, iter_opts)?);
                }
                let iter = default_iter.as_mut().unwrap();
                let key = with_ts(&c.user_key, c.start_ts);
                if !iter.seek(&key)? || iter.key() != key.as_slice() {
                    // The value was removed under the snapshot; skip the row.
                    continue;
                }
                f(&c.user_key, iter.value(), c.block_id);
            }
        }
        stats.rows += 1;
    }
    Ok(Some(stats))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use engine_traits::{
        CF_DEFAULT, CF_LOCK, CF_WRITE, CompactExt, KvEngine, ManualCompactionOptions, MiscExt,
        Mutable, MvccBlockSampleOptions, MvccBlockSampleStats, Snapshot, WriteBatch, WriteBatchExt,
    };
    use rocksdb::BlockBasedOptions;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use crate::{
        RocksCfOptions, RocksDbOptions, RocksEngine,
        util::{FixedSuffixSliceTransform, new_engine_opt},
    };

    const ROWS: u64 = 3000;
    const READ_TS: u64 = 45;

    fn cf_opts() -> RocksCfOptions {
        let mut opts = RocksCfOptions::default();
        let mut block = BlockBasedOptions::new();
        // Small blocks put the versions of many rows across block bounds.
        block.set_block_size(1024);
        block.set_bloom_filter(10.0, false);
        block.set_whole_key_filtering(false);
        opts.set_block_based_table_factory(&block);
        opts.set_prefix_extractor(
            "FixedSuffixSliceTransform",
            FixedSuffixSliceTransform::new(8),
        )
        .unwrap();
        opts.set_disable_auto_compactions(true);
        opts
    }

    fn user_key(h: u64) -> Key {
        Key::from_raw(format!("row{:06}", h).as_bytes())
    }

    fn value(h: u64, ts: u64) -> Vec<u8> {
        // Every 7th row has a long value, which lives in CF_DEFAULT.
        let len = if h % 7 == 0 { 300 } else { 20 };
        let mut v = format!("v{}@{}", h, ts).into_bytes();
        v.resize(len, b'.');
        v
    }

    /// Writes one MVCC record and keeps `truth` as the visible value of each
    /// row at READ_TS.
    fn write(
        engine: &RocksEngine,
        truth: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        h: u64,
        commit_ts: u64,
        write_type: WriteType,
    ) {
        let key = user_key(h);
        let start_ts = commit_ts - 1;
        let mut wb = engine.write_batch();
        let short = match write_type {
            WriteType::Put => {
                let v = value(h, commit_ts);
                if v.len() > 255 {
                    let dk = keys::data_key(key.clone().append_ts(start_ts.into()).as_encoded());
                    wb.put_cf(CF_DEFAULT, &dk, &v).unwrap();
                    None
                } else {
                    Some(v)
                }
            }
            _ => None,
        };
        let w = Write::new(write_type, TimeStamp::new(start_ts), short);
        let wk = keys::data_key(key.clone().append_ts(commit_ts.into()).as_encoded());
        wb.put_cf(CF_WRITE, &wk, &w.as_ref().to_bytes()).unwrap();
        wb.write().unwrap();
        if commit_ts <= READ_TS {
            let uk = keys::data_key(key.as_encoded());
            match write_type {
                WriteType::Put => {
                    truth.insert(uk, Some(value(h, commit_ts)));
                }
                WriteType::Delete => {
                    truth.insert(uk, None);
                }
                _ => {}
            }
        }
    }

    /// Bottom level, two L0 files and a memtable, with updates, deletes,
    /// locks, rollbacks and versions newer than READ_TS.
    fn build() -> (tempfile::TempDir, RocksEngine, BTreeMap<Vec<u8>, Vec<u8>>) {
        let dir = tempfile::Builder::new()
            .prefix("block_sample")
            .tempdir()
            .unwrap();
        let engine = new_engine_opt(
            dir.path().to_str().unwrap(),
            RocksDbOptions::default(),
            vec![
                (CF_DEFAULT, cf_opts()),
                (CF_LOCK, cf_opts()),
                (CF_WRITE, cf_opts()),
            ],
        )
        .unwrap();
        let mut truth = BTreeMap::new();
        for h in 0..ROWS {
            write(&engine, &mut truth, h, 10, WriteType::Put);
        }
        engine.flush_cfs(&[], true).unwrap();
        for cf in [CF_DEFAULT, CF_WRITE] {
            engine
                .compact_range_cf(cf, None, None, ManualCompactionOptions::new(false, 1, true))
                .unwrap();
        }
        for h in 0..ROWS {
            if h % 3 == 0 {
                write(&engine, &mut truth, h, 20, WriteType::Put);
            }
            if h % 11 == 0 {
                write(&engine, &mut truth, h, 22, WriteType::Lock);
            }
            if h % 5 == 0 {
                write(&engine, &mut truth, h, 25, WriteType::Rollback);
            }
        }
        engine.flush_cfs(&[], true).unwrap();
        for h in (0..ROWS).filter(|h| h % 4 == 0) {
            write(&engine, &mut truth, h, 30, WriteType::Delete);
        }
        engine.flush_cfs(&[], true).unwrap();
        for h in 0..ROWS {
            if h % 6 == 0 {
                write(&engine, &mut truth, h, 40, WriteType::Put);
            }
            if h % 13 == 0 {
                write(&engine, &mut truth, h, 50, WriteType::Put);
            }
        }
        let live = truth
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect();
        (dir, engine, live)
    }

    fn sample(
        engine: &RocksEngine,
        spacing: u64,
        seed: u64,
    ) -> (Vec<(Vec<u8>, Vec<u8>)>, MvccBlockSampleStats) {
        let snap = engine.snapshot();
        let opts = MvccBlockSampleOptions { spacing, seed };
        let mut rows = vec![];
        let stats = snap
            .sample_mvcc_rows(
                keys::DATA_MIN_KEY,
                keys::DATA_MAX_KEY,
                READ_TS,
                &opts,
                &mut |k, v, _| rows.push((k.to_vec(), v.to_vec())),
            )
            .unwrap()
            .unwrap();
        (rows, stats)
    }

    #[test]
    fn test_sample_all_blocks_returns_the_visible_rows() {
        let (_dir, engine, live) = build();
        let (mut rows, stats) = sample(&engine, 1, 7);
        rows.sort();
        let expected: Vec<_> = live.into_iter().collect();
        assert_eq!(rows.len(), expected.len(), "{:?}", stats);
        assert_eq!(rows, expected);
        assert_eq!(stats.blocks_selected, stats.blocks_in_range);
    }

    #[test]
    fn test_sampled_rows_are_visible_and_unbiased() {
        let (_dir, engine, live) = build();
        let mut total = 0;
        let seeds = 40;
        for seed in 0..seeds {
            let (rows, stats) = sample(&engine, 4, seed);
            assert!(
                stats.blocks_selected < stats.blocks_in_range / 2,
                "{:?}",
                stats
            );
            for (k, v) in &rows {
                assert_eq!(
                    live.get(k),
                    Some(v),
                    "seed {} returned a row that is not visible",
                    seed
                );
            }
            total += rows.len();
        }
        let estimate = total as f64 * 4.0 / seeds as f64;
        let error = (estimate - live.len() as f64).abs() / live.len() as f64;
        assert!(error < 0.05, "estimate={} live={}", estimate, live.len());
    }
}
