// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Feeds ANALYZE with rows sampled from data blocks instead of a full scan.
//!
//! When a request sets `ndv_rate < 1` and block sampling is on, every data
//! block of every sorted run is read with probability `ndv_rate` and only the
//! visible rows of the read blocks are analyzed. Each visible row is analyzed
//! with probability `ndv_rate`, as with row sampling, but the skipped blocks
//! are not read at all.

use engine_traits::{MvccBlockSampleOptions, MvccBlockSampleStats};
use kvproto::coprocessor::KeyRange;
use tidb_query_common::storage::{
    IntervalRange, OwnedKvPairEntry, PointRange, Result as QeResult, Storage,
};
use txn_types::Key;

use crate::{
    coprocessor::{Error, Result, dag::TikvStorage},
    storage::{Snapshot, SnapshotStore, Statistics},
};

/// Reads `TIKV_ANALYZE_BLOCK_SAMPLING` (block sampling is on with `on`, `1` or
/// `true`). If `TIKV_ANALYZE_BLOCK_SAMPLING_FILE`
/// names a file, its content is read on every request instead, so the switch
/// can change without a restart.
fn enabled() -> bool {
    let text = match std::env::var("TIKV_ANALYZE_BLOCK_SAMPLING_FILE") {
        Ok(path) => std::fs::read_to_string(path).unwrap_or_default(),
        Err(_) => std::env::var("TIKV_ANALYZE_BLOCK_SAMPLING").unwrap_or_default(),
    };
    let first = text.split_whitespace().next().unwrap_or_default();
    matches!(first, "on" | "1" | "true")
}

/// The rows of the sampled blocks, served in key order as if scanned.
pub(crate) struct SampledRows {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
    end: usize,
    scanned_bytes: usize,
}

/// The rows of a block sample in the order the scan returns them, with the
/// sampled block of each row when values are counted once per block.
pub(crate) struct BlockedRows {
    pub rows: SampledRows,
    /// Factor that scales the count of the rows to all visible rows.
    pub scale: f64,
    /// For NDV, a value repeated inside one block counts once (the COLLAPSE
    /// rule of Chaudhuri, Das and Srivastava, "Effective use of block-level
    /// sampling in statistics estimation", SIGMOD 2004): rows next to each
    /// other in key order share values much more than random rows do, so
    /// in-block repeats would make most values look common and hide unseen
    /// ones from the estimator.
    pub block_ids: Vec<u64>,
}

/// Samples the rows of `ranges` from data blocks with probability `ndv_rate`.
/// Returns `None` if block sampling is off or the engine cannot do it.
pub(crate) fn sample_blocks<S: Snapshot>(
    storage: &TikvStorage<SnapshotStore<S>>,
    ranges: &[KeyRange],
    ndv_rate: f64,
) -> Result<Option<BlockedRows>> {
    if !enabled() {
        return Ok(None);
    }
    // Round the spacing down, so that the blocks read are never fewer than
    // `ndv_rate` asks for. The small epsilon keeps 1 / 0.05 at 20.
    let spacing = (1.0 / ndv_rate + 1e-9).floor().max(1.0) as u64;
    // A spacing of 1 reads every block, probes every row and keeps the whole
    // region in memory, which costs more than the scan.
    if spacing < 2 {
        return Ok(None);
    }
    let opts = MvccBlockSampleOptions {
        spacing,
        seed: rand::random(),
    };
    let store = storage.store();
    let mut rows = Vec::new();
    let mut total = MvccBlockSampleStats::default();
    let mut decode_err = None;
    for range in ranges {
        let lower = Key::from_raw(range.get_start());
        let upper = Key::from_raw(range.get_end());
        let stats = store
            .sample_mvcc_rows(
                &lower,
                &upper,
                &opts,
                &mut |k, v, block| match Key::from_encoded_slice(k).into_raw() {
                    Ok(raw) => rows.push((raw, v.to_vec(), block)),
                    Err(e) => decode_err = Some(e),
                },
            )
            .map_err(Error::from)?;
        let Some(stats) = stats else {
            return Ok(None);
        };
        total.blocks_in_range += stats.blocks_in_range;
        total.blocks_selected += stats.blocks_selected;
        total.memtable_entries += stats.memtable_entries;
        total.candidates += stats.candidates;
        total.rows += stats.rows;
        total.deleted += stats.deleted;
        total.shadowed += stats.shadowed;
        total.block_reads += stats.block_reads;
    }
    if let Some(e) = decode_err {
        return Err(box_err!("decode sampled key: {:?}", e));
    }
    rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let block_ids = rows.iter().map(|r| r.2).collect();
    let rows: Vec<_> = rows.into_iter().map(|(k, v, _)| (k, v)).collect();
    info!("analyze sampled blocks"; "spacing" => spacing, "stats" => ?total);
    Ok(Some(BlockedRows {
        rows: SampledRows {
            rows,
            pos: 0,
            end: 0,
            scanned_bytes: 0,
        },
        scale: spacing as f64,
        block_ids,
    }))
}

/// The storage of a sampling ANALYZE: a normal scan, or sampled block rows.
pub(crate) enum AnalyzeStorage<S: Snapshot> {
    Scan(TikvStorage<SnapshotStore<S>>),
    Sampled(SampledRows),
}

impl<S: Snapshot> Storage for AnalyzeStorage<S> {
    type Statistics = Statistics;

    fn begin_scan(
        &mut self,
        is_backward_scan: bool,
        is_key_only: bool,
        load_commit_ts: bool,
        range: IntervalRange,
    ) -> QeResult<()> {
        match self {
            AnalyzeStorage::Scan(s) => {
                s.begin_scan(is_backward_scan, is_key_only, load_commit_ts, range)
            }
            AnalyzeStorage::Sampled(r) => {
                r.pos = r
                    .rows
                    .partition_point(|(k, _)| k.as_slice() < range.lower_inclusive.as_slice());
                r.end = r
                    .rows
                    .partition_point(|(k, _)| k.as_slice() < range.upper_exclusive.as_slice());
                Ok(())
            }
        }
    }

    fn scan_next_entry(&mut self) -> QeResult<Option<OwnedKvPairEntry>> {
        match self {
            AnalyzeStorage::Scan(s) => s.scan_next_entry(),
            AnalyzeStorage::Sampled(r) => {
                if r.pos >= r.end {
                    return Ok(None);
                }
                let (key, value) = std::mem::take(&mut r.rows[r.pos]);
                r.pos += 1;
                r.scanned_bytes += key.len() + value.len();
                Ok(Some(OwnedKvPairEntry {
                    key,
                    value,
                    commit_ts: None,
                }))
            }
        }
    }

    fn get_entry(
        &mut self,
        is_key_only: bool,
        load_commit_ts: bool,
        range: PointRange,
    ) -> QeResult<Option<OwnedKvPairEntry>> {
        match self {
            AnalyzeStorage::Scan(s) => s.get_entry(is_key_only, load_commit_ts, range),
            // Analyze reads interval ranges only.
            AnalyzeStorage::Sampled(_) => Ok(None),
        }
    }

    fn met_uncacheable_data(&self) -> Option<bool> {
        match self {
            AnalyzeStorage::Scan(s) => s.met_uncacheable_data(),
            AnalyzeStorage::Sampled(_) => None,
        }
    }

    fn collect_statistics(&mut self, dest: &mut Statistics) {
        if let AnalyzeStorage::Scan(s) = self {
            s.collect_statistics(dest)
        }
    }

    fn scanned_bytes(&self) -> usize {
        match self {
            AnalyzeStorage::Scan(s) => s.scanned_bytes(),
            AnalyzeStorage::Sampled(r) => r.scanned_bytes,
        }
    }
}
