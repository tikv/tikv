// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use engine_traits::{
    KvEngine, Range, RangeStats, TableProperties, TablePropertiesCollection,
    UserCollectedProperties, CF_WRITE,
};
use fail::fail_point;
use thiserror::Error;
use tikv_util::{box_try, error, info, time::Instant, warn, worker::Runnable};
use txn_types::TimeStamp;

use super::metrics::{CHECK_THEN_COMPACT_DURATION, COMPACT_RANGE_CF};

type Key = Vec<u8>;

pub enum Task {
    Compact {
        cf_name: String,
        start_key: Option<Key>, // None means smallest key
        end_key: Option<Key>,   // None means largest key
    },

    CheckAndCompact {
        // Column families need to compact
        cf_names: Vec<String>,
        // Ranges need to check
        ranges: Vec<Key>,
        // The minimum RocksDB tombstones/duplicate versions a range that need compacting has
        compact_threshold: CompactThreshold,
    },

    CheckThenCompactTopN {
        // Column families need to compact
        cf_names: Vec<String>,
        // Ranges need to check
        ranges: Vec<Key>,
        // The minimum RocksDB tombstones/duplicate versions a range that need compacting has
        compact_threshold: CompactThreshold,
        compaction_filter_enabled: bool,
        bottommost_level_force: bool,
        top_n: usize,
        gc_safe_point: u64,
        // RAII guard to indicate the task is still running. Store FSM will hold a weak ptr to this
        // guard, and will be aware of the task being dropped.
        // If the task is dropped, it means the check is finished or failed.
        // If the task is still running, it will block future checks.
        // Drop this guard *after* compaction is scheduled, to prevent periodic checks by Store FSM
        // races with the compaction tasks.
        finished: Arc<()>,
    },
}

pub struct CompactThreshold {
    pub tombstones_num_threshold: u64,
    pub tombstones_percent_threshold: u64,
    pub redundant_rows_threshold: u64,
    pub redundant_rows_percent_threshold: u64,
}

impl CompactThreshold {
    pub fn new(
        tombstones_num_threshold: u64,
        tombstones_percent_threshold: u64,
        redundant_rows_threshold: u64,
        redundant_rows_percent_threshold: u64,
    ) -> Self {
        Self {
            tombstones_num_threshold,
            tombstones_percent_threshold,
            redundant_rows_percent_threshold,
            redundant_rows_threshold,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Compact {
                ref cf_name,
                ref start_key,
                ref end_key,
            } => f
                .debug_struct("Compact")
                .field("cf_name", cf_name)
                .field(
                    "start_key",
                    &start_key.as_ref().map(|k| log_wrappers::Value::key(k)),
                )
                .field(
                    "end_key",
                    &end_key.as_ref().map(|k| log_wrappers::Value::key(k)),
                )
                .finish(),
            Task::CheckAndCompact {
                ref cf_names,
                ref ranges,
                ref compact_threshold,
            } => f
                .debug_struct("CheckAndCompact")
                .field("cf_names", cf_names)
                .field(
                    "ranges",
                    &(
                        ranges.first().as_ref().map(|k| log_wrappers::Value::key(k)),
                        ranges.last().as_ref().map(|k| log_wrappers::Value::key(k)),
                    ),
                )
                .field(
                    "tombstones_num_threshold",
                    &compact_threshold.tombstones_num_threshold,
                )
                .field(
                    "tombstones_percent_threshold",
                    &compact_threshold.tombstones_percent_threshold,
                )
                .field(
                    "redundant_rows_threshold",
                    &compact_threshold.redundant_rows_threshold,
                )
                .field(
                    "redundant_rows_percent_threshold",
                    &compact_threshold.redundant_rows_percent_threshold,
                )
                .finish(),
            Task::CheckThenCompactTopN {
                ref cf_names,
                ref compact_threshold,
                ref compaction_filter_enabled,
                ref bottommost_level_force,
                ref top_n,
                ref gc_safe_point,
                ..
            } => f
                .debug_struct("CheckThenCompactV2")
                .field("cf_names", &cf_names)
                .field(
                    "tombstones_num_threshold",
                    &compact_threshold.tombstones_num_threshold,
                )
                .field(
                    "tombstones_percent_threshold",
                    &compact_threshold.tombstones_percent_threshold,
                )
                .field(
                    "redundant_rows_threshold",
                    &compact_threshold.redundant_rows_threshold,
                )
                .field(
                    "redundant_rows_percent_threshold",
                    &compact_threshold.redundant_rows_percent_threshold,
                )
                .field("compaction_filter_enabled", &compaction_filter_enabled)
                .field("bottommost_level_force", &bottommost_level_force)
                .field("top_n", &top_n)
                .field("gc_safe_point", &gc_safe_point)
                .finish(),
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("compact failed {0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub struct Runner<E> {
    engine: E,
}

impl<E> Runner<E>
where
    E: KvEngine,
{
    pub fn new(engine: E) -> Runner<E> {
        Runner { engine }
    }

    /// Sends a compact range command to RocksDB to compact the range of the cf.
    pub fn compact_range_cf(
        &mut self,
        cf_name: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<(), Error> {
        fail_point!("on_compact_range_cf");
        let timer = Instant::now();
        box_try!(
            self.engine
                .compact_range_cf(cf_name, start_key, end_key, false, 1 /* threads */,)
        );
        info!(
            "compact range finished";
            "range_start" => start_key.map(::log_wrappers::Value::key),
            "range_end" => end_key.map(::log_wrappers::Value::key),
            "cf" => cf_name,
            "time_takes" => ?timer.saturating_elapsed(),
        );
        Ok(())
    }
}

impl<E> Runnable for Runner<E>
where
    E: KvEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Compact {
                cf_name,
                start_key,
                end_key,
            } => {
                let cf = &cf_name;
                let compact_range_timer = COMPACT_RANGE_CF
                    .with_label_values(&[cf])
                    .start_coarse_timer();
                if let Err(e) = self.compact_range_cf(cf, start_key.as_deref(), end_key.as_deref())
                {
                    error!("execute compact range failed"; "cf" => cf, "err" => %e);
                }
                compact_range_timer.observe_duration();
            }
            Task::CheckAndCompact {
                cf_names,
                ranges,
                compact_threshold,
            } => match collect_ranges_need_compact(&self.engine, ranges, compact_threshold) {
                Ok(mut ranges) => {
                    for (start, end) in ranges.drain(..) {
                        for cf in &cf_names {
                            if let Err(e) = self.compact_range_cf(cf, Some(&start), Some(&end)) {
                                error!(
                                    "compact range failed";
                                    "range_start" => log_wrappers::Value::key(&start),
                                    "range_end" => log_wrappers::Value::key(&end),
                                    "cf" => cf,
                                    "err" => %e,
                                );
                            }
                        }
                        fail_point!("raftstore::compact::CheckAndCompact:AfterCompact");
                    }
                }
                Err(e) => warn!("check ranges need reclaim failed"; "err" => %e),
            },
            Task::CheckThenCompactTopN {
                cf_names,
                ranges,
                compact_threshold,
                compaction_filter_enabled,
                bottommost_level_force: _,
                top_n,
                gc_safe_point,
                finished: _,
            } => {
                fail_point!("raftstore::compact::CheckThenCompactTopN:NotifyStart");
                fail_point!("raftstore::compact::CheckThenCompactTopN:Start");
                match select_compaction_candidates(
                    &self.engine,
                    ranges,
                    compact_threshold,
                    compaction_filter_enabled,
                    top_n,
                    gc_safe_point,
                ) {
                    Ok(candidates) => {
                        fail_point!(
                            "raftstore::compact::CheckThenCompactTopN:CheckRange2",
                            candidates.len() == 3
                                && candidates
                                    .iter()
                                    .any(|c| c.num_total_entries == 10 && c.num_discardable == 5),
                            |_| {
                                info!("Found candidate with 10 entries (range 2)");
                            }
                        );
                        fail_point!(
                            "raftstore::compact::CheckThenCompactTopN:CheckRange4",
                            candidates.len() == 3
                                && candidates
                                    .iter()
                                    .any(|c| c.num_total_entries == 20 && c.num_discardable == 7),
                            |_| {
                                info!("Found candidate with 20 entries (range 4)");
                            }
                        );
                        fail_point!(
                            "raftstore::compact::CheckThenCompactTopN:CheckRange5",
                            candidates.len() == 3
                                && candidates
                                    .iter()
                                    .any(|c| c.num_total_entries == 30 && c.num_discardable == 10),
                            |_| {
                                info!("Found candidate with 30 entries (range 5)");
                            }
                        );
                        if candidates.is_empty() {
                            // No ranges need compacting.
                            info!("no ranges need compacting");
                        } else {
                            for CompactionCandidate {
                                score,
                                start_key,
                                end_key,
                                num_tombstones,
                                num_discardable,
                                num_total_entries,
                            } in candidates
                            {
                                info!("check_then_compact found a range to compact";
                                    "score" => score,
                                    "num_tombstones" => num_tombstones,
                                    "num_discardable" => num_discardable,
                                    "num_total_entries" => num_total_entries,
                                );
                                let start_time =
                                    CHECK_THEN_COMPACT_DURATION.compact.start_coarse_timer();
                                for cf in &cf_names {
                                    if let Err(e) =
                                        self.compact_range_cf(cf, Some(&start_key), Some(&end_key))
                                    {
                                        error!(
                                            "compact range failed";
                                            "range_start" => log_wrappers::Value::key(&start_key),
                                            "range_end" => log_wrappers::Value::key(&end_key),
                                            "cf" => cf,
                                            "err" => %e,
                                        );
                                    }
                                }
                                start_time.observe_duration();
                            }
                        }
                    }
                    Err(e) => {
                        error!("get top n ranges to compact failed"; "err" => %e);
                    }
                }
                fail_point!("raftstore::compact::CheckThenCompactTopN:AfterCompact");
            }
        }
    }
}

pub fn need_compact(range_stats: &RangeStats, compact_threshold: &CompactThreshold) -> bool {
    if range_stats.num_entries < range_stats.num_versions {
        return false;
    }

    // We trigger region compaction when their are to many tombstones as well as
    // redundant keys, both of which can severly impact scan operation:
    let estimate_num_del = range_stats.num_entries - range_stats.num_versions;
    let redundant_keys = range_stats.redundant_keys();
    (redundant_keys >= compact_threshold.redundant_rows_threshold
        && redundant_keys * 100
            >= compact_threshold.redundant_rows_percent_threshold * range_stats.num_entries)
        || (estimate_num_del >= compact_threshold.tombstones_num_threshold
            && estimate_num_del * 100
                >= compact_threshold.tombstones_percent_threshold * range_stats.num_entries)
}

fn collect_ranges_need_compact(
    engine: &impl KvEngine,
    ranges: Vec<Key>,
    compact_threshold: CompactThreshold,
) -> Result<VecDeque<(Key, Key)>, Error> {
    // Check the SST properties for each range, and TiKV will compact a range if the
    // range contains too many RocksDB tombstones. TiKV will merge multiple
    // neighboring ranges that need compacting into a single range.
    let mut ranges_need_compact = VecDeque::new();
    let mut compact_start = None;
    let mut compact_end = None;
    for range in ranges.windows(2) {
        // Get total entries and total versions in this range and checks if it needs to
        // be compacted.
        if let Some(range_stats) = box_try!(engine.get_range_stats(CF_WRITE, &range[0], &range[1]))
        {
            if need_compact(&range_stats, &compact_threshold) {
                if compact_start.is_none() {
                    // The previous range doesn't need compacting.
                    compact_start = Some(range[0].clone());
                }
                compact_end = Some(range[1].clone());
                // Move to next range.
                continue;
            }
        }

        // Current range doesn't need compacting, save previous range that need
        // compacting.
        if compact_start.is_some() {
            assert!(compact_end.is_some());
        }
        if let (Some(cs), Some(ce)) = (compact_start, compact_end) {
            ranges_need_compact.push_back((cs, ce));
        }
        compact_start = None;
        compact_end = None;
    }

    // Save the last range that needs to be compacted.
    if compact_start.is_some() {
        assert!(compact_end.is_some());
    }
    if let (Some(cs), Some(ce)) = (compact_start, compact_end) {
        ranges_need_compact.push_back((cs, ce));
    }

    Ok(ranges_need_compact)
}

fn get_compact_score(
    num_tombstones: u64,
    num_discardable: u64,
    num_total_entries: u64,
    compact_threshold: &CompactThreshold,
    compaction_filter_enabled: bool,
) -> f64 {
    if num_total_entries == 0 || num_total_entries < num_discardable {
        return 0.0;
    }
    if !compaction_filter_enabled {
        // Only consider deletes (tombstones)
        let ratio = num_tombstones as f64 / num_total_entries as f64;
        if num_tombstones < compact_threshold.tombstones_num_threshold
            && ratio < compact_threshold.tombstones_percent_threshold as f64 / 100.0
        {
            return 0.0;
        }
        return num_tombstones as f64 * ratio;
    }
    // When compaction filter is enabled, ignore tombstone threshold,
    // just add deletes to redundant keys for scoring.
    let ratio = (num_tombstones + num_discardable) as f64 / num_total_entries as f64;
    if num_discardable < compact_threshold.redundant_rows_threshold
        && ratio < compact_threshold.redundant_rows_percent_threshold as f64 / 100.0
    {
        return 0.0;
    }
    num_discardable as f64 * ratio
}

#[derive(Debug, Clone)]
struct CompactionCandidate {
    score: f64,
    start_key: Key,
    end_key: Key,
    num_tombstones: u64,  // RocksDB tombstones
    num_discardable: u64, // Estimated discardable TiKV MVCC versions
    num_total_entries: u64,
}

impl PartialEq for CompactionCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for CompactionCandidate {}

impl PartialOrd for CompactionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Ord for CompactionCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Estimates the number of discardable MVCC entries based on GC safe point.
///
/// This function assumes uniform distribution of timestamps across the time
/// range [oldest_ts, newest_ts] and calculates how many entries fall before the
/// GC safe point.
///
/// Used for two types of discardable entries:
/// 1. Stale versions: num_versions - num_rows (redundant versions from updates)
/// 2. Delete versions: num_deletes (deleted rows)
///
/// The uniform distribution assumption works well in practice for estimating
/// compaction benefits without expensive range scans.
fn get_estimated_discardable_entries(
    num_entries: u64,
    oldest_ts: TimeStamp,
    newest_ts: TimeStamp,
    gc_safe_point: u64,
) -> u64 {
    // If there are no entries or the timestamps are invalid, return 0.
    if num_entries == 0 || oldest_ts > newest_ts {
        return 0;
    }
    let oldest_ts = oldest_ts.into_inner();
    let newest_ts = newest_ts.into_inner();

    // If gc_safe_point is before or equal to oldest_ts, all entries are
    // discardable.
    if gc_safe_point >= newest_ts {
        return num_entries;
    }

    // If gc_safe_point is after or equal to newest_ts, no entries are discardable.
    if gc_safe_point < oldest_ts {
        return 0;
    }

    // Otherwise, calculate the portion of entries between oldest_ts and
    // gc_safe_point.
    let total_range = newest_ts - oldest_ts;
    let discardable_range = gc_safe_point - oldest_ts;

    // Use floating point division for accuracy, then round to nearest integer.
    let portion = (discardable_range as f64) / (total_range as f64);
    (num_entries as f64 * portion).round() as u64
}

fn select_compaction_candidates(
    engine: &impl KvEngine,
    ranges: Vec<Key>,
    compact_threshold: CompactThreshold,
    compaction_filter_enabled: bool,
    top_n: usize,
    gc_safe_point: u64,
) -> Result<Vec<CompactionCandidate>, Error> {
    use std::{cmp::Reverse, collections::BinaryHeap};
    let check_timer = CHECK_THEN_COMPACT_DURATION.check.start_coarse_timer();

    let capacity = ranges.len().saturating_sub(1);
    let mut candidates = if top_n == 0 {
        Vec::with_capacity(capacity)
    } else {
        Vec::new()
    };
    let mut heap = if top_n != 0 {
        BinaryHeap::with_capacity(top_n + 1)
    } else {
        BinaryHeap::new()
    };

    for range in ranges.windows(2) {
        let mut num_tombstones = 0;
        let mut num_discardable = 0;
        let mut num_total_entries = 0;
        let collection = engine
            .table_properties_collection(CF_WRITE, &[Range::new(&range[0], &range[1])])
            .map_err(|e| Error::Other(Box::new(e)))?;
        collection.iter_table_properties(|table_prop| {
            let num_entries = table_prop.get_num_entries();
            num_total_entries += num_entries;

            if let Some(mvcc_properties) = table_prop
                .get_user_collected_properties()
                .get_mvcc_properties()
            {
                // RocksDB tombstones are guaranteed to be discardable
                num_tombstones += num_entries - mvcc_properties.num_versions;
                if compaction_filter_enabled {
                    // Estimate discardable TiKV MVCC delete versions
                    // These are deleted rows that can be physically removed after GC safe point
                    num_discardable += get_estimated_discardable_entries(
                        mvcc_properties.num_deletes,
                        mvcc_properties.oldest_delete_ts,
                        mvcc_properties.newest_delete_ts,
                        gc_safe_point,
                    );
                    // Estimate discardable stale MVCC versions
                    // These are redundant versions from row updates that can be physically removed
                    // after GC safe point
                    num_discardable += get_estimated_discardable_entries(
                        mvcc_properties.num_versions - mvcc_properties.num_rows,
                        mvcc_properties.oldest_stale_version_ts,
                        mvcc_properties.newest_stale_version_ts,
                        gc_safe_point,
                    );
                }
            }

            true
        });
        let score = get_compact_score(
            num_tombstones,
            num_discardable,
            num_total_entries,
            &compact_threshold,
            compaction_filter_enabled,
        );
        if score <= 0.0 {
            continue;
        }
        let candidate = CompactionCandidate {
            score,
            start_key: range[0].clone(),
            end_key: range[1].clone(),
            num_tombstones,
            num_discardable,
            num_total_entries,
        };

        if top_n == 0 {
            candidates.push(candidate);
        } else if heap.len() < top_n
            || score
                > heap
                    .peek()
                    .map(|r: &Reverse<CompactionCandidate>| r.0.score)
                    .unwrap_or(f64::MIN)
        {
            heap.push(Reverse(candidate));
            if heap.len() > top_n {
                heap.pop();
            }
        }
    }
    check_timer.observe_duration();

    if top_n == 0 {
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(candidates)
    } else {
        let mut result: Vec<_> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(t)| t)
            .collect();
        result.reverse();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{new_engine, new_engine_opt, KvTestEngine},
    };
    use engine_traits::{
        CompactExt, MiscExt, Mutable, SyncMutable, WriteBatch, WriteBatchExt, CF_DEFAULT, CF_LOCK,
        CF_RAFT, CF_WRITE,
    };
    use keys::data_key;
    use tempfile::Builder;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

    #[test]
    fn test_disable_manual_compaction() {
        let path = Builder::new()
            .prefix("test_disable_manual_compaction")
            .tempdir()
            .unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

        // Generate the first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Generate another SST file has the same content with first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();

        // Stop the assistant.
        {
            let _ = db.disable_manual_compaction();

            // Manually compact range.
            let _ = db.compact_range_cf(CF_DEFAULT, None, None, false, 1);

            // Get the total SST files size after compact range.
            let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
            assert_eq!(old_sst_files_size, new_sst_files_size);
        }
        // Restart the assistant.
        {
            let _ = db.enable_manual_compaction();

            // Manually compact range.
            let _ = db.compact_range_cf(CF_DEFAULT, None, None, false, 1);

            // Get the total SST files size after compact range.
            let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
            assert!(old_sst_files_size > new_sst_files_size);
        }
    }

    #[test]
    fn test_compact_range() {
        let path = Builder::new()
            .prefix("compact-range-test")
            .tempdir()
            .unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

        let mut runner = Runner::new(db.clone());

        // Generate the first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Generate another SST file has the same content with first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();

        // Schedule compact range task.
        runner.run(Task::Compact {
            cf_name: String::from(CF_DEFAULT),
            start_key: None,
            end_key: None,
        });
        sleep(Duration::from_secs(5));

        // Get the total SST files size after compact range.
        let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
        assert!(old_sst_files_size > new_sst_files_size);
    }

    fn mvcc_put(db: &KvTestEngine, k: &[u8], v: &[u8], start_ts: TimeStamp, commit_ts: TimeStamp) {
        let k = Key::from_encoded(data_key(k)).append_ts(commit_ts);
        let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
        db.put_cf(CF_WRITE, k.as_encoded(), &w.as_ref().to_bytes())
            .unwrap();
    }

    fn delete(db: &KvTestEngine, k: &[u8], commit_ts: TimeStamp) {
        let k = Key::from_encoded(data_key(k)).append_ts(commit_ts);
        db.delete_cf(CF_WRITE, k.as_encoded()).unwrap();
    }

    fn open_db(path: &str) -> KvTestEngine {
        let db_opts = DbOptions::default();
        let mut cf_opts = CfOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(8);
        let cfs_opts = vec![
            (CF_DEFAULT, CfOptions::new()),
            (CF_RAFT, CfOptions::new()),
            (CF_LOCK, CfOptions::new()),
            (CF_WRITE, cf_opts),
        ];
        new_engine_opt(path, db_opts, cfs_opts).unwrap()
    }

    #[test]
    fn test_check_space_redundancy() {
        let tmp_dir = Builder::new().prefix("test").tempdir().unwrap();
        let engine = open_db(tmp_dir.path().to_str().unwrap());

        // mvcc_put 0..5
        for i in 0..5 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 3.into(), 4.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        // gc 0..5
        for i in 0..5 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 4.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let (start, end) = (data_key(b"k0"), data_key(b"k5"));
        let range_stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(range_stats.num_entries, 15);
        assert_eq!(range_stats.num_versions, 10);
        assert_eq!(range_stats.num_rows, 5);

        // mvcc_put 5..10
        for i in 5..10 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        for i in 5..8 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 3.into(), 4.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let range_stats = engine.get_range_stats(CF_WRITE, &s, &e).unwrap().unwrap();
        assert_eq!(range_stats.num_entries, 8);
        assert_eq!(range_stats.num_versions, 8);
        assert_eq!(range_stats.num_rows, 5);

        // tombstone triggers compaction
        let ranges_need_to_compact = collect_ranges_need_compact(
            &engine,
            vec![data_key(b"k0"), data_key(b"k5"), data_key(b"k9")],
            CompactThreshold::new(4, 30, 100, 100),
        )
        .unwrap();
        let (s, e) = (data_key(b"k0"), data_key(b"k5"));
        let mut expected_ranges = VecDeque::new();
        expected_ranges.push_back((s, e));
        assert_eq!(ranges_need_to_compact, expected_ranges);

        // duplicated mvcc triggers compaction
        let ranges_need_to_compact = collect_ranges_need_compact(
            &engine,
            vec![data_key(b"k0"), data_key(b"k5"), data_key(b"k9")],
            CompactThreshold::new(100, 100, 5, 50),
        )
        .unwrap();
        assert_eq!(ranges_need_to_compact, expected_ranges);

        // gc 5..8
        for i in 5..8 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 4.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let range_stats = engine.get_range_stats(CF_WRITE, &s, &e).unwrap().unwrap();
        assert_eq!(range_stats.num_entries, 11);
        assert_eq!(range_stats.num_versions, 8);
        assert_eq!(range_stats.num_rows, 5);

        let ranges_need_to_compact = collect_ranges_need_compact(
            &engine,
            vec![data_key(b"k0"), data_key(b"k5"), data_key(b"k9")],
            CompactThreshold::new(3, 25, 100, 100),
        )
        .unwrap();
        let (s, e) = (data_key(b"k0"), data_key(b"k9"));
        let mut expected_ranges = VecDeque::new();
        expected_ranges.push_back((s, e));
        assert_eq!(ranges_need_to_compact, expected_ranges);

        let ranges_need_to_compact = collect_ranges_need_compact(
            &engine,
            vec![data_key(b"k0"), data_key(b"k5"), data_key(b"k9")],
            CompactThreshold::new(100, 100, 3, 35),
        )
        .unwrap();
        assert_eq!(ranges_need_to_compact, expected_ranges);
    }
}
