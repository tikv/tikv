// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    time::Duration,
};

use engine_traits::{KvEngine, RangeStats, CF_WRITE};
use fail::fail_point;
use thiserror::Error;
use tikv_util::{box_try, debug, error, info, time::Instant, warn, worker::Runnable};

use super::metrics::{COMPACT_RANGE_CF, FULL_COMPACT};

type Key = Vec<u8>;

pub enum Task {
    PeriodicFullCompact {
        // Ranges, or empty if we wish to compact the entire store
        ranges: Vec<(Key, Key)>,
        compact_load_controller: FullCompactController,
    },

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
}

type CompactPredicateFn = Box<dyn Fn() -> bool + Send>;

pub struct FullCompactController {
    pub initial_pause_duration_secs: u64,
    pub max_pause_duration_secs: u64,
    pub incremental_compaction_pred: Option<CompactPredicateFn>,
}

impl fmt::Debug for FullCompactController {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FullCompactController")
            .field(
                "initial_pause_duration_secs",
                &self.initial_pause_duration_secs,
            )
            .field("max_pause_duration_secs", &self.max_pause_duration_secs)
            .field(
                "has_incremental_compaction_pred",
                &self.incremental_compaction_pred.is_some(),
            )
            .finish()
    }
}
impl FullCompactController {
    pub fn new(
        initial_pause_duration_secs: u64,
        max_pause_duration_secs: u64,
        incremental_compaction_pred: Option<CompactPredicateFn>,
    ) -> Self {
        Self {
            initial_pause_duration_secs,
            max_pause_duration_secs,
            incremental_compaction_pred,
        }
    }

    /// Pause until `incremental_compaction_pred` is true.
    /// TODO: support a timeout and return an Error if timeout is reached.
    pub fn pause(&self) -> Result<(), Error> {
        if self.incremental_compaction_pred.is_none() {
            return Ok(());
        }
        let mut duration_secs = self.initial_pause_duration_secs;
        loop {
            std::thread::sleep(Duration::from_secs(duration_secs));
            if (self.incremental_compaction_pred.as_ref().unwrap())() {
                break;
            }
            duration_secs = self.max_pause_duration_secs.max(duration_secs * 2);
        }
        Ok(())
    }
}

#[derive(Debug)]
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
            Task::PeriodicFullCompact {
                ref ranges,
                ref compact_load_controller,
            } => f
                .debug_struct("PeriodicFullCompact")
                .field(
                    "ranges",
                    &(
                        ranges
                            .first()
                            .map(|k| log_wrappers::Value::key(k.0.as_slice())),
                        ranges
                            .last()
                            .map(|k| log_wrappers::Value::key(k.1.as_slice())),
                    ),
                )
                .field("coompact_load_controller", compact_load_controller)
                .finish(),
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

    /// Periodic full compaction.
    ///
    /// NOTE this is an experimental feature!
    ///
    /// TODO: Support stopping a full compaction.
    pub fn full_compact(
        &mut self,
        ranges: Vec<(Key, Key)>,
        compact_controller: FullCompactController,
    ) -> Result<(), Error> {
        fail_point!("on_full_compact");
        info!("full compaction started");
        let mut ranges: VecDeque<_> = ranges
            .iter()
            .map(|(start, end)| (Some(start.as_slice()), Some(end.as_slice())))
            .collect();
        if ranges.is_empty() {
            ranges.push_front((None, None))
        }

        let timer = Instant::now();
        let full_compact_timer = FULL_COMPACT.start_coarse_timer();

        while let Some(range) = ranges.pop_front() {
            debug!(
                "incremental range full compaction started";
            "start_key" => ?range.0.map(log_wrappers::Value::key),
            "end_key" => ?range.1.map(log_wrappers::Value::key),
             );
            box_try!(self.engine.compact_range(
                range.0, range.1, // Compact the entire key range.
                false,   // non-exclusive
                1,       // number of threads threads
            ));
            debug!(
                "finished incremental range full compaction";
                "remaining" => ranges.len(),
            );
            if let Some((next_range, predicate_fn)) = ranges
                .front()
                .zip(compact_controller.incremental_compaction_pred.as_ref())
            {
                if !predicate_fn() {
                    warn!("pausing full compaction before next increment";
                    "finished_start_key" => ?range.0.map(log_wrappers::Value::key),
                    "finished_end_key" => ?range.1.map(log_wrappers::Value::key),
                    "next_range_start_key" => ?next_range.0.map(log_wrappers::Value::key),
                    "next_range_end_key" => ?next_range.1.map(log_wrappers::Value::key),
                    "remaining" => ranges.len(),
                    );
                    compact_controller.pause()?;
                    info!("resuming incremental full compaction");
                }
            }
        }

        full_compact_timer.observe_duration();
        info!(
            "full compaction finished";
            "time_takes" => ?timer.saturating_elapsed(),
        );
        Ok(())
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
        let compact_range_timer = COMPACT_RANGE_CF
            .with_label_values(&[cf_name])
            .start_coarse_timer();
        box_try!(
            self.engine
                .compact_range_cf(cf_name, start_key, end_key, false, 1 /* threads */,)
        );
        compact_range_timer.observe_duration();
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
            Task::PeriodicFullCompact {
                ranges,
                compact_load_controller,
            } => {
                if let Err(e) = self.full_compact(ranges, compact_load_controller) {
                    error!("periodic full compaction failed"; "err" => %e);
                }
            }
            Task::Compact {
                cf_name,
                start_key,
                end_key,
            } => {
                let cf = &cf_name;
                if let Err(e) = self.compact_range_cf(cf, start_key.as_deref(), end_key.as_deref())
                {
                    error!("execute compact range failed"; "cf" => cf, "err" => %e);
                }
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
    let redundant_keys = range_stats.num_entries - range_stats.num_rows;
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

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{new_engine, new_engine_opt, KvTestEngine},
    };
    use engine_traits::{
        MiscExt, Mutable, SyncMutable, WriteBatch, WriteBatchExt, CF_DEFAULT, CF_LOCK, CF_RAFT,
        CF_WRITE,
    };
    use keys::data_key;
    use tempfile::Builder;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

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

    #[test]
    fn test_full_compact_deletes() {
        let tmp_dir = Builder::new().prefix("test").tempdir().unwrap();
        let engine = open_db(tmp_dir.path().to_str().unwrap());
        let mut runner = Runner::new(engine.clone());

        // mvcc_put 0..5
        for i in 0..5 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let (start, end) = (data_key(b"k0"), data_key(b"k5"));
        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries, stats.num_versions);

        for i in 0..5 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 3.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries - stats.num_versions, 5);

        runner.run(Task::PeriodicFullCompact {
            ranges: Vec::new(),
            compact_load_controller: FullCompactController::new(0, 0, None),
        });
        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries - stats.num_versions, 0);
    }

    #[test]
    fn test_full_compact_incremental_pausable() {
        let tmp_dir = Builder::new().prefix("test").tempdir().unwrap();
        let engine = open_db(tmp_dir.path().to_str().unwrap());
        let mut runner = Runner::new(engine.clone());

        // mvcc_put 0..100
        for i in 0..100 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let (start, end) = (data_key(b"k0"), data_key(b"k5"));
        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries, stats.num_versions);

        for i in 0..100 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 3.into());
        }
        engine.flush_cf(CF_WRITE, true).unwrap();

        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries - stats.num_versions, 100);

        let started_at = Instant::now();
        let started_at_clone = started_at.clone();
        let pred_fn: CompactPredicateFn =
            Box::new(move || Instant::now() - started_at > Duration::from_millis(500));
        let ranges = vec![
            (data_key(b"k0"), data_key(b"k25")),
            (data_key(b"k25"), data_key(b"k50")),
            (data_key(b"k50"), data_key(b"k100")),
        ];
        runner.run(Task::PeriodicFullCompact {
            ranges,
            compact_load_controller: FullCompactController::new(1, 5, Some(pred_fn)),
        });
        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries - stats.num_versions, 0);
        // Verify that periodic full compact slept at least once.
        assert!(Instant::now() - started_at_clone > Duration::from_secs(1));
    }
}
