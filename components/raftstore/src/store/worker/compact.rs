// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use engine_traits::{KvEngine, ManualCompactionOptions};
use fail::fail_point;
use futures_util::compat::Future01CompatExt;
use thiserror::Error;
use tikv_util::{
    box_try, debug, error, info, time::Instant, timer::GLOBAL_TIMER_HANDLE, worker::Runnable,
};
use yatp::Remote;

use super::metrics::{
    COMPACT_RANGE_CF, FULL_COMPACT, FULL_COMPACT_INCREMENTAL, FULL_COMPACT_PAUSE,
};

type Key = Vec<u8>;

static FULL_COMPACTION_IN_PROCESS: AtomicBool = AtomicBool::new(false);

pub enum Task {
    PeriodicFullCompact {
        // Ranges, or empty if we wish to compact the entire store
        ranges: Vec<(Key, Key)>,
        compact_load_controller: FullCompactController,
    },

    Compact {
        cf_name: String,
        start_key: Option<Key>,       // None means smallest key
        end_key: Option<Key>,         // None means largest key
        bottommost_level_force: bool, // Whether force the bottommost level to compact
    },
}

type CompactPredicateFn = Box<dyn Fn() -> bool + Send + Sync>;

pub struct FullCompactController {
    /// Initial delay between retries for ``FullCompactController::pause``.
    pub initial_pause_duration_secs: u64,
    /// Max delay between retries.
    pub max_pause_duration_secs: u64,
    /// Predicate function to evaluate that indicates if we can proceed with
    /// full compaction.
    pub incremental_compaction_pred: CompactPredicateFn,
}

impl fmt::Debug for FullCompactController {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FullCompactController")
            .field(
                "initial_pause_duration_secs",
                &self.initial_pause_duration_secs,
            )
            .field("max_pause_duration_secs", &self.max_pause_duration_secs)
            .finish()
    }
}
impl FullCompactController {
    pub fn new(
        initial_pause_duration_secs: u64,
        max_pause_duration_secs: u64,
        incremental_compaction_pred: CompactPredicateFn,
    ) -> Self {
        Self {
            initial_pause_duration_secs,
            max_pause_duration_secs,
            incremental_compaction_pred,
        }
    }

    /// Pause until `incremental_compaction_pred` evaluates to `true`: delay
    /// using exponential backoff (initial value
    /// `initial_pause_duration_secs`, max value `max_pause_duration_secs`)
    /// between retries.
    pub async fn pause(&self) -> Result<(), Error> {
        let mut duration_secs = self.initial_pause_duration_secs;
        loop {
            box_try!(
                GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + Duration::from_secs(duration_secs))
                    .compat()
                    .await
            );
            if (self.incremental_compaction_pred)() {
                break;
            };
            duration_secs = self.max_pause_duration_secs.max(duration_secs * 2);
        }
        Ok(())
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
                .field("compact_load_controller", compact_load_controller)
                .finish(),
            Task::Compact {
                ref cf_name,
                ref start_key,
                ref end_key,
                ref bottommost_level_force,
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
                .field("bottommost_level_force", bottommost_level_force)
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
    remote: Remote<yatp::task::future::TaskCell>,
}

impl<E> Runner<E>
where
    E: KvEngine,
{
    pub fn new(engine: E, remote: Remote<yatp::task::future::TaskCell>) -> Runner<E> {
        Runner { engine, remote }
    }

    /// Periodic full compaction.
    /// Note: this does not accept a `&self` due to async lifetime issues.
    ///
    /// NOTE this is an experimental feature!
    ///
    /// TODO: Support stopping a full compaction.
    async fn full_compact(
        engine: E,
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
            let incremental_timer = FULL_COMPACT_INCREMENTAL.start_coarse_timer();
            box_try!(engine.compact_range(
                range.0,
                range.1, // Compact the entire key range.
                ManualCompactionOptions::new(false, 1, false),
            ));
            incremental_timer.observe_duration();
            debug!(
                "finished incremental range full compaction";
                "remaining" => ranges.len(),
            );
            // If there is at least one range remaining in `ranges` remaining, evaluate
            // `compact_controller.incremental_compaction_pred`. If `true`, proceed to next
            // range; otherwise, pause this task
            // (see `FullCompactController::pause` for details) until predicate
            // evaluates to true.
            if let Some(next_range) = ranges.front() {
                if !(compact_controller.incremental_compaction_pred)() {
                    info!("pausing full compaction before next increment";
                    "finished_start_key" => ?range.0.map(log_wrappers::Value::key),
                    "finished_end_key" => ?range.1.map(log_wrappers::Value::key),
                    "next_range_start_key" => ?next_range.0.map(log_wrappers::Value::key),
                    "next_range_end_key" => ?next_range.1.map(log_wrappers::Value::key),
                    "remaining" => ranges.len(),
                    );
                    let pause_started = Instant::now();
                    let pause_timer = FULL_COMPACT_PAUSE.start_coarse_timer();
                    compact_controller.pause().await?;
                    pause_timer.observe_duration();
                    info!("resuming incremental full compaction";
                        "paused" => ?pause_started.saturating_elapsed(),
                    );
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
        bottommost_level_force: bool,
    ) -> Result<(), Error> {
        fail_point!("on_compact_range_cf");
        let timer = Instant::now();
        let compact_options = ManualCompactionOptions::new(false, 1, bottommost_level_force);
        box_try!(self.engine.compact_range_cf(
            cf_name,
            start_key,
            end_key,
            compact_options.clone()
        ));
        info!(
            "compact range finished";
            "range_start" => start_key.map(::log_wrappers::Value::key),
            "range_end" => end_key.map(::log_wrappers::Value::key),
            "cf" => cf_name,
            "time_takes" => ?timer.saturating_elapsed(),
            "compact_options" => ?compact_options,
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
                // Since periodic full compaction is submitted as a task to the background
                // worker pool, verify we will not start full compaction if
                // another full compaction is running in the background.
                if FULL_COMPACTION_IN_PROCESS.load(Ordering::SeqCst)
                    || FULL_COMPACTION_IN_PROCESS.swap(true, Ordering::SeqCst)
                {
                    info!("full compaction is already in process, not starting");
                    return;
                };
                let engine = self.engine.clone();
                self.remote.spawn(async move {
                    if let Err(e) =
                        Self::full_compact(engine, ranges, compact_load_controller).await
                    {
                        error!("periodic full compaction failed"; "err" => %e);
                    }
                    FULL_COMPACTION_IN_PROCESS.store(false, Ordering::SeqCst);
                });
            }
            Task::Compact {
                cf_name,
                start_key,
                end_key,
                bottommost_level_force,
            } => {
                let cf = &cf_name;
                let compact_range_timer = COMPACT_RANGE_CF
                    .with_label_values(&[cf])
                    .start_coarse_timer();
                if let Err(e) = self.compact_range_cf(
                    cf,
                    start_key.as_deref(),
                    end_key.as_deref(),
                    bottommost_level_force,
                ) {
                    error!("execute compact range failed"; "cf" => cf, "err" => %e);
                }
                compact_range_timer.observe_duration();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{thread::sleep, time::Duration};

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, new_engine, new_engine_opt},
    };
    use engine_traits::{
        CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, CompactExt, MiscExt, Mutable, SyncMutable,
        WriteBatch, WriteBatchExt,
    };
    use keys::data_key;
    use tempfile::Builder;
    use tikv_util::yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder};
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use super::*;

    fn make_compact_runner<E>(engine: E) -> (FuturePool, Runner<E>)
    where
        E: KvEngine,
    {
        let pool = YatpPoolBuilder::new(DefaultTicker::default()).build_future_pool();
        (pool.clone(), Runner::new(engine, pool.remote().clone()))
    }

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
            let _ = db.compact_range_cf(
                CF_DEFAULT,
                None,
                None,
                ManualCompactionOptions::new(false, 1, true),
            );

            // Get the total SST files size after compact range.
            let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
            assert_eq!(old_sst_files_size, new_sst_files_size);
        }
        // Restart the assistant.
        {
            let _ = db.enable_manual_compaction();

            // Manually compact range.
            let _ = db.compact_range_cf(
                CF_DEFAULT,
                None,
                None,
                ManualCompactionOptions::new(false, 1, true),
            );

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
        let (_pool, mut runner) = make_compact_runner(db.clone());

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
            bottommost_level_force: false,
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
    fn test_full_compact_deletes() {
        let tmp_dir = Builder::new().prefix("test").tempdir().unwrap();
        let engine = open_db(tmp_dir.path().to_str().unwrap());
        let (_pool, mut runner) = make_compact_runner(engine.clone());

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
            compact_load_controller: FullCompactController::new(0, 0, Box::new(|| true)),
        });
        std::thread::sleep(Duration::from_millis(500));
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
        let (_pool, mut runner) = make_compact_runner(engine.clone());

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
        let pred_fn: CompactPredicateFn =
            Box::new(move || Instant::now() - started_at > Duration::from_millis(500));
        let ranges = vec![
            (data_key(b"k0"), data_key(b"k25")),
            (data_key(b"k25"), data_key(b"k50")),
            (data_key(b"k50"), data_key(b"k100")),
        ];
        runner.run(Task::PeriodicFullCompact {
            ranges,
            compact_load_controller: FullCompactController::new(1, 5, pred_fn),
        });
        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries - stats.num_versions, 100);
        std::thread::sleep(Duration::from_secs(2));
        let stats = engine
            .get_range_stats(CF_WRITE, &start, &end)
            .unwrap()
            .unwrap();
        assert_eq!(stats.num_entries - stats.num_versions, 0);
    }
}
