// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use collections::{HashMap, HashSet};
use engine_traits::{CfOptions, DbOptions, KvEngine};
use futures_util::compat::Future01CompatExt;
use kvproto::{import_sstpb::*, metapb::Region};
use tikv_util::{resizable_threadpool::DeamonRuntimeHandle, timer::GLOBAL_TIMER_HANDLE};

use super::{
    Config, Result,
    util::{HashRange, range_overlaps, region_overlap_with_range},
};

pub type RocksDbMetricsFn = fn(cf: &str, name: &str, v: f64);

struct ImportModeSwitcherInner {
    // is_import used for global import mode, block all split
    is_import: Arc<AtomicBool>,
    backup_db_options: ImportModeDbOptions,
    backup_cf_options: Vec<(String, ImportModeCfOptions)>,
    timeout: Duration,
    next_check: Instant,
    metrics_fn: RocksDbMetricsFn,

    // import_mode_ranges used for regional import mode, block split for specific ranges
    // range in import mode -> timeout to restore to normal mode
    import_mode_ranges: HashMap<HashRange, Instant>,
    db_options_set: bool, // Track whether DB options have been set for import mode
}

impl ImportModeSwitcherInner {
    fn clear_import_mode_range(&mut self, range: HashRange) {
        self.import_mode_ranges.remove(&range);
    }

    fn has_import_ranges(&self) -> bool {
        !self.import_mode_ranges.is_empty()
    }

    fn should_be_in_import_mode(&self) -> bool {
        self.is_import.load(Ordering::Acquire) || self.has_import_ranges()
    }

    fn ensure_import_mode_db_options<E: KvEngine>(
        &mut self,
        db: &E,
        mf: RocksDbMetricsFn,
    ) -> Result<bool> {
        // Check if we need to set import mode DB options
        if !self.db_options_set {
            // DB options not set yet, set them now
            self.backup_db_options = ImportModeDbOptions::new_options(db);
            self.backup_cf_options.clear();

            let import_db_options = self.backup_db_options.optimized_for_import_mode();
            import_db_options.set_options(db)?;
            for cf_name in db.cf_names() {
                let cf_opts = ImportModeCfOptions::new_options(db, cf_name);
                let import_cf_options = cf_opts.optimized_for_import_mode();
                self.backup_cf_options.push((cf_name.to_owned(), cf_opts));
                import_cf_options.set_options(db, cf_name, mf)?;
            }
            self.db_options_set = true;
            return Ok(true);
        }
        Ok(false)
    }

    fn enter_normal_mode<E: KvEngine>(&mut self, db: &E, mf: RocksDbMetricsFn) -> Result<bool> {
        // Only exit import mode if both global and ranges are not in import mode
        if !self.is_import.load(Ordering::Acquire) && !self.has_import_ranges() {
            return Ok(false);
        }

        if self.has_import_ranges() {
            return Ok(false);
        }

        self.backup_db_options.set_options(db)?;
        for (cf_name, cf_opts) in &self.backup_cf_options {
            cf_opts.set_options(db, cf_name, mf)?;
        }

        info!("enter normal mode");
        self.is_import.store(false, Ordering::Release);
        self.db_options_set = false; // Reset the flag
        Ok(true)
    }

    fn enter_import_mode<E: KvEngine>(&mut self, db: &E, mf: RocksDbMetricsFn) -> Result<bool> {
        let was_global_import = self.is_import.load(Ordering::Acquire);

        // If global import mode is already set, no change needed
        if was_global_import {
            return Ok(false);
        }

        // Ensure DB options are set for import mode
        self.ensure_import_mode_db_options(db, mf)?;

        info!("enter import mode");
        self.is_import.store(true, Ordering::Release);
        Ok(true)
    }
}

#[derive(Clone)]
pub struct ImportModeSwitcher {
    inner: Arc<Mutex<ImportModeSwitcherInner>>,
    is_import: Arc<AtomicBool>,
}

impl ImportModeSwitcher {
    pub fn new(cfg: &Config) -> ImportModeSwitcher {
        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let timeout = cfg.import_mode_timeout.0;
        let is_import = Arc::new(AtomicBool::new(false));
        let inner = Arc::new(Mutex::new(ImportModeSwitcherInner {
            is_import: is_import.clone(),
            backup_db_options: ImportModeDbOptions::new(),
            backup_cf_options: Vec::new(),
            timeout,
            next_check: Instant::now() + timeout,
            metrics_fn: mf,
            import_mode_ranges: HashMap::default(),
            db_options_set: false,
        }));
        ImportModeSwitcher { inner, is_import }
    }

    // start_resizable_threads only serves for resizable runtime
    pub fn start_resizable_threads<E: KvEngine>(&self, executor: &DeamonRuntimeHandle, db: E) {
        // spawn a background future to put TiKV back into normal mode after timeout
        let inner = self.inner.clone();
        let switcher = Arc::downgrade(&inner);
        let timer_loop = async move {
            let mut prev_ranges = vec![];
            // loop until the switcher has been dropped
            while let Some(switcher) = switcher.upgrade() {
                let next_check = {
                    let now = Instant::now();
                    let mut switcher = switcher.lock().unwrap();

                    // Handle range timeouts first
                    for range in prev_ranges.drain(..) {
                        if let Some(next_check) = switcher.import_mode_ranges.get(&range) {
                            if now >= *next_check {
                                switcher.clear_import_mode_range(range);
                            }
                        }
                    }

                    // Handle global timeout
                    if now >= switcher.next_check {
                        if switcher.is_import.load(Ordering::Acquire) {
                            let mf = switcher.metrics_fn;
                            if let Err(e) = switcher.enter_normal_mode(&db, mf) {
                                error!(?e; "failed to put TiKV back into normal mode");
                            }
                        }
                        switcher.next_check = now + switcher.timeout
                    }

                    // Calculate next check time considering both global and range timeouts
                    let mut min_next_check = switcher.next_check;
                    for (range, next_check) in &switcher.import_mode_ranges {
                        if *next_check <= min_next_check {
                            if *next_check < min_next_check {
                                min_next_check = *next_check;
                                prev_ranges.clear();
                            }
                            prev_ranges.push(range.clone());
                        }
                    }
                    min_next_check
                };

                let ok = GLOBAL_TIMER_HANDLE.delay(next_check).compat().await.is_ok();

                if !ok {
                    warn!("failed to delay with global timer");
                }
            }
        };
        executor.spawn(timer_loop);
    }

    pub fn enter_normal_mode<E: KvEngine>(&self, db: &E, mf: RocksDbMetricsFn) -> Result<bool> {
        if !self.is_import.load(Ordering::Acquire) {
            return Ok(false);
        }
        self.inner.lock().unwrap().enter_normal_mode(db, mf)
    }

    pub fn enter_import_mode<E: KvEngine>(&self, db: &E, mf: RocksDbMetricsFn) -> Result<bool> {
        let mut inner = self.inner.lock().unwrap();
        let ret = inner.enter_import_mode(db, mf)?;
        inner.next_check = Instant::now() + inner.timeout;
        inner.metrics_fn = mf;
        Ok(ret)
    }

    // get_mode get SwitchMode,
    // however, after supporting regional import, it maybe useless.
    pub fn get_mode(&self) -> SwitchMode {
        let inner = self.inner.lock().unwrap();
        if inner.should_be_in_import_mode() {
            SwitchMode::Import
        } else {
            SwitchMode::Normal
        }
    }

    pub fn ranges_enter_import_mode(&self, ranges: Vec<Range>) {
        let mut inner = self.inner.lock().unwrap();

        let next_check = Instant::now() + inner.timeout;
        for range in ranges {
            let range = HashRange::from(range);
            // if the range exists before, the timeout is updated
            inner.import_mode_ranges.insert(range, next_check);
        }
    }

    pub fn clear_import_mode_range(&self, ranges: Vec<Range>) {
        let mut inner = self.inner.lock().unwrap();
        for range in ranges {
            let range = HashRange::from(range);
            inner.clear_import_mode_range(range);
        }
    }

    pub fn region_in_import_mode(&self, region: &Region) -> bool {
        let inner = self.inner.lock().unwrap();
        for r in inner.import_mode_ranges.keys() {
            if region_overlap_with_range(r, region) {
                return true;
            }
        }
        false
    }

    pub fn range_in_import_mode(&self, range: &Range) -> bool {
        let inner = self.inner.lock().unwrap();
        for r in inner.import_mode_ranges.keys() {
            if range_overlaps(r, range) {
                return true;
            }
        }
        false
    }

    pub fn ranges_in_import(&self) -> HashSet<HashRange> {
        let inner = self.inner.lock().unwrap();
        HashSet::from_iter(inner.import_mode_ranges.keys().cloned())
    }
}

struct ImportModeDbOptions {
    max_background_jobs: i32,
}

impl ImportModeDbOptions {
    fn new() -> Self {
        Self {
            max_background_jobs: 32,
        }
    }

    fn optimized_for_import_mode(&self) -> Self {
        Self {
            max_background_jobs: self.max_background_jobs.max(32),
        }
    }

    fn new_options(db: &impl KvEngine) -> ImportModeDbOptions {
        let db_opts = db.get_db_options();
        ImportModeDbOptions {
            max_background_jobs: db_opts.get_max_background_jobs(),
        }
    }

    fn set_options(&self, db: &impl KvEngine) -> Result<()> {
        let opts = [(
            "max_background_jobs".to_string(),
            self.max_background_jobs.to_string(),
        )];
        let tmp_opts: Vec<_> = opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        db.set_db_options(&tmp_opts)?;
        Ok(())
    }
}

struct ImportModeCfOptions {
    level0_stop_writes_trigger: i32,
    level0_slowdown_writes_trigger: i32,
    soft_pending_compaction_bytes_limit: u64,
    hard_pending_compaction_bytes_limit: u64,
}

impl ImportModeCfOptions {
    fn optimized_for_import_mode(&self) -> Self {
        Self {
            level0_stop_writes_trigger: self.level0_stop_writes_trigger.max(1 << 30),
            level0_slowdown_writes_trigger: self.level0_slowdown_writes_trigger.max(1 << 30),
            soft_pending_compaction_bytes_limit: 0,
            hard_pending_compaction_bytes_limit: 0,
        }
    }

    fn new_options(db: &impl KvEngine, cf_name: &str) -> ImportModeCfOptions {
        let cf_opts = db.get_options_cf(cf_name).unwrap(); //FIXME unwrap

        ImportModeCfOptions {
            level0_stop_writes_trigger: cf_opts.get_level_zero_stop_writes_trigger(),
            level0_slowdown_writes_trigger: cf_opts.get_level_zero_slowdown_writes_trigger(),
            soft_pending_compaction_bytes_limit: cf_opts.get_soft_pending_compaction_bytes_limit(),
            hard_pending_compaction_bytes_limit: cf_opts.get_hard_pending_compaction_bytes_limit(),
        }
    }

    fn set_options(&self, db: &impl KvEngine, cf_name: &str, mf: RocksDbMetricsFn) -> Result<()> {
        let opts = [
            (
                "level0_stop_writes_trigger".to_owned(),
                self.level0_stop_writes_trigger.to_string(),
            ),
            (
                "level0_slowdown_writes_trigger".to_owned(),
                self.level0_slowdown_writes_trigger.to_string(),
            ),
            (
                "soft_pending_compaction_bytes_limit".to_owned(),
                self.soft_pending_compaction_bytes_limit.to_string(),
            ),
            (
                "hard_pending_compaction_bytes_limit".to_owned(),
                self.hard_pending_compaction_bytes_limit.to_string(),
            ),
        ];

        let tmp_opts: Vec<_> = opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        db.set_options_cf(cf_name, tmp_opts.as_slice())?;
        for (key, value) in &opts {
            if let Ok(v) = value.parse::<f64>() {
                mf(cf_name, key, v);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use engine_traits::{CF_DEFAULT, KvEngine};
    use tempfile::Builder;
    use test_sst_importer::{new_test_engine, new_test_engine_with_options};
    use tikv_util::{config::ReadableDuration, resizable_threadpool::ResizableRuntime};
    use tokio::runtime::Runtime;
    type TokioResult<T> = std::io::Result<T>;

    use super::*;

    fn create_tokio_runtime(_: usize, _: &str) -> TokioResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
    }

    fn check_import_options<E>(
        db: &E,
        expected_db_opts: &ImportModeDbOptions,
        expected_cf_opts: &ImportModeCfOptions,
    ) where
        E: KvEngine,
    {
        let db_opts = db.get_db_options();
        assert_eq!(
            db_opts.get_max_background_jobs(),
            expected_db_opts.max_background_jobs,
        );

        for cf_name in db.cf_names() {
            let cf_opts = db.get_options_cf(cf_name).unwrap();
            assert_eq!(
                cf_opts.get_level_zero_stop_writes_trigger(),
                expected_cf_opts.level0_stop_writes_trigger
            );
            assert_eq!(
                cf_opts.get_level_zero_slowdown_writes_trigger(),
                expected_cf_opts.level0_slowdown_writes_trigger
            );
            assert_eq!(
                cf_opts.get_soft_pending_compaction_bytes_limit(),
                expected_cf_opts.soft_pending_compaction_bytes_limit
            );
            assert_eq!(
                cf_opts.get_hard_pending_compaction_bytes_limit(),
                expected_cf_opts.hard_pending_compaction_bytes_limit
            );
        }
    }

    #[test]
    fn test_import_mode_switcher() {
        let temp_dir = Builder::new()
            .prefix("test_import_mode_switcher")
            .tempdir()
            .unwrap();
        let db = new_test_engine(temp_dir.path().to_str().unwrap(), &[CF_DEFAULT, "a", "b"]);

        let normal_db_options = ImportModeDbOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_cf_options = ImportModeCfOptions::new_options(&db, "default");
        let import_cf_options = normal_cf_options.optimized_for_import_mode();

        assert!(
            import_cf_options.level0_stop_writes_trigger
                > normal_cf_options.level0_stop_writes_trigger
        );
        assert_eq!(import_cf_options.hard_pending_compaction_bytes_limit, 0);
        assert_eq!(import_cf_options.soft_pending_compaction_bytes_limit, 0);
        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let cfg = Config::default();

        let threads = ResizableRuntime::new(
            cfg.num_threads,
            "test",
            Box::new(create_tokio_runtime),
            Box::new(|_| {}),
        );
        let switcher = ImportModeSwitcher::new(&cfg);
        switcher.start_resizable_threads(&threads.handle(), db.clone());
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        assert!(switcher.enter_import_mode(&db, mf).unwrap());
        check_import_options(&db, &import_db_options, &import_cf_options);
        assert!(!switcher.enter_import_mode(&db, mf).unwrap());
        check_import_options(&db, &import_db_options, &import_cf_options);
        assert!(switcher.enter_normal_mode(&db, mf).unwrap());
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        assert!(!switcher.enter_normal_mode(&db, mf).unwrap());
        check_import_options(&db, &normal_db_options, &normal_cf_options);
    }

    #[test]
    fn test_import_mode_timeout() {
        let temp_dir = Builder::new()
            .prefix("test_import_mode_timeout")
            .tempdir()
            .unwrap();
        let db = new_test_engine(temp_dir.path().to_str().unwrap(), &[CF_DEFAULT, "a", "b"]);

        let normal_db_options = ImportModeDbOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_cf_options = ImportModeCfOptions::new_options(&db, "default");
        let import_cf_options = normal_cf_options.optimized_for_import_mode();

        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let cfg = Config {
            import_mode_timeout: ReadableDuration::millis(300),
            ..Config::default()
        };

        let threads =
            ResizableRuntime::new(4, "test", Box::new(create_tokio_runtime), Box::new(|_| {}));
        let switcher = ImportModeSwitcher::new(&cfg);
        let handle = threads.handle();

        switcher.start_resizable_threads(&handle, db.clone());
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        switcher.enter_import_mode(&db, mf).unwrap();
        check_import_options(&db, &import_db_options, &import_cf_options);

        thread::sleep(Duration::from_secs(1));
        handle.block_on(tokio::task::yield_now());

        check_import_options(&db, &normal_db_options, &normal_cf_options);
    }

    #[test]
    fn test_import_mode_should_not_decrease_level0_stop_writes_trigger() {
        // This checks issue tikv/tikv#6545.
        let temp_dir = Builder::new()
            .prefix("test_import_mode_should_not_decrease_level0_stop_writes_trigger")
            .tempdir()
            .unwrap();
        let db = new_test_engine_with_options(
            temp_dir.path().to_str().unwrap(),
            &["default"],
            |_, opt| opt.set_level_zero_stop_writes_trigger(2_000_000_000),
        );

        let normal_cf_options = ImportModeCfOptions::new_options(&db, "default");
        assert_eq!(normal_cf_options.level0_stop_writes_trigger, 2_000_000_000);
        let import_cf_options = normal_cf_options.optimized_for_import_mode();
        assert_eq!(import_cf_options.level0_stop_writes_trigger, 2_000_000_000);
    }

    #[test]
    fn test_range_overlaps() {
        let verify_overlap = |ranges1: &[(&str, &str)], ranges2: &[(&str, &str)], overlap: bool| {
            for r in ranges1 {
                let hash_range = HashRange {
                    start_key: r.0.as_bytes().to_vec(),
                    end_key: r.1.as_bytes().to_vec(),
                };

                for r2 in ranges2 {
                    let mut region = Region::default();
                    region.set_start_key(r2.0.as_bytes().to_vec());
                    region.set_end_key(r2.1.as_bytes().to_vec());

                    if overlap {
                        assert!(region_overlap_with_range(&hash_range, &region));
                    } else {
                        assert!(!region_overlap_with_range(&hash_range, &region));
                    }

                    let mut range = Range::default();
                    range.set_start(r2.0.as_bytes().to_vec());
                    range.set_end(r2.1.as_bytes().to_vec());
                    if overlap {
                        assert!(range_overlaps(&hash_range, &range));
                    } else {
                        assert!(!range_overlaps(&hash_range, &range));
                    }
                }
            }
        };

        let ranges1 = vec![("", ""), ("", "k10"), ("k01", ""), ("k01", "k08")];
        let ranges2 = vec![("", ""), ("k02", "k07"), ("k07", "k11"), ("k07", "")];
        verify_overlap(&ranges1, &ranges2, true);
        verify_overlap(&ranges2, &ranges1, true);

        let ranges1 = vec![("k10", "k20")];
        let ranges2 = vec![("", "k10"), ("k20", "k30"), ("k20", "")];
        verify_overlap(&ranges1, &ranges2, false);
        verify_overlap(&ranges2, &ranges1, false);
    }

    #[test]
    fn test_range_import_mode() {
        let cfg = Config::default();
        let switcher = ImportModeSwitcher::new(&cfg);
        let mut regions = vec![];
        for i in 1..=5 {
            let mut region = Region::default();
            region.set_id(i);
            region.set_start_key(format!("k{:02}", (i - 1) * 10).into());
            region.set_end_key(format!("k{:02}", i * 10).into());
            regions.push(region);
        }

        let mut key_range = Range::default();
        key_range.set_end(b"j".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range.clone()]);
        // no regions should be set in import mode
        for i in 1..=5 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }

        let mut r = Range::default();
        r.set_end(b"k".to_vec());
        assert!(switcher.range_in_import_mode(&r));

        // region 1 2 3 should be included
        key_range.set_start(b"k09".to_vec());
        key_range.set_end(b"k21".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range.clone()]);
        for i in 1..=3 {
            assert!(switcher.region_in_import_mode(&regions[i - 1]));
        }
        for i in 4..=5 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }

        let mut key_range2 = Range::default();
        // region 3 4 5 should be included
        key_range2.set_start(b"k29".to_vec());
        key_range2.set_end(b"".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range2.clone()]);
        for i in 1..=5 {
            assert!(switcher.region_in_import_mode(&regions[i - 1]));
        }

        switcher.clear_import_mode_range(vec![key_range]);
        for i in 1..=2 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }
        for i in 3..5 {
            assert!(switcher.region_in_import_mode(&regions[i - 1]));
        }

        switcher.clear_import_mode_range(vec![key_range2]);
        for i in 3..=5 {
            assert!(!switcher.region_in_import_mode(&regions[i - 1]));
        }
    }

    #[test]
    fn test_global_and_range_mode_interaction() {
        let temp_dir = Builder::new()
            .prefix("test_global_and_range_mode_interaction")
            .tempdir()
            .unwrap();
        let db = new_test_engine(temp_dir.path().to_str().unwrap(), &[CF_DEFAULT, "a", "b"]);

        let normal_db_options = ImportModeDbOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_cf_options = ImportModeCfOptions::new_options(&db, "default");
        let import_cf_options = normal_cf_options.optimized_for_import_mode();

        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let cfg = Config::default();
        let switcher = ImportModeSwitcher::new(&cfg);

        // Initially in normal mode
        assert_eq!(switcher.get_mode(), SwitchMode::Normal);
        check_import_options(&db, &normal_db_options, &normal_cf_options);

        // Add a range - should trigger import mode
        let mut key_range = Range::default();
        key_range.set_start(b"k1".to_vec());
        key_range.set_end(b"k2".to_vec());
        switcher.ranges_enter_import_mode(vec![key_range.clone()]);

        // Should be in import mode now due to range
        assert_eq!(switcher.get_mode(), SwitchMode::Import);

        // Manually enter import mode - should set DB options since ranges don't
        // automatically do it
        assert!(switcher.enter_import_mode(&db, mf).unwrap());
        check_import_options(&db, &import_db_options, &import_cf_options);

        // Try to exit import mode - should fail because range is still active
        assert!(!switcher.enter_normal_mode(&db, mf).unwrap());
        assert_eq!(switcher.get_mode(), SwitchMode::Import);
        check_import_options(&db, &import_db_options, &import_cf_options);

        // Clear the range - should still be in import mode due to global flag
        switcher.clear_import_mode_range(vec![key_range]);
        assert_eq!(switcher.get_mode(), SwitchMode::Import);

        // Now can exit import mode
        assert!(switcher.enter_normal_mode(&db, mf).unwrap());
        assert_eq!(switcher.get_mode(), SwitchMode::Normal);
        check_import_options(&db, &normal_db_options, &normal_cf_options);
    }
}
