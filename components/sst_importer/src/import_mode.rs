// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use engine_traits::{ColumnFamilyOptions, DBOptions, KvEngine};
use futures::executor::ThreadPool;
use futures_util::compat::Future01CompatExt;
use kvproto::import_sstpb::*;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

use super::Config;
use super::Result;

type RocksDBMetricsFn = fn(cf: &str, name: &str, v: f64);

struct ImportModeSwitcherInner<E: KvEngine> {
    mode: SwitchMode,
    backup_db_options: ImportModeDBOptions,
    backup_cf_options: Vec<(String, ImportModeCFOptions)>,
    timeout: Duration,
    next_check: Instant,
    db: E,
    metrics_fn: RocksDBMetricsFn,
}

impl<E: KvEngine> ImportModeSwitcherInner<E> {
    fn enter_normal_mode(&mut self, mf: RocksDBMetricsFn) -> Result<()> {
        if self.mode == SwitchMode::Normal {
            return Ok(());
        }

        self.backup_db_options.set_options(&self.db)?;
        for (cf_name, cf_opts) in &self.backup_cf_options {
            cf_opts.set_options(&self.db, cf_name, mf)?;
        }

        self.mode = SwitchMode::Normal;
        Ok(())
    }

    fn enter_import_mode(&mut self, mf: RocksDBMetricsFn) -> Result<()> {
        if self.mode == SwitchMode::Import {
            return Ok(());
        }

        self.backup_db_options = ImportModeDBOptions::new_options(&self.db);
        self.backup_cf_options.clear();

        let import_db_options = self.backup_db_options.optimized_for_import_mode();
        import_db_options.set_options(&self.db)?;
        for cf_name in self.db.cf_names() {
            let cf_opts = ImportModeCFOptions::new_options(&self.db, cf_name);
            let import_cf_options = cf_opts.optimized_for_import_mode();
            self.backup_cf_options.push((cf_name.to_owned(), cf_opts));
            import_cf_options.set_options(&self.db, cf_name, mf)?;
        }

        self.mode = SwitchMode::Import;
        Ok(())
    }

    fn get_mode(&self) -> SwitchMode {
        self.mode
    }
}

#[derive(Clone)]
pub struct ImportModeSwitcher<E: KvEngine> {
    inner: Arc<Mutex<ImportModeSwitcherInner<E>>>,
}

impl<E: KvEngine> ImportModeSwitcher<E> {
    pub fn new(cfg: &Config, executor: &ThreadPool, db: E) -> ImportModeSwitcher<E> {
        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let timeout = cfg.import_mode_timeout.0;
        let inner = Arc::new(Mutex::new(ImportModeSwitcherInner {
            mode: SwitchMode::Normal,
            backup_db_options: ImportModeDBOptions::new(),
            backup_cf_options: Vec::new(),
            timeout,
            next_check: Instant::now() + timeout,
            db,
            metrics_fn: mf,
        }));

        // spawn a background future to put TiKV back into normal mode after timeout
        let switcher = Arc::downgrade(&inner);
        let timer_loop = async move {
            // loop until the switcher has been dropped
            while let Some(switcher) = switcher.upgrade() {
                let next_check = {
                    let mut switcher = switcher.lock().unwrap();
                    let now = Instant::now();
                    if now >= switcher.next_check {
                        if switcher.mode == SwitchMode::Import {
                            let mf = switcher.metrics_fn;
                            if switcher.enter_normal_mode(mf).is_err() {
                                error!("failed to put TiKV back into normal mode");
                            }
                        }
                        switcher.next_check = now + switcher.timeout
                    }
                    switcher.next_check
                };

                let ok = GLOBAL_TIMER_HANDLE.delay(next_check).compat().await.is_ok();

                if !ok {
                    warn!("failed to delay with global timer");
                }
            }
        };
        executor.spawn_ok(timer_loop);

        ImportModeSwitcher { inner }
    }

    pub fn enter_normal_mode(&mut self, mf: RocksDBMetricsFn) -> Result<()> {
        self.inner.lock().unwrap().enter_normal_mode(mf)
    }

    pub fn enter_import_mode(&mut self, mf: RocksDBMetricsFn) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.enter_import_mode(mf)?;
        inner.next_check = Instant::now() + inner.timeout;
        inner.metrics_fn = mf;
        Ok(())
    }

    pub fn get_mode(&self) -> SwitchMode {
        self.inner.lock().unwrap().get_mode()
    }
}

struct ImportModeDBOptions {
    max_background_jobs: i32,
}

impl ImportModeDBOptions {
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

    fn new_options(db: &impl KvEngine) -> ImportModeDBOptions {
        let db_opts = db.get_db_options();
        ImportModeDBOptions {
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

struct ImportModeCFOptions {
    level0_stop_writes_trigger: u32,
    level0_slowdown_writes_trigger: u32,
    soft_pending_compaction_bytes_limit: u64,
    hard_pending_compaction_bytes_limit: u64,
}

impl ImportModeCFOptions {
    fn optimized_for_import_mode(&self) -> Self {
        Self {
            level0_stop_writes_trigger: self.level0_stop_writes_trigger.max(1 << 30),
            level0_slowdown_writes_trigger: self.level0_slowdown_writes_trigger.max(1 << 30),
            soft_pending_compaction_bytes_limit: 0,
            hard_pending_compaction_bytes_limit: 0,
        }
    }

    fn new_options(db: &impl KvEngine, cf_name: &str) -> ImportModeCFOptions {
        let cf_opts = db.get_options_cf(cf_name).unwrap(); //FIXME unwrap

        ImportModeCFOptions {
            level0_stop_writes_trigger: cf_opts.get_level_zero_stop_writes_trigger(),
            level0_slowdown_writes_trigger: cf_opts.get_level_zero_slowdown_writes_trigger(),
            soft_pending_compaction_bytes_limit: cf_opts.get_soft_pending_compaction_bytes_limit(),
            hard_pending_compaction_bytes_limit: cf_opts.get_hard_pending_compaction_bytes_limit(),
        }
    }

    fn set_options(&self, db: &impl KvEngine, cf_name: &str, mf: RocksDBMetricsFn) -> Result<()> {
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
    use super::*;

    use engine_traits::KvEngine;
    use futures::executor::ThreadPoolBuilder;
    use std::thread;
    use tempfile::Builder;
    use test_sst_importer::{new_test_engine, new_test_engine_with_options};
    use tikv_util::config::ReadableDuration;

    fn check_import_options<E>(
        db: &E,
        expected_db_opts: &ImportModeDBOptions,
        expected_cf_opts: &ImportModeCFOptions,
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
        let db = new_test_engine(temp_dir.path().to_str().unwrap(), &["a", "b"]);

        let normal_db_options = ImportModeDBOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_cf_options = ImportModeCFOptions::new_options(&db, "default");
        let import_cf_options = normal_cf_options.optimized_for_import_mode();

        assert!(
            import_cf_options.level0_stop_writes_trigger
                > normal_cf_options.level0_stop_writes_trigger
        );

        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let cfg = Config::default();
        let threads = ThreadPoolBuilder::new()
            .pool_size(cfg.num_threads)
            .name_prefix("sst-importer")
            .create()
            .unwrap();

        let mut switcher = ImportModeSwitcher::new(&cfg, &threads, db.clone());
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        switcher.enter_import_mode(mf).unwrap();
        check_import_options(&db, &import_db_options, &import_cf_options);
        switcher.enter_import_mode(mf).unwrap();
        check_import_options(&db, &import_db_options, &import_cf_options);
        switcher.enter_normal_mode(mf).unwrap();
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        switcher.enter_normal_mode(mf).unwrap();
        check_import_options(&db, &normal_db_options, &normal_cf_options);
    }

    #[test]
    fn test_import_mode_timeout() {
        let temp_dir = Builder::new()
            .prefix("test_import_mode_timeout")
            .tempdir()
            .unwrap();
        let db = new_test_engine(temp_dir.path().to_str().unwrap(), &["a", "b"]);

        let normal_db_options = ImportModeDBOptions::new_options(&db);
        let import_db_options = normal_db_options.optimized_for_import_mode();
        let normal_cf_options = ImportModeCFOptions::new_options(&db, "default");
        let import_cf_options = normal_cf_options.optimized_for_import_mode();

        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let cfg = Config {
            import_mode_timeout: ReadableDuration::millis(300),
            ..Config::default()
        };
        let threads = ThreadPoolBuilder::new()
            .pool_size(cfg.num_threads)
            .name_prefix("sst-importer")
            .create()
            .unwrap();

        let mut switcher = ImportModeSwitcher::new(&cfg, &threads, db.clone());
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        switcher.enter_import_mode(mf).unwrap();
        check_import_options(&db, &import_db_options, &import_cf_options);

        thread::sleep(Duration::from_secs(1));

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

        let normal_cf_options = ImportModeCFOptions::new_options(&db, "default");
        assert_eq!(normal_cf_options.level0_stop_writes_trigger, 2_000_000_000);
        let import_cf_options = normal_cf_options.optimized_for_import_mode();
        assert_eq!(import_cf_options.level0_stop_writes_trigger, 2_000_000_000);
    }
}
