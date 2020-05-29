// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::Instant;

use engine_traits::{ColumnFamilyOptions, DBOptions, KvEngine};
use futures_cpupool::CpuPool;
use futures_util::compat::{Compat, Future01CompatExt};
use futures_util::future::FutureExt;
use kvproto::import_sstpb::*;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

use super::Config;
use super::Result;

type RocksDBMetricsFn = fn(cf: &str, name: &str, v: f64);

struct ImportModeSwitcherInner<T: KvEngine> {
    mode: SwitchMode,
    backup_db_options: ImportModeDBOptions,
    backup_cf_options: Vec<(String, ImportModeCFOptions)>,
    timeout: Duration,
    next_check: Instant,
    db: T,
    metrics_fn: RocksDBMetricsFn,
}

impl<T: KvEngine> ImportModeSwitcherInner<T> {
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

        let import_db_options = ImportModeDBOptions::new();
        import_db_options.set_options(&self.db)?;
        let import_cf_options = ImportModeCFOptions::new();
        for cf_name in self.db.cf_names() {
            let cf_opts = ImportModeCFOptions::new_options(&self.db, cf_name);
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
pub struct ImportModeSwitcher<T: KvEngine> {
    inner: Arc<Mutex<ImportModeSwitcherInner<T>>>,
}

impl<T: KvEngine> ImportModeSwitcher<T> {
    pub fn new(cfg: &Config, executor: &CpuPool, db: T) -> ImportModeSwitcher<T> {
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
            loop {
                let next_check = match switcher.upgrade() {
                    Some(switcher) => {
                        let mut switcher = switcher.lock().unwrap();
                        let now = Instant::now();
                        if now >= switcher.next_check {
                            if switcher.mode == SwitchMode::Import {
                                let mf = switcher.metrics_fn;
                                if !switcher.enter_normal_mode(mf).is_ok() {
                                    error!("failed to put TiKV back into normal mode");
                                }
                            }
                            switcher.next_check = now + switcher.timeout
                        }
                        switcher.next_check
                    }
                    // if the switcher has been dropped, we can stop
                    None => break,
                };

                let ok = GLOBAL_TIMER_HANDLE.delay(next_check).compat().await.is_ok();

                if !ok {
                    warn!("failed to delay with global timer");
                }
            }
        };
        executor
            .spawn(Compat::new(timer_loop.unit_error().boxed()))
            .forget();

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
    fn new() -> ImportModeDBOptions {
        ImportModeDBOptions {
            max_background_jobs: 32,
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
    block_cache_size: u64,
    level0_stop_writes_trigger: u32,
    level0_slowdown_writes_trigger: u32,
    soft_pending_compaction_bytes_limit: u64,
    hard_pending_compaction_bytes_limit: u64,
}

impl ImportModeCFOptions {
    fn new() -> ImportModeCFOptions {
        ImportModeCFOptions {
            block_cache_size: 4 << 30,
            level0_stop_writes_trigger: 1 << 30,
            level0_slowdown_writes_trigger: 1 << 30,
            soft_pending_compaction_bytes_limit: 0,
            hard_pending_compaction_bytes_limit: 0,
        }
    }

    fn new_options(db: &impl KvEngine, cf_name: &str) -> ImportModeCFOptions {
        let cf = db.cf_handle(cf_name).unwrap();
        let cf_opts = db.get_options_cf(cf);

        ImportModeCFOptions {
            block_cache_size: cf_opts.get_block_cache_capacity(),
            level0_stop_writes_trigger: cf_opts.get_level_zero_stop_writes_trigger(),
            level0_slowdown_writes_trigger: cf_opts.get_level_zero_slowdown_writes_trigger(),
            soft_pending_compaction_bytes_limit: cf_opts.get_soft_pending_compaction_bytes_limit(),
            hard_pending_compaction_bytes_limit: cf_opts.get_hard_pending_compaction_bytes_limit(),
        }
    }

    fn set_options(&self, db: &impl KvEngine, cf_name: &str, mf: RocksDBMetricsFn) -> Result<()> {
        let cf = db.cf_handle(cf_name).unwrap();
        let cf_opts = db.get_options_cf(cf);
        cf_opts.set_block_cache_capacity(self.block_cache_size)?;
        mf(cf_name, "block_cache_size", self.block_cache_size as f64);
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
        db.set_options_cf(cf, tmp_opts.as_slice())?;
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
    use tempfile::Builder;
    use test_sst_importer::new_test_engine;

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
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_opts = db.get_options_cf(cf);
            assert_eq!(
                cf_opts.get_block_cache_capacity(),
                expected_cf_opts.block_cache_size
            );
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

        let import_db_options = ImportModeDBOptions::new();
        let normal_db_options = ImportModeDBOptions::new_options(&db);
        let import_cf_options = ImportModeCFOptions::new();
        let normal_cf_options = ImportModeCFOptions::new_options(&db, "default");

        fn mf(_cf: &str, _name: &str, _v: f64) {}

        let cfg = Config::default();
        let threads = futures_cpupool::Builder::new()
            .name_prefix("sst-importer")
            .pool_size(cfg.num_threads)
            .create();

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
}
