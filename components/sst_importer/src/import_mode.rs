// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{ColumnFamilyOptions, DBOptions, KvEngine};
use kvproto::import_sstpb::*;

use super::Result;

type RocksDBMetricsFn = fn(cf: &str, name: &str, v: f64);

pub struct ImportModeSwitcher {
    mode: SwitchMode,
    backup_db_options: ImportModeDBOptions,
    backup_cf_options: Vec<(String, ImportModeCFOptions)>,
}

impl ImportModeSwitcher {
    pub fn new() -> ImportModeSwitcher {
        ImportModeSwitcher {
            mode: SwitchMode::Normal,
            backup_db_options: ImportModeDBOptions::new(),
            backup_cf_options: Vec::new(),
        }
    }

    pub fn enter_normal_mode(&mut self, db: &impl KvEngine, mf: RocksDBMetricsFn) -> Result<()> {
        if self.mode == SwitchMode::Normal {
            return Ok(());
        }

        self.backup_db_options.set_options(db)?;
        for (cf_name, cf_opts) in &self.backup_cf_options {
            cf_opts.set_options(db, cf_name, mf)?;
        }

        self.mode = SwitchMode::Normal;
        Ok(())
    }

    pub fn enter_import_mode(&mut self, db: &impl KvEngine, mf: RocksDBMetricsFn) -> Result<()> {
        if self.mode == SwitchMode::Import {
            return Ok(());
        }

        self.backup_db_options = ImportModeDBOptions::new_options(db);
        self.backup_cf_options.clear();

        let import_db_options = ImportModeDBOptions::new();
        import_db_options.set_options(db)?;
        let import_cf_options = ImportModeCFOptions::new();
        for cf_name in db.cf_names() {
            let cf_opts = ImportModeCFOptions::new_options(db, cf_name);
            self.backup_cf_options.push((cf_name.to_owned(), cf_opts));
            import_cf_options.set_options(db, cf_name, mf)?;
        }

        self.mode = SwitchMode::Import;
        Ok(())
    }

    pub fn get_mode(&self) -> SwitchMode {
        self.mode
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

        let mut switcher = ImportModeSwitcher::new();
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        switcher.enter_import_mode(&db, mf).unwrap();
        check_import_options(&db, &import_db_options, &import_cf_options);
        switcher.enter_import_mode(&db, mf).unwrap();
        check_import_options(&db, &import_db_options, &import_cf_options);
        switcher.enter_normal_mode(&db, mf).unwrap();
        check_import_options(&db, &normal_db_options, &normal_cf_options);
        switcher.enter_normal_mode(&db, mf).unwrap();
        check_import_options(&db, &normal_db_options, &normal_cf_options);
    }
}
