// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::import_sstpb::*;
use rocksdb::DB;

use super::Result;

pub struct ImportModeSwitcher {
    mode: SwitchMode,
    backup_options: Vec<(String, ImportModeOptions)>,
}

impl ImportModeSwitcher {
    pub fn new() -> ImportModeSwitcher {
        ImportModeSwitcher {
            mode: SwitchMode::Normal,
            backup_options: Vec::new(),
        }
    }

    pub fn enter_normal_mode(&mut self, db: &DB) -> Result<()> {
        if self.mode == SwitchMode::Normal {
            return Ok(());
        }

        for (cf_name, cf_opts) in &self.backup_options {
            cf_opts.set_options_cf(db, cf_name)?;
        }

        self.mode = SwitchMode::Normal;
        Ok(())
    }

    pub fn enter_import_mode(&mut self, db: &DB) -> Result<()> {
        if self.mode == SwitchMode::Import {
            return Ok(());
        }

        let import_options = ImportModeOptions::new();
        self.backup_options.clear();
        for cf_name in db.cf_names() {
            let cf_opts = ImportModeOptions::new_options_cf(db, cf_name);
            self.backup_options.push((cf_name.to_owned(), cf_opts));
            import_options.set_options_cf(db, cf_name)?;
        }

        self.mode = SwitchMode::Import;
        Ok(())
    }
}

struct ImportModeOptions {
    block_cache_size: u64,
    disable_auto_compactions: bool,
    level0_stop_writes_trigger: u32,
    level0_slowdown_writes_trigger: u32,
    soft_pending_compaction_bytes_limit: u64,
    hard_pending_compaction_bytes_limit: u64,
}

impl ImportModeOptions {
    fn new() -> ImportModeOptions {
        ImportModeOptions {
            block_cache_size: 1 << 30,
            disable_auto_compactions: true,
            level0_stop_writes_trigger: 1 << 30,
            level0_slowdown_writes_trigger: 1 << 30,
            soft_pending_compaction_bytes_limit: 0,
            hard_pending_compaction_bytes_limit: 0,
        }
    }

    fn new_options_cf(db: &DB, cf_name: &str) -> ImportModeOptions {
        let cf = db.cf_handle(cf_name).unwrap();
        let cf_opts = db.get_options_cf(cf);

        ImportModeOptions {
            block_cache_size: cf_opts.get_block_cache_capacity(),
            disable_auto_compactions: cf_opts.get_disable_auto_compactions(),
            level0_stop_writes_trigger: cf_opts.get_level_zero_stop_writes_trigger(),
            level0_slowdown_writes_trigger: cf_opts.get_level_zero_slowdown_writes_trigger(),
            soft_pending_compaction_bytes_limit: cf_opts.get_soft_pending_compaction_bytes_limit(),
            hard_pending_compaction_bytes_limit: cf_opts.get_hard_pending_compaction_bytes_limit(),
        }
    }

    fn set_options_cf(&self, db: &DB, cf_name: &str) -> Result<()> {
        let cf = db.cf_handle(cf_name).unwrap();
        let cf_opts = db.get_options_cf(cf);
        cf_opts.set_block_cache_capacity(self.block_cache_size)?;

        let opts = [
            (
                "disable_auto_compactions".to_owned(),
                self.disable_auto_compactions.to_string(),
            ),
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::rocksdb_util::new_engine;
    use tempdir::TempDir;

    fn check_import_options(db: &DB, opts: &ImportModeOptions) {
        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_opts = db.get_options_cf(cf);
            assert_eq!(cf_opts.get_block_cache_capacity(), opts.block_cache_size);
            assert_eq!(
                cf_opts.get_disable_auto_compactions(),
                opts.disable_auto_compactions
            );
            assert_eq!(
                cf_opts.get_level_zero_stop_writes_trigger(),
                opts.level0_stop_writes_trigger
            );
            assert_eq!(
                cf_opts.get_level_zero_slowdown_writes_trigger(),
                opts.level0_slowdown_writes_trigger
            );
            // TODO: https://github.com/facebook/rocksdb/pull/3823
            // These options are set correctly, but we can't get them
            // because of the issue above.
            // assert_eq!(
            //     cf_opts.get_soft_pending_compaction_bytes_limit(),
            //     opts.soft_pending_compaction_bytes_limit
            // );
            // assert_eq!(
            //     cf_opts.get_hard_pending_compaction_bytes_limit(),
            //     opts.hard_pending_compaction_bytes_limit
            // );
        }
    }

    #[test]
    fn test_import_mode_switcher() {
        let temp_dir = TempDir::new("test_import_mode_switcher").unwrap();
        let db = new_engine(temp_dir.path().to_str().unwrap(), &["a", "b"], None).unwrap();

        let import_options = ImportModeOptions::new();
        let normal_options = ImportModeOptions::new_options_cf(&db, "default");

        let mut switcher = ImportModeSwitcher::new();
        check_import_options(&db, &normal_options);
        switcher.enter_import_mode(&db).unwrap();
        check_import_options(&db, &import_options);
        switcher.enter_import_mode(&db).unwrap();
        check_import_options(&db, &import_options);
        switcher.enter_normal_mode(&db).unwrap();
        check_import_options(&db, &normal_options);
        switcher.enter_normal_mode(&db).unwrap();
        check_import_options(&db, &normal_options);
    }
}
