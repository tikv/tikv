// Copyright 2017 PingCAP, Inc.
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

use std::i32;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use uuid::Uuid;
use tempdir::TempDir;

use rocksdb::{DBCompressionType, DBIterator, ReadOptions, Writable, WriteBatch as RawBatch, DB};
use kvproto::importpb::*;

use config::DbConfig;
use storage::{is_short_value, CF_DEFAULT, CF_WRITE};
use storage::types::Key;
use storage::mvcc::{Write, WriteType};
use util::config::GB;
use util::rocksdb::{new_engine_opt, CFOptions};

use super::{Error, Result};

pub struct Engine {
    db: Arc<DB>,
    uuid: Uuid,
    _temp_dir: TempDir, // Cleanup when engine is dropped.
}

impl Engine {
    pub fn new(cfg: &DbConfig, uuid: Uuid, temp_dir: TempDir) -> Result<Engine> {
        let db_opts = cfg.build_opt();
        let mut cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, cfg.defaultcf.build_opt()),
            CFOptions::new(CF_WRITE, cfg.writecf.build_opt()),
        ];
        for cf_opts in &mut cfs_opts {
            const DISABLED: i32 = i32::MAX;
            // Tune some performance related parameters here.
            cf_opts.set_num_levels(1);
            cf_opts.compression_per_level(&[DBCompressionType::Zstd]);
            cf_opts.set_write_buffer_size(GB);
            cf_opts.set_target_file_size_base(GB);
            cf_opts.set_max_write_buffer_number(6);
            cf_opts.set_min_write_buffer_number_to_merge(1);
            // Disable compaction and rate limit.
            cf_opts.set_disable_auto_compaction(true);
            cf_opts.set_soft_pending_compaction_bytes_limit(0);
            cf_opts.set_hard_pending_compaction_bytes_limit(0);
            cf_opts.set_level_zero_stop_writes_trigger(DISABLED);
            cf_opts.set_level_zero_slowdown_writes_trigger(DISABLED);
            cf_opts.set_level_zero_file_num_compaction_trigger(DISABLED);
        }
        let db = new_engine_opt(temp_dir.path().to_str().unwrap(), db_opts, cfs_opts)?;
        Ok(Engine {
            db: Arc::new(db),
            uuid: uuid,
            _temp_dir: temp_dir,
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        let wb = if batch.get_commit_ts() == 0 {
            self.raw_write(batch)
        } else {
            self.txn_write(batch)
        };
        // All data will be deleted on restart, so don't need to write WAL here.
        self.write_without_wal(wb).map_err(Error::from)
    }

    fn raw_write(&self, mut batch: WriteBatch) -> RawBatch {
        let wb = RawBatch::new();
        for m in batch.take_mutations().iter_mut() {
            let key = Key::from_encoded(m.take_key());
            wb.put(key.encoded(), m.get_value()).unwrap();
        }
        wb
    }

    fn txn_write(&self, mut batch: WriteBatch) -> RawBatch {
        let wb = RawBatch::new();
        let default_cf = self.cf_handle(CF_DEFAULT).unwrap();
        let write_cf = self.cf_handle(CF_WRITE).unwrap();
        let commit_ts = batch.get_commit_ts();
        for m in batch.take_mutations().iter_mut() {
            let key = Key::from_raw(m.get_key()).append_ts(commit_ts);
            if is_short_value(m.get_value()) {
                let value = Some(m.take_value());
                let write = Write::new(WriteType::Put, commit_ts, value).to_bytes();
                wb.put_cf(write_cf, key.encoded(), &write).unwrap();
            } else {
                let write = Write::new(WriteType::Put, commit_ts, None).to_bytes();
                wb.put_cf(write_cf, key.encoded(), &write).unwrap();
                wb.put_cf(default_cf, key.encoded(), m.get_value()).unwrap();
            }
        }
        wb
    }

    pub fn iter_cf(&self, cf_name: &str) -> DBIterator<Arc<DB>> {
        let cf_handle = self.cf_handle(cf_name).unwrap();
        // Don't need to cache since it is unlikely to read more than once.
        let mut ropts = ReadOptions::new();
        ropts.fill_cache(false);
        DBIterator::new_cf(self.db.clone(), cf_handle, ropts)
    }
}

impl Deref for Engine {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Engine {{uuid: {}, path: {}}}", self.uuid(), self.path())
    }
}
