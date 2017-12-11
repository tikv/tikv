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
use std::path::Path;
use std::sync::Arc;

use uuid::Uuid;
use tempdir::TempDir;

use rocksdb::{DBCompactionStyle, DBCompressionType, DBIterator, Env, EnvOptions, ReadOptions,
              SequentialFile, SstFileWriter, Writable, WriteBatch as RawBatch, DB};
use kvproto::importpb::*;

use config::DbConfig;
use storage::{is_short_value, CF_DEFAULT, CF_WRITE};
use storage::types::Key;
use storage::mvcc::{Write, WriteType};
use util::config::GB;
use util::rocksdb::{get_fastest_supported_compression_type, new_engine_opt, CFOptions};

use super::{Error, Result};

pub struct Engine {
    db: Arc<DB>,
    env: Arc<Env>,
    uuid: Uuid,
    _temp_dir: TempDir, // Cleanup when engine is dropped.
}

impl Engine {
    pub fn new(mut cfg: DbConfig, uuid: Uuid, temp_dir: TempDir) -> Result<Engine> {
        // Configuration recommendation:
        // 1. Use a large `write_buffer_size`, 1GB should be good enough.
        // 2. Choose a reasonable compression algorithm to balance between CPU and IO.
        // 3. Increase `max_background_jobs`, RocksDB preserves `max_background_jobs/4` for flush.

        cfg.enable_statistics = false;
        cfg.use_direct_io_for_flush_and_compaction = true;
        cfg.writecf.disable_block_cache = true;
        cfg.defaultcf.disable_block_cache = true;

        let db_opts = cfg.build_opt();
        let mut cfs_opts = vec![
            CFOptions::new(CF_WRITE, cfg.writecf.build_opt()),
            CFOptions::new(CF_DEFAULT, cfg.defaultcf.build_opt()),
        ];

        for cf_opts in &mut cfs_opts {
            const DISABLED: i32 = i32::MAX;
            // Disable compaction and rate limit.
            cf_opts.set_num_levels(1);
            cf_opts.set_target_file_size_base(GB);
            cf_opts.set_disable_auto_compactions(true);
            cf_opts.set_compaction_style(DBCompactionStyle::None);
            cf_opts.set_soft_pending_compaction_bytes_limit(0);
            cf_opts.set_hard_pending_compaction_bytes_limit(0);
            cf_opts.set_level_zero_stop_writes_trigger(DISABLED);
            cf_opts.set_level_zero_slowdown_writes_trigger(DISABLED);
            cf_opts.set_level_zero_file_num_compaction_trigger(DISABLED);
        }

        let db = new_engine_opt(temp_dir.path().to_str().unwrap(), db_opts, cfs_opts)?;
        Ok(Engine {
            db: Arc::new(db),
            env: Arc::new(Env::new_mem()),
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
        let write_cf = self.cf_handle(CF_WRITE).unwrap();
        let default_cf = self.cf_handle(CF_DEFAULT).unwrap();
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

    pub fn new_iter(&self, cf_name: &str, verify_checksum: bool) -> DBIterator<Arc<DB>> {
        let cf_handle = self.cf_handle(cf_name).unwrap();
        // Don't need to cache since it is unlikely to read more than once.
        let mut ropts = ReadOptions::new();
        ropts.fill_cache(false);
        ropts.set_verify_checksums(verify_checksum);
        DBIterator::new_cf(self.db.clone(), cf_handle, ropts)
    }

    pub fn new_sst_writer<P: AsRef<Path>>(&self, cf_name: &str, path: P) -> Result<SstFileWriter> {
        let path = path.as_ref().to_str().unwrap();
        let cf_handle = self.cf_handle(cf_name).unwrap();
        let mut cf_opts = self.get_options_cf(cf_handle);
        cf_opts.set_env(self.env.clone());
        cf_opts.compression_per_level(&[]);
        cf_opts.bottommost_compression(DBCompressionType::Disable);
        cf_opts.compression(get_fastest_supported_compression_type());
        let mut writer = SstFileWriter::new(EnvOptions::new(), cf_opts);
        writer.open(path)?;
        Ok(writer)
    }

    pub fn new_sst_reader<P: AsRef<Path>>(&self, path: P) -> Result<SequentialFile> {
        let path = path.as_ref().to_str().unwrap();
        let f = self.env.new_sequential_file(path, EnvOptions::new())?;
        Ok(f)
    }

    pub fn delete_sst_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        self.env.delete_file(path).map_err(Error::from)
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
