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

use rocksdb::{BlockBasedOptions, ColumnFamilyOptions, DBIterator, DBOptions, Env, EnvOptions,
              ReadOptions, SequentialFile, SstFileWriter, Writable, WriteBatch as RawBatch, DB};
use kvproto::importpb::*;

use config::DbConfig;
use storage::{is_short_value, CF_DEFAULT, CF_WRITE};
use storage::types::Key;
use storage::mvcc::{Write, WriteType};
use util::config::{KB, MB};
use util::rocksdb::{new_engine_opt, CFOptions};

use super::{Error, Result};

pub struct Engine {
    cfg: DbConfig,
    uuid: Uuid,
    db: Arc<DB>,
    env: Arc<Env>,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(cfg: DbConfig, uuid: Uuid, path: P) -> Result<Engine> {
        let db = {
            let (db_opts, cfs_opts) = tune_dbconfig_for_bulk_load(&cfg);
            new_engine_opt(path.as_ref().to_str().unwrap(), db_opts, cfs_opts)?
        };
        Ok(Engine {
            cfg: cfg,
            uuid: uuid,
            db: Arc::new(db),
            env: Arc::new(Env::new_mem()),
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

    pub fn flush(&self) -> Result<()> {
        for cf_name in self.cf_names() {
            let cf_handle = self.cf_handle(cf_name).unwrap();
            self.flush_cf(cf_handle, true)?;
        }
        Ok(())
    }

    pub fn new_iter(&self, cf_name: &str, verify_checksum: bool) -> DBIterator<Arc<DB>> {
        let cf_handle = self.cf_handle(cf_name).unwrap();
        // Don't need to cache since it is unlikely to read more than once.
        let mut ropts = ReadOptions::new();
        ropts.fill_cache(false);
        ropts.set_readahead_size(2 * MB as usize);
        ropts.set_verify_checksums(verify_checksum);
        DBIterator::new_cf(self.db.clone(), cf_handle, ropts)
    }

    pub fn new_sst_writer<P: AsRef<Path>>(&self, cf_name: &str, path: P) -> Result<SstFileWriter> {
        let mut cf_opts = match cf_name {
            "write" => self.cfg.writecf.build_opt(),
            "default" => self.cfg.defaultcf.build_opt(),
            _ => unreachable!(),
        };
        cf_opts.set_env(self.env.clone());
        let mut writer = SstFileWriter::new(EnvOptions::new(), cf_opts);
        writer.open(path.as_ref().to_str().unwrap())?;
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

fn tune_dbconfig_for_bulk_load(cfg: &DbConfig) -> (DBOptions, Vec<CFOptions>) {
    const DISABLED: i32 = i32::MAX;

    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    opts.enable_statistics(false);
    opts.enable_pipelined_write(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    // NOTE: RocksDB preserves `max_background_jobs/4` for flush.
    opts.set_max_background_jobs(cfg.max_background_jobs);

    // CF_WRITE and CF_DEFAULT use the same options.
    let mut block_base_opts = BlockBasedOptions::new();
    // Use a large block size for sequential access.
    block_base_opts.set_block_size(512 * KB as usize);
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_block_based_table_factory(&block_base_opts);
    cf_opts.compression_per_level(&cfg.defaultcf.compression_per_level);
    // NOTE: Consider using a large write buffer, 1GB should be good enough.
    cf_opts.set_write_buffer_size(cfg.defaultcf.write_buffer_size.0);
    cf_opts.set_target_file_size_base(cfg.defaultcf.write_buffer_size.0);
    cf_opts.set_max_write_buffer_number(cfg.defaultcf.max_write_buffer_number);
    cf_opts.set_min_write_buffer_number_to_merge(cfg.defaultcf.min_write_buffer_number_to_merge);
    // Disable compaction and rate limit.
    cf_opts.set_disable_auto_compactions(true);
    cf_opts.set_soft_pending_compaction_bytes_limit(0);
    cf_opts.set_hard_pending_compaction_bytes_limit(0);
    cf_opts.set_level_zero_stop_writes_trigger(DISABLED);
    cf_opts.set_level_zero_slowdown_writes_trigger(DISABLED);
    cf_opts.set_level_zero_file_num_compaction_trigger(DISABLED);

    // TODO: Use VectorMemtable instead of SkipList.

    let cfs_opts = vec![
        CFOptions::new(CF_WRITE, cf_opts.clone()),
        CFOptions::new(CF_DEFAULT, cf_opts.clone()),
    ];

    (opts, cfs_opts)
}
