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
use std::io::Read;
use std::ops::Deref;
use std::sync::Arc;
use std::path::Path;

use uuid::Uuid;

use rocksdb::{BlockBasedOptions, ColumnFamilyOptions, DBIterator, DBOptions, Env, EnvOptions,
              ExternalSstFileInfo, ReadOptions, SstFileWriter, Writable, WriteBatch as RawBatch,
              DB};
use kvproto::importpb::*;

use config::DbConfig;
use storage::{is_short_value, CF_DEFAULT, CF_WRITE};
use storage::types::{split_encoded_key_on_ts, Key};
use storage::mvcc::{Write, WriteType};
use raftstore::store::keys;
use util::config::MB;
use util::rocksdb::{new_engine_opt, CFOptions};

use super::Result;
use super::common::*;

pub struct Engine {
    db: Arc<DB>,
    uuid: Uuid,
    opts: DbConfig,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(path: P, uuid: Uuid, opts: DbConfig) -> Result<Engine> {
        let db = {
            let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(&opts);
            new_engine_opt(path.as_ref().to_str().unwrap(), db_opts, vec![cf_opts])?
        };
        Ok(Engine {
            db: Arc::new(db),
            uuid: uuid,
            opts: opts,
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn write(&self, mut batch: WriteBatch) -> Result<usize> {
        let wb = RawBatch::new();
        let commit_ts = batch.get_commit_ts();
        for m in batch.take_mutations().iter_mut() {
            let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
            wb.put(k.encoded(), m.get_value()).unwrap();
        }

        let size = wb.data_size();
        self.write_without_wal(wb)?;
        Ok(size)
    }

    pub fn new_iter(&self, verify_checksum: bool) -> DBIterator<Arc<DB>> {
        let mut ropts = ReadOptions::new();
        ropts.fill_cache(false);
        ropts.set_readahead_size(4 * MB as usize);
        ropts.set_verify_checksums(verify_checksum);
        DBIterator::new(self.db.clone(), ropts)
    }

    pub fn new_sst_writer(&self) -> Result<SSTWriter> {
        let env = Arc::new(Env::new_mem());

        let mut default_opts = self.opts.defaultcf.build_opt();
        default_opts.set_env(env.clone());
        let mut default = SstFileWriter::new(EnvOptions::new(), default_opts);
        default.open("default.sst")?;

        let mut write_opts = self.opts.writecf.build_opt();
        write_opts.set_env(env.clone());
        let mut write = SstFileWriter::new(EnvOptions::new(), write_opts);
        write.open("write.sst")?;

        Ok(SSTWriter::new(env, default, write))
    }
}

impl Deref for Engine {
    type Target = DB;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Engine")
            .field("uuid", &self.uuid())
            .field("path", &self.path().to_owned())
            .finish()
    }
}

pub struct SSTInfo {
    pub data: Vec<u8>,
    pub range: Range,
    pub cf_name: String,
}

impl SSTInfo {
    pub fn new(env: Arc<Env>, info: ExternalSstFileInfo, cf_name: &str) -> Result<SSTInfo> {
        let mut data = Vec::new();
        let path = info.file_path();
        let mut f = env.new_sequential_file(path.to_str().unwrap(), EnvOptions::new())?;
        f.read_to_end(&mut data)?;
        assert_eq!(data.len(), info.file_size() as usize);

        Ok(SSTInfo {
            data: data,
            range: new_range(info.smallest_key(), info.largest_key()),
            cf_name: cf_name.to_owned(),
        })
    }
}

pub struct SSTWriter {
    env: Arc<Env>,
    default: SstFileWriter,
    write: SstFileWriter,
}

impl SSTWriter {
    pub fn new(env: Arc<Env>, default: SstFileWriter, write: SstFileWriter) -> SSTWriter {
        SSTWriter {
            env: env,
            default: default,
            write: write,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = split_encoded_key_on_ts(key)?;
        if is_short_value(value) {
            let w = Write::new(WriteType::Put, commit_ts, Some(value.to_vec()));
            self.write.put(&k, &w.to_bytes())?;
        } else {
            self.default.put(&k, value)?;
            let w = Write::new(WriteType::Put, commit_ts, None);
            self.write.put(&k, &w.to_bytes())?;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<SSTInfo>> {
        let mut infos = Vec::new();
        if self.default.file_size() > 0 {
            let info = self.default.finish()?;
            infos.push(SSTInfo::new(self.env.clone(), info, CF_DEFAULT)?);
        }
        if self.write.file_size() > 0 {
            let info = self.write.finish()?;
            infos.push(SSTInfo::new(self.env.clone(), info, CF_WRITE)?);
        }
        Ok(infos)
    }
}

fn tune_dboptions_for_bulk_load(opts: &DbConfig) -> (DBOptions, CFOptions) {
    const DISABLED: i32 = i32::MAX;

    let mut db_opts = DBOptions::new();
    db_opts.create_if_missing(true);
    db_opts.enable_statistics(false);
    db_opts.allow_concurrent_memtable_write(false);
    db_opts.set_writable_file_max_buffer_size(4 * MB as i32);
    db_opts.set_use_direct_io_for_flush_and_compaction(true);
    // NOTE: RocksDB preserves `max_background_jobs/4` for flush.
    db_opts.set_max_background_jobs(opts.max_background_jobs);

    let mut block_base_opts = BlockBasedOptions::new();
    // Use a large block size for sequential access.
    block_base_opts.set_block_size(4 * MB as usize);
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_block_based_table_factory(&block_base_opts);
    cf_opts.compression_per_level(&opts.defaultcf.compression_per_level);
    // NOTE: Consider using a large write buffer.
    cf_opts.set_write_buffer_size(opts.defaultcf.write_buffer_size.0);
    cf_opts.set_target_file_size_base(opts.defaultcf.write_buffer_size.0);
    cf_opts.set_vector_memtable_factory(opts.defaultcf.write_buffer_size.0);
    cf_opts.set_max_write_buffer_number(opts.defaultcf.max_write_buffer_number);
    // Disable compaction and rate limit.
    cf_opts.set_disable_auto_compactions(true);
    cf_opts.set_soft_pending_compaction_bytes_limit(0);
    cf_opts.set_hard_pending_compaction_bytes_limit(0);
    cf_opts.set_level_zero_stop_writes_trigger(DISABLED);
    cf_opts.set_level_zero_slowdown_writes_trigger(DISABLED);
    cf_opts.set_level_zero_file_num_compaction_trigger(DISABLED);

    (db_opts, CFOptions::new(CF_DEFAULT, cf_opts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    fn new_engine() -> (TempDir, Engine) {
        let dir = TempDir::new("test_import_engine").unwrap();
        let uuid = Uuid::new_v4();
        let opts = DbConfig::default();
        let engine = Engine::new(dir.path(), uuid, opts).unwrap();
        (dir, engine)
    }

    fn new_write_batch(n: usize, ts: Option<u64>) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for i in 0..n {
            let s = format!("{:016}", i);
            let mut m = Mutation::new();
            m.set_op(Mutation_OP::Put);
            m.set_key(s.as_bytes().to_owned());
            m.set_value(s.as_bytes().to_owned());
            wb.mut_mutations().push(m);
        }
        if let Some(ts) = ts {
            wb.set_commit_ts(ts);
        }
        wb
    }

    #[test]
    fn test_engine_write() {
        let (_dir, engine) = new_engine();

        let n = 10;
        let commit_ts = 10;
        let wb = new_write_batch(n, Some(commit_ts));
        engine.write(wb).unwrap();

        for i in 0..n {
            let s = format!("{:016}", i);
            let k = Key::from_raw(s.as_bytes()).append_ts(commit_ts);
            assert_eq!(engine.get(k.encoded()).unwrap().unwrap(), s.as_bytes());
        }
    }
}
