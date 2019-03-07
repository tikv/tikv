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

use std::cmp;
use std::fmt;
use std::i32;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use uuid::Uuid;

use kvproto::import_kvpb::*;
use kvproto::import_sstpb::*;
use rocksdb::{
    BlockBasedOptions, ColumnFamilyOptions, DBIterator, DBOptions, Env, EnvOptions,
    ExternalSstFileInfo, ReadOptions, SstFileWriter, Writable, WriteBatch as RawBatch, DB,
};

use crate::config::DbConfig;
use crate::raftstore::store::keys;
use crate::storage::mvcc::{Write, WriteType};
use crate::storage::types::Key;
use crate::storage::{is_short_value, CF_DEFAULT, CF_WRITE};
use crate::util::config::MB;
use crate::util::rocksdb_util::{
    new_engine_opt,
    properties::{SizeProperties, SizePropertiesCollectorFactory},
    CFOptions,
};

use super::common::*;
use super::Result;
use crate::util::security;
use crate::util::security::SecurityConfig;

/// Engine wraps rocksdb::DB with customized options to support efficient bulk
/// write.
pub struct Engine {
    db: Arc<DB>,
    uuid: Uuid,
    db_cfg: DbConfig,
    security_cfg: SecurityConfig,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(
        path: P,
        uuid: Uuid,
        db_cfg: DbConfig,
        security_cfg: SecurityConfig,
    ) -> Result<Engine> {
        let db = {
            let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(&db_cfg);
            new_engine_opt(path.as_ref().to_str().unwrap(), db_opts, vec![cf_opts])?
        };
        Ok(Engine {
            db: Arc::new(db),
            uuid,
            db_cfg,
            security_cfg,
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn write(&self, mut batch: WriteBatch) -> Result<usize> {
        // Just a guess.
        let wb_cap = cmp::min(batch.get_mutations().len() * 128, MB as usize);
        let wb = RawBatch::with_capacity(wb_cap);
        let commit_ts = batch.get_commit_ts();
        for m in batch.take_mutations().iter_mut() {
            match m.get_op() {
                Mutation_OP::Put => {
                    let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
                    wb.put(k.as_encoded(), m.get_value()).unwrap();
                }
            }
        }

        let size = wb.data_size();
        self.write_without_wal(wb)?;

        Ok(size)
    }

    pub fn new_iter(&self, verify_checksum: bool) -> DBIterator<Arc<DB>> {
        let mut ropts = ReadOptions::new();
        ropts.fill_cache(false);
        ropts.set_verify_checksums(verify_checksum);
        DBIterator::new(Arc::clone(&self.db), ropts)
    }

    pub fn new_sst_writer(&self) -> Result<SSTWriter> {
        SSTWriter::new(&self.db_cfg, &self.security_cfg)
    }

    pub fn get_size_properties(&self) -> Result<SizeProperties> {
        let mut res = SizeProperties::default();
        let collection = self.get_properties_of_all_tables()?;
        for (_, v) in &*collection {
            let props = SizeProperties::decode(v.user_collected_properties())?;
            res.total_size += props.total_size;
            res.index_handles.extend(props.index_handles.clone());
        }
        Ok(res)
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

        // This range doesn't contain the data prefix, like the region range.
        let mut range = Range::new();
        range.set_start(keys::origin_key(info.smallest_key()).to_owned());
        range.set_end(keys::origin_key(info.largest_key()).to_owned());

        Ok(SSTInfo {
            data,
            range,
            cf_name: cf_name.to_owned(),
        })
    }
}

pub struct SSTWriter {
    env: Arc<Env>,
    // we need to preserve base env for reading raw file while env is an encrypted env
    base_env: Option<Arc<Env>>,
    default: SstFileWriter,
    default_entries: u64,
    write: SstFileWriter,
    write_entries: u64,
}

impl SSTWriter {
    pub fn new(db_cfg: &DbConfig, security_cfg: &SecurityConfig) -> Result<SSTWriter> {
        // Using a memory environment to generate SST in memory
        let mut env = Arc::new(Env::new_mem());
        let mut base_env = None;
        if !security_cfg.cipher_file.is_empty() {
            base_env = Some(Arc::clone(&env));
            env = security::encrypted_env_from_cipher_file(&security_cfg.cipher_file, Some(env))?;
        }

        // Creates a writer for default CF
        // Here is where we set table_properties_collector_factory, so that we can collect
        // some properties about SST
        let mut default_opts = db_cfg.defaultcf.build_opt();
        default_opts.set_env(Arc::clone(&env));

        let mut default = SstFileWriter::new(EnvOptions::new(), default_opts);
        default.open(CF_DEFAULT)?;

        // Creates a writer for write CF
        let mut write_opts = db_cfg.writecf.build_opt();
        write_opts.set_env(Arc::clone(&env));
        let mut write = SstFileWriter::new(EnvOptions::new(), write_opts);
        write.open(CF_WRITE)?;

        Ok(SSTWriter {
            env,
            base_env,
            default,
            default_entries: 0,
            write,
            write_entries: 0,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        if is_short_value(value) {
            let w = Write::new(WriteType::Put, commit_ts, Some(value.to_vec()));
            self.write.put(&k, &w.to_bytes())?;
            self.write_entries += 1;
        } else {
            let w = Write::new(WriteType::Put, commit_ts, None);
            self.write.put(&k, &w.to_bytes())?;
            self.write_entries += 1;
            self.default.put(&k, value)?;
            self.default_entries += 1;
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<Vec<SSTInfo>> {
        let mut infos = Vec::new();
        if self.default_entries > 0 {
            let info = self.default.finish()?;
            infos.push(SSTInfo::new(
                Arc::clone(self.base_env.as_ref().unwrap_or_else(|| &self.env)),
                info,
                CF_DEFAULT,
            )?);
        }
        if self.write_entries > 0 {
            let info = self.write.finish()?;
            infos.push(SSTInfo::new(
                Arc::clone(self.base_env.as_ref().unwrap_or_else(|| &self.env)),
                info,
                CF_WRITE,
            )?);
        }
        Ok(infos)
    }
}

/// Gets a set of approximately equal size ranges from `props`.
/// The maximum number of ranges cannot exceed `max_ranges`,
/// and the minimum number of ranges cannot be smaller than `min_range_size`
pub fn get_approximate_ranges(
    props: &SizeProperties,
    max_ranges: usize,
    min_range_size: usize,
) -> Vec<RangeInfo> {
    let range_size = (props.total_size as usize + max_ranges - 1) / max_ranges;
    let range_size = cmp::max(range_size, min_range_size);

    let mut size = 0;
    let mut start = RANGE_MIN;
    let mut ranges = Vec::new();
    for (i, (k, v)) in props.index_handles.iter().enumerate() {
        size += v.size as usize;
        let end = if i == (props.index_handles.len() - 1) {
            // Index range end is inclusive, so we need to use RANGE_MAX as
            // the last range end.
            RANGE_MAX
        } else {
            k
        };
        if size >= range_size || i == (props.index_handles.len() - 1) {
            let range = RangeInfo::new(start, end, size);
            ranges.push(range);
            size = 0;
            start = end;
        }
    }

    ranges
}

fn tune_dboptions_for_bulk_load(opts: &DbConfig) -> (DBOptions, CFOptions) {
    const DISABLED: i32 = i32::MAX;

    let mut db_opts = DBOptions::new();
    db_opts.create_if_missing(true);
    db_opts.enable_statistics(false);
    // Vector memtable doesn't support concurrent write.
    db_opts.allow_concurrent_memtable_write(false);
    // RocksDB preserves `max_background_jobs/4` for flush.
    db_opts.set_max_background_jobs(opts.max_background_jobs);

    // Put index and filter in block cache to restrict memory usage.
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_lru_cache(128 * MB as usize, -1, 0, 0.0);
    block_base_opts.set_cache_index_and_filter_blocks(true);
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_block_based_table_factory(&block_base_opts);
    cf_opts.compression_per_level(&opts.defaultcf.compression_per_level);
    // Consider using a large write buffer but be careful about OOM.
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
    // Add size properties to get approximate ranges wihout scan.
    let f = Box::new(SizePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.size-properties-collector", f);
    (db_opts, CFOptions::new(CF_DEFAULT, cf_opts))
}

#[cfg(test)]
mod tests {
    use super::*;

    use kvproto::kvrpcpb::IsolationLevel;
    use kvproto::metapb::{Peer, Region};
    use rocksdb::IngestExternalFileOptions;
    use std::fs::File;
    use std::io::Write;
    use tempdir::TempDir;

    use crate::raftstore::store::RegionSnapshot;
    use crate::storage::mvcc::MvccReader;
    use crate::util::file::file_exists;
    use crate::util::rocksdb_util::new_engine_opt;
    use crate::util::security::encrypted_env_from_cipher_file;

    fn new_engine() -> (TempDir, Engine) {
        let dir = TempDir::new("test_import_engine").unwrap();
        let uuid = Uuid::new_v4();
        let db_cfg = DbConfig::default();
        let security_cfg = SecurityConfig::default();
        let engine = Engine::new(dir.path(), uuid, db_cfg, security_cfg).unwrap();
        (dir, engine)
    }

    fn new_write_batch(n: u8, ts: u64) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for i in 0..n {
            let mut m = Mutation::new();
            m.set_op(Mutation_OP::Put);
            m.set_key(vec![i]);
            m.set_value(vec![i]);
            wb.mut_mutations().push(m);
        }
        wb.set_commit_ts(ts);
        wb
    }

    fn new_encoded_key(i: u8, ts: u64) -> Vec<u8> {
        Key::from_raw(&[i]).append_ts(ts).into_encoded()
    }

    #[test]
    fn test_write() {
        let (_dir, engine) = new_engine();

        let n = 10;
        let commit_ts = 10;
        let wb = new_write_batch(n, commit_ts);
        engine.write(wb).unwrap();

        for i in 0..n {
            let key = new_encoded_key(i, commit_ts);
            assert_eq!(engine.get(&key).unwrap().unwrap(), &[i]);
        }
    }

    #[test]
    fn test_sst_writer() {
        test_sst_writer_with(1, &[CF_WRITE], &SecurityConfig::default());
        test_sst_writer_with(1024, &[CF_DEFAULT, CF_WRITE], &SecurityConfig::default());

        let temp_dir = TempDir::new("/tmp/encrypted_env_from_cipher_file").unwrap();
        let security_cfg = create_security_cfg(&temp_dir);
        test_sst_writer_with(1, &[CF_WRITE], &security_cfg);
        test_sst_writer_with(1024, &[CF_DEFAULT, CF_WRITE], &security_cfg);
    }

    fn create_security_cfg(temp_dir: &TempDir) -> SecurityConfig {
        let path = temp_dir.path().join("cipher_file");
        let mut cipher_file = File::create(&path).unwrap();
        cipher_file.write_all(b"ACFFDBCC").unwrap();
        cipher_file.sync_all().unwrap();
        let mut security_cfg = SecurityConfig::default();
        security_cfg.cipher_file = path.to_str().unwrap().to_owned();
        assert_eq!(file_exists(&security_cfg.cipher_file), true);
        security_cfg
    }

    fn test_sst_writer_with(value_size: usize, cf_names: &[&str], security_cfg: &SecurityConfig) {
        let temp_dir = TempDir::new("_test_sst_writer").unwrap();

        let cfg = DbConfig::default();
        let mut db_opts = cfg.build_opt();
        if !security_cfg.cipher_file.is_empty() {
            let env = encrypted_env_from_cipher_file(&security_cfg.cipher_file, None).unwrap();
            db_opts.set_env(env);
        }
        let cfs_opts = cfg.build_cf_opts();
        let db = new_engine_opt(temp_dir.path().to_str().unwrap(), db_opts, cfs_opts).unwrap();
        let db = Arc::new(db);

        let n = 10;
        let commit_ts = 10;
        let mut w = SSTWriter::new(&cfg, &security_cfg).unwrap();

        // Write some keys.
        let value = vec![1u8; value_size];
        for i in 0..n {
            let key = new_encoded_key(i, commit_ts);
            w.put(&key, &value).unwrap();
        }

        let infos = w.finish().unwrap();
        assert_eq!(infos.len(), cf_names.len());

        for (info, cf_name) in infos.iter().zip(cf_names.iter()) {
            // Check SSTInfo
            let start = new_encoded_key(0, commit_ts);
            let end = new_encoded_key(n - 1, commit_ts);
            assert_eq!(info.range.get_start(), start.as_slice());
            assert_eq!(info.range.get_end(), end.as_slice());
            assert_eq!(info.cf_name, cf_name.to_owned());

            // Write the data to a file and ingest it to the engine.
            let path = Path::new(db.path()).join("test.sst");
            File::create(&path).unwrap().write_all(&info.data).unwrap();
            let mut opts = IngestExternalFileOptions::new();
            opts.move_files(true);
            let handle = db.cf_handle(cf_name).unwrap();
            db.ingest_external_file_cf(handle, &opts, &[path.to_str().unwrap()])
                .unwrap();
        }

        // Make a fake region snapshot.
        let mut region = Region::new();
        region.set_id(1);
        region.mut_peers().push(Peer::new());
        let snap = RegionSnapshot::from_raw(Arc::clone(&db), region);

        let mut reader = MvccReader::new(snap, None, false, None, None, IsolationLevel::SI);
        // Make sure that all kvs are right.
        for i in 0..n {
            let k = Key::from_raw(&[i]);
            let v = reader.get(&k, commit_ts).unwrap().unwrap();
            assert_eq!(&v, &value);
        }
        // Make sure that no extra keys are added.
        let (keys, _) = reader.scan_keys(None, (n + 1) as usize).unwrap();
        assert_eq!(keys.len(), n as usize);
        for (i, expected) in keys.iter().enumerate() {
            let k = Key::from_raw(&[i as u8]);
            assert_eq!(k.as_encoded(), expected.as_encoded());
        }
    }

    const SIZE_INDEX_DISTANCE: usize = 4 * 1024 * 1024;

    #[test]
    fn test_approximate_ranges() {
        let (_dir, engine) = new_engine();

        let num_files = 3;
        let num_entries = 3;
        for i in 0..num_files {
            for j in 0..num_entries {
                // (0, 3, 6), (1, 4, 7), (2, 5, 8)
                let k = [i + j * num_files];
                let v = vec![0u8; SIZE_INDEX_DISTANCE - k.len()];
                engine.put(&k, &v).unwrap();
                engine.flush(true).unwrap();
            }
        }

        let props = engine.get_size_properties().unwrap();

        let ranges = get_approximate_ranges(&props, 1, 0);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start, RANGE_MIN.to_owned());
        assert_eq!(ranges[0].end, RANGE_MAX.to_owned());

        let ranges = get_approximate_ranges(&props, 3, 0);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, RANGE_MIN.to_owned());
        assert_eq!(ranges[0].end, vec![2]);
        assert_eq!(ranges[1].start, vec![2]);
        assert_eq!(ranges[1].end, vec![5]);
        assert_eq!(ranges[2].start, vec![5]);
        assert_eq!(ranges[2].end, RANGE_MAX.to_owned());

        let ranges = get_approximate_ranges(&props, 4, SIZE_INDEX_DISTANCE * 4);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start, RANGE_MIN.to_owned());
        assert_eq!(ranges[0].end, vec![3]);
        assert_eq!(ranges[1].start, vec![3]);
        assert_eq!(ranges[1].end, vec![7]);
        assert_eq!(ranges[2].start, vec![7]);
        assert_eq!(ranges[2].end, RANGE_MAX.to_owned());
    }
}
