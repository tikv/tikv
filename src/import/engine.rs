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
use util::rocksdb::properties::{SizeProperties, SizePropertiesCollectorFactory};

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
        SSTWriter::new(&self.opts)
    }

    pub fn get_approximate_ranges(&self, num_ranges: usize) -> Result<Vec<RangeInfo>> {
        let mut res = SizeProperties::default();
        let collection = self.get_properties_of_all_tables()?;
        for (_, v) in &*collection {
            let props = SizeProperties::decode(v.user_collected_properties())?;
            res.total_size += props.total_size;
            res.index_handles.extend(props.index_handles.clone());
        }

        let range_size = (res.total_size as usize + num_ranges - 1) / num_ranges;

        let mut start = RANGE_MIN;
        let mut sub_size = 0;
        let mut sub_ranges = Vec::new();
        for (i, (k, v)) in res.index_handles.iter().enumerate() {
            sub_size += v.size as usize;
            let end = if i == (res.index_handles.len() - 1) {
                // Index range end is inclusive, so we need to use RANGE_MAX as
                // the last range end.
                RANGE_MAX
            } else {
                k
            };
            if sub_size >= range_size || i == (res.index_handles.len() - 1) {
                let sub_range = RangeInfo::new(start, end, sub_size);
                sub_ranges.push(sub_range);
                sub_size = 0;
                start = end;
            }
        }

        Ok(sub_ranges)
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

        // This range doesn't contain the data prefix, like the region range.
        let mut range = Range::new();
        range.set_start(keys::origin_key(info.smallest_key()).to_owned());
        range.set_end(keys::origin_key(info.largest_key()).to_owned());

        Ok(SSTInfo {
            data: data,
            range: range,
            cf_name: cf_name.to_owned(),
        })
    }
}

pub struct SSTWriter {
    env: Arc<Env>,
    default: SstFileWriter,
    default_entries: u64,
    write: SstFileWriter,
    write_entries: u64,
}

impl SSTWriter {
    pub fn new(cfg: &DbConfig) -> Result<SSTWriter> {
        let env = Arc::new(Env::new_mem());

        let mut default_opts = cfg.defaultcf.build_opt();
        default_opts.set_env(env.clone());
        let mut default = SstFileWriter::new(EnvOptions::new(), default_opts);
        default.open(CF_DEFAULT)?;

        let mut write_opts = cfg.writecf.build_opt();
        write_opts.set_env(env.clone());
        let mut write = SstFileWriter::new(EnvOptions::new(), write_opts);
        write.open(CF_WRITE)?;

        Ok(SSTWriter {
            env: env,
            default: default,
            default_entries: 0,
            write: write,
            write_entries: 0,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = split_encoded_key_on_ts(key)?;
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
            infos.push(SSTInfo::new(self.env.clone(), info, CF_DEFAULT)?);
        }
        if self.write_entries > 0 {
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
    // Add size properties to get approximate ranges wihout scan.
    let f = Box::new(SizePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.size-properties-collector", f);

    (db_opts, CFOptions::new(CF_DEFAULT, cf_opts))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Write;
    use tempdir::TempDir;
    use rocksdb::IngestExternalFileOptions;
    use kvproto::metapb::{Peer, Region};
    use kvproto::kvrpcpb::IsolationLevel;

    use storage::mvcc::MvccReader;
    use raftstore::coprocessor::RegionSnapshot;
    use util::rocksdb::new_engine_opt;

    fn new_engine() -> (TempDir, Engine) {
        let dir = TempDir::new("test_import_engine").unwrap();
        let uuid = Uuid::new_v4();
        let opts = DbConfig::default();
        let engine = Engine::new(dir.path(), uuid, opts).unwrap();
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

    #[test]
    fn test_write() {
        let (_dir, engine) = new_engine();

        let n = 10;
        let commit_ts = 10;
        let wb = new_write_batch(n, commit_ts);
        engine.write(wb).unwrap();

        for i in 0..n {
            let k = Key::from_raw(&[i]).append_ts(commit_ts);
            assert_eq!(engine.get(k.encoded()).unwrap().unwrap(), &[i]);
        }
    }

    #[test]
    fn test_sst_writer() {
        test_sst_writer_with(1, &[CF_WRITE]);
        test_sst_writer_with(1024, &[CF_DEFAULT, CF_WRITE]);
    }

    fn test_sst_writer_with(value_size: usize, cf_names: &[&str]) {
        let temp_dir = TempDir::new("_test_sst_writer").unwrap();

        let cfg = DbConfig::default();
        let db_opts = cfg.build_opt();
        let cfs_opts = cfg.build_cf_opts();
        let db = new_engine_opt(temp_dir.path().to_str().unwrap(), db_opts, cfs_opts).unwrap();
        let db = Arc::new(db);

        let n = 10;
        let commit_ts = 10;
        let mut w = SSTWriter::new(&cfg).unwrap();

        // Write some keys.
        let value = vec![1u8; value_size];
        for i in 0..n {
            let k = Key::from_raw(&[i]).append_ts(commit_ts);
            w.put(k.encoded(), &value).unwrap();
        }

        let infos = w.finish().unwrap();
        assert_eq!(infos.len(), cf_names.len());

        for (info, cf_name) in infos.iter().zip(cf_names.iter()) {
            // Check SSTInfo
            let start = Key::from_raw(&[0]).append_ts(commit_ts);
            let end = Key::from_raw(&[n - 1]).append_ts(commit_ts);
            assert_eq!(info.range.get_start(), start.encoded().as_slice());
            assert_eq!(info.range.get_end(), end.encoded().as_slice());
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
        let snap = Box::new(RegionSnapshot::from_raw(db.clone(), region));

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
            assert_eq!(k.encoded(), expected.encoded());
        }
    }

    const PROP_SIZE_INDEX_DISTANCE: usize = 4 * 1024 * 1024;

    #[test]
    fn test_approximate_ranges() {
        let (_dir, engine) = new_engine();

        let num_files = 3;
        let num_entries = 3;
        for i in 0..num_files {
            for j in 0..num_entries {
                // (0, 3, 6), (1, 4, 7), (2, 5, 8)
                let k = [i + j * num_files];
                let v = vec![0u8; PROP_SIZE_INDEX_DISTANCE - k.len()];
                engine.put(&k, &v).unwrap();
                engine.flush(true).unwrap();
            }
        }

        let ranges = engine.get_approximate_ranges(1).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].start.as_slice(), RANGE_MIN);
        assert_eq!(ranges[0].end.as_slice(), RANGE_MAX);

        let ranges = engine.get_approximate_ranges(2).unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].start.as_slice(), RANGE_MIN);
        assert_eq!(ranges[0].end.as_slice(), &[4]);
        assert_eq!(ranges[1].start.as_slice(), &[4]);
        assert_eq!(ranges[1].end.as_slice(), RANGE_MAX);

        let ranges = engine.get_approximate_ranges(3).unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].start.as_slice(), RANGE_MIN);
        assert_eq!(ranges[0].end.as_slice(), &[2]);
        assert_eq!(ranges[1].start.as_slice(), &[2]);
        assert_eq!(ranges[1].end.as_slice(), &[5]);
        assert_eq!(ranges[2].start.as_slice(), &[5]);
        assert_eq!(ranges[2].end.as_slice(), RANGE_MAX);
    }
}
