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
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use uuid::Uuid;

use kvproto::import_kvpb::*;
use rocksdb::{BlockBasedOptions, ColumnFamilyOptions, DBOptions, Writable, WriteBatch as RawBatch,
              DB};

use config::DbConfig;
use storage::CF_DEFAULT;
use storage::types::Key;
use util::config::MB;
use util::rocksdb::properties::{SizeProperties, SizePropertiesCollectorFactory};
use util::rocksdb::{new_engine_opt, CFOptions};

use super::Result;
use super::common::*;

pub struct Engine {
    db: Arc<DB>,
    uuid: Uuid,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(path: P, uuid: Uuid, opts: DbConfig) -> Result<Engine> {
        let db = {
            let (db_opts, cf_opts) = tune_dboptions_for_bulk_load(&opts);
            new_engine_opt(path.as_ref().to_str().unwrap(), db_opts, vec![cf_opts])?
        };
        Ok(Engine {
            db: Arc::new(db),
            uuid,
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
            let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
            wb.put(k.encoded(), m.get_value()).unwrap();
        }

        let size = wb.data_size();
        self.write_without_wal(wb)?;

        Ok(size)
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

/// Calculate subranges with approximately equal size from the size properties.
/// The `max_ranges` limits the maximum number of subranges it should returns.
/// The `min_range_size` limits the minimum size of a subrange.
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
    db_opts.set_use_direct_io_for_flush_and_compaction(true);
    // RocksDB preserves `max_background_jobs/4` for flush.
    db_opts.set_max_background_jobs(opts.max_background_jobs);

    let mut block_base_opts = BlockBasedOptions::new();
    // Use a large block size for sequential access.
    block_base_opts.set_block_size(MB as usize);
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

    use tempdir::TempDir;

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

    fn new_encoded_key(i: u8, ts: u64) -> Vec<u8> {
        Key::from_raw(&[i]).append_ts(ts).encoded().to_owned()
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
