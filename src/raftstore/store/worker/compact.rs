// Copyright 2016 PingCAP, Inc.
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

use std::fmt::{self, Display, Formatter};
use std::error;
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use util::worker::Runnable;
use util::rocksdb;
use util::escape;
use util::rocksdb::compact_range;
use util::properties::MvccProperties;
use rocksdb::{CFHandle, Range, DB};
use storage::CF_WRITE;

use super::metrics::COMPACT_RANGE_CF;

type Key = Vec<u8>;

pub enum Task {
    Compact {
        cf_name: String,
        start_key: Option<Key>, // None means smallest key
        end_key: Option<Key>,   // None means largest key
    },

    CheckAndCompact {
        cf_names: Vec<String>,     // Column families need to compact
        ranges: BTreeSet<Key>,     // Ranges need to check
        tombstones_threshold: u64, // The minimum RocksDB tombstones a range that need compacting has
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Compact {
                ref cf_name,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Compact CF[{}], range[{:?}, {:?}]",
                cf_name,
                start_key.as_ref().map(|k| escape(k)),
                end_key.as_ref().map(|k| escape(k))
            ),
            Task::CheckAndCompact {
                ref cf_names,
                ref ranges,
                tombstones_threshold,
            } => write!(
                f,
                "CheckAndCompact cf names {:?}, ranges count {}, tombstones threshold {}",
                cf_names,
                ranges.len(),
                tombstones_threshold
            ),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("compact failed {:?}", err)
        }
    }
}

pub struct Runner {
    engine: Arc<DB>,
}

impl Runner {
    pub fn new(engine: Arc<DB>) -> Runner {
        Runner { engine: engine }
    }

    pub fn compact_range_cf(
        &mut self,
        cf_name: String,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Result<(), Error> {
        let handle = box_try!(rocksdb::get_cf_handle(&self.engine, &cf_name));
        let compact_range_timer = COMPACT_RANGE_CF
            .with_label_values(&[&cf_name])
            .start_coarse_timer();
        let start = start_key.as_ref().map(Vec::as_slice);
        let end = end_key.as_ref().map(Vec::as_slice);
        compact_range(&self.engine, handle, start, end, false);
        compact_range_timer.observe_duration();
        Ok(())
    }

    pub fn collect_ranges_need_compact(
        &self,
        ranges: BTreeSet<Key>,
        tombstones_threshold: u64,
    ) -> Result<VecDeque<(Key, Key)>, Error> {
        collect_ranges_need_compact(&self.engine, ranges, tombstones_threshold)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match task {
            Task::Compact {
                cf_name,
                start_key,
                end_key,
            } => {
                let cf = cf_name.clone();
                if let Err(e) = self.compact_range_cf(cf_name, start_key, end_key) {
                    error!("execute compact range for cf {} failed, err {}", &cf, e);
                } else {
                    info!("compact range for cf {} finished", &cf);
                }
            }
            Task::CheckAndCompact {
                cf_names,
                ranges,
                tombstones_threshold,
            } => match self.collect_ranges_need_compact(ranges, tombstones_threshold) {
                Ok(mut ranges) => for (start, end) in ranges.drain(..) {
                    for cf in &cf_names {
                        if let Err(e) = self.compact_range_cf(
                            cf.clone(),
                            Some(start.clone()),
                            Some(end.clone()),
                        ) {
                            error!(
                                "compact range ({:?}, {:?}) for cf {:?} failed, error {:?}",
                                start, end, cf, e
                            );
                        }
                    }
                },
                Err(e) => warn!("check ranges need reclaim failed, err: {:?}", e),
            },
        }
    }
}

fn need_compact(num_entires: u64, num_versions: u64, tombstones_threshold: u64) -> bool {
    if num_entires <= num_versions {
        return false;
    }

    // When the number of tombstones exceed threshold and at least 50% entries
    // are tombstones, this range need compacting.
    let estimate_num_del = num_entires - num_versions;
    estimate_num_del >= tombstones_threshold && estimate_num_del * 2 >= num_versions
}

fn get_range_entries_and_versions(
    engine: &DB,
    cf: &CFHandle,
    start: &[u8],
    end: &[u8],
) -> Option<(u64, u64)> {
    let range = Range::new(start, end);
    let collection = match engine.get_properties_of_tables_in_range(cf, &[range]) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if collection.is_empty() {
        return None;
    }

    // Aggregate total MVCC properties and total number entries.
    let mut props = MvccProperties::new();
    let mut num_entries = 0;
    for (_, v) in &*collection {
        let mvcc = match MvccProperties::decode(v.user_collected_properties()) {
            Ok(v) => v,
            Err(_) => return None,
        };
        num_entries += v.num_entries();
        props.add(&mvcc);
    }

    Some((num_entries, props.num_versions))
}

fn collect_ranges_need_compact(
    engine: &DB,
    ranges: BTreeSet<Key>,
    tombstones_threshold: u64,
) -> Result<VecDeque<(Key, Key)>, Error> {
    // Check the SST properties for each range, and we will compact a range if the range
    // contains too much RocksDB tombstones. we will merge multiple neighbouring ranges
    // that need compacting into a single range.
    let mut ranges_need_compact = VecDeque::new();
    let cf = box_try!(rocksdb::get_cf_handle(engine, CF_WRITE));
    let mut last_key = None;
    let mut compact_start = None;
    for key in ranges {
        // First key.
        if last_key.is_none() {
            last_key = Some(key);
            continue;
        }

        // Get total entries and total versions in this range and check if need compacting.
        if let Some((num_entries, num_versions)) =
            get_range_entries_and_versions(engine, cf, last_key.as_ref().unwrap().as_slice(), &key)
        {
            if need_compact(num_entries, num_versions, tombstones_threshold) {
                if compact_start.is_none() {
                    // The previous range doesn't need compacting.
                    compact_start = last_key.take();
                }
                last_key = Some(key);
                // Move to next range.
                continue;
            }
        }

        // Current range doesn't need compacting, save previous range that need compacting.
        if compact_start.is_some() && last_key.is_some() {
            ranges_need_compact.push_back((compact_start.unwrap(), last_key.unwrap()));
            compact_start = None;
        }
        last_key = Some(key);
    }

    // Save the last range that need compacting.
    if compact_start.is_some() && last_key.is_some() {
        ranges_need_compact.push_back((compact_start.unwrap(), last_key.unwrap()));
    }

    Ok(ranges_need_compact)
}

#[cfg(test)]
mod test {
    use std::time::Duration;
    use std::thread::sleep;
    use std::collections::BTreeSet;

    use tempdir::TempDir;

    use rocksdb::{self, Writable, WriteBatch, DB};
    use storage::types::Key as MvccKey;
    use storage::mvcc::{Write, WriteType};
    use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use util::rocksdb::new_engine;
    use util::rocksdb::{get_cf_handle, new_engine_opt, CFOptions};
    use util::properties::MvccPropertiesCollectorFactory;
    use raftstore::store::keys::data_key;

    use super::*;

    const ROCKSDB_TOTAL_SST_FILES_SIZE: &str = "rocksdb.total-sst-files-size";

    #[test]
    fn test_compact_range() {
        let path = TempDir::new("compact-range-test").unwrap();
        let db = new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT], None).unwrap();
        let db = Arc::new(db);

        let mut runner = Runner::new(Arc::clone(&db));

        let handle = get_cf_handle(&db, CF_DEFAULT).unwrap();

        // generate first sst file.
        let wb = WriteBatch::new();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(handle, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        db.write(wb).unwrap();
        db.flush_cf(handle, true).unwrap();

        // generate another sst file has the same content with first sst file.
        let wb = WriteBatch::new();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(handle, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        db.write(wb).unwrap();
        db.flush_cf(handle, true).unwrap();

        // get total sst files size.
        let old_sst_files_size = db.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .unwrap();

        // schedule compact range task
        runner.run(Task::Compact {
            cf_name: String::from(CF_DEFAULT),
            start_key: None,
            end_key: None,
        });
        sleep(Duration::from_secs(5));

        // get total sst files size after compact range.
        let new_sst_files_size = db.get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
            .unwrap();
        assert!(old_sst_files_size > new_sst_files_size);
    }

    fn mvcc_put(db: &DB, k: &[u8], v: &[u8], start_ts: u64, commit_ts: u64) {
        let cf = get_cf_handle(db, CF_WRITE).unwrap();
        let k = MvccKey::from_encoded(data_key(k));
        let k = k.append_ts(commit_ts);
        let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
        db.put_cf(cf, k.encoded(), &w.to_bytes()).unwrap();
    }

    fn delete(db: &DB, k: &[u8], commit_ts: u64) {
        let cf = get_cf_handle(db, CF_WRITE).unwrap();
        let k = MvccKey::from_encoded(data_key(k));
        let k = k.append_ts(commit_ts);
        db.delete_cf(cf, k.encoded()).unwrap();
    }

    fn open_db(path: &str) -> DB {
        let db_opts = rocksdb::DBOptions::new();
        let mut cf_opts = rocksdb::ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(8);
        let f = Box::new(MvccPropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.test-collector", f);
        let cfs_opts = vec![
            CFOptions::new(CF_DEFAULT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_RAFT, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_LOCK, rocksdb::ColumnFamilyOptions::new()),
            CFOptions::new(CF_WRITE, cf_opts),
        ];
        new_engine_opt(path, db_opts, cfs_opts).unwrap()
    }

    #[test]
    fn test_check_space_redundancy() {
        let p = TempDir::new("test").unwrap();
        let engine = open_db(p.path().to_str().unwrap());
        let cf = get_cf_handle(&engine, CF_WRITE).unwrap();

        // mvcc_put 0..5
        for i in 0..5 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1, 2);
        }
        engine.flush_cf(cf, true).unwrap();

        // gc 0..5
        for i in 0..5 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 2);
        }
        engine.flush_cf(cf, true).unwrap();

        let (s, e) = (data_key(b"k0"), data_key(b"k5"));
        let (entries, version) = get_range_entries_and_versions(&engine, cf, &s, &e).unwrap();
        assert_eq!(entries, 10);
        assert_eq!(version, 5);

        // mvcc_put 5..10
        for i in 5..10 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1, 2);
        }
        engine.flush_cf(cf, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let (entries, version) = get_range_entries_and_versions(&engine, cf, &s, &e).unwrap();
        assert_eq!(entries, 5);
        assert_eq!(version, 5);

        let mut ranges_to_check = BTreeSet::new();
        ranges_to_check.insert(data_key(b"k0"));
        ranges_to_check.insert(data_key(b"k5"));
        ranges_to_check.insert(data_key(b"k9"));

        let ranges_need_to_compact =
            collect_ranges_need_compact(&engine, ranges_to_check, 1).unwrap();
        let (s, e) = (data_key(b"k0"), data_key(b"k5"));
        let mut expected_ranges = VecDeque::new();
        expected_ranges.push_back((s, e));
        assert_eq!(ranges_need_to_compact, expected_ranges);

        // gc 5..10
        for i in 5..10 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 2);
        }
        engine.flush_cf(cf, true).unwrap();

        let (s, e) = (data_key(b"k5"), data_key(b"k9"));
        let (entries, version) = get_range_entries_and_versions(&engine, cf, &s, &e).unwrap();
        assert_eq!(entries, 10);
        assert_eq!(version, 5);

        let mut ranges_to_check = BTreeSet::new();
        ranges_to_check.insert(data_key(b"k0"));
        ranges_to_check.insert(data_key(b"k5"));
        ranges_to_check.insert(data_key(b"k9"));

        let ranges_need_to_compact =
        collect_ranges_need_compact(&engine, ranges_to_check, 1).unwrap();
        let (s, e) = (data_key(b"k0"), data_key(b"k9"));
        let mut expected_ranges = VecDeque::new();
        expected_ranges.push_back((s, e));
        assert_eq!(ranges_need_to_compact, expected_ranges);
    }
}
