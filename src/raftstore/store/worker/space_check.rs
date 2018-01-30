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

use std::collections::{BTreeSet, VecDeque};
use std::error;
use std::sync::mpsc::Sender;

use rocksdb::{CFHandle, Range, DB};
use std::sync::Arc;
use std::fmt::{self, Display, Formatter};

use util::rocksdb;
use util::worker::Runnable;
use util::properties::MvccProperties;
use storage::CF_WRITE;

type Key = Vec<u8>;

pub struct Task {
    pub ranges: BTreeSet<Key>,
    pub min_num_del: u64,
}

#[derive(Default, Debug)]
pub struct TaskRes {
    pub ranges_need_compact: VecDeque<(Key, Key)>,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ranges count {}", self.ranges.len())
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("check ranges need reclaim failed, err: {:?}", err)
        }
    }
}

pub struct Runner {
    engine: Arc<DB>,
    notifier: Sender<TaskRes>,
}

impl Runner {
    pub fn new(engine: Arc<DB>, notifier: Sender<TaskRes>) -> Runner {
        Runner {
            engine: engine,
            notifier: notifier,
        }
    }

    pub fn collect_ranges_need_compact(&self, ranges: BTreeSet<Key>, min_num_del: u64) -> Result<TaskRes, Error> {
        collect_ranges_need_compact(&self.engine, ranges, min_num_del)
    }
}

impl Runnable<Task> for Runner {
    fn run(&mut self, task: Task) {
        match self.collect_ranges_need_compact(task.ranges, task.min_num_del) {
            Ok(task_res) => self.notifier.send(task_res).unwrap(),
            Err(e) => warn!("check ranges need reclaim failed, err: {:?}", e),
        }
    }
}

fn need_compact(num_dels: u64, num_versions: u64, min_num_del: u64) -> bool {
    num_dels >= min_num_del && num_dels * 2 >= num_versions
}

fn gather_range_entries_and_puts(
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
        println!("mvcc {:?}", mvcc);
    }
    Some((num_entries, props.num_versions))
}

fn collect_ranges_need_compact(engine: &DB, ranges: BTreeSet<Key>, min_num_del: u64) -> Result<TaskRes, Error> {
    let cf = box_try!(rocksdb::get_cf_handle(engine, CF_WRITE));
    let mut task_res = TaskRes::default();
    let mut last_start_key = vec![];
    let mut compact_start = None;
    for key in &ranges {
        if last_start_key.is_empty() {
            last_start_key = key.clone();
            continue;
        }
        if let Some((num_entries, num_versions)) =
        gather_range_entries_and_puts(engine, cf, &last_start_key, key)
        {
            if num_entries > num_versions {
                println!("[{:?}, {:?}]", last_start_key, key);
                println!("num_entries: {}, num_versions: {}", num_entries, num_versions);
                let estimate_num_del = num_entries - num_versions;
                if need_compact(estimate_num_del, num_versions, min_num_del) {
                    if compact_start.is_none() {
                        compact_start = Some(last_start_key.clone());
                    }
                    last_start_key = key.clone();

                    println!("compact_start: {:?}, last_start_key: {:?}", compact_start, last_start_key);

                    continue;
                }
            }
        }
        // collect ranges need manual compaction
        if compact_start.is_some() {
            task_res
            .ranges_need_compact
            .push_back((compact_start.unwrap().to_vec(), last_start_key));
            compact_start = None;
        }
        last_start_key = key.clone();
    }

    if compact_start.is_some() {
        task_res
        .ranges_need_compact
        .push_back((compact_start.unwrap().to_vec(), last_start_key.to_vec()));
    }
    info!("{:?}", task_res);
    Ok(task_res)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempdir::TempDir;

    use rocksdb::{self, DB, Writable};
    use storage::types::Key as MvccKey;
    use storage::mvcc::{Write, WriteType};
    use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use util::rocksdb::{new_engine_opt, get_cf_handle, CFOptions};
    use util::properties::MvccPropertiesCollectorFactory;
    use raftstore::store::keys::data_key;

    use super::*;

    fn mvcc_put(db: &DB, k: &[u8], v: &[u8], start_ts: u64, commit_ts: u64) {
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();
        let k = MvccKey::from_encoded(data_key(k));
        let k = k.append_ts(commit_ts);
        let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
        db.put_cf(cf, k.encoded(), &w.to_bytes()).unwrap();
    }

    fn mvcc_delete(db: &DB, k: &[u8], start_ts: u64, commit_ts: u64) {
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();
        let k = MvccKey::from_encoded(data_key(k));
        let k = k.append_ts(commit_ts);
        let w = Write::new(WriteType::Delete, start_ts, None);
        db.put_cf(cf, k.encoded(), &w.to_bytes()).unwrap();
    }

    fn delete(db: &DB, k: &[u8], commit_ts: u64) {
        let cf = get_cf_handle(&db, CF_WRITE).unwrap();
        let k = MvccKey::from_encoded(data_key(k));
        let k = k.append_ts(commit_ts);
        db.delete_cf(cf, k.encoded()).unwrap();
    }

    fn open_db(path: &str) -> DB {
        let db_opts = rocksdb::DBOptions::new();
        let mut cf_opts = rocksdb::ColumnFamilyOptions::new();
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

        // mvcc_put 0..50
        for i in 0..50 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1, 2);
        }
        engine.flush_cf(cf, true).unwrap();

        // gc 0..50
        for i in 0..50 {
            let k = format!("k{}", i);
            delete(&engine, k.as_bytes(), 2);
        }
        engine.flush_cf(cf, true).unwrap();

        // mvcc_put 51..100
        for i in 51..100 {
            let (k, v) = (format!("k{}", i), format!("value{}", i));
            mvcc_put(&engine, k.as_bytes(), v.as_bytes(), 1, 2);
        }
        engine.flush_cf(cf, true).unwrap();

        let mut ranges_to_check = BTreeSet::new();
        ranges_to_check.insert(data_key(b"k0"));
        ranges_to_check.insert(data_key(b"k50"));
        ranges_to_check.insert(data_key(b"k99"));

        let ranges_need_to_compact = collect_ranges_need_compact(&engine, ranges_to_check, 1).unwrap();
        println!("ranges_need_to_compact {:?}", ranges_need_to_compact);
    }
}
