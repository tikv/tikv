// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

use crate::metadata::store::EtcdStore;

use super::errors::Result;
use kvproto::raft_cmdpb::CmdType;
use tidb_query_datatype::codec::table::decode_table_id;
use txn_types::{Key, TimeStamp};
// TODO: maybe replace it with tokio fs.
use std::fs::File;
use std::io::Write;

/// An Router for Backup Stream.
///
/// It works as a table-filter.
///   1. route the kv event to different task
///   2. filter the kv event not belong to the task

// TODO maybe we should introduce table key from tidb_query_datatype module.
#[derive(Debug, Default)]
pub struct Router {
    // TODO find a proper way to record the ranges of table_filter.
    ranges: BTreeMap<KeyRange, TaskRange>,
    tables: BTreeMap<String, TableRouter>,
    prefix: PathBuf,
}

pub struct NewKv {
    key: Vec<u8>,
    value: Vec<u8>,
    cf: &'static str,
    region_id: i64,
    region_resolved_ts: u64,
    cmd_type: CmdType,
}

impl Router {
    pub fn new() -> Self {
        Router {
            ..Default::default()
        }
    }
    // keep ranges in memory to filter kv events not in these ranges.
    pub fn register_ranges(&mut self, task_name: &str, ranges: Vec<(Vec<u8>, Vec<u8>)>) {
        // TODO reigister ranges to filter kv event
        // register ranges has two main purpose.
        // 1. filter kv event that no need to backup
        // 2. route kv event to the corresponding file.

        for range in ranges {
            let key_range = KeyRange(range.0);
            let task_range = TaskRange {
                end: range.1,
                task_name: task_name.to_string(),
            };
            self.ranges.insert(key_range, task_range);
        }
    }

    // filter key not in ranges
    pub fn key_in_ranges(&self, key: &[u8]) -> bool {
        self.get_task_by_key(key).is_some()
    }

    pub fn get_task_by_key(&self, key: &[u8]) -> Option<String> {
        // TODO avoid key.to_vec()
        let k = &KeyRange(key.to_vec());
        self.ranges
            .range(..k)
            .next_back()
            .filter(|r| key <= &r.1.end[..] && key >= &r.0.0[..])
            .map_or_else(
                || {
                    self.ranges
                        .range(k..)
                        .next()
                        .filter(|r| key <= &r.1.end[..] && key >= &r.0.0[..])
                        .map(|r| r.1.task_name.clone())
                },
                |r| Some(r.1.task_name.clone()),
            )
    }

    pub fn on_event(&mut self, kv: NewKv) -> Result<()> {
        if let Some(task) = self.get_task_by_key(&kv.key) {
            let inner_router = self
                .tables
                .entry(task)
                .or_insert_with(|| TableRouter::new(self.prefix.join(&task)));
            inner_router.on_event(kv)?
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
struct TableRouter {
    temp_dir: PathBuf,
    // (table_id, region_id)
    files: HashMap<(i64, i64), DataFile>,
}

impl TableRouter {
    pub fn new(temp_dir: PathBuf) -> Self {
        Self {
            temp_dir,
            ..Default::default()
        }
    }

    pub fn on_event(&mut self, kv: NewKv) -> Result<()> {
        let table_id = decode_table_id(&kv.key).expect("TableRouter: Not table key");
        if !self.files.contains_key(&(table_id, kv.region_id)) {
            let path = self
                .temp_dir
                .join(format!("{:08}_{:08}.temp.log", table_id, kv.region_id));
            let file = File::create(path)?;
            let data_file = DataFile::new(file, kv.region_id);
            self.files.insert((table_id, kv.region_id), data_file);
        }
        self.files
            .get(&(table_id, kv.region_id))
            .unwrap()
            .on_event(kv)?;
        Ok(())
    }
}

struct DataFile {
    region: i64,
    min_ts: Cell<TimeStamp>,
    max_ts: Cell<TimeStamp>,
    resolved_ts: Cell<TimeStamp>,
    inner: File,
}

impl DataFile {
    fn new(file: File, region_id: i64) -> Self {
        Self {
            region: region_id,
            min_ts: Cell::new(TimeStamp::max()),
            max_ts: Cell::new(TimeStamp::zero()),
            resolved_ts: Cell::new(TimeStamp::zero()),
            inner: file,
        }
    }

    fn on_event(&self, mut kv: NewKv) -> Result<()> {
        let encoded = super::endpoint::Endpoint::<EtcdStore>::encode_event(&kv.key, &kv.value);
        let key = Key::from_encoded(std::mem::take(&mut kv.key));
        let ts = key.decode_ts().expect("key without ts");
        if ts < self.min_ts.get() {
            self.min_ts.set(ts)
        }
        if ts > self.max_ts.get() {
            self.max_ts.set(ts)
        }
        if kv.region_resolved_ts > self.resolved_ts.get().into_inner() {
            self.resolved_ts.set(kv.region_resolved_ts.into())
        }
        self.inner.write(encoded.as_slice())?;
        Ok(())
    }
}

impl std::fmt::Debug for DataFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("region", &self.region)
            .field("min_ts", &self.min_ts)
            .field("max_ts", &self.max_ts)
            .field("resolved_ts", &self.resolved_ts)
            .finish()
    }
}

#[derive(Clone, Ord, PartialOrd, PartialEq, Eq, Debug)]
struct KeyRange(Vec<u8>);

#[derive(Clone, Debug)]
struct TaskRange {
    end: Vec<u8>,
    task_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register() {
        let mut router = Router::new();
        // -----t1.start-----t1.end-----t2.start-----t2.end------
        // --|------------|----------|------------|-----------|--
        // case1        case2      case3        case4       case5
        // None        Found(t1)    None        Found(t2)   None
        router.register_ranges("t1", vec![(vec![1, 2, 3], vec![2, 3, 4])]);

        router.register_ranges("t2", vec![(vec![2, 3, 6], vec![3, 4])]);

        assert_eq!(router.get_task_by_key(&[1, 1, 1]), None);
        assert_eq!(router.get_task_by_key(&[1, 2, 4]), Some("t1".to_string()),);
        assert_eq!(router.get_task_by_key(&[2, 3, 5]), None);
        assert_eq!(router.get_task_by_key(&[2, 4]), Some("t2".to_string()),);
        assert_eq!(router.get_task_by_key(&[4, 4]), None,)
    }
}
