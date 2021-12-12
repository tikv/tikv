// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
};

use crate::{endpoint::Task, errors::Error, metadata::store::EtcdStore};

use super::errors::Result;
use file_system::Sha256Reader;
use kvproto::{
    brpb::LogFile,
    raft_cmdpb::{CmdType, RaftCmdRequest, Request},
};
use openssl::hash::{Hasher, MessageDigest};
use raftstore::coprocessor::{Cmd, CmdBatch};
use slog_global::debug;
use tidb_query_datatype::codec::table::decode_table_id;
use tikv::storage::kv::BTreeEngine;
use tikv_util::{
    box_err,
    config::{ReadableSize, MIB},
    worker::Scheduler,
};
use tokio::sync::Mutex;
use txn_types::{Key, TimeStamp};
// TODO: maybe replace it with tokio fs.
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

impl NewKv {
    pub fn from_cmd_batch(mut cmd: CmdBatch, resolved_ts: u64) -> Vec<Self> {
        let region_id = cmd.region_id;
        let mut result = vec![];
        for req in cmd
            .cmds
            .into_iter()
            .filter(|cmd| {
                if cmd.response.get_header().has_error() {
                    // Add some log for skipping the error.
                    false
                } else if cmd.request.has_admin_request() {
                    // Add some log for skipping the admin request.
                    false
                } else {
                    true
                }
            })
            .flat_map(|cmd| cmd.request.take_requests().into_iter())
        {
            let (key, value, cf) = match req.get_cmd_type() {
                CmdType::Put => {
                    let mut put = req.take_put();
                    (put.take_key(), put.take_value(), put.cf)
                }
                CmdType::Delete => {
                    let mut del = req.take_delete();
                    (del.take_key(), Vec::new(), del.cf)
                }
                _ => {
                    debug!(
                        "backup stream skip other command";
                        "command" => ?req,
                    );
                    continue;
                }
            };
            result.push(Self {
                key,
                value,
                cf,
                region_id,
                region_resolved_ts: resolved_ts,
                cmd_type: req.get_cmd_type(),
            })
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct Router(Arc<Mutex<RouterInner>>);

impl Router {
    pub fn new(prefix: PathBuf, scheduler: Scheduler<Task>) -> Self {
        Self(Arc::new(Mutex::new(RouterInner::new(prefix, scheduler))))
    }
}

impl std::ops::Deref for Router {
    type Target = Mutex<RouterInner>;

    fn deref(&self) -> &Self::Target {
        Arc::deref(&self.0)
    }
}

/// An Router for Backup Stream.
///
/// It works as a table-filter.
///   1. route the kv event to different task
///   2. filter the kv event not belong to the task
// TODO maybe we should introduce table key from tidb_query_datatype module.
pub struct RouterInner {
    // TODO find a proper way to record the ranges of table_filter.
    ranges: BTreeMap<KeyRange, TaskRange>,
    // TODO replace all map like things with lock free map, to get rid of the Mutex.
    temp_files_of_task: HashMap<String, TemporaryFiles>,
    prefix: PathBuf,

    scheduler: Scheduler<Task>,
    /// The size limit of temporary file per task.
    temp_file_size_limit: u64,
}

impl std::fmt::Debug for RouterInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterInner")
            .field("ranges", &self.ranges)
            .field("temp_files_of_task", &self.temp_files_of_task)
            .field("prefix", &self.prefix)
            .finish()
    }
}

pub struct NewKv {
    key: Vec<u8>,
    value: Vec<u8>,
    cf: String,
    region_id: u64,
    region_resolved_ts: u64,
    cmd_type: CmdType,
}

impl RouterInner {
    pub fn new(prefix: PathBuf, scheduler: Scheduler<Task>) -> Self {
        RouterInner {
            ranges: BTreeMap::default(),
            temp_files_of_task: HashMap::default(),
            prefix,
            scheduler,
            // Maybe make it configurable?
            temp_file_size_limit: 128 * MIB,
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

    pub async fn on_event(&mut self, kv: NewKv) -> Result<()> {
        let prefix = &self.prefix;
        if let Some(task) = self.get_task_by_key(&kv.key) {
            let inner_router = self
                .temp_files_of_task
                .entry(task.clone())
                .or_insert_with(|| TemporaryFiles::new(prefix.join(&task)));
            let prev_size = inner_router.total_size();
            inner_router.on_event(kv).await?;

            // When this event make the size of temporary files exceeds the size limit, make a flush.
            // Note that we only flush if the size is less than the limit before the event,
            // or we may send multiplied flush requests.
            if prev_size < self.temp_file_size_limit
                && inner_router.total_size() > self.temp_file_size_limit
            {
                self.scheduler.schedule(Task::Flush(task));
            }
        }
        Ok(())
    }

    pub fn take_temporary_files(&mut self, task: &str) -> Result<TemporaryFiles> {
        self.temp_files_of_task.remove(task).map_or(
            Err(Error::NoSuchTask {
                task_name: task.to_owned(),
            }),
            |data| Ok(data),
        )
    }

    pub fn flush_task(&mut self, task: &str) -> Result<Vec<DataFileWithMeta>> {
        let collector = self.take_temporary_files(task)?;
        Ok(collector.generate_metadata()?)
    }
}

#[derive(Default, Debug)]
struct TemporaryFiles {
    temp_dir: PathBuf,
    // (table_id, region_id)
    files: HashMap<(i64, u64), DataFile>,
    total_size: usize,
}

impl TemporaryFiles {
    pub fn new(temp_dir: PathBuf) -> Self {
        Self {
            temp_dir,
            ..Default::default()
        }
    }

    // TODO: make a file-level lock for git rid of the &mut.
    pub async fn on_event(&mut self, kv: NewKv) -> Result<()> {
        let table_id = decode_table_id(&kv.key).expect("TableRouter: Not table key");
        if !self.files.contains_key(&(table_id, kv.region_id)) {
            let path = self
                .temp_dir
                .join(format!("{:08}_{:08}.temp.log", table_id, kv.region_id));
            let data_file = DataFile::new(path, kv.region_id).await?;
            self.files.insert((table_id, kv.region_id), data_file);
        }
        self.total_size += self
            .files
            .get_mut(&(table_id, kv.region_id))
            .unwrap()
            .on_event(kv)
            .await?;
        Ok(())
    }

    /// Return the name with path prefix.
    fn path_to_log_file(table_id: i64, min_ts: i64) -> String {
        format!(
            "/v1/t{:12}/{:12}-{}.log",
            table_id,
            min_ts,
            uuid::Uuid::new_v4()
        )
    }

    pub fn total_size(&self) -> u64 {
        self.total_size as _
    }

    /// Flush all temporary files.
    pub async fn flush(&mut self) -> Result<()> {
        futures::future::join_all(self.files.values_mut().map(|f| f.inner.flush()))
            .await
            .into_iter()
            .map(|r| r.map_err(Error::from))
            .fold(Ok(()), Result::and)
    }

    pub fn generate_metadata(self) -> Result<Vec<DataFileWithMeta>> {
        let mut result = Vec::with_capacity(self.files.len());
        for ((table_id, region_id), data_file) in self.files.into_iter() {
            let mut file_meta = data_file.generate_metadata()?;
            file_meta
                .meta
                .set_path(Self::path_to_log_file(table_id, file_meta.meta.min_ts));
            result.push(file_meta)
        }
        Ok(result)
    }
}

struct DataFile {
    region: u64,
    min_ts: TimeStamp,
    max_ts: TimeStamp,
    resolved_ts: TimeStamp,
    sha256: Hasher,
    inner: File,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    number_of_entries: usize,
    local_path: PathBuf,
}

#[derive(Debug)]
pub struct DataFileWithMeta {
    pub meta: LogFile,
    pub local_path: PathBuf,
}

impl DataFile {
    async fn new(local_path: impl AsRef<Path>, region_id: u64) -> Result<Self> {
        let sha256 = Hasher::new(MessageDigest::sha256())
            .map_err(|err| box_err!("openssl hasher failed to init: {}", err).into());
        Ok(Self {
            region: region_id,
            min_ts: TimeStamp::max(),
            max_ts: TimeStamp::zero(),
            resolved_ts: TimeStamp::zero(),
            inner: File::create(local_path.as_ref()).await?,
            sha256,
            number_of_entries: 0,
            start_key: vec![],
            end_key: vec![],
            local_path: local_path.as_ref().to_owned(),
        })
    }

    /// Add a new KV pair to the file, returning its size.
    async fn on_event(&mut self, mut kv: NewKv) -> Result<usize> {
        let encoded = super::endpoint::Endpoint::<EtcdStore>::encode_event(&kv.key, &kv.value);
        let size = encoded.len();
        let key = Key::from_encoded(std::mem::take(&mut kv.key));
        let ts = key.decode_ts().expect("key without ts");
        self.min_ts = self.min_ts.min(ts);
        self.max_ts = self.max_ts.max(ts);
        self.resolved_ts = self.resolved_ts.max(kv.region_resolved_ts.into());
        self.inner.write_all(encoded.as_slice()).await?;
        self.sha256
            .update(encoded.as_slice())
            .map_err(|err| box_err!("openssl hasher failed to update: {}", err).into())?;
        self.update_key_bound(key.into_encoded());
        Ok(size)
    }

    fn update_key_bound(&mut self, key: Vec<u8>) {
        if self.start_key.is_empty() && self.end_key.is_empty() {
            self.start_key = key.clone();
            self.end_key = key;
        }
        if self.start_key > key {
            self.start_key = key;
        } else if self.end_key < key {
            self.end_key = key;
        }
    }

    fn generate_metadata(mut self) -> Result<DataFileWithMeta> {
        let mut meta = LogFile::new();
        meta.set_sha_256(
            self.sha256
                .finish()
                .map(|bytes| bytes.to_vec())
                .map_err(|err| box_err!("openssl hasher failed to finish: {}", err).into())?,
        );
        meta.set_number_of_entries(self.number_of_entries as _);
        meta.set_max_ts(self.max_ts.into_inner() as _);
        meta.set_min_ts(self.min_ts.into_inner() as _);
        meta.set_start_key(std::mem::take(&mut self.start_key));
        meta.set_end_key(std::mem::take(&mut self.end_key));
        let file_with_meta = DataFileWithMeta {
            meta,
            local_path: self.local_path,
        };
        Ok(file_with_meta)
    }
}

impl std::fmt::Debug for DataFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataFile")
            .field("region", &self.region)
            .field("min_ts", &self.min_ts)
            .field("max_ts", &self.max_ts)
            .field("resolved_ts", &self.resolved_ts)
            .field("local_path", &self.local_path.display())
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
    use tikv_util::worker::dummy_scheduler;

    use super::*;

    #[test]
    fn test_register() {
        let (tx, _) = dummy_scheduler();
        let mut router = RouterInner::new(PathBuf::new(), tx);
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
