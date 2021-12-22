// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{endpoint::Task, errors::Error, metadata::store::EtcdStore};

use super::errors::Result;
use engine_traits::{CF_DEFAULT, CF_WRITE};

use kvproto::{brpb::DataFileInfo, raft_cmdpb::CmdType};
use openssl::hash::{Hasher, MessageDigest};
use raftstore::coprocessor::CmdBatch;
use slog_global::debug;
use tidb_query_datatype::codec::table::decode_table_id;

use tikv_util::{box_err, worker::Scheduler};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use txn_types::{Key, TimeStamp};

impl ApplyEvent {
    /// Convert a [CmdBatch] to a vector of events. Ignoring admin / error commands.
    /// Assuming the resolved ts of the region is `resolved_ts`.
    /// Note: the resolved ts cannot be advanced if there is no command,
    ///       maybe we also need to update resolved_ts when flushing?
    pub fn from_cmd_batch(cmd: CmdBatch, resolved_ts: u64) -> Vec<Self> {
        let region_id = cmd.region_id;
        let mut result = vec![];
        for mut req in cmd
            .cmds
            .into_iter()
            .filter(|cmd| {
                // We will add some log then, this is just a template.
                #[allow(clippy::if_same_then_else)]
                #[allow(clippy::needless_bool)]
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
            .flat_map(|mut cmd| cmd.request.take_requests().into_iter())
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

    /// Check whether the key associate to the event is a meta key.
    pub fn is_meta(&self) -> bool {
        // Can we make things not looking so hacky?
        self.key.starts_with(b"m")
    }

    /// Check whether the event should be recorded.
    /// (We would ignore LOCK cf)
    pub fn should_record(&self) -> bool {
        let cf_can_handle = self.cf == CF_DEFAULT || self.cf == CF_WRITE;
        // should we handle prewrite here?
        let cmd_can_handle = self.cmd_type == CmdType::Delete || self.cmd_type == CmdType::Put;
        cf_can_handle && cmd_can_handle
    }
}

/// The shared version of router.
#[derive(Debug, Clone)]
pub struct Router(Arc<Mutex<RouterInner>>);

impl Router {
    /// Create a new router with the temporary folder.
    pub fn new(prefix: PathBuf, scheduler: Scheduler<Task>, temp_file_size_limit: u64) -> Self {
        Self(Arc::new(Mutex::new(RouterInner::new(
            prefix,
            scheduler,
            temp_file_size_limit,
        ))))
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
    // TODO replace all map like things with lock free map, to get rid of the Mutex.
    /// The index for search tasks by range.
    /// It uses the `start_key` of range as the key.
    /// Given there isn't overlapping, we can simply use binary search to find
    /// which range a point belongs to.
    ranges: BTreeMap<KeyRange, TaskRange>,
    /// The temporary files associated to some task.
    temp_files_of_task: HashMap<String, TemporaryFiles>,
    /// The temporary directory for all tasks.
    prefix: PathBuf,

    /// The handle to Endpoint, we should send `Flush` to endpoint if there are too many temporary files.
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

#[derive(Debug)]
pub struct ApplyEvent {
    key: Vec<u8>,
    value: Vec<u8>,
    cf: String,
    region_id: u64,
    region_resolved_ts: u64,
    cmd_type: CmdType,
}

impl RouterInner {
    pub fn new(prefix: PathBuf, scheduler: Scheduler<Task>, temp_file_size_limit: u64) -> Self {
        RouterInner {
            ranges: BTreeMap::default(),
            temp_files_of_task: HashMap::default(),
            prefix,
            scheduler,
            temp_file_size_limit,
        }
    }

    /// Register some ranges associated to some task.
    /// Because the observer interface yields encoded data key, the key should be ENCODED DATA KEY too.    
    /// (i.e. encoded by `Key::from_raw(key).into_encoded()`, [`utils::wrap_key`] could be a shortcut.).    
    /// We keep ranges in memory to filter kv events not in these ranges.  
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
            debug!(
                "backup stream register observe range";
                "task_name" => task_name,
                "start_key" => &log_wrappers::Value::key(&key_range.0),
                "end_key" => &log_wrappers::Value::key(&task_range.end),
            );
            self.ranges.insert(key_range, task_range);
        }
    }

    /// get the task name by a key.
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

    pub async fn on_event(&mut self, kv: ApplyEvent) -> Result<()> {
        let prefix = &self.prefix;
        if let Some(task) = self.get_task_by_key(&kv.key) {
            debug!(
                "backup stream kv";
                "cmdtype" => ?kv.cmd_type,
                "cf" => ?kv.cf,
                "key" => &log_wrappers::Value::key(&kv.key),
            );
            let inner_router = {
                if !self.temp_files_of_task.contains_key(&task) {
                    self.temp_files_of_task
                        .insert(task.clone(), TemporaryFiles::new(prefix.join(&task)).await?);
                }
                self.temp_files_of_task.get_mut(&task).unwrap()
            };
            let prev_size = inner_router.total_size();
            inner_router.on_event(kv).await?;

            // When this event make the size of temporary files exceeds the size limit, make a flush.
            // Note that we only flush if the size is less than the limit before the event,
            // or we may send multiplied flush requests.

            debug!(
                "backup stream statics size";
                "task" => ?task,
                "prev_size" => prev_size,
                "next_size" => inner_router.total_size(),
                "size_limit" => self.temp_file_size_limit,
            );

            if prev_size < self.temp_file_size_limit
                && inner_router.total_size() >= self.temp_file_size_limit
            {
                // TODO: maybe delay the schedule when failure? (Why the scheduler doesn't support blocking send...)
                self.scheduler
                    .schedule(Task::Flush(task))
                    .expect("failed to schedule");
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
}

/// The handle of a temporary file.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
struct TempFileKey {
    is_meta: bool,
    table_id: i64,
    region_id: u64,
    cf: String,
    cmd_type: CmdType,
}

impl TempFileKey {
    /// Create the key for an event. The key can be used to find which temporary file the event should be stored.
    fn of(kv: &ApplyEvent) -> Self {
        let table_id = if kv.is_meta() {
            // Force table id of meta key be zero.
            0
        } else {
            // When we cannot extract the table key, use 0 for the table key(perhaps we insert meta key here.).
            // Can we emit the copy here(or at least, take a slice of key instead of decoding the whole key)?
            Key::from_encoded_slice(&kv.key)
                .into_raw()
                .ok()
                .and_then(|decoded_key| decode_table_id(&decoded_key).ok())
                .unwrap_or(0)
        };
        Self {
            is_meta: kv.is_meta(),
            table_id,
            region_id: kv.region_id,
            cf: kv.cf.clone(),
            cmd_type: kv.cmd_type,
        }
    }

    /// The full name of the file owns the key.
    fn temp_file_name(&self) -> String {
        if self.is_meta {
            format!(
                "meta_{:08}_{}_{:?}.temp.log",
                self.region_id, self.cf, self.cmd_type
            )
        } else {
            format!(
                "{:08}_{:08}_{}_{:?}.temp.log",
                self.table_id, self.region_id, self.cf, self.cmd_type
            )
        }
    }
}

#[derive(Default, Debug)]
pub struct TemporaryFiles {
    /// The parent directory of temporary files.
    temp_dir: PathBuf,
    /// The temporary file index. Both meta (m prefixed keys) and data (t prefixed keys).
    files: HashMap<TempFileKey, DataFile>,
    /// The min resolved TS of all regions involved.
    min_resolved_ts: TimeStamp,
    /// Total size of all temporary files in byte.
    total_size: usize,
}

impl TemporaryFiles {
    /// Create a new temporary file set at the `temp_dir`.
    pub async fn new(temp_dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&temp_dir).await?;
        Ok(Self {
            temp_dir,
            min_resolved_ts: TimeStamp::max(),
            ..Default::default()
        })
    }

    // TODO: make a file-level lock for getting rid of the &mut.
    /// Append a event to the files. This wouldn't trigger `fsync` syscall.
    /// i.e. No guarantee of persistence.
    pub async fn on_event(&mut self, kv: ApplyEvent) -> Result<()> {
        let key = TempFileKey::of(&kv);
        if !self.files.contains_key(&key) {
            let path = self.temp_dir.join(key.temp_file_name());
            let data_file = DataFile::new(path).await?;
            self.files.insert(key.clone(), data_file);
        }
        self.total_size += self.files.get_mut(&key).unwrap().on_event(kv).await?;
        Ok(())
    }

    fn path_to_log_file(table_id: i64, min_ts: u64) -> String {
        format!(
            "/v1/t{:012}/{:012}-{}.log",
            table_id,
            min_ts,
            uuid::Uuid::new_v4()
        )
    }

    fn path_to_schema_file(min_ts: u64) -> String {
        format!("/v1/m/{:012}-{}.log", min_ts, uuid::Uuid::new_v4())
    }

    pub fn total_size(&self) -> u64 {
        self.total_size as _
    }

    /// Flush all temporary files to disk.
    async fn flush(&mut self) -> Result<()> {
        futures::future::join_all(self.files.values_mut().map(|f| f.inner.sync_all()))
            .await
            .into_iter()
            .map(|r| r.map_err(Error::from))
            .fold(Ok(()), Result::and)
    }

    /// Flush all files and generate corresponding metadata.
    pub async fn generate_metadata(mut self) -> Result<MetadataOfFlush> {
        self.flush().await?;
        let mut result = MetadataOfFlush::with_capacity(self.files.len());
        for (
            TempFileKey {
                table_id,
                cf,
                region_id,
                is_meta,
                ..
            },
            data_file,
        ) in self.files.into_iter()
        {
            let mut file_meta = data_file.generate_metadata()?;
            let path = if is_meta {
                Self::path_to_schema_file(file_meta.meta.min_ts)
            } else {
                Self::path_to_log_file(table_id, file_meta.meta.min_ts)
            };
            file_meta.meta.set_path(path);
            file_meta.meta.set_cf(cf);
            file_meta.meta.set_region_id(region_id as _);
            result.push(file_meta)
        }
        Ok(result)
    }
}

/// A opened log file with some metadata.
struct DataFile {
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
    pub meta: DataFileInfo,
    pub local_path: PathBuf,
}

#[derive(Debug)]
pub struct MetadataOfFlush {
    pub files: Vec<DataFileWithMeta>,
    pub min_resolved_ts: u64,
}

impl MetadataOfFlush {
    fn with_capacity(cap: usize) -> Self {
        Self {
            files: Vec::with_capacity(cap),
            min_resolved_ts: u64::MAX,
        }
    }

    fn push(&mut self, file: DataFileWithMeta) {
        let rts = file.meta.resolved_ts;
        self.min_resolved_ts = self.min_resolved_ts.min(rts);
        self.files.push(file);
    }
}

impl DataFile {
    /// create and open a logfile at the path.
    /// Note: if a file with same name exists, would truncate it.
    async fn new(local_path: impl AsRef<Path>) -> Result<Self> {
        let sha256 = Hasher::new(MessageDigest::sha256())
            .map_err(|err| Error::Other(box_err!("openssl hasher failed to init: {}", err)))?;
        Ok(Self {
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
    async fn on_event(&mut self, mut kv: ApplyEvent) -> Result<usize> {
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
            .map_err(|err| Error::Other(box_err!("openssl hasher failed to update: {}", err)))?;
        self.update_key_bound(key.into_encoded());
        Ok(size)
    }

    /// Update the `start_key` and `end_key` of `self` as if a new key added.
    fn update_key_bound(&mut self, key: Vec<u8>) {
        // if there is nothing in file, fill the start_key and end_key by current key.
        if self.start_key.is_empty() && self.end_key.is_empty() {
            self.start_key = key.clone();
            self.end_key = key;
            return;
        }

        // expand the start_key and end_key if key out-of-range joined.
        if self.start_key > key {
            self.start_key = key;
        } else if self.end_key < key {
            self.end_key = key;
        }
    }

    /// generate the metadata in protocol buffer of the file.
    fn generate_metadata(mut self) -> Result<DataFileWithMeta> {
        let mut meta = DataFileInfo::new();
        meta.set_sha_256(
            self.sha256
                .finish()
                .map(|bytes| bytes.to_vec())
                .map_err(|err| {
                    Error::Other(box_err!("openssl hasher failed to finish: {}", err))
                })?,
        );
        meta.set_number_of_entries(self.number_of_entries as _);
        meta.set_max_ts(self.max_ts.into_inner() as _);
        meta.set_min_ts(self.min_ts.into_inner() as _);
        meta.set_resolved_ts(self.resolved_ts.into_inner() as _);
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
    use crate::utils;
    use std::time::Duration;
    use tikv_util::{
        codec::number::NumberEncoder,
        worker::{dummy_scheduler, ReceiverWrapper},
    };

    use super::*;

    #[derive(Debug)]
    struct KvEventsBuilder {
        region_id: u64,
        region_resolved_ts: u64,
        events: Vec<ApplyEvent>,
    }

    fn make_table_key(table_id: i64, key: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut table_key = b"t".to_vec();
        // make it comparable to uint.
        table_key
            .encode_u64(table_id as u64 ^ 0x8000_0000_0000_0000)
            .unwrap();
        Write::write_all(&mut table_key, key).unwrap();
        table_key
    }

    impl KvEventsBuilder {
        fn new(region_id: u64, region_resolved_ts: u64) -> Self {
            Self {
                region_id,
                region_resolved_ts,
                events: vec![],
            }
        }

        fn wrap_key(&self, key: Vec<u8>) -> Vec<u8> {
            let key = Key::from_encoded(utils::wrap_key(key));
            key.append_ts(TimeStamp::compose(
                TimeStamp::physical_now(),
                self.events.len() as _,
            ))
            .into_encoded()
        }

        fn put_event(&self, cf: &'static str, key: Vec<u8>, value: Vec<u8>) -> ApplyEvent {
            ApplyEvent {
                key: self.wrap_key(key),
                value,
                cf: cf.to_owned(),
                region_id: self.region_id,
                region_resolved_ts: self.region_resolved_ts,
                cmd_type: CmdType::Put,
            }
        }

        fn delete_event(&self, cf: &'static str, key: Vec<u8>) -> ApplyEvent {
            ApplyEvent {
                key: self.wrap_key(key),
                value: vec![],
                cf: cf.to_owned(),
                region_id: self.region_id,
                region_resolved_ts: self.region_resolved_ts,
                cmd_type: CmdType::Delete,
            }
        }

        fn put_table(&mut self, cf: &'static str, table: i64, key: &[u8], value: &[u8]) {
            let table_key = make_table_key(table, key);
            self.events
                .push(self.put_event(cf, table_key, value.to_vec()));
        }

        fn delete_table(&mut self, cf: &'static str, table: i64, key: &[u8]) {
            let table_key = make_table_key(table, key);
            self.events.push(self.delete_event(cf, table_key));
        }

        fn flush_events(&mut self) -> Vec<ApplyEvent> {
            std::mem::take(&mut self.events)
        }
    }

    #[test]
    fn test_register() {
        let (tx, _) = dummy_scheduler();
        let mut router = RouterInner::new(PathBuf::new(), tx, 1024);
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

    fn collect_recv(mut rx: ReceiverWrapper<Task>) -> Vec<Task> {
        let mut result = vec![];
        while let Ok(Some(task)) = rx.recv_timeout(Duration::from_secs(0)) {
            result.push(task);
        }
        result
    }

    #[tokio::test]
    async fn test_basic_file() -> Result<()> {
        let tmp = std::env::temp_dir().join(format!("{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&tmp).await?;
        let (tx, rx) = dummy_scheduler();
        let mut router = RouterInner::new(tmp.clone(), tx, 32);
        router.register_ranges(
            "dummy",
            vec![(
                utils::wrap_key(make_table_key(1, b"")),
                utils::wrap_key(make_table_key(2, b"")),
            )],
        );
        let now = TimeStamp::physical_now();
        let mut region1 = KvEventsBuilder::new(1, now);
        let start_ts = TimeStamp::physical_now();
        region1.put_table(CF_DEFAULT, 1, b"hello", b"world");
        region1.put_table(CF_WRITE, 1, b"hello", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"bonjour", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"nihao", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 2, b"hello", b"this isn't a write record :3");
        region1.put_table(CF_WRITE, 1, b"hello", b"still isn't a write record :3");
        region1.delete_table(CF_DEFAULT, 1, b"hello");
        println!("{:?}", region1);
        let events = region1.flush_events();
        for event in events {
            router.on_event(event).await?;
        }
        let end_ts = TimeStamp::physical_now();
        let files = router.take_temporary_files("dummy")?;
        let meta = files.generate_metadata().await?;
        assert_eq!(meta.files.len(), 3);
        assert!(
            meta.files.iter().all(|item| {
                TimeStamp::new(item.meta.min_ts as _).physical() >= start_ts
                    && TimeStamp::new(item.meta.max_ts as _).physical() <= end_ts
                    && item.meta.min_ts <= item.meta.max_ts
            }),
            "meta = {:#?}; start ts = {}, end ts = {}",
            meta.files,
            start_ts,
            end_ts
        );
        println!("{:#?}", meta);
        drop(router);
        let cmds = collect_recv(rx);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            Task::Flush(task) => assert_eq!(task, "dummy"),
            _ => panic!("the cmd isn't flush!"),
        }

        for path in meta.files.iter().map(|meta| &meta.local_path) {
            let f = tokio::fs::metadata(&path).await?;
            assert!(f.is_file(), "log file {} is not a file", path.display());
            // The file should contains some data.
            assert!(
                f.len() > 10,
                "the log file {} is too small (size = {}B)",
                path.display(),
                f.len()
            );
        }
        Ok(())
    }
}
