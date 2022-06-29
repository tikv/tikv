// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{mpsc::SyncSender, Arc, Mutex, RwLock},
    thread::{self, JoinHandle},
};

use bytes::{Buf, Bytes};
use dashmap::mapref::one::Ref;
use raft_proto::eraftpb;
use slog_global::info;
use tikv_util::time::Instant;

use crate::{
    log_batch::{RaftLogBlock, RaftLogs},
    metrics::*,
    write_batch::{RegionBatch, WriteBatch},
    *,
};

/// `RfEngine` is a persistent storage engine for multi-raft logs.
/// It stores part of raft logs and states(key/value pair) in memory and persists them to disk.
///
/// A typical directory structure is:
///   .
///   ├── {epoch}.wal
///   ├── {epoch}.states
///   ├── {epoch}_{region_id}_{first_log_index}_{last_log_index}.rlog
///   ├── {epoch}_{region_id}_{first_log_index}_{last_log_index}.rlog
///   ├── ...
///   └── recycle
///       └── {epoch}.wal
///       └── {epoch}.wal
///       └── ...
///
/// # Memory Layout
///
/// `RfEngine` contains all raft group states and non-truncated logs in memory, so that it
/// can get raft logs quickly.
///
/// # WAL
///
/// `RfEngine` writes all raft groups' logs and states to a WAL file sequentially.
/// When the WAL file size exceeds the threshold, it triggers rotation and switching to a new WAL file.
/// The name of a WAL file is `{epoch}.wal`. Epoch increases when rotating.
///
/// ## Rotation
///
/// Rotation splits the data of a WAL file to several files:
///   - `{epoch}.states`: Contains **all** raft groups states, not just states in the corresponding WAL file.
///   The old states file will be removed after rewriting.
///
///   - `{epoch}_{region_id}_{first_log_index}_{last_log_index}.rlog`: Contains logs in
///   [first_log_index, last_log_index) of a single raft group.
///
/// After splitting, the WAL file is moved to the `recycle` directory for later use.
/// `RfEngine` recycles old WAL files for better I/O performance. To distinguish between
/// old data and new data, the data format of WAL contains epoch, i.e., valid data's epoch equals the
/// epoch in the WAL file name.
///
/// # Garbage Collection
///
/// Raft logs that has been applied and persisted to FSM can be truncated. All in-memory logs and `rlog` files before the
/// truncated index will be removed.
#[derive(Clone)]
pub struct RfEngine {
    pub dir: PathBuf,

    pub(crate) writer: Arc<Mutex<WalWriter>>,

    pub(crate) regions: Arc<dashmap::DashMap<u64, RwLock<RegionData>>>,

    pub(crate) task_sender: SyncSender<Task>,

    pub(crate) worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl RfEngine {
    pub fn open(dir: &Path, wal_size: usize) -> Result<Self> {
        maybe_create_recycle_dir(dir)?;
        let mut epoches = read_epoches(dir)?;
        let epoch_id = epoches.last().map(|e| e.id).unwrap_or(1);

        let (tx, rx) = std::sync::mpsc::sync_channel(1024);
        let writer = WalWriter::new(dir, epoch_id, wal_size);
        let mut en = Self {
            dir: dir.to_owned(),
            regions: Arc::default(),
            writer: Arc::new(Mutex::new(writer)),
            task_sender: tx,
            worker_handle: Arc::default(),
        };
        let mut offset = 0;
        let mut worker_states = HashMap::new();
        for ep in &epoches {
            offset = en.load_epoch(ep)?;
            if ep.has_state_file {
                for region in en.regions.iter() {
                    let region = region.read().unwrap();
                    worker_states.insert(region.region_id, region.states.clone());
                }
            }
        }
        // open_file() will overwrite WAL's header, so we have to call it after loading WALs.
        en.writer.lock().unwrap().open_file()?;
        en.writer.lock().unwrap().seek(offset);
        if !epoches.is_empty() {
            epoches.pop();
        }

        {
            let mut worker = Worker::new(dir.to_owned(), epoches, rx, worker_states);
            let join_handle = thread::spawn(move || worker.run());
            en.worker_handle.lock().unwrap().replace(join_handle);
        }

        Ok(en)
    }

    pub(crate) fn get_or_init_region_data(
        &self,
        region_id: u64,
    ) -> Ref<'_, u64, RwLock<RegionData>> {
        self.regions
            .entry(region_id)
            .or_insert_with(|| RwLock::new(RegionData::new(region_id)))
            .downgrade()
    }

    /// Applies and persists the write batch.
    pub fn write(&self, wb: WriteBatch) -> Result<usize> {
        self.apply(&wb);
        self.persist(wb)
    }

    /// Applies the write batch to memory without persisting it to WAL.
    pub fn apply(&self, wb: &WriteBatch) {
        let timer = Instant::now_coarse();
        for (&region_id, batch_data) in &wb.regions {
            let region_data = self.get_or_init_region_data(region_id);
            let mut region_data = region_data.write().unwrap();
            let truncated = region_data.apply(batch_data);
            let truncated_index = region_data.truncated_idx;
            drop(region_data);
            if !truncated.is_empty() {
                self.task_sender
                    .send(Task::Truncate {
                        region_id,
                        truncated_index,
                        truncated,
                    })
                    .unwrap();
            }
        }
        ENGINE_APPLY_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
    }

    /// Persists the write batch to WAL. It can be used in another thread to implement async I/O,
    /// i.e., call `apply` in the main thread and call `persist` in the I/O thread.
    pub fn persist(&self, wb: WriteBatch) -> Result<usize> {
        let timer = Instant::now_coarse();
        let mut writer = self.writer.lock().unwrap();
        let epoch_id = writer.epoch_id;
        for data in wb.regions.values() {
            writer.append_region_data(data);
        }
        let (size, rotated) = writer.flush()?;
        drop(writer);
        if rotated {
            self.task_sender.send(Task::Rotate { epoch_id }).unwrap();
        }
        ENGINE_PERSIST_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        Ok(size)
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    pub fn get_term(&self, region_id: u64, index: u64) -> Option<u64> {
        self.regions
            .get(&region_id)
            .and_then(|data| data.read().unwrap().term(index))
    }

    pub fn get_last_index(&self, region_id: u64) -> Option<u64> {
        self.regions
            .get(&region_id)
            .map(|data| data.read().unwrap().raft_logs.last_index())
            .and_then(|index| if index != 0 { Some(index) } else { None })
    }

    pub fn get_state(&self, region_id: u64, key: &[u8]) -> Option<Bytes> {
        self.regions.get(&region_id).and_then(|data| {
            data.read().unwrap().get_state(key).and_then(|val| {
                // TODO: seems it's impossible.
                if !val.is_empty() {
                    Some(val.clone())
                } else {
                    None
                }
            })
        })
    }

    /// Get the value of the last state key with the `prefix`. `prefix` must be non-empty.
    pub fn get_last_state_with_prefix(&self, region_id: u64, prefix: &[u8]) -> Option<Bytes> {
        debug_assert!(!prefix.is_empty());
        let region_data = self.regions.get(&region_id)?;
        let region_data = region_data.read().unwrap();

        let mut end_prefix = prefix.to_vec();
        end_prefix[prefix.len() - 1] += 1;
        let range = Bytes::copy_from_slice(prefix)..Bytes::from(end_prefix);
        region_data
            .states
            .range(range)
            .rev()
            .next()
            .map(|(_, v)| v.clone())
    }

    /// Iterates states of the region in order or in desc order if `desc` is true until `f` returns
    /// error.
    pub fn iterate_region_states<F>(&self, region_id: u64, desc: bool, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let region_data = self.regions.get(&region_id);
        let region_data = match &region_data {
            Some(data) => data.read().unwrap(),
            None => return Ok(()),
        };

        let states = &region_data.states;
        if desc {
            for (k, v) in states.iter().rev() {
                f(k.chunk(), v.chunk())?;
            }
        } else {
            for (k, v) in states.iter() {
                f(k.chunk(), v.chunk())?;
            }
        }
        Ok(())
    }

    /// Iterates stats of all regions in order or in desc order if `desc` is true and breaks one
    /// regions iteration if `f` returns false.
    pub fn iterate_all_states<F>(&self, desc: bool, mut f: F)
    where
        F: FnMut(u64, &[u8], &[u8]) -> bool,
    {
        self.regions.iter().for_each(|data| {
            let data = data.read().unwrap();
            if desc {
                data.states
                    .iter()
                    .rev()
                    .take_while(|(k, v)| f(data.region_id, k, v))
                    .count();
            } else {
                data.states
                    .iter()
                    .take_while(|(k, v)| f(data.region_id, k, v))
                    .count();
            }
        });
    }

    pub fn stop_worker(&mut self) {
        self.task_sender.send(Task::Close).unwrap();
        let mut handle_ref = self.worker_handle.lock().unwrap();
        if let Some(h) = handle_ref.take() {
            h.join().unwrap();
        }
    }

    /// After split and before the new region is initially flushed, the old region's raft log
    /// can not be truncated, otherwise, it would not be able to recover the new region.
    /// So we can call `add_dependent` after split to protect the raft log.
    /// After the new region is initially flushed or re-ingested or destroyed, call
    /// `remove_dependent` to resume truncating the raft log.
    pub fn add_dependent(&self, region_id: u64, dependent_id: u64) {
        if let Some(len) = self.regions.get(&region_id).map(|data| {
            let mut data = data.write().unwrap();
            data.dependents.insert(dependent_id);
            data.dependents.len()
        }) {
            info!(
                "region {} add dependent {}, dependents_len {}",
                region_id, dependent_id, len
            );
        }
    }

    pub fn remove_dependent(&self, region_id: u64, dependent_id: u64) {
        if let Some(len) = self.regions.get(&region_id).map(|data| {
            let mut data = data.write().unwrap();
            data.dependents.remove(&dependent_id);
            data.dependents.len()
        }) {
            info!(
                "region {} remove dependent {}, dependents_len {}",
                region_id, dependent_id, len
            );
        }
    }

    /// Dumps the state of the engine.
    pub fn get_engine_stats(&self) -> EngineStats {
        let mut total_mem_size = 0;
        let mut total_mem_entries = 0;
        let mut regions_stats = self
            .regions
            .iter()
            .map(|data| {
                let region_stats = data.read().unwrap().get_stats();
                total_mem_size += region_stats.size;
                total_mem_entries += region_stats.num_logs;
                region_stats
            })
            .collect::<Vec<RegionStats>>();
        regions_stats.sort_by(|a, b| (b.size).cmp(&a.size));
        regions_stats.truncate(10);

        let mut disk_size = 0;
        let mut num_files = 0;
        if let Ok(read_dir) = self.dir.read_dir() {
            for e in read_dir.flatten() {
                if let Ok(m) = e.metadata() {
                    num_files += 1;
                    disk_size += m.size();
                }
            }
        }
        EngineStats {
            total_mem_size,
            total_mem_entries,
            disk_size,
            num_files,
            top_10_size_regions: regions_stats,
        }
    }

    /// Dumps the state of the region.
    pub fn get_region_stats(&self, region_id: u64) -> RegionStats {
        self.regions
            .get(&region_id)
            .map(|data| data.read().unwrap().get_stats())
            .unwrap_or_default()
    }
}

pub(crate) fn maybe_create_recycle_dir(dir: &Path) -> Result<()> {
    let recycle_path = dir.join(RECYCLE_DIR);
    if !recycle_path.exists() {
        fs::create_dir_all(recycle_path)?;
        file_system::sync_dir(dir)?;
    } else if !recycle_path.is_dir() {
        return Err(Error::Open(String::from("recycle path is not dir")));
    }
    Ok(())
}

/// `RegionData` contains region data and state in memory.
#[derive(Clone, Default)]
pub(crate) struct RegionData {
    pub(crate) region_id: u64,
    /// `truncated_idx` is the max index that doesn't exists.
    /// After truncating, the first index would be truncated_idx + 1.
    pub(crate) truncated_idx: u64,
    pub(crate) raft_logs: RaftLogs,
    pub(crate) states: BTreeMap<Bytes, Bytes>,
    pub(crate) dependents: HashSet<u64>,
}

impl RegionData {
    pub(crate) fn new(region_id: u64) -> Self {
        Self {
            region_id,
            ..Default::default()
        }
    }

    pub(crate) fn get(&self, index: u64) -> Option<eraftpb::Entry> {
        self.raft_logs.get(index)
    }

    pub(crate) fn term(&self, index: u64) -> Option<u64> {
        self.raft_logs.get(index).map(|e| e.term)
    }

    pub(crate) fn get_state(&self, key: &[u8]) -> Option<&Bytes> {
        self.states.get(key)
    }

    pub(crate) fn apply(&mut self, batch: &RegionBatch) -> Vec<RaftLogBlock> {
        debug_assert_eq!(self.region_id, batch.region_id);
        let mut truncated_blocks = vec![];
        if self.truncated_idx < batch.truncated_idx {
            self.truncated_idx = batch.truncated_idx;
        }
        if self.need_truncate() {
            truncated_blocks = self.raft_logs.truncate(self.truncated_idx);
        }
        for (key, val) in &batch.states {
            if val.is_empty() {
                self.states.remove(key.chunk());
            } else {
                self.states.insert(key.clone(), val.clone());
            }
        }
        for op in &batch.raft_logs {
            let truncated = self.raft_logs.append(op.clone());
            if !truncated.is_empty() {
                truncated_blocks.extend(truncated);
            }
        }
        truncated_blocks
    }

    pub(crate) fn need_truncate(&self) -> bool {
        let first = self.raft_logs.first_index();
        self.dependents.is_empty() && first > 0 && self.truncated_idx >= first
    }

    pub(crate) fn get_stats(&self) -> RegionStats {
        let size = self.raft_logs.size();
        let first_idx = self.raft_logs.first_index();
        let last_idx = self.raft_logs.last_index();
        let num_logs = if last_idx != 0 {
            (last_idx - first_idx + 1) as usize
        } else {
            0
        };
        RegionStats {
            id: self.region_id,
            size,
            num_logs,
            num_states: self.states.len(),
            first_idx,
            last_idx,
            truncated_idx: self.truncated_idx,
            dep_count: self.dependents.len(),
        }
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStats {
    pub total_mem_size: usize,
    pub total_mem_entries: usize,
    pub num_files: usize,
    pub disk_size: u64,
    pub top_10_size_regions: Vec<RegionStats>,
}

#[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RegionStats {
    pub id: u64,
    pub size: usize,
    pub num_logs: usize,
    pub num_states: usize,
    pub first_idx: u64,
    pub last_idx: u64,
    pub truncated_idx: u64,
    pub dep_count: usize,
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::BufReader, os::unix::prelude::FileExt};

    use bytes::{BufMut, BytesMut};
    use engine_traits::{Error as TraitError, RaftEngineReadOnly};
    use eraftpb::{Entry, EntryType};
    use protobuf::Message;
    use slog::o;

    use super::*;
    use crate::log_batch::RaftLogOp;

    #[test]
    fn test_rfengine() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 128 * 1024_usize;
        let mut engine = RfEngine::open(tmp_dir.path(), wal_size).unwrap();
        let mut wb = WriteBatch::new();
        for region_id in 1..=10_u64 {
            let (key, val) = make_state_kv(2, 1);
            wb.set_state(region_id, key.chunk(), val.chunk());
        }
        engine.write(wb).unwrap();

        for idx in 1..=1050_u64 {
            let mut wb = WriteBatch::new();
            for region_id in 1..=10_u64 {
                if region_id == 1 && (idx > 100 && idx < 900) {
                    continue;
                }
                wb.append_raft_log(region_id, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(1, idx);
                wb.set_state(region_id, key.chunk(), val.chunk());
                if idx % 100 == 0 && region_id != 1 {
                    wb.truncate_raft_log(region_id, idx - 100);
                }
            }
            engine.write(wb).unwrap();
        }
        assert_eq!(engine.regions.len(), 10);
        for _ in 0..10 {
            let wal_cnt = engine
                .dir
                .read_dir()
                .unwrap()
                .filter(|p| {
                    p.as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .map_or(false, |e| e == "wal")
                })
                .count();
            if wal_cnt <= 2 {
                break;
            }
            thread::sleep(std::time::Duration::from_secs(1));
        }
        let recycled = engine.dir.join(RECYCLE_DIR).read_dir().unwrap().count();
        assert!(0 < recycled && recycled <= 3);

        let mut old_entries_map = HashMap::new();
        for region_ref in engine.regions.iter() {
            let region_data = region_ref.read().unwrap();
            assert_eq!(*region_ref.key(), region_data.region_id);
            old_entries_map.insert(region_data.region_id, region_data.clone());
        }
        assert_eq!(old_entries_map.len(), 10);
        engine.stop_worker();

        for _ in 0..2 {
            let engine = RfEngine::open(tmp_dir.path(), wal_size).unwrap();
            engine.iterate_all_states(false, |region_id, key, _| {
                let old_region_data = old_entries_map.get(&region_id).unwrap();
                assert!(old_region_data.get_state(key).is_some());
                true
            });
            assert_eq!(engine.regions.len(), 10);
            for new_data_ref in engine.regions.iter() {
                let new_data = new_data_ref.read().unwrap();
                let old_data = old_entries_map.get(new_data_ref.key()).unwrap();
                assert_eq!(old_data.truncated_idx, new_data.truncated_idx);
                assert_eq!(
                    old_data.raft_logs.first_index(),
                    new_data.raft_logs.first_index()
                );
                assert_eq!(
                    old_data.raft_logs.last_index(),
                    new_data.raft_logs.last_index()
                );
                for i in new_data.raft_logs.first_index()..=new_data.raft_logs.last_index() {
                    let entry = new_data.raft_logs.get(i).unwrap();
                    assert_eq!(
                        old_data.get(entry.index).unwrap().data.chunk(),
                        entry.data.chunk()
                    );
                }
            }
        }
    }

    fn make_log_data(index: u64, size: usize) -> eraftpb::Entry {
        let mut entry = eraftpb::Entry::new();
        entry.set_entry_type(eraftpb::EntryType::EntryConfChange);
        entry.set_index(index);
        entry.set_term(1);

        let mut data = BytesMut::with_capacity(size);
        data.resize(size, 0);
        entry.set_data(data.freeze());
        entry
    }

    fn make_state_kv(key_byte: u8, idx: u64) -> (BytesMut, BytesMut) {
        let mut key = BytesMut::new();
        key.put_u8(key_byte);
        let mut val = BytesMut::new();
        val.put_u64_le(idx);
        (key, val)
    }

    fn init_logger() {
        use slog::Drain;
        let decorator = slog_term::PlainDecorator::new(std::io::stdout());
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, o!());
        slog_global::set_global(logger);
    }

    fn new_raft_entry(tp: EntryType, term: u64, index: u64, data: &[u8], context: u8) -> Entry {
        let mut entry = Entry::new();
        entry.set_entry_type(tp);
        entry.set_term(term);
        entry.set_index(index);
        entry.set_data(data.to_vec().into());
        if context > 0 {
            entry.set_context(vec![context].into());
        }
        entry
    }

    #[test]
    fn test_region_data() {
        let mut region_data = RegionData::new(1);

        let mut region_batch = RegionBatch::new(1);
        for i in 1..=5 {
            region_batch.append_raft_log(RaftLogOp::new(&new_raft_entry(
                EntryType::EntryNormal,
                i,
                i,
                b"data",
                0,
            )));
        }
        assert!(region_data.apply(&region_batch).is_empty());
        for i in 1..=5 {
            assert_eq!(
                region_data.get(i).unwrap(),
                region_batch.raft_logs[(i - 1) as usize].to_entry()
            );
            assert_eq!(region_data.term(i).unwrap(), i);
        }
        assert!(region_data.get(6).is_none());
        let region_stats = region_data.get_stats();
        assert_eq!(
            region_stats,
            RegionStats {
                id: 1,
                size: 20,
                num_logs: 5,
                num_states: 0,
                first_idx: 1,
                last_idx: 5,
                truncated_idx: 0,
                dep_count: 0
            }
        );

        region_batch = RegionBatch::new(1);
        region_batch.truncate(5);
        region_batch.set_state(b"k1", b"v1");
        region_batch.set_state(b"k2", b"v2");
        let truncated = region_data.apply(&region_batch);
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].first_index(), 1);
        assert_eq!(truncated[0].last_index(), 5);
        for i in 1..=5 {
            assert!(region_data.get(i).is_none());
        }
        assert_eq!(region_data.get_state(b"k1"), Some(&b"v1".to_vec().into()));
        assert_eq!(region_data.get_state(b"k2"), Some(&b"v2".to_vec().into()));
        let region_stats = region_data.get_stats();
        assert_eq!(
            region_stats,
            RegionStats {
                id: 1,
                size: 0,
                num_logs: 0,
                num_states: 2,
                first_idx: 0,
                last_idx: 0,
                truncated_idx: 5,
                dep_count: 0
            }
        );

        region_batch = RegionBatch::new(1);
        region_batch.truncate(5);
        region_batch.set_state(b"k1", b"");
        assert!(region_data.apply(&region_batch).is_empty());
        assert!(region_data.get_state(b"k1").is_none());
        assert_eq!(region_data.get_state(b"k2"), Some(&b"v2".to_vec().into()));
    }

    #[test]
    fn test_rfengine_basic() {
        const STATE_PREFIX: u8 = b'p';

        let dir = tempfile::tempdir().unwrap();
        let engine = RfEngine::open(dir.path(), 128 * 1024).unwrap();

        // Write 10 logs and states to 2 region.
        let mut data_map = HashMap::new();
        let mut wb = WriteBatch::new();
        for region_id in 1..=2 {
            for i in 1..=10 {
                let entry = new_raft_entry(EntryType::EntryNormal, region_id, i, b"data", 0);
                let (state_key, state_val) = (&[STATE_PREFIX, i as u8], &[i as u8]);
                wb.append_raft_log(region_id, &entry);
                wb.set_state(region_id, state_key, state_val);

                let (entries, states) = data_map
                    .entry(region_id)
                    .or_insert_with(|| (vec![], BTreeMap::new()));
                entries.push(entry);
                states.insert(state_key.to_vec(), state_val.to_vec());
            }
        }
        assert!(engine.write(wb).is_ok());

        assert_eq!(engine.get_term(1, 1), Some(1));
        assert_eq!(engine.get_term(1, 11), None);
        assert_eq!(engine.get_term(3, 1), None);
        assert_eq!(engine.get_last_index(1), Some(10));
        assert_eq!(engine.get_last_index(3), None);

        // Test `get_entry` and `get_state`.
        for (&region_id, (entries, states)) in &data_map {
            entries.iter().for_each(|entry| {
                assert_eq!(
                    entry,
                    &engine.get_entry(region_id, entry.index).unwrap().unwrap(),
                );
            });
            states
                .iter()
                .for_each(|(key, val)| assert_eq!(val, &engine.get_state(region_id, key).unwrap()));
        }
        assert!(engine.get_entry(1, 11).unwrap().is_none());
        assert!(engine.get_entry(3, 1).unwrap().is_none());
        assert!(engine.get_state(1, b"k").is_none());

        // Test `fetch_entries_to`.
        let mut buf = vec![];
        for region_id in 1..=2 {
            for low in 1..=10 {
                for high in low + 1..=11 {
                    assert_eq!(
                        engine
                            .fetch_entries_to(region_id, low, high, None, &mut buf)
                            .unwrap(),
                        (high - low) as usize
                    );
                    assert_eq!(
                        data_map.get(&region_id).unwrap().0
                            [(low - 1) as usize..(high - 1) as usize],
                        buf
                    );
                    buf.clear();
                }
            }
        }
        // Test `fetch_entries_to` should push logs to the buf.
        let region1_entries = &data_map.get(&1).unwrap().0;
        for i in 1..=10 {
            assert_eq!(
                engine
                    .fetch_entries_to(1, i, i + 1, None, &mut buf)
                    .unwrap(),
                1
            );
            assert_eq!(buf, region1_entries[..i as usize]);
        }
        assert!(matches!(
            engine.fetch_entries_to(1, 11, 12, None, &mut buf),
            Err(TraitError::EntriesUnavailable),
        ));
        // Test `fetch_entries_to` limits size.
        let mut max_size = 0;
        for (i, entry) in region1_entries.iter().enumerate() {
            buf.clear();
            max_size += entry.compute_size();
            assert_eq!(
                engine
                    .fetch_entries_to(1, 1, 11, Some(max_size as usize), &mut buf)
                    .unwrap(),
                i + 1
            );
            assert_eq!(buf, region1_entries[..=i]);
        }

        // Test `get_last_state_with_prefix`
        assert_eq!(
            engine
                .get_last_state_with_prefix(1, &[STATE_PREFIX])
                .unwrap(),
            [10_u8].as_slice()
        );
        assert!(
            engine
                .get_last_state_with_prefix(1, &[STATE_PREFIX + 1])
                .is_none()
        );

        // Test `iterate_region_states`
        for desc in [false, true] {
            let mut expect_index = if desc { 10 } else { 1 };
            assert!(
                engine
                    .iterate_region_states(1, desc, |k, v| {
                        assert_eq!(k, &[STATE_PREFIX, expect_index]);
                        assert_eq!(v, &[expect_index]);
                        if desc {
                            expect_index -= 1;
                        } else {
                            expect_index += 1;
                        }
                        Ok(())
                    })
                    .is_ok()
            );
            assert_eq!(expect_index, if desc { 0 } else { 11 });
        }
        // Test `iterate_region_states` breaks.
        let mut count = 0;
        assert!(matches!(
            engine.iterate_region_states(1, false, |_, _| {
                count += 1;
                Err(Error::EOF)
            }),
            Err(Error::EOF)
        ));
        assert_eq!(count, 1);

        // Test `iterate_all_states`
        for desc in [false, true] {
            let mut count = 0;
            engine.iterate_all_states(desc, |id, k, v| {
                assert_eq!(v, data_map.get(&id).unwrap().1.get(k).unwrap());
                count += 1;
                true
            });
            assert_eq!(count, 20);
        }
        // Test `iterate_all_states` breaks.
        let mut count = 0;
        engine.iterate_all_states(false, |_, _, _| {
            count += 1;
            false
        });
        assert_eq!(count, 2);

        // Test `add_dependent` and `remove_dependent`.
        engine.add_dependent(1, 1);
        assert!(
            engine
                .regions
                .get(&1)
                .unwrap()
                .read()
                .unwrap()
                .dependents
                .contains(&1)
        );
        engine.remove_dependent(1, 1);
        assert!(
            !engine
                .regions
                .get(&1)
                .unwrap()
                .read()
                .unwrap()
                .dependents
                .contains(&1)
        );
    }

    #[test]
    fn test_rfengine_wal() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 128 * 1024_usize;
        let dir_path = tmp_dir.path();
        let mut engine = RfEngine::open(dir_path, wal_size).unwrap();
        let mut wb = WriteBatch::new();
        for region_id in 1..=10_u64 {
            let (key, val) = make_state_kv(2, 1);
            wb.set_state(region_id, key.chunk(), val.chunk());
        }
        engine.write(wb).unwrap();
        for idx in 1..=1050_u64 {
            let mut wb = WriteBatch::new();
            for region_id in 1..=10_u64 {
                wb.append_raft_log(region_id, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(1, idx);
                wb.set_state(region_id, key.chunk(), val.chunk());
            }
            engine.write(wb).unwrap();
        }
        assert_eq!(engine.regions.len(), 10);
        engine.stop_worker();
        for _ in 0..2 {
            let mut engine = RfEngine::open(dir_path, wal_size).unwrap();
            assert_eq!(engine.regions.len(), 10);
            engine.stop_worker();
        }
        let epoches = read_epoches(dir_path).unwrap();
        let mut wal_epoches = Vec::new();
        for ep in &epoches {
            if ep.has_wal_file {
                wal_epoches.push(ep);
            }
        }
        assert!(!wal_epoches.is_empty());
        for ep in &wal_epoches {
            let filename = wal_file_name(dir_path, ep.id);
            let mut it = WALIterator::new(dir_path.to_owned(), ep.id);
            let fd = fs::File::open(filename.clone()).unwrap();
            let mut buf_reader = BufReader::new(fd);
            let wal_header = it.check_wal_header(&mut buf_reader).unwrap();
            let mut offsets = vec![it.offset];
            loop {
                match it.read_batch(&mut buf_reader, &wal_header) {
                    Err(err) => {
                        if let Error::EOF = err {
                            break;
                        }
                        panic!("{:?}", err);
                    }
                    Ok(_data) => offsets.push(it.offset),
                }
            }
            offsets.pop().unwrap();
            for (idx, offset) in offsets.iter().enumerate() {
                if idx == 0 || idx == offsets.len() / 2 || idx == offsets.len() - 1 {
                    for pos in &vec![0, 4, 8, 12] {
                        let fd = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .open(filename.as_path())
                            .unwrap();
                        let mut buf = [0u8; 4096];
                        fd.read_exact_at(&mut buf, *offset).unwrap();
                        buf[*pos] += 1;
                        fd.write_all_at(buf.as_ref(), *offset).unwrap();
                        fd.sync_data().unwrap();
                        assert!(RfEngine::open(dir_path, wal_size).is_err());
                        buf[*pos] -= 1;
                        fd.write_all_at(buf.as_ref(), *offset).unwrap();
                        fd.sync_data().unwrap();
                    }
                }
            }
        }
    }
}
