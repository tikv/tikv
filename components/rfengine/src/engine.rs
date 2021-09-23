// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::{Buf, Bytes};
use error_code::{ErrorCode, ErrorCodeExt};
use libc::NOTE_EXITSTATUS;
use protobuf::ProtobufEnum;
use raft_proto::eraftpb;
use slog_global::info;
use std::sync::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs, mem,
    num::ParseIntError,
    path::Path,
    path::PathBuf,
    sync::mpsc::SyncSender,
    thread::{self, JoinHandle},
    time::Instant,
};
use thiserror::Error as ThisError;

use crate::*;

pub struct RFEngine {
    pub(crate) dir: PathBuf,
    pub(crate) writer: Mutex<WALWriter>,

    pub(crate) entries_map: RwLock<HashMap<u64, RegionRaftLogs>>,
    pub(crate) states: RwLock<BTreeMap<StateKey, Bytes>>,

    pub(crate) task_sender: SyncSender<Task>,
    pub(crate) worker_handle: Option<JoinHandle<()>>,
}

pub struct WriteBatch {
    truncates: Vec<TruncateOp>,
    raft_log_ops: Vec<RaftLogOp>,
    state_ops: Vec<StateOp>,
    size: usize,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            truncates: Default::default(),
            raft_log_ops: Default::default(),
            state_ops: Default::default(),
            size: BATCH_HEADER_SIZE,
        }
    }

    pub fn append_raft_log(&mut self, region_id: u64, entry: &eraftpb::Entry) {
        let op = RaftLogOp::new(region_id, entry);
        self.size += raft_log_size(&op);
        self.raft_log_ops.push(op);
    }

    pub fn truncate_raft_log(&mut self, region_id: u64, index: u64) {
        let op = TruncateOp { region_id, index };
        self.truncates.push(op);
    }

    pub fn set_state(&mut self, region_id: u64, key: &[u8], val: &[u8]) {
        let op = StateOp {
            region_id,
            key: Bytes::copy_from_slice(key),
            val: Bytes::copy_from_slice(val),
        };
        self.size += state_size(key, val);
        self.state_ops.push(op);
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn num_entries(&self) -> usize {
        self.truncates.len() + self.raft_log_ops.len() + self.state_ops.len()
    }

    pub fn reset(&mut self) {
        self.truncates.truncate(0);
        self.raft_log_ops.truncate(0);
        self.state_ops.truncate(0);
        self.size = BATCH_HEADER_SIZE;
    }

    pub fn is_empty(&self) -> bool {
        return self.size == BATCH_HEADER_SIZE;
    }
}

pub(crate) fn get_region_raft_logs(
    entries_map: &mut HashMap<u64, RegionRaftLogs>,
    region_id: u64,
) -> &mut RegionRaftLogs {
    if !entries_map.contains_key(&region_id) {
        let region_logs = RegionRaftLogs::default();
        entries_map.insert(region_id, region_logs);
    }
    entries_map.get_mut(&region_id).unwrap()
}

struct TruncateOp {
    region_id: u64,
    index: u64,
}

#[derive(Clone)]
pub(crate) struct RaftLogOp {
    pub(crate) region_id: u64,
    pub(crate) index: u64,
    pub(crate) term: u32,
    pub(crate) e_type: i32,
    pub(crate) data: Bytes,
}

impl RaftLogOp {
    fn new(region_id: u64, entry: &eraftpb::Entry) -> Self {
        Self {
            region_id,
            index: entry.index,
            term: entry.term as u32,
            e_type: entry.entry_type.value(),
            data: entry.data.clone(),
        }
    }
}

struct StateOp {
    region_id: u64,
    key: Bytes,
    val: Bytes,
}

impl RFEngine {
    pub fn open(dir: &Path, wal_size: usize) -> Result<Self> {
        maybe_create_dir(&dir)?;
        let mut epoches = read_epoches(dir)?;
        let mut epoch_id = 1;
        if epoches.len() > 0 {
            epoch_id = epoches.last().unwrap().id;
        }

        let (tx, rx) = std::sync::mpsc::sync_channel(1024);
        let writer = WALWriter::new(dir.clone(), epoch_id, wal_size)?;
        let mut en = Self {
            dir: dir.to_owned(),
            entries_map: RwLock::new(HashMap::new()),
            states: RwLock::new(BTreeMap::new()),
            writer: Mutex::new(writer),
            task_sender: tx,
            worker_handle: None,
        };
        let mut offset = 0;
        for ep in &epoches {
            offset = en.load_epoch(ep)?;
        }
        en.writer.lock().unwrap().seek(offset);
        if epoches.len() > 0 {
            epoches.pop();
        }
        let mut worker = Worker::new(dir.to_owned(), epoches, rx);
        let join_handle = thread::spawn(move || worker.run());
        en.worker_handle = Some(join_handle);
        Ok(en)
    }

    pub fn write(&self, wb: &WriteBatch) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        if wb.size as u64 + writer.offset() > writer.wal_size as u64 {
            let epoch_id = writer.epoch_id;
            writer.rotate()?;
            self.task_sender
                .send(Task::Rotate {
                    epoch_id,
                    states: self.states.read().unwrap().clone(),
                })
                .unwrap();
        }

        for op in &wb.raft_log_ops {
            writer.append_raft_log(op.clone());
            self.init_region_raft_logs(op.region_id);
            let mut entries_map = self.entries_map.write().unwrap();
            let region_logs = entries_map.get_mut(&op.region_id).unwrap();
            region_logs.append(op.clone());
        }
        for op in &wb.state_ops {
            writer.append_state(op.region_id, op.key.chunk(), op.val.chunk());
            let state_key = StateKey::new(op.region_id, op.key.chunk());
            if op.val.len() > 0 {
                self.states
                    .write()
                    .unwrap()
                    .insert(state_key, op.val.clone());
            } else {
                self.states.write().unwrap().remove(&state_key);
            }
        }
        for op in &wb.truncates {
            let region_id = op.region_id;
            let index = op.index;
            writer.append_truncate(region_id, index);
            self.init_region_raft_logs(op.region_id);
            let mut entries_map = self.entries_map.write().unwrap();
            let region_logs = entries_map.get_mut(&op.region_id).unwrap();
            let empty = region_logs.truncate(index);
            if empty {
                entries_map.remove(&op.region_id);
            }
            self.task_sender
                .send(Task::Truncate { region_id, index })
                .unwrap();
        }
        writer.flush()
    }

    pub fn is_empty(&self) -> bool {
        self.entries_map.read().unwrap().len() + self.states.read().unwrap().len() == 0
    }

    pub fn get_state(&self, region_id: u64, key: &[u8]) -> Option<Bytes> {
        self.states
            .read()
            .unwrap()
            .get(&StateKey::new(region_id, key))
            .map(|x| x.clone())
    }

    pub fn init_region_raft_logs(&self, region_id: u64) {
        let mut entries_map = self.entries_map.write().unwrap();
        if entries_map.get(&region_id).is_some() {
            return;
        }
        let region_logs = RegionRaftLogs::default();
        entries_map.insert(region_id, region_logs);
    }

    pub fn iterate_region_states<F>(&self, region_id: u64, desc: bool, f: F)
    where
        F: Fn(&[u8], &[u8]),
    {
        let start_key = StateKey::new(region_id, &[]);
        let end_key = StateKey::new(region_id + 1, &[]);
        let states = self.states.read().unwrap();
        let range = states.range(start_key..end_key);
        if desc {
            for (k, v) in range.rev() {
                f(k.key.chunk(), v.chunk())
            }
        } else {
            for (k, v) in range {
                f(k.key.chunk(), v.chunk())
            }
        }
    }

    pub fn iterate_all_states<F>(&self, desc: bool, f: F)
    where
        F: Fn(u64, &[u8], &[u8]),
    {
        let states = self.states.read().unwrap();
        let iter = states.iter();
        if desc {
            for (k, v) in iter.rev() {
                f(k.region_id, k.key.chunk(), v.chunk())
            }
        } else {
            for (k, v) in iter {
                f(k.region_id, k.key.chunk(), v.chunk())
            }
        }
    }

    pub fn stop_worker(&mut self) {
        self.task_sender.send(Task::Close);
        let handle = mem::replace(&mut self.worker_handle, None);
        if let Some(h) = handle {
            h.join();
        }
    }
}

pub(crate) fn maybe_create_dir(dir: &Path) -> Result<()> {
    let recycle_path = dir.join(RECYCLE_DIR);
    if !recycle_path.exists() {
        fs::create_dir_all(recycle_path)?;
    } else if !recycle_path.is_dir() {
        return Err(Error::Open(String::from("recycle path is not dir")));
    }
    Ok(())
}

#[derive(Default, Clone)]
pub(crate) struct RegionRaftLogs {
    pub(crate) range: RaftLogRange,
    pub(crate) raft_logs: VecDeque<RaftLogOp>,
}

impl RegionRaftLogs {
    pub(crate) fn prepare_append(&mut self, index: u64) {
        if self.range.start_index == 0 {
            // initialize index
            self.range.start_index = index;
            self.range.end_index = index;
            return;
        }
        if index < self.range.start_index || self.range.end_index < index {
            // Out of bound index truncate all entries.
            self.range.start_index = index;
            self.range.end_index = index;
            self.raft_logs.truncate(0);
            return;
        }
        let local_idx = index - self.range.start_index;
        self.raft_logs.truncate(local_idx as usize);
        self.range.end_index = index;
    }

    pub(crate) fn append(&mut self, op: RaftLogOp) {
        self.prepare_append(op.index);
        self.raft_logs.push_back(op);
        self.range.end_index += 1;
    }

    pub(crate) fn truncate(&mut self, index: u64) -> bool {
        if index <= self.range.start_index {
            return false;
        }
        if index > self.range.end_index {
            self.range.start_index = index;
            self.range.end_index = index;
            self.raft_logs.truncate(0);
            return true;
        }
        let local_idx = index - self.range.start_index;
        self.range.start_index = index;
        let new_len = self.raft_logs.len() - local_idx as usize;
        self.raft_logs.rotate_left(local_idx as usize);
        self.raft_logs.truncate(new_len);
        self.raft_logs.len() == 0
    }

    pub(crate) fn get(&self, index: u64) -> Option<eraftpb::Entry> {
        if index < self.range.start_index || index >= self.range.end_index {
            return None;
        }
        let local_idx = index - self.range.start_index;
        let op = self.raft_logs.get(local_idx as usize).unwrap();
        let mut entry = eraftpb::Entry::new();
        entry.set_entry_type(eraftpb::EntryType::from_i32(op.e_type).unwrap());
        entry.set_term(op.term as u64);
        entry.set_index(index);
        entry.set_data(op.data.clone());
        Some(entry)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("IO error: {0:?}")]
    Io(std::io::Error),
    #[error("EOF")]
    EOF,
    #[error("parse error")]
    ParseError,
    #[error("checksum not match")]
    Checksum,
    #[error("Open error: {0}")]
    Open(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Error::EOF;
        }
        Error::Io(e)
    }
}

impl From<ParseIntError> for Error {
    fn from(_: ParseIntError) -> Self {
        Error::ParseError
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use slog::o;

    use super::*;

    #[test]
    fn test_rfengine() {
        init_logger();
        let tmp_dir = tempfile::tempdir().unwrap();
        let wal_size = 128 * 1024 as usize;
        let mut engine = RFEngine::open(tmp_dir.path(), wal_size).unwrap();

        for idx in 1..=1050_u64 {
            let mut wb = WriteBatch::new();
            for region_id in 1..=10_u64 {
                if region_id == 1 && (idx > 100 && idx < 900) {
                    continue;
                }
                wb.append_raft_log(region_id, &make_log_data(idx, 128));
                let (key, val) = make_state_kv(region_id, idx);
                wb.set_state(region_id, key.chunk(), val.chunk());
                if idx % 100 == 0 && region_id != 1 {
                    wb.truncate_raft_log(region_id, idx - 100);
                }
            }
            engine.write(&wb).unwrap();
        }
        let old_states = engine.states.clone();
        let old_entries_map = engine.entries_map.clone();
        assert!(old_states.len() > 0);
        assert_eq!(old_entries_map.len(), 10);
        engine.stop_worker();
        for _ in 0..1 {
            let engine = RFEngine::open(tmp_dir.path(), wal_size).unwrap();
            assert_eq!(old_states.len(), engine.states.len());
            engine.iterate_all_states(false, |region_id, key, _| {
                let state_key = StateKey::new(region_id, key);
                assert!(old_states.contains_key(&state_key));
            });
            assert_eq!(engine.entries_map.len(), 10);
            for (k, entries) in &engine.entries_map {
                let old = old_entries_map.get(k).unwrap();
                assert_eq!(old.raft_logs.len(), entries.raft_logs.len());
                assert_eq!(old.range, entries.range);
                for op in &old.raft_logs {
                    assert_eq!(entries.get(op.index).unwrap().data.chunk(), op.data.chunk());
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

    fn make_state_kv(region_id: u64, idx: u64) -> (BytesMut, BytesMut) {
        let mut key = BytesMut::new();
        key.put_u64_le(region_id);
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
}
