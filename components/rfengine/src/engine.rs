// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes};
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use protobuf::ProtobufEnum;
use raft_proto::eraftpb;
use slog_global::info;
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    num::ParseIntError,
    path::Path,
    path::PathBuf,
    sync::mpsc::SyncSender,
    thread::{self, JoinHandle},
};
use thiserror::Error as ThisError;

use crate::metrics::*;
use crate::*;

#[derive(Clone)]
pub struct RFEngine {
    pub dir: PathBuf,
    pub(crate) writer: Arc<Mutex<WALWriter>>,

    pub(crate) regions: Arc<dashmap::DashMap<u64, RegionData>>,

    pub(crate) task_sender: SyncSender<Task>,
    pub(crate) worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Default)]
pub struct WriteBatch {
    pub(crate) regions: HashMap<u64, RegionBatch>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            regions: Default::default(),
        }
    }

    pub(crate) fn get_region(&mut self, region_id: u64) -> &mut RegionBatch {
        match self.regions.entry(region_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RegionBatch::new(region_id)),
        }
    }

    pub fn append_raft_log(&mut self, region_id: u64, entry: &eraftpb::Entry) {
        let op = RaftLogOp::new(entry);
        self.get_region(region_id).append_raft_log(op);
    }

    pub fn truncate_raft_log(&mut self, region_id: u64, index: u64) {
        self.get_region(region_id).truncate(index);
    }

    pub fn set_state(&mut self, region_id: u64, key: &[u8], val: &[u8]) {
        self.get_region(region_id).set_state(key, val);
    }

    pub fn reset(&mut self) {
        self.regions.clear()
    }

    pub fn is_empty(&self) -> bool {
        return self.regions.is_empty();
    }

    pub(crate) fn merge_region(&mut self, region_batch: RegionBatch) {
        self.get_region(region_batch.region_id).merge(region_batch);
    }
}

pub(crate) struct RegionBatch {
    pub(crate) region_id: u64,
    pub(crate) truncated_idx: u64,
    pub(crate) states: BTreeMap<Bytes, Bytes>,
    pub(crate) raft_logs: VecDeque<RaftLogOp>,
}

impl RegionBatch {
    pub(crate) fn new(region_id: u64) -> Self {
        Self {
            region_id,
            truncated_idx: 0,
            states: Default::default(),
            raft_logs: Default::default(),
        }
    }

    pub(crate) fn truncate(&mut self, idx: u64) {
        while let Some(front) = self.raft_logs.front() {
            if front.index >= idx {
                break;
            }
            self.raft_logs.pop_front();
        }
    }

    pub fn set_state(&mut self, key: &[u8], val: &[u8]) {
        self.states
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(val));
    }

    pub fn append_raft_log(&mut self, op: RaftLogOp) {
        while let Some(back) = self.raft_logs.back() {
            if back.index + 1 == op.index {
                break;
            }
            self.raft_logs.pop_back();
        }
        self.raft_logs.push_back(op);
    }

    pub fn merge(&mut self, other: RegionBatch) {
        for (k, v) in other.states {
            self.states.insert(k, v);
        }
        for op in other.raft_logs {
            self.append_raft_log(op);
        }
        if self.truncated_idx < other.truncated_idx {
            self.truncated_idx = other.truncated_idx;
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        // region_id, start_index, end_index, truncated_idx, states_len.
        let mut len = 8 + 8 + 8 + 8 + 4;
        for (key, val) in &self.states {
            len += 2 + key.len() + 4 + val.len();
        }
        len += self.raft_logs.len() * 4;
        for op in &self.raft_logs {
            len += op.encoded_len();
        }
        len
    }

    pub(crate) fn encode_to(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.region_id);
        let first = self.raft_logs.front().map_or(0, |x| x.index);
        let end = self.raft_logs.back().map_or(0, |x| x.index + 1);
        buf.put_u64_le(first);
        buf.put_u64_le(end);
        buf.put_u64_le(self.truncated_idx);
        buf.put_u32_le(self.states.len() as u32);
        for (key, val) in &self.states {
            buf.put_u16_le(key.len() as u16);
            buf.put_slice(key.chunk());
            buf.put_u32_le(val.len() as u32);
            buf.put_slice(val.chunk());
        }
        let mut end_off = 0;
        for op in &self.raft_logs {
            end_off += op.encoded_len() as u32;
            buf.put_u32_le(end_off);
        }
        for op in &self.raft_logs {
            op.encode_to(buf);
        }
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Self {
        let mut batch = RegionBatch::new(0);
        batch.region_id = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        let first = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        let end = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        batch.truncated_idx = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        let states_len = LittleEndian::read_u32(buf);
        buf = &buf[4..];
        for _ in 0..states_len {
            let key_len = LittleEndian::read_u16(buf) as usize;
            buf = &buf[2..];
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf = &buf[key_len..];
            let val_len = LittleEndian::read_u32(buf) as usize;
            buf = &buf[4..];
            let val = Bytes::copy_from_slice(&buf[..val_len]);
            buf = &buf[val_len..];
            batch.states.insert(key, val);
        }
        let num_logs = (end - first) as usize;
        let log_index_len = num_logs * 4;
        let mut log_index_buf = &buf[..log_index_len];
        buf = &buf[log_index_len..];
        let mut start_index = 0;
        for _ in 0..num_logs {
            let end_index = LittleEndian::read_u32(log_index_buf) as usize;
            log_index_buf = &log_index_buf[4..];
            let log_op = RaftLogOp::decode(&buf[start_index..end_index]);
            start_index = end_index;
            batch.raft_logs.push_back(log_op);
        }
        batch
    }
}

#[derive(Default, Clone)]
pub(crate) struct RaftLogOp {
    pub(crate) index: u64,
    pub(crate) term: u32,
    pub(crate) e_type: u8,
    pub(crate) context: u8,
    pub(crate) data: Bytes,
}

impl RaftLogOp {
    fn new(entry: &eraftpb::Entry) -> Self {
        let context = *entry.context.last().unwrap_or(&0);
        Self {
            index: entry.index,
            term: entry.term as u32,
            e_type: entry.entry_type.value() as u8,
            context,
            data: entry.data.clone(),
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        8 + 4 + 1 + 1 + self.data.len()
    }

    pub(crate) fn encode_to(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.index);
        buf.put_u32_le(self.term);
        buf.put_u8(self.e_type);
        buf.put_u8(self.context);
        buf.put_slice(self.data.chunk());
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Self {
        let mut op = RaftLogOp::default();
        op.index = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        op.term = LittleEndian::read_u32(buf);
        buf = &buf[4..];
        op.e_type = buf[0];
        buf = &buf[1..];
        op.context = buf[0];
        buf = &buf[1..];
        op.data = Bytes::copy_from_slice(buf);
        op
    }
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
            regions: Arc::new(DashMap::default()),
            writer: Arc::new(Mutex::new(writer)),
            task_sender: tx,
            worker_handle: Arc::new(Mutex::new(None)),
        };
        let mut offset = 0;
        let mut worker_states = HashMap::new();
        for ep in &epoches {
            offset = en.load_epoch(ep)?;
            if ep.has_state_file {
                for e in en.regions.iter() {
                    let region = e.value();
                    let states = region.states.read().unwrap();
                    worker_states.insert(region.region_id, states.clone());
                }
            }
        }
        en.writer.lock().unwrap().seek(offset);
        if epoches.len() > 0 {
            epoches.pop();
        }
        {
            let mut worker = Worker::new(
                dir.to_owned(),
                epoches,
                rx,
                en.regions.clone(),
                worker_states,
            );
            let join_handle = thread::spawn(move || worker.run());
            let mut handle_ref = en.worker_handle.lock().unwrap();
            *handle_ref = Some(join_handle);
        }
        Ok(en)
    }

    pub fn write(&self, wb: WriteBatch) -> Result<usize> {
        self.apply(&wb);
        self.persist(wb)
    }

    pub fn persist(&self, wb: WriteBatch) -> Result<usize> {
        let timer = Instant::now();
        let mut writer = self.writer.lock().unwrap();
        let epoch_id = writer.epoch_id;
        for (_, data) in &wb.regions {
            writer.append_region_data(data);
        }
        let size = writer.buf_size();
        let rotated = writer.flush()?;
        drop(writer);
        if rotated {
            self.task_sender.send(Task::Rotate { epoch_id }).unwrap();
        }
        ENGINE_PERSIST_DURATION_HISTOGRAM.observe(elapsed_secs(timer));
        Ok(size)
    }

    // apply applies the write batch to the memory before persist, so it can be retrieved
    // before persist.
    // It is used for async I/O that persist the write batch in another thread.
    pub fn apply(&self, wb: &WriteBatch) {
        let timer = Instant::now();
        for (region_id, batch_data) in &wb.regions {
            let region_id = *region_id;
            let region_data = self.get_or_init_region_data(region_id);
            region_data.apply(batch_data);
            if region_data.need_truncate() {
                self.task_sender.send(Task::Truncate { region_id }).unwrap();
            }
        }
        ENGINE_APPLY_DURATION_HISTOGRAM.observe(elapsed_secs(timer));
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    pub fn get_term(&self, region_id: u64, index: u64) -> Option<u64> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let region_data = map_ref.unwrap();
        region_data.term(index)
    }

    pub fn get_range(&self, region_id: u64) -> Option<(u64, u64)> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let region_data = map_ref.unwrap();
        let first = region_data.raft_logs.load_first();
        let end = region_data.raft_logs.load_end();
        if first == 0 {
            return None;
        }
        Some((first, end))
    }

    pub fn get_state(&self, region_id: u64, key: &[u8]) -> Option<Bytes> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let region_data = map_ref.unwrap();
        match region_data.get_state(key) {
            Some(val) => {
                if val.len() > 0 {
                    Some(val.clone())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn get_last_state_with_prefix(&self, region_id: u64, prefix: &[u8]) -> Option<Bytes> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let region_data = map_ref.unwrap();
        let mut end_prefix = Vec::from(prefix);
        end_prefix[prefix.len() - 1] += 1;
        let r = Bytes::copy_from_slice(prefix)..Bytes::copy_from_slice(&end_prefix);
        let states = region_data.states.read().unwrap();
        let range = states.range(r);
        for (k, v) in range.rev() {
            if k.chunk() == end_prefix.as_slice() {
                continue;
            }
            return Some(v.clone());
        }
        None
    }

    pub(crate) fn get_or_init_region_data(&self, region_id: u64) -> Ref<u64, RegionData> {
        match self.regions.get(&region_id) {
            Some(region_data) => {
                return region_data;
            }
            None => {}
        }
        let region_data = RegionData::new(region_id);
        self.regions.insert(region_id, region_data);
        self.regions.get(&region_id).unwrap()
    }

    pub fn iterate_region_states<F>(&self, region_id: u64, desc: bool, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return Ok(());
        }
        let region_data = map_ref.unwrap();
        let states = region_data.states.read().unwrap();
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

    /// f returns false to stop iterating.
    pub fn iterate_all_states<F>(&self, desc: bool, mut f: F)
    where
        F: FnMut(u64, &[u8], &[u8]) -> bool,
    {
        for region_data in self.regions.iter() {
            let states = region_data.states.read().unwrap();
            let iter = states.iter();
            if desc {
                for (k, v) in iter.rev() {
                    if !f(region_data.region_id, k.chunk(), v.chunk()) {
                        break;
                    }
                }
            } else {
                for (k, v) in iter {
                    if !f(region_data.region_id, k.chunk(), v.chunk()) {
                        break;
                    }
                }
            }
        }
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
    /// So we can call add_dependent after split to protect the raft log.
    /// After the new region is initially flushed or re-ingested or destroyed, call
    /// remove_dependent to resume truncate the raft log.
    pub fn add_dependent(&self, region_id: u64, dependent_id: u64) {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return;
        }
        let region_data = map_ref.unwrap();
        let mut deps = region_data.dependents.write().unwrap();
        deps.insert(dependent_id);
        let dependents_len = deps.len();
        drop(deps);
        info!(
            "region {} add dependent {}, dependents_len {}",
            region_id, dependent_id, dependents_len
        );
    }

    pub fn remove_dependent(&self, region_id: u64, dependent_id: u64) {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return;
        }
        let region_data = map_ref.unwrap();
        let mut deps = region_data.dependents.write().unwrap();
        deps.remove(&dependent_id);
        let dependents_len = deps.len();
        info!(
            "region {} remove dependent {}, dependents_len {}",
            region_id, dependent_id, dependents_len
        );
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

pub(crate) struct RegionData {
    pub(crate) region_id: u64,
    pub(crate) truncated_idx: AtomicU64,
    pub(crate) raft_logs: RaftLogs,
    pub(crate) states: RwLock<BTreeMap<Bytes, Bytes>>,
    pub(crate) dependents: RwLock<HashSet<u64>>,
}

impl Clone for RegionData {
    fn clone(&self) -> Self {
        let raft_logs = self.raft_logs.clone();
        let states = self.states.read().unwrap().clone();
        let dependents = self.dependents.read().unwrap().clone();
        Self {
            region_id: self.region_id,
            truncated_idx: AtomicU64::new(self.load_truncate_idx()),
            raft_logs,
            states: RwLock::new(states),
            dependents: RwLock::new(dependents),
        }
    }
}

impl RegionData {
    pub(crate) fn new(region_id: u64) -> Self {
        Self {
            region_id,
            truncated_idx: AtomicU64::new(0),
            raft_logs: RaftLogs::new(0, 0),
            states: RwLock::new(BTreeMap::new()),
            dependents: RwLock::new(HashSet::new()),
        }
    }

    pub(crate) fn append(&self, op: RaftLogOp) {
        self.raft_logs.append(op);
    }

    pub(crate) fn load_truncate_idx(&self) -> u64 {
        self.truncated_idx.load(Ordering::Acquire)
    }

    pub(crate) fn store_truncate_idx(&self, idx: u64) {
        self.truncated_idx.store(idx, Ordering::Release)
    }

    pub(crate) fn truncate_self(&self) {
        self.raft_logs.truncate(self.load_truncate_idx());
    }

    pub(crate) fn get(&self, index: u64) -> Option<eraftpb::Entry> {
        self.raft_logs.get(index)
    }

    pub(crate) fn term(&self, index: u64) -> Option<u64> {
        if let Some(op) = self.raft_logs.get(index) {
            return Some(op.term as u64);
        }
        None
    }

    pub(crate) fn get_state(&self, key: &[u8]) -> Option<Bytes> {
        let states = self.states.read().unwrap();
        states.get(key).cloned()
    }

    pub(crate) fn apply(&self, batch: &RegionBatch) {
        let self_truncated_idx = self.load_truncate_idx();
        if self_truncated_idx < batch.truncated_idx {
            self.store_truncate_idx(batch.truncated_idx);
        }
        let mut states = self.states.write().unwrap();
        for (key, val) in &batch.states {
            if val.len() == 0 {
                states.remove(key.chunk());
            } else {
                states.insert(key.clone(), val.clone());
            }
        }
        drop(states);
        for op in &batch.raft_logs {
            self.append(op.clone())
        }
    }

    pub(crate) fn need_truncate(&self) -> bool {
        let dependent_empty = self.dependents.read().unwrap().is_empty();
        let first = self.raft_logs.load_first();
        dependent_empty && first > 0 && self.load_truncate_idx() > first
    }
}

pub(crate) struct RaftLogs {
    first: AtomicU64,
    end: AtomicU64,
    m: dashmap::DashMap<u64, RaftLogOp>,
}

impl Clone for RaftLogs {
    fn clone(&self) -> Self {
        Self {
            first: AtomicU64::new(self.load_first()),
            end: AtomicU64::new(self.load_end()),
            m: self.m.clone(),
        }
    }
}

impl RaftLogs {
    pub(crate) fn new(first: u64, end: u64) -> RaftLogs {
        Self {
            first: AtomicU64::new(first),
            end: AtomicU64::new(end),
            m: dashmap::DashMap::new(),
        }
    }

    pub(crate) fn append(&self, op: RaftLogOp) {
        let op_idx = op.index;
        self.prepare_append(op_idx);
        self.m.insert(op_idx, op);
        self.store_end(op_idx + 1);
    }

    pub(crate) fn load_first(&self) -> u64 {
        self.first.load(Ordering::Acquire)
    }

    pub(crate) fn store_first(&self, idx: u64) {
        self.first.store(idx, Ordering::Release);
    }

    pub(crate) fn load_end(&self) -> u64 {
        self.end.load(Ordering::Acquire)
    }

    pub(crate) fn store_end(&self, idx: u64) {
        self.end.store(idx, Ordering::Release);
    }

    pub(crate) fn prepare_append(&self, index: u64) {
        let first = self.load_first();
        let end = self.load_end();
        if first == 0 || index < first || end < index {
            self.store_first(index);
        }
    }

    pub(crate) fn truncate(&self, index: u64) {
        let first = self.load_first();
        let end = self.load_end();
        if first == end || index <= first {
            return;
        }
        let truncate_end = std::cmp::min(end, index);
        self.store_first(truncate_end);
        for x in &self.m {
            let idx = x.index;
            if idx < truncate_end {
                self.m.remove(&idx);
            }
        }
    }

    pub(crate) fn get(&self, index: u64) -> Option<eraftpb::Entry> {
        let first = self.load_first();
        let end = self.load_end();
        if index < first || index >= end {
            return None;
        }
        let op = self.m.get(&index).unwrap();
        let mut entry = eraftpb::Entry::new();
        entry.set_entry_type(eraftpb::EntryType::from_i32(op.e_type as i32).unwrap());
        entry.set_term(op.term as u64);
        entry.set_index(index);
        if op.context > 0 {
            entry.set_context(vec![op.context].into());
        }
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
        let mut old_entries_map = HashMap::new();
        for region_data in engine.regions.iter() {
            assert_eq!(*region_data.key(), region_data.region_id);
            old_entries_map.insert(region_data.region_id, region_data.clone());
        }
        assert_eq!(old_entries_map.len(), 10);
        engine.stop_worker();
        for _ in 0..2 {
            let engine = RFEngine::open(tmp_dir.path(), wal_size).unwrap();
            engine.iterate_all_states(false, |region_id, key, _| {
                let old_region_data = old_entries_map.get(&region_id).unwrap();
                assert!(old_region_data.get_state(key).is_some());
                true
            });
            assert_eq!(engine.regions.len(), 10);
            for new_data in engine.regions.iter() {
                let old_data = old_entries_map.get(new_data.key()).unwrap();
                assert_eq!(old_data.load_truncate_idx(), new_data.load_truncate_idx());
                assert_eq!(
                    old_data.raft_logs.load_first(),
                    new_data.raft_logs.load_first()
                );
                assert_eq!(old_data.raft_logs.load_end(), new_data.raft_logs.load_end());
                assert_eq!(old_data.raft_logs.len(), new_data.raft_logs.len());
                for i in new_data.raft_logs.load_first()..new_data.raft_logs.load_end() {
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
}
