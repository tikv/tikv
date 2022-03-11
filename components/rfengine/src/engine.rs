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
use std::sync::{Arc, Mutex, RwLock};
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

use crate::*;

#[derive(Clone)]
pub struct RFEngine {
    pub dir: PathBuf,
    pub(crate) writer: Arc<Mutex<WALWriter>>,

    pub(crate) regions: Arc<dashmap::DashMap<u64, RwLock<RegionData>>>,

    pub(crate) task_sender: SyncSender<Task>,
    pub(crate) worker_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Default)]
pub struct WriteBatch {
    pub(crate) regions: HashMap<u64, RegionData>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            regions: Default::default(),
        }
    }

    pub(crate) fn get_region(&mut self, region_id: u64) -> &mut RegionData {
        match self.regions.entry(region_id) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RegionData::new(region_id)),
        }
    }

    pub fn append_raft_log(&mut self, region_id: u64, entry: &eraftpb::Entry) {
        let region = self.get_region(region_id);
        let op = RaftLogOp::new(entry);
        region.append(op);
    }

    pub fn truncate_raft_log(&mut self, region_id: u64, index: u64) {
        let region = self.get_region(region_id);
        region.truncated_idx = index;
    }

    pub fn set_state(&mut self, region_id: u64, key: &[u8], val: &[u8]) {
        let region = self.get_region(region_id);
        region
            .states
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(val));
    }

    pub fn reset(&mut self) {
        self.regions.clear()
    }

    pub fn is_empty(&self) -> bool {
        return self.regions.is_empty();
    }

    pub(crate) fn merge_region(&mut self, region_data: &RegionData) {
        self.get_region(region_data.region_id).merge(region_data);
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
                    let region = e.value().read().unwrap();
                    worker_states.insert(region.region_id, region.states.clone());
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

    pub fn write(&self, wb: WriteBatch) -> Result<()> {
        self.apply(&wb);
        self.persist(wb)
    }

    pub fn persist(&self, wb: WriteBatch) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let epoch_id = writer.epoch_id;
        for (_, data) in &wb.regions {
            writer.append_region_data(data);
        }
        let rotated = writer.flush()?;
        drop(writer);
        if rotated {
            self.task_sender.send(Task::Rotate { epoch_id }).unwrap();
        }
        Ok(())
    }

    // apply applies the write batch to the memory before persist, so it can be retrieved
    // before persist.
    // It is used for async I/O that persist the write batch in another thread.
    pub fn apply(&self, wb: &WriteBatch) {
        for (region_id, batch_data) in &wb.regions {
            let region_id = *region_id;
            let data_ref = self.get_or_init_region_data(region_id);
            let mut region_data = data_ref.write().unwrap();
            region_data.merge(batch_data);
            region_data.remove_empty_states();
            if region_data.need_truncate() {
                self.task_sender.send(Task::Truncate { region_id }).unwrap();
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    pub fn get_term(&self, region_id: u64, index: u64) -> Option<u64> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let map_ref = map_ref.unwrap();
        let region_data = map_ref.read().unwrap();
        region_data.term(index)
    }

    pub fn get_range(&self, region_id: u64) -> Option<(u64, u64)> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let map_ref = map_ref.unwrap();
        let region_data = map_ref.read().unwrap();
        if region_data.range.start_index == 0 {
            return None;
        }
        Some((region_data.range.start_index, region_data.range.end_index))
    }

    pub fn get_state(&self, region_id: u64, key: &[u8]) -> Option<Bytes> {
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return None;
        }
        let map_ref = map_ref.unwrap();
        let region_data = map_ref.read().unwrap();
        match region_data.states.get(key) {
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
        let map_ref = map_ref.unwrap();
        let region_data = map_ref.read().unwrap();
        let mut end_prefix = Vec::from(prefix);
        end_prefix[prefix.len() - 1] += 1;
        let r = Bytes::copy_from_slice(prefix)..Bytes::copy_from_slice(&end_prefix);
        let range = region_data.states.range(r);
        for (k, v) in range.rev() {
            if k.chunk() == end_prefix.as_slice() {
                continue;
            }
            return Some(v.clone());
        }
        None
    }

    pub(crate) fn get_or_init_region_data(&self, region_id: u64) -> Ref<u64, RwLock<RegionData>> {
        match self.regions.get(&region_id) {
            Some(region_data) => {
                return region_data;
            }
            None => {}
        }
        let region_data = RwLock::new(RegionData::new(region_id));
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
        let map_ref = map_ref.unwrap();
        let region_data = map_ref.read().unwrap();
        if desc {
            for (k, v) in region_data.states.iter().rev() {
                f(k.chunk(), v.chunk())?;
            }
        } else {
            for (k, v) in region_data.states.iter() {
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
        for item in self.regions.iter() {
            let region_data = item.value().read().unwrap();
            let iter = region_data.states.iter();
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
        let map_ref = map_ref.unwrap();
        let mut region_data = map_ref.write().unwrap();
        region_data.dependents.insert(dependent_id);
        let dependents_len = region_data.dependents.len();
        drop(region_data);
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
        let map_ref = map_ref.unwrap();
        let mut region_data = map_ref.write().unwrap();
        region_data.dependents.remove(&dependent_id);
        let dependents_len = region_data.dependents.len();
        drop(region_data);
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

#[derive(Default, Clone)]
pub(crate) struct RegionData {
    pub(crate) region_id: u64,
    pub(crate) range: RaftLogRange,
    pub(crate) truncated_idx: u64,
    pub(crate) raft_logs: VecDeque<RaftLogOp>,
    pub(crate) states: BTreeMap<Bytes, Bytes>,
    pub(crate) dependents: HashSet<u64>,
}

impl RegionData {
    pub(crate) fn new(region_id: u64) -> Self {
        let mut r = RegionData::default();
        r.region_id = region_id;
        r
    }

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

    pub(crate) fn truncate_self(&mut self) {
        let idx = self.truncated_idx;
        self.truncate(idx);
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
        entry.set_entry_type(eraftpb::EntryType::from_i32(op.e_type as i32).unwrap());
        entry.set_term(op.term as u64);
        entry.set_index(index);
        if op.context > 0 {
            entry.set_context(vec![op.context].into());
        }
        entry.set_data(op.data.clone());
        Some(entry)
    }

    pub(crate) fn term(&self, index: u64) -> Option<u64> {
        if index < self.range.start_index || index >= self.range.end_index {
            return None;
        }
        let local_idx = index - self.range.start_index;
        let op = self.raft_logs.get(local_idx as usize).unwrap();
        return Some(op.term as u64);
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
        buf.put_u64_le(self.range.start_index);
        buf.put_u64_le(self.range.end_index);
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
        let mut data = RegionData::default();
        data.region_id = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        data.range.start_index = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        data.range.end_index = LittleEndian::read_u64(buf);
        buf = &buf[8..];
        data.truncated_idx = LittleEndian::read_u64(buf);
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
            data.states.insert(key, val);
        }
        let num_logs = (data.range.end_index - data.range.start_index) as usize;
        let log_index_len = num_logs * 4;
        let mut log_index_buf = &buf[..log_index_len];
        buf = &buf[log_index_len..];
        let mut start_index = 0;
        for _ in 0..num_logs {
            let end_index = LittleEndian::read_u32(log_index_buf) as usize;
            log_index_buf = &log_index_buf[4..];
            let log_op = RaftLogOp::decode(&buf[start_index..end_index]);
            start_index = end_index;
            data.raft_logs.push_back(log_op);
        }
        data
    }

    pub(crate) fn merge(&mut self, other: &RegionData) {
        if self.truncated_idx < other.truncated_idx {
            self.truncated_idx = other.truncated_idx;
        }
        for (key, val) in &other.states {
            self.states.insert(key.clone(), val.clone());
        }
        if other.raft_logs.len() == 0 {
            return;
        }
        if self.raft_logs.len() == 0 {
            self.range = other.range;
            self.raft_logs = other.raft_logs.clone();
            return;
        }
        self.prepare_append(other.range.start_index);
        for log_op in &other.raft_logs {
            self.raft_logs.push_back(log_op.clone());
        }
        self.range.end_index = other.range.end_index;
    }

    pub(crate) fn remove_empty_states(&mut self) {
        let mut empty_keys = vec![];
        for (key, val) in &self.states {
            if val.len() == 0 {
                empty_keys.push(key.clone());
            }
        }
        for empty_key in empty_keys {
            self.states.remove(&empty_key);
        }
    }

    pub(crate) fn need_truncate(&self) -> bool {
        self.dependents.is_empty()
            && self.range.start_index > 0
            && self.truncated_idx > self.range.start_index
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
        for entry in engine.regions.iter() {
            let region_data = entry.value().read().unwrap();
            assert_eq!(*entry.key(), region_data.region_id);
            old_entries_map.insert(region_data.region_id, region_data.clone());
        }
        assert_eq!(old_entries_map.len(), 10);
        engine.stop_worker();
        for _ in 0..2 {
            let engine = RFEngine::open(tmp_dir.path(), wal_size).unwrap();
            engine.iterate_all_states(false, |region_id, key, _| {
                let old_region_data = old_entries_map.get(&region_id).unwrap();
                assert!(old_region_data.states.contains_key(key));
                true
            });
            assert_eq!(engine.regions.len(), 10);
            for item in engine.regions.iter() {
                let old_data = old_entries_map.get(item.key()).unwrap();
                let new_data = item.value().read().unwrap();
                assert_eq!(old_data.truncated_idx, new_data.truncated_idx);
                assert_eq!(old_data.range, new_data.range);
                assert_eq!(old_data.raft_logs.len(), new_data.raft_logs.len());
                for op in &new_data.raft_logs {
                    assert_eq!(
                        old_data.get(op.index).unwrap().data.chunk(),
                        op.data.chunk()
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
