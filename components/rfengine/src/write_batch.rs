// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap, VecDeque};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes};
use raft_proto::eraftpb;

use crate::log_batch::RaftLogOp;

/// `WriteBatch` contains multiple regions' `RegionBatch`.
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
        self.regions
            .entry(region_id)
            .or_insert_with(|| RegionBatch::new(region_id))
    }

    pub fn append_raft_log(&mut self, region_id: u64, entry: &eraftpb::Entry) {
        let op = RaftLogOp::new(entry);
        self.get_region(region_id).append_raft_log(op);
    }

    pub fn truncate_raft_log(&mut self, region_id: u64, index: u64, term: u64) {
        self.get_region(region_id).truncate(index, term);
    }

    pub fn set_state(&mut self, region_id: u64, key: &[u8], val: &[u8]) {
        self.get_region(region_id).set_state(key, val);
    }

    pub fn reset(&mut self) {
        self.regions.clear()
    }

    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    pub(crate) fn merge_region(&mut self, region_batch: RegionBatch) {
        self.get_region(region_batch.region_id).merge(region_batch);
    }
}

/// `RegionBatch` is a batch of modifications in one region.
pub(crate) struct RegionBatch {
    pub(crate) region_id: u64,
    pub(crate) truncated_term: u64,
    pub(crate) truncated_idx: u64,
    pub(crate) states: BTreeMap<Bytes, Bytes>,
    pub(crate) raft_logs: VecDeque<RaftLogOp>,
}

impl RegionBatch {
    pub(crate) fn new(region_id: u64) -> Self {
        Self {
            region_id,
            truncated_term: 0,
            truncated_idx: 0,
            states: Default::default(),
            raft_logs: Default::default(),
        }
    }

    pub(crate) fn truncate(&mut self, idx: u64, term: u64) {
        if idx <= self.truncated_idx {
            return;
        }
        while let Some(true) = self.raft_logs.front().map(|l| l.index <= idx) {
            self.raft_logs.pop_front();
        }
        self.truncated_idx = idx;
        self.truncated_term = term;
    }

    pub fn set_state(&mut self, key: &[u8], val: &[u8]) {
        self.states
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(val));
    }

    pub fn append_raft_log(&mut self, op: RaftLogOp) {
        debug_assert!(self.truncated_idx < op.index);
        while let Some(true) = self.raft_logs.back().map(|l| l.index + 1 != op.index) {
            self.raft_logs.pop_back();
        }
        debug_assert!(
            self.raft_logs.is_empty()
                || self
                    .raft_logs
                    .back()
                    .map(|l| l.index + 1 == op.index)
                    .unwrap()
        );
        self.raft_logs.push_back(op);
    }

    pub fn merge(&mut self, other: RegionBatch) {
        debug_assert_eq!(self.region_id, other.region_id);
        self.states.extend(other.states);
        for op in other.raft_logs {
            self.append_raft_log(op);
        }
        if self.truncated_idx < other.truncated_idx {
            self.truncated_idx = other.truncated_idx;
            self.truncated_term = other.truncated_term;
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        let mut len = 8 /* region_id */ + 8 /* start_index */ + 8 /* end_index */ +
            8 /* truncated_index */ + 8 /* truncated_term */ + 4 /* states_len */;
        for (key, val) in &self.states {
            len += 2 /* key_len */ + key.len() + 4 /* val_len */ + val.len();
        }
        len += self.raft_logs.len() * 4 /* log_end_offset */;
        self.raft_logs
            .iter()
            .fold(len, |acc, l| acc + l.encoded_len())
    }

    ///  +-------------+---------------+--------------+-------------------+------------------+--------------+-----------------+------------+-------------------+--------------+-----+------------------+-----+--------------+-----+
    ///  |region_id(8B)|first_index(8B)|last_index(8B)|truncated_index(8B)|truncated_term(8B)|states_len(4B)|state_key_len(2B)|state_key(n)|state_value_len(4B)|state_value(n)| ... |log_end_offset(4B)| ... |raft_log_op(n)| ... |
    ///  +-------------+---------------+--------------+-------------------+------------------+--------------+-----------------+------------+-------------------+--------------+-----+------------------+-----+--------------+-----+
    pub(crate) fn encode_to(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.region_id);
        let first = self.raft_logs.front().map_or(0, |x| x.index);
        let end = self.raft_logs.back().map_or(0, |x| x.index + 1);
        buf.put_u64_le(first);
        buf.put_u64_le(end);
        buf.put_u64_le(self.truncated_idx);
        buf.put_u64_le(self.truncated_term);
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
        batch.truncated_term = LittleEndian::read_u64(buf);
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

#[cfg(test)]
mod tests {
    use eraftpb::{Entry, EntryType};

    use super::*;

    fn new_raft_entry(tp: EntryType, term: u64, index: u64, data: &[u8], context: u8) -> Entry {
        let mut entry = Entry::new();
        entry.set_entry_type(tp);
        entry.set_term(term);
        entry.set_index(index);
        entry.set_data(data.to_vec().into());
        entry.set_context(vec![context].into());
        entry
    }

    #[test]
    fn test_region_batch() {
        let mut region_batch = RegionBatch::new(1);

        let mut logs = vec![];
        for i in 1..=10 {
            let log = RaftLogOp::new(&new_raft_entry(EntryType::EntryNormal, 1, i, b"data", 0));
            logs.push(log.clone());
            region_batch.append_raft_log(log);
        }
        assert_eq!(region_batch.raft_logs, logs);
        region_batch.set_state(b"k1", b"v1");
        region_batch.set_state(b"k2", b"v2");
        region_batch.truncate(5, 1);
        assert_eq!(region_batch.truncated_idx, 5);
        assert_eq!(region_batch.truncated_term, 1);
        assert_eq!(region_batch.raft_logs, &logs[5..]);

        let mut buf = vec![];
        region_batch.encode_to(&mut buf);
        assert_eq!(buf.len(), region_batch.encoded_len());
        let decoded = RegionBatch::decode(&buf);
        assert_eq!(decoded.region_id, region_batch.region_id);
        assert_eq!(decoded.truncated_idx, region_batch.truncated_idx);
        assert_eq!(decoded.truncated_term, region_batch.truncated_term);
        assert_eq!(decoded.states, region_batch.states);
        assert_eq!(decoded.raft_logs, region_batch.raft_logs);

        // append conflicted log
        let log = RaftLogOp::new(&new_raft_entry(EntryType::EntryNormal, 1, 6, b"data", 0));
        region_batch.append_raft_log(log.clone());
        assert_eq!(region_batch.raft_logs.len(), 1);
        assert_eq!(region_batch.raft_logs[0], log);

        let mut region_batch = RegionBatch::new(1);
        for log in &logs[..5] {
            region_batch.append_raft_log(log.clone());
        }
        region_batch.set_state(b"k0", b"v0");
        region_batch.merge(decoded);
        assert_eq!(region_batch.truncated_idx, 5);
        assert_eq!(region_batch.truncated_term, 1);
        assert_eq!(region_batch.raft_logs, logs);
        assert_eq!(region_batch.states.len(), 3);
    }
}
