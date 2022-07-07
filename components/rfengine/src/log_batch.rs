// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp::Ordering, collections::VecDeque};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes};
use protobuf::ProtobufEnum;
use raft_proto::eraftpb;

/// `RaftLogOp` is identical to `eraftpb::Entry`. It implements custom
/// serialization/deserialization for better performance than protobuf.
#[derive(Default, Clone, PartialEq, Debug)]
pub(crate) struct RaftLogOp {
    pub(crate) index: u64,
    pub(crate) term: u32,
    pub(crate) e_type: u8,
    // `ProposalContext` in components/rfstore/src/store/peer.rs.
    pub(crate) context: u8,
    pub(crate) data: Bytes,
}

impl RaftLogOp {
    pub fn new(entry: &eraftpb::Entry) -> Self {
        let context = *entry.context.last().unwrap_or(&0);
        Self {
            index: entry.index,
            term: entry.term as u32,
            e_type: entry.entry_type.value() as u8,
            context,
            data: entry.data.clone(),
        }
    }

    pub(crate) fn to_entry(&self) -> eraftpb::Entry {
        let mut entry = eraftpb::Entry::new();
        entry.set_entry_type(eraftpb::EntryType::from_i32(self.e_type as i32).unwrap());
        entry.set_term(self.term as u64);
        entry.set_index(self.index);
        if self.context > 0 {
            entry.set_context(vec![self.context].into());
        }
        entry.set_data(self.data.clone());
        entry
    }

    pub(crate) fn encoded_len(&self) -> usize {
        8 /* index */ + 4 /* term */ + 1 /* entry_type */ + 1 /* context */ + self.data.len()
    }

    /// +-------------+-----------+-------------------+---------------+--------------+
    /// |             |           |                   |               |              |
    /// |  index(8B)  |  term(4B) |   entry_type(1B)  |  context(1B)  |   data(n)    |
    /// |             |           |                   |               |              |
    /// +-------------+-----------+-------------------+---------------+--------------+
    // It's faster than protobuf, so as `decode`. See `bench_encode_and_decode_raft_log_op`.
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

// VecDeque is a ringbuffer which leaves one space empty, so that the capacity
// is (n + 1).next_power_of_two() - 1 which means 256 corresponds to 511.
const RAFT_LOG_BLOCK_CAP: usize = 255;

/// `RaftLogBlock` contains fixed count raft logs.
/// It's the building block of `RaftLogs`. Caller should make sure index is in the range.
#[derive(Clone)]
pub(crate) struct RaftLogBlock {
    logs: VecDeque<RaftLogOp>,
    size: usize,
}

impl Default for RaftLogBlock {
    fn default() -> Self {
        RaftLogBlock::new()
    }
}

impl RaftLogBlock {
    fn new() -> Self {
        Self {
            logs: VecDeque::with_capacity(RAFT_LOG_BLOCK_CAP),
            size: 0,
        }
    }

    pub(crate) fn first_index(&self) -> u64 {
        self.logs.front().map_or(0, |front| front.index)
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.logs.back().map_or(0, |back| back.index)
    }

    /// Truncates and returns all logs whose index is less than or equal to the `truncated_idx`.
    /// It's used to truncate persisted logs.
    fn truncate_left(&mut self, truncated_idx: u64) -> RaftLogBlock {
        debug_assert!(self.first_index() <= truncated_idx && truncated_idx < self.last_index());
        let mut truncated_block = RaftLogBlock::new();
        while let Some(true) = self.logs.front().map(|f| f.index <= truncated_idx) {
            truncated_block.append(self.logs.pop_front().unwrap());
        }
        self.size -= truncated_block.size;
        truncated_block
    }

    /// Truncates all logs whose index is greater than or equal to the `truncated_idx`.
    /// It's used to truncate conflicted logs.
    fn truncate_right(&mut self, truncated_idx: u64) {
        debug_assert!(self.first_index() < truncated_idx && truncated_idx <= self.last_index());
        while let Some(back) = self.logs.pop_back() {
            if back.index < truncated_idx {
                self.logs.push_back(back);
                break;
            }
            self.size -= back.data.len();
        }
    }

    fn append(&mut self, op: RaftLogOp) {
        debug_assert!(self.last_index() == 0 || self.last_index() + 1 == op.index);
        self.size += op.data.len();
        self.logs.push_back(op);
    }

    /// Gets the `RaftLogOp` with the corresponding `index`.
    fn get(&self, index: u64) -> &RaftLogOp {
        debug_assert!(self.first_index() <= index && index <= self.last_index());
        &self.logs[(index - self.first_index()) as usize]
    }
}

/// `RaftLogs` contains continuous raft logs in memory for a single raft group.
#[derive(Clone, Default)]
pub(crate) struct RaftLogs {
    // Actually one `VecDeque<RaftLogOp>` can satisfiy our requirements. However, when
    // truncating, it may drop lots of raft logs in the main thread which hurts the performance,
    // so we allocate small chunks of raft logs and transfer them to the background worker to drop.
    blocks: VecDeque<RaftLogBlock>,
}

impl RaftLogs {
    pub(crate) fn size(&self) -> usize {
        self.blocks.iter().fold(0, |acc, b| acc + b.size)
    }

    pub(crate) fn first_index(&self) -> u64 {
        self.blocks.front().map_or(0, |front| front.first_index())
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.blocks.back().map_or(0, |back| back.last_index())
    }

    /// Appends the raft log. It handles log continuity internally and returns conflicted logs if any.
    pub(crate) fn append(&mut self, op: RaftLogOp) -> Vec<RaftLogBlock> {
        let mut truncated_blocks = vec![];
        let next_idx = self.last_index() + 1;
        let op_idx = op.index;
        match op_idx.cmp(&next_idx) {
            Ordering::Greater => {
                // There is gap between existing logs and next log, clear all.
                truncated_blocks.extend(self.blocks.drain(..));
            }
            Ordering::Less => {
                while let Some(mut block) = self.blocks.pop_back() {
                    if op_idx <= block.first_index() {
                        truncated_blocks.push(block);
                    } else {
                        block.truncate_right(op_idx);
                        self.blocks.push_back(block);
                        break;
                    }
                }
            }
            Ordering::Equal => {}
        }
        match self.blocks.back() {
            None => {
                self.blocks.push_back(RaftLogBlock::new());
            }
            Some(back) => {
                if back.logs.len() == back.logs.capacity() {
                    self.blocks.push_back(RaftLogBlock::new());
                }
            }
        }
        let back_block = self.blocks.back_mut().unwrap();
        back_block.append(op);
        truncated_blocks
    }

    /// Truncates and returns all logs whose index is less than or equal to the `index`.
    pub(crate) fn truncate(&mut self, index: u64) -> Vec<RaftLogBlock> {
        let mut truncated = Vec::with_capacity(self.blocks.len());
        while let Some(mut front) = self.blocks.pop_front() {
            if front.last_index() <= index {
                truncated.push(front);
                continue;
            }
            if front.first_index() <= index {
                let truncated_block = front.truncate_left(index);
                truncated.push(truncated_block);
            }
            self.blocks.push_front(front);
            break;
        }
        truncated
    }

    /// Gets the entry with the corresponding `index`.
    pub(crate) fn get(&self, index: u64) -> Option<eraftpb::Entry> {
        if index == 0 || index < self.first_index() || self.last_index() < index {
            return None;
        }
        let block_idx = match self.blocks.binary_search_by_key(&index, |b| b.last_index()) {
            Ok(i) => i,
            Err(i) => i,
        };
        Some(self.blocks[block_idx].get(index).to_entry())
    }
}

#[cfg(test)]
mod tests {
    use eraftpb::{Entry, EntryType};
    use protobuf::Message;

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
    fn test_raft_log_op() {
        let entry = new_raft_entry(EntryType::EntryNormal, 1, 2, b"data", 0b0000_0001);

        let log = RaftLogOp::new(&entry);
        assert_eq!(log.e_type, entry.entry_type.value() as u8);
        assert_eq!(log.term, entry.term as u32);
        assert_eq!(log.index, entry.index);
        assert_eq!(log.context, *entry.context.last().unwrap());
        assert_eq!(log.data, entry.data);
        assert_eq!(log.to_entry(), entry);

        let mut buf = vec![];
        log.encode_to(&mut buf);
        assert_eq!(buf.len(), log.encoded_len());
        let decoded = RaftLogOp::decode(&buf);
        assert_eq!(decoded, log);
    }

    fn bench_encode_and_decode_raft_log_op(b: &mut test::Bencher, entry: &Entry) {
        let log = RaftLogOp::new(entry);
        let mut buf = vec![];

        b.iter(|| {
            log.encode_to(&mut buf);
            test::black_box(RaftLogOp::decode(&buf));
            unsafe {
                buf.set_len(0);
            }
        })
    }

    #[bench]
    fn bench_encode_and_decode_small_raft_log_op(b: &mut test::Bencher) {
        let entry = new_raft_entry(EntryType::EntryNormal, 1, 2, b"data", 0b0000_0001);
        bench_encode_and_decode_raft_log_op(b, &entry);
    }

    #[bench]
    fn bench_encode_and_decode_large_raft_log_op(b: &mut test::Bencher) {
        let entry = new_raft_entry(
            EntryType::EntryNormal,
            u32::MAX as u64,
            u32::MAX as u64,
            b"x".repeat(1024 * 10).as_slice(),
            0b0000_0001,
        );
        bench_encode_and_decode_raft_log_op(b, &entry);
    }

    fn bench_encode_and_decode_eraftpb_entry(b: &mut test::Bencher, entry: &mut Entry) {
        let mut buf = vec![];

        b.iter(|| {
            test::black_box(entry.write_to_vec(&mut buf).is_ok());
            test::black_box(entry.merge_from_bytes(&buf).is_ok());
            unsafe {
                buf.set_len(0);
            }
        })
    }

    #[bench]
    fn bench_encode_and_decode_small_eraftpb_entry(b: &mut test::Bencher) {
        let mut entry = new_raft_entry(EntryType::EntryNormal, 1, 2, b"data", 0b0000_0001);
        bench_encode_and_decode_eraftpb_entry(b, &mut entry);
    }

    #[bench]
    fn bench_encode_and_decode_large_eraftpb_entry(b: &mut test::Bencher) {
        let mut entry = new_raft_entry(
            EntryType::EntryNormal,
            u32::MAX as u64,
            u32::MAX as u64,
            b"x".repeat(1024 * 10).as_slice(),
            0b0000_0001,
        );
        bench_encode_and_decode_eraftpb_entry(b, &mut entry);
    }

    #[test]
    fn test_raft_log_block() {
        let mut block = RaftLogBlock::new();
        assert_eq!(block.logs.capacity(), 255);

        let mut logs = vec![];
        for i in 1..=5 {
            let log = RaftLogOp::new(&new_raft_entry(EntryType::EntryNormal, 1, i, b"data", 0));
            logs.push(log.clone());
            block.append(log);
            assert_eq!(block.first_index(), 1);
            assert_eq!(block.last_index(), i);
            assert_eq!(block.size, 4 * i as usize);
        }
        assert_eq!(block.logs, logs);
        for log in &logs {
            assert_eq!(block.get(log.index), log);
        }

        let truncated = block.truncate_left(3);
        assert_eq!(truncated.first_index(), 1);
        assert_eq!(truncated.last_index(), 3);
        assert_eq!(truncated.size, 12);
        assert_eq!(truncated.logs, &logs[..3]);
        assert_eq!(block.first_index(), 4);
        assert_eq!(block.last_index(), 5);
        assert_eq!(block.size, 8);
        assert_eq!(block.logs, &logs[3..]);

        block.truncate_right(5);
        assert_eq!(block.first_index(), 4);
        assert_eq!(block.last_index(), 4);
        assert_eq!(block.size, 4);
        assert_eq!(block.logs, &logs[3..4]);
    }

    #[test]
    fn test_raft_logs() {
        let mut raft_logs = RaftLogs::default();
        assert_eq!(raft_logs.first_index(), 0);
        assert_eq!(raft_logs.last_index(), 0);

        let mut logs = vec![];
        for i in 1..=RAFT_LOG_BLOCK_CAP {
            let log = RaftLogOp::new(&new_raft_entry(
                EntryType::EntryNormal,
                1,
                i as u64,
                b"data",
                0,
            ));
            logs.push(log.clone());
            assert!(raft_logs.append(log).is_empty());
        }
        assert_eq!(raft_logs.blocks.len(), 1);
        assert_eq!(raft_logs.first_index(), 1);
        assert_eq!(raft_logs.last_index(), 255);

        // Extend one block.
        let log = RaftLogOp::new(&new_raft_entry(EntryType::EntryNormal, 1, 256, b"data", 0));
        logs.push(log.clone());
        assert!(raft_logs.append(log).is_empty());
        assert_eq!(raft_logs.blocks.len(), 2);
        assert_eq!(raft_logs.first_index(), 1);
        assert_eq!(raft_logs.last_index(), 256);
        assert_eq!(
            raft_logs.size(),
            raft_logs.blocks[0].size + raft_logs.blocks[1].size
        );

        for log in &logs {
            assert_eq!(raft_logs.get(log.index).unwrap(), log.to_entry());
        }
        assert!(raft_logs.get(257).is_none());

        // Append conflicted logs.
        let log = RaftLogOp::new(&new_raft_entry(EntryType::EntryNormal, 1, 255, b"data1", 0));
        logs[(log.index - 1) as usize] = log.clone();
        let conflicted = raft_logs.append(log.clone());
        assert_eq!(conflicted.len(), 1);
        assert_eq!(conflicted[0].logs, &logs[log.index as usize..]);
        assert_eq!(raft_logs.last_index(), log.index);
        assert_eq!(raft_logs.blocks.len(), 1);
        for log in &logs[..log.index as usize] {
            assert_eq!(raft_logs.get(log.index).unwrap(), log.to_entry());
        }

        // Truncate.
        assert!(raft_logs.append(logs.last().unwrap().clone()).is_empty());
        assert_eq!(raft_logs.blocks.len(), 2);

        let truncated = raft_logs.truncate(10);
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].logs, &logs[0..10]);
        assert_eq!(raft_logs.first_index(), 11);
        assert_eq!(raft_logs.last_index(), 256);
        assert_eq!(raft_logs.blocks.len(), 2);
        for log in &logs[11..] {
            assert_eq!(raft_logs.get(log.index).unwrap(), log.to_entry());
        }

        let truncated = raft_logs.truncate(255);
        assert_eq!(truncated.len(), 1);
        assert_eq!(truncated[0].logs, &logs[10..255]);
        assert_eq!(raft_logs.first_index(), 256);
        assert_eq!(raft_logs.last_index(), 256);
        assert_eq!(raft_logs.blocks.len(), 1);
        assert_eq!(raft_logs.get(256).unwrap(), logs.last().unwrap().to_entry());

        // Append a log with hole.
        let log = RaftLogOp::new(&new_raft_entry(EntryType::EntryNormal, 1, 500, b"data", 0));
        let conflicted = raft_logs.append(log.clone());
        assert_eq!(conflicted.len(), 1);
        assert_eq!(conflicted[0].logs, &logs[255..]);
        assert_eq!(raft_logs.first_index(), 500);
        assert_eq!(raft_logs.last_index(), 500);
        assert_eq!(raft_logs.blocks.len(), 1);
        assert_eq!(raft_logs.get(500).unwrap(), log.to_entry());

        // Get a truncated log.
        assert!(raft_logs.get(1).is_none());
    }
}
