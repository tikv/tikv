// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{RFEngine, StateKey, WriteBatch};
use bytes::{BufMut, Bytes, BytesMut};
use engine_traits::Error::EntriesUnavailable;
use engine_traits::{RaftEngine, RaftEngineReadOnly, RaftLogBatch, Result};
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;

impl RaftEngineReadOnly for RFEngine {
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        panic!()
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let map = self.entries_map.read().unwrap();
        let res = map.get(&raft_group_id);
        if res.is_none() {
            return Ok(None);
        }
        let region_logs = res.unwrap();
        Ok(region_logs.get(index))
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let old_len = buf.len();
        let map = self.entries_map.read().unwrap();
        let res = map.get(&region_id);
        if res.is_none() {
            return Err(EntriesUnavailable);
        }
        let region_logs = res.unwrap();
        for i in low..high {
            let res = region_logs.get(i);
            if res.is_none() {
                return Err(EntriesUnavailable);
            }
            buf.push(res.unwrap());
            if let Some(max_size) = max_size {
                if buf.len() - old_len >= max_size as usize {
                    break;
                }
            }
        }
        return Ok(buf.len() - old_len);
    }
}

impl RaftEngine for RFEngine {
    type LogBatch = WriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        panic!()
    }

    fn sync(&self) -> Result<()> {
        panic!()
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        panic!()
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize> {
        panic!()
    }

    fn clean(
        &self,
        raft_group_id: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        panic!()
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        panic!()
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        panic!()
    }

    fn gc(&self, raft_group_id: u64, mut from: u64, to: u64) -> Result<usize> {
        panic!()
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        panic!()
    }

    fn has_builtin_entry_cache(&self) -> bool {
        panic!()
    }

    fn flush_metrics(&self, instance: &str) {
        panic!()
    }

    fn reset_statistics(&self) {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_engine_size(&self) -> Result<u64> {
        panic!()
    }
}

impl RaftLogBatch for WriteBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        panic!()
    }

    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64) {
        panic!()
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        panic!()
    }

    fn persist_size(&self) -> usize {
        panic!()
    }

    fn is_empty(&self) -> bool {
        panic!()
    }

    fn merge(&mut self, _: Self) {
        panic!()
    }
}
