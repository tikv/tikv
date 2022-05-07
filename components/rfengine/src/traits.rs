// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::*;
use crate::{RFEngine, WriteBatch};
use engine_traits::Error::EntriesUnavailable;
use engine_traits::{RaftEngine, RaftEngineReadOnly, RaftLogBatch, Result};
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;
use std::time::Instant;

impl RaftEngineReadOnly for RFEngine {
    fn get_raft_state(&self, _raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        panic!()
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let map_ref = self.regions.get(&raft_group_id);
        if map_ref.is_none() {
            return Ok(None);
        }
        let map_ref = map_ref.unwrap();
        let region_data = map_ref.read().unwrap();
        Ok(region_data.get(index))
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
        let map_ref = self.regions.get(&region_id);
        if map_ref.is_none() {
            return Ok(0);
        }
        let map_ref = map_ref.unwrap();
        let timer = Instant::now();
        let region_data = map_ref.read().unwrap();
        for i in low..high {
            let res = region_data.get(i);
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
        ENGINE_FETCH_ENTRIES_DURATION_HISTOGRAM.observe(elapsed_secs(timer));
        Ok(buf.len() - old_len)
    }

    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        unreachable!("todo")
    }
}

impl RaftEngine for RFEngine {
    type LogBatch = WriteBatch;

    fn log_batch(&self, _capacity: usize) -> Self::LogBatch {
        panic!()
    }

    fn sync(&self) -> Result<()> {
        panic!()
    }

    fn consume(&self, _batch: &mut Self::LogBatch, _sync_log: bool) -> Result<usize> {
        panic!()
    }

    fn consume_and_shrink(
        &self,
        _batch: &mut Self::LogBatch,
        _sync_log: bool,
        _max_capacity: usize,
        _shrink_to: usize,
    ) -> Result<usize> {
        panic!()
    }

    fn clean(
        &self,
        _raft_group_id: u64,
        _first_index: u64,
        _state: &RaftLocalState,
        _batch: &mut Self::LogBatch,
    ) -> Result<()> {
        panic!()
    }

    fn append(&self, _raft_group_id: u64, _entries: Vec<Entry>) -> Result<usize> {
        panic!()
    }

    fn put_raft_state(&self, _raft_group_id: u64, _state: &RaftLocalState) -> Result<()> {
        panic!()
    }

    fn gc(&self, _raft_group_id: u64, mut _from: u64, _to: u64) -> Result<usize> {
        panic!()
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        panic!()
    }

    fn has_builtin_entry_cache(&self) -> bool {
        panic!()
    }

    fn flush_metrics(&self, _instance: &str) {
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
    fn append(&mut self, _raft_group_id: u64, _entries: Vec<Entry>) -> Result<()> {
        panic!()
    }

    fn cut_logs(&mut self, _raft_group_id: u64, _from: u64, _to: u64) {
        panic!()
    }

    fn put_raft_state(&mut self, _raft_group_id: u64, _state: &RaftLocalState) -> Result<()> {
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
