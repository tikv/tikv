// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Error, RaftEngine, RaftEngineReadOnly, RaftLogBatch, Result};
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::eraftpb::Entry;
use tikv_util::time::Instant;

use crate::{metrics::*, RfEngine, WriteBatch};

impl RaftEngineReadOnly for RfEngine {
    fn get_raft_state(&self, _raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        panic!()
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        Ok(self
            .regions
            .get(&raft_group_id)
            .and_then(|data| data.read().unwrap().get(index)))
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>, // size limit of fetched entries
        buf: &mut Vec<Entry>,
    ) -> Result<usize> /* entry count */ {
        if high <= low {
            return Ok(0);
        }
        let old_len = buf.len();
        let region_data = self
            .regions
            .get(&region_id)
            .ok_or(Error::EntriesCompacted)?;
        let region_data = region_data.read().unwrap();
        if low <= region_data.truncated_idx {
            return Err(Error::EntriesCompacted);
        }

        let timer = Instant::now_coarse();
        let mut total_size = 0;
        for i in low..high {
            let entry = region_data.get(i).ok_or(Error::EntriesUnavailable)?;
            total_size += entry.compute_size() as usize;
            buf.push(entry);
            if max_size.map_or(false, |s| total_size >= s) {
                // At least return one entry regardless of size limit.
                break;
            }
        }
        ENGINE_FETCH_ENTRIES_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        Ok(buf.len() - old_len)
    }

    fn get_all_entries_to(&self, _region_id: u64, _buf: &mut Vec<Entry>) -> Result<()> {
        unreachable!("todo")
    }
}

impl RaftEngine for RfEngine {
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

    fn merge(&mut self, _: Self) -> Result<()> {
        panic!()
    }
}
