// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use engine_traits::{RaftEngine, RaftLogBatch};
use raft::eraftpb::Entry;
use raft_engine::{Entry as EntryTrait, LogBatch as LogBatchBase, RaftLogEngine as RawRaftEngine};

impl EntryTrait for Entry {
    fn index(&self) -> u64 {
        self.get_index()
    }
}

pub struct RaftLogEngine(RawRaftEngine<Entry>);
pub type LogBatch = LogBatchBase<Entry>;

impl RaftEngine for RaftLogEngine {
    type LogBatch = LogBatch;

    fn log_batch(&self, _capacity: usize) -> Self::LogBatch {
        LogBatch::default()
    }

    fn sync(&self) -> Result<()> {
        self.0.sync()
    }

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        self.0.get_msg(raft_group_id, RAFT_LOG_STATE_KEY)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        self.0.get_entry(raft_group_id, index)
    }

    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize> {
        self.0
            .fetch_entries_to(raft_group_id, begin, end, max_size, to)
    }

    fn write(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize> {
        self.0.write(std::mem::take(batch), sync)
    }

    fn write_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        _: usize,
        _: usize,
    ) -> Result<usize> {
        self.write(batch, sync)
    }

    fn clean(&self, raft_group_id: u64, _: &RaftLocalState, batch: &mut LogBatch) -> Result<()> {
        batch.clean_region(raft_group_id);
        Ok(())
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut batch = LogBatch::default();
        batch.add_entries(raft_group_id, entries);
        self.write(batch, false)
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.0.put_msg(raft_group_id, RAFT_LOG_STATE_KEY, state)
    }

    fn gc(&self, raft_group_id: u64, _from: u64, to: u64) -> Result<usize> {
        Ok(self.compact_to(raft_group_id, to) as usize)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let _purge_mutex = match self.purge_mutex.try_lock() {
            Ok(locked) if self.needs_purge_log_files() => locked,
            _ => return Ok(vec![]),
        };

        let (rewrite_limit, compact_limit) = self.latest_inactive_file_num();
        let mut will_force_compact = Vec::new();
        self.regions_rewrite_or_force_compact(
            rewrite_limit,
            compact_limit,
            &mut will_force_compact,
        );

        let min_file_num = self.memtables.fold(u64::MAX, |min, t| {
            cmp::min(min, t.min_file_num().unwrap_or(u64::MAX))
        });
        let purged = self.pipe_log.purge_to(min_file_num)?;
        info!("purged {} expired log files", purged);

        Ok(will_force_compact)
    }

    fn has_builtin_entry_cache(&self) -> bool {
        true
    }

    fn gc_entry_cache(&self, raft_group_id: u64, to: u64) {
        self.compact_cache_to(raft_group_id, to)
    }

    fn stop(&self) {
        let mut workers = self.workers.wl();
        workers.cache_evict.stop();
    }
}
