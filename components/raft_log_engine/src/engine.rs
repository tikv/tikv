// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::path::Path;

use engine_traits::{
    CacheStats, RaftEngine, RaftEngineReadOnly, RaftLogBatch as RaftLogBatchTrait, Result,
};
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;
use raft_engine::{
    Command, Error as RaftEngineError, LogBatch, MessageExt, RaftLogEngine as RawRaftEngine,
};

pub use raft_engine::{Config as RaftEngineConfig, RecoveryMode};

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

#[derive(Clone)]
pub struct RaftLogEngine(RawRaftEngine<MessageExtTyped>);

impl RaftLogEngine {
    pub fn new(config: RaftEngineConfig) -> Result<Self> {
        Ok(RaftLogEngine(
            RawRaftEngine::open(config).map_err(transfer_error)?,
        ))
    }

    /// If path is not an empty directory, we say db exists.
    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn raft_groups(&self) -> Vec<u64> {
        self.0.raft_groups()
    }

    pub fn first_index(&self, raft_id: u64) -> Option<u64> {
        self.0.first_index(raft_id)
    }

    pub fn last_index(&self, raft_id: u64) -> Option<u64> {
        self.0.last_index(raft_id)
    }
}

#[derive(Default)]
pub struct RaftLogBatch(LogBatch<MessageExtTyped>);

const RAFT_LOG_STATE_KEY: &[u8] = b"R";

impl RaftLogBatchTrait for RaftLogBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        self.0.add_entries(raft_group_id, entries);
        Ok(())
    }

    fn cut_logs(&mut self, _: u64, _: u64, _: u64) {
        // It's unnecessary because overlapped entries can be handled in `append`.
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.0
            .put_message(raft_group_id, RAFT_LOG_STATE_KEY.to_vec(), state)
            .map_err(transfer_error)
    }

    fn persist_size(&self) -> usize {
        self.0.approximate_size()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn merge(&mut self, src: Self) {
        self.0.merge(src.0);
    }
}

impl RaftEngineReadOnly for RaftLogEngine {
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        self.0
            .get_message(raft_group_id, RAFT_LOG_STATE_KEY)
            .map_err(transfer_error)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        self.0
            .get_entry(raft_group_id, index)
            .map_err(transfer_error)
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
            .map_err(transfer_error)
    }

    fn get_all_entries_to(&self, raft_group_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        if let Some(first) = self.0.first_index(raft_group_id) {
            let last = self.0.last_index(raft_group_id).unwrap();
            buf.reserve((last - first + 1) as usize);
            self.fetch_entries_to(raft_group_id, first, last + 1, None, buf)?;
        }
        Ok(())
    }
}

impl RaftEngine for RaftLogEngine {
    type LogBatch = RaftLogBatch;

    fn log_batch(&self, _capacity: usize) -> Self::LogBatch {
        RaftLogBatch::default()
    }

    fn sync(&self) -> Result<()> {
        self.0.sync().map_err(transfer_error)
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize> {
        self.0.write(&mut batch.0, sync).map_err(transfer_error)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        _: usize,
        _: usize,
    ) -> Result<usize> {
        self.0.write(&mut batch.0, sync).map_err(transfer_error)
    }

    fn clean(
        &self,
        raft_group_id: u64,
        _: u64,
        _: &RaftLocalState,
        batch: &mut RaftLogBatch,
    ) -> Result<()> {
        batch.0.add_command(raft_group_id, Command::Clean);
        Ok(())
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut batch = Self::LogBatch::default();
        batch.0.add_entries(raft_group_id, entries);
        self.0.write(&mut batch.0, false).map_err(transfer_error)
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.0
            .put_message(raft_group_id, RAFT_LOG_STATE_KEY, state)
            .map_err(transfer_error)
    }

    fn gc(&self, raft_group_id: u64, _from: u64, to: u64) -> Result<usize> {
        Ok(self.0.compact_to(raft_group_id, to) as usize)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        self.0.purge_expired_files().map_err(transfer_error)
    }

    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    fn gc_entry_cache(&self, _raft_group_id: u64, _to: u64) {}

    /// Flush current cache stats.
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String> {
        // Raft engine won't dump anything.
        Ok("".to_owned())
    }

    fn get_engine_size(&self) -> Result<u64> {
        //TODO impl this when RaftLogEngine is ready to go online.
        Ok(0)
    }
}

fn transfer_error(e: RaftEngineError) -> engine_traits::Error {
    match e {
        RaftEngineError::StorageCompacted => engine_traits::Error::EntriesCompacted,
        RaftEngineError::StorageUnavailable => engine_traits::Error::EntriesUnavailable,
        e => {
            let e = box_err!(e);
            engine_traits::Error::Other(e)
        }
    }
}
