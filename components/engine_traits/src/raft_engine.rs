use crate::*;
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;

pub trait RaftEngine: Clone + Sync + Send + 'static {
    type LogBatch: RaftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Synchronize the Raft engine.
    fn sync(&self) -> Result<()>;

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>>;

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>>;

    /// Return count of fetched entries.
    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize>;

    /// Consume the write batch by moving the content into the engine itself
    /// and return written bytes.
    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize>;

    /// Like `consume` but shrink `batch` if need.
    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize>;

    fn clean(
        &self,
        raft_group_id: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Append some log entries and return written bytes.
    ///
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize>;

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    /// Like `cut_logs` but the range could be very large. Return the deleted count.
    /// Generally, `from` can be passed in `0`.
    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize>;

    /// Purge expired logs files and return a set of Raft group ids
    /// which needs to be compacted ASAP.
    fn purge_expired_files(&self) -> Result<Vec<u64>>;

    /// The `RaftEngine` has a builtin entry cache or not.
    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    /// GC the builtin entry cache.
    fn gc_entry_cache(&self, _raft_group_id: u64, _to: u64) {}

    fn flush_metrics(&self, _instance: &str) {}
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }
    fn reset_statistics(&self) {}

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String>;
}

pub trait RaftLogBatch: Send {
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

    /// Remove Raft logs in [`from`, `to`) which will be overwritten later.
    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64);

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    fn persist_size(&self) -> usize;

    fn is_empty(&self) -> bool;
}

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}
