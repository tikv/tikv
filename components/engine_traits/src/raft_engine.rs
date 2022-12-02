// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    metapb::Region,
    raft_serverpb::{
        RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent, StoreRecoverState,
    },
};
use raft::eraftpb::Entry;

use crate::*;

pub const RAFT_LOG_MULTI_GET_CNT: u64 = 8;

pub trait RaftEngineReadOnly: Sync + Send + 'static {
    fn is_empty(&self) -> Result<bool>;

    fn get_store_ident(&self) -> Result<Option<StoreIdent>>;
    fn get_prepare_bootstrap_region(&self) -> Result<Option<Region>>;

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>>;
    fn get_region_state(&self, raft_group_id: u64) -> Result<Option<RegionLocalState>>;
    fn get_apply_state(&self, raft_group_id: u64) -> Result<Option<RaftApplyState>>;
    fn get_recover_state(&self) -> Result<Option<StoreRecoverState>>;

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

    /// Get all available entries in the region.
    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()>;
}

pub trait RaftEngineDebug: RaftEngine + Sync + Send + 'static {
    /// Scan all log entries of given raft group in order.
    fn scan_entries<F>(&self, raft_group_id: u64, f: F) -> Result<()>
    where
        F: FnMut(&Entry) -> Result<bool>;

    /// Put all data of given raft group into a log batch.
    fn dump_all_data(&self, region_id: u64) -> <Self as RaftEngine>::LogBatch {
        let mut batch = self.log_batch(0);
        let mut entries = Vec::new();
        self.scan_entries(region_id, |e| {
            entries.push(e.clone());
            Ok(true)
        })
        .unwrap();
        batch.append(region_id, entries).unwrap();
        if let Some(state) = self.get_raft_state(region_id).unwrap() {
            batch.put_raft_state(region_id, &state).unwrap();
        }
        batch
    }
}

pub struct RaftLogGcTask {
    pub raft_group_id: u64,
    pub from: u64,
    pub to: u64,
}

// TODO: Refactor common methods between Kv and Raft engine into a shared trait.
pub trait RaftEngine: RaftEngineReadOnly + PerfContextExt + Clone + Sync + Send + 'static {
    type LogBatch: RaftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Synchronize the Raft engine.
    fn sync(&self) -> Result<()>;

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
        first_index: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Append some log entries and return written bytes.
    ///
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize>;

    fn put_store_ident(&self, ident: &StoreIdent) -> Result<()>;

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    /// Like `cut_logs` but the range could be very large. Return the deleted
    /// count. Generally, `from` can be passed in `0`.
    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize>;

    fn batch_gc(&self, tasks: Vec<RaftLogGcTask>) -> Result<usize> {
        let mut total = 0;
        for task in tasks {
            total += self.gc(task.raft_group_id, task.from, task.to)?;
        }
        Ok(total)
    }

    fn need_manual_purge(&self) -> bool {
        false
    }

    /// Purge expired logs files and return a set of Raft group ids
    /// which needs to be compacted ASAP.
    fn manual_purge(&self) -> Result<Vec<u64>> {
        unimplemented!()
    }

    fn flush_metrics(&self, _instance: &str) {}
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }
    fn reset_statistics(&self) {}

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String>;

    fn get_engine_size(&self) -> Result<u64>;

    /// The path to the directory on the filesystem where the raft log is stored
    fn get_engine_path(&self) -> &str;

    /// Visit all available raft groups.
    ///
    /// If any error is returned, the iteration will stop.
    fn for_each_raft_group<E, F>(&self, f: &mut F) -> std::result::Result<(), E>
    where
        F: FnMut(u64) -> std::result::Result<(), E>,
        E: From<Error>;

    /// Indicate whether region states should be recovered from raftdb and
    /// replay raft logs.
    /// When kvdb's write-ahead-log is disabled, the sequence number of the last
    /// boot time is saved.
    fn put_recover_state(&self, state: &StoreRecoverState) -> Result<()>;
}

pub trait RaftLogBatch: Send {
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

    /// Remove Raft logs in [`from`, `to`) which will be overwritten later.
    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64);

    fn put_store_ident(&mut self, ident: &StoreIdent) -> Result<()>;

    fn put_prepare_bootstrap_region(&mut self, region: &Region) -> Result<()>;
    fn remove_prepare_bootstrap_region(&mut self) -> Result<()>;

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;
    fn put_region_state(&mut self, raft_group_id: u64, state: &RegionLocalState) -> Result<()>;
    fn put_apply_state(&mut self, raft_group_id: u64, state: &RaftApplyState) -> Result<()>;

    /// The data size of this RaftLogBatch.
    fn persist_size(&self) -> usize;

    /// Whether it is empty or not.
    fn is_empty(&self) -> bool;

    /// Merge another RaftLogBatch to itself.
    fn merge(&mut self, _: Self) -> Result<()>;
}

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}
