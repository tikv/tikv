// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Error, RaftEngine, RaftEngineDebug, RaftEngineReadOnly, RaftLogBatch, Result};
use kvproto::{
    metapb::Region,
    raft_serverpb::{RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent},
};
use raft::eraftpb::Entry;

use crate::{engine::PanicEngine, write_batch::PanicWriteBatch};

impl RaftEngineReadOnly for PanicEngine {
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        panic!()
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        panic!()
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        panic!()
    }

    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        panic!()
    }

    fn is_empty(&self) -> Result<bool> {
        panic!()
    }

    fn get_store_ident(&self) -> Result<Option<StoreIdent>> {
        panic!()
    }

    fn get_prepare_bootstrap_region(&self) -> Result<Option<Region>> {
        panic!()
    }

    fn get_region_state(&self, raft_group_id: u64) -> Result<Option<RegionLocalState>> {
        panic!()
    }

    fn get_apply_state(&self, raft_group_id: u64) -> Result<Option<RaftApplyState>> {
        panic!()
    }
}

impl RaftEngineDebug for PanicEngine {
    fn scan_entries<F>(&self, _: u64, _: F) -> Result<()>
    where
        F: FnMut(&Entry) -> Result<bool>,
    {
        panic!()
    }

    fn dump_all_data(&self, _: u64) -> <Self as RaftEngine>::LogBatch {
        panic!()
    }
}

impl RaftEngine for PanicEngine {
    type LogBatch = PanicWriteBatch;

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
        first_index: u64,
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

    fn put_store_ident(&self, ident: &StoreIdent) -> Result<()> {
        panic!()
    }
}

impl RaftLogBatch for PanicWriteBatch {
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

    fn merge(&mut self, _: Self) -> Result<()> {
        panic!()
    }

    fn put_store_ident(&mut self, ident: &StoreIdent) -> Result<()> {
        panic!()
    }

    fn put_prepare_bootstrap_region(&mut self, region: &Region) -> Result<()> {
        panic!()
    }

    fn remove_prepare_bootstrap_region(&mut self) -> Result<()> {
        panic!()
    }

    fn put_region_state(&mut self, raft_group_id: u64, state: &RegionLocalState) -> Result<()> {
        panic!()
    }

    fn put_apply_state(&mut self, raft_group_id: u64, state: &RaftApplyState) -> Result<()> {
        panic!()
    }
}
