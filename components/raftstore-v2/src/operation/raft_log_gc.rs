// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the compaction of logs stored in Raft Engine and
//! entry cache.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb,
    raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest},
};
use raftstore::store::{needs_evict_entry_cache, Transport};
use slog::{debug, error};
use tikv_util::sys::memory_usage_reaches_high_water;

use crate::{
    batch::StoreContext, fsm::PeerFsmDelegate, raft::Peer, router::PeerTick, worker::RaftLogGcTask,
};

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_raft_log_gc(&mut self) {
        self.schedule_tick(PeerTick::RaftLogGc);
        if needs_evict_entry_cache(self.store_ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm
                .peer_mut()
                .entry_storage_mut()
                .evict_entry_cache(true);
            if !self.fsm.peer().entry_storage().is_entry_cache_empty() {
                self.schedule_tick(PeerTick::EntryCacheEvict);
            }
        }
    }

    pub fn on_entry_cache_evict(&mut self) {
        if needs_evict_entry_cache(self.store_ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm
                .peer_mut()
                .entry_storage_mut()
                .evict_entry_cache(true);
        }
        let mut _usage = 0;
        if memory_usage_reaches_high_water(&mut _usage)
            && !self.fsm.peer().entry_storage().is_entry_cache_empty()
        {
            self.schedule_tick(PeerTick::EntryCacheEvict);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn schedule_raftlog_gc<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        compact_to: u64,
    ) {
        let last_compacted_idx = 0;
        let task = RaftLogGcTask::gc(self.region_id(), last_compacted_idx, compact_to);
        debug!(
            self.logger,
            "scheduling raft log gc task";
            "region_id" => self.region_id(),
            "peer_id" => self.peer_id(),
            "task" => %task,
        );
        if let Err(e) = store_ctx.raft_log_gc_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to schedule raft log gc task";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => %e,
            );
        }
    }
}
