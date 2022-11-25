// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module drives the compaction of Raft logs stored in entry cache and
//! Raft engine.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb,
    raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest},
};
use raftstore::store::{needs_evict_entry_cache, Transport};
use slog::{debug, error};
use tikv_util::sys::memory_usage_reaches_high_water;

use crate::{batch::StoreContext, fsm::PeerFsmDelegate, raft::Peer, router::PeerTick};

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_raft_log_gc(&mut self) {
        if !self.fsm.peer().is_leader() {
            // `compact_cache_to` is called when apply, there is no need to call
            // `compact_to` here, snapshot generating has already been cancelled
            // when the role becomes follower.
            return;
        }
        self.schedule_tick(PeerTick::RaftLogGc);

        self.fsm.peer_mut().raft_log_gc_imp(self.store_ctx);

        if needs_evict_entry_cache(self.store_ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm
                .peer_mut()
                .entry_storage_mut()
                .evict_entry_cache(true);
            if !self.fsm.peer_mut().entry_storage().is_entry_cache_empty() {
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
    fn raft_log_gc_imp<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        // As leader, we would not keep caches for the peers that didn't response
        // heartbeat in the last few seconds. That happens probably because
        // another TiKV is down. In this case if we do not clean up the cache,
        // it may keep growing.
        let drop_cache_duration =
            store_ctx.cfg.raft_heartbeat_interval() + store_ctx.cfg.raft_entry_cache_life_time.0;
        let cache_alive_limit = std::time::Instant::now() - drop_cache_duration;

        // Leader will replicate the compact log command to followers,
        // If we use current replicated_index (like 10) as the compact index,
        // when we replicate this log, the newest replicated_index will be 11,
        // but we only compact the log to 10, not 11, at that time,
        // the first index is 10, and replicated_index is 11, with an extra log,
        // and we will do compact again with compact index 11, in cycles...
        // So we introduce a threshold, if replicated index - first index > threshold,
        // we will try to compact log.
        // raft log entries[..............................................]
        //                  ^                                       ^
        //                  |-----------------threshold------------ |
        //              first_index                         replicated_index
        // `alive_cache_idx` is the smallest `replicated_index` of healthy up nodes.
        // `alive_cache_idx` is only used to gc cache.
        let applied_idx = self.entry_storage().applied_index();
        let truncated_idx = self.entry_storage().truncated_index();
        let first_idx = self.entry_storage().first_index();
        let last_idx = self.entry_storage().last_index();

        let (mut replicated_idx, mut alive_cache_idx) = (last_idx, last_idx);
        for (peer_id, p) in self.raft_group().raft.prs().iter() {
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if let Some(last_heartbeat) = self.peer_heartbeats.get(peer_id) {
                if *last_heartbeat > cache_alive_limit {
                    if alive_cache_idx > p.matched && p.matched >= truncated_idx {
                        alive_cache_idx = p.matched;
                    } else if p.matched == 0 {
                        // the new peer is still applying snapshot, do not compact cache now
                        alive_cache_idx = 0;
                    }
                }
            }
        }

        // When an election happened or a new peer is added, replicated_idx can be 0.
        if replicated_idx > 0 {
            assert!(
                last_idx >= replicated_idx,
                "expect last index {} >= replicated index {}",
                last_idx,
                replicated_idx
            );
        }

        // leader may call `get_term()` on the latest replicated index, so compact
        // entries before `alive_cache_idx` instead of `alive_cache_idx + 1`.
        self.entry_storage_mut()
            .compact_entry_cache(std::cmp::min(alive_cache_idx, applied_idx + 1));

        let mut compact_idx = if (applied_idx > first_idx
            && applied_idx - first_idx >= store_ctx.cfg.raft_log_gc_count_limit())
            || (self.raft_log_size_hint >= store_ctx.cfg.raft_log_gc_size_limit().0)
        {
            std::cmp::max(first_idx + (last_idx - first_idx) / 2, replicated_idx)
        } else if replicated_idx < first_idx || last_idx - first_idx < 3 {
            return;
        } else if replicated_idx - first_idx < store_ctx.cfg.raft_log_gc_threshold
            && self.skip_gc_raft_log_ticks < store_ctx.cfg.raft_log_reserve_max_ticks
        {
            self.skip_gc_raft_log_ticks += 1;
            return;
        } else {
            replicated_idx
        };
        assert!(compact_idx >= first_idx);
        // Have no idea why subtract 1 here, but original code did this by magic.
        compact_idx -= 1;
        if compact_idx < first_idx {
            return;
        }

        // Create a compact log request and notify directly.
        let region_id = self.region().get_id();
        let peer = self.peer().clone();
        // TODO: move this into a function
        let term = self.raft_group().raft.raft_log.term(compact_idx).unwrap();

        let mut req = self.new_admin_request();
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::CompactLog);
        admin.mut_compact_log().set_compact_index(compact_idx);
        admin.mut_compact_log().set_compact_term(term);
        req.set_admin_request(admin);

        self.propose_command(store_ctx, req);

        self.skip_gc_raft_log_ticks = 0;
    }
}
