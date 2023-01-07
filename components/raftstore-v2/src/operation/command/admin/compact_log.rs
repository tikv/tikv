// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains processing logic of the following:
//!
//! # `CompactLog` and `EntryCacheEvict` ticks
//!
//! On region leader, periodically compacts useless Raft logs from the
//! underlying log engine, and evicts logs from entry cache if it reaches memory
//! limit.
//!
//! # `CompactLog` command
//!
//! Updates truncated index, and compacts logs if the corresponding changes have
//! been persisted in kvdb.

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, RaftCmdRequest};
use protobuf::Message;
use raftstore::{
    store::{fsm::new_admin_request, needs_evict_entry_cache, Transport, WriteTask},
    Result,
};
use slog::{debug, error, info};
use tikv_util::{box_err, Either};

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::{CmdResChannel, PeerTick},
    worker::tablet_gc,
};

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_compact_log_tick(&mut self, force: bool) {
        if !self.fsm.peer().is_leader() {
            // `compact_cache_to` is called when apply, there is no need to call
            // `compact_to` here, snapshot generating has already been cancelled
            // when the role becomes follower.
            return;
        }
        self.schedule_tick(PeerTick::CompactLog);

        self.fsm
            .peer_mut()
            .maybe_propose_compact_log(self.store_ctx, force);

        self.on_entry_cache_evict();
    }

    pub fn on_entry_cache_evict(&mut self) {
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
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    // Mirrors v1::on_raft_gc_log_tick.
    fn maybe_propose_compact_log<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        force: bool,
    ) {
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
            if self.peer_heartbeat_is_fresh(*peer_id, &cache_alive_limit) {
                if alive_cache_idx > p.matched && p.matched >= truncated_idx {
                    alive_cache_idx = p.matched;
                } else if p.matched == 0 {
                    // the new peer is still applying snapshot, do not compact cache now
                    alive_cache_idx = 0;
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

        let mut compact_idx = if force && replicated_idx > first_idx {
            replicated_idx
        } else if applied_idx > first_idx
            && applied_idx - first_idx >= store_ctx.cfg.raft_log_gc_count_limit()
            || self.approximate_raft_log_size() >= store_ctx.cfg.raft_log_gc_size_limit().0
        {
            std::cmp::max(first_idx + (last_idx - first_idx) / 2, replicated_idx)
        } else if replicated_idx < first_idx
            || last_idx - first_idx < 3
            || replicated_idx - first_idx < store_ctx.cfg.raft_log_gc_threshold
                && self.maybe_skip_compact_log(store_ctx.cfg.raft_log_reserve_max_ticks)
        {
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
        // TODO: move this into a function
        let term = self.raft_group().raft.raft_log.term(compact_idx).unwrap();

        let mut req = new_admin_request(self.region_id(), self.peer().clone());
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::CompactLog);
        admin.mut_compact_log().set_compact_index(compact_idx);
        admin.mut_compact_log().set_compact_term(term);
        req.set_admin_request(admin);

        let (ch, _) = CmdResChannel::pair();
        self.on_admin_command(store_ctx, req, ch);

        self.reset_skip_compact_log_ticks();
    }
}

#[derive(Debug)]
pub struct CompactLogResult {
    index: u64,
    compact_index: u64,
    compact_term: u64,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_compact_log<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        let compact_log = req.get_admin_request().get_compact_log();
        // TODO: add unit tests to cover all the message integrity checks.
        if compact_log.get_compact_term() == 0 {
            info!(
                self.logger,
                "compact term missing, skip";
                "command" => ?compact_log
            );
            // old format compact log command, safe to ignore.
            return Err(box_err!(
                "command format is outdated, please upgrade leader"
            ));
        }

        let data = req.write_to_bytes().unwrap();
        self.propose(store_ctx, data)
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_compact_log(
        &mut self,
        req: &AdminRequest,
        index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        Ok((
            AdminResponse::default(),
            AdminCmdResult::CompactLog(CompactLogResult {
                index,
                compact_index: req.get_compact_log().get_compact_index(),
                compact_term: req.get_compact_log().get_compact_term(),
            }),
        ))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_apply_res_compact_log<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: CompactLogResult,
    ) {
        let first_index = self.entry_storage().first_index();
        if res.compact_index <= first_index {
            debug!(
                self.logger,
                "compact index <= first index, no need to compact";
                "compact_index" => res.compact_index,
                "first_index" => first_index,
            );
            return;
        }
        // TODO: check is_merging
        // TODO: check entry_cache_warmup_state
        self.entry_storage_mut()
            .compact_entry_cache(res.compact_index);
        self.storage_mut()
            .cancel_generating_snap_due_to_compacted(res.compact_index);

        let truncated_state = self
            .entry_storage_mut()
            .apply_state_mut()
            .mut_truncated_state();
        let old_truncated = truncated_state.get_index();
        truncated_state.set_index(res.compact_index);
        truncated_state.set_term(res.compact_term);

        let region_id = self.region_id();
        // TODO: get around this clone.
        let apply_state = self.entry_storage().apply_state().clone();
        self.state_changes_mut()
            .put_apply_state(region_id, res.index, &apply_state)
            .unwrap();
        self.set_has_extra_write();

        self.maybe_compact_log_from_engine(store_ctx, Either::Right(old_truncated));
    }

    #[inline]
    pub fn on_advance_persisted_apply_index<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        old_persisted: u64,
        task: &mut WriteTask<EK, ER>,
    ) {
        let new_persisted = self.storage().apply_trace().persisted_apply_index();
        if old_persisted < new_persisted {
            let region_id = self.region_id();
            // TODO: batch it.
            if let Err(e) = store_ctx.engine.delete_all_but_one_states_before(
                region_id,
                new_persisted,
                self.state_changes_mut(),
            ) {
                error!(self.logger, "failed to delete raft states"; "err" => ?e);
            } else {
                self.set_has_extra_write();
            }
            self.maybe_compact_log_from_engine(store_ctx, Either::Left(old_persisted));
            if self.remove_tombstone_tablets_before(new_persisted) {
                let sched = store_ctx.schedulers.tablet_gc.clone();
                task.persisted_cbs.push(Box::new(move || {
                    let _ = sched.schedule(tablet_gc::Task::destroy(region_id, new_persisted));
                }))
            }
        }
    }

    pub fn maybe_compact_log_from_engine<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        old_index: Either<u64, u64>,
    ) {
        let truncated = self.entry_storage().truncated_index();
        let persisted = self.storage().apply_trace().persisted_apply_index();
        match old_index {
            Either::Left(old_persisted) if old_persisted >= truncated => return,
            Either::Right(old_truncated) if old_truncated >= persisted => return,
            _ => {}
        }
        let compact_index = std::cmp::min(truncated, persisted);
        // Raft Engine doesn't care about first index.
        if let Err(e) =
            store_ctx
                .engine
                .gc(self.region_id(), 0, compact_index, self.state_changes_mut())
        {
            error!(self.logger, "failed to compact raft logs"; "err" => ?e);
        } else {
            self.set_has_extra_write();
            let applied = self.storage().apply_state().get_applied_index();
            let total_cnt = applied - self.storage().entry_storage().first_index() + 1;
            let remain_cnt = applied - compact_index;
            self.update_approximate_raft_log_size(|s| s * remain_cnt / total_cnt);
        }
    }
}
