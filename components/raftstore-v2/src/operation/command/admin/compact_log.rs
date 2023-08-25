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

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, AdminResponse, RaftCmdRequest};
use protobuf::Message;
use raftstore::{
    store::{
        entry_storage::MAX_WARMED_UP_CACHE_KEEP_TIME, fsm::new_admin_request,
        metrics::REGION_MAX_LOG_LAG, needs_evict_entry_cache, Transport, WriteTask,
        RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use slog::{debug, error, info};
use tikv_util::{box_err, log::SlogFormat};

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{Apply, Peer},
    router::{CmdResChannel, PeerTick},
    worker::tablet,
};

#[derive(Debug)]
pub struct CompactLogContext {
    skipped_ticks: usize,
    approximate_log_size: u64,
    last_applying_index: u64,
    /// The index of last compacted raft log.
    last_compacted_idx: u64,
    /// Tombstone tablets can only be destroyed when the tablet that replaces it
    /// is persisted. This is a list of tablet index that awaits to be
    /// persisted. When persisted_apply is advanced, we need to notify tablet
    /// worker to destroy them.
    tombstone_tablets_wait_index: Vec<u64>,
    /// Sometimes a tombstone tablet can be registered after tablet index is
    /// advanced. We should not consider it as an active tablet otherwise it
    /// might block peer destroy progress.
    persisted_tablet_index: Arc<AtomicU64>,
}

impl CompactLogContext {
    pub fn new(last_applying_index: u64, persisted_applied: u64) -> CompactLogContext {
        CompactLogContext {
            skipped_ticks: 0,
            approximate_log_size: 0,
            last_applying_index,
            last_compacted_idx: 0,
            tombstone_tablets_wait_index: vec![],
            persisted_tablet_index: AtomicU64::new(persisted_applied).into(),
        }
    }

    #[inline]
    pub fn maybe_skip_compact_log(&mut self, max_skip_ticks: usize) -> bool {
        if self.skipped_ticks < max_skip_ticks {
            self.skipped_ticks += 1;
            true
        } else {
            false
        }
    }

    pub fn add_log_size(&mut self, size: u64) {
        self.approximate_log_size += size;
    }

    pub fn set_last_applying_index(&mut self, index: u64) {
        self.last_applying_index = index;
    }

    #[inline]
    pub fn last_applying_index(&self) -> u64 {
        self.last_applying_index
    }

    pub fn set_last_compacted_idx(&mut self, index: u64) {
        self.last_compacted_idx = index;
    }

    pub fn last_compacted_idx(&self) -> u64 {
        self.last_compacted_idx
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_compact_log_tick(&mut self, force: bool) {
        // Might read raft logs.
        debug_assert!(self.fsm.peer().serving());
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
    fn maybe_propose_compact_log<T: Transport>(
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
            REGION_MAX_LOG_LAG.observe((last_idx - replicated_idx) as f64);
        }

        // leader may call `get_term()` on the latest replicated index, so compact
        // entries before `alive_cache_idx` instead of `alive_cache_idx + 1`.
        self.entry_storage_mut()
            .compact_entry_cache(std::cmp::min(alive_cache_idx, applied_idx + 1));

        let mut compact_idx = if force && replicated_idx > first_idx {
            replicated_idx
        } else if applied_idx > first_idx
            && applied_idx - first_idx >= store_ctx.cfg.raft_log_gc_count_limit()
            || self.compact_log_context().approximate_log_size
                >= store_ctx.cfg.raft_log_gc_size_limit().0
        {
            std::cmp::max(first_idx + (last_idx - first_idx) / 2, replicated_idx)
        } else if replicated_idx < first_idx || last_idx - first_idx < 3 {
            store_ctx.raft_metrics.raft_log_gc_skipped.reserve_log.inc();
            return;
        } else if replicated_idx - first_idx < store_ctx.cfg.raft_log_gc_threshold
            && self
                .compact_log_context_mut()
                .maybe_skip_compact_log(store_ctx.cfg.raft_log_reserve_max_ticks)
        {
            store_ctx
                .raft_metrics
                .raft_log_gc_skipped
                .threshold_limit
                .inc();
            return;
        } else {
            replicated_idx
        };
        assert!(compact_idx >= first_idx);
        // Have no idea why subtract 1 here, but original code did this by magic.
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
            store_ctx
                .raft_metrics
                .raft_log_gc_skipped
                .compact_idx_too_small
                .inc();
            return;
        }

        // Create a compact log request and notify directly.
        let term = self.index_term(compact_idx);
        let mut req = new_admin_request(self.region_id(), self.peer().clone());
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::CompactLog);
        admin.mut_compact_log().set_compact_index(compact_idx);
        admin.mut_compact_log().set_compact_term(term);
        req.set_admin_request(admin);

        let (ch, _) = CmdResChannel::pair();
        self.on_admin_command(store_ctx, req, ch);

        self.compact_log_context_mut().skipped_ticks = 0;
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
    #[inline]
    pub fn record_tombstone_tablet<T>(
        &mut self,
        ctx: &StoreContext<EK, ER, T>,
        old_tablet: EK,
        new_tablet_index: u64,
    ) {
        info!(
            self.logger,
            "record tombstone tablet";
            "prev_tablet_path" => old_tablet.path(),
            "new_tablet_index" => new_tablet_index
        );
        let compact_log_context = self.compact_log_context_mut();
        compact_log_context
            .tombstone_tablets_wait_index
            .push(new_tablet_index);
        let _ = ctx
            .schedulers
            .tablet
            .schedule(tablet::Task::prepare_destroy(
                old_tablet,
                self.region_id(),
                new_tablet_index,
            ));
    }

    #[inline]
    pub fn record_tombstone_tablet_path<T>(
        &mut self,
        ctx: &StoreContext<EK, ER, T>,
        old_tablet: PathBuf,
        new_tablet_index: u64,
    ) {
        info!(
            self.logger,
            "record tombstone tablet";
            "prev_tablet_path" => old_tablet.display(),
            "new_tablet_index" => new_tablet_index
        );
        let compact_log_context = self.compact_log_context_mut();
        compact_log_context
            .tombstone_tablets_wait_index
            .push(new_tablet_index);
        let _ = ctx
            .schedulers
            .tablet
            .schedule(tablet::Task::prepare_destroy_path(
                old_tablet,
                self.region_id(),
                new_tablet_index,
            ));
    }

    #[inline]
    pub fn record_tombstone_tablet_path_callback<T>(
        &mut self,
        ctx: &StoreContext<EK, ER, T>,
        old_tablet: PathBuf,
        new_tablet_index: u64,
        cb: impl FnOnce() + Send + 'static,
    ) {
        info!(
            self.logger,
            "record tombstone tablet";
            "prev_tablet_path" => old_tablet.display(),
            "new_tablet_index" => new_tablet_index
        );
        let compact_log_context = self.compact_log_context_mut();
        compact_log_context
            .tombstone_tablets_wait_index
            .push(new_tablet_index);
        let _ = ctx
            .schedulers
            .tablet
            .schedule(tablet::Task::prepare_destroy_path_callback(
                old_tablet,
                self.region_id(),
                new_tablet_index,
                cb,
            ));
    }

    /// Returns if there's any tombstone being removed. `persisted` state may
    /// not be persisted yet, caller is responsible for actually destroying the
    /// physical tablets afterwards.
    #[inline]
    pub fn remove_tombstone_tablets(&mut self, persisted: u64) -> bool {
        let compact_log_context = self.compact_log_context_mut();
        let removed = compact_log_context
            .tombstone_tablets_wait_index
            .iter()
            .take_while(|i| **i <= persisted)
            .count();
        if removed > 0 {
            compact_log_context
                .tombstone_tablets_wait_index
                .drain(..removed);
            true
        } else {
            false
        }
    }

    /// User can only increase this counter.
    #[inline]
    pub fn remember_persisted_tablet_index(&self) -> Arc<AtomicU64> {
        self.compact_log_context().persisted_tablet_index.clone()
    }

    /// Returns whether there's any tombstone tablet newer than persisted tablet
    /// index. They might still be referenced by inflight apply and cannot be
    /// destroyed.
    pub fn has_pending_tombstone_tablets(&self) -> bool {
        let ctx = self.compact_log_context();
        let persisted = ctx.persisted_tablet_index.load(Ordering::Relaxed);
        ctx.tombstone_tablets_wait_index
            .iter()
            .any(|i| *i > persisted)
    }

    #[inline]
    pub fn record_tombstone_tablet_for_destroy<T>(
        &mut self,
        ctx: &StoreContext<EK, ER, T>,
        task: &mut WriteTask<EK, ER>,
    ) {
        let applied_index = self.entry_storage().applied_index();
        self.remove_tombstone_tablets(applied_index);
        assert!(
            !self.has_pending_tombstone_tablets(),
            "{} all tombstone should be cleared before being destroyed.",
            SlogFormat(&self.logger)
        );
        let tablet = match self.tablet() {
            Some(tablet) => tablet.clone(),
            None => return,
        };
        let region_id = self.region_id();
        let sched = ctx.schedulers.tablet.clone();
        let _ = sched.schedule(tablet::Task::prepare_destroy(
            tablet,
            self.region_id(),
            applied_index,
        ));
        task.persisted_cbs.push(Box::new(move || {
            let _ = sched.schedule(tablet::Task::destroy(region_id, applied_index));
        }));
    }

    pub fn on_apply_res_compact_log<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        mut res: CompactLogResult,
    ) {
        let first_index = self.entry_storage().first_index();
        if let Some(i) = self.merge_context().and_then(|c| c.max_compact_log_index())
            && res.compact_index > i
        {
            info!(
                self.logger,
                "in merging mode, adjust compact index";
                "old_index" => res.compact_index,
                "new_index" => i,
            );
            res.compact_index = i;
        }
        if res.compact_index <= first_index {
            debug!(
                self.logger,
                "compact index <= first index, no need to compact";
                "compact_index" => res.compact_index,
                "first_index" => first_index,
            );
            return;
        }
        assert!(
            res.compact_index < self.compact_log_context().last_applying_index,
            "{}: {}, {}",
            SlogFormat(&self.logger),
            res.compact_index,
            self.compact_log_context().last_applying_index
        );

        // Since this peer may be warming up the entry cache, log compaction should be
        // temporarily skipped. Otherwise, the warmup task may fail.
        if let Some(state) = self.entry_storage_mut().entry_cache_warmup_state_mut() {
            if !state.check_stale(MAX_WARMED_UP_CACHE_KEEP_TIME) {
                return;
            }
        }

        self.entry_storage_mut()
            .compact_entry_cache(res.compact_index);
        self.storage_mut()
            .cancel_generating_snap_due_to_compacted(res.compact_index);

        let truncated_state = self
            .entry_storage_mut()
            .apply_state_mut()
            .mut_truncated_state();
        truncated_state.set_index(res.compact_index);
        truncated_state.set_term(res.compact_term);

        let region_id = self.region_id();
        // TODO: get around this clone.
        let apply_state = self.entry_storage().apply_state().clone();
        self.state_changes_mut()
            .put_apply_state(region_id, res.index, &apply_state)
            .unwrap();
        self.set_has_extra_write();

        // All logs < persisted_apply will be deleted.
        let prev_first_index = first_index;
        if prev_first_index < self.storage().apply_trace().persisted_apply_index()
            && let Some(index) = self.compact_log_index()
        {
            // Raft Engine doesn't care about first index.
            if let Err(e) = store_ctx
                .engine
                .gc(self.region_id(), 0, index, self.state_changes_mut())
            {
                error!(self.logger, "failed to compact raft logs"; "err" => ?e);
            }
            self.compact_log_context_mut().set_last_compacted_idx(index);
            // Extra write set right above.
        }

        let context = self.compact_log_context_mut();
        let applied = context.last_applying_index;
        let total_cnt = applied - prev_first_index;
        let remain_cnt = applied - res.compact_index;
        context.approximate_log_size =
            (context.approximate_log_size as f64 * (remain_cnt as f64 / total_cnt as f64)) as u64;
    }

    /// Called when apply index is persisted.
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
            // TODO: avoid allocation if there is nothing to delete.
            if let Err(e) = store_ctx.engine.delete_all_but_one_states_before(
                region_id,
                new_persisted,
                task.extra_write
                    .ensure_v2(|| self.entry_storage().raft_engine().log_batch(0)),
            ) {
                error!(self.logger, "failed to delete raft states"; "err" => ?e);
            }
            // If it's snapshot, logs are gc already.
            if !task.has_snapshot
                && old_persisted < self.entry_storage().truncated_index() + 1
                && let Some(index) = self.compact_log_index()
            {
                let batch = task.extra_write.ensure_v2(|| self.entry_storage().raft_engine().log_batch(0));
                // Raft Engine doesn't care about first index.
                if let Err(e) =
                store_ctx
                    .engine
                    .gc(self.region_id(), 0, index, batch)
                {
                    error!(self.logger, "failed to compact raft logs"; "err" => ?e);
                }
            }
            if self.remove_tombstone_tablets(new_persisted) {
                let sched = store_ctx.schedulers.tablet.clone();
                let counter = self.remember_persisted_tablet_index();
                if !task.has_snapshot {
                    task.persisted_cbs.push(Box::new(move || {
                        let _ = sched.schedule(tablet::Task::destroy(region_id, new_persisted));
                        // Writer guarantees no race between different callbacks.
                        counter.store(new_persisted, Ordering::Relaxed);
                    }));
                } else {
                    // In snapshot, the index is persisted, tablet can be destroyed directly.
                    let _ = sched.schedule(tablet::Task::destroy(region_id, new_persisted));
                    counter.store(new_persisted, Ordering::Relaxed);
                }
            }
        }
    }

    fn compact_log_index(&mut self) -> Option<u64> {
        let first_index = self.entry_storage().first_index();
        let persisted_applied = self.storage().apply_trace().persisted_apply_index();
        let compact_index = std::cmp::min(first_index, persisted_applied);
        if compact_index == RAFT_INIT_LOG_INDEX + 1 {
            // There is no logs at RAFT_INIT_LOG_INDEX, nothing to delete.
            return None;
        }
        assert!(
            compact_index <= self.raft_group().raft.raft_log.committed,
            "{}: compact_index={}, committed={}",
            SlogFormat(&self.logger),
            compact_index,
            self.raft_group().raft.raft_log.committed,
        );
        // TODO: make this debug when stable.
        info!(
            self.logger,
            "compact log";
            "index" => compact_index,
            "apply_trace" => ?self.storage().apply_trace(),
            "truncated" => ?self.entry_storage().apply_state()
        );
        Some(compact_index)
    }
}
