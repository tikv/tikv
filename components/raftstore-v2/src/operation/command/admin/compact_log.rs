// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! `CompactLog` command.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest};
use protobuf::Message;
use raftstore::Result;
use slog::{debug, error, info};
use tikv_util::box_err;

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{write_initial_states, Apply, Peer, Storage},
    router::{ApplyRes, PeerMsg, StoreMsg},
    worker::RaftLogGcTask,
};

#[derive(Debug)]
pub struct CompactLogResult {
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
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "command" => ?compact_log
            );
            // old format compact log command, safe to ignore.
            return Err(box_err!(
                "command format is outdated, please upgrade leader"
            ));
        }

        let data = req.write_to_bytes().unwrap();
        self.propose_with_ctx(store_ctx, data, vec![])
    }

    pub fn on_ready_compact_log<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: CompactLogResult,
    ) {
        let first_index = self.entry_storage().first_index();
        if res.compact_index <= first_index {
            debug!(
                self.logger,
                "compact index <= first index, no need to compact";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "compact_index" => res.compact_index,
                "first_index" => first_index,
            );
            return;
        }
        // TODO: check is_merging
        // compact failure is safe to be omitted, no need to assert.
        let mut entry_storage = self.entry_storage_mut();

        if res.compact_index <= entry_storage.truncated_index()
            || res.compact_index > entry_storage.applied_index()
        {
            return;
        }
        // we don't actually delete the logs now, we add an async task to do it.
        entry_storage
            .apply_state_mut()
            .mut_truncated_state()
            .set_index(res.compact_index);
        entry_storage
            .apply_state_mut()
            .mut_truncated_state()
            .set_term(res.compact_term);

        // TODO: check entry_cache_warmup_state
        self.schedule_raft_log_gc(store_ctx, res.compact_index);
        self.entry_storage_mut()
            .compact_entry_cache(res.compact_index);
        self.storage_mut()
            .cancel_generating_snap(Some(res.compact_index));
    }

    pub fn schedule_raft_log_gc<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        compact_index: u64,
    ) {
        let task = RaftLogGcTask::gc(self.region_id(), self.last_compacted_index, compact_index);
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
        } else {
            self.last_compacted_index = compact_index;

            let total_cnt = self.storage().apply_state().get_applied_index()
                - self.storage().entry_storage().first_index();
            // the size of current CompactLog command can be ignored.
            let remain_cnt = self.storage().apply_state().get_applied_index() - compact_index - 1;
            self.raft_log_size_hint = self.raft_log_size_hint * remain_cnt / total_cnt;
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_compact_log(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        Ok((
            AdminResponse::default(),
            AdminCmdResult::CompactLog(CompactLogResult {
                compact_index: req.get_compact_log().get_compact_index(),
                compact_term: req.get_compact_log().get_compact_term(),
            }),
        ))
    }
}
