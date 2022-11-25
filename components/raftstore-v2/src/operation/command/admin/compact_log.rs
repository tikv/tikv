// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! `CompactLog` command.

use std::collections::VecDeque;

use crossbeam::channel::{SendError, TrySendError};
use engine_traits::{
    Checkpointer, DeleteStrategy, KvEngine, OpenOptions, RaftEngine, RaftLogBatch, Range,
    CF_DEFAULT, SPLIT_PREFIX,
};
use fail::fail_point;
use keys::enc_end_key;
use kvproto::{
    metapb::{self, Region, RegionEpoch},
    raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest, SplitRequest},
    raft_serverpb::RegionLocalState,
};
use protobuf::Message;
use raft::RawNode;
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{
        fsm::apply::validate_batch_split,
        metrics::PEER_ADMIN_CMD_COUNTER,
        util::{self, KeysInfoFormatter},
        PeerPessimisticLocks, PeerStat, ProposalContext, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use slog::{debug, error, info, warn, Logger};
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
    compact_to: u64,
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_ready_compact_log<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        res: CompactLogResult,
    ) {
        // TODO: check entry_cache_warmup_state
        self.schedule_raft_log_gc(store_ctx, res.compact_to);
        self.entry_storage_mut().compact_entry_cache(res.compact_to);
        // TODO: cancel_generating_snap
    }

    pub fn schedule_raft_log_gc<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        compact_to: u64,
    ) {
        let task = RaftLogGcTask::gc(self.region_id(), self.last_compacted_index, compact_to);
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
            self.last_compacted_index = compact_to;

            let total_cnt = self.storage().apply_state().get_applied_index()
                - self.storage().entry_storage().first_index();
            // the size of current CompactLog command can be ignored.
            let remain_cnt = self.storage().apply_state().get_applied_index() - compact_to - 1;
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
        let compact_index = req.get_compact_log().get_compact_index();
        let resp = AdminResponse::default();
        // TODO: check before propose?
        Ok((
            resp,
            AdminCmdResult::CompactLog(CompactLogResult {
                compact_to: compact_index,
            }),
        ))
    }
}
