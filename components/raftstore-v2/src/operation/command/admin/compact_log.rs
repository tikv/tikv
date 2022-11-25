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
use slog::{error, info, warn, Logger};
use tikv_util::box_err;

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{write_initial_states, Apply, Peer, Storage},
    router::{ApplyRes, PeerMsg, StoreMsg},
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
        self.schedule_raftlog_gc(store_ctx, res.compact_to);
        self.entry_storage_mut().compact_entry_cache(res.compact_to);
        // TODO: cancel_generating_snap
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_compact_log(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        unimplemented!()
    }
}
