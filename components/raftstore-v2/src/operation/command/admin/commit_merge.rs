// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    raft_cmdpb::{AdminCmdType, RaftCmdRequest},
    raft_serverpb::{MergeState, PeerState, RegionLocalState},
};
use slog::{info, Logger};

#[derive(Default)]
pub struct MergeContext {
    /// Used at source region ///
    /// When a fence <idx, cmd> is present, we (1) delay the PrepareMerge
    /// command `cmd` until all writes before `idx` are applied (2) reject all
    /// in-coming write proposals.
    /// Before proposing `PrepareMerge`, we first serialize and propose the lock
    /// table. Locks marked as deleted (but not removed yet) will be
    /// serialized as normal locks.
    /// Thanks to the fence, we can ensure at the time of lock transfer, locks
    /// are either removed (when applying logs) or won't be removed before
    /// merge (the proposals to remove them are rejected).
    pub prepare_fence: Option<u64>,
    /// Used when `prepare_fence` is present.
    pub pending_prepare: Option<RaftCmdRequest>,
    /// Whether a `PrepareMerge` command has been applied.
    /// When it is set, all write proposals except for `RollbackMerge` will be
    /// rejected.
    pub pending: Option<MergeState>,
}

impl MergeContext {
    #[inline]
    pub fn from_region_state(logger: &Logger, state: &RegionLocalState) -> Self {
        let mut ctx = Self::default();
        if state.get_state() == PeerState::Merging {
            info!(logger, "region is merging"; "region_state" => ?state);
            ctx.pending = Some(state.get_merge_state().clone());
        }
        ctx
    }

    #[inline]
    pub fn should_block_write(&self, admin_type: Option<AdminCmdType>) -> bool {
        self.pending.is_some() && admin_type != Some(AdminCmdType::RollbackMerge)
            || self.prepare_fence.is_some() && admin_type != Some(AdminCmdType::PrepareMerge)
    }

    #[inline]
    pub fn cancel_pending_prepare(&mut self) {
        self.prepare_fence.take();
        self.pending_prepare.take();
    }

    #[inline]
    pub fn maybe_take_pending_prepare(&mut self, applied: Option<u64>) -> Option<RaftCmdRequest> {
        if self
            .prepare_fence
            .map_or(true, |f| applied.map_or(false, |i| i >= f))
        {
            // The other fields should be released during processing the request.
            return self.pending_prepare.take();
        }
        None
    }
}
