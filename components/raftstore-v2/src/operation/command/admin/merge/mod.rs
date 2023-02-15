// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

pub mod prepare;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    raft_cmdpb::{AdminCmdType, RaftCmdRequest},
    raft_serverpb::{MergeState, PeerState, RegionLocalState},
};
use raft::{ProgressState, INVALID_INDEX};
use raftstore::Result;
use slog::{info, warn, Logger};
use tikv_util::box_err;

use crate::raft::Peer;

pub enum PrepareStatus {
    /// When a fence <idx, cmd> is present, we (1) delay the PrepareMerge
    /// command `cmd` until all writes before `idx` are applied (2) reject all
    /// in-coming write proposals.
    /// Before proposing `PrepareMerge`, we first serialize and propose the lock
    /// table. Locks marked as deleted (but not removed yet) will be
    /// serialized as normal locks.
    /// Thanks to the fence, we can ensure at the time of lock transfer, locks
    /// are either removed (when applying logs) or won't be removed before
    /// merge (the proposals to remove them are rejected).
    ///
    /// The request can be `None` because we needs to take it out to redo the
    /// propose. In the meantime the fence is needed to bypass the check.
    WaitForFence(u64, Option<RaftCmdRequest>),
    /// In this state, all write proposals except for `RollbackMerge` will be
    /// rejected.
    Applied(MergeState),
}

#[derive(Default)]
pub struct MergeContext {
    pub prepare_status: Option<PrepareStatus>,
}

impl MergeContext {
    #[inline]
    pub fn from_region_state(logger: &Logger, state: &RegionLocalState) -> Option<Self> {
        if state.get_state() == PeerState::Merging {
            info!(logger, "region is merging"; "region_state" => ?state);
            let mut ctx = Self::default();
            ctx.prepare_status = Some(PrepareStatus::Applied(state.get_merge_state().clone()));
            Some(ctx)
        } else {
            None
        }
    }

    #[inline]
    pub fn maybe_redo_pending_prepare(&mut self, applied: u64) -> Option<RaftCmdRequest> {
        if let Some(PrepareStatus::WaitForFence(f, cmd)) = self.prepare_status.as_mut()
            && applied >= *f
        {
            // The status will be updated during processing the proposal.
            return cmd.take();
        }
        None
    }

    #[inline]
    pub fn should_block_write(&self, admin_type: Option<AdminCmdType>) -> bool {
        matches!(self.prepare_status, Some(PrepareStatus::Applied(_)))
            && admin_type != Some(AdminCmdType::RollbackMerge)
            || matches!(self.prepare_status, Some(PrepareStatus::WaitForFence(..)))
                && admin_type != Some(AdminCmdType::PrepareMerge)
    }

    #[inline]
    pub fn has_applied_prepare_merge(&self) -> bool {
        matches!(self.prepare_status, Some(PrepareStatus::Applied(_)))
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Returns (minimal matched, minimal committed)
    pub fn calculate_min_progress(&self) -> Result<(u64, u64)> {
        let (mut min_m, mut min_c) = (None, None);
        if let Some(progress) = self.raft_group().status().progress {
            for (id, pr) in progress.iter() {
                // Reject merge if there is any pending request snapshot,
                // because a target region may merge a source region which is in
                // an invalid state.
                if pr.state == ProgressState::Snapshot
                    || pr.pending_request_snapshot != INVALID_INDEX
                {
                    return Err(box_err!(
                        "there is a pending snapshot peer {} [{:?}], skip merge",
                        id,
                        pr
                    ));
                }
                if min_m.unwrap_or(u64::MAX) > pr.matched {
                    min_m = Some(pr.matched);
                }
                if min_c.unwrap_or(u64::MAX) > pr.committed_index {
                    min_c = Some(pr.committed_index);
                }
            }
        }
        let (mut min_m, min_c) = (min_m.unwrap_or(0), min_c.unwrap_or(0));
        if min_m < min_c {
            warn!(
                self.logger,
                "min_matched < min_committed, raft progress is inaccurate";
                "min_matched" => min_m,
                "min_committed" => min_c,
            );
            // Reset `min_matched` to `min_committed`, since the raft log at `min_committed`
            // is known to be committed in all peers, all of the peers should also have
            // replicated it
            min_m = min_c;
        }
        Ok((min_m, min_c))
    }
}
