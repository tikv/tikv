// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

pub mod commit;
pub mod prepare;
pub mod rollback;

use std::path::PathBuf;

use commit::CatchUpLogs;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry};
use kvproto::{
    raft_cmdpb::RaftCmdRequest,
    raft_serverpb::{MergeState, PeerState, RegionLocalState},
};
use prepare::PrepareStatus;
use raft::{ProgressState, INVALID_INDEX};
use raftstore::Result;
use slog::{info, warn, Logger};
use tikv_util::box_err;

use crate::raft::Peer;

pub const MERGE_SOURCE_PREFIX: &str = "merge-source";

// `index` is the commit index of `PrepareMergeRequest`, `commit` field of
// `CommitMergeRequest`.
pub fn merge_source_path<EK>(
    registry: &TabletRegistry<EK>,
    source_region_id: u64,
    index: u64,
) -> PathBuf {
    let tablet_name = registry.tablet_name(MERGE_SOURCE_PREFIX, source_region_id, index);
    registry.tablet_root().join(tablet_name)
}

/// This context is only used at source region.
#[derive(Default)]
pub struct MergeContext {
    prepare_status: Option<PrepareStatus>,
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
    pub fn maybe_take_pending_prepare(&mut self, applied: u64) -> Option<RaftCmdRequest> {
        if let Some(PrepareStatus::WaitForFence { fence, req, .. }) = self.prepare_status.as_mut()
            && applied >= *fence
        {
            // The status will be updated during processing the proposal.
            return req.take();
        }
        None
    }

    #[inline]
    pub fn max_compact_log_index(&self) -> Option<u64> {
        if let Some(PrepareStatus::WaitForFence { ctx, .. }) = self.prepare_status.as_ref() {
            Some(ctx.min_matched)
        } else {
            None
        }
    }

    #[inline]
    pub fn prepare_merge_index(&self) -> Option<u64> {
        if let Some(PrepareStatus::Applied(state)) = self.prepare_status.as_ref() {
            Some(state.get_commit())
        } else {
            None
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn update_merge_progress_on_became_follower(&mut self) {
        if let Some(MergeContext {
            prepare_status: Some(status),
        }) = self.merge_context()
            && matches!(
                status,
                PrepareStatus::WaitForTrimStatus { .. } | PrepareStatus::WaitForFence { .. }
            )
        {
            self.take_merge_context();
            self.proposal_control_mut().set_pending_prepare_merge(false);
        }
    }

    /// Returns (minimal matched, minimal committed)
    fn calculate_min_progress(&self) -> Result<(u64, u64)> {
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

    #[inline]
    fn applied_merge_state(&self) -> Option<&MergeState> {
        self.merge_context().and_then(|ctx| {
            if let Some(PrepareStatus::Applied(state)) = ctx.prepare_status.as_ref() {
                Some(state)
            } else {
                None
            }
        })
    }
}
