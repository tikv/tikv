// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{raft_cmdpb::RaftCmdResponse, raft_serverpb::RegionLocalState};
use raftstore::store::fsm::apply::DEFAULT_APPLY_WB_SIZE;
use slog::Logger;

use super::Peer;
use crate::{
    fsm::ApplyResReporter,
    router::{ApplyRes, CmdResChannel},
    tablet::CachedTablet,
};

/// Apply applies all the committed commands to kv db.
pub struct Apply<EK: KvEngine, R> {
    remote_tablet: CachedTablet<EK>,
    tablet: EK,
    write_batch: Option<EK::WriteBatch>,

    callbacks: Vec<(Vec<CmdResChannel>, RaftCmdResponse)>,

    applied_index: u64,
    applied_term: u64,

    region_state: RegionLocalState,
    state_changed: bool,

    res_reporter: R,
    pub(crate) logger: Logger,
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn new(
        region_state: RegionLocalState,
        res_reporter: R,
        mut remote_tablet: CachedTablet<EK>,
        logger: Logger,
    ) -> Self {
        Apply {
            tablet: remote_tablet.latest().unwrap().clone(),
            remote_tablet,
            write_batch: None,
            callbacks: vec![],
            applied_index: 0,
            applied_term: 0,
            region_state,
            state_changed: false,
            res_reporter,
            logger,
        }
    }

    #[inline]
    pub fn res_reporter(&self) -> &R {
        &self.res_reporter
    }

    #[inline]
    pub fn callbacks_mut(&mut self) -> &mut Vec<(Vec<CmdResChannel>, RaftCmdResponse)> {
        &mut self.callbacks
    }

    #[inline]
    pub fn write_batch_mut(&mut self) -> &mut Option<EK::WriteBatch> {
        &mut self.write_batch
    }

    #[inline]
    pub fn write_batch_or_default(&mut self) -> &mut EK::WriteBatch {
        if self.write_batch.is_none() {
            self.write_batch = Some(self.tablet.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE));
        }
        self.write_batch.as_mut().unwrap()
    }

    #[inline]
    pub fn set_apply_progress(&mut self, index: u64, term: u64) {
        self.applied_index = index;
        self.applied_term = term;
    }

    #[inline]
    pub fn apply_progress(&self) -> (u64, u64) {
        (self.applied_index, self.applied_term)
    }

    #[inline]
    pub fn region_state(&self) -> &RegionLocalState {
        &self.region_state
    }

    #[inline]
    pub fn reset_state_changed(&mut self) -> bool {
        std::mem::take(&mut self.state_changed)
    }

    /// Publish the tablet so that it can be used by read worker.
    ///
    /// Note, during split/merge, lease is expired explicitly and read is
    /// forbidden. So publishing it immediately is OK.
    #[inline]
    pub fn publish_tablet(&mut self, tablet: EK) {
        self.remote_tablet.set(tablet.clone());
        self.tablet = tablet;
    }
}
