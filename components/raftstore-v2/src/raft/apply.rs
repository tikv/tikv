// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use kvproto::{raft_cmdpb::RaftCmdResponse, raft_serverpb::RegionLocalState};
use raftstore::store::fsm::apply::DEFAULT_APPLY_WB_SIZE;
use slog::Logger;

use super::Peer;
use crate::{
    fsm::ApplyResReporter,
    router::{ApplyRes, CmdResChannel, ExecResult},
    tablet::CachedTablet,
};

/// Apply applies all the committed commands to kv db.
pub struct Apply<EK: KvEngine, ER: RaftEngine, R> {
    pub(crate) store_id: u64,

    remote_tablet: CachedTablet<EK>,
    tablet: EK,
    write_batch: Option<EK::WriteBatch>,

    pub(crate) raft_engine: ER,
    pub(crate) tablet_factory: Arc<dyn TabletFactory<EK>>,

    callbacks: Vec<(Vec<CmdResChannel>, RaftCmdResponse)>,

    applied_index: u64,
    applied_term: u64,

    region_state: RegionLocalState,
    state_changed: bool,

    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they
    /// are exactly the same peer.
    pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,

    exec_results: VecDeque<ExecResult>,

    res_reporter: R,
    pub(crate) logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine, R> Apply<EK, ER, R> {
    #[inline]
    pub fn new(
        store_id: u64,
        region_state: RegionLocalState,
        res_reporter: R,
        mut remote_tablet: CachedTablet<EK>,
        raft_engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
        logger: Logger,
    ) -> Self {
        Apply {
            store_id,
            tablet: remote_tablet.latest().unwrap().clone(),
            remote_tablet,
            write_batch: None,
            callbacks: vec![],
            applied_index: 0,
            applied_term: 0,
            region_state,
            state_changed: false,
            pending_create_peers,
            raft_engine,
            tablet_factory,
            exec_results: VecDeque::new(),
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
    pub fn set_region_state(&mut self, region_state: RegionLocalState) {
        self.region_state = region_state;
    }

    #[inline]
    pub fn reset_state_changed(&mut self) -> bool {
        std::mem::take(&mut self.state_changed)
    }

    #[inline]
    pub fn push_exec_result(&mut self, exec_result: ExecResult) {
        self.exec_results.push_back(exec_result);
    }

    #[inline]
    pub fn take_exec_result(&mut self) -> VecDeque<ExecResult> {
        std::mem::take(&mut self.exec_results)
    }

    #[inline]
    pub fn tablet(&mut self) -> Option<EK> {
        self.remote_tablet.latest().cloned()
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

    #[inline]
    pub fn pending_create_peers(&self) -> &Arc<Mutex<HashMap<u64, (u64, bool)>>> {
        &self.pending_create_peers
    }

    #[inline]
    pub fn raft_engine(&self) -> &ER {
        &self.raft_engine
    }
}
