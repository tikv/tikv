// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, sync::Arc};

use engine_traits::{
    FlushState, KvEngine, PerfContextKind, TabletRegistry, WriteBatch, DATA_CFS_LEN,
};
use kvproto::{metapb, raft_cmdpb::RaftCmdResponse, raft_serverpb::RegionLocalState};
use raftstore::store::{
    fsm::{apply::DEFAULT_APPLY_WB_SIZE, ApplyMetrics},
    Config, ReadTask,
};
use slog::Logger;
use tikv_util::{log::SlogFormat, worker::Scheduler};

use crate::{
    operation::{AdminCmdResult, ApplyFlowControl, DataTrace},
    router::CmdResChannel,
};

/// Apply applies all the committed commands to kv db.
pub struct Apply<EK: KvEngine, R> {
    peer: metapb::Peer,
    tablet: EK,
    perf_context: EK::PerfContext,
    pub write_batch: Option<EK::WriteBatch>,
    /// A buffer for encoding key.
    pub key_buffer: Vec<u8>,

    tablet_registry: TabletRegistry<EK>,

    callbacks: Vec<(Vec<CmdResChannel>, RaftCmdResponse)>,

    flow_control: ApplyFlowControl,

    /// A flag indicates whether the peer is destroyed by applying admin
    /// command.
    tombstone: bool,
    applied_term: u64,
    // Apply progress is set after every command in case there is a flush. But it's
    // wrong to update flush_state immediately as a manual flush from other thread
    // can fetch the wrong apply index from flush_state.
    applied_index: u64,
    /// The largest index that have modified each column family.
    modifications: DataTrace,
    admin_cmd_result: Vec<AdminCmdResult>,
    flush_state: Arc<FlushState>,
    /// The flushed indexes of each column family before being restarted.
    ///
    /// If an apply index is less than the flushed index, the log can be
    /// skipped. `None` means logs should apply to all required column
    /// families.
    log_recovery: Option<Box<DataTrace>>,

    region_state: RegionLocalState,

    res_reporter: R,
    read_scheduler: Scheduler<ReadTask<EK>>,
    pub(crate) metrics: ApplyMetrics,
    pub(crate) logger: Logger,
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn new(
        cfg: &Config,
        peer: metapb::Peer,
        region_state: RegionLocalState,
        res_reporter: R,
        tablet_registry: TabletRegistry<EK>,
        read_scheduler: Scheduler<ReadTask<EK>>,
        flush_state: Arc<FlushState>,
        log_recovery: Option<Box<DataTrace>>,
        applied_term: u64,
        logger: Logger,
    ) -> Self {
        let mut remote_tablet = tablet_registry
            .get(region_state.get_region().get_id())
            .unwrap();
        assert_ne!(applied_term, 0, "{}", SlogFormat(&logger));
        let applied_index = flush_state.applied_index();
        assert_ne!(applied_index, 0, "{}", SlogFormat(&logger));
        let tablet = remote_tablet.latest().unwrap().clone();
        let perf_context = tablet.get_perf_context(cfg.perf_level, PerfContextKind::RaftstoreApply);
        Apply {
            peer,
            tablet,
            perf_context,
            write_batch: None,
            callbacks: vec![],
            flow_control: ApplyFlowControl::new(cfg),
            tombstone: false,
            applied_term,
            applied_index: flush_state.applied_index(),
            modifications: [0; DATA_CFS_LEN],
            admin_cmd_result: vec![],
            region_state,
            tablet_registry,
            read_scheduler,
            key_buffer: vec![],
            res_reporter,
            flush_state,
            log_recovery,
            metrics: ApplyMetrics::default(),
            logger,
        }
    }

    #[inline]
    pub fn tablet_registry(&self) -> &TabletRegistry<EK> {
        &self.tablet_registry
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
    pub fn ensure_write_buffer(&mut self) {
        if self.write_batch.is_some() {
            return;
        }
        self.write_batch = Some(self.tablet.write_batch_with_cap(DEFAULT_APPLY_WB_SIZE));
    }

    #[inline]
    pub fn set_apply_progress(&mut self, index: u64, term: u64) {
        self.applied_index = index;
        self.applied_term = term;
        if self.log_recovery.is_none() {
            return;
        }
        let log_recovery = self.log_recovery.as_ref().unwrap();
        if log_recovery.iter().all(|v| index >= *v) {
            self.log_recovery.take();
        }
    }

    #[inline]
    pub fn apply_progress(&self) -> (u64, u64) {
        (self.applied_index, self.applied_term)
    }

    #[inline]
    pub fn read_scheduler(&self) -> &Scheduler<ReadTask<EK>> {
        &self.read_scheduler
    }

    #[inline]
    pub fn region_state(&self) -> &RegionLocalState {
        &self.region_state
    }

    #[inline]
    pub fn region_state_mut(&mut self) -> &mut RegionLocalState {
        &mut self.region_state
    }

    /// The tablet can't be public yet, otherwise content of latest tablet
    /// doesn't matches its epoch in both readers and peer fsm.
    #[inline]
    pub fn set_tablet(&mut self, tablet: EK) {
        assert!(
            self.write_batch.as_ref().map_or(true, |wb| wb.is_empty()),
            "{} setting tablet while still have dirty write batch",
            SlogFormat(&self.logger)
        );
        self.write_batch.take();
        self.tablet = tablet;
    }

    #[inline]
    pub fn tablet(&self) -> &EK {
        &self.tablet
    }

    #[inline]
    pub fn perf_context(&mut self) -> &mut EK::PerfContext {
        &mut self.perf_context
    }

    #[inline]
    pub fn peer(&self) -> &metapb::Peer {
        &self.peer
    }

    #[inline]
    pub fn set_peer(&mut self, peer: metapb::Peer) {
        self.peer = peer;
    }

    #[inline]
    pub fn mark_tombstone(&mut self) {
        self.tombstone = true;
    }

    #[inline]
    pub fn tombstone(&self) -> bool {
        self.tombstone
    }

    #[inline]
    pub fn push_admin_result(&mut self, admin_result: AdminCmdResult) {
        self.admin_cmd_result.push(admin_result);
    }

    #[inline]
    pub fn take_admin_result(&mut self) -> Vec<AdminCmdResult> {
        mem::take(&mut self.admin_cmd_result)
    }

    #[inline]
    pub fn release_memory(&mut self) {
        mem::take(&mut self.key_buffer);
        if self.write_batch.as_ref().map_or(false, |wb| wb.is_empty()) {
            self.write_batch = None;
        }
    }

    #[inline]
    pub fn modifications_mut(&mut self) -> &mut DataTrace {
        &mut self.modifications
    }

    #[inline]
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.flush_state
    }

    #[inline]
    pub fn log_recovery(&self) -> &Option<Box<DataTrace>> {
        &self.log_recovery
    }

    #[inline]
    pub fn apply_flow_control_mut(&mut self) -> &mut ApplyFlowControl {
        &mut self.flow_control
    }

    pub fn apply_flow_control(&self) -> &ApplyFlowControl {
        &self.flow_control
    }
}
