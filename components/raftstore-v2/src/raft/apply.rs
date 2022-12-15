// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use engine_traits::{CachedTablet, KvEngine, TabletRegistry, WriteBatch};
use kvproto::{metapb, raft_cmdpb::RaftCmdResponse, raft_serverpb::RegionLocalState};
use raftstore::store::{fsm::apply::DEFAULT_APPLY_WB_SIZE, ReadTask};
use slog::Logger;
use tikv_util::worker::Scheduler;

use crate::{operation::AdminCmdResult, router::CmdResChannel};

/// Apply applies all the committed commands to kv db.
pub struct Apply<EK: KvEngine, R> {
    peer: metapb::Peer,
    /// publish the update of the tablet
    remote_tablet: CachedTablet<EK>,
    tablet: EK,
    pub write_batch: Option<EK::WriteBatch>,
    /// A buffer for encoding key.
    pub key_buffer: Vec<u8>,

    tablet_registry: TabletRegistry<EK>,

    callbacks: Vec<(Vec<CmdResChannel>, RaftCmdResponse)>,

    /// A flag indicates whether the peer is destroyed by applying admin
    /// command.
    tombstone: bool,
    applied_index: u64,
    applied_term: u64,
    admin_cmd_result: Vec<AdminCmdResult>,

    region_state: RegionLocalState,

    res_reporter: R,
    read_scheduler: Scheduler<ReadTask<EK>>,
    pub(crate) logger: Logger,
}

impl<EK: KvEngine, R> Apply<EK, R> {
    #[inline]
    pub fn new(
        peer: metapb::Peer,
        region_state: RegionLocalState,
        res_reporter: R,
        tablet_registry: TabletRegistry<EK>,
        read_scheduler: Scheduler<ReadTask<EK>>,
        logger: Logger,
    ) -> Self {
        let mut remote_tablet = tablet_registry
            .get(region_state.get_region().get_id())
            .unwrap();
        Apply {
            peer,
            tablet: remote_tablet.latest().unwrap().clone(),
            remote_tablet,
            write_batch: None,
            callbacks: vec![],
            tombstone: false,
            applied_index: 0,
            applied_term: 0,
            admin_cmd_result: vec![],
            region_state,
            tablet_registry,
            read_scheduler,
            key_buffer: vec![],
            res_reporter,
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
    pub fn tablet(&self) -> &EK {
        &self.tablet
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
}
