// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains all structs related to apply batch system.
//!
//! After being started, each thread will have its own `ApplyPoller` and poll
//! using `ApplyContext`. For more information, see the documentation of
//! batch-system.

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use batch_system::{
    BasicMailbox, BatchRouter, BatchSystem, HandleResult, HandlerBuilder, PollHandler,
};
use engine_traits::{KvEngine, RaftEngine};
use raftstore::store::{
    fsm::{
        apply::{ControlFsm, ControlMsg},
        ApplyNotifier,
    },
    util::LatencyInspector,
    Config,
};
use slog::Logger;
use tikv_util::config::{Tracker, VersionTrack};

use crate::{
    fsm::{ApplyFsm, ApplyFsmDelegate},
    raft::{Apply, Peer},
    router::ApplyTask,
};

pub struct ApplyContext {
    cfg: Config,
}

impl ApplyContext {
    pub fn new(cfg: Config) -> Self {
        ApplyContext { cfg }
    }
}

pub struct ApplyPoller {
    apply_task_buf: Vec<ApplyTask>,
    pending_latency_inspect: Vec<LatencyInspector>,
    apply_ctx: ApplyContext,
    cfg_tracker: Tracker<Config>,
}

impl ApplyPoller {
    pub fn new(apply_ctx: ApplyContext, cfg_tracker: Tracker<Config>) -> ApplyPoller {
        Self {
            apply_task_buf: Vec::new(),
            pending_latency_inspect: Vec::new(),
            apply_ctx,
            cfg_tracker,
        }
    }

    /// Updates the internal buffer to match the latest configuration.
    fn apply_buf_capacity(&mut self) {
        let new_cap = self.messages_per_tick();
        tikv_util::set_vec_capacity(&mut self.apply_task_buf, new_cap);
    }

    #[inline]
    fn messages_per_tick(&self) -> usize {
        self.apply_ctx.cfg.messages_per_tick
    }
}

impl<EK> PollHandler<ApplyFsm<EK>, ControlFsm> for ApplyPoller
where
    EK: KvEngine,
{
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a batch_system::Config),
    {
        let cfg = self.cfg_tracker.any_new().map(|c| c.clone());
        if let Some(cfg) = cfg {
            let last_messages_per_tick = self.messages_per_tick();
            self.apply_ctx.cfg = cfg;
            if self.apply_ctx.cfg.messages_per_tick != last_messages_per_tick {
                self.apply_buf_capacity();
            }
            update_cfg(&self.apply_ctx.cfg.apply_batch_system);
        }
    }

    fn handle_control(&mut self, control: &mut ControlFsm) -> Option<usize> {
        control.handle_messages(&mut self.pending_latency_inspect);
        for inspector in self.pending_latency_inspect.drain(..) {
            // TODO: support apply duration.
            inspector.finish();
        }
        Some(0)
    }

    fn handle_normal(
        &mut self,
        normal: &mut impl DerefMut<Target = ApplyFsm<EK>>,
    ) -> batch_system::HandleResult {
        let received_cnt = normal.recv(&mut self.apply_task_buf);
        let handle_result = if received_cnt == self.messages_per_tick() {
            HandleResult::KeepProcessing
        } else {
            HandleResult::stop_at(0, false)
        };
        let mut delegate = ApplyFsmDelegate::new(normal, &mut self.apply_ctx);
        delegate.handle_msgs(&mut self.apply_task_buf);
        handle_result
    }

    fn end(&mut self, batch: &mut [Option<impl DerefMut<Target = ApplyFsm<EK>>>]) {
        // TODO: support memory trace
    }
}

pub struct ApplyPollerBuilder {
    cfg: Arc<VersionTrack<Config>>,
}

impl ApplyPollerBuilder {
    pub fn new(cfg: Arc<VersionTrack<Config>>) -> Self {
        Self { cfg }
    }
}

impl<EK: KvEngine> HandlerBuilder<ApplyFsm<EK>, ControlFsm> for ApplyPollerBuilder {
    type Handler = ApplyPoller;

    fn build(&mut self, priority: batch_system::Priority) -> Self::Handler {
        let apply_ctx = ApplyContext::new(self.cfg.value().clone());
        let cfg_tracker = self.cfg.clone().tracker("apply".to_string());
        ApplyPoller::new(apply_ctx, cfg_tracker)
    }
}

/// Batch system for applying logs pipeline.
pub struct ApplySystem<EK: KvEngine> {
    system: BatchSystem<ApplyFsm<EK>, ControlFsm>,
}

impl<EK: KvEngine> Deref for ApplySystem<EK> {
    type Target = BatchSystem<ApplyFsm<EK>, ControlFsm>;

    fn deref(&self) -> &BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &self.system
    }
}

impl<EK: KvEngine> DerefMut for ApplySystem<EK> {
    fn deref_mut(&mut self) -> &mut BatchSystem<ApplyFsm<EK>, ControlFsm> {
        &mut self.system
    }
}

impl<EK: KvEngine> ApplySystem<EK> {
    pub fn schedule_all<'a, ER: RaftEngine>(&self, peers: impl Iterator<Item = &'a Peer<EK, ER>>) {
        let mut mailboxes = Vec::with_capacity(peers.size_hint().0);
        for peer in peers {
            let apply = Apply::new(peer);
            let (tx, fsm) = ApplyFsm::new(apply);
            mailboxes.push((
                peer.region_id(),
                BasicMailbox::new(tx, fsm, self.router().state_cnt().clone()),
            ));
        }
        self.router().register_all(mailboxes);
    }
}

pub type ApplyRouter<EK> = BatchRouter<ApplyFsm<EK>, ControlFsm>;

pub fn create_apply_batch_system<EK: KvEngine>(cfg: &Config) -> (ApplyRouter<EK>, ApplySystem<EK>) {
    let (control_tx, control_fsm) = ControlFsm::new();
    let (router, system) =
        batch_system::create_system(&cfg.apply_batch_system, control_tx, control_fsm);
    let system = ApplySystem { system };
    (router, system)
}
