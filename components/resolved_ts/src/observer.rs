// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

pub struct ChangeDataObserver<E: KvEngine> {
    cmd_batches: RefCell<Vec<CmdBatch>>,
    scheduler: Scheduler<Task<E::Snapshot>>,
}

impl<E: KvEngine> ChangeDataObserver<E> {
    pub fn new(scheduler: Scheduler<Task<E::Snapshot>>) -> Self {
        ChangeDataObserver {
            cmd_batches: RefCell::default(),
            scheduler,
        }
    }
}

impl<E: KvEngine> Coprocessor for ChangeDataObserver<E> {}

impl<E: KvEngine> CmdObserver<E> for ChangeDataObserver<E> {
    fn on_prepare_for_apply(&self, observe_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(observe_id, region_id));
    }

    fn on_apply_cmd(&self, observe_id: ObserveID, region_id: u64, cmd: Cmd) {
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .unwrap_or_else(|| panic!("region {} should exist some cmd batch", region_id))
            .push(observe_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            // TODO: only need it after enabling old value, may add a flag to indicate whether to get it.
            let snapshot =
                RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
            if let Err(e) = self.scheduler.schedule(Task::ChangeLog {
                cmd_batch: batches,
                snapshot,
            }) {
                info!("failed to schedule change log event"; "err" => ?e);
            }
        }
    }
}

impl<E: KvEngine> RoleObserver for ChangeDataObserver<E> {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if let Err(e) = self.scheduler.schedule(Task::RegionRoleChanged {
            role,
            region: ctx.region().clone(),
        }) {
            info!("failed to schedule region role changed event"; "err" => ?e);
        }
    }
}

impl<E: KvEngine> RegionChangeObserver for ChangeDataObserver<E> {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        match event {
            RegionChangeEvent::Destroy => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionDestroyed(ctx.region().clone()))
                {
                    info!("failed to schedule region destroyed event"; "err" => ?e);
                }
            }
            RegionChangeEvent::Update => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionUpdated(ctx.region().clone()))
                {
                    info!("failed to schedule region updated event"; "err" => ?e);
                }
            }
            RegionChangeEvent::Create => (),
        }
    }
}
