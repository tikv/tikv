// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use engine_traits::{KvEngine, Peekable};
use kvproto::metapb::*;
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;

pub type ChangeDataSnapshot<S> = Box<dyn Peekable<DBVector = <S as Peekable>::DBVector> + Send>;

struct ChangeDataObserver<E: KvEngine> {
    cmd_batches: RefCell<Vec<CmdBatch>>,
    scheduler: Scheduler<Task<E::Snapshot>>,
}

impl<E: KvEngine> ChangeDataObserver<E> {
    // pub fn new() -> ChangeDataObserver {
    //     ChangeDataObserver {
    //         cmd_batches: RefCell::default(),
    //     }
    // }
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
            .expect("should exist some cmd batch")
            .push(observe_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let snapshot: ChangeDataSnapshot<E::Snapshot> = Box::new(engine.snapshot());
            if let Err(e) = self.scheduler.schedule(Task::ChangeLog {
                cmd_batch: batches,
                snapshot,
            }) {
                info!(""; "err" => ?e);
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
            info!(""; "err" => ?e);
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
                    info!(""; "err" => ?e);
                }
            }
            RegionChangeEvent::Update => {
                if let Err(e) = self
                    .scheduler
                    .schedule(Task::RegionUpdated(ctx.region().clone()))
                {
                    info!(""; "err" => ?e);
                }
            }
            RegionChangeEvent::Create => (),
        }
    }
}
