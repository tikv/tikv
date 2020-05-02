// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::{Arc, RwLock};

use engine_rocks::RocksEngine;
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::Error as RaftStoreError;
use tikv_util::collections::HashMap;
use tikv_util::worker::Scheduler;

use crate::endpoint::{Deregister, Task};
use crate::Error as CdcError;

/// An Observer for CDC.
///
/// It observes raftstore internal events, such as:
///   1. Raft role change events,
///   2. Apply command events.
#[derive(Clone)]
pub struct CdcObserver {
    sched: Scheduler<Task>,
    // A shared registry for managing observed regions.
    // TODO: it may become a bottleneck, find a better way to manage the registry.
    observe_regions: Arc<RwLock<HashMap<u64, ObserveID>>>,
    cmd_batches: RefCell<Vec<CmdBatch>>,
}

impl CdcObserver {
    /// Create a new `CdcObserver`.
    ///
    /// Events are strong ordered, so `sched` must be implemented as
    /// a FIFO queue.
    pub fn new(sched: Scheduler<Task>) -> CdcObserver {
        CdcObserver {
            sched,
            observe_regions: Arc::default(),
            cmd_batches: RefCell::default(),
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<RocksEngine>) {
        // 100 is the priority of the observer. CDC should have a high priority.
        coprocessor_host
            .registry
            .register_cmd_observer(100, BoxCmdObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_role_observer(100, BoxRoleObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_region_change_observer(100, BoxRegionChangeObserver::new(self.clone()));
    }

    /// Subscribe an region, the observer will sink events of the region into
    /// its scheduler.
    ///
    /// Return pervious ObserveID if there is one.
    pub fn subscribe_region(&self, region_id: u64, observe_id: ObserveID) -> Option<ObserveID> {
        self.observe_regions
            .write()
            .unwrap()
            .insert(region_id, observe_id)
    }

    /// Stops observe the region.
    ///
    /// Return ObserverID if unsubscribe successfully.
    pub fn unsubscribe_region(&self, region_id: u64, observe_id: ObserveID) -> Option<ObserveID> {
        let mut regions = self.observe_regions.write().unwrap();
        // To avoid ABA problem, we must check the unique ObserveID.
        if let Some(oid) = regions.get(&region_id) {
            if *oid == observe_id {
                return regions.remove(&region_id);
            }
        }
        None
    }

    /// Check whether the region is subscribed or not.
    pub fn is_subscribed(&self, region_id: u64) -> Option<ObserveID> {
        self.observe_regions
            .read()
            .unwrap()
            .get(&region_id)
            .cloned()
    }
}

impl Coprocessor for CdcObserver {}

impl CmdObserver for CdcObserver {
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

    fn on_flush_apply(&self) {
        fail_point!("before_cdc_flush_apply");
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            if let Err(e) = self.sched.schedule(Task::MultiBatch { multi: batches }) {
                warn!("schedule cdc task failed"; "error" => ?e);
            }
        }
    }
}

impl RoleObserver for CdcObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if role != StateRole::Leader {
            let region_id = ctx.region().get_id();
            if let Some(observe_id) = self.is_subscribed(region_id) {
                // Unregister all downstreams.
                let store_err = RaftStoreError::NotLeader(region_id, None);
                let deregister = Deregister::Region {
                    region_id,
                    observe_id,
                    err: CdcError::Request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("schedule cdc task failed"; "error" => ?e);
                }
            }
        }
    }
}

impl RegionChangeObserver for CdcObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        if let RegionChangeEvent::Destroy = event {
            let region_id = ctx.region().get_id();
            if let Some(observe_id) = self.is_subscribed(region_id) {
                // Unregister all downstreams.
                let store_err = RaftStoreError::RegionNotFound(region_id);
                let deregister = Deregister::Region {
                    region_id,
                    observe_id,
                    err: CdcError::Request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("schedule cdc task failed"; "error" => ?e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::*;
    use std::time::Duration;

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler);
        let observe_id = ObserveID::new();

        observer.on_prepare_for_apply(observe_id, 0);
        observer.on_apply_cmd(
            observe_id,
            0,
            Cmd::new(0, RaftCmdRequest::default(), RaftCmdResponse::default()),
        );
        observer.on_flush_apply();

        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::MultiBatch { multi } => {
                assert_eq!(multi.len(), 1);
                assert_eq!(multi[0].len(), 1);
            }
            _ => panic!("unexpected task"),
        };

        // Does not send unsubscribed region events.
        let mut region = Region::default();
        region.set_id(1);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        let oid = ObserveID::new();
        observer.subscribe_region(1, oid);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister(Deregister::Region {
                region_id,
                observe_id,
                ..
            }) => {
                assert_eq!(region_id, 1);
                assert_eq!(observe_id, oid);
            }
            _ => panic!("unexpected task"),
        };

        // No event if it changes to leader.
        observer.on_role_change(&mut ctx, StateRole::Leader);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // unsubscribed fail if observer id is different.
        assert_eq!(observer.unsubscribe_region(1, ObserveID::new()), None);

        // No event if it is unsubscribed.
        let oid_ = observer.unsubscribe_region(1, oid).unwrap();
        assert_eq!(oid_, oid);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        region.set_id(999);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }
}
