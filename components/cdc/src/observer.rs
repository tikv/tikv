// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::{Arc, RwLock};

use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::Error as RaftStoreError;
use tikv_util::collections::{HashMap, HashSet};
use tikv_util::worker::Scheduler;

use crate::endpoint::Task;
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
    observe_regions: Arc<RwLock<HashSet<u64>>>,
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

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost) {
        // 100 is the priority of the observer. CDC should have a high priority.
        coprocessor_host
            .registry
            .register_cmd_observer(100, BoxCmdObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_role_observer(100, BoxRoleObserver::new(self.clone()));
    }

    /// Subscribe an region, the observer will sink events of the region into
    /// its scheduler.
    pub fn subscribe_region(&self, region_id: u64) {
        self.observe_regions.write().unwrap().insert(region_id);
    }

    /// Stops observe the region.
    pub fn unsubscribe_region(&self, region_id: u64) {
        self.observe_regions.write().unwrap().remove(&region_id);
    }

    /// Check whether the region is subscribed or not.
    pub fn is_subscribed(&self, region_id: u64) -> bool {
        self.observe_regions.read().unwrap().contains(&region_id)
    }
}

impl Coprocessor for CdcObserver {}

impl CmdObserver for CdcObserver {
    fn on_prepare_for_apply(&self, region_id: u64) {
        self.cmd_batches.borrow_mut().push(CmdBatch::new(region_id));
    }

    fn on_apply_cmd(&self, region_id: u64, cmd: Cmd) {
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .expect("should exist some cmd batch")
            .push(region_id, cmd);
    }

    fn on_flush_apply(&self) {
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
            if self.is_subscribed(region_id) {
                // Unregister all downstreams.
                let store_err = RaftStoreError::NotLeader(region_id, None);
                if let Err(e) = self.sched.schedule(Task::Deregister {
                    region_id,
                    downstream_id: None,
                    conn_id: None,
                    err: Some(CdcError::Request(store_err.into())),
                }) {
                    warn!("schedule cdc task failed"; "error" => ?e);
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

        observer.on_prepare_for_apply(0);
        observer.on_apply_cmd(
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

        observer.subscribe_region(1);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister {
                region_id,
                downstream_id,
                conn_id,
                err,
            } => {
                assert_eq!(region_id, 1);
                assert!(downstream_id.is_none(), "{:?}", downstream_id);
                assert!(conn_id.is_none(), "{:?}", conn_id);
                assert!(err.is_some(), "{:?}", err);
            }
            _ => panic!("unexpected task"),
        };

        // No event if it changes to leader.
        observer.on_role_change(&mut ctx, StateRole::Leader);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        observer.unsubscribe_region(1);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        region.set_id(999);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, StateRole::Follower);
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }
}
