// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use collections::HashMap;
use engine_traits::KvEngine;
use fail::fail_point;
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::RegionSnapshot;
use raftstore::Error as RaftStoreError;
use tikv_util::worker::Scheduler;
use tikv_util::{error, warn};

use crate::endpoint::{Deregister, Task};
use crate::old_value::{self, OldValueCache, OldValueReader};
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
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        // use 0 as the priority of the cmd observer. CDC should have a higher priority than
        // the `resolved-ts`'s cmd observer
        coprocessor_host
            .registry
            .register_cmd_observer(0, BoxCmdObserver::new(self.clone()));
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

impl<E: KvEngine> CmdObserver<E> for CdcObserver {
    // `CdcObserver::on_flush_applied_cmd_batch` should only invoke if `cmd_batches` is not empty
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        engine: &E,
    ) {
        assert!(!cmd_batches.is_empty());
        fail_point!("before_cdc_flush_apply");
        if max_level < ObserveLevel::All {
            return;
        }
        let cmd_batches: Vec<_> = cmd_batches
            .iter()
            .filter(|cb| cb.level == ObserveLevel::All && !cb.is_empty())
            .cloned()
            .collect();
        if cmd_batches.is_empty() {
            return;
        }
        let mut region = Region::default();
        region.mut_peers().push(Peer::default());
        // Create a snapshot here for preventing the old value was GC-ed.
        // TODO: only need it after enabling old value, may add a flag to indicate whether to get it.
        let snapshot = RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
        let reader = OldValueReader::new(snapshot);
        let get_old_value = move |key, query_ts, old_value_cache: &mut OldValueCache| {
            old_value::get_old_value(&reader, key, query_ts, old_value_cache)
        };
        if let Err(e) = self.sched.schedule(Task::MultiBatch {
            multi: cmd_batches,
            old_value_cb: Box::new(get_old_value),
        }) {
            warn!("cdc schedule task failed"; "error" => ?e);
        }
    }

    fn on_applied_current_term(&self, _: StateRole, _: &Region) {}
}

impl RoleObserver for CdcObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if role != StateRole::Leader {
            let region_id = ctx.region().get_id();
            if let Some(observe_id) = self.is_subscribed(region_id) {
                // Unregister all downstreams.
                let store_err = RaftStoreError::NotLeader(region_id, None);
                let deregister = Deregister::Delegate {
                    region_id,
                    observe_id,
                    err: CdcError::request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("cdc schedule cdc task failed"; "error" => ?e);
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
                let deregister = Deregister::Delegate {
                    region_id,
                    observe_id,
                    err: CdcError::request(store_err.into()),
                };
                if let Err(e) = self.sched.schedule(Task::Deregister(deregister)) {
                    error!("cdc schedule cdc task failed"; "error" => ?e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::RocksEngine;
    use kvproto::metapb::Region;
    use std::time::Duration;
    use tikv::storage::kv::TestEngineBuilder;

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, mut rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler);
        let observe_info = CmdObserveInfo::from_handle(ObserveHandle::new(), ObserveHandle::new());
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();

        let mut cb = CmdBatch::new(&observe_info, Region::default());
        cb.push(&observe_info, 0, Cmd::default());
        <CdcObserver as CmdObserver<RocksEngine>>::on_flush_applied_cmd_batch(
            &observer,
            cb.level,
            &mut vec![cb],
            &engine,
        );
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::MultiBatch { multi, .. } => {
                assert_eq!(multi.len(), 1);
                assert_eq!(multi[0].len(), 1);
            }
            _ => panic!("unexpected task"),
        };

        // Stop observing cmd
        observe_info.cdc_id.stop_observing();
        let mut cb = CmdBatch::new(&observe_info, Region::default());
        cb.push(&observe_info, 0, Cmd::default());
        <CdcObserver as CmdObserver<RocksEngine>>::on_flush_applied_cmd_batch(
            &observer,
            cb.level,
            &mut vec![cb],
            &engine,
        );
        match rx.recv_timeout(Duration::from_millis(10)) {
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            _ => panic!("unexpected result"),
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
            Task::Deregister(Deregister::Delegate {
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
