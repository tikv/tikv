// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use api_version::{ApiV2, KeyMode, KvFormat};
use causal_ts::{Error as CausalTsError, RawTsTracker, Result as CausalTsResult};
use collections::HashMap;
use engine_traits::KvEngine;
use fail::fail_point;
use kvproto::{
    kvrpcpb::ApiVersion,
    metapb::{Peer, Region},
    raft_cmdpb::CmdType,
};
use raft::StateRole;
use raftstore::{coprocessor::*, store::RegionSnapshot, Error as RaftStoreError};
use tikv::storage::Statistics;
use tikv_util::{box_err, defer, error, warn, worker::Scheduler};
use txn_types::{Key, TimeStamp};

use crate::{
    endpoint::{Deregister, Task},
    old_value::{self, OldValueCache},
    Error as CdcError,
};

// max_ts presents the max ts in one batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawRegionTs {
    pub region_id: u64,
    pub cdc_id: ObserveId,
    pub max_ts: TimeStamp,
}

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
    observe_regions: Arc<RwLock<HashMap<u64, ObserveId>>>,
    api_version: ApiVersion,
}

impl CdcObserver {
    /// Create a new `CdcObserver`.
    ///
    /// Events are strong ordered, so `sched` must be implemented as
    /// a FIFO queue.
    pub fn new(sched: Scheduler<Task>, api_version: ApiVersion) -> CdcObserver {
        CdcObserver {
            sched,
            observe_regions: Arc::default(),
            api_version,
        }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        // use 0 as the priority of the cmd observer. CDC should have a higher priority
        // than the `resolved-ts`'s cmd observer
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
    /// Return previous ObserveId if there is one.
    pub fn subscribe_region(&self, region_id: u64, observe_id: ObserveId) -> Option<ObserveId> {
        self.observe_regions
            .write()
            .unwrap()
            .insert(region_id, observe_id)
    }

    /// Stops observe the region.
    ///
    /// Return ObserverID if unsubscribe successfully.
    pub fn unsubscribe_region(&self, region_id: u64, observe_id: ObserveId) -> Option<ObserveId> {
        let mut regions = self.observe_regions.write().unwrap();
        // To avoid ABA problem, we must check the unique ObserveId.
        if let Some(oid) = regions.get(&region_id) {
            if *oid == observe_id {
                return regions.remove(&region_id);
            }
        }
        None
    }

    /// Check whether the region is subscribed or not.
    pub fn is_subscribed(&self, region_id: u64) -> Option<ObserveId> {
        self.observe_regions
            .read()
            .unwrap()
            .get(&region_id)
            .cloned()
    }

    fn untrack_raw_ts(&self, raw_region_ts: Vec<RawRegionTs>) {
        if raw_region_ts.is_empty() {
            return;
        }
        if let Err(e) = self.sched.schedule(Task::RawUntrackTs { raw_region_ts }) {
            warn!("cdc schedule task failed"; "error" => ?e);
        }
    }

    // parse rawkv cmd from CmdBatch Vec and return the max ts of every region.
    pub fn get_raw_region_ts(&self, cmd_batches: &Vec<CmdBatch>) -> Vec<RawRegionTs> {
        if self.api_version != ApiVersion::V2 {
            return vec![];
        }
        let mut region_ts = vec![];
        for batch in cmd_batches {
            if batch.is_empty() {
                continue;
            }
            let region_id = batch.region_id;
            let cdc_id = batch.cdc_id;
            if !self
                .is_subscribed(region_id)
                .map_or(false, |ob_id| ob_id == cdc_id)
            {
                continue;
            }
            // Find the max ts in one batch
            // The raw request's ts is non-decreasing, only need find the last one.
            batch.cmds.iter().rfind(|cmd| {
                if let Some(last_key) = cmd
                    .request
                    .get_requests()
                    .iter()
                    .rfind(|req| {
                        CmdType::Put == req.get_cmd_type()
                            && ApiV2::parse_key_mode(req.get_put().get_key()) == KeyMode::Raw
                    })
                    .map(|req| req.get_put().get_key())
                {
                    match ApiV2::decode_raw_key_owned(Key::from_encoded_slice(last_key), true) {
                        Ok((_, ts)) => {
                            region_ts.push(RawRegionTs {
                                region_id,
                                cdc_id,
                                max_ts: ts.unwrap(),
                            });
                        }
                        // error is ignored, raw dead lock is resolved in Endpoint::on_min_ts
                        Err(e) => warn!("decode raw key fails"; "err" => ?e),
                    }
                    true
                } else {
                    false
                }
            });
        }
        region_ts
    }
}

impl Coprocessor for CdcObserver {}

impl<E: KvEngine> CmdObserver<E> for CdcObserver {
    // `CdcObserver::on_flush_applied_cmd_batch` should only invoke if `cmd_batches`
    // is not empty
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        engine: &E,
    ) {
        assert!(!cmd_batches.is_empty());
        fail_point!("before_cdc_flush_apply");

        // Untrack raw ts regardless of the ob level.
        // Because RawKV locks is tracked regardless of observe level as it is in Raft
        // propose procedure and can not get an accurate observe level.
        let raw_region_ts = self.get_raw_region_ts(cmd_batches);
        defer!(self.untrack_raw_ts(raw_region_ts));

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
        // TODO: only need it after enabling old value, may add a flag to indicate
        // whether to get it.
        let snapshot = RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
        let get_old_value = move |key,
                                  query_ts,
                                  old_value_cache: &mut OldValueCache,
                                  statistics: &mut Statistics| {
            old_value::get_old_value(&snapshot, key, query_ts, old_value_cache, statistics)
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
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        if role_change.state != StateRole::Leader {
            let region_id = ctx.region().get_id();
            if let Some(observe_id) = self.is_subscribed(region_id) {
                let leader_id = if role_change.leader_id != raft::INVALID_ID {
                    Some(role_change.leader_id)
                } else if role_change.prev_lead_transferee == role_change.vote {
                    Some(role_change.prev_lead_transferee)
                } else {
                    None
                };
                let leader = leader_id
                    .and_then(|x| ctx.region().get_peers().iter().find(|p| p.id == x))
                    .cloned();

                // Unregister all downstreams.
                let store_err = RaftStoreError::NotLeader(region_id, leader);
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

impl RawTsTracker for CdcObserver {
    fn track_ts(&self, region_id: u64, ts: TimeStamp) -> CausalTsResult<()> {
        if self.is_subscribed(region_id).is_some() {
            self.sched
                .schedule(Task::RawTrackTs { region_id, ts })
                .map_err(|err| {
                    CausalTsError::Other(box_err!(
                        "sched raw track ts err: {:?}, region: {:?}, ts: {:?}",
                        err,
                        region_id,
                        ts
                    ))
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use engine_rocks::RocksEngine;
    use engine_traits::CF_WRITE;
    use kvproto::{
        metapb::Region,
        raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, Request},
    };
    use raftstore::{coprocessor::RoleChange, store::util::new_peer};
    use tikv::storage::kv::TestEngineBuilder;

    use super::*;

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, mut rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler, ApiVersion::V1);
        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::new(),
        );
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();

        let mut cb = CmdBatch::new(&observe_info, 0);
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
        observe_info.pitr_id.stop_observing();
        let mut cb = CmdBatch::new(&observe_info, 0);
        cb.push(&observe_info, 0, Cmd::default());
        <CdcObserver as CmdObserver<RocksEngine>>::on_flush_applied_cmd_batch(
            &observer,
            cb.level,
            &mut vec![cb],
            &engine,
        );
        match rx.recv_timeout(Duration::from_millis(10)) {
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            any => panic!("unexpected result: {:?}", any),
        };

        // Does not send unsubscribed region events.
        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(new_peer(2, 2));
        region.mut_peers().push(new_peer(3, 3));

        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, &RoleChange::new(StateRole::Follower));
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        let oid = ObserveId::new();
        observer.subscribe_region(1, oid);
        let mut ctx = ObserverContext::new(&region);

        // NotLeader error should contains the new leader.
        observer.on_role_change(
            &mut ctx,
            &RoleChange {
                state: StateRole::Follower,
                leader_id: 2,
                prev_lead_transferee: raft::INVALID_ID,
                vote: raft::INVALID_ID,
            },
        );
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister(Deregister::Delegate {
                region_id,
                observe_id,
                err,
            }) => {
                assert_eq!(region_id, 1);
                assert_eq!(observe_id, oid);
                let store_err = RaftStoreError::NotLeader(region_id, Some(new_peer(2, 2)));
                match err {
                    CdcError::Request(err) => assert_eq!(*err, store_err.into()),
                    _ => panic!("unexpected err"),
                }
            }
            _ => panic!("unexpected task"),
        };

        // NotLeader error should includes leader transferee.
        observer.on_role_change(
            &mut ctx,
            &RoleChange {
                state: StateRole::Follower,
                leader_id: raft::INVALID_ID,
                prev_lead_transferee: 3,
                vote: 3,
            },
        );
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::Deregister(Deregister::Delegate {
                region_id,
                observe_id,
                err,
            }) => {
                assert_eq!(region_id, 1);
                assert_eq!(observe_id, oid);
                let store_err = RaftStoreError::NotLeader(region_id, Some(new_peer(3, 3)));
                match err {
                    CdcError::Request(err) => assert_eq!(*err, store_err.into()),
                    _ => panic!("unexpected err"),
                }
            }
            _ => panic!("unexpected task"),
        };

        // No event if it changes to leader.
        observer.on_role_change(&mut ctx, &RoleChange::new(StateRole::Leader));
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // track for unregistered region id.
        observer.track_ts(2, 10.into()).unwrap();
        // no event for unregistered region id.
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
        observer.track_ts(1, 10.into()).unwrap();
        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::RawTrackTs { region_id, ts } => {
                assert_eq!(region_id, 1);
                assert_eq!(ts, 10.into());
            }
            _ => panic!("unexpected task"),
        };

        // unsubscribed fail if observer id is different.
        assert_eq!(observer.unsubscribe_region(1, ObserveId::new()), None);

        // No event if it is unsubscribed.
        let oid_ = observer.unsubscribe_region(1, oid).unwrap();
        assert_eq!(oid_, oid);
        observer.on_role_change(&mut ctx, &RoleChange::new(StateRole::Follower));
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();

        // No event if it is unsubscribed.
        region.set_id(999);
        let mut ctx = ObserverContext::new(&region);
        observer.on_role_change(&mut ctx, &RoleChange::new(StateRole::Follower));
        rx.recv_timeout(Duration::from_millis(10)).unwrap_err();
    }

    fn put_cf(cf: &str, key: &[u8], value: &[u8]) -> Request {
        let mut cmd = Request::default();
        cmd.set_cmd_type(CmdType::Put);
        cmd.mut_put().set_cf(cf.to_owned());
        cmd.mut_put().set_key(key.to_vec());
        cmd.mut_put().set_value(value.to_vec());
        cmd
    }

    #[test]
    fn test_get_raw_region_ts() {
        let (scheduler, mut rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler, ApiVersion::V2);
        let region_id = 1;
        let mut cmd = Cmd::new(0, 0, RaftCmdRequest::default(), RaftCmdResponse::default());
        cmd.request.mut_requests().clear();
        // Both cdc and resolved-ts worker are observing
        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::default(),
        );
        let mut cb = CmdBatch::new(&observe_info, region_id);
        cb.push(&observe_info, region_id, cmd.clone());
        let cmd_batches = vec![cb];
        let ret = observer.get_raw_region_ts(&cmd_batches);
        assert!(ret.is_empty());

        let data = vec![put_cf(CF_WRITE, b"k7", b"v"), put_cf(CF_WRITE, b"k8", b"v")];
        for put in &data {
            cmd.request.mut_requests().push(put.clone());
        }
        let mut cb = CmdBatch::new(&observe_info, region_id);
        cb.push(&observe_info, region_id, cmd.clone());
        let cmd_batches = vec![cb];
        let ret = observer.get_raw_region_ts(&cmd_batches);
        assert!(ret.is_empty()); // no apiv2 key
        cmd.request.mut_requests().clear();
        let data = vec![
            put_cf(
                CF_WRITE,
                ApiV2::encode_raw_key(b"ra", Some(TimeStamp::from(100))).as_encoded(),
                b"v1",
            ),
            put_cf(
                CF_WRITE,
                ApiV2::encode_raw_key(b"rb", Some(TimeStamp::from(200))).as_encoded(),
                b"v2",
            ),
        ];
        for put in &data {
            cmd.request.mut_requests().push(put.clone());
        }
        let mut cb1 = CmdBatch::new(&observe_info, region_id);
        cb1.push(&observe_info, region_id, cmd.clone());
        let mut cmd2 = Cmd::new(0, 0, RaftCmdRequest::default(), RaftCmdResponse::default());
        cmd2.request.mut_requests().clear();
        let data2 = vec![
            put_cf(
                CF_WRITE,
                ApiV2::encode_raw_key(b"ra", Some(TimeStamp::from(300))).as_encoded(),
                b"v1",
            ),
            put_cf(
                CF_WRITE,
                ApiV2::encode_raw_key(b"rb", Some(TimeStamp::from(400))).as_encoded(),
                b"v2",
            ),
        ];
        for put in &data2 {
            cmd2.request.mut_requests().push(put.clone());
        }
        let mut cb2 = CmdBatch::new(&observe_info, region_id + 1);
        cb2.push(&observe_info, region_id + 1, cmd2.clone());
        let mut cmd_batches = vec![cb1.clone(), cb2.clone()];
        let ret = observer.get_raw_region_ts(&cmd_batches);
        assert_eq!(ret.len(), 0); // region is not subscribed.
        observer.subscribe_region(region_id, observe_info.cdc_id.id);
        observer.subscribe_region(region_id + 1, observe_info.cdc_id.id);
        let ret = observer.get_raw_region_ts(&cmd_batches);
        assert_eq!(ret.len(), 2); // two batch and both subscribed.
        assert_eq!(
            ret[0],
            RawRegionTs {
                region_id,
                cdc_id: observe_info.cdc_id.id,
                max_ts: TimeStamp::from(200)
            }
        );
        assert_eq!(
            ret[1],
            RawRegionTs {
                region_id: region_id + 1,
                cdc_id: observe_info.cdc_id.id,
                max_ts: TimeStamp::from(400)
            }
        );
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();
        <CdcObserver as CmdObserver<RocksEngine>>::on_flush_applied_cmd_batch(
            &observer,
            ObserveLevel::LockRelated,
            &mut cmd_batches,
            &engine,
        );
        // schedule task even if max level is not `All`.
        match rx
            .recv_timeout(Duration::from_millis(100))
            .unwrap()
            .unwrap()
        {
            Task::RawUntrackTs { raw_region_ts } => {
                assert_eq!(raw_region_ts.len(), 2); // two batch and both subscribed.
                assert_eq!(
                    raw_region_ts[0],
                    RawRegionTs {
                        region_id,
                        cdc_id: observe_info.cdc_id.id,
                        max_ts: TimeStamp::from(200)
                    }
                );
                assert_eq!(
                    raw_region_ts[1],
                    RawRegionTs {
                        region_id: region_id + 1,
                        cdc_id: observe_info.cdc_id.id,
                        max_ts: TimeStamp::from(400)
                    }
                );
            }
            _ => panic!("unexpected task"),
        };

        // non-rawkv
        let data3 = vec![
            put_cf(
                CF_WRITE,
                ApiV2::encode_raw_key(b"ra", Some(TimeStamp::from(500))).as_encoded(),
                b"v1",
            ),
            put_cf(
                CF_WRITE, // this is non-rawkv
                ApiV2::encode_raw_key(b"b", Some(TimeStamp::from(600))).as_encoded(),
                b"v2",
            ),
        ];
        let mut cmd3 = Cmd::new(0, 0, RaftCmdRequest::default(), RaftCmdResponse::default());
        for put in &data3 {
            cmd3.request.mut_requests().push(put.clone());
        }
        cb2.push(&observe_info, region_id + 1, cmd3.clone());
        let cmd_batches = vec![cb1, cb2];
        let ret = observer.get_raw_region_ts(&cmd_batches);
        assert_eq!(ret.len(), 2); // two batch and both subscribed.
        assert_eq!(
            ret[0],
            RawRegionTs {
                region_id,
                cdc_id: observe_info.cdc_id.id,
                max_ts: TimeStamp::from(200)
            }
        );
        assert_eq!(
            ret[1],
            RawRegionTs {
                region_id: region_id + 1,
                cdc_id: observe_info.cdc_id.id,
                max_ts: TimeStamp::from(500) // 600 is not rawkey
            }
        );
        let (scheduler, _) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler, ApiVersion::V1);
        let ret = observer.get_raw_region_ts(&cmd_batches);
        assert!(ret.is_empty()); // v1 does nothing.
    }
}
