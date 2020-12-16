// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::ops::{Bound, Deref};
use std::sync::{Arc, RwLock};

use engine_rocks::RocksEngine;
use engine_traits::{
    IterOptions, KvEngine, ReadOptions, Snapshot, CF_DEFAULT, CF_LOCK, CF_WRITE,
    DATA_KEY_PREFIX_LEN,
};
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use raftstore::Error as RaftStoreError;
use tikv::storage::{Cursor, ScanMode, Snapshot as EngineSnapshot, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::time::Instant;
use tikv_util::worker::Scheduler;
use txn_types::{Key, Lock, MutationType, TimeStamp, TxnExtra, Value, WriteRef, WriteType};

use crate::endpoint::{Deregister, Task};
use crate::metrics::*;
use crate::{Error as CdcError, Result};

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

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost) {
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

impl CmdObserver<RocksEngine> for CdcObserver {
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

    fn on_flush_apply(&self, txn_extras: Vec<TxnExtra>, engine: RocksEngine) {
        fail_point!("before_cdc_flush_apply");
        let mut txn_extra = TxnExtra::default();
        txn_extras
            .into_iter()
            .for_each(|mut e| txn_extra.extend(&mut e));
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            let snapshot = RegionSnapshot::from_snapshot(engine.snapshot().into_sync(), region);
            let mut reader = OldValueReader::new(snapshot);
<<<<<<< HEAD
            let get_old_value = move |key, statistics: &mut Statistics| {
                if let Some((old_value, mutation_type)) = txn_extra.mut_old_values().remove(&key) {
=======
            let get_old_value = move |key,
                                      query_ts,
                                      old_value_cache: &mut OldValueCache,
                                      statistics: &mut Statistics| {
                old_value_cache.access_count += 1;
                if let Some((old_value, mutation_type)) = old_value_cache.cache.remove(&key) {
>>>>>>> 45efd0751... cdc: use for_update_ts to get old value (#9275)
                    match mutation_type {
                        MutationType::Insert => {
                            assert!(old_value.is_none());
                            return None;
                        }
                        MutationType::Put | MutationType::Delete => {
                            if let Some(old_value) = old_value {
                                let start_ts = old_value.start_ts;
                                return old_value.short_value.or_else(|| {
                                    let prev_key = key.truncate_ts().unwrap().append_ts(start_ts);
                                    let start = Instant::now();
                                    let mut opts = ReadOptions::new();
                                    opts.set_fill_cache(false);
                                    let value = reader.get_value_default(&prev_key, statistics);
                                    CDC_OLD_VALUE_DURATION_HISTOGRAM
                                        .with_label_values(&["get"])
                                        .observe(start.elapsed().as_secs_f64());
                                    value
                                });
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                // Cannot get old value from cache, seek for it in engine.
                let start = Instant::now();
                let key = key.truncate_ts().unwrap().append_ts(query_ts);
                let value = reader
                    .near_seek_old_value(&key, statistics)
                    .unwrap_or_default();
                CDC_OLD_VALUE_DURATION_HISTOGRAM
                    .with_label_values(&["seek"])
                    .observe(start.elapsed().as_secs_f64());
                value
            };
            if let Err(e) = self.sched.schedule(Task::MultiBatch {
                multi: batches,
                old_value_cb: Box::new(get_old_value),
            }) {
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

struct OldValueReader<S: EngineSnapshot> {
    snapshot: S,
}

impl<S: EngineSnapshot> OldValueReader<S> {
    fn new(snapshot: S) -> Self {
        Self { snapshot }
    }

    fn new_write_cursor(&self, key: &Key) -> Cursor<S::Iter> {
        let mut iter_opts = IterOptions::default();
        let ts = Key::decode_ts_from(key.as_encoded()).unwrap();
        let upper = Key::from_encoded_slice(Key::truncate_ts_for(key.as_encoded()).unwrap())
            .append_ts(TimeStamp::zero());
        iter_opts.set_fill_cache(false);
        iter_opts.set_hint_max_ts(Bound::Included(ts.into_inner()));
        iter_opts.set_lower_bound(key.as_encoded(), DATA_KEY_PREFIX_LEN);
        iter_opts.set_upper_bound(upper.as_encoded(), DATA_KEY_PREFIX_LEN);
        self.snapshot
            .iter_cf(CF_WRITE, iter_opts, ScanMode::Mixed)
            .unwrap()
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn get_value_default(&mut self, key: &Key, statistics: &mut Statistics) -> Option<Value> {
        statistics.data.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        self.snapshot
            .get_cf_opt(opts, CF_DEFAULT, &key)
            .unwrap()
            .map(|v| v.deref().to_vec())
    }

    fn check_lock(&mut self, key: &Key, statistics: &mut Statistics) -> bool {
        statistics.lock.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        let key_slice = key.as_encoded();
        let user_key = Key::from_encoded_slice(Key::truncate_ts_for(key_slice).unwrap());

        match self.snapshot.get_cf_opt(opts, CF_LOCK, &user_key).unwrap() {
            Some(v) => {
                let lock = Lock::parse(v.deref()).unwrap();
                std::cmp::max(lock.ts, lock.for_update_ts)
                    == Key::decode_ts_from(key_slice).unwrap()
            }
            None => false,
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn near_seek_old_value(
        &mut self,
        key: &Key,
        statistics: &mut Statistics,
    ) -> Result<Option<Value>> {
        let user_key = Key::truncate_ts_for(key.as_encoded()).unwrap();
        let mut write_cursor = self.new_write_cursor(key);
        if write_cursor.near_seek(key, &mut statistics.write)?
            && Key::is_user_key_eq(write_cursor.key(&mut statistics.write), user_key)
        {
            if write_cursor.key(&mut statistics.write) == key.as_encoded().as_slice() {
                // Key was committed, move cursor to the next key to seek for old value.
                if !write_cursor.next(&mut statistics.write) {
                    // Do not has any next key, return empty value.
                    return Ok(Some(Vec::default()));
                }
            } else if !self.check_lock(key, statistics) {
                // Key was not committed, check if the lock is corresponding to the key.
                return Ok(None);
            }

            let mut old_value = Some(Vec::default());
            while Key::is_user_key_eq(write_cursor.key(&mut statistics.write), user_key) {
                let write = WriteRef::parse(write_cursor.value(&mut statistics.write)).unwrap();
                old_value = match write.write_type {
                    WriteType::Put => match write.short_value {
                        Some(short_value) => Some(short_value.to_vec()),
                        None => {
                            let key = key.clone().truncate_ts().unwrap().append_ts(write.start_ts);
                            self.get_value_default(&key, statistics)
                        }
                    },
                    WriteType::Delete => Some(Vec::default()),
                    WriteType::Rollback | WriteType::Lock => {
                        if !write_cursor.next(&mut statistics.write) {
                            Some(Vec::default())
                        } else {
                            continue;
                        }
                    }
                };
                break;
            }
            Ok(old_value)
        } else if self.check_lock(key, statistics) {
            Ok(Some(Vec::default()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::RocksEngine;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::*;
    use std::time::Duration;
    use tikv::storage::kv::TestEngineBuilder;
<<<<<<< HEAD
=======
    use tikv::storage::txn::tests::*;
>>>>>>> 45efd0751... cdc: use for_update_ts to get old value (#9275)

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler);
        let observe_id = ObserveID::new();
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();

        <CdcObserver as CmdObserver<RocksEngine>>::on_prepare_for_apply(&observer, observe_id, 0);
        <CdcObserver as CmdObserver<RocksEngine>>::on_apply_cmd(
            &observer,
            observe_id,
            0,
            Cmd::new(0, RaftCmdRequest::default(), RaftCmdResponse::default()),
        );
        observer.on_flush_apply(Vec::default(), RocksEngine::from_db(engine));

        match rx.recv_timeout(Duration::from_millis(10)).unwrap().unwrap() {
            Task::MultiBatch { multi, .. } => {
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
<<<<<<< HEAD
=======

    #[test]
    fn test_old_value_reader() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();
        let k = b"k";
        let key = Key::from_raw(k);

        let must_get_eq = |ts: u64, value| {
            let mut old_value_reader = OldValueReader::new(Arc::new(kv_engine.snapshot()));
            let mut statistics = Statistics::default();
            assert_eq!(
                old_value_reader
                    .near_seek_old_value(&key.clone().append_ts(ts.into()), &mut statistics)
                    .unwrap(),
                value
            );
            let mut opts = ReadOptions::new();
            opts.set_fill_cache(false);
        };

        must_prewrite_put(&engine, k, b"v1", k, 1);
        must_get_eq(2, None);
        must_get_eq(1, Some(vec![]));
        must_commit(&engine, k, 1, 1);
        must_get_eq(1, Some(vec![]));

        must_prewrite_put(&engine, k, b"v2", k, 2);
        must_get_eq(2, Some(b"v1".to_vec()));
        must_rollback(&engine, k, 2);

        must_prewrite_put(&engine, k, b"v3", k, 3);
        must_get_eq(3, Some(b"v1".to_vec()));
        must_commit(&engine, k, 3, 3);

        must_prewrite_delete(&engine, k, k, 4);
        must_get_eq(4, Some(b"v3".to_vec()));
        must_commit(&engine, k, 4, 4);

        must_prewrite_put(&engine, k, vec![b'v'; 5120].as_slice(), k, 5);
        must_get_eq(5, Some(vec![]));
        must_commit(&engine, k, 5, 5);

        must_prewrite_delete(&engine, k, k, 6);
        must_get_eq(6, Some(vec![b'v'; 5120]));
        must_rollback(&engine, k, 6);

        must_prewrite_put(&engine, k, b"v4", k, 7);
        must_commit(&engine, k, 7, 9);

        must_acquire_pessimistic_lock(&engine, k, k, 8, 10);
        must_pessimistic_prewrite_put(&engine, k, b"v5", k, 8, 10, true);
        must_get_eq(10, Some(b"v4".to_vec()));
        must_commit(&engine, k, 8, 11);
    }
>>>>>>> 45efd0751... cdc: use for_update_ts to get old value (#9275)
}
