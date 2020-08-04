// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use engine_rocks::RocksEngine;
use engine_traits::{IterOptions, KvEngine, ReadOptions, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use raftstore::Error as RaftStoreError;
use tikv::storage::{Cursor, ScanMode, Snapshot as EngineSnapshot, Statistics};
use tikv_util::collections::HashMap;
use tikv_util::worker::Scheduler;
use txn_types::{Key, Lock, MutationType, TxnExtra, Value, WriteRef, WriteType};

use crate::endpoint::{Deregister, Task};
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

impl<E: KvEngine> CmdObserver<E> for CdcObserver {
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

    fn on_flush_apply(&self, txn_extras: Vec<TxnExtra>, engine: E) {
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
            let snapshot =
                RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
            let mut reader = OldValueReader::new(snapshot);
            let get_old_value = move |key| {
                if let Some((old_value, mutation_type)) = txn_extra.mut_old_values().remove(&key) {
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
                                    let mut opts = ReadOptions::new();
                                    opts.set_fill_cache(false);
                                    reader.get_value_default(&prev_key)
                                });
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                reader.near_seek_old_value(&key).unwrap_or_default()
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
    write_cursor: Cursor<S::Iter>,
    // TODO(5kbpers): add a metric here.
    statistics: Statistics,
}

impl<S: EngineSnapshot> OldValueReader<S> {
    fn new(snapshot: S) -> Self {
        let mut iter_opts = IterOptions::default()
            .use_prefix_seek()
            .set_prefix_same_as_start(true);
        iter_opts.set_fill_cache(false);
        let write_cursor = snapshot
            .iter_cf(CF_WRITE, iter_opts, ScanMode::Mixed)
            .unwrap();
        Self {
            snapshot,
            write_cursor,
            statistics: Statistics::default(),
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn get_value_default(&mut self, key: &Key) -> Option<Value> {
        self.statistics.data.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        self.snapshot
            .get_cf_opt(opts, CF_DEFAULT, &key)
            .unwrap()
            .map(|v| v.deref().to_vec())
    }

    fn check_lock(&mut self, key: &Key) -> bool {
        self.statistics.lock.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        let key_slice = key.as_encoded();
        let user_key = Key::from_encoded_slice(Key::truncate_ts_for(key_slice).unwrap());

        match self.snapshot.get_cf_opt(opts, CF_LOCK, &user_key).unwrap() {
            Some(v) => {
                let lock = Lock::parse(v.deref()).unwrap();
                lock.ts == Key::decode_ts_from(key_slice).unwrap()
            }
            None => false,
        }
    }

    // return Some(vec![]) if value is empty.
    // return None if key not exist.
    fn near_seek_old_value(&mut self, key: &Key) -> Result<Option<Value>> {
        let user_key = Key::truncate_ts_for(key.as_encoded()).unwrap();
        if self
            .write_cursor
            .near_seek(key, &mut self.statistics.write)?
            && Key::is_user_key_eq(self.write_cursor.key(&mut self.statistics.write), user_key)
        {
            if self.write_cursor.key(&mut self.statistics.write) == key.as_encoded().as_slice() {
                // Key was committed, move cursor to the next key to seek for old value.
                if !self.write_cursor.next(&mut self.statistics.write) {
                    // Do not has any next key, return empty value.
                    return Ok(Some(Vec::default()));
                }
            } else if !self.check_lock(key) {
                return Ok(None);
            }

            // Key was not committed, check if the lock is corresponding to the key.
            let mut old_value = Some(Vec::default());
            while Key::is_user_key_eq(self.write_cursor.key(&mut self.statistics.write), user_key) {
                let write =
                    WriteRef::parse(self.write_cursor.value(&mut self.statistics.write)).unwrap();
                old_value = match write.write_type {
                    WriteType::Put => match write.short_value {
                        Some(short_value) => Some(short_value.to_vec()),
                        None => {
                            let key = key.clone().truncate_ts().unwrap().append_ts(write.start_ts);
                            self.get_value_default(&key)
                        }
                    },
                    WriteType::Delete => Some(Vec::default()),
                    WriteType::Rollback | WriteType::Lock => {
                        if !self.write_cursor.next(&mut self.statistics.write) {
                            Some(Vec::default())
                        } else {
                            continue;
                        }
                    }
                };
                break;
            }
            Ok(old_value)
        } else if self.check_lock(key) {
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
    use tikv::storage::mvcc::tests::*;

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
        observer.on_flush_apply(Vec::default(), engine);

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

    #[test]
    fn test_old_value_reader() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();
        let k = b"k";
        let key = Key::from_raw(k);

        let must_get_eq = |ts: u64, value| {
            let mut old_value_reader = OldValueReader::new(Arc::new(kv_engine.snapshot()));
            assert_eq!(
                old_value_reader
                    .near_seek_old_value(&key.clone().append_ts(ts.into()))
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
    }
}
