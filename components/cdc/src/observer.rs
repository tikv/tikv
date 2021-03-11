// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_traits::{KvEngine, Mutable, ReadOptions, CF_DEFAULT, CF_WRITE};
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::coprocessor::*;
use raftstore::store::fsm::ObserveID;
use raftstore::store::RegionSnapshot;
use raftstore::Error as RaftStoreError;
use tikv::storage::{Cursor, CursorBuilder, ScanMode, Snapshot as EngineSnapshot, Statistics};
use tikv_util::time::Instant;
use tikv_util::worker::Scheduler;
use txn_types::{Key, MutationType, OldValue, TimeStamp, Value, WriteRef, WriteType};

use crate::endpoint::{Deregister, OldValueCache, Task};
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
    fn on_prepare_for_apply(&self, cdc_id: ObserveID, rts_id: ObserveID, region_id: u64) {
        self.cmd_batches
            .borrow_mut()
            .push(CmdBatch::new(cdc_id, rts_id, region_id));
    }

    fn on_apply_cmd(&self, cdc_id: ObserveID, rts_id: ObserveID, region_id: u64, cmd: Cmd) {
        self.cmd_batches
            .borrow_mut()
            .last_mut()
            .expect("should exist some cmd batch")
            .push(cdc_id, rts_id, region_id, cmd);
    }

    fn on_flush_apply(&self, engine: E) {
        fail_point!("before_cdc_flush_apply");
        self.cmd_batches.borrow_mut().retain(|b| !b.is_empty());
        if !self.cmd_batches.borrow().is_empty() {
            let batches = self.cmd_batches.replace(Vec::default());
            let mut region = Region::default();
            region.mut_peers().push(Peer::default());
            // Create a snapshot here for preventing the old value was GC-ed.
            let snapshot =
                RegionSnapshot::from_snapshot(Arc::new(engine.snapshot()), Arc::new(region));
            let mut reader = OldValueReader::new(snapshot);
            let get_old_value = move |key,
                                      query_ts,
                                      old_value_cache: &mut OldValueCache,
                                      statistics: &mut Statistics| {
                old_value_cache.access_count += 1;
                if let Some((old_value, mutation_type)) = old_value_cache.cache.remove(&key) {
                    match mutation_type {
                        MutationType::Insert => {
                            assert!(!old_value.exists());
                            return None;
                        }
                        MutationType::Put | MutationType::Delete => {
                            if let OldValue::Value {
                                start_ts,
                                short_value,
                            } = old_value
                            {
                                return short_value.or_else(|| {
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
                old_value_cache.miss_count += 1;
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
        let ts = Key::decode_ts_from(key.as_encoded()).unwrap();
        let upper = Key::from_encoded_slice(Key::truncate_ts_for(key.as_encoded()).unwrap())
            .append_ts(TimeStamp::zero());
        CursorBuilder::new(&self.snapshot, CF_WRITE)
            .fill_cache(false)
            .scan_mode(ScanMode::Mixed)
            .range(Some(key.clone()), Some(upper))
            .hint_max_ts(Some(ts))
            .build()
            .unwrap()
    }

    fn get_value_default(&mut self, key: &Key, statistics: &mut Statistics) -> Option<Value> {
        statistics.data.get += 1;
        let mut opts = ReadOptions::new();
        opts.set_fill_cache(false);
        self.snapshot
            .get_cf_opt(opts, CF_DEFAULT, &key)
            .unwrap()
            .map(|v| v.deref().to_vec())
    }

    /// Gets the latest value to the key with an older or equal version.
    ///
    /// The key passed in should be a key with a timestamp. This function will returns
    /// the latest value of the entry if the user key is the same to the given key and
    /// the timestamp is older than or equal to the timestamp in the given key.
    fn near_seek_old_value(
        &mut self,
        key: &Key,
        statistics: &mut Statistics,
    ) -> Result<Option<Value>> {
        let (user_key, seek_ts) = Key::split_on_ts_for(key.as_encoded()).unwrap();
        let mut write_cursor = self.new_write_cursor(key);
        if write_cursor.near_seek(key, &mut statistics.write)?
            && Key::is_user_key_eq(write_cursor.key(&mut statistics.write), user_key)
        {
            let mut old_value = None;
            while Key::is_user_key_eq(write_cursor.key(&mut statistics.write), user_key) {
                let write = WriteRef::parse(write_cursor.value(&mut statistics.write)).unwrap();
                old_value = match write.write_type {
                    WriteType::Put if write.check_gc_fence_as_latest_version(seek_ts) => {
                        match write.short_value {
                            Some(short_value) => Some(short_value.to_vec()),
                            None => {
                                let key =
                                    key.clone().truncate_ts().unwrap().append_ts(write.start_ts);
                                self.get_value_default(&key, statistics)
                            }
                        }
                    }
                    WriteType::Delete | WriteType::Put => None,
                    WriteType::Rollback | WriteType::Lock => {
                        if !write_cursor.next(&mut statistics.write) {
                            None
                        } else {
                            continue;
                        }
                    }
                };
                break;
            }
            Ok(old_value)
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
    use tikv::storage::txn::tests::*;

    #[test]
    fn test_register_and_deregister() {
        let (scheduler, mut rx) = tikv_util::worker::dummy_scheduler();
        let observer = CdcObserver::new(scheduler);
        let observe_id = ObserveID::new();
        let engine = TestEngineBuilder::new().build().unwrap().get_rocksdb();

        <CdcObserver as CmdObserver<RocksEngine>>::on_prepare_for_apply(
            &observer, observe_id, observe_id, 0,
        );
        <CdcObserver as CmdObserver<RocksEngine>>::on_apply_cmd(
            &observer,
            observe_id,
            observe_id,
            0,
            Cmd::new(0, RaftCmdRequest::default(), RaftCmdResponse::default()),
        );
        observer.on_flush_apply(engine);

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
            let mut statistics = Statistics::default();
            assert_eq!(
                old_value_reader
                    .near_seek_old_value(&key.clone().append_ts(ts.into()), &mut statistics)
                    .unwrap(),
                value
            );
        };

        must_prewrite_put(&engine, k, b"v1", k, 1);
        must_get_eq(2, None);
        must_get_eq(1, None);
        must_commit(&engine, k, 1, 1);
        must_get_eq(1, Some(b"v1".to_vec()));

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
        must_get_eq(5, None);
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

    #[test]
    fn test_old_value_reader_check_gc_fence() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let kv_engine = engine.get_rocksdb();

        let must_get_eq = |key: &[u8], ts: u64, value| {
            let mut old_value_reader = OldValueReader::new(Arc::new(kv_engine.snapshot()));
            let mut statistics = Statistics::default();
            assert_eq!(
                old_value_reader
                    .near_seek_old_value(&Key::from_raw(key).append_ts(ts.into()), &mut statistics)
                    .unwrap(),
                value
            );
        };

        // PUT,      Read
        //  `--------------^
        must_prewrite_put(&engine, b"k1", b"v1", b"k1", 10);
        must_commit(&engine, b"k1", 10, 20);
        must_cleanup_with_gc_fence(&engine, b"k1", 20, 0, 50, true);

        // PUT,      Read
        //  `---------^
        must_prewrite_put(&engine, b"k2", b"v2", b"k2", 11);
        must_commit(&engine, b"k2", 11, 20);
        must_cleanup_with_gc_fence(&engine, b"k2", 20, 0, 40, true);

        // PUT,      Read
        //  `-----^
        must_prewrite_put(&engine, b"k3", b"v3", b"k3", 12);
        must_commit(&engine, b"k3", 12, 20);
        must_cleanup_with_gc_fence(&engine, b"k3", 20, 0, 30, true);

        // PUT,   PUT,       Read
        //  `-----^ `----^
        must_prewrite_put(&engine, b"k4", b"v4", b"k4", 13);
        must_commit(&engine, b"k4", 13, 14);
        must_prewrite_put(&engine, b"k4", b"v4x", b"k4", 15);
        must_commit(&engine, b"k4", 15, 20);
        must_cleanup_with_gc_fence(&engine, b"k4", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&engine, b"k4", 20, 0, 30, true);

        // PUT,   DEL,       Read
        //  `-----^ `----^
        must_prewrite_put(&engine, b"k5", b"v5", b"k5", 13);
        must_commit(&engine, b"k5", 13, 14);
        must_prewrite_delete(&engine, b"k5", b"v5", 15);
        must_commit(&engine, b"k5", 15, 20);
        must_cleanup_with_gc_fence(&engine, b"k5", 14, 0, 20, false);
        must_cleanup_with_gc_fence(&engine, b"k5", 20, 0, 30, true);

        // PUT, LOCK, LOCK,   Read
        //  `------------------------^
        must_prewrite_put(&engine, b"k6", b"v6", b"k6", 16);
        must_commit(&engine, b"k6", 16, 20);
        must_prewrite_lock(&engine, b"k6", b"k6", 25);
        must_commit(&engine, b"k6", 25, 26);
        must_prewrite_lock(&engine, b"k6", b"k6", 28);
        must_commit(&engine, b"k6", 28, 29);
        must_cleanup_with_gc_fence(&engine, b"k6", 20, 0, 50, true);

        // PUT, LOCK,   LOCK,   Read
        //  `---------^
        must_prewrite_put(&engine, b"k7", b"v7", b"k7", 16);
        must_commit(&engine, b"k7", 16, 20);
        must_prewrite_lock(&engine, b"k7", b"k7", 25);
        must_commit(&engine, b"k7", 25, 26);
        must_cleanup_with_gc_fence(&engine, b"k7", 20, 0, 27, true);
        must_prewrite_lock(&engine, b"k7", b"k7", 28);
        must_commit(&engine, b"k7", 28, 29);

        // PUT,  Read
        //  * (GC fence ts is 0)
        must_prewrite_put(&engine, b"k8", b"v8", b"k8", 17);
        must_commit(&engine, b"k8", 17, 30);
        must_cleanup_with_gc_fence(&engine, b"k8", 30, 0, 0, true);

        // PUT, LOCK,     Read
        // `-----------^
        must_prewrite_put(&engine, b"k9", b"v9", b"k9", 18);
        must_commit(&engine, b"k9", 18, 20);
        must_prewrite_lock(&engine, b"k9", b"k9", 25);
        must_commit(&engine, b"k9", 25, 26);
        must_cleanup_with_gc_fence(&engine, b"k9", 20, 0, 27, true);

        let expected_results = vec![
            (b"k1", Some(b"v1")),
            (b"k2", None),
            (b"k3", None),
            (b"k4", None),
            (b"k5", None),
            (b"k6", Some(b"v6")),
            (b"k7", None),
            (b"k8", Some(b"v8")),
            (b"k9", None),
        ];

        for (k, v) in expected_results {
            must_get_eq(k, 40, v.map(|v| v.to_vec()));
        }
    }
}
