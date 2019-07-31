// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::client::Client;
use super::metrics::*;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use super::{Error, Lock, Result};
use crate::pd::{RpcClient, INVALID_ID};
use crate::server::resolve::StoreAddrResolver;
use crate::tikv_util::collections::{HashMap, HashSet};
use crate::tikv_util::future::paired_future_callback;
use crate::tikv_util::security::SecurityManager;
use crate::tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};
use futures::{Future, Sink, Stream};
use grpcio::{
    self, DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode::*, UnarySink, WriteFlags,
};
use kvproto::deadlock::*;
use kvproto::deadlock_grpc;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tikv_util::time;
use tokio_core::reactor::Handle;
use tokio_timer::Interval;

/// Used to detect the deadlock of wait-for-lock in the cluster.
pub struct DetectTable {
    /// Keeps the DAG of wait-for-lock. Every edge from `txn_ts` to `waited_txns` has a survival time -- `ttl`.
    /// When checking the deadlock, if the ttl has elpased, the corresponding edge will be removed.
    /// `last_detect_ts` is the start time of the edge. `Detect` requests will refresh it.
    // txn_ts => waited_txn,which means the txn_ts is waiting for the waited_txns
    wait_for_map: HashMap<u64, WaitForTxns>,

    /// The ttl of every edge.
    ttl: time::Duration,

    /// The time of last `active_expire`.
    last_active_expire: time::Instant,
}

/// `WaitForTxns` maintain a transaction list.
#[derive(Default)]
struct WaitForTxns {
    // txn_ts => transaction's information.
    txns: HashMap<u64, DetectedTxn>,
}

/// `DetectedTxn` maintain a lock list which belongs to one transaction.
struct DetectedTxn {
    locks: HashSet<u64>, // the locked keys
    last_detect_ts: time::Instant,
}

impl DetectedTxn {
    /// Creates a new `DetectedTxn`.
    fn new(lock_key: u64, detect_ts: time::Instant) -> Self {
        let mut locks: HashSet<u64> = HashSet::default();
        locks.insert(lock_key);
        Self {
            locks,
            last_detect_ts: detect_ts,
        }
    }

    /// Pushes the lock and updates `last_detect_ts`.
    fn push(&mut self, lock_key: u64, detect_ts: time::Instant) {
        self.locks.insert(lock_key);
        self.last_detect_ts = detect_ts
    }

    /// Removes the `lock_key` and returns true if the `Locks` is empty.
    /// It happens when the client rollback a pessmistic lock.
    fn remove(&mut self, lock_key: u64) -> bool {
        self.locks.remove(&lock_key);
        self.locks.is_empty()
    }

    /// Returns true if the `Locks` is expired since last detection.
    fn is_expired(&mut self, now: time::Instant, ttl: Duration) -> bool {
        now.duration_since(self.last_detect_ts) >= ttl
    }

    fn first_locked_key(&self) -> u64 {
        for lock in self.locks.iter() {
            return *lock;
        }
        unreachable!();
    }
}

impl WaitForTxns {
    /// clean the timeout transactions, return true once the current trasaction is empty.
    fn clean_expired_txns(&mut self, now: time::Instant, ttl: Duration) -> bool {
        self.txns.retain(|_, locks| !locks.is_expired(now, ttl));
        self.txns.is_empty()
    }

    /// register the current key if the corresponding transaction already exist.
    fn register_key_if_txn_exist(
        &mut self,
        ts: u64,
        locked_key: u64,
        detect_ts: time::Instant,
    ) -> bool {
        self.txns
            .get_mut(&ts)
            .map(|txn| txn.push(locked_key, detect_ts))
            .is_some()
    }

    /// register the current transaction with a locked key, the current transaction should not exist before.
    fn new_wait_for_txn(&mut self, ts: u64, lock: u64, detected_time: time::Instant) {
        assert!(!self.txns.contains_key(&ts));
        let waited_txn_keys = DetectedTxn::new(lock, detected_time);
        self.txns.insert(ts, waited_txn_keys);
    }

    /// clean the locked keys which the caller is waiting for, returns true once there is no more transaction.
    fn clean_wait_for_locked_keys(&mut self, wait_for_ts: u64, wait_for_lock: u64) -> bool {
        let txn = self.txns.get_mut(&wait_for_ts);
        let txn_empty = txn.and_then(|locks| {
            let is_empty = locks.remove(wait_for_lock);
            Some(is_empty)
        });
        match txn_empty {
            Some(true) => {
                self.txns.remove(&wait_for_ts);
                self.txns.is_empty()
            }
            Some(false) => false,
            None => true,
        }
    }
}

impl DetectTable {
    /// Creates a auto-expiring detect table.
    pub fn new(ttl: time::Duration) -> Self {
        Self {
            wait_for_map: HashMap::default(),
            ttl,
            last_active_expire: time::Instant::now_coarse(),
        }
    }

    /// Returns the key hash which causes deadlock.
    pub fn detect(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) -> Option<u64> {
        let _timer = DETECTOR_HISTOGRAM_VEC.detect.start_coarse_timer();
        TASK_COUNTER_VEC.detect.inc();

        let detect_ts = time::Instant::now_coarse();
        self.active_expire(detect_ts);

        // If `txn_ts` is waiting for `lock_ts`, it won't cause deadlock.
        if self.register_if_existed(txn_ts, lock_ts, lock_hash, detect_ts) {
            return None;
        }

        let dead_lock = self.do_detect(txn_ts, lock_ts, detect_ts);
        if dead_lock.is_none() {
            self.register(txn_ts, lock_ts, lock_hash, detect_ts);
        }
        dead_lock
    }

    /// Checks if there is a edge from `wait_for_ts` to `txn_ts`.
    fn do_detect(
        &mut self,
        txn_ts: u64,
        wait_for_ts: u64,
        detect_ts: time::Instant,
    ) -> Option<u64> {
        let ttl = self.ttl;
        let mut stack = vec![wait_for_ts];
        // visited collected the txn which is already visited by BFS.
        let mut visited: HashSet<u64> = HashSet::default();
        visited.insert(wait_for_ts);
        while let Some(wait_for_ts) = stack.pop() {
            let txns = if let Some(wait_for) = self.wait_for_map.get_mut(&wait_for_ts) {
                wait_for
            } else {
                continue;
            };

            // Remove expired edges.
            let is_empty = txns.clean_expired_txns(detect_ts, ttl);
            if is_empty {
                self.wait_for_map.remove(&wait_for_ts);
                continue;
            }
            // do BFS
            for (ts, locks) in txns.txns.iter() {
                if *ts == txn_ts {
                    return Some(locks.first_locked_key());
                }
                if !visited.contains(ts) {
                    stack.push(*ts);
                    visited.insert(*ts);
                }
            }
        }
        None
    }

    /// Returns true and adds to the detect table if `txn_ts` is waiting for `lock_ts`.
    fn register_if_existed(
        &mut self,
        txn_ts: u64,
        lock_ts: u64,
        lock_hash: u64,
        detect_ts: time::Instant,
    ) -> bool {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            return wait_for.register_key_if_txn_exist(lock_ts, lock_hash, detect_ts);
        }
        false
    }

    /// Adds to the detect table. The edge from `txn_ts` to `lock_ts` must not exist.
    fn register(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64, detect_ts: time::Instant) {
        let wait_for = self.wait_for_map.entry(txn_ts).or_default();
        wait_for.new_wait_for_txn(lock_ts, lock_hash, detect_ts);
    }

    /// Removes the corresponding wait_for_entry.
    pub fn clean_up_wait_for(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            let is_empty = wait_for.clean_wait_for_locked_keys(lock_ts, lock_hash);
            if is_empty {
                self.wait_for_map.remove(&txn_ts);
            }
        }
        TASK_COUNTER_VEC.clean_up_wait_for.inc();
    }

    /// Removes the entries of the transaction.
    pub fn clean_up(&mut self, txn_ts: u64) {
        self.wait_for_map.remove(&txn_ts);
        TASK_COUNTER_VEC.clean_up.inc();
    }

    /// Clears the whole detect table.
    pub fn clear(&mut self) {
        self.wait_for_map.clear();
    }

    /// The threshold of detect table size to trigger `active_expire`.
    const ACTIVE_EXPIRE_THRESHOLD: usize = 100000;
    /// The interval between `active_expire`.
    const ACTIVE_EXPIRE_INTERVAL: Duration = Duration::from_secs(3600);

    /// Iterates the whole table to remove all expired entries.
    fn active_expire(&mut self, detect_ts: time::Instant) {
        if self.wait_for_map.len() >= Self::ACTIVE_EXPIRE_THRESHOLD
            && detect_ts.duration_since(self.last_active_expire) >= Self::ACTIVE_EXPIRE_INTERVAL
        {
            let ttl = self.ttl;
            self.wait_for_map
                .retain(|_, wait_for| !wait_for.clean_expired_txns(detect_ts, ttl));
            self.last_active_expire = detect_ts;
        }
    }
}

#[derive(Debug)]
pub enum DetectType {
    Detect,
    CleanUpWaitFor,
    CleanUp,
}

pub enum Task {
    Detect {
        tp: DetectType,
        txn_ts: u64,
        lock: Lock,
    },
    DetectRpc {
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Detect { tp, txn_ts, lock } => write!(
                f,
                "Detect {{ tp: {:?}, txn_ts: {}, lock: {:?} }}",
                tp, txn_ts, lock
            ),
            Task::DetectRpc { .. } => write!(f, "detect rpc"),
        }
    }
}

#[derive(Clone)]
pub struct Scheduler(FutureScheduler<Task>);

impl Scheduler {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self(scheduler)
    }

    fn notify_scheduler(&self, task: Task) {
        if let Err(Stopped(task)) = self.0.schedule(task) {
            error!("failed to send task to deadlock_detector"; "task" => %task);
        }
    }

    pub fn detect(&self, txn_ts: u64, lock: Lock) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::Detect,
            txn_ts,
            lock,
        });
    }

    pub fn clean_up_wait_for(&self, txn_ts: u64, lock: Lock) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUpWaitFor,
            txn_ts,
            lock,
        });
    }

    pub fn clean_up(&self, txn_ts: u64) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUp,
            txn_ts,
            lock: Lock::default(),
        });
    }
}

#[derive(Clone)]
struct Inner {
    store_id: u64,
    // (leader_id, leader_addr)
    leader_info: Option<(u64, String)>,
    leader_client: Option<Client>,
    detect_table: Rc<RefCell<DetectTable>>,
    waiter_mgr_scheduler: WaiterMgrScheduler,
    security_mgr: Arc<SecurityManager>,
}

impl Inner {
    fn new(
        store_id: u64,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        security_mgr: Arc<SecurityManager>,
        ttl: u64,
    ) -> Self {
        Self {
            store_id,
            leader_info: None,
            leader_client: None,
            detect_table: Rc::new(RefCell::new(DetectTable::new(time::Duration::from_millis(
                ttl,
            )))),
            waiter_mgr_scheduler,
            security_mgr,
        }
    }

    fn leader_id(&self) -> u64 {
        self.leader_info.as_ref().unwrap().0
    }

    fn leader_addr(&self) -> &str {
        &self.leader_info.as_ref().unwrap().1
    }

    fn is_leader(&self) -> bool {
        self.leader_info.is_some() && self.leader_id() == self.store_id
    }

    fn reset(&mut self) {
        self.leader_client.take();
        self.detect_table.borrow_mut().clear();
    }

    fn change_role_if_needed(&mut self, leader_id: u64, leader_addr: String) {
        match self.leader_info {
            Some((id, ref addr)) if id == leader_id && *addr == leader_addr => {
                debug!("leader not change"; "leader_id" => leader_id, "leader_addr" => leader_addr);
            }
            _ => {
                if self.store_id == leader_id {
                    info!("become the leader of deadlock detector!"; "self_id" => self.store_id);
                } else {
                    if self.is_leader() {
                        info!("changed from leader to follower"; "self_id" => self.store_id);
                    }
                    info!("leader changed"; "leader_id" => leader_id);
                }
                self.reset();
                self.leader_info.replace((leader_id, leader_addr));
            }
        }
    }

    fn reconnect_leader(&mut self, handle: &Handle) {
        assert!(self.leader_client.is_none());
        ERROR_COUNTER_VEC.reconnect_leader.inc();
        let mut leader_client = Client::new(Arc::clone(&self.security_mgr), self.leader_addr());
        let waiter_mgr_scheduler = self.waiter_mgr_scheduler.clone();
        let (send, recv) = leader_client.register_detect_handler(Box::new(move |mut resp| {
            let WaitForEntry {
                txn,
                wait_for_txn,
                key_hash,
                ..
            } = resp.take_entry();
            waiter_mgr_scheduler.deadlock(
                txn,
                Lock {
                    ts: wait_for_txn,
                    hash: key_hash,
                },
                resp.get_deadlock_key_hash(),
            )
        }));
        handle.spawn(send.map_err(|e| error!("leader client failed"; "err" => ?e)));
        // No need to log it again.
        handle.spawn(recv.map_err(|_| ()));
        self.leader_client = Some(leader_client);
        info!("reconnect leader succeeded"; "leader_id" => self.leader_id());
    }
}

pub struct Detector<S: StoreAddrResolver + 'static> {
    pd_client: Arc<RpcClient>,
    resolver: S,
    inner: Rc<RefCell<Inner>>,
    monitor_membership_interval: u64,
    is_initialized: bool,
}

unsafe impl<S: StoreAddrResolver + 'static> Send for Detector<S> {}

impl<S: StoreAddrResolver + 'static> Detector<S> {
    pub fn new(
        store_id: u64,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        security_mgr: Arc<SecurityManager>,
        pd_client: Arc<RpcClient>,
        resolver: S,
        monitor_membership_interval: u64,
        ttl: u64,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            pd_client,
            resolver,
            inner: Rc::new(RefCell::new(Inner::new(
                store_id,
                waiter_mgr_scheduler,
                security_mgr,
                ttl,
            ))),
            monitor_membership_interval,
            is_initialized: false,
        }
    }

    fn monitor_membership_change(
        pd_client: &Arc<RpcClient>,
        resolver: &S,
        inner: &Rc<RefCell<Inner>>,
    ) -> Result<()> {
        let _timer = DETECTOR_HISTOGRAM_VEC
            .monitor_membership_change
            .start_coarse_timer();
        let (_, leader) = pd_client.get_region_and_leader(b"")?;
        if let Some(leader) = leader {
            let leader_id = leader.get_store_id();
            return match wait_op!(|cb| resolver
                .resolve(leader_id, cb)
                .map_err(|e| Error::Other(box_err!(e))))
            {
                Some(ref addr) if addr.is_ok() => {
                    inner
                        .borrow_mut()
                        .change_role_if_needed(leader_id, addr.as_ref().unwrap().to_owned());
                    Ok(())
                }
                _ => Err(box_err!("failed to resolve leader address")),
            };
        }
        warn!("leader not found");
        ERROR_COUNTER_VEC.leader_not_found.inc();
        Ok(())
    }

    fn schedule_membership_change_monitor(&self, handle: &Handle) {
        info!("schedule membership change monitor");
        let pd_client = Arc::clone(&self.pd_client);
        let resolver = self.resolver.clone();
        let inner = Rc::clone(&self.inner);
        let timer = Interval::new(
            Instant::now(),
            Duration::from_millis(self.monitor_membership_interval),
        )
        .for_each(move |_| {
            if let Err(e) = Self::monitor_membership_change(&pd_client, &resolver, &inner) {
                error!("monitor membership change failed"; "err" => ?e);
                ERROR_COUNTER_VEC.monitor_membership_change.inc();
            }
            Ok(())
        })
        .map_err(|e| panic!("unexpected err: {:?}", e));
        handle.spawn(timer);
    }

    fn initialize(&mut self, handle: &Handle) {
        assert!(!self.is_initialized);
        // Get leader info now because tokio_timer::Interval can't execute immediately.
        let _ = Self::monitor_membership_change(&self.pd_client, &self.resolver, &self.inner);
        self.schedule_membership_change_monitor(handle);
        self.is_initialized = true;
    }

    fn handle_detect(&self, handle: &Handle, tp: DetectType, txn_ts: u64, lock: Lock) {
        let mut inner = self.inner.borrow_mut();
        if inner.is_leader() {
            match tp {
                DetectType::Detect => {
                    if let Some(deadlock_key_hash) = inner
                        .detect_table
                        .borrow_mut()
                        .detect(txn_ts, lock.ts, lock.hash)
                    {
                        inner
                            .waiter_mgr_scheduler
                            .deadlock(txn_ts, lock, deadlock_key_hash);
                    }
                }
                DetectType::CleanUpWaitFor => inner
                    .detect_table
                    .borrow_mut()
                    .clean_up_wait_for(txn_ts, lock.ts, lock.hash),
                DetectType::CleanUp => inner.detect_table.borrow_mut().clean_up(txn_ts),
            }
        } else {
            for _ in 0..2 {
                if inner.leader_client.is_none() && inner.leader_info.is_some() {
                    inner.reconnect_leader(handle);
                }
                if let Some(leader_client) = &inner.leader_client {
                    let tp = match tp {
                        DetectType::Detect => DeadlockRequestType::Detect,
                        DetectType::CleanUpWaitFor => DeadlockRequestType::CleanUpWaitFor,
                        DetectType::CleanUp => DeadlockRequestType::CleanUp,
                    };
                    let mut entry = WaitForEntry::default();
                    entry.set_txn(txn_ts);
                    entry.set_wait_for_txn(lock.ts);
                    entry.set_key_hash(lock.hash);
                    let mut req = DeadlockRequest::default();
                    req.set_tp(tp);
                    req.set_entry(entry);
                    if leader_client.detect(req).is_ok() {
                        return;
                    }
                    inner.leader_client.take();
                }
            }
            warn!("detect request dropped"; "tp" => ?tp, "txn_ts" =>  txn_ts, "lock" => ?lock);
            ERROR_COUNTER_VEC.dropped.inc();
        }
    }

    fn handle_detect_rpc(
        &self,
        handle: &Handle,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        if !self.inner.borrow().is_leader() {
            let status = RpcStatus::new(
                GRPC_STATUS_FAILED_PRECONDITION,
                Some("i'm not the leader of deadlock detector".to_string()),
            );
            handle.spawn(sink.fail(status).map_err(|_| ()));
            ERROR_COUNTER_VEC.not_leader.inc();
            return;
        }

        let inner = Rc::clone(&self.inner);
        let s = stream
            .map_err(Error::Grpc)
            .and_then(move |mut req| {
                // It's possible the leader changes after registering this handler.
                let inner = inner.borrow();
                if !inner.is_leader() {
                    ERROR_COUNTER_VEC.not_leader.inc();
                    return Err(Error::Other(box_err!("leader changed")));
                }

                let WaitForEntry {
                    txn,
                    wait_for_txn,
                    key_hash,
                    ..
                } = req.get_entry();

                let mut detect_table = inner.detect_table.borrow_mut();
                let res = match req.get_tp() {
                    DeadlockRequestType::Detect => {
                        if let Some(deadlock_key_hash) =
                            detect_table.detect(*txn, *wait_for_txn, *key_hash)
                        {
                            let mut resp = DeadlockResponse::default();
                            resp.set_entry(req.take_entry());
                            resp.set_deadlock_key_hash(deadlock_key_hash);
                            Some((resp, WriteFlags::default()))
                        } else {
                            None
                        }
                    }

                    DeadlockRequestType::CleanUpWaitFor => {
                        detect_table.clean_up_wait_for(*txn, *wait_for_txn, *key_hash);
                        None
                    }

                    DeadlockRequestType::CleanUp => {
                        detect_table.clean_up(*txn);
                        None
                    }
                };
                Ok(res)
            })
            .filter_map(|resp| resp);
        handle.spawn(
            sink.sink_map_err(Error::Grpc)
                .send_all(s)
                .map(|_| ())
                .map_err(|_| ()),
        );
    }
}

impl<S: StoreAddrResolver + 'static> FutureRunnable<Task> for Detector<S> {
    fn run(&mut self, task: Task, handle: &Handle) {
        if !self.is_initialized {
            self.initialize(handle);
        }

        match task {
            Task::Detect { tp, txn_ts, lock } => {
                self.handle_detect(handle, tp, txn_ts, lock);
            }
            Task::DetectRpc { stream, sink } => {
                self.handle_detect_rpc(handle, stream, sink);
            }
        }
    }
}

#[derive(Clone)]
pub struct Service {
    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: Scheduler,
}

impl Service {
    pub fn new(waiter_mgr_scheduler: WaiterMgrScheduler, detector_scheduler: Scheduler) -> Self {
        Self {
            waiter_mgr_scheduler,
            detector_scheduler,
        }
    }
}

impl deadlock_grpc::Deadlock for Service {
    // TODO: remove it
    fn get_wait_for_entries(
        &mut self,
        ctx: RpcContext<'_>,
        _req: WaitForEntriesRequest,
        sink: UnarySink<WaitForEntriesResponse>,
    ) {
        let (cb, f) = paired_future_callback();
        if !self.waiter_mgr_scheduler.dump_wait_table(cb) {
            let status = RpcStatus::new(
                GRPC_STATUS_RESOURCE_EXHAUSTED,
                Some("waiter manager has stopped".to_owned()),
            );
            ctx.spawn(sink.fail(status).map_err(|_| ()))
        } else {
            ctx.spawn(
                f.map_err(Error::from)
                    .map(|v| {
                        let mut resp = WaitForEntriesResponse::default();
                        resp.set_entries(v.into());
                        resp
                    })
                    .and_then(|resp| sink.success(resp).map_err(Error::Grpc))
                    .map_err(move |e| {
                        debug!("get_wait_for_entries failed"; "err" => ?e);
                    }),
            );
        }
    }

    fn detect(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        let task = Task::DetectRpc { stream, sink };
        if let Err(Stopped(Task::DetectRpc { sink, .. })) = self.detector_scheduler.0.schedule(task)
        {
            let status = RpcStatus::new(
                GRPC_STATUS_RESOURCE_EXHAUSTED,
                Some("deadlock detector has stopped".to_owned()),
            );
            ctx.spawn(sink.fail(status).map_err(|_| ()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_detect_table() {
        let mut detect_table = DetectTable::new(Duration::from_secs(10));

        // Deadlock: 1 -> 2 -> 1
        assert_eq!(detect_table.detect(1, 2, 2), None);
        assert_eq!(detect_table.detect(2, 1, 1).unwrap(), 2);
        // Deadlock: 1 -> 2 -> 3 -> 1
        assert_eq!(detect_table.detect(2, 3, 3), None);
        assert_eq!(detect_table.detect(3, 1, 1).unwrap(), 3);
        detect_table.clean_up(2);
        assert_eq!(detect_table.wait_for_map.contains_key(&2), false);

        // After cycle is broken, no deadlock.
        assert_eq!(detect_table.detect(3, 1, 1), None);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().txns.len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .txns
                .get(&1)
                .unwrap()
                .locks
                .len(),
            1
        );

        // Different key_hash grows the list.
        assert_eq!(detect_table.detect(3, 1, 2), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .txns
                .get(&1)
                .unwrap()
                .locks
                .len(),
            2
        );

        // Same key_hash doesn't grow the list.
        assert_eq!(detect_table.detect(3, 1, 2), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .txns
                .get(&1)
                .unwrap()
                .locks
                .len(),
            2
        );

        // Different lock_ts grows the map.
        assert_eq!(detect_table.detect(3, 2, 2), None);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().txns.len(), 2);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .txns
                .get(&2)
                .unwrap()
                .locks
                .len(),
            1
        );

        // Clean up entries shrinking the map.
        detect_table.clean_up_wait_for(3, 1, 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .txns
                .get(&1)
                .unwrap()
                .locks
                .len(),
            1
        );
        detect_table.clean_up_wait_for(3, 1, 2);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().txns.len(), 1);
        detect_table.clean_up_wait_for(3, 2, 2);
        assert_eq!(detect_table.wait_for_map.contains_key(&3), false);

        // Clean up non-exist entry
        detect_table.clean_up(3);
        detect_table.clean_up_wait_for(3, 1, 1);
    }

    #[test]
    fn test_detect_table_expire() {
        let mut detect_table = DetectTable::new(Duration::from_millis(100));

        // Deadlock
        assert!(detect_table.detect(1, 2, 1).is_none());
        assert!(detect_table.detect(2, 1, 2).is_some());
        // After sleep, the expired entry has been removed. So there is no deadlock.
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(detect_table.wait_for_map.len(), 1);
        assert!(detect_table.detect(2, 1, 2).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);

        // `Detect` updates the last_detect_ts, so the entry won't be removed.
        detect_table.clear();
        assert!(detect_table.detect(1, 2, 1).is_none());
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(1, 2, 1).is_none());
        assert!(detect_table.detect(2, 1, 2).is_some());

        // Remove expired entry shrinking the map.
        detect_table.clear();
        assert!(detect_table.detect(1, 2, 1).is_none());
        assert!(detect_table.detect(1, 3, 1).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(1, 3, 2).is_none());
        assert!(detect_table.detect(2, 1, 2).is_none());
        assert_eq!(detect_table.wait_for_map.get(&1).unwrap().txns.len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&1)
                .unwrap()
                .txns
                .get(&3)
                .unwrap()
                .locks
                .len(),
            2
        );
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(3, 2, 3).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 2);
        assert!(detect_table.detect(3, 1, 3).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);
    }
}
