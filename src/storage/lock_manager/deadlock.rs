// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::client::Client;
use super::config::Config;
use super::metrics::*;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use super::{Error, Lock, Result};
use crate::server::resolve::StoreAddrResolver;
use futures::{Future, Sink, Stream};
use grpcio::{
    self, DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink, WriteFlags,
};
use kvproto::deadlock::*;
use kvproto::deadlock_grpc;
use pd_client::{RpcClient, INVALID_ID};
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tikv_util::collections::{HashMap, HashSet};
use tikv_util::future::paired_future_callback;
use tikv_util::security::SecurityManager;
use tikv_util::time;
use tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};
use tokio_core::reactor::Handle;
use tokio_timer::Interval;

/// `Locks` is a set of locks belonging to one transaction.
struct Locks {
    ts: u64,
    hashes: Vec<u64>,
    last_detect_time: time::Instant,
}

impl Locks {
    /// Creates a new `Locks`.
    fn new(ts: u64, hash: u64, last_detect_time: time::Instant) -> Self {
        Self {
            ts,
            hashes: vec![hash],
            last_detect_time,
        }
    }

    /// Pushes the `hash` if not exist and updates `last_detect_time`.
    fn push(&mut self, lock_hash: u64, now: time::Instant) {
        if !self.hashes.contains(&lock_hash) {
            self.hashes.push(lock_hash)
        }
        self.last_detect_time = now
    }

    /// Removes the `lock_hash` and returns true if the `Locks` is empty.
    fn remove(&mut self, lock_hash: u64) -> bool {
        if let Some(idx) = self.hashes.iter().position(|hash| *hash == lock_hash) {
            self.hashes.remove(idx);
        }
        self.hashes.is_empty()
    }

    /// Returns true if the `Locks` is expired.
    fn is_expired(&self, now: time::Instant, ttl: Duration) -> bool {
        now.duration_since(self.last_detect_time) >= ttl
    }
}

/// Used to detect the deadlock of wait-for-lock in the cluster.
pub struct DetectTable {
    /// Keeps the DAG of wait-for-lock. Every edge from `txn_ts` to `lock_ts` has a survival time -- `ttl`.
    /// When checking the deadlock, if the ttl has elpased, the corresponding edge will be removed.
    /// `last_detect_time` is the start time of the edge. `Detect` requests will refresh it.
    // txn_ts => (lock_ts => Locks)
    wait_for_map: HashMap<u64, HashMap<u64, Locks>>,

    /// The ttl of every edge.
    ttl: time::Duration,

    /// The time of last `active_expire`.
    last_active_expire: time::Instant,

    now: time::Instant,
}

impl DetectTable {
    /// Creates a auto-expiring detect table.
    pub fn new(ttl: time::Duration) -> Self {
        Self {
            wait_for_map: HashMap::default(),
            ttl,
            last_active_expire: time::Instant::now_coarse(),
            now: time::Instant::now_coarse(),
        }
    }

    /// Returns the key hash which causes deadlock.
    pub fn detect(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) -> Option<u64> {
        let _timer = DETECTOR_HISTOGRAM_VEC.detect.start_coarse_timer();
        TASK_COUNTER_VEC.detect.inc();

        self.now = time::Instant::now_coarse();
        self.active_expire();

        // If `txn_ts` is waiting for `lock_ts`, it won't cause deadlock.
        if self.register_if_existed(txn_ts, lock_ts, lock_hash) {
            return None;
        }

        if let Some(deadlock_key_hash) = self.do_detect(txn_ts, lock_ts) {
            return Some(deadlock_key_hash);
        }
        self.register(txn_ts, lock_ts, lock_hash);
        None
    }

    /// Checks if there is a edge from `wait_for_ts` to `txn_ts`.
    fn do_detect(&mut self, txn_ts: u64, wait_for_ts: u64) -> Option<u64> {
        let now = self.now;
        let ttl = self.ttl;

        let mut stack = vec![wait_for_ts];
        // Memorize the pushed vertexes to avoid duplicate search.
        let mut pushed: HashSet<u64> = HashSet::default();
        pushed.insert(wait_for_ts);
        while let Some(wait_for_ts) = stack.pop() {
            if let Some(wait_for) = self.wait_for_map.get_mut(&wait_for_ts) {
                // Remove expired edges.
                wait_for.retain(|_, locks| !locks.is_expired(now, ttl));
                if wait_for.is_empty() {
                    self.wait_for_map.remove(&wait_for_ts);
                } else {
                    for (lock_ts, locks) in wait_for {
                        if *lock_ts == txn_ts {
                            return Some(locks.hashes[0]);
                        }
                        if !pushed.contains(lock_ts) {
                            stack.push(*lock_ts);
                            pushed.insert(*lock_ts);
                        }
                    }
                }
            }
        }
        None
    }

    /// Returns true and adds to the detect table if `txn_ts` is waiting for `lock_ts`.
    fn register_if_existed(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) -> bool {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_ts) {
                locks.push(lock_hash, self.now);
                return true;
            }
        }
        false
    }

    /// Adds to the detect table. The edge from `txn_ts` to `lock_ts` must not exist.
    fn register(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) {
        let wait_for = self.wait_for_map.entry(txn_ts).or_default();
        assert!(!wait_for.contains_key(&lock_ts));
        let locks = Locks::new(lock_ts, lock_hash, self.now);
        wait_for.insert(locks.ts, locks);
    }

    /// Removes the corresponding wait_for_entry.
    pub fn clean_up_wait_for(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_ts) {
                if locks.remove(lock_hash) {
                    wait_for.remove(&lock_ts);
                    if wait_for.is_empty() {
                        self.wait_for_map.remove(&txn_ts);
                    }
                }
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
    fn active_expire(&mut self) {
        if self.wait_for_map.len() >= Self::ACTIVE_EXPIRE_THRESHOLD
            && self.now.duration_since(self.last_active_expire) >= Self::ACTIVE_EXPIRE_INTERVAL
        {
            let now = self.now;
            let ttl = self.ttl;
            for (_, wait_for) in self.wait_for_map.iter_mut() {
                wait_for.retain(|_, locks| !locks.is_expired(now, ttl));
            }
            self.wait_for_map.retain(|_, wait_for| !wait_for.is_empty());
            self.last_active_expire = self.now;
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
        cfg: &Config,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            pd_client,
            resolver,
            inner: Rc::new(RefCell::new(Inner::new(
                store_id,
                waiter_mgr_scheduler,
                security_mgr,
                cfg.wait_for_lock_timeout,
            ))),
            monitor_membership_interval: cfg.monitor_membership_interval,
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
                RpcStatusCode::FAILED_PRECONDITION,
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
                RpcStatusCode::RESOURCE_EXHAUSTED,
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
                RpcStatusCode::RESOURCE_EXHAUSTED,
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
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .get(&1)
                .unwrap()
                .hashes
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
                .get(&1)
                .unwrap()
                .hashes
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
                .get(&1)
                .unwrap()
                .hashes
                .len(),
            2
        );

        // Different lock_ts grows the map.
        assert_eq!(detect_table.detect(3, 2, 2), None);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().len(), 2);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3)
                .unwrap()
                .get(&2)
                .unwrap()
                .hashes
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
                .get(&1)
                .unwrap()
                .hashes
                .len(),
            1
        );
        detect_table.clean_up_wait_for(3, 1, 2);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().len(), 1);
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

        // `Detect` updates the last_detect_time, so the entry won't be removed.
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
        assert_eq!(detect_table.wait_for_map.get(&1).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&1)
                .unwrap()
                .get(&3)
                .unwrap()
                .hashes
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
