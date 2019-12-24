// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::client::{self, Client};
use super::config::Config;
use super::metrics::*;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use super::{Error, Result};
use crate::raftstore::coprocessor::{Coprocessor, ObserverContext, RoleObserver};
use crate::server::resolve::StoreAddrResolver;
use crate::storage::lock_manager::Lock;
use futures::{Future, Sink, Stream};
use grpcio::{
    self, DuplexSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink,
    WriteFlags,
};
use kvproto::deadlock::*;
use kvproto::metapb::Region;
use pd_client::{PdClient, INVALID_ID};
use raft::StateRole;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::Arc;
use tikv_util::collections::{HashMap, HashSet};
use tikv_util::future::paired_future_callback;
use tikv_util::security::SecurityManager;
use tikv_util::time::{Duration, Instant};
use tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};
use tokio_core::reactor::Handle;
use txn_types::TimeStamp;

/// `Locks` is a set of locks belonging to one transaction.
struct Locks {
    ts: TimeStamp,
    hashes: Vec<u64>,
    last_detect_time: Instant,
}

impl Locks {
    /// Creates a new `Locks`.
    fn new(ts: TimeStamp, hash: u64, last_detect_time: Instant) -> Self {
        Self {
            ts,
            hashes: vec![hash],
            last_detect_time,
        }
    }

    /// Pushes the `hash` if not exist and updates `last_detect_time`.
    fn push(&mut self, lock_hash: u64, now: Instant) {
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
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.duration_since(self.last_detect_time) >= ttl
    }
}

/// Used to detect the deadlock of wait-for-lock in the cluster.
pub struct DetectTable {
    /// Keeps the DAG of wait-for-lock. Every edge from `txn_ts` to `lock_ts` has a survival time -- `ttl`.
    /// When checking the deadlock, if the ttl has elpased, the corresponding edge will be removed.
    /// `last_detect_time` is the start time of the edge. `Detect` requests will refresh it.
    // txn_ts => (lock_ts => Locks)
    wait_for_map: HashMap<TimeStamp, HashMap<TimeStamp, Locks>>,

    /// The ttl of every edge.
    ttl: Duration,

    /// The time of last `active_expire`.
    last_active_expire: Instant,

    now: Instant,
}

impl DetectTable {
    /// Creates a auto-expiring detect table.
    pub fn new(ttl: Duration) -> Self {
        Self {
            wait_for_map: HashMap::default(),
            ttl,
            last_active_expire: Instant::now_coarse(),
            now: Instant::now_coarse(),
        }
    }

    /// Returns the key hash which causes deadlock.
    pub fn detect(&mut self, txn_ts: TimeStamp, lock_ts: TimeStamp, lock_hash: u64) -> Option<u64> {
        DETECTOR_HISTOGRAM_METRICS.with(|m| {
            let res = m
                .detect
                .observe_closure_duration(|| self.detect_inner(txn_ts, lock_ts, lock_hash));
            m.may_flush_all();
            res
        })
    }

    fn detect_inner(
        &mut self,
        txn_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
    ) -> Option<u64> {
        TASK_COUNTER_METRICS.with(|m| {
            m.detect.inc();
            m.may_flush_all()
        });

        self.now = Instant::now_coarse();
        self.active_expire();

        // If `txn_ts` is waiting for `lock_ts`, it won't cause deadlock.
        if self.register_if_existed(txn_ts, lock_ts, lock_hash) {
            return None;
        }

        if let Some(deadlock_key_hash) = self.do_detect(txn_ts, lock_ts) {
            ERROR_COUNTER_METRICS.with(|m| {
                m.deadlock.inc();
                m.may_flush_all()
            });
            return Some(deadlock_key_hash);
        }
        self.register(txn_ts, lock_ts, lock_hash);
        None
    }

    /// Checks if there is an edge from `wait_for_ts` to `txn_ts`.
    fn do_detect(&mut self, txn_ts: TimeStamp, wait_for_ts: TimeStamp) -> Option<u64> {
        let now = self.now;
        let ttl = self.ttl;

        let mut stack = vec![wait_for_ts];
        // Memorize the pushed vertexes to avoid duplicate search.
        let mut pushed: HashSet<TimeStamp> = HashSet::default();
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
    fn register_if_existed(
        &mut self,
        txn_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
    ) -> bool {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_ts) {
                locks.push(lock_hash, self.now);
                return true;
            }
        }
        false
    }

    /// Adds to the detect table. The edge from `txn_ts` to `lock_ts` must not exist.
    fn register(&mut self, txn_ts: TimeStamp, lock_ts: TimeStamp, lock_hash: u64) {
        let wait_for = self.wait_for_map.entry(txn_ts).or_default();
        assert!(!wait_for.contains_key(&lock_ts));
        let locks = Locks::new(lock_ts, lock_hash, self.now);
        wait_for.insert(locks.ts, locks);
    }

    /// Removes the corresponding wait_for_entry.
    fn clean_up_wait_for(&mut self, txn_ts: TimeStamp, lock_ts: TimeStamp, lock_hash: u64) {
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
        TASK_COUNTER_METRICS.with(|m| {
            m.clean_up_wait_for.inc();
            m.may_flush_all()
        });
    }

    /// Removes the entries of the transaction.
    fn clean_up(&mut self, txn_ts: TimeStamp) {
        self.wait_for_map.remove(&txn_ts);
        TASK_COUNTER_METRICS.with(|m| {
            m.clean_up.inc();
            m.may_flush_all()
        });
    }

    /// Clears the whole detect table.
    fn clear(&mut self) {
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

/// The leader of the region containing the LEADER_KEY is the leader of deadlock detector.
const LEADER_KEY: &[u8] = b"";

/// Returns true if the region containing the LEADER_KEY.
fn is_leader_region(region: &'_ Region) -> bool {
    // The key range of a new created region is empty which misleads the leader
    // of the deadlock detector stepping down.
    //
    // If the peers of a region is not empty, the region info is complete.
    !region.get_peers().is_empty()
        && region.get_start_key() <= LEADER_KEY
        && (region.get_end_key().is_empty() || LEADER_KEY < region.get_end_key())
}

/// The role of the detector.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Role {
    /// The node is the leader of the detector.
    Leader,
    /// The node is a follower of the leader.
    Follower,
}

impl Default for Role {
    fn default() -> Role {
        Role::Follower
    }
}

impl From<StateRole> for Role {
    fn from(role: StateRole) -> Role {
        match role {
            StateRole::Leader => Role::Leader,
            _ => Role::Follower,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DetectType {
    Detect,
    CleanUpWaitFor,
    CleanUp,
}

pub enum Task {
    /// The detect request of itself.
    Detect {
        tp: DetectType,
        txn_ts: TimeStamp,
        lock: Lock,
    },
    /// The detect request of other nodes.
    DetectRpc {
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    },
    /// If the node has the leader region and the role of the node changes,
    /// a `ChangeRole` task will be scheduled.
    ///
    /// It's the only way to change the node from leader to follower, and vice versa.
    ChangeRole(Role),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Detect { tp, txn_ts, lock } => write!(
                f,
                "Detect {{ tp: {:?}, txn_ts: {}, lock: {:?} }}",
                tp, txn_ts, lock
            ),
            Task::DetectRpc { .. } => write!(f, "Detect Rpc"),
            Task::ChangeRole(role) => write!(f, "ChangeRole {{ role: {:?} }}", role),
        }
    }
}

/// `Scheduler` is the wrapper of the `FutureScheduler<Task>` to simplify scheduling tasks
/// to the deadlock detector.
#[derive(Clone)]
pub struct Scheduler(FutureScheduler<Task>);

impl Scheduler {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self(scheduler)
    }

    fn notify_scheduler(&self, task: Task) {
        // Only when the deadlock detector is stopped, an error will be returned.
        // So there is no need to handle the error.
        if let Err(Stopped(task)) = self.0.schedule(task) {
            error!("failed to send task to deadlock_detector"; "task" => %task);
        }
    }

    pub fn detect(&self, txn_ts: TimeStamp, lock: Lock) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::Detect,
            txn_ts,
            lock,
        });
    }

    pub fn clean_up_wait_for(&self, txn_ts: TimeStamp, lock: Lock) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUpWaitFor,
            txn_ts,
            lock,
        });
    }

    pub fn clean_up(&self, txn_ts: TimeStamp) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUp,
            txn_ts,
            lock: Lock::default(),
        });
    }

    fn change_role(&self, role: Role) {
        self.notify_scheduler(Task::ChangeRole(role));
    }
}

impl Coprocessor for Scheduler {}

/// Implements observer traits for `Scheduler`.
/// If the role of the node in the leader region changes, notifys the deadlock detector.
///
/// If the leader region is merged or splited in the node, the role of the node won't change.
/// If the leader region is removed and the node is the leader, it will change to follower first.
/// So there is no need to observe region change events.
impl RoleObserver for Scheduler {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role: StateRole) {
        if is_leader_region(ctx.region()) {
            self.change_role(role.into());
        }
    }
}

struct Inner {
    /// The role of the deadlock detector. Default is `Role::Follower`.
    role: Role,

    detect_table: DetectTable,
}

/// Detector is used to detect deadlocks between transactions. There is a leader
/// in the cluster which collects all `wait_for_entry` from other followers.
pub struct Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
    /// The store id of the node.
    store_id: u64,
    /// Used to create clients to the leader.
    env: Arc<Environment>,
    /// The leader's id and address if exists.
    leader_info: Option<(u64, String)>,
    /// The connection to the leader.
    leader_client: Option<Client>,
    /// Used to get the leader of leader region from PD.
    pd_client: Arc<P>,
    /// Used to resolve store address.
    resolver: S,
    /// Used to connect other nodes.
    security_mgr: Arc<SecurityManager>,
    /// Used to schedule Deadlock msgs to the waiter manager.
    waiter_mgr_scheduler: WaiterMgrScheduler,

    inner: Rc<RefCell<Inner>>,
}

unsafe impl<S, P> Send for Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
}

impl<S, P> Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
    pub fn new(
        store_id: u64,
        pd_client: Arc<P>,
        resolver: S,
        security_mgr: Arc<SecurityManager>,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        cfg: &Config,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            store_id,
            env: client::env(),
            leader_info: None,
            leader_client: None,
            pd_client,
            resolver,
            security_mgr,
            waiter_mgr_scheduler,
            inner: Rc::new(RefCell::new(Inner {
                role: Role::Follower,
                detect_table: DetectTable::new(Duration::from_millis(cfg.wait_for_lock_timeout)),
            })),
        }
    }

    /// Returns true if it is the leader of the deadlock detector.
    fn is_leader(&self) -> bool {
        self.inner.borrow().role == Role::Leader
    }

    /// Resets to the initial state.
    fn reset(&mut self, role: Role) {
        let mut inner = self.inner.borrow_mut();
        inner.detect_table.clear();
        inner.role = role;
        self.leader_client.take();
        self.leader_info.take();
    }

    /// Refreshes the leader info. Returns true if the leader exists.
    fn refresh_leader_info(&mut self) -> bool {
        assert!(!self.is_leader());
        match self.get_leader_info() {
            Ok(Some((leader_id, leader_addr))) => {
                self.update_leader_info(leader_id, leader_addr);
            }
            Ok(None) => {
                // The leader is gone, reset state.
                info!("no leader");
                self.reset(Role::Follower);
            }
            Err(e) => {
                error!("get leader info failed"; "err" => ?e);
            }
        };
        self.leader_info.is_some()
    }

    /// Gets leader info from PD.
    fn get_leader_info(&self) -> Result<Option<(u64, String)>> {
        let leader = self.pd_client.get_region_info(LEADER_KEY)?.leader;
        match leader {
            Some(leader) => {
                let leader_id = leader.get_store_id();
                let leader_addr = self.resolve_store_address(leader_id)?;
                Ok(Some((leader_id, leader_addr)))
            }

            None => {
                ERROR_COUNTER_METRICS.with(|m| {
                    m.leader_not_found.inc();
                    m.may_flush_all()
                });
                Ok(None)
            }
        }
    }

    /// Resolves store address.
    fn resolve_store_address(&self, store_id: u64) -> Result<String> {
        match wait_op!(|cb| self
            .resolver
            .resolve(store_id, cb)
            .map_err(|e| Error::Other(box_err!(e))))
        {
            Some(Ok(addr)) => Ok(addr),
            _ => Err(box_err!("failed to resolve store address")),
        }
    }

    /// Updates the leader info.
    fn update_leader_info(&mut self, leader_id: u64, leader_addr: String) {
        match self.leader_info {
            Some((id, ref addr)) if id == leader_id && *addr == leader_addr => {
                debug!("leader not change"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
            }
            _ => {
                // The leader info is stale if the leader is itself.
                if leader_id == self.store_id {
                    info!("stale leader info");
                } else {
                    info!("leader changed"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
                    self.leader_client.take();
                    self.leader_info.replace((leader_id, leader_addr));
                }
            }
        }
    }

    /// Resets state if role changes.
    fn change_role(&mut self, role: Role) {
        if self.inner.borrow().role != role {
            match role {
                Role::Leader => info!("became the leader of deadlock detector!"; "self_id" => self.store_id),
                Role::Follower => info!("changed from the leader of deadlock detector to follower!"; "self_id" => self.store_id),
            }
        }
        // If the node is a follower, it will receive a `ChangeRole(Follower)` msg when the leader
        // is changed. It should reset itself even if the role of the node is not changed.
        self.reset(role);
    }

    /// Reconnects the leader. The leader info must exist.
    fn reconnect_leader(&mut self, handle: &Handle) {
        assert!(self.leader_client.is_none() && self.leader_info.is_some());
        ERROR_COUNTER_METRICS.with(|m| {
            m.reconnect_leader.inc();
            m.may_flush_all()
        });
        let (leader_id, leader_addr) = self.leader_info.as_ref().unwrap();
        // Create the connection to the leader and registers the callback to receive
        // the deadlock response.
        let mut leader_client = Client::new(
            Arc::clone(&self.env),
            Arc::clone(&self.security_mgr),
            leader_addr,
        );
        let waiter_mgr_scheduler = self.waiter_mgr_scheduler.clone();
        let (send, recv) = leader_client.register_detect_handler(Box::new(move |mut resp| {
            let WaitForEntry {
                txn,
                wait_for_txn,
                key_hash,
                ..
            } = resp.take_entry();
            waiter_mgr_scheduler.deadlock(
                txn.into(),
                Lock {
                    ts: wait_for_txn.into(),
                    hash: key_hash,
                },
                resp.get_deadlock_key_hash(),
            )
        }));
        handle.spawn(send.map_err(|e| error!("leader client failed"; "err" => ?e)));
        // No need to log it again.
        handle.spawn(recv.map_err(|_| ()));

        self.leader_client = Some(leader_client);
        info!("reconnect leader succeeded"; "leader_id" => leader_id);
    }

    /// Returns true if sends successfully.
    ///
    /// If the client is None, reconnects the leader first, then sends the request to the leader.
    /// If sends failed, sets the client to None for retry.
    fn send_request_to_leader(
        &mut self,
        handle: &Handle,
        tp: DetectType,
        txn_ts: TimeStamp,
        lock: Lock,
    ) -> bool {
        assert!(!self.is_leader() && self.leader_info.is_some());

        if self.leader_client.is_none() {
            self.reconnect_leader(handle);
        }
        if let Some(leader_client) = &self.leader_client {
            let tp = match tp {
                DetectType::Detect => DeadlockRequestType::Detect,
                DetectType::CleanUpWaitFor => DeadlockRequestType::CleanUpWaitFor,
                DetectType::CleanUp => DeadlockRequestType::CleanUp,
            };
            let mut entry = WaitForEntry::default();
            entry.set_txn(txn_ts.into_inner());
            entry.set_wait_for_txn(lock.ts.into_inner());
            entry.set_key_hash(lock.hash);
            let mut req = DeadlockRequest::default();
            req.set_tp(tp);
            req.set_entry(entry);
            if leader_client.detect(req).is_ok() {
                return true;
            }
            // The client is disconnected. Take it for retry.
            self.leader_client.take();
        }
        false
    }

    fn handle_detect_locally(&self, tp: DetectType, txn_ts: TimeStamp, lock: Lock) {
        let detect_table = &mut self.inner.borrow_mut().detect_table;
        match tp {
            DetectType::Detect => {
                if let Some(deadlock_key_hash) = detect_table.detect(txn_ts, lock.ts, lock.hash) {
                    self.waiter_mgr_scheduler
                        .deadlock(txn_ts, lock, deadlock_key_hash);
                }
            }
            DetectType::CleanUpWaitFor => {
                detect_table.clean_up_wait_for(txn_ts, lock.ts, lock.hash)
            }
            DetectType::CleanUp => detect_table.clean_up(txn_ts),
        }
    }

    /// Handles detect requests of itself.
    fn handle_detect(&mut self, handle: &Handle, tp: DetectType, txn_ts: TimeStamp, lock: Lock) {
        if self.is_leader() {
            self.handle_detect_locally(tp, txn_ts, lock);
        } else {
            for _ in 0..2 {
                // TODO: If the leader hasn't been elected, it requests Pd for
                // each detect request. Maybe need flow control here.
                //
                // Refresh leader info when the connection to the leader is disconnected.
                if self.leader_client.is_none() && !self.refresh_leader_info() {
                    break;
                }
                if self.send_request_to_leader(handle, tp, txn_ts, lock) {
                    return;
                }
                // Because the client is asynchronous, it won't be closed until failing to send a
                // request. So retry to refresh the leader info and send it again.
            }
            // If a request which causes deadlock is dropped, it leads to the waiter timeout.
            // TiDB will retry to acquire the lock and detect deadlock again.
            warn!("detect request dropped"; "tp" => ?tp, "txn_ts" => txn_ts, "lock" => ?lock);
            ERROR_COUNTER_METRICS.with(|m| {
                m.dropped.inc();
                m.may_flush_all()
            });
        }
    }

    /// Handles detect requests of other nodes.
    fn handle_detect_rpc(
        &self,
        handle: &Handle,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        if !self.is_leader() {
            let status = RpcStatus::new(
                RpcStatusCode::FAILED_PRECONDITION,
                Some("I'm not the leader of deadlock detector".to_string()),
            );
            handle.spawn(sink.fail(status).map_err(|_| ()));
            ERROR_COUNTER_METRICS.with(|m| {
                m.not_leader.inc();
                m.may_flush_all()
            });
            return;
        }

        let inner = Rc::clone(&self.inner);
        let s = stream
            .map_err(Error::Grpc)
            .and_then(move |mut req| {
                // It's possible the leader changes after registering this handler.
                let mut inner = inner.borrow_mut();
                if inner.role != Role::Leader {
                    ERROR_COUNTER_METRICS.with(|m| {
                        m.not_leader.inc();
                        m.may_flush_all()
                    });
                    return Err(Error::Other(box_err!("leader changed")));
                }

                let WaitForEntry {
                    txn,
                    wait_for_txn,
                    key_hash,
                    ..
                } = req.get_entry();

                let detect_table = &mut inner.detect_table;
                let res = match req.get_tp() {
                    DeadlockRequestType::Detect => {
                        if let Some(deadlock_key_hash) =
                            detect_table.detect(txn.into(), wait_for_txn.into(), *key_hash)
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
                        detect_table.clean_up_wait_for(txn.into(), wait_for_txn.into(), *key_hash);
                        None
                    }

                    DeadlockRequestType::CleanUp => {
                        detect_table.clean_up(txn.into());
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

    fn handle_change_role(&mut self, role: Role) {
        debug!("handle change role"; "role" => ?role);
        self.change_role(role);
    }
}

impl<S, P> FutureRunnable<Task> for Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
    fn run(&mut self, task: Task, handle: &Handle) {
        match task {
            Task::Detect { tp, txn_ts, lock } => {
                self.handle_detect(handle, tp, txn_ts, lock);
            }
            Task::DetectRpc { stream, sink } => {
                self.handle_detect_rpc(handle, stream, sink);
            }
            Task::ChangeRole(role) => self.handle_change_role(role),
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

impl Deadlock for Service {
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

    #[test]
    fn test_detect_table() {
        let mut detect_table = DetectTable::new(Duration::from_secs(10));

        // Deadlock: 1 -> 2 -> 1
        assert_eq!(detect_table.detect(1.into(), 2.into(), 2), None);
        assert_eq!(detect_table.detect(2.into(), 1.into(), 1).unwrap(), 2);
        // Deadlock: 1 -> 2 -> 3 -> 1
        assert_eq!(detect_table.detect(2.into(), 3.into(), 3), None);
        assert_eq!(detect_table.detect(3.into(), 1.into(), 1).unwrap(), 3);
        detect_table.clean_up(2.into());
        assert_eq!(detect_table.wait_for_map.contains_key(&2.into()), false);

        // After cycle is broken, no deadlock.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 1), None);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            1
        );

        // Different key_hash grows the list.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 2), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            2
        );

        // Same key_hash doesn't grow the list.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 2), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            2
        );

        // Different lock_ts grows the map.
        assert_eq!(detect_table.detect(3.into(), 2.into(), 2), None);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 2);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&2.into())
                .unwrap()
                .hashes
                .len(),
            1
        );

        // Clean up entries shrinking the map.
        detect_table.clean_up_wait_for(3.into(), 1.into(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .hashes
                .len(),
            1
        );
        detect_table.clean_up_wait_for(3.into(), 1.into(), 2);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 1);
        detect_table.clean_up_wait_for(3.into(), 2.into(), 2);
        assert_eq!(detect_table.wait_for_map.contains_key(&3.into()), false);

        // Clean up non-exist entry
        detect_table.clean_up(3.into());
        detect_table.clean_up_wait_for(3.into(), 1.into(), 1);
    }

    #[test]
    fn test_detect_table_expire() {
        let mut detect_table = DetectTable::new(Duration::from_millis(100));

        // Deadlock
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_some());
        // After sleep, the expired entry has been removed. So there is no deadlock.
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(detect_table.wait_for_map.len(), 1);
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);

        // `Detect` updates the last_detect_time, so the entry won't be removed.
        detect_table.clear();
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_some());

        // Remove expired entry shrinking the map.
        detect_table.clear();
        assert!(detect_table.detect(1.into(), 2.into(), 1).is_none());
        assert!(detect_table.detect(1.into(), 3.into(), 1).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(1.into(), 3.into(), 2).is_none());
        assert!(detect_table.detect(2.into(), 1.into(), 2).is_none());
        assert_eq!(detect_table.wait_for_map.get(&1.into()).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&1.into())
                .unwrap()
                .get(&3.into())
                .unwrap()
                .hashes
                .len(),
            2
        );
        std::thread::sleep(Duration::from_millis(500));
        assert!(detect_table.detect(3.into(), 2.into(), 3).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 2);
        assert!(detect_table.detect(3.into(), 1.into(), 3).is_none());
        assert_eq!(detect_table.wait_for_map.len(), 1);
    }
}
