// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::client::Client;
use super::metrics::*;
use super::util::extract_physical_timestamp;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use super::{Error, Lock, Result};
use crate::pd::{RpcClient, INVALID_ID};
use crate::raftstore::coprocessor::{Coprocessor, CoprocessorHost, ObserverContext, RoleObserver};
use crate::server::resolve::StoreAddrResolver;
use crate::tikv_util::collections::HashMap;
use crate::tikv_util::future::paired_future_callback;
use crate::tikv_util::security::SecurityManager;
use crate::tikv_util::worker::{FutureRunnable, FutureScheduler, FutureWorker, Stopped};
use futures::{Future, Sink, Stream};
use grpcio::{
    self, DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink, WriteFlags,
};
use kvproto::deadlock::*;
use kvproto::deadlock_grpc;
use kvproto::metapb::Region;
use protobuf::RepeatedField;
use raft::StateRole;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio_core::reactor::Handle;
use tokio_timer::Interval;

/// The leader of the region containing the LEADER_KEY is the leader of deadlock detector.
const LEADER_KEY: &[u8] = b"";

/// Returns true if the region containing the LEADER_KEY.
fn is_leader_region(region: &'_ Region) -> bool {
    region.get_start_key() <= LEADER_KEY && LEADER_KEY < region.get_end_key()
}

// 2 mins
const TXN_DETECT_INFO_TTL: u64 = 120000;

#[derive(Default)]
struct DetectTable {
    wait_for_map: HashMap<u64, Vec<Lock>>,
}

impl DetectTable {
    /// Return deadlock key hash if deadlocked
    pub fn detect(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) -> Option<u64> {
        let _timer = DETECTOR_HISTOGRAM_VEC.detect.start_coarse_timer();
        TASK_COUNTER_VEC.detect.inc();

        if let Some(deadlock_key_hash) = self.do_detect(txn_ts, lock_ts) {
            return Some(deadlock_key_hash);
        }
        self.register(txn_ts, lock_ts, lock_hash);
        None
    }

    fn do_detect(&self, txn_ts: u64, wait_for_ts: u64) -> Option<u64> {
        if let Some(locks) = self.wait_for_map.get(&wait_for_ts) {
            for lock in locks {
                if lock.ts == txn_ts {
                    return Some(lock.hash);
                }
                if let Some(deadlock_key_hash) = self.do_detect(txn_ts, lock.ts) {
                    return Some(deadlock_key_hash);
                }
            }
        }
        None
    }

    pub fn register(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) {
        let locks = self.wait_for_map.entry(txn_ts).or_insert(vec![]);
        let lock = Lock {
            ts: lock_ts,
            hash: lock_hash,
        };
        if !locks.contains(&lock) {
            locks.push(lock);
        }
    }

    pub fn clean_up_wait_for(&mut self, txn_ts: u64, lock_ts: u64, lock_hash: u64) {
        if let Some(locks) = self.wait_for_map.get_mut(&txn_ts) {
            let idx = locks
                .iter()
                .position(|lock| lock.ts == lock_ts && lock.hash == lock_hash);
            if let Some(idx) = idx {
                locks.remove(idx);
                if locks.is_empty() {
                    self.wait_for_map.remove(&txn_ts);
                }
            }
        }
        TASK_COUNTER_VEC.clean_up_wait_for.inc();
    }

    pub fn clean_up(&mut self, txn_ts: u64) {
        self.wait_for_map.remove(&txn_ts);
        TASK_COUNTER_VEC.clean_up.inc();
    }

    pub fn clear(&mut self) {
        self.wait_for_map.clear();
    }

    pub fn expire<F>(&mut self, is_expired: F)
    where
        F: Fn(u64) -> bool,
    {
        self.wait_for_map.retain(|ts, _| !is_expired(*ts));
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
        txn_ts: u64,
        lock: Lock,
    },
    /// The detect request of other nodes.
    DetectRpc {
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    },
    /// If the node has the leader region, and the role of the node changes,
    /// a `ChangeRole` task will be scheduled.
    ///
    /// It's the only way to change the node from leader to follower, and vice versa.
    ChangeRole(StateRole),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Detect { tp, txn_ts, lock } => write!(
                f,
                "Detect {{ tp: {:?}, txn_ts: {:?}, lock: {:?} }}",
                tp, txn_ts, lock
            ),
            Task::DetectRpc { .. } => write!(f, "detect rpc"),
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
        // Only when the deadlock detector is stopped, an error will return.
        // So there is no need to handle the error.
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

    pub fn change_role(&self, role: StateRole) {
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
            self.change_role(role)
        }
    }
}

/// Creates the `Scheduler` and registers it to the CoprocessorHost.
pub fn register_role_change_observer(host: &mut CoprocessorHost, worker: &FutureWorker<Task>) {
    let scheduler = Scheduler::new(worker.scheduler());
    host.registry.register_role_observer(1, Box::new(scheduler));
}

#[derive(Default)]
struct Inner {
    /// The role of the deadlock detector. Default is `StateRole::Follower`.
    role: StateRole,

    detect_table: DetectTable,

    /// Max transaction's timestamp the deadlock detector received.
    /// Used to remove out of date entries in detect table.
    max_ts: u64,
}

/// Detector is used to detect deadlock between transactions. There is a leader
/// in the cluster which collects all `wait_for_entry` from other followers.
pub struct Detector<S: StoreAddrResolver + 'static> {
    /// The store id of the node.
    store_id: u64,
    /// The leader's id and address if exists.
    leader_info: Option<(u64, String)>,
    /// The connection to the leader.
    leader_client: Option<Client>,
    /// Used to get the leader of leader region from PD.
    pd_client: Arc<RpcClient>,
    /// Used to resolve store address.
    resolver: S,
    /// Used to connect other nodes.
    security_mgr: Arc<SecurityManager>,
    /// Used to schedule Deadlock msgs to the waiter manager.
    waiter_mgr_scheduler: WaiterMgrScheduler,

    inner: Rc<RefCell<Inner>>,

    is_initialized: bool,
}

unsafe impl<S: StoreAddrResolver + 'static> Send for Detector<S> {}

impl<S: StoreAddrResolver + 'static> Detector<S> {
    pub fn new(
        store_id: u64,
        pd_client: Arc<RpcClient>,
        resolver: S,
        security_mgr: Arc<SecurityManager>,
        waiter_mgr_scheduler: WaiterMgrScheduler,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            store_id,
            leader_info: None,
            leader_client: None,
            pd_client,
            resolver,
            security_mgr,
            waiter_mgr_scheduler,
            inner: Rc::new(RefCell::new(Inner::default())),
            is_initialized: false,
        }
    }

    fn initialize_if_needed(&mut self, handle: &Handle) {
        if !self.is_initialized {
            self.schedule_detect_table_expiration(handle);
            self.is_initialized = true;
        }
    }

    // TODO: If the performance of the `Delay` timer is good, register the `Delay` timer to
    // cleanup out of date entries and there is no need to send clean up msgs to the leader when
    // waiters time out. I will bench it.
    /// Schedules timer to remove out of date entries in the detect table periodically.
    fn schedule_detect_table_expiration(&self, handle: &Handle) {
        info!("schedule detect table expiration");
        let inner = Rc::clone(&self.inner);
        let timer = Interval::new_interval(Duration::from_millis(TXN_DETECT_INFO_TTL))
            .for_each(move |_| {
                let max_ts = extract_physical_timestamp(inner.borrow().max_ts);
                inner.borrow_mut().detect_table.expire(|ts| {
                    let ts = extract_physical_timestamp(ts);
                    ts + TXN_DETECT_INFO_TTL <= max_ts
                });
                Ok(())
            })
            .map_err(|e| panic!("unexpected err: {:?}", e));
        handle.spawn(timer);
    }

    /// Returns true if it is the leader of the deadlock detector.
    fn is_leader(&self) -> bool {
        self.inner.borrow().role == StateRole::Leader
    }

    /// Resets to the initial state.
    fn reset(&mut self, role: StateRole) {
        let mut inner = self.inner.borrow_mut();
        inner.detect_table.clear();
        inner.role = role;
        self.leader_client.take();
        self.leader_info.take();
    }

    /// Refreshes the leader info.
    /// Returns true if the leader exists.
    fn refresh_leader_info(&mut self) -> bool {
        assert!(!self.is_leader());
        match self.get_leader_info() {
            Ok(Some((leader_id, leader_addr))) => {
                self.update_leader_info(leader_id, leader_addr);
            }
            Ok(None) => {
                // The leader is gone, reset state.
                info!("no leader");
                self.reset(StateRole::Follower);
            }
            Err(e) => {
                error!("get leader info failed"; "err" => ?e);
            }
        };
        self.leader_info.is_some()
    }

    /// Gets leader info from PD.
    fn get_leader_info(&self) -> Result<Option<(u64, String)>> {
        let (_, leader) = self.pd_client.get_region_and_leader(LEADER_KEY)?;
        match leader {
            Some(leader) => {
                let leader_id = leader.get_store_id();
                let leader_addr = self.resolve_store_address(leader_id)?;
                Ok(Some((leader_id, leader_addr)))
            }

            None => {
                ERROR_COUNTER_VEC.leader_not_found.inc();
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
                if leader_id != self.store_id {
                    info!("leader changed"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
                    self.leader_client.take();
                    self.leader_info.replace((leader_id, leader_addr));
                }
            }
        }
    }

    /// Resets state if role changes.
    fn change_role(&mut self, role: StateRole) {
        if self.inner.borrow().role != role {
            if role == StateRole::Leader {
                info!("became the leader of deadlock detector!"; "self_id" => self.store_id);
            } else {
                info!("changed from the leader of deadlock detector to follower!"; "self_id" => self.store_id);
            }
            self.reset(role);
        }
    }

    /// Reconnects the leader.
    fn reconnect_leader(&mut self, handle: &Handle) {
        assert!(self.leader_client.is_none() && self.leader_info.is_some());
        ERROR_COUNTER_VEC.reconnect_leader.inc();

        let (leader_id, leader_addr) = self.leader_info.as_ref().unwrap();
        // Create the connection to the leader and registers the callback to receive
        // the deadlock response.
        let mut leader_client = Client::new(Arc::clone(&self.security_mgr), leader_addr);
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
        info!("reconnect leader succeeded"; "leader_id" => leader_id);
    }

    /// Returns true if sends successfully.
    ///
    /// If the client is None, reconnects the leader first. Then sends the request to the leader.
    /// If send failed, sets the client to None for retry.
    fn send_request_to_leader(
        &mut self,
        handle: &Handle,
        tp: DetectType,
        txn_ts: u64,
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
            let mut entry = WaitForEntry::new();
            entry.set_txn(txn_ts);
            entry.set_wait_for_txn(lock.ts);
            entry.set_key_hash(lock.hash);
            let mut req = DeadlockRequest::new();
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

    fn handle_detect_locally(&self, tp: DetectType, txn_ts: u64, lock: Lock) {
        let mut inner = self.inner.borrow_mut();
        if txn_ts > inner.max_ts {
            inner.max_ts = txn_ts;
        }
        match tp {
            DetectType::Detect => {
                if let Some(deadlock_key_hash) =
                    inner.detect_table.detect(txn_ts, lock.ts, lock.hash)
                {
                    self.waiter_mgr_scheduler
                        .deadlock(txn_ts, lock, deadlock_key_hash);
                }
            }
            DetectType::CleanUpWaitFor => inner
                .detect_table
                .clean_up_wait_for(txn_ts, lock.ts, lock.hash),
            DetectType::CleanUp => inner.detect_table.clean_up(txn_ts),
        }
    }

    /// Handles detect requests of itself.
    fn handle_detect(&mut self, handle: &Handle, tp: DetectType, txn_ts: u64, lock: Lock) {
        if self.is_leader() {
            self.handle_detect_locally(tp, txn_ts, lock);
        } else {
            for _ in 0..2 {
                // TODO: If the leader hasn't been elected, it requests Pd for
                // each detect request. Maybe need flow control here.
                //
                // Refresh leader info when the connection to the leader is disconnected.
                if self.leader_client.is_none() && !self.refresh_leader_info() {
                    // Return if the leader doesn't exist.
                    return;
                }
                // Because the client is asynchronous, it won't be closed until failing to send a
                // request. So retry to refresh the leader info and send it again.
                if self.send_request_to_leader(handle, tp, txn_ts, lock) {
                    return;
                }
            }
            warn!("detect request dropped"; "tp" => ?tp, "txn_ts" => txn_ts, "lock" => ?lock);
            ERROR_COUNTER_VEC.dropped.inc();
        }
    }

    /// Handles detect requests of other nodes.
    fn handle_detect_rpc(
        &mut self,
        handle: &Handle,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        if !self.is_leader() {
            let status = RpcStatus::new(
                RpcStatusCode::FailedPrecondition,
                Some("I'm not the leader of deadlock detector".to_string()),
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
                let mut inner = inner.borrow_mut();
                if inner.role != StateRole::Leader {
                    ERROR_COUNTER_VEC.not_leader.inc();
                    return Err(Error::Other(box_err!("leader changed")));
                }

                let WaitForEntry {
                    txn,
                    wait_for_txn,
                    key_hash,
                    ..
                } = req.get_entry();

                if *txn > inner.max_ts {
                    inner.max_ts = *txn;
                }
                let detect_table = &mut inner.detect_table;
                let res = match req.get_tp() {
                    DeadlockRequestType::Detect => {
                        if let Some(deadlock_key_hash) =
                            detect_table.detect(*txn, *wait_for_txn, *key_hash)
                        {
                            let mut resp = DeadlockResponse::new();
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

    fn handle_change_role(&mut self, role: StateRole) {
        debug!("handle change role"; "role" => ?role);
        let role = match role {
            StateRole::Leader => StateRole::Leader,
            // Change Follower|Candidate|PreCandidate to Follower.
            _ => StateRole::Follower,
        };
        self.change_role(role);
    }
}

impl<S: StoreAddrResolver + 'static> FutureRunnable<Task> for Detector<S> {
    fn run(&mut self, task: Task, handle: &Handle) {
        match task {
            Task::Detect { tp, txn_ts, lock } => {
                // Schedules timer only when the pessimistic transaction is used.
                self.initialize_if_needed(handle);
                self.handle_detect(handle, tp, txn_ts, lock);
            }
            Task::DetectRpc { stream, sink } => {
                // Schedules timer only when the pessimistic transaction is used.
                self.initialize_if_needed(handle);
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
            let status = RpcStatus::new(RpcStatusCode::ResourceExhausted, None);
            ctx.spawn(sink.fail(status).map_err(|_| ()))
        } else {
            ctx.spawn(
                f.map_err(Error::from)
                    .map(|v| {
                        let mut resp = WaitForEntriesResponse::new();
                        resp.set_entries(RepeatedField::from_vec(v));
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
            let status = RpcStatus::new(RpcStatusCode::ResourceExhausted, None);
            ctx.spawn(sink.fail(status).map_err(|_| ()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::util::PHYSICAL_SHIFT_BITS;
    use super::*;
    use crate::tikv_util::time::duration_to_ms;
    use std::time::SystemTime;

    #[test]
    fn test_detect_table() {
        let mut detect_table = DetectTable::default();

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

        // Different key_hash grows the list.
        assert_eq!(detect_table.detect(3, 1, 2), None);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().len(), 2);

        // Same key_hash doesn't grow the list.
        assert_eq!(detect_table.detect(3, 1, 2), None);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().len(), 2);

        detect_table.clean_up_wait_for(3, 1, 1);
        assert_eq!(detect_table.wait_for_map.get(&3).unwrap().len(), 1);
        detect_table.clean_up_wait_for(3, 1, 2);
        assert_eq!(detect_table.wait_for_map.contains_key(&3), false);

        // clean up non-exist entry
        detect_table.clean_up(3);
        detect_table.clean_up_wait_for(3, 1, 1);
    }

    #[test]
    fn test_detect_table_expire() {
        let mut detect_table = DetectTable::default();
        let now = duration_to_ms(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap(),
        );
        detect_table.detect(now << PHYSICAL_SHIFT_BITS, 1, 1);
        detect_table.detect((now - 100) << PHYSICAL_SHIFT_BITS, 1, 1);
        detect_table.detect((now - 200) << PHYSICAL_SHIFT_BITS, 1, 1);
        assert_eq!(detect_table.wait_for_map.len(), 3);
        detect_table.expire(|ts| {
            let ts = extract_physical_timestamp(ts);
            ts + 101 <= now
        });
        assert_eq!(detect_table.wait_for_map.len(), 2);
    }
}
