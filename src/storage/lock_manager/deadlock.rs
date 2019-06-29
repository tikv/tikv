// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::client::Client;
use super::leader_change_notifier::LeaderChangeNotifier;
use super::metrics::*;
use super::util::extract_physical_timestamp;
use super::waiter_manager::Scheduler as WaiterMgrScheduler;
use super::{Error, Lock};
use crate::pd::INVALID_ID;
use crate::tikv_util::collections::HashMap;
use crate::tikv_util::future::paired_future_callback;
use crate::tikv_util::security::SecurityManager;
use crate::tikv_util::worker::{FutureRunnable, FutureScheduler, Stopped};
use futures::{Future, Sink, Stream};
use grpcio::{
    self, DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink, WriteFlags,
};
use kvproto::deadlock::*;
use kvproto::deadlock_grpc;
use protobuf::RepeatedField;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio_core::reactor::Handle;
use tokio_timer::Interval;

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
    LeaderChange {
        id: u64,
        addr: String,
    },
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
            Task::LeaderChange { id, addr } => {
                write!(f, "LeaderChange {{ id: {}, addr: {} }}", id, addr)
            }
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

    pub fn leader_change(&self, leader_id: u64, leader_addr: String) {
        self.notify_scheduler(Task::LeaderChange {
            id: leader_id,
            addr: leader_addr,
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
    max_ts: u64,
}

impl Inner {
    fn new(
        store_id: u64,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Self {
            store_id,
            leader_info: None,
            leader_client: None,
            detect_table: Rc::new(RefCell::new(DetectTable::default())),
            waiter_mgr_scheduler,
            security_mgr,
            max_ts: 0,
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

    fn update_max_ts_if_needed(&mut self, ts: u64) {
        if ts > self.max_ts {
            self.max_ts = ts;
        }
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

    // TODO: get leader info from leader_change_notifier when reconnecting.
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

pub struct Detector<L: LeaderChangeNotifier + 'static> {
    leader_change_notifier: L,
    scheduler: Option<Scheduler>,
    inner: Rc<RefCell<Inner>>,
    is_initialized: bool,
}

unsafe impl<L: LeaderChangeNotifier + 'static> Send for Detector<L> {}

impl<L: LeaderChangeNotifier + 'static> Detector<L> {
    pub fn new(
        store_id: u64,
        leader_change_notifier: L,
        scheduler: Scheduler,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            leader_change_notifier,
            scheduler: Some(scheduler),
            inner: Rc::new(RefCell::new(Inner::new(
                store_id,
                waiter_mgr_scheduler,
                security_mgr,
            ))),
            is_initialized: false,
        }
    }

    /// Starts the leader change notifier and gets leader info from it immediately.
    fn start_leader_change_notifier(&mut self) {
        info!("start leader change notifier");
        let scheduler = self.scheduler.take().unwrap();
        self.leader_change_notifier.start(Box::new(move |res| {
            if let Ok((leader_id, leader_addr)) = res {
                scheduler.leader_change(leader_id, leader_addr);
            }
        }));
        // Gets the leader info immediately and initializes the leader change notifier.
        if let Ok((leader_id, leader_addr)) = self
            .leader_change_notifier
            .get_leader_info(Duration::from_secs(3))
        {
            self.handle_leader_change(leader_id, leader_addr);
        }
    }

    fn schedule_detect_table_expiration(&self, handle: &Handle) {
        info!("schedule detect table expiration");
        let inner = Rc::clone(&self.inner);
        let timer = Interval::new_interval(Duration::from_millis(TXN_DETECT_INFO_TTL))
            .for_each(move |_| {
                let max_ts = extract_physical_timestamp(inner.borrow().max_ts);
                inner.borrow().detect_table.borrow_mut().expire(|ts| {
                    let ts = extract_physical_timestamp(ts);
                    ts + TXN_DETECT_INFO_TTL <= max_ts
                });
                Ok(())
            })
            .map_err(|e| panic!("unexpected err: {:?}", e));
        handle.spawn(timer);
    }

    fn initialize(&mut self, handle: &Handle) {
        assert!(!self.is_initialized);
        self.start_leader_change_notifier();
        self.schedule_detect_table_expiration(handle);
        self.is_initialized = true;
    }

    fn handle_detect(&self, handle: &Handle, tp: DetectType, txn_ts: u64, lock: Lock) {
        let mut inner = self.inner.borrow_mut();
        if inner.is_leader() {
            inner.update_max_ts_if_needed(txn_ts);
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
                // TODO: get leader info
                if inner.leader_client.is_none() && inner.leader_info.is_some() {
                    inner.reconnect_leader(handle);
                }
                if let Some(leader_client) = &inner.leader_client {
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
                RpcStatusCode::FailedPrecondition,
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
                let mut inner = inner.borrow_mut();
                if !inner.is_leader() {
                    ERROR_COUNTER_VEC.not_leader.inc();
                    return Err(box_err!("leader changed"));
                }

                let WaitForEntry {
                    txn,
                    wait_for_txn,
                    key_hash,
                    ..
                } = req.get_entry();

                inner.update_max_ts_if_needed(*txn);
                let mut detect_table = inner.detect_table.borrow_mut();
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

    fn handle_leader_change(&self, leader_id: u64, leader_addr: String) {
        self.inner
            .borrow_mut()
            .change_role_if_needed(leader_id, leader_addr);
    }
}

impl<L: LeaderChangeNotifier + 'static> FutureRunnable<Task> for Detector<L> {
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
            Task::LeaderChange { id, addr } => self.handle_leader_change(id, addr),
        }
    }

    /// Stop the leader change notifier, When the detector is stopped.
    fn shutdown(&mut self) {
        self.leader_change_notifier.stop();
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
