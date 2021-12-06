// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use keys::origin_key;
use std::cmp::Ordering::*;
use std::fmt::{self, Debug, Display};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use txn_types::Key;

use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, CF_LOCK};
use kvproto::kvrpcpb::LockInfo;
use kvproto::raft_cmdpb::CmdType;
use tikv_util::worker::{Builder as WorkerBuilder, Runnable, ScheduleError, Scheduler, Worker};

use crate::storage::mvcc::{ErrorInner as MvccErrorInner, Lock, TimeStamp};
use crate::storage::txn::Error as TxnError;
use raftstore::coprocessor::{
    ApplySnapshotObserver, BoxApplySnapshotObserver, BoxQueryObserver, Cmd, Coprocessor,
    CoprocessorHost, ObserverContext, QueryObserver,
};

// TODO: Use new error type for GCWorker instead of storage::Error.
use super::{Error, ErrorInner, Result};

const MAX_COLLECT_SIZE: usize = 1024;

/// The state of the observer. Shared between all clones.
#[derive(Default)]
struct LockObserverState {
    max_ts: AtomicU64,

    /// `is_clean` is true, only it's sure that all applying of stale locks (locks with start_ts <=
    /// specified max_ts) are monitored and collected. If there are too many stale locks or any
    /// error happens, `is_clean` must be set to `false`.
    is_clean: AtomicBool,
}

impl LockObserverState {
    fn load_max_ts(&self) -> TimeStamp {
        self.max_ts.load(Ordering::Acquire).into()
    }

    fn store_max_ts(&self, max_ts: TimeStamp) {
        self.max_ts.store(max_ts.into_inner(), Ordering::Release)
    }

    fn is_clean(&self) -> bool {
        self.is_clean.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        self.is_clean.store(true, Ordering::Release);
    }

    fn mark_dirty(&self) {
        self.is_clean.store(false, Ordering::Release);
    }
}

pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

enum LockCollectorTask {
    // Messages from observer
    ObservedLocks(Vec<(Key, Lock)>),

    // Messages from client
    StartCollecting {
        max_ts: TimeStamp,
        callback: Callback<()>,
    },
    GetCollectedLocks {
        max_ts: TimeStamp,
        callback: Callback<(Vec<LockInfo>, bool)>,
    },
    StopCollecting {
        max_ts: TimeStamp,
        callback: Callback<()>,
    },
}

impl Debug for LockCollectorTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockCollectorTask::ObservedLocks(locks) => f
                .debug_struct("ObservedLocks")
                .field("locks", locks)
                .finish(),
            LockCollectorTask::StartCollecting { max_ts, .. } => f
                .debug_struct("StartCollecting")
                .field("max_ts", max_ts)
                .finish(),
            LockCollectorTask::GetCollectedLocks { max_ts, .. } => f
                .debug_struct("GetCollectedLocks")
                .field("max_ts", max_ts)
                .finish(),
            LockCollectorTask::StopCollecting { max_ts, .. } => f
                .debug_struct("StopCollecting")
                .field("max_ts", max_ts)
                .finish(),
        }
    }
}

impl Display for LockCollectorTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self, f)
    }
}

/// `LockObserver` observes apply events and apply snapshot events. If it happens in CF_LOCK, it
/// checks the `start_ts`s of the locks being written. If a lock's `start_ts` <= specified `max_ts`
/// in the `state`, it will send the lock to through the `sender`, so the receiver can collect it.
#[derive(Clone)]
struct LockObserver {
    state: Arc<LockObserverState>,
    sender: Scheduler<LockCollectorTask>,
}

impl LockObserver {
    pub fn new(state: Arc<LockObserverState>, sender: Scheduler<LockCollectorTask>) -> Self {
        Self { state, sender }
    }

    pub fn register(self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        coprocessor_host
            .registry
            .register_apply_snapshot_observer(1, BoxApplySnapshotObserver::new(self.clone()));
        coprocessor_host
            .registry
            .register_query_observer(1, BoxQueryObserver::new(self));
    }

    fn send(&self, locks: Vec<(Key, Lock)>) {
        #[cfg(feature = "failpoints")]
        let injected_full = (|| {
            fail_point!("lock_observer_send_full", |_| {
                info!("[failpoint] injected lock observer channel full"; "locks" => ?locks);
                true
            });
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let injected_full = false;

        let res = if injected_full {
            Err(ScheduleError::Full(LockCollectorTask::ObservedLocks(locks)))
        } else {
            self.sender
                .schedule(LockCollectorTask::ObservedLocks(locks))
        };

        match res {
            Ok(()) => (),
            Err(ScheduleError::Stopped(_)) => {
                error!("lock observer failed to send locks because collector is stopped");
            }
            Err(ScheduleError::Full(_)) => {
                fail_point!("lock_observer_before_mark_dirty_on_full");
                self.state.mark_dirty();
                warn!("cannot collect all applied lock because channel is full");
            }
        }
    }
}

impl Coprocessor for LockObserver {}

impl QueryObserver for LockObserver {
    fn post_apply_query(&self, _: &mut ObserverContext<'_>, cmd: &Cmd) {
        fail_point!("notify_lock_observer_query");
        let max_ts = self.state.load_max_ts();
        if max_ts.is_zero() {
            return;
        }

        if !self.state.is_clean() {
            return;
        }

        let mut locks = vec![];
        // For each put in CF_LOCK, collect it if its ts <= max_ts.
        for req in cmd.request.get_requests() {
            if req.get_cmd_type() != CmdType::Put {
                continue;
            }
            let put_request = req.get_put();
            if put_request.get_cf() != CF_LOCK {
                continue;
            }

            let lock = match Lock::parse(put_request.get_value()) {
                Ok(l) => l,
                Err(e) => {
                    error!(?e;
                        "cannot parse lock";
                        "value" => log_wrappers::Value::value(put_request.get_value()),
                    );
                    self.state.mark_dirty();
                    return;
                }
            };

            if lock.ts <= max_ts {
                let key = Key::from_encoded_slice(put_request.get_key());
                locks.push((key, lock));
            }
        }
        if !locks.is_empty() {
            self.send(locks);
        }
    }
}

impl ApplySnapshotObserver for LockObserver {
    fn apply_plain_kvs(
        &self,
        _: &mut ObserverContext<'_>,
        cf: CfName,
        kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        fail_point!("notify_lock_observer_snapshot");
        if cf != CF_LOCK {
            return;
        }

        let max_ts = self.state.load_max_ts();
        if max_ts.is_zero() {
            return;
        }

        if !self.state.is_clean() {
            return;
        }

        let locks: Result<Vec<_>> = kv_pairs
            .iter()
            .map(|(key, value)| {
                Lock::parse(value)
                    .map(|lock| (key, lock))
                    .map_err(|e| ErrorInner::Txn(TxnError::from_mvcc(e)).into())
            })
            .filter(|result| result.is_err() || result.as_ref().unwrap().1.ts <= max_ts)
            .map(|result| {
                // `apply_plain_keys` will be invoked with the data_key in RocksDB layer. So we
                // need to remove the `z` prefix.
                result.map(|(key, lock)| (Key::from_encoded_slice(origin_key(key)), lock))
            })
            .collect();

        match locks {
            Err(e) => {
                error!(?e; "cannot parse lock");
                self.state.mark_dirty()
            }
            Ok(l) => self.send(l),
        }
    }

    fn apply_sst(&self, _: &mut ObserverContext<'_>, cf: CfName, _path: &str) {
        if cf == CF_LOCK {
            error!("cannot collect all applied lock: snapshot of lock cf applied from sst file");
            self.state.mark_dirty();
        }
    }
}

struct LockCollectorRunner {
    observer_state: Arc<LockObserverState>,

    collected_locks: Vec<(Key, Lock)>,
}

impl LockCollectorRunner {
    pub fn new(observer_state: Arc<LockObserverState>) -> Self {
        Self {
            observer_state,
            collected_locks: vec![],
        }
    }

    fn handle_observed_locks(&mut self, mut locks: Vec<(Key, Lock)>) {
        if self.collected_locks.len() >= MAX_COLLECT_SIZE {
            return;
        }

        if locks.len() + self.collected_locks.len() >= MAX_COLLECT_SIZE {
            self.observer_state.mark_dirty();
            info!("lock collector marked dirty because received too many locks");
            locks.truncate(MAX_COLLECT_SIZE - self.collected_locks.len());
        }
        self.collected_locks.extend(locks);
    }

    fn start_collecting(&mut self, max_ts: TimeStamp) -> Result<()> {
        let curr_max_ts = self.observer_state.load_max_ts();
        match max_ts.cmp(&curr_max_ts) {
            Less => Err(box_err!(
                "collecting locks with a greater max_ts: {}",
                curr_max_ts
            )),
            Equal => {
                // Stale request. Ignore it.
                Ok(())
            }
            Greater => {
                info!("start collecting locks"; "max_ts" => max_ts);
                self.collected_locks.clear();
                // TODO: `is_clean` may be unexpectedly set to false here, if any error happens on a
                // previous observing. It need to be solved, although it's very unlikely to happen and
                // doesn't affect correctness of data.
                self.observer_state.mark_clean();
                self.observer_state.store_max_ts(max_ts);
                Ok(())
            }
        }
    }

    fn get_collected_locks(&mut self, max_ts: TimeStamp) -> Result<(Vec<LockInfo>, bool)> {
        let curr_max_ts = self.observer_state.load_max_ts();
        if curr_max_ts != max_ts {
            warn!(
                "trying to fetch collected locks but now collecting with another max_ts";
                "req_max_ts" => max_ts,
                "current_max_ts" => curr_max_ts,
            );
            return Err(box_err!(
                "trying to fetch collected locks but now collecting with another max_ts"
            ));
        }

        let locks: Result<_> = self
            .collected_locks
            .iter()
            .map(|(k, l)| {
                k.to_raw()
                    .map(|raw_key| l.clone().into_lock_info(raw_key))
                    .map_err(|e| Error::from(TxnError::from_mvcc(e)))
            })
            .collect();

        Ok((locks?, self.observer_state.is_clean()))
    }

    fn stop_collecting(&mut self, max_ts: TimeStamp) -> Result<()> {
        let res = self.observer_state.max_ts.compare_exchange(
            max_ts.into_inner(),
            0,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        if res.is_ok() {
            self.collected_locks.clear();
            info!("stop collecting locks"; "max_ts" => max_ts);
            Ok(())
        } else {
            warn!(
                "trying to stop collecting locks, but now collecting with a different max_ts";
                "stopping_max_ts" => max_ts,
                "current_max_ts" => TimeStamp::new(res.unwrap_err()),
            );
            Err(box_err!("collecting locks with another max_ts"))
        }
    }
}

impl Runnable for LockCollectorRunner {
    type Task = LockCollectorTask;

    fn run(&mut self, task: LockCollectorTask) {
        match task {
            LockCollectorTask::ObservedLocks(locks) => self.handle_observed_locks(locks),
            LockCollectorTask::StartCollecting { max_ts, callback } => {
                callback(self.start_collecting(max_ts))
            }
            LockCollectorTask::GetCollectedLocks { max_ts, callback } => {
                callback(self.get_collected_locks(max_ts))
            }
            LockCollectorTask::StopCollecting { max_ts, callback } => {
                callback(self.stop_collecting(max_ts))
            }
        }
    }
}

pub struct AppliedLockCollector {
    worker: Mutex<Worker>,
    scheduler: Scheduler<LockCollectorTask>,
    concurrency_manager: ConcurrencyManager,
}

impl AppliedLockCollector {
    pub fn new(
        coprocessor_host: &mut CoprocessorHost<impl KvEngine>,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<Self> {
        let worker = Mutex::new(WorkerBuilder::new("lock-collector").create());

        let state = Arc::new(LockObserverState::default());
        let runner = LockCollectorRunner::new(Arc::clone(&state));
        let scheduler = worker.lock().unwrap().start("lock-collector", runner);
        let observer = LockObserver::new(state, scheduler.clone());

        observer.register(coprocessor_host);

        // Start the worker

        Ok(Self {
            worker,
            scheduler,
            concurrency_manager,
        })
    }

    pub fn stop(&self) {
        self.worker.lock().unwrap().stop();
    }

    /// Starts collecting applied locks whose `start_ts` <= `max_ts`. Only one `max_ts` is valid
    /// at one time.
    pub fn start_collecting(&self, max_ts: TimeStamp, callback: Callback<()>) -> Result<()> {
        // Before starting collecting, check the concurrency manager to avoid later prewrite
        // requests uses a min_commit_ts less than the safepoint.
        // `max_ts` here is the safepoint of the current round of GC.
        // Ths is similar to that we update max_ts and check memory lock when handling other
        // transactional read requests. However this is done at start_collecting instead of
        // physical_scan_locks. The reason is that, to fully scan a TiKV store, it might needs more
        // than one physical_scan_lock requests. However memory lock needs to be checked before
        // scanning the locks, and we can't know the `end_key` of the scan range at that time. As
        // a result, each physical_scan_lock request will cause scanning memory lock from the
        // start_key to the very-end of the TiKV node, which is a waste. But since we always start
        // collecting applied locks before physical scan lock, so a better idea is to check the
        // memory lock before physical_scan_lock.
        self.concurrency_manager.update_max_ts(max_ts);
        self.concurrency_manager
            .read_range_check(None, None, |key, lock| {
                // `Lock::check_ts_conflict` can't be used here, because LockType::Lock
                // can't be ignored in this case.
                if lock.ts <= max_ts {
                    Err(TxnError::from_mvcc(MvccErrorInner::KeyIsLocked(
                        lock.clone().into_lock_info(key.to_raw()?),
                    )))
                } else {
                    Ok(())
                }
            })?;
        self.scheduler
            .schedule(LockCollectorTask::StartCollecting { max_ts, callback })
            .map_err(|e| box_err!("failed to schedule task: {:?}", e))
    }

    /// Get the collected locks after `start_collecting`. Only valid when `max_ts` matches the
    /// `max_ts` provided to `start_collecting`.
    /// Collects at most `MAX_COLLECT_SIZE` locks. If there are (even potentially) more locks than
    /// `MAX_COLLECT_SIZE` or any error happens, the flag `is_clean` will be unset, which represents
    /// `AppliedLockCollector` cannot collect all locks.
    pub fn get_collected_locks(
        &self,
        max_ts: TimeStamp,
        callback: Callback<(Vec<LockInfo>, bool)>,
    ) -> Result<()> {
        self.scheduler
            .schedule(LockCollectorTask::GetCollectedLocks { max_ts, callback })
            .map_err(|e| box_err!("failed to schedule task: {:?}", e))
    }

    /// Stop collecting locks. Only valid when `max_ts` matches the `max_ts` provided to
    /// `start_collecting`.
    pub fn stop_collecting(&self, max_ts: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.scheduler
            .schedule(LockCollectorTask::StopCollecting { max_ts, callback })
            .map_err(|e| box_err!("failed to schedule task: {:?}", e))
    }
}

impl Drop for AppliedLockCollector {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_test::kv::KvTestEngine;
    use engine_traits::CF_DEFAULT;
    use futures::executor::block_on;
    use kvproto::kvrpcpb::Op;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{
        PutRequest, RaftCmdRequest, RaftCmdResponse, Request as RaftRequest,
    };
    use std::sync::mpsc::channel;
    use txn_types::LockType;

    fn lock_info_to_kv(mut lock_info: LockInfo) -> (Vec<u8>, Vec<u8>) {
        let key = Key::from_raw(lock_info.get_key()).into_encoded();
        let lock = Lock::new(
            match lock_info.get_lock_type() {
                Op::Put => LockType::Put,
                Op::Del => LockType::Delete,
                Op::Lock => LockType::Lock,
                Op::PessimisticLock => LockType::Pessimistic,
                _ => unreachable!(),
            },
            lock_info.take_primary_lock(),
            lock_info.get_lock_version().into(),
            lock_info.get_lock_ttl(),
            None,
            0.into(),
            lock_info.get_txn_size(),
            0.into(),
        );
        let value = lock.to_bytes();
        (key, value)
    }

    fn make_apply_request(
        key: Vec<u8>,
        value: Vec<u8>,
        cf: &str,
        cmd_type: CmdType,
    ) -> RaftRequest {
        let mut put_req = PutRequest::default();
        put_req.set_cf(cf.to_owned());
        put_req.set_key(key);
        put_req.set_value(value);

        let mut req = RaftRequest::default();
        req.set_cmd_type(cmd_type);
        req.set_put(put_req);
        req
    }

    fn make_raft_cmd(requests: Vec<RaftRequest>) -> Cmd {
        let mut req = RaftCmdRequest::default();
        req.set_requests(requests.into());
        Cmd::new(0, req, RaftCmdResponse::default())
    }

    fn new_test_collector() -> (AppliedLockCollector, CoprocessorHost<KvTestEngine>) {
        let mut coprocessor_host = CoprocessorHost::default();
        let collector =
            AppliedLockCollector::new(&mut coprocessor_host, ConcurrencyManager::new(1.into()))
                .unwrap();
        (collector, coprocessor_host)
    }

    fn start_collecting(c: &AppliedLockCollector, max_ts: u64) -> Result<()> {
        let (tx, rx) = channel();
        c.start_collecting(max_ts.into(), Box::new(move |r| tx.send(r).unwrap()))
            .and_then(move |()| rx.recv().unwrap())
    }

    fn get_collected_locks(c: &AppliedLockCollector, max_ts: u64) -> Result<(Vec<LockInfo>, bool)> {
        let (tx, rx) = channel();
        c.get_collected_locks(max_ts.into(), Box::new(move |r| tx.send(r).unwrap()))
            .unwrap();
        rx.recv().unwrap()
    }

    fn stop_collecting(c: &AppliedLockCollector, max_ts: u64) -> Result<()> {
        let (tx, rx) = channel();
        c.stop_collecting(max_ts.into(), Box::new(move |r| tx.send(r).unwrap()))
            .unwrap();
        rx.recv().unwrap()
    }

    #[test]
    fn test_start_stop() {
        let (c, _) = new_test_collector();
        // Not started.
        get_collected_locks(&c, 1).unwrap_err();
        stop_collecting(&c, 1).unwrap_err();

        // Started.
        start_collecting(&c, 2).unwrap();
        assert_eq!(c.concurrency_manager.max_ts(), 2.into());
        get_collected_locks(&c, 2).unwrap();
        stop_collecting(&c, 2).unwrap();
        // Stopped.
        get_collected_locks(&c, 2).unwrap_err();
        stop_collecting(&c, 2).unwrap_err();

        // When start_collecting is invoked with a larger ts, the later one will ovewrite the
        // previous one.
        start_collecting(&c, 3).unwrap();
        assert_eq!(c.concurrency_manager.max_ts(), 3.into());
        get_collected_locks(&c, 3).unwrap();
        get_collected_locks(&c, 4).unwrap_err();
        start_collecting(&c, 4).unwrap();
        assert_eq!(c.concurrency_manager.max_ts(), 4.into());
        get_collected_locks(&c, 3).unwrap_err();
        get_collected_locks(&c, 4).unwrap();
        // Do not allow aborting previous observing with a smaller max_ts.
        start_collecting(&c, 3).unwrap_err();
        get_collected_locks(&c, 3).unwrap_err();
        get_collected_locks(&c, 4).unwrap();
        // Do not allow stoping observing with a different max_ts.
        stop_collecting(&c, 3).unwrap_err();
        stop_collecting(&c, 5).unwrap_err();
        stop_collecting(&c, 4).unwrap();
    }

    #[test]
    fn test_check_memlock_on_start() {
        let (c, _) = new_test_collector();
        let cm = c.concurrency_manager.clone();

        let mem_lock = |k: &[u8], ts: u64, lock_type| {
            let key = Key::from_raw(k);
            let guard = block_on(cm.lock_key(&key));
            guard.with_lock(|lock| {
                *lock = Some(txn_types::Lock::new(
                    lock_type,
                    k.to_vec(),
                    ts.into(),
                    100,
                    None,
                    0.into(),
                    1,
                    20.into(),
                ));
            });
            guard
        };

        let guard = mem_lock(b"a", 100, LockType::Put);
        start_collecting(&c, 90).unwrap();
        stop_collecting(&c, 90).unwrap();
        start_collecting(&c, 100).unwrap_err();
        // Use get_collected_locks to check it's not collecting.
        get_collected_locks(&c, 100).unwrap_err();
        start_collecting(&c, 110).unwrap_err();
        get_collected_locks(&c, 110).unwrap_err();
        drop(guard);

        let guard = mem_lock(b"b", 100, LockType::Lock);
        start_collecting(&c, 90).unwrap();
        stop_collecting(&c, 90).unwrap();
        start_collecting(&c, 100).unwrap_err();
        get_collected_locks(&c, 100).unwrap_err();
        start_collecting(&c, 110).unwrap_err();
        get_collected_locks(&c, 110).unwrap_err();
        drop(guard);

        start_collecting(&c, 200).unwrap();
        stop_collecting(&c, 200).unwrap();
    }

    #[test]
    fn test_apply() {
        let locks: Vec<_> = vec![
            (b"k0", 10),
            (b"k1", 110),
            (b"k5", 100),
            (b"k2", 101),
            (b"k3", 90),
            (b"k2", 99),
        ]
        .into_iter()
        .map(|(k, ts)| {
            let mut lock_info = LockInfo::default();
            lock_info.set_key(k.to_vec());
            lock_info.set_primary_lock(k.to_vec());
            lock_info.set_lock_type(Op::Put);
            lock_info.set_lock_version(ts);
            lock_info
        })
        .collect();
        let lock_kvs: Vec<_> = locks
            .iter()
            .map(|lock| lock_info_to_kv(lock.clone()))
            .collect();

        let (c, coprocessor_host) = new_test_collector();
        let mut expected_result = vec![];

        start_collecting(&c, 100).unwrap();
        assert_eq!(get_collected_locks(&c, 100).unwrap(), (vec![], true));

        // Only puts in lock cf will be monitered.
        let req = vec![
            make_apply_request(
                lock_kvs[0].0.clone(),
                lock_kvs[0].1.clone(),
                CF_LOCK,
                CmdType::Put,
            ),
            make_apply_request(b"1".to_vec(), b"1".to_vec(), CF_DEFAULT, CmdType::Put),
            make_apply_request(b"2".to_vec(), b"2".to_vec(), CF_LOCK, CmdType::Delete),
        ];
        coprocessor_host.post_apply(&Region::default(), &make_raft_cmd(req));
        expected_result.push(locks[0].clone());
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_result.clone(), true)
        );

        // When start collecting with the same max_ts again, shouldn't clean up the observer state.
        start_collecting(&c, 100).unwrap();
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_result.clone(), true)
        );

        // Only locks with ts <= 100 will be collected.
        let req: Vec<_> = lock_kvs
            .iter()
            .map(|(k, v)| make_apply_request(k.clone(), v.clone(), CF_LOCK, CmdType::Put))
            .collect();
        expected_result.extend(
            locks
                .iter()
                .filter(|l| l.get_lock_version() <= 100)
                .cloned(),
        );
        coprocessor_host.post_apply(&Region::default(), &make_raft_cmd(req.clone()));
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_result, true)
        );

        // When start_collecting is double-invoked again with larger ts, the previous results are
        // dropped.
        start_collecting(&c, 110).unwrap();
        assert_eq!(get_collected_locks(&c, 110).unwrap(), (vec![], true));
        coprocessor_host.post_apply(&Region::default(), &make_raft_cmd(req));
        assert_eq!(get_collected_locks(&c, 110).unwrap(), (locks, true));
    }

    #[test]
    fn test_apply_snapshot() {
        let locks: Vec<_> = vec![
            (b"k0", 10),
            (b"k1", 110),
            (b"k5", 100),
            (b"k2", 101),
            (b"k3", 90),
            (b"k2", 99),
        ]
        .into_iter()
        .map(|(k, ts)| {
            let mut lock_info = LockInfo::default();
            lock_info.set_key(k.to_vec());
            lock_info.set_primary_lock(k.to_vec());
            lock_info.set_lock_type(Op::Put);
            lock_info.set_lock_version(ts);
            lock_info
        })
        .collect();
        let lock_kvs: Vec<_> = locks
            .iter()
            .map(|lock| lock_info_to_kv(lock.clone()))
            .map(|(k, v)| (keys::data_key(&k), v))
            .collect();

        let (c, coprocessor_host) = new_test_collector();
        start_collecting(&c, 100).unwrap();

        // Apply plain file to other CFs. Nothing happens.
        coprocessor_host.post_apply_plain_kvs_from_snapshot(
            &Region::default(),
            CF_DEFAULT,
            &lock_kvs,
        );
        assert_eq!(get_collected_locks(&c, 100).unwrap(), (vec![], true));

        // Apply plain file to lock cf. Locks with ts before 100 will be collected.
        let expected_locks: Vec<_> = locks
            .iter()
            .filter(|l| l.get_lock_version() <= 100)
            .cloned()
            .collect();
        coprocessor_host.post_apply_plain_kvs_from_snapshot(&Region::default(), CF_LOCK, &lock_kvs);
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_locks.clone(), true)
        );
        // Fetch result twice gets the same result.
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_locks.clone(), true)
        );

        // When stale start_collecting request arrives, the previous collected results shouldn't
        // be dropped.
        start_collecting(&c, 100).unwrap();
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_locks.clone(), true)
        );
        start_collecting(&c, 90).unwrap_err();
        assert_eq!(
            get_collected_locks(&c, 100).unwrap(),
            (expected_locks, true)
        );

        // When start_collecting is double-invoked again with larger ts, the previous results are
        // dropped.
        start_collecting(&c, 110).unwrap();
        assert_eq!(get_collected_locks(&c, 110).unwrap(), (vec![], true));
        coprocessor_host.post_apply_plain_kvs_from_snapshot(&Region::default(), CF_LOCK, &lock_kvs);
        assert_eq!(get_collected_locks(&c, 110).unwrap(), (locks.clone(), true));

        // Apply SST file to other cfs. Nothing happens.
        coprocessor_host.post_apply_sst_from_snapshot(&Region::default(), CF_DEFAULT, "");
        assert_eq!(get_collected_locks(&c, 110).unwrap(), (locks.clone(), true));

        // Apply SST file to lock cf is not supported. This will cause error and therefore
        // `is_clean` will be set to false.
        coprocessor_host.post_apply_sst_from_snapshot(&Region::default(), CF_LOCK, "");
        assert_eq!(get_collected_locks(&c, 110).unwrap(), (locks, false));
    }

    #[test]
    fn test_not_clean() {
        let (c, coprocessor_host) = new_test_collector();
        start_collecting(&c, 1).unwrap();
        // When error happens, `is_clean` should be set to false.
        // The value is not a valid lock.
        let (k, v) = (Key::from_raw(b"k1").into_encoded(), b"v1".to_vec());
        let req = make_apply_request(k.clone(), v.clone(), CF_LOCK, CmdType::Put);
        coprocessor_host.post_apply(&Region::default(), &make_raft_cmd(vec![req]));
        assert_eq!(get_collected_locks(&c, 1).unwrap(), (vec![], false));

        // `is_clean` should be reset after invoking `start_collecting`.
        start_collecting(&c, 2).unwrap();
        assert_eq!(get_collected_locks(&c, 2).unwrap(), (vec![], true));
        coprocessor_host.post_apply_plain_kvs_from_snapshot(
            &Region::default(),
            CF_LOCK,
            &[(keys::data_key(&k), v)],
        );
        assert_eq!(get_collected_locks(&c, 2).unwrap(), (vec![], false));

        start_collecting(&c, 3).unwrap();
        assert_eq!(get_collected_locks(&c, 3).unwrap(), (vec![], true));

        // If there are too many locks, `is_clean` should be set to false.
        let mut lock = LockInfo::default();
        lock.set_key(b"k2".to_vec());
        lock.set_primary_lock(b"k2".to_vec());
        lock.set_lock_type(Op::Put);
        lock.set_lock_version(1);

        let batch_generate_locks = |count| {
            let (k, v) = lock_info_to_kv(lock.clone());
            let req = make_apply_request(k, v, CF_LOCK, CmdType::Put);
            let raft_cmd = make_raft_cmd(vec![req; count]);
            coprocessor_host.post_apply(&Region::default(), &raft_cmd);
        };

        batch_generate_locks(MAX_COLLECT_SIZE - 1);
        let (locks, is_clean) = get_collected_locks(&c, 3).unwrap();
        assert_eq!(locks.len(), MAX_COLLECT_SIZE - 1);
        assert!(is_clean);

        batch_generate_locks(1);
        let (locks, is_clean) = get_collected_locks(&c, 3).unwrap();
        assert_eq!(locks.len(), MAX_COLLECT_SIZE);
        assert!(!is_clean);

        batch_generate_locks(1);
        // If there are more locks, they will be dropped.
        let (locks, is_clean) = get_collected_locks(&c, 3).unwrap();
        assert_eq!(locks.len(), MAX_COLLECT_SIZE);
        assert!(!is_clean);

        start_collecting(&c, 4).unwrap();
        assert_eq!(get_collected_locks(&c, 4).unwrap(), (vec![], true));

        batch_generate_locks(MAX_COLLECT_SIZE - 5);
        let (locks, is_clean) = get_collected_locks(&c, 4).unwrap();
        assert_eq!(locks.len(), MAX_COLLECT_SIZE - 5);
        assert!(is_clean);

        batch_generate_locks(10);
        let (locks, is_clean) = get_collected_locks(&c, 4).unwrap();
        assert_eq!(locks.len(), MAX_COLLECT_SIZE);
        assert!(!is_clean);
    }
}
