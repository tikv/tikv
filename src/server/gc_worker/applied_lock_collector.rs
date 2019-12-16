// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use keys::origin_key;
use std::fmt::{self, Debug, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use txn_types::Key;

use engine::{CfName, CF_LOCK};
use kvproto::kvrpcpb::LockInfo;
use kvproto::raft_cmdpb::Request as RaftRequest;
use tikv_util::worker::{Builder as WorkerBuilder, Runnable, ScheduleError, Scheduler, Worker};

use crate::raftstore::coprocessor::{
    ApplySnapshotObserver, Coprocessor, CoprocessorHost, ObserverContext, QueryObserver,
};
use crate::storage::mvcc::{Error as MvccError, Lock, TimeStamp};

// TODO: Use new error type for GCWorker instead of storege::Error.
use super::{Error, ErrorInner, Result};

const MAX_COLLECT_SIZE: usize = 1024;

#[derive(Default)]
struct LockObserverState {
    max_ts: AtomicU64,
    // collected_locks: AtomicUsize,
    // is_clean: AtomicBool,
}

#[derive(Debug)]
enum LockObserverMsg {
    // A lock is applied.
    Locks(Vec<(Key, Lock)>),
    // A error occurs in the observer,
    Err(Error),
}

impl Display for LockObserverMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self, f)
    }
}

pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;

enum LockCollectorTask {
    // From observer
    ObserverMsg(LockObserverMsg),

    // From client
    StartCollecting {
        max_ts: TimeStamp,
        callback: Callback<()>,
    },
    FetchResult {
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
            LockCollectorTask::ObserverMsg(msg) => {
                f.debug_struct("ObserverMsg").field("msg", msg).finish()
            }
            LockCollectorTask::StartCollecting { max_ts, .. } => f
                .debug_struct("StartCollecting")
                .field("max_ts", max_ts)
                .finish(),
            LockCollectorTask::FetchResult { max_ts, .. } => f
                .debug_struct("FetchResult")
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

#[derive(Clone)]
struct LockObserver {
    state: Arc<LockObserverState>,
    sender: Scheduler<LockCollectorTask>,
}

impl LockObserver {
    pub fn new(state: Arc<LockObserverState>, sender: Scheduler<LockCollectorTask>) -> Self {
        Self { state, sender }
    }

    pub fn register(self, coprocessor_host: &mut CoprocessorHost) {
        coprocessor_host
            .registry
            .register_apply_snapshot_observer(1, Box::new(self.clone()));
        coprocessor_host
            .registry
            .register_query_observer(1, Box::new(self));
    }

    fn send(&self, msg: LockObserverMsg) {
        match self.sender.schedule(LockCollectorTask::ObserverMsg(msg)) {
            Ok(()) => (),
            Err(ScheduleError::Stopped(m)) => {
                error!("failed to send lock observer msg"; "msg" => ?m);
            }
            Err(ScheduleError::Full(m)) => {
                warn!("cannot collect all applied lock because channel is full"; "msg" => ?m);
            }
        }
    }

    fn load_max_ts(&self) -> TimeStamp {
        self.state.max_ts.load(Ordering::Acquire).into()
    }
}

impl Coprocessor for LockObserver {}

impl QueryObserver for LockObserver {
    fn pre_apply_query(&self, _: &mut ObserverContext<'_>, requests: &[RaftRequest]) {
        let max_ts = self.load_max_ts();
        if max_ts.is_zero() {
            return;
        }

        for req in requests {
            let put_request = req.get_put();
            if put_request.get_cf() != CF_LOCK {
                continue;
            }

            let lock = match Lock::parse(put_request.get_value()) {
                Ok(l) => l,
                Err(e) => {
                    error!(
                        "cannot parse lock";
                        "value" => hex::encode_upper(put_request.get_value()),
                        "err" => ?e
                    );
                    self.send(LockObserverMsg::Err(ErrorInner::Mvcc(e.into()).into()));
                    return;
                }
            };

            if lock.ts <= max_ts {
                let key = Key::from_encoded_slice(put_request.get_key());
                self.send(LockObserverMsg::Locks(vec![(key, lock)]));
            }
        }
    }
}

impl ApplySnapshotObserver for LockObserver {
    fn pre_apply_plain_keys(
        &self,
        _: &mut ObserverContext<'_>,
        cf: CfName,
        kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        if cf != CF_LOCK {
            return;
        }

        let max_ts = self.load_max_ts();
        if max_ts.is_zero() {
            return;
        }

        let locks: Result<Vec<_>> = kv_pairs
            .iter()
            .map(|(key, value)| {
                Lock::parse(value)
                    .map(|lock| (key, lock))
                    .map_err(|e| ErrorInner::Mvcc(e.into()).into())
            })
            .filter(|result| result.is_err() || result.as_ref().unwrap().1.ts <= max_ts)
            .map(|result| {
                result.map(|(key, lock)| (Key::from_encoded_slice(origin_key(key)), lock))
            })
            .collect();

        match locks {
            Err(e) => self.send(LockObserverMsg::Err(e)),
            Ok(l) => self.send(LockObserverMsg::Locks(l)),
        }
    }

    fn pre_apply_sst(&self, _: &mut ObserverContext<'_>, cf: CfName) {
        if cf == CF_LOCK {
            let e = box_err!("snapshot of lock cf applied from sst file");
            error!("cannot collect all applied lock"; "err" => ?e);
            self.send(LockObserverMsg::Err(e));
        }
    }
}

struct LockCollectorRunner {
    observer_state: Arc<LockObserverState>,

    collected_locks: Vec<(Key, Lock)>,

    // TODO: when dirty, stop collecting.
    is_clean: bool,
}

impl LockCollectorRunner {
    pub fn new(observer_state: Arc<LockObserverState>) -> Self {
        Self {
            observer_state,
            collected_locks: vec![],
            is_clean: true,
        }
    }

    fn handle_observer_msg(&mut self, msg: LockObserverMsg) {
        if !self.is_clean {
            return;
        }

        match msg {
            LockObserverMsg::Err(e) => {
                self.is_clean = false;
                info!("lock collector marked dirty because received error"; "err" => ?e);
            }
            LockObserverMsg::Locks(mut locks) => {
                if locks.len() + self.collected_locks.len() > MAX_COLLECT_SIZE {
                    self.is_clean = false;
                    info!("lock collector marked dirty because received too many locks");
                    locks.truncate(MAX_COLLECT_SIZE - self.collected_locks.len());
                }
                self.collected_locks.extend(locks);
            }
        }
    }

    fn start_collecting(&mut self, max_ts: TimeStamp) -> Result<()> {
        info!("start collecting locks"; "max_ts" => max_ts);
        self.collected_locks.clear();
        self.is_clean = true;
        self.observer_state
            .max_ts
            .store(max_ts.into_inner(), Ordering::Release);
        Ok(())
    }

    fn fetch_result(&mut self, max_ts: TimeStamp) -> Result<(Vec<LockInfo>, bool)> {
        let curr_max_ts = self.observer_state.max_ts.load(Ordering::Acquire);
        let curr_max_ts = TimeStamp::new(curr_max_ts);
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
            .drain(..)
            .map(|(k, l)| {
                k.into_raw()
                    .map(|raw_key| l.into_lock_info(raw_key))
                    .map_err(|e| Error::from(MvccError::from(e)))
            })
            .collect();

        Ok((locks?, self.is_clean))
    }

    fn stop_collecting(&mut self, max_ts: TimeStamp) -> Result<()> {
        let curr_max_ts =
            self.observer_state
                .max_ts
                .compare_and_swap(max_ts.into_inner(), 0, Ordering::SeqCst);
        let curr_max_ts = TimeStamp::new(curr_max_ts);

        if curr_max_ts == max_ts {
            self.collected_locks.clear();
            info!("stop collecting locks"; "max_ts" => max_ts);
            Ok(())
        } else {
            warn!(
                "trying to stop collecting locks, but now collecting with a different max_ts";
                "stopping_max_ts" => max_ts,
                "current_max_ts" => curr_max_ts,
            );
            Err(box_err!("collecting locks with another max_ts"))
        }
    }
}

impl Runnable<LockCollectorTask> for LockCollectorRunner {
    fn run(&mut self, task: LockCollectorTask) {
        match task {
            LockCollectorTask::ObserverMsg(msg) => self.handle_observer_msg(msg),
            LockCollectorTask::StartCollecting { max_ts, callback } => {
                callback(self.start_collecting(max_ts))
            }
            LockCollectorTask::FetchResult { max_ts, callback } => {
                callback(self.fetch_result(max_ts))
            }
            LockCollectorTask::StopCollecting { max_ts, callback } => {
                callback(self.stop_collecting(max_ts))
            }
        }
    }
}

pub struct AppliedLockCollector {
    worker: Mutex<Worker<LockCollectorTask>>,
    scheduler: Scheduler<LockCollectorTask>,
}

impl AppliedLockCollector {
    pub fn new(coprocessor_host: &mut CoprocessorHost) -> Result<Self> {
        let worker = Mutex::new(WorkerBuilder::new("lock-collector").create());

        let scheduler = worker.lock().unwrap().scheduler();

        let state = Arc::new(LockObserverState::default());
        let runner = LockCollectorRunner::new(Arc::clone(&state));
        let observer = LockObserver::new(state, scheduler.clone());

        observer.register(coprocessor_host);

        // Start the worker
        worker.lock().unwrap().start(runner)?;

        Ok(Self { worker, scheduler })
    }

    pub fn stop(&self) -> Result<()> {
        if let Some(h) = self.worker.lock().unwrap().stop() {
            if let Err(e) = h.join() {
                return Err(box_err!(
                    "failed to join applied_lock_collector handle, err: {:?}",
                    e
                ));
            }
        }
        Ok(())
    }

    pub fn start_collecting(&self, max_ts: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.scheduler
            .schedule(LockCollectorTask::StartCollecting { max_ts, callback })
            .map_err(|e| box_err!("failed to schedule task: {:?}", e))
    }

    pub fn fetch_result(
        &self,
        max_ts: TimeStamp,
        callback: Callback<(Vec<LockInfo>, bool)>,
    ) -> Result<()> {
        self.scheduler
            .schedule(LockCollectorTask::FetchResult { max_ts, callback })
            .map_err(|e| box_err!("failed to schedule task: {:?}", e))
    }

    pub fn stop_collecting(&self, max_ts: TimeStamp, callback: Callback<()>) -> Result<()> {
        self.scheduler
            .schedule(LockCollectorTask::StopCollecting { max_ts, callback })
            .map_err(|e| box_err!("failed to schedule task: {:?}", e))
    }
}

impl Drop for AppliedLockCollector {
    fn drop(&mut self) {
        let r = self.stop();
        if let Err(e) = r {
            error!("Failed to stop applied_lock_collector"; "err" => ?e);
        }
    }
}
