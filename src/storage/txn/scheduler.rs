// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients,
//! executes them against the MVCC layer storage engine.
//!
//! Logically, the data organization hierarchy from bottom to top is row ->
//! region -> store -> database. But each region is replicated onto N stores for
//! reliability, the replicas form a Raft group, one of which acts as the
//! leader. When the client read or write a row, the command is sent to the
//! scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are
//! delegated to a pool of worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure
//! serialized access to the overlapping rows involved in concurrent commands.
//! But note that scheduler only ensures serialized access to the overlapping
//! rows at command level, but a transaction may consist of multiple commands,
//! therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library,
//! which is transparent to the scheduler.

use std::{
    iter::Iterator,
    marker::PhantomData,
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::channel,
        Arc,
    },
    time::Duration,
    u64,
};

use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use crossbeam::utils::CachePadded;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::compat::Future01CompatExt;
use kvproto::{
    kvrpcpb::{CommandPri, Context, DiskFullOpt, ExtraOp},
    pdpb::QueryKind,
};
use parking_lot::{Mutex, MutexGuard, RwLockWriteGuard};
use pd_client::{Feature, FeatureGate};
use raftstore::store::TxnExt;
use resource_metering::{FutureExt, ResourceTagFactory};
use tikv_kv::{Modify, Snapshot, SnapshotExt, WriteData};
use tikv_util::{quota_limiter::QuotaLimiter, time::Instant, timer::GLOBAL_TIMER_HANDLE};
use tracker::{get_tls_tracker_token, set_tls_tracker_token, TrackerToken};
use txn_types::TimeStamp;

use crate::{
    server::lock_manager::waiter_manager,
    storage::{
        config::Config,
        get_priority_tag,
        kv::{
            self, with_tls_engine, Engine, Error as EngineError, ExtCallback, FlowStatsReporter,
            Result as EngineResult, SnapContext, Statistics,
        },
        lock_manager::{
            self, DiagnosticContext, LockDigest, LockManager, LockWaitToken, WaitTimeout,
        },
        metrics::{self, *},
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, ReleasedLock},
        txn::{
            commands::{
                self, wake_up_legacy_pessimistic_lock_waits::WakeUpLegacyPessimisticLockWaits,
                CallbackWithArcError, Command, PessimisticLockKeyCallback, ReleasedLocks,
                ResponsePolicy, SyncCommandContext, WriteContext, WriteResult, WriteResultLockInfo,
            },
            flow_controller::FlowController,
            latch::{Latches, Lock, LockWaitQueueMap},
            sched_pool::{tls_collect_query, tls_collect_scan_details, SchedPool},
            Error, ProcessResult,
        },
        types::{PessimisticLockKeyResult, PessimisticLockResults, StorageCallback},
        DynamicConfigs, Error as StorageError, ErrorInner as StorageErrorInner,
    },
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

// The default limit is set to be very large. Then, requests without
// `max_exectuion_duration` will not be aborted unexpectedly.
pub const DEFAULT_EXECUTION_DURATION_LIMIT: Duration = Duration::from_secs(24 * 60 * 60);

const IN_MEMORY_PESSIMISTIC_LOCK: Feature = Feature::require(6, 0, 0);

/// Task is a running command.
pub(super) struct Task {
    pub(super) cid: u64,
    pub(super) tracker: TrackerToken,
    pub(super) cmd: Command,
    pub(super) extra_op: ExtraOp,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn new(cid: u64, tracker: TrackerToken, cmd: Command) -> Task {
        Task {
            cid,
            tracker,
            cmd,
            extra_op: ExtraOp::Noop,
        }
    }
}

struct CmdTimer {
    tag: CommandKind,
    begin: Instant,
}

impl Drop for CmdTimer {
    fn drop(&mut self) {
        SCHED_HISTOGRAM_VEC_STATIC
            .get(self.tag)
            .observe(self.begin.saturating_elapsed_secs());
    }
}

// It stores context of a task.
struct TaskContext {
    task: Option<Task>,

    lock: Lock,
    cb: Option<SchedulerTaskCallback>,
    pr: Option<ProcessResult>,
    // The one who sets `owned` from false to true is allowed to take
    // `cb` and `pr` safely.
    owned: AtomicBool,
    write_bytes: usize,
    tag: CommandKind,
    // How long it waits on latches.
    // latch_timer: Option<Instant>,
    latch_timer: Instant,
    // Total duration of a command.
    _cmd_timer: CmdTimer,
}

impl TaskContext {
    fn new(task: Task, cb: SchedulerTaskCallback, prepared_latches: Option<Lock>) -> TaskContext {
        let tag = task.cmd.tag();
        let lock = prepared_latches.unwrap_or_else(|| task.cmd.gen_lock());
        // The initial locks should be either all acquired or all not acquired.
        assert!(lock.owned_count == 0 || lock.owned_count == lock.required_hashes.len());
        // Write command should acquire write lock.
        if !task.cmd.readonly() && !lock.is_write_lock() {
            panic!("write lock is expected for command {}", task.cmd);
        }
        let write_bytes = if lock.is_write_lock() {
            task.cmd.write_bytes()
        } else {
            0
        };

        TaskContext {
            task: Some(task),
            lock,
            cb: Some(cb),
            pr: None,
            owned: AtomicBool::new(false),
            write_bytes,
            tag,
            latch_timer: Instant::now(),
            _cmd_timer: CmdTimer {
                tag,
                begin: Instant::now(),
            },
        }
    }

    fn on_schedule(&mut self) {
        SCHED_LATCH_HISTOGRAM_VEC
            .get(self.tag)
            .observe(self.latch_timer.saturating_elapsed_secs());
    }

    // Try to own this TaskContext by setting `owned` from false to true.
    // Returns whether it succeeds to own the TaskContext.
    fn try_own(&self) -> bool {
        self.owned
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
    }
}

pub enum SchedulerTaskCallback {
    NormalRequestCallback(StorageCallback),
    LockKeyCallbacks(Vec<CallbackWithArcError<PessimisticLockKeyResult>>),
}

impl SchedulerTaskCallback {
    fn execute(self, pr: ProcessResult) {
        match self {
            Self::NormalRequestCallback(cb) => cb.execute(pr),
            Self::LockKeyCallbacks(cbs) => match pr {
                ProcessResult::Failed { err }
                | ProcessResult::PessimisticLockRes { res: Err(err) } => {
                    let err = Arc::new(err);
                    for cb in cbs {
                        cb(Err(err.clone()));
                    }
                }
                ProcessResult::PessimisticLockRes { res: Ok(v) } => {
                    assert_eq!(v.0.len(), cbs.len());
                    for (res, cb) in v.0.into_iter().zip(cbs) {
                        cb(Ok(res))
                    }
                }
                _ => unreachable!(),
            },
        }
    }
}

struct SchedulerInner<L: LockManager> {
    // slot_id -> { cid -> `TaskContext` } in the slot.
    task_slots: Vec<CachePadded<Mutex<HashMap<u64, TaskContext>>>>,

    // cmd id generator
    id_alloc: CachePadded<AtomicU64>,

    // write concurrency control
    latches: Latches,

    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: SchedPool,

    // high priority commands and system commands will be delivered to this pool
    high_priority_pool: SchedPool,

    // used to control write flow
    running_write_bytes: CachePadded<AtomicUsize>,

    flow_controller: Arc<FlowController>,

    control_mutex: Arc<tokio::sync::Mutex<bool>>,

    lock_mgr: L,

    concurrency_manager: ConcurrencyManager,

    pipelined_pessimistic_lock: Arc<AtomicBool>,

    in_memory_pessimistic_lock: Arc<AtomicBool>,

    enable_async_apply_prewrite: bool,

    resource_tag_factory: ResourceTagFactory,

    quota_limiter: Arc<QuotaLimiter>,
    feature_gate: FeatureGate,
}

#[inline]
fn id_index(cid: u64) -> usize {
    cid as usize % TASKS_SLOTS_NUM
}

impl<L: LockManager> SchedulerInner<L> {
    /// Generates the next command ID.
    #[inline]
    fn gen_id(&self) -> u64 {
        let id = self.id_alloc.fetch_add(1, Ordering::Relaxed);
        id + 1
    }

    #[inline]
    fn get_task_slot(&self, cid: u64) -> MutexGuard<'_, HashMap<u64, TaskContext>> {
        self.task_slots[id_index(cid)].lock()
    }

    fn new_task_context(
        &self,
        task: Task,
        callback: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) -> TaskContext {
        let tctx = TaskContext::new(task, callback, prepared_latches);
        let running_write_bytes = self
            .running_write_bytes
            .fetch_add(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes + tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.inc();
        tctx
    }

    fn dequeue_task_context(&self, cid: u64) -> TaskContext {
        let tctx = self.get_task_slot(cid).remove(&cid).unwrap();

        let running_write_bytes = self
            .running_write_bytes
            .fetch_sub(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes - tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.dec();

        tctx
    }

    fn take_task_cb_and_pr(
        &self,
        cid: u64,
    ) -> (Option<SchedulerTaskCallback>, Option<ProcessResult>) {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(|tctx| (tctx.cb.take(), tctx.pr.take()))
            .unwrap_or((None, None))
    }

    fn store_pr(&self, cid: u64, pr: ProcessResult) {
        self.get_task_slot(cid).get_mut(&cid).unwrap().pr = Some(pr);
    }

    fn too_busy(&self, region_id: u64) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire) >= self.sched_pending_write_threshold
            || self.flow_controller.should_drop(region_id)
    }

    /// Tries to acquire all the required latches for a command when waken up by
    /// another finished command.
    ///
    /// Returns a deadline error if the deadline is exceeded. Returns the `Task`
    /// if all latches are acquired, returns `None` otherwise.
    fn acquire_lock_on_wakeup(
        &self,
        cid: u64,
    ) -> Result<Option<(Task, MutexGuard<'_, HashMap<u64, TaskContext>>)>, StorageError> {
        let mut task_slot = self.get_task_slot(cid);
        let tctx = task_slot.get_mut(&cid).unwrap();
        // Check deadline early during acquiring latches to avoid expired requests
        // blocking other requests.
        if let Err(e) = tctx.task.as_ref().unwrap().cmd.deadline().check() {
            // `acquire_lock_on_wakeup` is called when another command releases its locks
            // and wakes up command `cid`. This command inserted its lock before
            // and now the lock is at the front of the queue. The actual
            // acquired count is one more than the `owned_count` recorded in the
            // lock, so we increase one to make `release` work.
            tctx.lock.owned_count += 1;
            return Err(e.into());
        }
        if self.latches.acquire(&mut tctx.lock, cid) {
            tctx.on_schedule();
            match tctx.task.take() {
                Some(task) => return Ok(Some((task, task_slot))),
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.lock_mgr.dump_wait_for_entries(cb);
    }

    fn scale_pool_size(&self, pool_size: usize) {
        self.worker_pool.pool.scale_pool_size(pool_size);
        self.high_priority_pool
            .pool
            .scale_pool_size(std::cmp::max(1, pool_size / 2));
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct Scheduler<E: Engine, L: LockManager> {
    inner: Arc<SchedulerInner<L>>,
    // The engine can be fetched from the thread local storage of scheduler threads.
    // So, we don't store the engine here.
    _engine: PhantomData<E>,
}

unsafe impl<E: Engine, L: LockManager> Send for Scheduler<E, L> {}

impl<E: Engine, L: LockManager> Scheduler<E, L> {
    /// Creates a scheduler.
    pub(in crate::storage) fn new<R: FlowStatsReporter>(
        engine: E,
        lock_mgr: L,
        concurrency_manager: ConcurrencyManager,
        config: &Config,
        dynamic_configs: DynamicConfigs,
        flow_controller: Arc<FlowController>,
        reporter: R,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
        feature_gate: FeatureGate,
    ) -> Self {
        let t = Instant::now_coarse();
        let mut task_slots = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_slots.push(Mutex::new(Default::default()).into());
        }

        let inner = Arc::new(SchedulerInner {
            task_slots,
            id_alloc: AtomicU64::new(0).into(),
            latches: Latches::new(config.scheduler_concurrency),
            running_write_bytes: AtomicUsize::new(0).into(),
            sched_pending_write_threshold: config.scheduler_pending_write_threshold.0 as usize,
            worker_pool: SchedPool::new(
                engine.clone(),
                config.scheduler_worker_pool_size,
                reporter.clone(),
                "sched-worker-pool",
            ),
            high_priority_pool: SchedPool::new(
                engine,
                std::cmp::max(1, config.scheduler_worker_pool_size / 2),
                reporter,
                "sched-high-pri-pool",
            ),
            control_mutex: Arc::new(tokio::sync::Mutex::new(false)),
            lock_mgr,
            concurrency_manager,
            pipelined_pessimistic_lock: dynamic_configs.pipelined_pessimistic_lock,
            in_memory_pessimistic_lock: dynamic_configs.in_memory_pessimistic_lock,
            enable_async_apply_prewrite: config.enable_async_apply_prewrite,
            flow_controller,
            resource_tag_factory,
            quota_limiter,
            feature_gate,
        });

        slow_log!(
            t.saturating_elapsed(),
            "initialized the transaction scheduler"
        );
        let scheduler = Scheduler {
            inner,
            _engine: PhantomData,
        };

        let scheduler1 = scheduler.clone();

        scheduler
            .inner
            .lock_mgr
            .set_key_wake_up_delay_callback(Box::new(
                move |key: &txn_types::Key,
                      conflicting_start_ts: TimeStamp,
                      conflicting_commit_ts: TimeStamp,
                      wake_up_before: std::time::Instant| {
                    scheduler1.schedule_command(
                        None,
                        WakeUpLegacyPessimisticLockWaits::new(
                            key.clone(),
                            conflicting_start_ts,
                            conflicting_commit_ts,
                            wake_up_before,
                            Context::default(),
                        )
                        .into(),
                        SchedulerTaskCallback::NormalRequestCallback(StorageCallback::Boolean(
                            Box::new(|_| ()),
                        )),
                        None,
                    );
                },
            ));

        scheduler
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.inner.dump_wait_for_entries(cb);
    }

    pub fn scale_pool_size(&self, pool_size: usize) {
        self.inner.scale_pool_size(pool_size)
    }

    pub(in crate::storage) fn run_cmd(&self, cmd: Command, callback: StorageCallback) {
        // write flow control
        if cmd.need_flow_control() && self.inner.too_busy(cmd.ctx().region_id) {
            SCHED_TOO_BUSY_COUNTER_VEC.get(cmd.tag()).inc();
            callback.execute(ProcessResult::Failed {
                err: StorageError::from(StorageErrorInner::SchedTooBusy),
            });
            return;
        }
        self.schedule_command(
            None,
            cmd,
            SchedulerTaskCallback::NormalRequestCallback(callback),
            None,
        );
    }

    /// Releases all the latches held by a command.
    fn release_latches(
        &self,
        lock: Lock,
        cid: u64,
        wait_for_locks: Option<Vec<WriteResultLockInfo>>,
        released_locks: Option<ReleasedLocks>,
        tag: metrics::CommandKind,
    ) {
        let start_time = Instant::now();
        let next_cid_for_holding_latches = if released_locks.is_some() {
            Some(self.inner.gen_id())
        } else {
            None
        };
        let (cmd_wakeup_list, latch_keep_list, mut txn_lock_wakeup_list, queues) =
            self.inner.latches.release(
                lock,
                cid,
                wait_for_locks,
                released_locks,
                next_cid_for_holding_latches,
            );
        for wcid in cmd_wakeup_list {
            self.try_to_wake_up(wcid);
        }

        let mut awakened_legacy_locks = vec![];

        {
            let mut i = 0;
            while i < txn_lock_wakeup_list.len() {
                if txn_lock_wakeup_list[i].allow_lock_with_conflict {
                    i += 1;
                } else {
                    awakened_legacy_locks.push(txn_lock_wakeup_list.swap_remove(i));
                }
            }
        }

        assert_eq!(latch_keep_list.is_empty(), txn_lock_wakeup_list.is_empty());

        if !txn_lock_wakeup_list.is_empty() {
            self.schedule_awakened_pessimistic_locks(
                next_cid_for_holding_latches.unwrap(),
                txn_lock_wakeup_list,
                latch_keep_list,
                queues,
            );
        }

        if !awakened_legacy_locks.is_empty() {
            let mut wake_up_events = Vec::with_capacity(awakened_legacy_locks.len());
            for lock in &awakened_legacy_locks {
                wake_up_events.push(lock_manager::KeyWakeUpEvent {
                    key: lock.key.clone(),
                    released_start_ts: 0.into(),
                    released_commit_ts: 0.into(),
                    awakened_start_ts: lock.parameters.start_ts,
                    awakened_allow_resuming: lock.allow_lock_with_conflict,
                })
            }
            self.inner.lock_mgr.on_keys_wakeup(wake_up_events);
            self.wake_up_legacy_pessimistic_locks(awakened_legacy_locks.into_iter().map(|item| {
                let key = item.key.clone();
                (item, ReleasedLock::new(0.into(), None, key, false))
            }))
        }

        STORAGE_RELEASE_LATCH_DURATION_HISTOGRAM_STATIC
            .get(tag)
            .observe(start_time.saturating_elapsed_secs())
    }

    fn schedule_command(
        &self,
        specified_cid: Option<u64>,
        cmd: Command,
        callback: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) {
        let cid = specified_cid.unwrap_or_else(|| self.inner.gen_id());
        let tracker = get_tls_tracker_token();
        debug!("received new command"; "cid" => cid, "cmd" => ?cmd, "tracker" => ?tracker);

        let tag = cmd.tag();
        let priority_tag = get_priority_tag(cmd.priority());
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();

        let mut task_slot = self.inner.get_task_slot(cid);
        let tctx = task_slot.entry(cid).or_insert_with(|| {
            self.inner
                .new_task_context(Task::new(cid, tracker, cmd), callback, prepared_latches)
        });
        let deadline = tctx.task.as_ref().unwrap().cmd.deadline();
        if self.inner.latches.acquire(&mut tctx.lock, cid) {
            fail_point!("txn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            self.execute(task, task_slot);
            return;
        }
        // Check deadline in background.
        let sched = self.clone();
        self.inner
            .high_priority_pool
            .pool
            .spawn(async move {
                GLOBAL_TIMER_HANDLE
                    .delay(deadline.to_std_instant())
                    .compat()
                    .await
                    .unwrap();
                let cb = sched
                    .inner
                    .get_task_slot(cid)
                    .get_mut(&cid)
                    .and_then(|tctx| if tctx.try_own() { tctx.cb.take() } else { None });
                if let Some(cb) = cb {
                    cb.execute(ProcessResult::Failed {
                        err: StorageErrorInner::DeadlineExceeded.into(),
                    })
                }
            })
            .unwrap();
        fail_point!("txn_scheduler_acquire_fail");
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches
    /// are acquired, the method initiates a get snapshot operation for further
    /// processing.
    fn try_to_wake_up(&self, cid: u64) {
        match self.inner.acquire_lock_on_wakeup(cid) {
            Ok(Some((task, task_slot))) => {
                fail_point!("txn_scheduler_try_to_wake_up");
                self.execute(task, task_slot);
            }
            Ok(None) => {}
            Err(err) => {
                // Spawn the finish task to the pool to avoid stack overflow
                // when many queuing tasks fail successively.
                let this = self.clone();
                self.inner
                    .worker_pool
                    .pool
                    .spawn(async move {
                        this.finish_with_err(cid, err);
                    })
                    .unwrap();
            }
        }
    }

    fn schedule_awakened_pessimistic_locks(
        &self,
        cid: u64,
        mut awakened_locks_info: Vec<WriteResultLockInfo>,
        latches: Vec<u64>,
        lock_wait_queues: HashMap<u64, LockWaitQueueMap>,
    ) {
        let start_time = Instant::now();

        let key_callbacks: Vec<_> = awakened_locks_info
            .iter_mut()
            .map(|i| i.key_cb.take().unwrap())
            .collect();

        let latches = Lock::new_already_acquired(latches, lock_wait_queues);
        let cmd = commands::AcquirePessimisticLock::new_resumed_from_lock_info(awakened_locks_info);

        // TODO: Make flow control take effect on this thing.
        self.schedule_command(
            Some(cid),
            cmd.into(),
            SchedulerTaskCallback::LockKeyCallbacks(key_callbacks),
            Some(latches),
        );

        STORAGE_KEYWISE_PESSIMISTIC_LOCK_HANDLE_DURATION_HISTOGRAM_STATIC
            .schedule
            .observe(start_time.saturating_elapsed_secs());
    }

    // pub for test
    pub fn get_sched_pool(&self, priority: CommandPri) -> &SchedPool {
        if priority == CommandPri::High {
            &self.inner.high_priority_pool
        } else {
            &self.inner.worker_pool
        }
    }

    /// Executes the task in the sched pool.
    fn execute(&self, mut task: Task, task_slot: MutexGuard<'_, HashMap<u64, TaskContext>>) {
        if task.cmd.is_sync_cmd() {
            self.process_sync(task, task_slot);
            return;
        }

        // Release the mutex.
        drop(task_slot);

        set_tls_tracker_token(task.tracker);
        let sched = self.clone();
        self.get_sched_pool(task.cmd.priority())
            .pool
            .spawn(async move {
                fail_point!("scheduler_start_execute");
                if sched.check_task_deadline_exceeded(&task) {
                    return;
                }

                let tag = task.cmd.tag();
                SCHED_STAGE_COUNTER_VEC.get(tag).snapshot.inc();

                let snap_ctx = SnapContext {
                    pb_ctx: task.cmd.ctx(),
                    ..Default::default()
                };
                // The program is currently in scheduler worker threads.
                // Safety: `self.inner.worker_pool` should ensure that a TLS engine exists.
                match unsafe { with_tls_engine(|engine: &E| kv::snapshot(engine, snap_ctx)) }.await
                {
                    Ok(snapshot) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();
                        let term = snapshot.ext().get_term();
                        let extra_op = snapshot.ext().get_txn_extra_op();
                        if !sched
                            .inner
                            .get_task_slot(task.cid)
                            .get(&task.cid)
                            .unwrap()
                            .try_own()
                        {
                            sched.finish_with_err(task.cid, StorageErrorInner::DeadlineExceeded);
                            return;
                        }

                        if let Some(term) = term {
                            task.cmd.ctx_mut().set_term(term.get());
                        }
                        task.extra_op = extra_op;

                        debug!(
                            "process cmd with snapshot";
                            "cid" => task.cid, "term" => ?term, "extra_op" => ?extra_op,
                            "trakcer" => ?task.tracker
                        );
                        sched.process(snapshot, task).await;
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();

                        info!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                        sched.finish_with_err(task.cid, Error::from(err));
                    }
                }
            })
            .unwrap();
    }

    /// Calls the callback with an error.
    fn finish_with_err<ER>(&self, cid: u64, err: ER)
    where
        StorageError: From<ER>,
    {
        debug!("write command finished with error"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);

        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).error.inc();

        let pr = ProcessResult::Failed {
            err: StorageError::from(err),
        };
        if let Some(cb) = tctx.cb {
            cb.execute(pr);
        }

        self.release_latches(tctx.lock, cid, None, None, tctx.tag);
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers
    /// the result to the callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: CommandKind) {
        SCHED_STAGE_COUNTER_VEC.get(tag).read_finish.inc();

        debug!("read command finished"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
            self.schedule_command(None, cmd, tctx.cb.unwrap(), None);
        } else {
            tctx.cb.unwrap().execute(pr);
        }

        self.release_latches(tctx.lock, cid, None, None, tag);
    }

    /// Event handler for the success of write.
    fn on_write_finished(
        &self,
        cid: u64,
        pr: Option<ProcessResult>,
        result: EngineResult<()>,
        lock_guards: Vec<KeyHandleGuard>,
        wait_for_locks: Option<Vec<WriteResultLockInfo>>,
        mut released_locks: Option<ReleasedLocks>,
        pipelined: bool,
        async_apply_prewrite: bool,
        tag: CommandKind,
    ) {
        assert!(!(wait_for_locks.is_some() && released_locks.is_some()));

        // TODO: Does async apply prewrite worth a special metric here?
        if pipelined {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .pipelined_write_finish
                .inc();
        } else if async_apply_prewrite {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .async_apply_prewrite_finish
                .inc();
        } else {
            SCHED_STAGE_COUNTER_VEC.get(tag).write_finish.inc();
        }

        debug!("write command finished";
            "cid" => cid, "pipelined" => pipelined, "async_apply_prewrite" => async_apply_prewrite);
        drop(lock_guards);
        let tctx = self.inner.dequeue_task_context(cid);

        // If pipelined pessimistic lock or async apply prewrite takes effect, it's not
        // guaranteed that the proposed or committed callback is surely invoked, which
        // takes and invokes `tctx.cb(tctx.pr)`.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr.or(tctx.pr).unwrap(),
                Err(e) => {
                    if !Self::need_wake_up_pessimistic_lock_on_error(&e) {
                        released_locks = None;
                    }
                    ProcessResult::Failed {
                        err: StorageError::from(e),
                    }
                }
            };
            if let ProcessResult::NextCommand { cmd } = pr {
                SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
                self.schedule_command(None, cmd, cb, None);
            } else {
                cb.execute(pr);
            }
        } else {
            assert!((pipelined && wait_for_locks.is_none()) || async_apply_prewrite);
        }
        self.release_latches(tctx.lock, cid, wait_for_locks, released_locks, tag);
    }

    /// Event handler for the request of waiting for lock
    fn on_wait_for_lock(
        &self,
        context: &Context,
        token: LockWaitToken,
        start_ts: TimeStamp,
        is_first_lock: bool,
        lock_info: &[WriteResultLockInfo],
        cancel_callback: Box<dyn FnOnce(StorageError) + Send>,
        wait_timeout: Option<WaitTimeout>,
        diag_ctx: DiagnosticContext,
    ) {
        // debug!("command waits for lock released"; "cid" => cid);
        let wait_info = lock_info
            .iter()
            .map(|lock| lock_manager::KeyLockWaitInfo {
                key: lock.key.clone(),
                lock_digest: LockDigest {
                    ts: lock.last_found_lock.get_lock_version().into(),
                    hash: lock.lock_hash,
                },
                lock_info: lock.last_found_lock.clone(),
            })
            .collect();
        self.inner.lock_mgr.wait_for(
            token,
            context.get_region_id(),
            context.get_region_epoch().clone(),
            context.get_term(),
            start_ts,
            wait_info,
            is_first_lock,
            wait_timeout,
            cancel_callback,
            diag_ctx,
        );
    }

    fn update_wait_for_lock(&self) {
        // TODO: unimplemented.
    }

    fn need_wake_up_pessimistic_lock_on_error(_e: &EngineError) -> bool {
        // TODO: If there's some cases that `engine.async_write` returns error but it's
        // still possible that the data is successfully written, return true.
        // However if it's caused by region leader transferring or epoch change, it
        // doesn't need to return true because we'll have other way to cancel
        // the waiting request.
        false
    }

    fn early_response(
        cid: u64,
        cb: SchedulerTaskCallback,
        pr: ProcessResult,
        tag: CommandKind,
        stage: CommandStageKind,
    ) {
        debug!("early return response"; "cid" => cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).get(stage).inc();
        cb.execute(pr);
        // It won't release locks here until write finished.
    }

    /// Process the task in the current thread.
    async fn process(self, snapshot: E::Snap, task: Task) {
        if self.check_task_deadline_exceeded(&task) {
            return;
        }

        let resource_tag = self.inner.resource_tag_factory.new_tag(task.cmd.ctx());
        async {
            let tag = task.cmd.tag();
            fail_point!("scheduler_async_snapshot_finish");
            SCHED_STAGE_COUNTER_VEC.get(tag).process.inc();

            let timer = Instant::now();

            let region_id = task.cmd.ctx().get_region_id();
            let ts = task.cmd.ts();
            let mut statistics = Statistics::default();
            match &task.cmd {
                Command::Prewrite(_) | Command::PrewritePessimistic(_) => {
                    tls_collect_query(region_id, QueryKind::Prewrite);
                }
                Command::AcquirePessimisticLock(_) => {
                    tls_collect_query(region_id, QueryKind::AcquirePessimisticLock);
                }
                Command::Commit(_) => {
                    tls_collect_query(region_id, QueryKind::Commit);
                }
                Command::Rollback(_) | Command::PessimisticRollback(_) => {
                    tls_collect_query(region_id, QueryKind::Rollback);
                }
                _ => {}
            }

            fail_point!("scheduler_process");
            if task.cmd.readonly() {
                self.process_read(snapshot, task, &mut statistics);
            } else {
                self.process_write(snapshot, task, &mut statistics).await;
            };
            tls_collect_scan_details(tag.get_str(), &statistics);
            let elapsed = timer.saturating_elapsed();
            slow_log!(
                elapsed,
                "[region {}] scheduler handle command: {}, ts: {}",
                region_id,
                tag,
                ts
            );
        }
        .in_resource_metering_tag(resource_tag)
        .await;
    }

    /// Processes a read command within a worker thread, then posts
    /// `ReadFinished` message back to the `Scheduler`.
    fn process_read(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);

        let tag = task.cmd.tag();

        let begin_instant = Instant::now();
        let cmd = task.cmd;
        let pr = unsafe {
            with_perf_context::<E, _, _>(tag, || {
                cmd.process_read(snapshot, statistics)
                    .unwrap_or_else(|e| ProcessResult::Failed { err: e.into() })
            })
        };
        SCHED_PROCESSING_READ_HISTOGRAM_STATIC
            .get(tag)
            .observe(begin_instant.saturating_elapsed_secs());
        self.on_read_finished(task.cid, pr, tag);
    }

    /// Processes a write command within a worker thread, then posts either a
    /// `WriteFinished` message if successful or a `FinishedWithErr` message
    /// back to the `Scheduler`.
    async fn process_write(self, snapshot: E::Snap, mut task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_write");
        let write_bytes = task.cmd.write_bytes();
        let tag = task.cmd.tag();
        let cid = task.cid;
        let priority = task.cmd.priority();
        let scheduler = self.clone();
        let quota_limiter = self.inner.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample(true);
        let pessimistic_lock_mode = self.pessimistic_lock_mode();
        let pipelined =
            task.cmd.can_be_pipelined() && pessimistic_lock_mode == PessimisticLockMode::Pipelined;
        let txn_ext = snapshot.ext().get_txn_ext().cloned();

        // Keep some information to use after the command being consumed.
        let pessimistic_lock_single_request_meta =
            if let Command::AcquirePessimisticLock(cmd) = &mut task.cmd {
                cmd.get_single_request_meta()
            } else {
                None
            };

        let deadline = task.cmd.deadline();
        let write_result = {
            let _guard = sample.observe_cpu();
            let context = WriteContext {
                lock_mgr: &self.inner.lock_mgr,
                concurrency_manager: self.inner.concurrency_manager.clone(),
                extra_op: task.extra_op,
                statistics,
                async_apply_prewrite: self.inner.enable_async_apply_prewrite,
            };
            let begin_instant = Instant::now();
            let res = unsafe {
                with_perf_context::<E, _, _>(tag, || {
                    task.cmd
                        .process_write(snapshot, context)
                        .map_err(StorageError::from)
                })
            };
            SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                .get(tag)
                .observe(begin_instant.saturating_elapsed_secs());
            res
        };

        if write_result.is_ok() {
            // TODO: write bytes can be a bit inaccurate due to error requests or in-memory
            // pessimistic locks.
            sample.add_write_bytes(write_bytes);
        }
        let read_bytes = statistics.cf_statistics(CF_DEFAULT).flow_stats.read_bytes
            + statistics.cf_statistics(CF_LOCK).flow_stats.read_bytes
            + statistics.cf_statistics(CF_WRITE).flow_stats.read_bytes;
        sample.add_read_bytes(read_bytes);
        let quota_delay = quota_limiter.consume_sample(sample, true).await;
        if !quota_delay.is_zero() {
            TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                .get(tag)
                .inc_by(quota_delay.as_micros() as u64);
        }

        let WriteResult {
            ctx,
            mut to_be_write,
            rows,
            pr,
            mut released_locks,
            lock_guards,
            response_policy,
        } = match deadline
            .check()
            .map_err(StorageError::from)
            .and(write_result)
        {
            // Write prepare failure typically means conflicting transactions are detected. Delivers
            // the error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();
                debug!("write command failed"; "cid" => cid, "err" => ?err);
                scheduler.finish_with_err(cid, err);
                return;
            }
            // Initiates an async write operation on the storage engine, there'll be a
            // `WriteFinished` message when it finishes.
            Ok(res) => res,
        };
        let region_id = ctx.get_region_id();
        SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

        let mut pr = Some(pr);
        let mut lock_info = if tag == CommandKind::acquire_pessimistic_lock
            || tag == CommandKind::acquire_pessimistic_lock_resumed
        {
            if let Some(cmd_meta) = pessimistic_lock_single_request_meta {
                let conversion_start = Instant::now();

                let mut lock_info_list =
                    Self::take_lock_info_from_pessimistic_lock_pr(pr.as_mut().unwrap());
                // We currently do not allow multi-waiting in a single request.
                assert!(lock_info_list.is_none() || lock_info_list.as_ref().unwrap().len() == 1);
                if let Some(l) = lock_info_list.as_mut() {
                    // Enter key-wise waiting state.
                    let diag_ctx = DiagnosticContext {
                        key: l[0].key.to_raw().unwrap(),
                        resource_group_tag: ctx.get_resource_group_tag().into(),
                    };
                    let wait_token = scheduler.inner.lock_mgr.allocate_token();

                    let first_batch_size = cmd_meta.keys_count - l.len();

                    if cmd_meta.allow_lock_with_conflict {
                        // Only single-key requests are allowed in the new mode currently.
                        assert_eq!(first_batch_size, 0);
                    }

                    let lock_req_ctx = scheduler.convert_to_keywise_callbacks(
                        cid,
                        wait_token,
                        cmd_meta.start_ts,
                        cmd_meta.for_update_ts,
                        mem::replace(&mut pr, Some(ProcessResult::Res)).unwrap(),
                        cmd_meta.keys_count,
                        first_batch_size,
                        cmd_meta.allow_lock_with_conflict,
                    );

                    scheduler.on_wait_for_lock(
                        &ctx,
                        wait_token,
                        cmd_meta.start_ts,
                        cmd_meta.is_first_lock,
                        l,
                        Box::new(lock_req_ctx.get_callback_for_cancellation()),
                        l[0].parameters.wait_timeout,
                        diag_ctx,
                    );
                    // It's possible that the request is cancelled immediately due to no timeout
                    // (non-blocking locking).
                    if lock_req_ctx.shared_states.finished.load(Ordering::Acquire) {
                        lock_info_list = None;
                    } else {
                        let mut key_callback_count = 0;
                        for lock_info in l {
                            assert!(lock_info.key_cb.is_none());
                            // TODO: Check if there are paths that the cb is never invoked.
                            lock_info.key_cb = Some(
                                lock_req_ctx
                                    .get_callback_for_blocked_key(lock_info.index_in_request),
                            );
                            lock_info.req_states = Some(lock_req_ctx.get_shared_states());
                            key_callback_count += 1;
                        }
                        assert_eq!(key_callback_count, 1);
                    }
                }

                STORAGE_KEYWISE_PESSIMISTIC_LOCK_HANDLE_DURATION_HISTOGRAM_STATIC
                    .convert_from_normal
                    .observe(conversion_start.saturating_elapsed_secs());
                lock_info_list
            } else {
                // It's already in key-wise mode.
                let start_time = Instant::now();
                scheduler.update_wait_for_lock();
                scheduler.tidy_up_pessimistic_lock_result(cid, pr.as_mut().unwrap());
                let lock_info_list =
                    Self::take_lock_info_from_pessimistic_lock_pr(pr.as_mut().unwrap());

                STORAGE_KEYWISE_PESSIMISTIC_LOCK_HANDLE_DURATION_HISTOGRAM_STATIC
                    .tidy_up_result
                    .observe(start_time.saturating_elapsed_secs());
                lock_info_list
            }
        } else {
            None
        };

        if let Some(lock_info) = lock_info.as_mut() {
            // Set the start time of lock waiting
            let now = std::time::Instant::now();
            lock_info
                .iter_mut()
                .for_each(|i| i.wait_start_time = Some(now));
        }

        if let Some(released_locks) = released_locks.as_mut() {
            scheduler.on_release_locks_pre_persist(cid, released_locks);
        }

        if to_be_write.modifies.is_empty() {
            scheduler.on_write_finished(
                cid,
                pr,
                Ok(()),
                lock_guards,
                lock_info,
                released_locks,
                false,
                false,
                tag,
            );
            return;
        }

        if (tag == CommandKind::acquire_pessimistic_lock
            || tag == CommandKind::acquire_pessimistic_lock_resumed)
            && pessimistic_lock_mode == PessimisticLockMode::InMemory
            && self.try_write_in_memory_pessimistic_locks(
                txn_ext.as_deref(),
                &mut to_be_write,
                &ctx,
            )
        {
            // Safety: `self.sched_pool` ensures a TLS engine exists.
            unsafe {
                with_tls_engine(|engine: &E| {
                    // We skip writing the raftstore, but to improve CDC old value hit rate,
                    // we should send the old values to the CDC scheduler.
                    engine.schedule_txn_extra(to_be_write.extra);
                })
            }
            scheduler.on_write_finished(
                cid,
                pr,
                Ok(()),
                lock_guards,
                lock_info,
                released_locks,
                false,
                false,
                tag,
            );
            return;
        }

        let mut is_async_apply_prewrite = false;
        let write_size = to_be_write.size();
        if ctx.get_disk_full_opt() == DiskFullOpt::AllowedOnAlmostFull {
            to_be_write.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull
        }
        to_be_write.deadline = Some(deadline);

        let sched = scheduler.clone();
        let sched_pool = scheduler.get_sched_pool(priority).pool.clone();

        let (proposed_cb, committed_cb): (Option<ExtCallback>, Option<ExtCallback>) =
            match response_policy {
                ResponsePolicy::OnApplied => (None, None),
                ResponsePolicy::OnCommitted => {
                    self.inner.store_pr(cid, pr.take().unwrap());
                    let sched = scheduler.clone();
                    // Currently, the only case that response is returned after finishing
                    // commit is async applying prewrites for async commit transactions.
                    // The committed callback is not guaranteed to be invoked. So store
                    // the `pr` to the tctx instead of capturing it to the closure.
                    let committed_cb = Box::new(move || {
                        fail_point!("before_async_apply_prewrite_finish", |_| {});
                        let (cb, pr) = sched.inner.take_task_cb_and_pr(cid);
                        Self::early_response(
                            cid,
                            cb.unwrap(),
                            pr.unwrap(),
                            tag,
                            CommandStageKind::async_apply_prewrite,
                        );
                    });
                    is_async_apply_prewrite = true;
                    (None, Some(committed_cb))
                }
                ResponsePolicy::OnProposed => {
                    // The only work `proposed_cb` currently does is to reply the response to the
                    // client. If there are some locks to wait for, we do not set the callback
                    // and `pr` is still `Some`.
                    if pipelined && lock_info.is_none() {
                        // The normal write process is respond to clients and release
                        // latches after async write finished. If pipelined pessimistic
                        // locking is enabled, the process becomes parallel and there are
                        // two msgs for one command:
                        //   1. Msg::PipelinedWrite: respond to clients
                        //   2. Msg::WriteFinished: deque context and release latches
                        // The proposed callback is not guaranteed to be invoked. So store
                        // the `pr` to the tctx instead of capturing it to the closure.
                        self.inner.store_pr(cid, pr.take().unwrap());
                        let sched = scheduler.clone();
                        // Currently, the only case that response is returned after finishing
                        // proposed phase is pipelined pessimistic lock.
                        // TODO: Unify the code structure of pipelined pessimistic lock and
                        // async apply prewrite.
                        let proposed_cb = Box::new(move || {
                            fail_point!("before_pipelined_write_finish", |_| {});
                            let (cb, pr) = sched.inner.take_task_cb_and_pr(cid);
                            Self::early_response(
                                cid,
                                cb.unwrap(),
                                pr.unwrap(),
                                tag,
                                CommandStageKind::pipelined_write,
                            );
                        });
                        (Some(proposed_cb), None)
                    } else {
                        (None, None)
                    }
                }
            };

        if self.inner.flow_controller.enabled() {
            if self.inner.flow_controller.is_unlimited(region_id) {
                // no need to delay if unthrottled, just call consume to record write flow
                let _ = self.inner.flow_controller.consume(region_id, write_size);
            } else {
                let start = Instant::now_coarse();
                // Control mutex is used to ensure there is only one request consuming the
                // quota. The delay may exceed 1s, and the speed limit is changed every second.
                // If the speed of next second is larger than the one of first second, without
                // the mutex, the write flow can't throttled strictly.
                let control_mutex = self.inner.control_mutex.clone();
                let _guard = control_mutex.lock().await;
                let delay = self.inner.flow_controller.consume(region_id, write_size);
                let delay_end = Instant::now_coarse() + delay;
                while !self.inner.flow_controller.is_unlimited(region_id) {
                    let now = Instant::now_coarse();
                    if now >= delay_end {
                        break;
                    }
                    if now >= deadline.inner() {
                        scheduler.finish_with_err(cid, StorageErrorInner::DeadlineExceeded);
                        self.inner.flow_controller.unconsume(region_id, write_size);
                        SCHED_THROTTLE_TIME.observe(start.saturating_elapsed_secs());
                        return;
                    }
                    GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + Duration::from_millis(1))
                        .compat()
                        .await
                        .unwrap();
                }
                SCHED_THROTTLE_TIME.observe(start.saturating_elapsed_secs());
            }
        }

        let (version, term) = (ctx.get_region_epoch().get_version(), ctx.get_term());
        // Mutations on the lock CF should overwrite the memory locks.
        // We only set a deleted flag here, and the lock will be finally removed when it
        // finishes applying. See the comments in `PeerPessimisticLocks` for how this
        // flag is used.
        let txn_ext2 = txn_ext.clone();
        let mut pessimistic_locks_guard = txn_ext2
            .as_ref()
            .map(|txn_ext| txn_ext.pessimistic_locks.write());
        let removed_pessimistic_locks = match pessimistic_locks_guard.as_mut() {
            Some(locks)
                // If there is a leader or region change, removing the locks is unnecessary.
                if locks.term == term && locks.version == version && !locks.is_empty() =>
            {
                to_be_write
                    .modifies
                    .iter()
                    .filter_map(|write| match write {
                        Modify::Put(cf, key, ..) | Modify::Delete(cf, key) if *cf == CF_LOCK => {
                            locks.get_mut(key).map(|(_, deleted)| {
                                *deleted = true;
                                key.to_owned()
                            })
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            }
            _ => vec![],
        };
        // Keep the read lock guard of the pessimistic lock table until the request is
        // sent to the raftstore.
        //
        // If some in-memory pessimistic locks need to be proposed, we will propose
        // another TransferLeader command. Then, we can guarentee even if the proposed
        // locks don't include the locks deleted here, the response message of the
        // transfer leader command must be later than this write command because this
        // write command has been sent to the raftstore. Then, we don't need to worry
        // this request will fail due to the voluntary leader transfer.
        let _downgraded_guard = pessimistic_locks_guard.and_then(|guard| {
            (!removed_pessimistic_locks.is_empty()).then(|| RwLockWriteGuard::downgrade(guard))
        });

        // The callback to receive async results of write prepare from the storage
        // engine.
        let engine_cb = Box::new(move |result: EngineResult<()>| {
            let ok = result.is_ok();
            if ok && !removed_pessimistic_locks.is_empty() {
                // Removing pessimistic locks when it succeeds to apply. This should be done in
                // the apply thread, to make sure it happens before other admin commands are
                // executed.
                if let Some(mut pessimistic_locks) = txn_ext
                    .as_ref()
                    .map(|txn_ext| txn_ext.pessimistic_locks.write())
                {
                    // If epoch version or term does not match, region or leader change has
                    // happened, so we needn't remove the key.
                    if pessimistic_locks.term == term && pessimistic_locks.version == version {
                        for key in removed_pessimistic_locks {
                            pessimistic_locks.remove(&key);
                        }
                    }
                }
            }

            sched_pool
                .spawn(async move {
                    fail_point!("scheduler_async_write_finish");

                    sched.on_write_finished(
                        cid,
                        pr,
                        result,
                        lock_guards,
                        lock_info,
                        released_locks,
                        pipelined,
                        is_async_apply_prewrite,
                        tag,
                    );
                    KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                        .get(tag)
                        .observe(rows as f64);

                    if !ok {
                        // Only consume the quota when write succeeds, otherwise failed write
                        // requests may exhaust the quota and other write requests would be in long
                        // delay.
                        if sched.inner.flow_controller.enabled() {
                            sched.inner.flow_controller.unconsume(region_id, write_size);
                        }
                    }
                })
                .unwrap()
        });

        // Safety: `self.sched_pool` ensures a TLS engine exists.
        unsafe {
            with_tls_engine(|engine: &E| {
                if let Err(e) =
                    engine.async_write_ext(&ctx, to_be_write, engine_cb, proposed_cb, committed_cb)
                {
                    SCHED_STAGE_COUNTER_VEC.get(tag).async_write_err.inc();

                    info!("engine async_write failed"; "cid" => cid, "err" => ?e);
                    scheduler.finish_with_err(cid, e);
                }
            })
        }
    }

    fn process_sync(&self, task: Task, mut task_slot: MutexGuard<'_, HashMap<u64, TaskContext>>) {
        let cid = task.cid;
        let tag = task.cmd.tag();
        let priority = task.cmd.priority();
        let mut on_finished_cb = None;
        let released_locks = task.cmd.process_sync(SyncCommandContext {
            latch: &mut task_slot.get_mut(&cid).unwrap().lock,
            on_finished: &mut on_finished_cb,
        });

        let sched = self.clone();
        self.get_sched_pool(priority)
            .pool
            .spawn(async move {
                let tctx = sched.inner.dequeue_task_context(cid);
                sched.release_latches(tctx.lock, cid, None, released_locks, tag);
                if let Some(cb) = on_finished_cb {
                    cb();
                }
                if let Some(cb) = tctx.cb {
                    cb.execute(ProcessResult::Res);
                }
            })
            .unwrap();
    }

    /// Returns whether it succeeds to write pessimistic locks to the in-memory
    /// lock table.
    fn try_write_in_memory_pessimistic_locks(
        &self,
        txn_ext: Option<&TxnExt>,
        to_be_write: &mut WriteData,
        context: &Context,
    ) -> bool {
        let txn_ext = match txn_ext {
            Some(txn_ext) => txn_ext,
            None => return false,
        };
        let mut pessimistic_locks = txn_ext.pessimistic_locks.write();
        // When not writable, it only means we cannot write locks to the in-memory lock
        // table, but it is still possible for the region to propose request.
        // When term or epoch version has changed, the request must fail. To be simple,
        // here we just let the request fallback to propose and let raftstore generate
        // an appropriate error.
        if !pessimistic_locks.is_writable()
            || pessimistic_locks.term != context.get_term()
            || pessimistic_locks.version != context.get_region_epoch().get_version()
        {
            return false;
        }
        match pessimistic_locks.insert(mem::take(&mut to_be_write.modifies)) {
            Ok(()) => {
                IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER_STATIC.success.inc();
                true
            }
            Err(modifies) => {
                IN_MEMORY_PESSIMISTIC_LOCKING_COUNTER_STATIC.full.inc();
                to_be_write.modifies = modifies;
                false
            }
        }
    }

    /// If the task has expired, return `true` and call the callback of
    /// the task with a `DeadlineExceeded` error.
    #[inline]
    fn check_task_deadline_exceeded(&self, task: &Task) -> bool {
        if let Err(e) = task.cmd.deadline().check() {
            self.finish_with_err(task.cid, e);
            true
        } else {
            false
        }
    }

    fn pessimistic_lock_mode(&self) -> PessimisticLockMode {
        let pipelined = self
            .inner
            .pipelined_pessimistic_lock
            .load(Ordering::Relaxed);
        let in_memory = self
            .inner
            .in_memory_pessimistic_lock
            .load(Ordering::Relaxed)
            && self
                .inner
                .feature_gate
                .can_enable(IN_MEMORY_PESSIMISTIC_LOCK);
        if pipelined && in_memory {
            PessimisticLockMode::InMemory
        } else if pipelined {
            PessimisticLockMode::Pipelined
        } else {
            PessimisticLockMode::Sync
        }
    }

    fn on_release_locks_pre_persist(&self, cid: u64, released_locks: &mut ReleasedLocks) {
        let mut wake_up_events = vec![];
        let mut legacy_wakeup_list = vec![];

        {
            let mut slot = self.inner.get_task_slot(cid);
            let wait_queues = &mut slot.get_mut(&cid).unwrap().lock.lock_wait_queues;
            released_locks.0 = std::mem::take(&mut released_locks.0)
                .into_iter()
                .filter_map(|released_lock| {
                    let key_queue_map = wait_queues.get_mut(&released_lock.hash_for_latch)?;
                    let queue = key_queue_map.get_mut(&released_lock.key)?;

                    // Lazy cleanup invalidated entries
                    while let Some(front) = queue.peek() {
                        if front
                            .0
                            .req_states
                            .as_ref()
                            .unwrap()
                            .finished
                            .load(Ordering::Acquire)
                        {
                            queue.pop();
                            continue;
                        }

                        break;
                    }

                    if queue.is_empty() {
                        // Unreachable, but keep it for safety.
                        key_queue_map.remove(&released_lock.key);
                        return None;
                    }

                    let front = queue.peek().unwrap();
                    wake_up_events.push(lock_manager::KeyWakeUpEvent {
                        key: released_lock.key.clone(),
                        released_start_ts: released_lock.start_ts,
                        released_commit_ts: released_lock.commit_ts,
                        awakened_start_ts: front.0.parameters.start_ts,
                        awakened_allow_resuming: front.0.allow_lock_with_conflict,
                    });

                    if front.0.allow_lock_with_conflict {
                        Some(released_lock)
                    } else {
                        let lock_info = queue.pop().unwrap();
                        if queue.is_empty() {
                            key_queue_map.remove(&released_lock.key);
                        }
                        legacy_wakeup_list.push((lock_info.unwrap(), released_lock));
                        None
                    }
                })
                .collect();
            // Release lock of the task slot.
        }

        self.inner.lock_mgr.on_keys_wakeup(wake_up_events);
        self.wake_up_legacy_pessimistic_locks(legacy_wakeup_list);
    }

    fn wake_up_legacy_pessimistic_locks(
        &self,
        legacy_wakeup_list: impl IntoIterator<Item = (WriteResultLockInfo, ReleasedLock)>
        + Send
        + 'static,
    ) {
        self.get_sched_pool(CommandPri::High)
            .pool
            .spawn(async move {
                for (mut lock_info, released_lock) in legacy_wakeup_list {
                    let cb = lock_info.key_cb.unwrap();
                    let e = StorageError::from(Error::from(MvccError::from(
                        MvccErrorInner::WriteConflict {
                            start_ts: lock_info.parameters.start_ts,
                            conflict_start_ts: released_lock.start_ts,
                            conflict_commit_ts: released_lock.commit_ts,
                            key: released_lock.key.into_raw().unwrap(),
                            primary: lock_info.last_found_lock.take_primary_lock(),
                        },
                    )));
                    let res = Err(Arc::new(e));
                    cb(res);
                }
            })
            .unwrap();
    }

    fn convert_to_keywise_callbacks(
        &self,
        cid: u64,
        lock_wait_token: LockWaitToken,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        first_batch_pr: ProcessResult,
        total_size: usize,
        first_batch_size: usize,
        allow_lock_with_conflict: bool,
    ) -> PartialPessimisticLockRequestContext<L> {
        let mut slot = self.inner.get_task_slot(cid);
        let task_ctx = slot.get_mut(&cid).unwrap();
        let cb = match task_ctx.cb.take() {
            Some(SchedulerTaskCallback::NormalRequestCallback(cb)) => cb,
            _ => unreachable!(),
        };

        let ctx = PartialPessimisticLockRequestContext::new(
            self.inner.lock_mgr.clone(),
            lock_wait_token,
            start_ts,
            for_update_ts,
            first_batch_pr,
            cb,
            total_size,
            allow_lock_with_conflict,
        );
        let first_batch_cb = ctx.get_callback_for_first_write_batch(first_batch_size);
        // TODO: Do not use NormalRequestCallback and remove
        // get_callback_for_first_write_batch
        task_ctx.cb = Some(SchedulerTaskCallback::NormalRequestCallback(first_batch_cb));
        ctx
    }

    fn tidy_up_pessimistic_lock_result(&self, cid: u64, pr: &mut ProcessResult) {
        let results = match pr {
            ProcessResult::PessimisticLockRes {
                res: Ok(PessimisticLockResults(res)),
            } => res,
            _ => unreachable!(),
        };

        let mut slot = self.inner.get_task_slot(cid);
        let task_ctx = slot.get_mut(&cid).unwrap();
        let cbs = match task_ctx.cb {
            Some(SchedulerTaskCallback::LockKeyCallbacks(ref mut v)) => v,
            _ => unreachable!(),
        };
        assert_eq!(results.len(), cbs.len());

        let mut finished_cbs = Vec::with_capacity(results.len());
        (0..results.len())
            .into_iter()
            .zip(std::mem::take(cbs))
            .for_each(|(res_index, cb)| {
                let result = &mut results[res_index];
                if let PessimisticLockKeyResult::Waiting(lock_info) = result {
                    lock_info.as_mut().unwrap().key_cb = Some(cb);
                } else {
                    finished_cbs.push(cb);
                }
            });
        *results = results
            .drain(..)
            .filter(|r| match r {
                PessimisticLockKeyResult::Waiting(_) => false,
                _ => true,
            })
            .collect();
        *cbs = finished_cbs;
        assert_eq!(results.len(), cbs.len());
    }

    fn take_lock_info_from_pessimistic_lock_pr(
        pr: &mut ProcessResult,
    ) -> Option<Vec<WriteResultLockInfo>> {
        let res = match pr {
            ProcessResult::PessimisticLockRes {
                res: Ok(PessimisticLockResults(res)),
            } => res
                .iter_mut()
                .filter_map(|l: &mut _| {
                    if let PessimisticLockKeyResult::Waiting(lock_info) = l {
                        Some(lock_info.take().unwrap())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
            _ => unreachable!(),
        };
        if res.is_empty() { None } else { Some(res) }
    }
}

pub struct PartialPessimisticLockRequestContextInner {
    pr: ProcessResult,
    cb: StorageCallback,
    result_rx: std::sync::mpsc::Receiver<(
        usize,
        std::result::Result<PessimisticLockKeyResult, Arc<StorageError>>,
    )>,
    lock_wait_token: LockWaitToken,
}

pub struct PartialPessimisticLockRequestSharedState {
    ctx_inner: Mutex<Option<PartialPessimisticLockRequestContextInner>>,
    remaining_keys: AtomicUsize,
    pub finished: AtomicBool,
}

#[derive(Clone)]
struct PartialPessimisticLockRequestContext<L: LockManager> {
    // (inner_ctx, remaining_count)
    shared_states: Arc<PartialPessimisticLockRequestSharedState>,
    tx: std::sync::mpsc::Sender<(
        usize,
        std::result::Result<PessimisticLockKeyResult, Arc<StorageError>>,
    )>,

    lock_manager: L,
    allow_lock_with_conflict: bool,

    // Fields for logging:
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
}

impl<L: LockManager> PartialPessimisticLockRequestContext<L> {
    fn new(
        lock_manager: L,
        lock_wait_token: LockWaitToken,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
        first_batch_pr: ProcessResult,
        cb: StorageCallback,
        size: usize,
        allow_lock_with_conflict: bool,
    ) -> Self {
        let (tx, rx) = channel();
        let inner = PartialPessimisticLockRequestContextInner {
            pr: first_batch_pr,
            cb,
            result_rx: rx,
            lock_wait_token,
        };
        Self {
            shared_states: Arc::new(PartialPessimisticLockRequestSharedState {
                ctx_inner: Mutex::new(Some(inner)),
                remaining_keys: AtomicUsize::new(size),
                finished: AtomicBool::new(false),
            }),
            tx,
            lock_manager,
            allow_lock_with_conflict,
            start_ts,
            for_update_ts,
        }
    }

    fn get_shared_states(&self) -> Arc<PartialPessimisticLockRequestSharedState> {
        self.shared_states.clone()
    }

    fn get_callback_for_first_write_batch(&self, first_batch_size: usize) -> StorageCallback {
        let ctx = self.clone();
        if self.allow_lock_with_conflict {
            StorageCallback::Boolean(Box::new(move |res| {
                if first_batch_size != 0 {
                    let prev = ctx
                        .shared_states
                        .remaining_keys
                        .fetch_sub(first_batch_size, Ordering::SeqCst);
                    if prev == first_batch_size {
                        ctx.finish_request(res.err(), true);
                    }
                }
            }))
        } else {
            StorageCallback::Boolean(Box::new(|res| {
                res.unwrap();
            }))
        }
    }

    fn get_callback_for_blocked_key(&self, index: usize) -> PessimisticLockKeyCallback {
        let ctx = self.clone();
        if self.allow_lock_with_conflict {
            Box::new(move |res| {
                // TODO: Handle error.
                if let Err(e) = ctx.tx.send((index, res)) {
                    info!("sending partial pessimistic lock result to closed channel, maybe the request is already cancelled due to error";
                        "start_ts" => ctx.start_ts,
                        "for_update_ts" => ctx.for_update_ts,
                        "err" => ?e
                    );
                    return;
                }
                let prev = ctx
                    .shared_states
                    .remaining_keys
                    .fetch_sub(1, Ordering::SeqCst);
                if prev == 1 {
                    // All keys are finished.
                    ctx.finish_request(None, false);
                }
            })
        } else {
            Box::new(move |res| {
                ctx.finish_request(Some(Arc::try_unwrap(res.unwrap_err()).unwrap()), false);
            })
        }
    }

    fn get_callback_for_cancellation(&self) -> impl FnOnce(StorageError) {
        let ctx = self.clone();
        move |e| {
            ctx.finish_request(Some(e), false);
        }
    }

    fn finish_request(&self, external_error: Option<StorageError>, fail_all_on_error: bool) {
        let start_time = Instant::now();
        let mut ctx_inner = if let Some(inner) = self.shared_states.ctx_inner.lock().take() {
            inner
        } else {
            info!("shared state for partial pessimistic lock already taken, perhaps due to error";
                "start_ts" => self.start_ts,
                "for_update_ts" => self.for_update_ts
            );
            return;
        };

        self.shared_states.finished.store(true, Ordering::Release);

        self.lock_manager
            .remove_lock_wait(ctx_inner.lock_wait_token);
        let results = match ctx_inner.pr {
            ProcessResult::PessimisticLockRes {
                res: Ok(PessimisticLockResults(ref mut v)),
            } => v,
            _ => unreachable!(),
        };

        if !self.allow_lock_with_conflict {
            assert_eq!(results.len(), 1);
            assert!(matches!(
                results[0],
                PessimisticLockKeyResult::Waiting(None)
            ));
            ctx_inner.cb.execute(ProcessResult::Failed {
                err: external_error.unwrap(),
            });
            return;
        }

        while let Ok((index, msg)) = ctx_inner.result_rx.try_recv() {
            assert!(matches!(
                results[index],
                PessimisticLockKeyResult::Waiting(None)
            ));
            match msg {
                Ok(lock_key_res) => results[index] = lock_key_res,
                Err(e) => results[index] = PessimisticLockKeyResult::Failed(e), // ???
            }
        }

        if let Some(e) = external_error {
            let e = Arc::new(e);
            for r in results {
                if fail_all_on_error || matches!(r, PessimisticLockKeyResult::Waiting(_)) {
                    *r = PessimisticLockKeyResult::Failed(e.clone());
                }
            }
        }
        STORAGE_KEYWISE_PESSIMISTIC_LOCK_HANDLE_DURATION_HISTOGRAM_STATIC
            .finish_request
            .observe(start_time.saturating_elapsed_secs());

        ctx_inner.cb.execute(ctx_inner.pr);
    }
}

#[derive(Debug, PartialEq)]
enum PessimisticLockMode {
    // Return success only if the pessimistic lock is persisted.
    Sync,
    // Return success after the pessimistic lock is proposed successfully.
    Pipelined,
    // Try to store pessimistic locks only in the memory.
    InMemory,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures_executor::block_on;
    use kvproto::kvrpcpb::{BatchRollbackRequest, CheckTxnStatusRequest, Context};
    use raftstore::store::{ReadStats, WriteStats};
    use tikv_util::{config::ReadableSize, future::paired_future_callback};
    use txn_types::Key;

    use super::*;
    use crate::storage::{
        lock_manager::DummyLockManager,
        mvcc::{self, Mutation},
        test_util::latest_feature_gate,
        txn::{
            commands,
            commands::TypedCommand,
            flow_controller::{EngineFlowController, FlowController},
        },
        TestEngineBuilder, TxnStatus,
    };

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        let readonly_cmds: Vec<Command> = vec![
            commands::ResolveLockReadPhase::new(temp_map.clone(), None, Context::default()).into(),
            commands::MvccByKey::new(Key::from_raw(b"k"), Context::default()).into(),
            commands::MvccByStartTs::new(25.into(), Context::default()).into(),
        ];
        let write_cmds: Vec<Command> = vec![
            commands::Prewrite::with_defaults(
                vec![Mutation::make_put(Key::from_raw(b"k"), b"v".to_vec())],
                b"k".to_vec(),
                10.into(),
            )
            .into(),
            commands::AcquirePessimisticLock::new_normal(
                vec![(Key::from_raw(b"k"), false)],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                TimeStamp::default(),
                Some(WaitTimeout::Default),
                false,
                TimeStamp::default(),
                false,
                false,
                Context::default(),
            )
            .into(),
            commands::Commit::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Cleanup::new(
                Key::from_raw(b"k"),
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Rollback::new(vec![Key::from_raw(b"k")], 10.into(), Context::default())
                .into(),
            commands::PessimisticRollback::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::ResolveLock::new(
                temp_map,
                None,
                vec![(
                    Key::from_raw(b"k"),
                    mvcc::Lock::new(
                        mvcc::LockType::Put,
                        b"k".to_vec(),
                        10.into(),
                        20,
                        None,
                        TimeStamp::zero(),
                        0,
                        TimeStamp::zero(),
                    ),
                )],
                Context::default(),
            )
            .into(),
            commands::ResolveLockLite::new(
                10.into(),
                TimeStamp::zero(),
                vec![Key::from_raw(b"k")],
                Context::default(),
            )
            .into(),
            commands::TxnHeartBeat::new(Key::from_raw(b"k"), 10.into(), 100, Context::default())
                .into(),
        ];

        let latches = Latches::new(1024);
        let write_locks: Vec<Lock> = write_cmds
            .into_iter()
            .enumerate()
            .map(|(id, cmd)| {
                let mut lock = cmd.gen_lock();
                assert_eq!(latches.acquire(&mut lock, id as u64), id == 0);
                lock
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut lock = cmd.gen_lock();
            assert!(latches.acquire(&mut lock, id as u64));
        }

        // acquire/release locks one by one.
        let max_id = write_locks.len() as u64 - 1;
        for (id, mut lock) in write_locks.into_iter().enumerate() {
            let id = id as u64;
            if id != 0 {
                assert!(latches.acquire(&mut lock, id));
            }
            let unlocked = latches.release(lock, id, None, None, None).0;
            if id as u64 == max_id {
                assert!(unlocked.is_empty());
            } else {
                assert_eq!(unlocked, vec![id + 1]);
            }
        }
    }

    #[test]
    fn test_acquire_latch_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
        );

        let mut lock = Lock::new(&[Key::from_raw(b"b")]);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut lock, cid));

        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));

        // The task waits for 200ms until it acquires the latch, but the execution
        // time limit is 100ms. Before the latch is released, it should return
        // DeadlineExceeded error.
        thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));
        scheduler.release_latches(lock, cid, None, None, CommandKind::prewrite);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_pool_available_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
        );

        // Spawn a task that sleeps for 500ms to occupy the pool. The next request
        // cannot run within 500ms.
        scheduler
            .get_sched_pool(CommandPri::Normal)
            .pool
            .spawn(async { thread::sleep(Duration::from_millis(500)) })
            .unwrap();

        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));

        // But the max execution duration is 100ms, so the deadline is exceeded.
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_flow_control_trottle_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
        );

        let mut req = CheckTxnStatusRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_primary_key(b"a".to_vec());
        req.set_lock_ts(10);
        req.set_rollback_if_not_exist(true);

        let cmd: TypedCommand<TxnStatus> = req.into();
        let (cb, f) = paired_future_callback();

        scheduler.inner.flow_controller.enable(true);
        scheduler.inner.flow_controller.set_speed_limit(0, 1.0);
        scheduler.run_cmd(cmd.cmd, StorageCallback::TxnStatus(cb));
        // The task waits for 200ms until it locks the control_mutex, but the execution
        // time limit is 100ms. Before the mutex is locked, it should return
        // DeadlineExceeded error.
        thread::sleep(Duration::from_millis(200));
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));
        // should unconsume if the request fails
        assert_eq!(scheduler.inner.flow_controller.total_bytes_consumed(0), 0);

        // A new request should not be blocked without flow control.
        scheduler
            .inner
            .flow_controller
            .set_speed_limit(0, f64::INFINITY);
        let mut req = CheckTxnStatusRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_primary_key(b"a".to_vec());
        req.set_lock_ts(10);
        req.set_rollback_if_not_exist(true);

        let cmd: TypedCommand<TxnStatus> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::TxnStatus(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_accumulate_many_expired_commands() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            latest_feature_gate(),
        );

        let mut lock = Lock::new(&[Key::from_raw(b"b")]);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut lock, cid));

        // Push lots of requests in the queue.
        for _ in 0..65536 {
            let mut req = BatchRollbackRequest::default();
            req.mut_context().max_execution_duration_ms = 100;
            req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());

            let cmd: TypedCommand<()> = req.into();
            let (cb, _) = paired_future_callback();
            scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        }

        // The task waits for 200ms until it acquires the latch, but the execution
        // time limit is 100ms.
        thread::sleep(Duration::from_millis(200));

        // When releasing the lock, the queuing tasks should be all waken up without
        // stack overflow.
        scheduler.release_latches(lock, cid, None, None, CommandKind::prewrite);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    #[test]
    fn test_pessimistic_lock_mode() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        let feature_gate = FeatureGate::default();
        feature_gate.set_version("6.0.0").unwrap();

        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            feature_gate.clone(),
        );
        // Use sync mode if pipelined_pessimistic_lock is false.
        assert_eq!(scheduler.pessimistic_lock_mode(), PessimisticLockMode::Sync);
        // Use sync mode even when in_memory is true.
        scheduler
            .inner
            .in_memory_pessimistic_lock
            .store(true, Ordering::SeqCst);
        assert_eq!(scheduler.pessimistic_lock_mode(), PessimisticLockMode::Sync);
        // Mode is InMemory when both pipelined and in_memory is true.
        scheduler
            .inner
            .pipelined_pessimistic_lock
            .store(true, Ordering::SeqCst);
        assert_eq!(
            scheduler.pessimistic_lock_mode(),
            PessimisticLockMode::InMemory
        );
        // Test the feature gate. The feature should not work under 6.0.0.
        unsafe { feature_gate.reset_version("5.4.0").unwrap() };
        assert_eq!(
            scheduler.pessimistic_lock_mode(),
            PessimisticLockMode::Pipelined
        );
        feature_gate.set_version("6.0.0").unwrap();
        // Mode is Pipelined when only pipelined is true.
        scheduler
            .inner
            .in_memory_pessimistic_lock
            .store(false, Ordering::SeqCst);
        assert_eq!(
            scheduler.pessimistic_lock_mode(),
            PessimisticLockMode::Pipelined
        );
    }
}
