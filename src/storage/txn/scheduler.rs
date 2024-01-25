// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
//! TxnScheduler which schedules the execution of `storage::Command`s.
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
//! TxnScheduler runs in a single-thread event loop, but command executions are
//! delegated to a pool of worker thread.
//!
//! TxnScheduler keeps track of all the running commands and uses latches to
//! ensure serialized access to the overlapping rows involved in concurrent
//! commands. But note that scheduler only ensures serialized access to the
//! overlapping rows at command level, but a transaction may consist of multiple
//! commands, therefore conflicts may happen at transaction level. Transaction
//! semantics is ensured by the transaction protocol implemented in the client
//! library, which is transparent to the scheduler.

use std::{
    marker::PhantomData,
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    u64,
};

use causal_ts::CausalTsProviderImpl;
use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use crossbeam::utils::CachePadded;
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use futures::{compat::Future01CompatExt, StreamExt};
use kvproto::{
    kvrpcpb::{self, CommandPri, Context, DiskFullOpt},
    pdpb::QueryKind,
};
use parking_lot::{Mutex, MutexGuard, RwLockWriteGuard};
use pd_client::{Feature, FeatureGate};
use raftstore::store::TxnExt;
use resource_control::ResourceController;
use resource_metering::{FutureExt, ResourceTagFactory};
use smallvec::{smallvec, SmallVec};
use tikv_kv::{Modify, Snapshot, SnapshotExt, WriteData, WriteEvent};
use tikv_util::{quota_limiter::QuotaLimiter, time::Instant, timer::GLOBAL_TIMER_HANDLE};
<<<<<<< HEAD
use tracker::{get_tls_tracker_token, set_tls_tracker_token, TrackerToken, GLOBAL_TRACKERS};
=======
use tracker::{set_tls_tracker_token, TrackerToken, GLOBAL_TRACKERS};
use txn_types::TimeStamp;
>>>>>>> 8780c0494b (storage: refactor command marco and task (#16440))

use super::task::Task;
use crate::{
    server::lock_manager::waiter_manager,
    storage::{
        config::Config,
        errors::SharedError,
        get_causal_ts, get_priority_tag, get_raw_key_guard,
        kv::{
            self, with_tls_engine, Engine, FlowStatsReporter, Result as EngineResult, SnapContext,
            Statistics,
        },
        lock_manager::{
            self,
            lock_wait_context::{LockWaitContext, PessimisticLockKeyCallback},
            lock_waiting_queue::{DelayedNotifyAllFuture, LockWaitEntry, LockWaitQueues},
            DiagnosticContext, LockManager, LockWaitToken,
        },
        metrics::*,
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner, ReleasedLock},
        txn::{
            commands,
            commands::{
                Command, RawExt, ReleasedLocks, ResponsePolicy, WriteContext, WriteResult,
                WriteResultLockInfo,
            },
            flow_controller::FlowController,
            latch::{Latches, Lock},
            sched_pool::{tls_collect_query, tls_collect_scan_details, SchedPool},
            Error, ErrorInner, ProcessResult,
        },
        types::StorageCallback,
        DynamicConfigs, Error as StorageError, ErrorInner as StorageErrorInner,
        PessimisticLockKeyResult, PessimisticLockResults,
    },
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

// The default limit is set to be very large. Then, requests without
// `max_exectuion_duration` will not be aborted unexpectedly.
pub const DEFAULT_EXECUTION_DURATION_LIMIT: Duration = Duration::from_secs(24 * 60 * 60);

const IN_MEMORY_PESSIMISTIC_LOCK: Feature = Feature::require(6, 0, 0);
pub const LAST_CHANGE_TS: Feature = Feature::require(6, 5, 0);

type SVec<T> = SmallVec<[T; 4]>;

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
    woken_up_resumable_lock_requests: SVec<Box<LockWaitEntry>>,
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
        let tag = task.cmd().tag();
        let lock = prepared_latches.unwrap_or_else(|| task.cmd().gen_lock());
        // The initial locks should be either all acquired or all not acquired.
        assert!(lock.owned_count == 0 || lock.owned_count == lock.required_hashes.len());
        // Write command should acquire write lock.
        if !task.cmd().readonly() && !lock.is_write_lock() {
            panic!("write lock is expected for command {}", task.cmd());
        }
        let write_bytes = if lock.is_write_lock() {
            task.cmd().write_bytes()
        } else {
            0
        };

        TaskContext {
            task: Some(task),
            lock,
            cb: Some(cb),
            pr: None,
            woken_up_resumable_lock_requests: smallvec![],
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
        let elapsed = self.latch_timer.saturating_elapsed();
        if let Some(task) = &self.task.as_ref() {
            GLOBAL_TRACKERS.with_tracker(task.tracker(), |tracker| {
                tracker.metrics.latch_wait_nanos = elapsed.as_nanos() as u64;
            });
        }
        SCHED_LATCH_HISTOGRAM_VEC
            .get(self.tag)
            .observe(elapsed.as_secs_f64());
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
    LockKeyCallbacks(Vec<PessimisticLockKeyCallback>),
}

impl SchedulerTaskCallback {
    fn execute(self, pr: ProcessResult) {
        match self {
            Self::NormalRequestCallback(cb) => cb.execute(pr),
            Self::LockKeyCallbacks(cbs) => match pr {
                ProcessResult::Failed { err }
                | ProcessResult::PessimisticLockRes { res: Err(err) } => {
                    let err = SharedError::from(err);
                    for cb in cbs {
                        cb(Err(err.clone()), false);
                    }
                }
                ProcessResult::PessimisticLockRes { res: Ok(v) } => {
                    assert_eq!(v.0.len(), cbs.len());
                    for (res, cb) in v.0.into_iter().zip(cbs) {
                        cb(Ok(res), false)
                    }
                }
                _ => unreachable!(),
            },
        }
    }

    fn unwrap_normal_request_callback(self) -> StorageCallback {
        match self {
            Self::NormalRequestCallback(cb) => cb,
            _ => panic!(""),
        }
    }
}

struct TxnSchedulerInner<L: LockManager> {
    // slot_id -> { cid -> `TaskContext` } in the slot.
    task_slots: Vec<CachePadded<Mutex<HashMap<u64, TaskContext>>>>,

    // cmd id generator
    id_alloc: CachePadded<AtomicU64>,

    // write concurrency control
    latches: Latches,

    sched_pending_write_threshold: usize,

    // all tasks are executed in this pool
    sched_worker_pool: SchedPool,

    // used to control write flow
    running_write_bytes: CachePadded<AtomicUsize>,

    flow_controller: Arc<FlowController>,

    // used for apiv2
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    control_mutex: Arc<tokio::sync::Mutex<bool>>,

    lock_mgr: L,

    concurrency_manager: ConcurrencyManager,

    pipelined_pessimistic_lock: Arc<AtomicBool>,

    in_memory_pessimistic_lock: Arc<AtomicBool>,

    enable_async_apply_prewrite: bool,

    pessimistic_lock_wake_up_delay_duration_ms: Arc<AtomicU64>,

    resource_tag_factory: ResourceTagFactory,

    lock_wait_queues: LockWaitQueues<L>,

    quota_limiter: Arc<QuotaLimiter>,
    feature_gate: FeatureGate,
}

#[inline]
fn id_index(cid: u64) -> usize {
    cid as usize % TASKS_SLOTS_NUM
}

impl<L: LockManager> TxnSchedulerInner<L> {
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

    /// Try to own the corresponding task context and take the callback.
    ///
    /// If the task is been processing, it should be owned.
    /// If it has been finished, then it is not in the slot.
    /// In both cases, cb should be None. Otherwise, cb should be some.
    fn try_own_and_take_cb(&self, cid: u64) -> Option<SchedulerTaskCallback> {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .and_then(|tctx| if tctx.try_own() { tctx.cb.take() } else { None })
    }

    fn take_task_cb(&self, cid: u64) -> Option<SchedulerTaskCallback> {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(|tctx| tctx.cb.take())
            .unwrap_or(None)
    }

    fn store_lock_changes(
        &self,
        cid: u64,
        woken_up_resumable_lock_requests: SVec<Box<LockWaitEntry>>,
    ) {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(move |tctx| {
                assert!(tctx.woken_up_resumable_lock_requests.is_empty());
                tctx.woken_up_resumable_lock_requests = woken_up_resumable_lock_requests;
            })
            .unwrap();
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
    ) -> Result<Option<Task>, (String, CommandPri, StorageError)> {
        let mut task_slot = self.get_task_slot(cid);
        let tctx = task_slot.get_mut(&cid).unwrap();
        // Check deadline early during acquiring latches to avoid expired requests
        // blocking other requests.
        let cmd = tctx.task.as_ref().unwrap().cmd();
        if let Err(e) = cmd.deadline().check() {
            // `acquire_lock_on_wakeup` is called when another command releases its locks
            // and wakes up command `cid`. This command inserted its lock before
            // and now the lock is at the front of the queue. The actual
            // acquired count is one more than the `owned_count` recorded in the
            // lock, so we increase one to make `release` work.
            tctx.lock.owned_count += 1;
            return Err((cmd.group_name(), cmd.priority(), e.into()));
        }
        if self.latches.acquire(&mut tctx.lock, cid) {
            tctx.on_schedule();
            return Ok(tctx.task.take());
        }
        Ok(None)
    }

    fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.lock_mgr.dump_wait_for_entries(cb);
    }

    fn scale_pool_size(&self, pool_size: usize) {
        self.sched_worker_pool.scale_pool_size(pool_size);
    }
}

/// TxnScheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct TxnScheduler<E: Engine, L: LockManager> {
    inner: Arc<TxnSchedulerInner<L>>,
    // The engine can be fetched from the thread local storage of scheduler threads.
    // So, we don't store the engine here.
    _engine: PhantomData<E>,
}

unsafe impl<E: Engine, L: LockManager> Send for TxnScheduler<E, L> {}

impl<E: Engine, L: LockManager> TxnScheduler<E, L> {
    /// Creates a scheduler.
    pub(in crate::storage) fn new<R: FlowStatsReporter>(
        engine: E,
        lock_mgr: L,
        concurrency_manager: ConcurrencyManager,
        config: &Config,
        dynamic_configs: DynamicConfigs,
        flow_controller: Arc<FlowController>,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
        reporter: R,
        resource_tag_factory: ResourceTagFactory,
        quota_limiter: Arc<QuotaLimiter>,
        feature_gate: FeatureGate,
        resource_ctl: Option<Arc<ResourceController>>,
    ) -> Self {
        let t = Instant::now_coarse();
        let mut task_slots = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_slots.push(Mutex::new(Default::default()).into());
        }

        let lock_wait_queues = LockWaitQueues::new(lock_mgr.clone());

        let inner = Arc::new(TxnSchedulerInner {
            task_slots,
            id_alloc: AtomicU64::new(0).into(),
            latches: Latches::new(config.scheduler_concurrency),
            running_write_bytes: AtomicUsize::new(0).into(),
            sched_pending_write_threshold: config.scheduler_pending_write_threshold.0 as usize,
            sched_worker_pool: SchedPool::new(
                engine,
                config.scheduler_worker_pool_size,
                reporter,
                feature_gate.clone(),
                resource_ctl,
            ),
            control_mutex: Arc::new(tokio::sync::Mutex::new(false)),
            lock_mgr,
            concurrency_manager,
            pipelined_pessimistic_lock: dynamic_configs.pipelined_pessimistic_lock,
            in_memory_pessimistic_lock: dynamic_configs.in_memory_pessimistic_lock,
            enable_async_apply_prewrite: config.enable_async_apply_prewrite,
            pessimistic_lock_wake_up_delay_duration_ms: dynamic_configs.wake_up_delay_duration_ms,
            flow_controller,
            causal_ts_provider,
            resource_tag_factory,
            lock_wait_queues,
            quota_limiter,
            feature_gate,
        });

        slow_log!(
            t.saturating_elapsed(),
            "initialized the transaction scheduler"
        );
        TxnScheduler {
            inner,
            _engine: PhantomData,
        }
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
        let cid = self.inner.gen_id();
        let task = Task::new(cid, cmd);
        self.schedule_command(
            task,
            SchedulerTaskCallback::NormalRequestCallback(callback),
            None,
        );
    }

    /// Releases all the latches held by a command.
    fn release_latches(
        &self,
        lock: Lock,
        cid: u64,
        keep_latches_for_next_cmd: Option<(u64, &Lock)>,
    ) {
        let wakeup_list = self
            .inner
            .latches
            .release(&lock, cid, keep_latches_for_next_cmd);
        for wcid in wakeup_list {
            self.try_to_wake_up(wcid);
        }
    }

    fn schedule_command(
        &self,
        task: Task,
        callback: SchedulerTaskCallback,
        prepared_latches: Option<Lock>,
    ) {
        let cid = task.cid();
        let tracker = task.tracker();
        let cmd = task.cmd();
        debug!("received new command"; "cid" => cid, "cmd" => ?cmd, "tracker" => ?tracker);

        let tag = cmd.tag();
        let priority_tag = get_priority_tag(cmd.priority());
        cmd.incr_cmd_metric();
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();

        let mut task_slot = self.inner.get_task_slot(cid);
        let tctx = task_slot.entry(cid).or_insert_with(|| {
            self.inner
                .new_task_context(task, callback, prepared_latches)
        });

        if self.inner.latches.acquire(&mut tctx.lock, cid) {
            fail_point!("txn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            drop(task_slot);
            self.execute(task);
            return;
        }
        let task = tctx.task.as_ref().unwrap();
        self.fail_fast_or_check_deadline(cid, task.cmd());
        fail_point!("txn_scheduler_acquire_fail");
    }

    fn fail_fast_or_check_deadline(&self, cid: u64, cmd: &Command) {
        let tag = cmd.tag();
        let ctx = cmd.ctx().clone();
        let deadline = cmd.deadline();
        let sched = self.clone();
        self.get_sched_pool()
            .spawn(&cmd.group_name(), cmd.priority(), async move {
                match unsafe {
                    with_tls_engine(|engine: &mut E| engine.precheck_write_with_ctx(&ctx))
                } {
                    // Precheck failed, try to return err early.
                    Err(e) => {
                        let cb = sched.inner.try_own_and_take_cb(cid);
                        // The task is not processing or finished currently. It's safe
                        // to response early here. In the future, the task will be waked up
                        // and it will finished with DeadlineExceeded error.
                        // As the cb is taken here, it will not be executed anymore.
                        if let Some(cb) = cb {
                            let pr = ProcessResult::Failed {
                                err: StorageError::from(e),
                            };
                            Self::early_response(
                                cid,
                                cb,
                                pr,
                                tag,
                                CommandStageKind::precheck_write_err,
                            );
                        }
                    }
                    Ok(()) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).precheck_write_ok.inc();
                        // Check deadline in background.
                        GLOBAL_TIMER_HANDLE
                            .delay(deadline.to_std_instant())
                            .compat()
                            .await
                            .unwrap();
                        let cb = sched.inner.try_own_and_take_cb(cid);
                        if let Some(cb) = cb {
                            cb.execute(ProcessResult::Failed {
                                err: StorageErrorInner::DeadlineExceeded.into(),
                            })
                        }
                    }
                }
            })
            .unwrap();
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches
    /// are acquired, the method initiates a get snapshot operation for further
    /// processing.
    fn try_to_wake_up(&self, cid: u64) {
        match self.inner.acquire_lock_on_wakeup(cid) {
            Ok(Some(task)) => {
                fail_point!("txn_scheduler_try_to_wake_up");
                self.execute(task);
            }
            Ok(None) => {}
            Err((group_name, pri, err)) => {
                // Spawn the finish task to the pool to avoid stack overflow
                // when many queuing tasks fail successively.
                let this = self.clone();
                self.get_sched_pool()
                    .spawn(&group_name, pri, async move {
                        this.finish_with_err(cid, err);
                    })
                    .unwrap();
            }
        }
    }

    fn schedule_awakened_pessimistic_locks(
        &self,
        specified_cid: Option<u64>,
        prepared_latches: Option<Lock>,
        mut awakened_entries: SVec<Box<LockWaitEntry>>,
    ) {
        let key_callbacks: Vec<_> = awakened_entries
            .iter_mut()
            .map(|i| i.key_cb.take().unwrap().into_inner())
            .collect();

        let cmd = commands::AcquirePessimisticLockResumed::from_lock_wait_entries(awakened_entries);
        let cid = specified_cid.unwrap_or_else(|| self.inner.gen_id());
        let task = Task::new(cid, cmd.into());

        // TODO: Make flow control take effect on this thing.
        self.schedule_command(
            task,
            SchedulerTaskCallback::LockKeyCallbacks(key_callbacks),
            prepared_latches,
        );
    }

    // pub for test
    pub fn get_sched_pool(&self) -> &SchedPool {
        &self.inner.sched_worker_pool
    }

    /// Executes the task in the sched pool.
    fn execute(&self, mut task: Task) {
        set_tls_tracker_token(task.tracker());
        let sched = self.clone();
<<<<<<< HEAD

        self.get_sched_pool()
            .spawn(&task.cmd.group_name(), task.cmd.priority(), async move {
=======
        let metadata = TaskMetadata::from_ctx(task.cmd().resource_control_ctx());

        self.get_sched_pool()
            .spawn(metadata, task.cmd().priority(), async move {
>>>>>>> 8780c0494b (storage: refactor command marco and task (#16440))
                fail_point!("scheduler_start_execute");
                if sched.check_task_deadline_exceeded(&task) {
                    return;
                }

                let tag = task.cmd().tag();
                SCHED_STAGE_COUNTER_VEC.get(tag).snapshot.inc();

                let mut snap_ctx = SnapContext {
                    pb_ctx: task.cmd().ctx(),
                    ..Default::default()
                };
                if matches!(
                    task.cmd(),
                    Command::FlashbackToVersionReadPhase { .. }
                        | Command::FlashbackToVersion { .. }
                ) {
                    snap_ctx.allowed_in_flashback = true;
                }
                // The program is currently in scheduler worker threads.
                // Safety: `self.inner.worker_pool` should ensure that a TLS engine exists.
                match unsafe { with_tls_engine(|engine: &mut E| kv::snapshot(engine, snap_ctx)) }
                    .await
                {
                    Ok(snapshot) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();
                        let term = snapshot.ext().get_term();
                        let extra_op = snapshot.ext().get_txn_extra_op();
                        if !sched
                            .inner
                            .get_task_slot(task.cid())
                            .get(&task.cid())
                            .unwrap()
                            .try_own()
                        {
<<<<<<< HEAD
                            sched.finish_with_err(task.cid, StorageErrorInner::DeadlineExceeded);
=======
                            sched.finish_with_err(
                                task.cid(),
                                StorageErrorInner::DeadlineExceeded,
                                None,
                            );
>>>>>>> 8780c0494b (storage: refactor command marco and task (#16440))
                            return;
                        }

                        if let Some(term) = term {
                            task.cmd_mut().ctx_mut().set_term(term.get());
                        }
                        task.set_extra_op(extra_op);

                        debug!(
                            "process cmd with snapshot";
                            "cid" => task.cid(), "term" => ?term, "extra_op" => ?extra_op,
                            "tracker" => ?task.tracker()
                        );
                        sched.process(snapshot, task).await;
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();

<<<<<<< HEAD
                        info!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                        sched.finish_with_err(task.cid, Error::from(err));
=======
                        info!("get snapshot failed"; "cid" => task.cid(), "err" => ?err);
                        sched.finish_with_err(task.cid(), Error::from(err), None);
>>>>>>> 8780c0494b (storage: refactor command marco and task (#16440))
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

        if !tctx.woken_up_resumable_lock_requests.is_empty() {
            self.put_back_lock_wait_entries(tctx.woken_up_resumable_lock_requests);
        }
        self.release_latches(tctx.lock, cid, None);
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
            let task = Task::new(self.inner.gen_id(), cmd);
            self.schedule_command(task, tctx.cb.unwrap(), None);
        } else {
            tctx.cb.unwrap().execute(pr);
        }

        self.release_latches(tctx.lock, cid, None);
    }

    /// Event handler for the success of write.
    fn on_write_finished(
        &self,
        cid: u64,
        pr: Option<ProcessResult>,
        result: EngineResult<()>,
        lock_guards: Vec<KeyHandleGuard>,
        pipelined: bool,
        async_apply_prewrite: bool,
        new_acquired_locks: Vec<kvrpcpb::LockInfo>,
        tag: CommandKind,
        group_name: &str,
        sched_details: &SchedulerDetails,
    ) {
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

        let mut do_wake_up = !tctx.woken_up_resumable_lock_requests.is_empty();
        // If pipelined pessimistic lock or async apply prewrite takes effect, it's not
        // guaranteed that the proposed or committed callback is surely invoked, which
        // takes and invokes `tctx.cb(tctx.pr)`.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr.or(tctx.pr).unwrap(),
                Err(e) => {
                    if !Self::is_undetermined_error(&e) {
                        do_wake_up = false;
                    } else {
                        panic!(
                            "undetermined error: {:?} cid={}, tag={}, process
                        result={:?}",
                            e, cid, tag, &pr
                        );
                    }
                    ProcessResult::Failed {
                        err: StorageError::from(e),
                    }
                }
            };
            if let ProcessResult::NextCommand { cmd } = pr {
                SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
                let task = Task::new(self.inner.gen_id(), cmd);
                self.schedule_command(task, cb, None);
            } else {
                GLOBAL_TRACKERS.with_tracker(sched_details.tracker, |tracker| {
                    tracker.metrics.scheduler_process_nanos = sched_details
                        .start_process_instant
                        .saturating_elapsed()
                        .as_nanos()
                        as u64;
                    tracker.metrics.scheduler_throttle_nanos =
                        sched_details.flow_control_nanos + sched_details.quota_limit_delay_nanos;
                });
                cb.execute(pr);
            }
        } else {
            assert!(pipelined || async_apply_prewrite);
        }

        self.on_acquired_locks_finished(group_name, new_acquired_locks);

        if do_wake_up {
            let woken_up_resumable_lock_requests = tctx.woken_up_resumable_lock_requests;
            let next_cid = self.inner.gen_id();
            let mut next_latches =
                Self::gen_latches_for_lock_wait_entries(woken_up_resumable_lock_requests.iter());

            self.release_latches(tctx.lock, cid, Some((next_cid, &next_latches)));

            next_latches.force_assume_acquired();
            self.schedule_awakened_pessimistic_locks(
                Some(next_cid),
                Some(next_latches),
                woken_up_resumable_lock_requests,
            );
        } else {
            if !tctx.woken_up_resumable_lock_requests.is_empty() {
                self.put_back_lock_wait_entries(tctx.woken_up_resumable_lock_requests);
            }
            self.release_latches(tctx.lock, cid, None);
        }
    }

    fn gen_latches_for_lock_wait_entries<'a>(
        entries: impl IntoIterator<Item = &'a Box<LockWaitEntry>>,
    ) -> Lock {
        Lock::new(entries.into_iter().map(|entry| &entry.key))
    }

    /// Event handler for the request of waiting for lock
    fn on_wait_for_lock(
        &self,
        ctx: &Context,
        cid: u64,
        lock_info: WriteResultLockInfo,
        tracker: TrackerToken,
    ) {
        let key = lock_info.key.clone();
        let lock_digest = lock_info.lock_digest;
        let start_ts = lock_info.parameters.start_ts;
        let is_first_lock = lock_info.parameters.is_first_lock;
        let wait_timeout = lock_info.parameters.wait_timeout;
        let allow_lock_with_conflict = lock_info.parameters.allow_lock_with_conflict;

        let diag_ctx = DiagnosticContext {
            key: lock_info.key.to_raw().unwrap(),
            resource_group_tag: ctx.get_resource_group_tag().into(),
            tracker,
        };
        let wait_token = self.inner.lock_mgr.allocate_token();

        let (lock_req_ctx, lock_wait_entry, lock_info_pb) =
            self.make_lock_waiting(cid, wait_token, lock_info);

        // The entry must be pushed to the lock waiting queue before sending to
        // `lock_mgr`. When the request is canceled in anywhere outside the lock
        // waiting queue (including `lock_mgr`), it first tries to remove the
        // entry from the lock waiting queue. If the entry doesn't exist
        // in the queue, it will be regarded as already popped out from the queue and
        // therefore will woken up, thus the canceling operation will be
        // skipped. So pushing the entry to the queue must be done before any
        // possible cancellation.
        self.inner
            .lock_wait_queues
            .push_lock_wait(lock_wait_entry, lock_info_pb.clone());

        let wait_info = lock_manager::KeyLockWaitInfo {
            key,
            lock_digest,
            lock_info: lock_info_pb,
            allow_lock_with_conflict,
        };
        self.inner.lock_mgr.wait_for(
            wait_token,
            ctx.get_region_id(),
            ctx.get_region_epoch().clone(),
            ctx.get_term(),
            start_ts,
            wait_info,
            is_first_lock,
            wait_timeout,
            lock_req_ctx.get_callback_for_cancellation(),
            diag_ctx,
        );
    }

    fn on_release_locks(
        &self,
        group_name: &str,
        released_locks: ReleasedLocks,
    ) -> SVec<Box<LockWaitEntry>> {
        // This function is always called when holding the latch of the involved keys.
        // So if we found the lock waiting queues are empty, there's no chance
        // that other threads/commands adds new lock-wait entries to the keys
        // concurrently. Therefore it's safe to skip waking up when we found the
        // lock waiting queues are empty.
        if self.inner.lock_wait_queues.is_empty() {
            return smallvec![];
        }

        let mut legacy_wake_up_list = SVec::new();
        let mut delay_wake_up_futures = SVec::new();
        let mut resumable_wake_up_list = SVec::new();
        let wake_up_delay_duration_ms = self
            .inner
            .pessimistic_lock_wake_up_delay_duration_ms
            .load(Ordering::Relaxed);

        released_locks.into_iter().for_each(|released_lock| {
            let (lock_wait_entry, delay_wake_up_future) =
                match self.inner.lock_wait_queues.pop_for_waking_up(
                    &released_lock.key,
                    released_lock.start_ts,
                    released_lock.commit_ts,
                    wake_up_delay_duration_ms,
                ) {
                    Some(e) => e,
                    None => return,
                };

            if lock_wait_entry.parameters.allow_lock_with_conflict {
                resumable_wake_up_list.push(lock_wait_entry);
            } else {
                legacy_wake_up_list.push((lock_wait_entry, released_lock));
            }
            if let Some(f) = delay_wake_up_future {
                delay_wake_up_futures.push(f);
            }
        });

        if !legacy_wake_up_list.is_empty() || !delay_wake_up_futures.is_empty() {
            self.wake_up_legacy_pessimistic_locks(
                group_name,
                legacy_wake_up_list,
                delay_wake_up_futures,
            );
        }

        resumable_wake_up_list
    }

    fn on_acquired_locks_finished(
        &self,
        group_name: &str,
        new_acquired_locks: Vec<kvrpcpb::LockInfo>,
    ) {
        if new_acquired_locks.is_empty() || self.inner.lock_wait_queues.is_empty() {
            return;
        }

        // If there are not too many new locks, do not spawn the task to the high
        // priority pool since it may consume more CPU.
        if new_acquired_locks.len() < 30 {
            self.inner
                .lock_wait_queues
                .update_lock_wait(new_acquired_locks);
        } else {
            let lock_wait_queues = self.inner.lock_wait_queues.clone();
            self.get_sched_pool()
                .spawn(group_name, CommandPri::High, async move {
                    lock_wait_queues.update_lock_wait(new_acquired_locks);
                })
                .unwrap();
        }
    }

    fn wake_up_legacy_pessimistic_locks(
        &self,
        group_name: &str,
        legacy_wake_up_list: impl IntoIterator<Item = (Box<LockWaitEntry>, ReleasedLock)>
        + Send
        + 'static,
        delayed_wake_up_futures: impl IntoIterator<Item = DelayedNotifyAllFuture> + Send + 'static,
    ) {
        let self1 = self.clone();
        let group_name1 = group_name.to_owned();
        self.get_sched_pool()
            .spawn(group_name, CommandPri::High, async move {
                for (lock_info, released_lock) in legacy_wake_up_list {
                    let cb = lock_info.key_cb.unwrap().into_inner();
                    let e = StorageError::from(Error::from(MvccError::from(
                        MvccErrorInner::WriteConflict {
                            start_ts: lock_info.parameters.start_ts,
                            conflict_start_ts: released_lock.start_ts,
                            conflict_commit_ts: released_lock.commit_ts,
                            key: released_lock.key.into_raw().unwrap(),
                            primary: lock_info.parameters.primary,
                            reason: kvrpcpb::WriteConflictReason::PessimisticRetry,
                        },
                    )));
                    cb(Err(e.into()), false);
                }

                for f in delayed_wake_up_futures {
                    let self2 = self1.clone();
                    self1
                        .get_sched_pool()
                        .spawn(&group_name1, CommandPri::High, async move {
                            let res = f.await;
                            if let Some(resumable_lock_wait_entry) = res {
                                self2.schedule_awakened_pessimistic_locks(
                                    None,
                                    None,
                                    smallvec![resumable_lock_wait_entry],
                                );
                            }
                        })
                        .unwrap();
                }
            })
            .unwrap();
    }

    // Return true if raftstore returns error and the underlying write status could
    // not be decided.
    fn is_undetermined_error(e: &tikv_kv::Error) -> bool {
        if let tikv_kv::ErrorInner::Undetermined(err_msg) = &*(e.0) {
            error!(
                "undetermined error is encountered, exit the tikv-server msg={:?}",
                err_msg
            );
            true
        } else {
            false
        }
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

        let resource_tag = self.inner.resource_tag_factory.new_tag(task.cmd().ctx());
        async {
            let tag = task.cmd().tag();
            fail_point!("scheduler_async_snapshot_finish");
            SCHED_STAGE_COUNTER_VEC.get(tag).process.inc();

            let timer = Instant::now();

            let region_id = task.cmd().ctx().get_region_id();
            let ts = task.cmd().ts();
            let mut sched_details = SchedulerDetails::new(task.tracker(), timer);
            match task.cmd() {
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
            if task.cmd().readonly() {
                self.process_read(snapshot, task, &mut sched_details);
            } else {
                self.process_write(snapshot, task, &mut sched_details).await;
            };
            tls_collect_scan_details(tag.get_str(), &sched_details.stat);
            let elapsed = timer.saturating_elapsed();
            slow_log!(
                elapsed,
                "[region {}] scheduler handle command: {}, ts: {}, details: {:?}",
                region_id,
                tag,
                ts,
                sched_details,
            );
        }
        .in_resource_metering_tag(resource_tag)
        .await;
    }

    /// Processes a read command within a worker thread, then posts
    /// `ReadFinished` message back to the `TxnScheduler`.
    fn process_read(self, snapshot: E::Snap, task: Task, sched_details: &mut SchedulerDetails) {
        fail_point!("txn_before_process_read");
        let cid = task.cid();
        debug!("process read cmd in worker pool"; "cid" => cid);

        let tag = task.cmd().tag();

        let begin_instant = Instant::now();
        let pr = unsafe {
            with_perf_context::<E, _, _>(tag, || {
                task.process_read(snapshot, &mut sched_details.stat)
                    .unwrap_or_else(|e| ProcessResult::Failed { err: e.into() })
            })
        };
        SCHED_PROCESSING_READ_HISTOGRAM_STATIC
            .get(tag)
            .observe(begin_instant.saturating_elapsed_secs());
        self.on_read_finished(cid, pr, tag);
    }

    /// Processes a write command within a worker thread, then posts either a
    /// `WriteFinished` message if successful or a `FinishedWithErr` message
    /// back to the `TxnScheduler`.
    async fn process_write(
        self,
        snapshot: E::Snap,
        task: Task,
        sched_details: &mut SchedulerDetails,
    ) {
        fail_point!("txn_before_process_write");
<<<<<<< HEAD
        let write_bytes = task.cmd.write_bytes();
        let tag = task.cmd.tag();
        let cid = task.cid;
        let group_name = task.cmd.group_name();
        let tracker = task.tracker;
        let scheduler = self.clone();
        let quota_limiter = self.inner.quota_limiter.clone();
=======
        let write_bytes = task.cmd().write_bytes();
        let tag = task.cmd().tag();
        let cid = task.cid();
        let metadata = TaskMetadata::from_ctx(task.cmd().resource_control_ctx());
        let tracker = task.tracker();
        let scheduler = self.clone();
        let quota_limiter = self.inner.quota_limiter.clone();
        let resource_limiter = self.inner.resource_manager.as_ref().and_then(|m| {
            let ctx = task.cmd().ctx();
            m.get_resource_limiter(
                ctx.get_resource_control_context().get_resource_group_name(),
                ctx.get_request_source(),
                ctx.get_resource_control_context().get_override_priority(),
            )
        });
>>>>>>> 8780c0494b (storage: refactor command marco and task (#16440))
        let mut sample = quota_limiter.new_sample(true);
        let pessimistic_lock_mode = self.pessimistic_lock_mode();
        let pipelined = task.cmd().can_be_pipelined()
            && pessimistic_lock_mode == PessimisticLockMode::Pipelined;
        let txn_ext = snapshot.ext().get_txn_ext().cloned();
        let max_ts_synced = snapshot.ext().is_max_ts_synced();
        let causal_ts_provider = self.inner.causal_ts_provider.clone();
        let concurrency_manager = self.inner.concurrency_manager.clone();

        let raw_ext = get_raw_ext(
            causal_ts_provider,
            concurrency_manager.clone(),
            max_ts_synced,
            task.cmd(),
        )
        .await;
        if let Err(err) = raw_ext {
            info!("get_raw_ext failed"; "cid" => cid, "err" => ?err);
            scheduler.finish_with_err(cid, err);
            return;
        }
        let raw_ext = raw_ext.unwrap();

        let deadline = task.cmd().deadline();
        let write_result = {
            let _guard = sample.observe_cpu();
            let context = WriteContext {
                lock_mgr: &self.inner.lock_mgr,
                concurrency_manager,
                extra_op: task.extra_op(),
                statistics: &mut sched_details.stat,
                async_apply_prewrite: self.inner.enable_async_apply_prewrite,
                raw_ext,
            };
            let begin_instant = Instant::now();
            let res = unsafe {
                with_perf_context::<E, _, _>(tag, || {
                    task.process_write(snapshot, context)
                        .map_err(StorageError::from)
                })
            };
            SCHED_PROCESSING_READ_HISTOGRAM_STATIC
                .get(tag)
                .observe(begin_instant.saturating_elapsed_secs());
            res
        };

        let process_end = Instant::now();
        if write_result.is_ok() {
            // TODO: write bytes can be a bit inaccurate due to error requests or in-memory
            // pessimistic locks.
            sample.add_write_bytes(write_bytes);
        }
        let read_bytes = sched_details
            .stat
            .cf_statistics(CF_DEFAULT)
            .flow_stats
            .read_bytes
            + sched_details
                .stat
                .cf_statistics(CF_LOCK)
                .flow_stats
                .read_bytes
            + sched_details
                .stat
                .cf_statistics(CF_WRITE)
                .flow_stats
                .read_bytes;
        sample.add_read_bytes(read_bytes);
        let quota_delay = quota_limiter.consume_sample(sample, true).await;
        if !quota_delay.is_zero() {
            let actual_quota_delay = process_end.saturating_elapsed();
            sched_details.quota_limit_delay_nanos = actual_quota_delay.as_nanos() as u64;
            TXN_COMMAND_THROTTLE_TIME_COUNTER_VEC_STATIC
                .get(tag)
                .inc_by(quota_delay.as_micros() as u64);
        }

        let WriteResult {
            ctx,
            mut to_be_write,
            rows,
            pr,
            lock_info,
            released_locks,
            new_acquired_locks,
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

        if !lock_info.is_empty() {
            if tag == CommandKind::acquire_pessimistic_lock {
                assert_eq!(lock_info.len(), 1);
                let lock_info = lock_info.into_iter().next().unwrap();

                // Only handle lock waiting if `wait_timeout` is set. Otherwise it indicates
                // that it's a lock-no-wait request and we need to report error
                // immediately.
                if lock_info.parameters.wait_timeout.is_some() {
                    assert_eq!(to_be_write.size(), 0);
                    pr = Some(ProcessResult::Res);

                    scheduler.on_wait_for_lock(&ctx, cid, lock_info, tracker);
                } else {
                    // For requests with `allow_lock_with_conflict`, key errors are set key-wise.
                    // TODO: It's better to return this error from
                    // `commands::AcquirePessimisticLocks::process_write`.
                    if lock_info.parameters.allow_lock_with_conflict {
                        pr = Some(ProcessResult::PessimisticLockRes {
                            res: Err(StorageError::from(Error::from(MvccError::from(
                                MvccErrorInner::KeyIsLocked(lock_info.lock_info_pb),
                            )))),
                        });
                    }
                }
            } else if tag == CommandKind::acquire_pessimistic_lock_resumed {
                // Some requests meets lock again after waiting and resuming.
                scheduler.on_wait_for_lock_after_resuming(cid, pr.as_mut().unwrap(), lock_info);
            } else {
                // WriteResult returning lock info is only expected to exist for pessimistic
                // lock requests.
                unreachable!();
            }
        }

        let woken_up_resumable_entries = if !released_locks.is_empty() {
            scheduler.on_release_locks(&group_name, released_locks)
        } else {
            smallvec![]
        };

        if !woken_up_resumable_entries.is_empty() {
            scheduler
                .inner
                .store_lock_changes(cid, woken_up_resumable_entries);
        }

        if to_be_write.modifies.is_empty() {
            scheduler.on_write_finished(
                cid,
                pr,
                Ok(()),
                lock_guards,
                false,
                false,
                new_acquired_locks,
                tag,
                &group_name,
                sched_details,
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
                with_tls_engine(|engine: &mut E| {
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
                false,
                false,
                new_acquired_locks,
                tag,
                &group_name,
                sched_details,
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

        let mut subscribed = WriteEvent::BASIC_EVENT;
        match response_policy {
            ResponsePolicy::OnCommitted => {
                subscribed |= WriteEvent::EVENT_COMMITTED;
                is_async_apply_prewrite = true;
            }
            ResponsePolicy::OnProposed if pipelined => subscribed |= WriteEvent::EVENT_PROPOSED,
            _ => (),
        }

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
                let elapsed = start.saturating_elapsed();
                SCHED_THROTTLE_TIME.observe(elapsed.as_secs_f64());
                sched_details.flow_control_nanos = elapsed.as_nanos() as u64;
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
        // another TransferLeader command. Then, we can guarantee even if the proposed
        // locks don't include the locks deleted here, the response message of the
        // transfer leader command must be later than this write command because this
        // write command has been sent to the raftstore. Then, we don't need to worry
        // this request will fail due to the voluntary leader transfer.
        let downgraded_guard = pessimistic_locks_guard.and_then(|guard| {
            (!removed_pessimistic_locks.is_empty()).then(|| RwLockWriteGuard::downgrade(guard))
        });
        let on_applied = Box::new(move |res: &mut kv::Result<()>| {
            if res.is_ok() && !removed_pessimistic_locks.is_empty() {
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
        });

        let mut res = unsafe {
            with_tls_engine(|e: &mut E| {
                e.async_write(&ctx, to_be_write, subscribed, Some(on_applied))
            })
        };
        drop(downgraded_guard);

        while let Some(ev) = res.next().await {
            match ev {
                WriteEvent::Committed => {
                    let early_return = (|| {
                        fail_point!("before_async_apply_prewrite_finish", |_| false);
                        true
                    })();
                    if WriteEvent::subscribed_committed(subscribed) && early_return {
                        // Currently, the only case that response is returned after finishing
                        // commit is async applying prewrites for async commit transactions.
                        let cb = scheduler.inner.take_task_cb(cid);
                        Self::early_response(
                            cid,
                            cb.unwrap(),
                            pr.take().unwrap(),
                            tag,
                            CommandStageKind::async_apply_prewrite,
                        );
                    }
                }
                WriteEvent::Proposed => {
                    let early_return = (|| {
                        fail_point!("before_pipelined_write_finish", |_| false);
                        true
                    })();
                    if WriteEvent::subscribed_proposed(subscribed) && early_return {
                        // The normal write process is respond to clients and release
                        // latches after async write finished. If pipelined pessimistic
                        // locking is enabled, the process becomes parallel and there are
                        // two msgs for one command:
                        //   1. Msg::PipelinedWrite: respond to clients
                        //   2. Msg::WriteFinished: deque context and release latches
                        // Currently, the only case that response is returned after finishing
                        // proposed phase is pipelined pessimistic lock.
                        // TODO: Unify the code structure of pipelined pessimistic lock and
                        // async apply prewrite.
                        let cb = scheduler.inner.take_task_cb(cid);
                        Self::early_response(
                            cid,
                            cb.unwrap(),
                            pr.take().unwrap(),
                            tag,
                            CommandStageKind::pipelined_write,
                        );
                    }
                }
                WriteEvent::Finished(res) => {
                    fail_point!("scheduler_async_write_finish");
                    let ok = res.is_ok();

                    sched.on_write_finished(
                        cid,
                        pr,
                        res,
                        lock_guards,
                        pipelined,
                        is_async_apply_prewrite,
                        new_acquired_locks,
                        tag,
                        &group_name,
                        sched_details,
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
                    return;
                }
            }
        }
        // If it's not finished while the channel is closed, it means the write
        // is undeterministic. in this case, we don't know whether the
        // request is finished or not, so we should not release latch as
        // it may break correctness.
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
<<<<<<< HEAD
    fn check_task_deadline_exceeded(&self, task: &Task) -> bool {
        if let Err(e) = task.cmd.deadline().check() {
            self.finish_with_err(task.cid, e);
=======
    fn check_task_deadline_exceeded(
        &self,
        task: &Task,
        sched_details: Option<&SchedulerDetails>,
    ) -> bool {
        if let Err(e) = task.cmd().deadline().check() {
            self.finish_with_err(task.cid(), e, sched_details);
>>>>>>> 8780c0494b (storage: refactor command marco and task (#16440))
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

    fn make_lock_waiting(
        &self,
        cid: u64,
        lock_wait_token: LockWaitToken,
        lock_info: WriteResultLockInfo,
    ) -> (LockWaitContext<L>, Box<LockWaitEntry>, kvrpcpb::LockInfo) {
        let mut slot = self.inner.get_task_slot(cid);
        let task_ctx = slot.get_mut(&cid).unwrap();
        let cb = task_ctx.cb.take().unwrap();

        let ctx = LockWaitContext::new(
            lock_info.key.clone(),
            self.inner.lock_wait_queues.clone(),
            lock_wait_token,
            cb.unwrap_normal_request_callback(),
            lock_info.parameters.allow_lock_with_conflict,
        );
        let first_batch_cb = ctx.get_callback_for_first_write_batch();
        task_ctx.cb = Some(SchedulerTaskCallback::NormalRequestCallback(first_batch_cb));
        drop(slot);

        assert!(lock_info.req_states.is_none());

        let lock_wait_entry = Box::new(LockWaitEntry {
            key: lock_info.key,
            lock_hash: lock_info.lock_digest.hash,
            parameters: lock_info.parameters,
            should_not_exist: lock_info.should_not_exist,
            lock_wait_token,
            req_states: ctx.get_shared_states().clone(),
            legacy_wake_up_index: None,
            key_cb: Some(ctx.get_callback_for_blocked_key().into()),
        });

        (ctx, lock_wait_entry, lock_info.lock_info_pb)
    }

    fn make_lock_waiting_after_resuming(
        &self,
        lock_info: WriteResultLockInfo,
        cb: PessimisticLockKeyCallback,
    ) -> Box<LockWaitEntry> {
        Box::new(LockWaitEntry {
            key: lock_info.key,
            lock_hash: lock_info.lock_digest.hash,
            parameters: lock_info.parameters,
            should_not_exist: lock_info.should_not_exist,
            lock_wait_token: lock_info.lock_wait_token,
            // This must be called after an execution fo AcquirePessimisticLockResumed, in which
            // case there must be a valid req_state.
            req_states: lock_info.req_states.unwrap(),
            legacy_wake_up_index: None,
            key_cb: Some(cb.into()),
        })
    }

    fn on_wait_for_lock_after_resuming(
        &self,
        cid: u64,
        pr: &mut ProcessResult,
        lock_info: Vec<WriteResultLockInfo>,
    ) {
        if lock_info.is_empty() {
            return;
        }

        // TODO: Update lock wait relationship.

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

        let finished_len = results.len() - lock_info.len();

        let original_results = std::mem::replace(results, Vec::with_capacity(finished_len));
        let original_cbs = std::mem::replace(cbs, Vec::with_capacity(finished_len));
        let mut lock_wait_entries = SmallVec::<[_; 10]>::with_capacity(lock_info.len());
        let mut lock_info_it = lock_info.into_iter();

        for (result, cb) in original_results.into_iter().zip(original_cbs) {
            if let PessimisticLockKeyResult::Waiting = &result {
                let lock_info = lock_info_it.next().unwrap();
                let lock_info_pb = lock_info.lock_info_pb.clone();
                let entry = self.make_lock_waiting_after_resuming(lock_info, cb);
                lock_wait_entries.push((entry, lock_info_pb));
            } else {
                results.push(result);
                cbs.push(cb);
            }
        }

        assert!(lock_info_it.next().is_none());
        assert_eq!(results.len(), cbs.len());

        // Release the mutex in the latch slot.
        drop(slot);

        // Add to the lock waiting queue.
        // TODO: the request may be canceled from lock manager at this time. If so, it
        // should not be added to the queue.
        for (entry, lock_info_pb) in lock_wait_entries {
            self.inner
                .lock_wait_queues
                .push_lock_wait(entry, lock_info_pb);
        }
    }

    fn put_back_lock_wait_entries(&self, entries: impl IntoIterator<Item = Box<LockWaitEntry>>) {
        for entry in entries.into_iter() {
            // TODO: Do not pass `default` as the lock info. Here we need another method
            // `put_back_lock_wait`, which doesn't require updating lock info and
            // additionally checks if the lock wait entry is already canceled.
            self.inner
                .lock_wait_queues
                .push_lock_wait(entry, Default::default());
        }
    }
}

pub async fn get_raw_ext(
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
    concurrency_manager: ConcurrencyManager,
    max_ts_synced: bool,
    cmd: &Command,
) -> Result<Option<RawExt>, Error> {
    if causal_ts_provider.is_some() {
        match cmd {
            Command::RawCompareAndSwap(_) | Command::RawAtomicStore(_) => {
                if !max_ts_synced {
                    return Err(ErrorInner::RawKvMaxTimestampNotSynced {
                        region_id: cmd.ctx().get_region_id(),
                    }
                    .into());
                }
                let key_guard = get_raw_key_guard(&causal_ts_provider, concurrency_manager)
                    .await
                    .map_err(|err: StorageError| {
                        ErrorInner::Other(box_err!("failed to key guard: {:?}", err))
                    })?;
                let ts =
                    get_causal_ts(&causal_ts_provider)
                        .await
                        .map_err(|err: StorageError| {
                            ErrorInner::Other(box_err!("failed to get casual ts: {:?}", err))
                        })?;
                return Ok(Some(RawExt {
                    ts: ts.unwrap(),
                    key_guard: key_guard.unwrap(),
                }));
            }
            _ => {}
        }
    }
    Ok(None)
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

#[derive(Debug)]
struct SchedulerDetails {
    tracker: TrackerToken,
    stat: Statistics,
    start_process_instant: Instant,
    quota_limit_delay_nanos: u64,
    flow_control_nanos: u64,
}

impl SchedulerDetails {
    fn new(tracker: TrackerToken, start_process_instant: Instant) -> Self {
        SchedulerDetails {
            tracker,
            stat: Default::default(),
            start_process_instant,
            quota_limit_delay_nanos: 0,
            flow_control_nanos: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use futures_executor::block_on;
    use kvproto::kvrpcpb::{BatchRollbackRequest, CheckTxnStatusRequest, Context};
    use raftstore::store::{ReadStats, WriteStats};
    use tikv_util::{config::ReadableSize, future::paired_future_callback};
    use txn_types::{Key, TimeStamp};

    use super::*;
    use crate::storage::{
        kv::{Error as KvError, ErrorInner as KvErrorInner},
        lock_manager::{MockLockManager, WaitTimeout},
        mvcc::{self, Mutation},
        test_util::latest_feature_gate,
        txn::{
            commands,
            commands::TypedCommand,
            flow_controller::{EngineFlowController, FlowController},
            latch::*,
        },
        RocksEngine, TestEngineBuilder, TxnStatus,
    };

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    // TODO(cosven): use this in the following test cases to reduce duplicate code.
    fn new_test_scheduler() -> (TxnScheduler<RocksEngine, MockLockManager>, RocksEngine) {
        let engine = TestEngineBuilder::new().build().unwrap();
        let config = Config {
            scheduler_concurrency: 1024,
            scheduler_worker_pool_size: 1,
            scheduler_pending_write_threshold: ReadableSize(100 * 1024 * 1024),
            enable_async_apply_prewrite: false,
            ..Default::default()
        };
        (
            TxnScheduler::new(
                engine.clone(),
                MockLockManager::new(),
                ConcurrencyManager::new(1.into()),
                &config,
                DynamicConfigs {
                    pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
                    in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                    wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
                },
                Arc::new(FlowController::Singleton(EngineFlowController::empty())),
                None,
                DummyReporter,
                ResourceTagFactory::new_for_test(),
                Arc::new(QuotaLimiter::default()),
                latest_feature_gate(),
                Some(Arc::new(ResourceController::new("test".to_owned(), true))),
            ),
            engine,
        )
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
            commands::AcquirePessimisticLock::new(
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
            let unlocked = latches.release(&lock, id, None);
            if id == max_id {
                assert!(unlocked.is_empty());
            } else {
                assert_eq!(unlocked, vec![id + 1]);
            }
        }
    }

    #[test]
    fn test_acquire_latch_deadline() {
        let (scheduler, _) = new_test_scheduler();

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
        scheduler.release_latches(lock, cid, None);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        block_on(f).unwrap().unwrap();
    }

    /// When all latches are acquired, the command should be executed directly.
    /// When any latch is not acquired, the command should be prechecked.
    #[test]
    fn test_schedule_command_with_fail_fast_mode() {
        let (scheduler, engine) = new_test_scheduler();

        // req can acquire all latches, so it should be executed directly.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 10000;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        // It must be executed (and succeed).
        block_on(f).unwrap().unwrap();

        // Acquire the latch, so that next command(req2) can't require all latches.
        let mut lock = Lock::new(&[Key::from_raw(b"d")]);
        let cid = scheduler.inner.gen_id();
        assert!(scheduler.inner.latches.acquire(&mut lock, cid));

        engine.trigger_not_leader();

        // req2 can't acquire all latches, req2 will be prechecked.
        let mut req2 = BatchRollbackRequest::default();
        req2.mut_context().max_execution_duration_ms = 10000;
        req2.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec()].into());
        let cmd2: TypedCommand<()> = req2.into();
        let (cb2, f2) = paired_future_callback();
        scheduler.run_cmd(cmd2.cmd, StorageCallback::Boolean(cb2));

        // Precheck should return NotLeader error.
        assert!(matches!(
            block_on(f2).unwrap(),
            Err(StorageError(box StorageErrorInner::Kv(KvError(
                box KvErrorInner::Request(ref e),
            )))) if e.has_not_leader(),
        ));
        // The task context should be owned, and it's cb should be taken.
        let cid2 = cid + 1; // Hack: get the cid of req2.
        let mut task_slot = scheduler.inner.get_task_slot(cid2);
        let tctx = task_slot.get_mut(&cid2).unwrap();
        assert!(!tctx.try_own());
        assert!(tctx.cb.is_none());
    }

    #[test]
    fn test_pool_available_deadline() {
        let (scheduler, _) = new_test_scheduler();

        // Spawn a task that sleeps for 500ms to occupy the pool. The next request
        // cannot run within 500ms.
        scheduler
            .get_sched_pool()
            .spawn("", CommandPri::Normal, async {
                thread::sleep(Duration::from_millis(500))
            })
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
        let (scheduler, _) = new_test_scheduler();

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
        let (scheduler, _) = new_test_scheduler();

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
        scheduler.release_latches(lock, cid, None);

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

        let scheduler = TxnScheduler::new(
            engine,
            MockLockManager::new(),
            ConcurrencyManager::new(1.into()),
            &config,
            DynamicConfigs {
                pipelined_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                in_memory_pessimistic_lock: Arc::new(AtomicBool::new(false)),
                wake_up_delay_duration_ms: Arc::new(AtomicU64::new(0)),
            },
            Arc::new(FlowController::Singleton(EngineFlowController::empty())),
            None,
            DummyReporter,
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            feature_gate.clone(),
            Some(Arc::new(ResourceController::new("test".to_owned(), true))),
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
