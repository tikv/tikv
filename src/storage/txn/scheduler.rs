// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath
//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients, executes them against
//! the MVCC layer storage engine.
//!
//! Logically, the data organization hierarchy from bottom to top is row -> region -> store ->
//! database. But each region is replicated onto N stores for reliability, the replicas form a Raft
//! group, one of which acts as the leader. When the client read or write a row, the command is
//! sent to the scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are delegated to a pool of
//! worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure serialized access
//! to the overlapping rows involved in concurrent commands. But note that scheduler only ensures
//! serialized access to the overlapping rows at command level, but a transaction may consist of
//! multiple commands, therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library, which is transparent
//! to the scheduler.

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
            self, with_tls_engine, Engine, ExtCallback, FlowStatsReporter, Result as EngineResult,
            SnapContext, Statistics,
        },
        lock_manager::{self, DiagnosticContext, LockManager, WaitTimeout},
        metrics::*,
        txn::{
            commands::{Command, ResponsePolicy, WriteContext, WriteResult, WriteResultLockInfo},
            flow_controller::FlowController,
            latch::{Latches, Lock},
            sched_pool::{tls_collect_query, tls_collect_scan_details, SchedPool},
            Error, ProcessResult,
        },
        types::StorageCallback,
        DynamicConfigs, Error as StorageError, ErrorInner as StorageErrorInner,
    },
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

// The default limit is set to be very large. Then, requests without `max_exectuion_duration`
// will not be aborted unexpectedly.
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
    cb: Option<StorageCallback>,
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
    fn new(task: Task, cb: StorageCallback) -> TaskContext {
        let tag = task.cmd.tag();
        let lock = task.cmd.gen_lock();
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
            latch_timer: Instant::now_coarse(),
            _cmd_timer: CmdTimer {
                tag,
                begin: Instant::now_coarse(),
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

    fn new_task_context(&self, task: Task, callback: StorageCallback) -> TaskContext {
        let tctx = TaskContext::new(task, callback);
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

    fn take_task_cb_and_pr(&self, cid: u64) -> (Option<StorageCallback>, Option<ProcessResult>) {
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
    /// Returns a deadline error if the deadline is exceeded. Returns the `Task` if
    /// all latches are acquired, returns `None` otherwise.
    fn acquire_lock_on_wakeup(&self, cid: u64) -> Result<Option<Task>, StorageError> {
        let mut task_slot = self.get_task_slot(cid);
        let tctx = task_slot.get_mut(&cid).unwrap();
        // Check deadline early during acquiring latches to avoid expired requests blocking
        // other requests.
        if let Err(e) = tctx.task.as_ref().unwrap().cmd.deadline().check() {
            // `acquire_lock_on_wakeup` is called when another command releases its locks and wakes up
            // command `cid`. This command inserted its lock before and now the lock is at the
            // front of the queue. The actual acquired count is one more than the `owned_count`
            // recorded in the lock, so we increase one to make `release` work.
            tctx.lock.owned_count += 1;
            return Err(e.into());
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
        Scheduler {
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
        self.schedule_command(cmd, callback);
    }

    /// Releases all the latches held by a command.
    fn release_lock(&self, lock: &Lock, cid: u64) {
        let wakeup_list = self.inner.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.try_to_wake_up(wcid);
        }
    }

    fn schedule_command(&self, cmd: Command, callback: StorageCallback) {
        let cid = self.inner.gen_id();
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
                .new_task_context(Task::new(cid, tracker, cmd), callback)
        });
        let deadline = tctx.task.as_ref().unwrap().cmd.deadline();
        if self.inner.latches.acquire(&mut tctx.lock, cid) {
            fail_point!("txn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            drop(task_slot);
            self.execute(task);
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

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for further processing.
    fn try_to_wake_up(&self, cid: u64) {
        match self.inner.acquire_lock_on_wakeup(cid) {
            Ok(Some(task)) => {
                fail_point!("txn_scheduler_try_to_wake_up");
                self.execute(task);
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

    // pub for test
    pub fn get_sched_pool(&self, priority: CommandPri) -> &SchedPool {
        if priority == CommandPri::High {
            &self.inner.high_priority_pool
        } else {
            &self.inner.worker_pool
        }
    }

    /// Executes the task in the sched pool.
    fn execute(&self, mut task: Task) {
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

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: CommandKind) {
        SCHED_STAGE_COUNTER_VEC.get(tag).read_finish.inc();

        debug!("read command finished"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
            self.schedule_command(cmd, tctx.cb.unwrap());
        } else {
            tctx.cb.unwrap().execute(pr);
        }

        self.release_lock(&tctx.lock, cid);
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
        tag: CommandKind,
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

        // If pipelined pessimistic lock or async apply prewrite takes effect, it's not guaranteed
        // that the proposed or committed callback is surely invoked, which takes and invokes
        // `tctx.cb(tctx.pr)`.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr.or(tctx.pr).unwrap(),
                Err(e) => ProcessResult::Failed {
                    err: StorageError::from(e),
                },
            };
            if let ProcessResult::NextCommand { cmd } = pr {
                SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
                self.schedule_command(cmd, cb);
            } else {
                cb.execute(pr);
            }
        } else {
            assert!(pipelined || async_apply_prewrite);
        }

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the request of waiting for lock
    fn on_wait_for_lock(
        &self,
        cid: u64,
        start_ts: TimeStamp,
        pr: ProcessResult,
        lock: lock_manager::Lock,
        is_first_lock: bool,
        wait_timeout: Option<WaitTimeout>,
        diag_ctx: DiagnosticContext,
    ) {
        debug!("command waits for lock released"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).lock_wait.inc();
        self.inner.lock_mgr.wait_for(
            start_ts,
            tctx.cb.unwrap(),
            pr,
            lock,
            is_first_lock,
            wait_timeout,
            diag_ctx,
        );
        self.release_lock(&tctx.lock, cid);
    }

    fn early_response(
        cid: u64,
        cb: StorageCallback,
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

            let timer = Instant::now_coarse();

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

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
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

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    async fn process_write(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_write");
        let write_bytes = task.cmd.write_bytes();
        let tag = task.cmd.tag();
        let cid = task.cid;
        let priority = task.cmd.priority();
        let ts = task.cmd.ts();
        let scheduler = self.clone();
        let quota_limiter = self.inner.quota_limiter.clone();
        let mut sample = quota_limiter.new_sample();
        let pessimistic_lock_mode = self.pessimistic_lock_mode();
        let pipelined =
            task.cmd.can_be_pipelined() && pessimistic_lock_mode == PessimisticLockMode::Pipelined;
        let txn_ext = snapshot.ext().get_txn_ext().cloned();

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
            // TODO: write bytes can be a bit inaccurate due to error requests or in-memory pessimistic locks.
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
            lock_info,
            lock_guards,
            response_policy,
        } = match deadline
            .check()
            .map_err(StorageError::from)
            .and(write_result)
        {
            // Write prepare failure typically means conflicting transactions are detected. Delivers the
            // error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();
                debug!("write command failed"; "cid" => cid, "err" => ?err);
                scheduler.finish_with_err(cid, err);
                return;
            }
            // Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
            // message when it finishes.
            Ok(res) => res,
        };
        let region_id = ctx.get_region_id();
        SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

        if let Some(lock_info) = lock_info {
            let WriteResultLockInfo {
                lock,
                key,
                is_first_lock,
                wait_timeout,
            } = lock_info;
            let diag_ctx = DiagnosticContext {
                key,
                resource_group_tag: ctx.get_resource_group_tag().into(),
            };
            scheduler.on_wait_for_lock(cid, ts, pr, lock, is_first_lock, wait_timeout, diag_ctx);
            return;
        }

        let mut pr = Some(pr);
        if to_be_write.modifies.is_empty() {
            scheduler.on_write_finished(cid, pr, Ok(()), lock_guards, false, false, tag);
            return;
        }

        if tag == CommandKind::acquire_pessimistic_lock
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
            scheduler.on_write_finished(cid, pr, Ok(()), lock_guards, false, false, tag);
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
                    if pipelined {
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
                // Control mutex is used to ensure there is only one request consuming the quota.
                // The delay may exceed 1s, and the speed limit is changed every second.
                // If the speed of next second is larger than the one of first second,
                // without the mutex, the write flow can't throttled strictly.
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
        // We only set a deleted flag here, and the lock will be finally removed when it finishes
        // applying. See the comments in `PeerPessimisticLocks` for how this flag is used.
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
        // Keep the read lock guard of the pessimistic lock table until the request is sent to the raftstore.
        //
        // If some in-memory pessimistic locks need to be proposed, we will propose another TransferLeader
        // command. Then, we can guarentee even if the proposed locks don't include the locks deleted here,
        // the response message of the transfer leader command must be later than this write command because
        // this write command has been sent to the raftstore. Then, we don't need to worry this request will
        // fail due to the voluntary leader transfer.
        let _downgraded_guard = pessimistic_locks_guard.and_then(|guard| {
            (!removed_pessimistic_locks.is_empty()).then(|| RwLockWriteGuard::downgrade(guard))
        });

        // The callback to receive async results of write prepare from the storage engine.
        let engine_cb = Box::new(move |result: EngineResult<()>| {
            let ok = result.is_ok();
            if ok && !removed_pessimistic_locks.is_empty() {
                // Removing pessimistic locks when it succeeds to apply. This should be done in the apply
                // thread, to make sure it happens before other admin commands are executed.
                if let Some(mut pessimistic_locks) = txn_ext
                    .as_ref()
                    .map(|txn_ext| txn_ext.pessimistic_locks.write())
                {
                    // If epoch version or term does not match, region or leader change has happened,
                    // so we needn't remove the key.
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
                        pipelined,
                        is_async_apply_prewrite,
                        tag,
                    );
                    KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                        .get(tag)
                        .observe(rows as f64);

                    if !ok {
                        // Only consume the quota when write succeeds, otherwise failed write requests may exhaust
                        // the quota and other write requests would be in long delay.
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

    /// Returns whether it succeeds to write pessimistic locks to the in-memory lock table.
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
        // When not writable, it only means we cannot write locks to the in-memory lock table,
        // but it is still possible for the region to propose request.
        // When term or epoch version has changed, the request must fail. To be simple, here we just
        // let the request fallback to propose and let raftstore generate an appropriate error.
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
    use txn_types::{Key, OldValues};

    use super::*;
    use crate::storage::{
        lock_manager::DummyLockManager,
        mvcc::{self, Mutation},
        test_util::latest_feature_gate,
        txn::{
            commands,
            commands::TypedCommand,
            flow_controller::{EngineFlowController, FlowController},
            latch::*,
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
                OldValues::default(),
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
            let unlocked = latches.release(&lock, id);
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
        scheduler.release_lock(&lock, cid);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.mut_context().max_execution_duration_ms = 100;
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(block_on(f).unwrap().is_ok());
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
        assert!(block_on(f).unwrap().is_ok());
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
        assert!(block_on(f).unwrap().is_ok());
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

        // When releasing the lock, the queuing tasks should be all waken up without stack overflow.
        scheduler.release_lock(&lock, cid);

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(block_on(f).is_ok());
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
