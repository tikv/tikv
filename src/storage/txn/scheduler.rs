// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

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

use crossbeam::utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::u64;

use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use kvproto::kvrpcpb::{CommandPri, ExtraOp};
use tikv_util::{callback::must_call, deadline::Deadline, time::Instant};
use txn_types::TimeStamp;

use crate::server::lock_manager::waiter_manager;
use crate::storage::kv::{
    drop_snapshot_callback, with_tls_engine, Engine, ExtCallback, Result as EngineResult,
    SnapContext, Statistics,
};
use crate::storage::lock_manager::{self, DiagnosticContext, LockManager, WaitTimeout};
use crate::storage::metrics::{
    self, KV_COMMAND_KEYWRITE_HISTOGRAM_VEC, SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC,
    SCHED_CONTEX_GAUGE, SCHED_HISTOGRAM_VEC_STATIC, SCHED_LATCH_HISTOGRAM_VEC,
    SCHED_STAGE_COUNTER_VEC, SCHED_TOO_BUSY_COUNTER_VEC, SCHED_WRITING_BYTES_GAUGE,
};
use crate::storage::txn::commands::{
    ResponsePolicy, WriteContext, WriteResult, WriteResultLockInfo,
};
use crate::storage::txn::{
    commands::Command,
    latch::{Latches, Lock},
    sched_pool::{tls_collect_read_duration, tls_collect_scan_details, SchedPool},
    Error, ProcessResult,
};
use crate::storage::{
    get_priority_tag, types::StorageCallback, Error as StorageError,
    ErrorInner as StorageErrorInner,
};
use req_cpu::RequestTags;

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

// The default limit is set to be very large. Then, requests without `max_exectuion_duration`
// will not be aborted unexpectedly.
const DEFAULT_EXECUTION_DURATION_LIMIT: Duration = Duration::from_secs(24 * 60 * 60);

/// Task is a running command.
pub(super) struct Task {
    pub(super) cid: u64,
    pub(super) cmd: Command,
    pub(super) extra_op: ExtraOp,
    pub(super) deadline: Deadline,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn new(cid: u64, cmd: Command) -> Task {
        let max_execution_duration_ms = cmd.ctx().max_execution_duration_ms;
        let execution_duration_limit = if max_execution_duration_ms == 0 {
            DEFAULT_EXECUTION_DURATION_LIMIT
        } else {
            Duration::from_millis(max_execution_duration_ms)
        };
        let deadline = Deadline::from_now(execution_duration_limit);
        Task {
            cid,
            cmd,
            extra_op: ExtraOp::Noop,
            deadline,
        }
    }
}

struct CmdTimer {
    tag: metrics::CommandKind,
    begin: Instant,
}

impl Drop for CmdTimer {
    fn drop(&mut self) {
        SCHED_HISTOGRAM_VEC_STATIC
            .get(self.tag)
            .observe(self.begin.elapsed_secs());
    }
}

// It stores context of a task.
struct TaskContext {
    task: Option<Task>,

    lock: Lock,
    cb: Option<StorageCallback>,
    pr: Option<ProcessResult>,
    write_bytes: usize,
    tag: metrics::CommandKind,
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
            .observe(self.latch_timer.elapsed_secs());
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

    lock_mgr: L,

    concurrency_manager: ConcurrencyManager,

    pipelined_pessimistic_lock: Arc<AtomicBool>,

    enable_async_apply_prewrite: bool,
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
    fn get_task_slot(&self, cid: u64) -> MutexGuard<HashMap<u64, TaskContext>> {
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

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire) >= self.sched_pending_write_threshold
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
        if let Err(e) = tctx.task.as_ref().unwrap().deadline.check() {
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
}

/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler<E: Engine, L: LockManager> {
    // `engine` is `None` means currently the program is in scheduler worker threads.
    engine: Option<E>,
    inner: Arc<SchedulerInner<L>>,
}

unsafe impl<E: Engine, L: LockManager> Send for Scheduler<E, L> {}

impl<E: Engine, L: LockManager> Scheduler<E, L> {
    /// Creates a scheduler.
    pub(in crate::storage) fn new(
        engine: E,
        lock_mgr: L,

        concurrency_manager: ConcurrencyManager,
        concurrency: usize,
        worker_pool_size: usize,
        sched_pending_write_threshold: usize,
        pipelined_pessimistic_lock: Arc<AtomicBool>,
        enable_async_apply_prewrite: bool,
    ) -> Self {
        let t = Instant::now_coarse();
        let mut task_slots = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_slots.push(Mutex::new(Default::default()).into());
        }

        let inner = Arc::new(SchedulerInner {
            task_slots,
            id_alloc: AtomicU64::new(0).into(),
            latches: Latches::new(concurrency),
            running_write_bytes: AtomicUsize::new(0).into(),
            sched_pending_write_threshold,
            worker_pool: SchedPool::new(engine.clone(), worker_pool_size, "sched-worker-pool"),
            high_priority_pool: SchedPool::new(
                engine.clone(),
                std::cmp::max(1, worker_pool_size / 2),
                "sched-high-pri-pool",
            ),
            lock_mgr,
            concurrency_manager,
            pipelined_pessimistic_lock,
            enable_async_apply_prewrite,
        });

        slow_log!(t.elapsed(), "initialized the transaction scheduler");
        Scheduler {
            engine: Some(engine),
            inner,
        }
    }

    pub fn dump_wait_for_entries(&self, cb: waiter_manager::Callback) {
        self.inner.dump_wait_for_entries(cb);
    }

    pub(in crate::storage) fn run_cmd(&self, cmd: Command, callback: StorageCallback) {
        // write flow control
        if cmd.need_flow_control() && self.inner.too_busy() {
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
        debug!("received new command"; "cid" => cid, "cmd" => ?cmd);

        let tag = cmd.tag();
        let priority_tag = get_priority_tag(cmd.priority());
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();

        let mut task_slot = self.inner.get_task_slot(cid);
        let tctx = task_slot
            .entry(cid)
            .or_insert_with(|| self.inner.new_task_context(Task::new(cid, cmd), callback));
        if self.inner.latches.acquire(&mut tctx.lock, cid) {
            fail_point!("txn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            drop(task_slot);
            self.execute(task);
            return;
        }
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
                self.finish_with_err(cid, err);
            }
        }
    }

    fn get_sched_pool(&self, priority: CommandPri) -> &SchedPool {
        if priority == CommandPri::High {
            &self.inner.high_priority_pool
        } else {
            &self.inner.worker_pool
        }
    }

    /// Initiates an async operation to get a snapshot from the storage engine, then execute the
    /// task in the sched pool.
    fn execute(&self, mut task: Task) {
        let cid = task.cid;
        let tag = task.cmd.tag();
        let ctx = task.cmd.ctx().clone();
        let sched = self.clone();

        let cb = must_call(
            move |(cb_ctx, snapshot)| {
                debug!(
                    "receive snapshot finish msg";
                    "cid" => task.cid, "cb_ctx" => ?cb_ctx
                );

                match snapshot {
                    Ok(snapshot) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();

                        if let Some(term) = cb_ctx.term {
                            task.cmd.ctx_mut().set_term(term);
                        }
                        task.extra_op = cb_ctx.txn_extra_op;

                        debug!(
                            "process cmd with snapshot";
                            "cid" => task.cid, "cb_ctx" => ?cb_ctx
                        );
                        sched.process_by_worker(snapshot, task);
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();

                        info!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                        sched
                            .get_sched_pool(task.cmd.priority())
                            .clone()
                            .pool
                            .spawn(async move {
                                sched.finish_with_err(task.cid, Error::from(err));
                            })
                            .unwrap();
                    }
                }
            },
            drop_snapshot_callback::<E>,
        );

        let f = |engine: &E| {
            let snap_ctx = SnapContext {
                pb_ctx: &ctx,
                ..Default::default()
            };
            if let Err(e) = engine.async_snapshot(snap_ctx, cb) {
                SCHED_STAGE_COUNTER_VEC.get(tag).async_snapshot_err.inc();

                info!("engine async_snapshot failed"; "err" => ?e);
                self.finish_with_err(cid, e);
            } else {
                SCHED_STAGE_COUNTER_VEC.get(tag).snapshot.inc();
            }
        };

        if let Some(engine) = self.engine.as_ref() {
            f(engine)
        } else {
            // The program is currently in scheduler worker threads.
            // Safety: `self.inner.worker_pool` should ensure that a TLS engine exists.
            unsafe { with_tls_engine(f) }
        }
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
        tctx.cb.unwrap().execute(pr);

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: metrics::CommandKind) {
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
        tag: metrics::CommandKind,
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
        tag: metrics::CommandKind,
        stage: metrics::CommandStageKind,
    ) {
        debug!("early return response"; "cid" => cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).get(stage).inc();
        cb.execute(pr);
        // It won't release locks here until write finished.
    }

    /// Delivers a command to a worker thread for processing.
    fn process_by_worker(self, snapshot: E::Snap, task: Task) {
        if self.check_task_deadline_exceeded(&task) {
            return;
        }

        let tag = task.cmd.tag();
        self.get_sched_pool(task.cmd.priority())
            .clone()
            .pool
            .spawn(async move {
                fail_point!("scheduler_async_snapshot_finish");
                SCHED_STAGE_COUNTER_VEC.get(tag).process.inc();

                if self.check_task_deadline_exceeded(&task) {
                    return;
                }

                let timer = Instant::now_coarse();

                let req_tags = Arc::new(RequestTags::from_rpc_context(task.cmd.ctx()));
                let _g = req_tags.attach();

                let region_id = task.cmd.ctx().get_region_id();
                let ts = task.cmd.ts();
                let mut statistics = Statistics::default();

                if task.cmd.readonly() {
                    self.process_read(snapshot, task, &mut statistics);
                } else {
                    // Safety: `self.sched_pool` ensures a TLS engine exists.
                    unsafe {
                        with_tls_engine(|engine| {
                            self.process_write(engine, snapshot, task, &mut statistics)
                        });
                    }
                };
                tls_collect_scan_details(tag.get_str(), &statistics);
                let elapsed = timer.elapsed();
                slow_log!(
                    elapsed,
                    "[region {}] scheduler handle command: {}, ts: {}",
                    region_id,
                    tag,
                    ts
                );

                tls_collect_read_duration(tag.get_str(), elapsed);
            })
            .unwrap();
    }

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
    fn process_read(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);

        let tag = task.cmd.tag();

        let pr = task
            .cmd
            .process_read(snapshot, statistics)
            .unwrap_or_else(|e| ProcessResult::Failed { err: e.into() });
        self.on_read_finished(task.cid, pr, tag);
    }

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    fn process_write(self, engine: &E, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_write");
        let tag = task.cmd.tag();
        let cid = task.cid;
        let priority = task.cmd.priority();
        let ts = task.cmd.ts();
        let scheduler = self.clone();
        let pipelined_pessimistic_lock = self
            .inner
            .pipelined_pessimistic_lock
            .load(Ordering::Relaxed);
        let pipelined = pipelined_pessimistic_lock && task.cmd.can_be_pipelined();

        let context = WriteContext {
            lock_mgr: &self.inner.lock_mgr,
            concurrency_manager: self.inner.concurrency_manager.clone(),
            extra_op: task.extra_op,
            statistics,
            async_apply_prewrite: self.inner.enable_async_apply_prewrite,
        };

        let deadline = task.deadline;
        let write_result = task
            .cmd
            .process_write(snapshot, context)
            .map_err(StorageError::from);
        match deadline
            .check()
            .map_err(StorageError::from)
            .and(write_result)
        {
            // Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
            // message when it finishes.
            Ok(WriteResult {
                mut ctx,
                mut to_be_write,
                rows,
                pr,
                lock_info,
                lock_guards,
                response_policy,
            }) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

                if let Some(lock_info) = lock_info {
                    let WriteResultLockInfo {
                        lock,
                        key,
                        is_first_lock,
                        wait_timeout,
                    } = lock_info;
                    // Currently only pessimistic_lock request may wait for other locks, and a
                    // single request may wait lock at most once in its lifecycle. Take the tag out
                    // instead of cloning it.
                    let resource_group_tag = ctx.take_resource_group_tag();
                    let diag_ctx = DiagnosticContext {
                        key,
                        resource_group_tag,
                    };
                    scheduler.on_wait_for_lock(
                        cid,
                        ts,
                        pr,
                        lock,
                        is_first_lock,
                        wait_timeout,
                        diag_ctx,
                    );
                } else if to_be_write.modifies.is_empty() {
                    scheduler.on_write_finished(
                        cid,
                        Some(pr),
                        Ok(()),
                        lock_guards,
                        false,
                        false,
                        tag,
                    );
                } else {
                    let mut pr = Some(pr);
                    let mut is_async_apply_prewrite = false;

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
                                        metrics::CommandStageKind::async_apply_prewrite,
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
                                            metrics::CommandStageKind::pipelined_write,
                                        );
                                    });
                                    (Some(proposed_cb), None)
                                } else {
                                    (None, None)
                                }
                            }
                        };

                    let sched = scheduler.clone();
                    let sched_pool = scheduler.get_sched_pool(priority).pool.clone();
                    // The callback to receive async results of write prepare from the storage engine.
                    let engine_cb = Box::new(move |(_, result)| {
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
                            })
                            .unwrap()
                    });

                    to_be_write.deadline = Some(deadline);
                    if let Err(e) = engine.async_write_ext(
                        &ctx,
                        to_be_write,
                        engine_cb,
                        proposed_cb,
                        committed_cb,
                    ) {
                        SCHED_STAGE_COUNTER_VEC.get(tag).async_write_err.inc();

                        info!("engine async_write failed"; "cid" => cid, "err" => ?e);
                        scheduler.finish_with_err(cid, e);
                    }
                }
            }
            // Write prepare failure typically means conflicting transactions are detected. Delivers the
            // error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();
                debug!("write command failed at prewrite"; "cid" => cid, "err" => ?err);
                scheduler.finish_with_err(cid, err);
            }
        }
    }

    /// If the task has expired, return `true` and call the callback of
    /// the task with a `DeadlineExceeded` error.
    #[inline]
    fn check_task_deadline_exceeded(&self, task: &Task) -> bool {
        if let Err(e) = task.deadline.check() {
            self.finish_with_err(task.cid, e);
            true
        } else {
            false
        }
    }
}

impl<E: Engine, L: LockManager> Clone for Scheduler<E, L> {
    fn clone(&self) -> Self {
        Scheduler {
            engine: self.engine.clone(),
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::storage::{
        lock_manager::DummyLockManager,
        mvcc::{self, Mutation},
        txn::commands::TypedCommand,
    };
    use crate::storage::{
        txn::{commands, latch::*},
        TestEngineBuilder,
    };
    use futures_executor::block_on;
    use kvproto::kvrpcpb::{BatchRollbackRequest, Context};
    use tikv_util::future::paired_future_callback;
    use txn_types::{Key, OldValues};

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
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
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
        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            1024,
            1,
            100 * 1024 * 1024,
            Arc::new(AtomicBool::new(true)),
            false,
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
        // time limit is 100ms.
        thread::sleep(Duration::from_millis(200));
        scheduler.release_lock(&lock, cid);
        assert!(matches!(
            block_on(f).unwrap(),
            Err(StorageError(box StorageErrorInner::DeadlineExceeded))
        ));

        // A new request should not be blocked.
        let mut req = BatchRollbackRequest::default();
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(block_on(f).is_ok());
    }

    #[test]
    fn test_pool_available_deadline() {
        let engine = TestEngineBuilder::new().build().unwrap();
        let scheduler = Scheduler::new(
            engine,
            DummyLockManager,
            ConcurrencyManager::new(1.into()),
            1024,
            1,
            100 * 1024 * 1024,
            Arc::new(AtomicBool::new(true)),
            false,
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
        req.set_keys(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        let cmd: TypedCommand<()> = req.into();
        let (cb, f) = paired_future_callback();
        scheduler.run_cmd(cmd.cmd, StorageCallback::Boolean(cb));
        assert!(block_on(f).is_ok());
    }
}
