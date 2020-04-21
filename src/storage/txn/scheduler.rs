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

use parking_lot::Mutex;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::u64;

use kvproto::kvrpcpb::CommandPri;
use tikv_util::{collections::HashMap, time::Instant};
use txn_types::TimeStamp;

use crate::storage::kv::{with_tls_engine, Engine, Result as EngineResult};
use crate::storage::lock_manager::{self, LockManager, WaitTimeout};
use crate::storage::metrics::{
    self, SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC, SCHED_CONTEX_GAUGE, SCHED_HISTOGRAM_VEC_STATIC,
    SCHED_LATCH_HISTOGRAM_VEC, SCHED_STAGE_COUNTER_VEC, SCHED_TOO_BUSY_COUNTER_VEC,
    SCHED_WRITING_BYTES_GAUGE,
};
use crate::storage::txn::{
    commands::Command,
    latch::{Latches, Lock},
    process::{Executor, MsgScheduler, Task},
    sched_pool::SchedPool,
    Error, ProcessResult,
};
use crate::storage::{
    get_priority_tag, types::StorageCallback, Error as StorageError,
    ErrorInner as StorageErrorInner,
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

/// Message types for the scheduler event loop.
pub enum Msg {
    RawCmd {
        cmd: Command,
        cb: StorageCallback,
    },
    ReadFinished {
        cid: u64,
        pr: ProcessResult,
        tag: metrics::CommandKind,
    },
    WriteFinished {
        cid: u64,
        pr: ProcessResult,
        result: EngineResult<()>,
        pipelined: bool,
        tag: metrics::CommandKind,
    },
    FinishedWithErr {
        cid: u64,
        err: Error,
        tag: metrics::CommandKind,
    },
    WaitForLock {
        cid: u64,
        start_ts: TimeStamp,
        pr: ProcessResult,
        lock: lock_manager::Lock,
        is_first_lock: bool,
        wait_timeout: Option<WaitTimeout>,
    },
    PipelinedWrite {
        cid: u64,
        pr: ProcessResult,
        tag: metrics::CommandKind,
    },
}

/// Debug for messages.
impl Debug for Msg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Display for messages.
impl Display for Msg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {}", cmd),
            Msg::ReadFinished { cid, .. } => write!(f, "ReadFinished [cid={}]", cid),
            Msg::WriteFinished { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
            Msg::FinishedWithErr { cid, .. } => write!(f, "FinishedWithErr [cid={}]", cid),
            Msg::WaitForLock { cid, .. } => write!(f, "WaitForLock [cid={}]", cid),
            Msg::PipelinedWrite { cid, .. } => write!(f, "PipelinedWrite [cid={}]", cid),
        }
    }
}

// It stores context of a task.
struct TaskContext {
    task: Option<Task>,

    lock: Lock,
    cb: Option<StorageCallback>,
    write_bytes: usize,
    tag: metrics::CommandKind,
    // How long it waits on latches.
    // latch_timer: Option<Instant>,
    latch_timer: Instant,
    // Total duration of a command.
    _cmd_timer: CmdTimer,
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

impl TaskContext {
    fn new(task: Task, latches: &Latches, cb: StorageCallback) -> TaskContext {
        let tag = task.cmd().tag();
        let lock = task.cmd().gen_lock(latches);
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
    task_contexts: Vec<Mutex<HashMap<u64, TaskContext>>>,

    // cmd id generator
    id_alloc: AtomicU64,

    // write concurrency control
    latches: Latches,

    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: SchedPool,

    // high priority commands and system commands will be delivered to this pool
    high_priority_pool: SchedPool,

    // used to control write flow
    running_write_bytes: AtomicUsize,

    lock_mgr: Option<L>,

    pipelined_pessimistic_lock: bool,
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

    fn dequeue_task(&self, cid: u64) -> Task {
        let mut tasks = self.task_contexts[id_index(cid)].lock();
        let task = tasks.get_mut(&cid).unwrap().task.take().unwrap();
        assert_eq!(task.cid, cid);
        task
    }

    fn enqueue_task(&self, task: Task, callback: StorageCallback) {
        let cid = task.cid;
        let tctx = TaskContext::new(task, &self.latches, callback);

        let running_write_bytes = self
            .running_write_bytes
            .fetch_add(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes + tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.inc();

        let mut tasks = self.task_contexts[id_index(cid)].lock();
        if tasks.insert(cid, tctx).is_some() {
            panic!("TaskContext cid={} shouldn't exist", cid);
        }
    }

    fn dequeue_task_context(&self, cid: u64) -> TaskContext {
        let tctx = self.task_contexts[id_index(cid)]
            .lock()
            .remove(&cid)
            .unwrap();

        let running_write_bytes = self
            .running_write_bytes
            .fetch_sub(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes - tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.dec();

        tctx
    }

    fn take_task_cb(&self, cid: u64) -> Option<StorageCallback> {
        self.task_contexts[id_index(cid)]
            .lock()
            .get_mut(&cid)
            .map(|tctx| tctx.cb.take().unwrap())
    }

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire) >= self.sched_pending_write_threshold
    }

    /// Tries to acquire all the required latches for a command.
    ///
    /// Returns `true` if successful; returns `false` otherwise.
    fn acquire_lock(&self, cid: u64) -> bool {
        let mut task_contexts = self.task_contexts[id_index(cid)].lock();
        let tctx = task_contexts.get_mut(&cid).unwrap();
        if self.latches.acquire(&mut tctx.lock, cid) {
            tctx.on_schedule();
            return true;
        }
        false
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct Scheduler<E: Engine, L: LockManager> {
    // `engine` is `None` means currently the program is in scheduler worker threads.
    engine: Option<E>,
    inner: Arc<SchedulerInner<L>>,
}

unsafe impl<E: Engine, L: LockManager> Send for Scheduler<E, L> {}

impl<E: Engine, L: LockManager> Scheduler<E, L> {
    /// Creates a scheduler.
    pub fn new(
        engine: E,
        lock_mgr: Option<L>,
        concurrency: usize,
        worker_pool_size: usize,
        sched_pending_write_threshold: usize,
        pipelined_pessimistic_lock: bool,
    ) -> Self {
        // Add 2 logs records how long is need to initialize TASKS_SLOTS_NUM * 2048000 `Mutex`es.
        // In a 3.5G Hz machine it needs 1.3s, which is a notable duration during start-up.
        let t = Instant::now_coarse();
        let mut task_contexts = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_contexts.push(Mutex::new(Default::default()));
        }

        let inner = Arc::new(SchedulerInner {
            task_contexts,
            id_alloc: AtomicU64::new(0),
            latches: Latches::new(concurrency),
            running_write_bytes: AtomicUsize::new(0),
            sched_pending_write_threshold,
            worker_pool: SchedPool::new(engine.clone(), worker_pool_size, "sched-worker-pool"),
            high_priority_pool: SchedPool::new(
                engine.clone(),
                std::cmp::max(1, worker_pool_size / 2),
                "sched-high-pri-pool",
            ),
            lock_mgr,
            pipelined_pessimistic_lock,
        });

        slow_log!(t.elapsed(), "initialized the transaction scheduler");
        Scheduler {
            engine: Some(engine),
            inner,
        }
    }

    pub fn run_cmd(&self, cmd: Command, callback: StorageCallback) {
        self.on_receive_new_cmd(cmd, callback);
    }
}

impl<E: Engine, L: LockManager> Scheduler<E, L> {
    fn fetch_executor(&self, priority: CommandPri, is_sys_cmd: bool) -> Executor<E, Self, L> {
        let pool = if priority == CommandPri::High || is_sys_cmd {
            self.inner.high_priority_pool.clone()
        } else {
            self.inner.worker_pool.clone()
        };
        let scheduler = Scheduler {
            engine: None,
            inner: Arc::clone(&self.inner),
        };
        Executor::new(
            scheduler,
            pool,
            self.inner.lock_mgr.clone(),
            self.inner.pipelined_pessimistic_lock,
        )
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
        let task = Task::new(cid, cmd);
        // TODO: enqueue_task should return an reference of the tctx.
        self.inner.enqueue_task(task, callback);
        self.try_to_wake_up(cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for further processing.
    fn try_to_wake_up(&self, cid: u64) {
        if self.inner.acquire_lock(cid) {
            self.get_snapshot(cid);
        }
    }

    fn on_receive_new_cmd(&self, cmd: Command, callback: StorageCallback) {
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

    /// Initiates an async operation to get a snapshot from the storage engine, then posts a
    /// `SnapshotFinished` message back to the event loop when it finishes.
    fn get_snapshot(&self, cid: u64) {
        let task = self.inner.dequeue_task(cid);
        let tag = task.tag;
        let ctx = task.context().clone();
        let executor = self.fetch_executor(task.priority(), task.cmd().is_sys_cmd());

        let cb = Box::new(move |(cb_ctx, snapshot)| {
            executor.execute(cb_ctx, snapshot, task);
        });

        let f = |engine: &E| {
            if let Err(e) = engine.async_snapshot(&ctx, cb) {
                SCHED_STAGE_COUNTER_VEC.get(tag).async_snapshot_err.inc();

                info!("engine async_snapshot failed"; "err" => ?e);
                self.finish_with_err(cid, e.into());
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
    fn finish_with_err(&self, cid: u64, err: Error) {
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
        pr: ProcessResult,
        result: EngineResult<()>,
        pipelined: bool,
        tag: metrics::CommandKind,
    ) {
        if !pipelined {
            SCHED_STAGE_COUNTER_VEC.get(tag).write_finish.inc();
        } else {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .pipelined_write_finish
                .inc();
        }

        debug!("write command finished"; "cid" => cid, "pipelined" => pipelined);
        let tctx = self.inner.dequeue_task_context(cid);

        // It's possible we receive a Msg::WriteFinished before Msg::PipelinedWrite.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr,
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
            assert!(pipelined);
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
    ) {
        debug!("command waits for lock released"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).lock_wait.inc();
        self.inner.lock_mgr.as_ref().unwrap().wait_for(
            start_ts,
            tctx.cb.unwrap(),
            pr,
            lock,
            is_first_lock,
            wait_timeout,
        );
        self.release_lock(&tctx.lock, cid);
    }

    fn on_pipelined_write(&self, cid: u64, pr: ProcessResult, tag: metrics::CommandKind) {
        debug!("pipelined write"; "cid" => cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).pipelined_write.inc();
        // It's possible we receive a Msg::WriteFinished before Msg::PipelinedWrite.
        // The task ctx has been dequeued.
        if let Some(cb) = self.inner.take_task_cb(cid) {
            cb.execute(pr);
        }
        // It won't release locks here until write finished.
    }
}

impl<E: Engine, L: LockManager> MsgScheduler for Scheduler<E, L> {
    fn on_msg(&self, task: Msg) {
        match task {
            Msg::ReadFinished { cid, tag, pr } => self.on_read_finished(cid, pr, tag),
            Msg::WriteFinished {
                cid,
                tag,
                pr,
                pipelined,
                result,
            } => self.on_write_finished(cid, pr, result, pipelined, tag),
            Msg::FinishedWithErr { cid, err, .. } => self.finish_with_err(cid, err),
            Msg::WaitForLock {
                cid,
                start_ts,
                pr,
                lock,
                is_first_lock,
                wait_timeout,
            } => self.on_wait_for_lock(cid, start_ts, pr, lock, is_first_lock, wait_timeout),
            Msg::PipelinedWrite { cid, pr, tag } => self.on_pipelined_write(cid, pr, tag),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::{self, Mutation};
    use crate::storage::txn::{commands, latch::*};
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        let readonly_cmds: Vec<Command> = vec![
            commands::ScanLock::new(5.into(), None, 0, Context::default()).into(),
            commands::ResolveLock::new(temp_map.clone(), None, vec![], Context::default()).into(),
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
                let mut lock = cmd.gen_lock(&latches);
                assert_eq!(latches.acquire(&mut lock, id as u64), id == 0);
                lock
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut lock = cmd.gen_lock(&latches);
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
}
