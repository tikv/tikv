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

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::u64;

use kvproto::kvrpcpb::CommandPri;
use prometheus::HistogramTimer;
use tikv_util::collections::HashMap;
use tikv_util::threadpool::{self, ThreadPool, ThreadPoolBuilder};
use tikv_util::worker::ScheduleError;

use crate::storage::kv::Result as EngineResult;
use crate::storage::txn::process::{
    execute_callback, Executor, MsgScheduler, ProcessResult, SchedContext, SchedContextFactory,
    Task,
};
use crate::storage::txn::latch::{Latches, Lock};
use crate::storage::txn::Error;
use crate::storage::{metrics::*, Key};
use crate::storage::{Command, Engine, Error as StorageError, StorageCb};

const TASKS_SLOTS_NUM: usize = 1024;

/// Message types for the scheduler event loop.
pub enum Msg {
    Quit,
    RawCmd {
        cmd: Command,
        cb: StorageCb,
    },
    ReadFinished {
        cid: u64,
        pr: ProcessResult,
        tag: &'static str,
    },
    WriteFinished {
        cid: u64,
        pr: ProcessResult,
        result: EngineResult<()>,
        tag: &'static str,
    },
    FinishedWithErr {
        cid: u64,
        err: Error,
        tag: &'static str,
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
            Msg::Quit => write!(f, "Quit"),
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {:?}", cmd),
            Msg::ReadFinished { cid, .. } => write!(f, "ReadFinished [cid={}]", cid),
            Msg::WriteFinished { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
            Msg::FinishedWithErr { cid, .. } => write!(f, "FinishedWithErr [cid={}]", cid),
        }
    }
}

// It stores context of a task.
struct TaskContext {
    lock: Lock,
    cb: StorageCb,
    write_bytes: usize,
    tag: &'static str,
    // How long it waits on latches.
    latch_timer: Option<HistogramTimer>,
    // Total duration of a command.
    _cmd_timer: HistogramTimer,
}

impl TaskContext {
    fn new(lock: Lock, cb: StorageCb, cmd: &Command) -> TaskContext {
        let write_bytes = if lock.is_write_lock() {
            cmd.write_bytes()
        } else {
            0
        };

        TaskContext {
            lock,
            cb,
            write_bytes,
            tag: cmd.tag(),
            latch_timer: Some(
                SCHED_LATCH_HISTOGRAM_VEC
                    .with_label_values(&[cmd.tag()])
                    .start_coarse_timer(),
            ),
            _cmd_timer: SCHED_HISTOGRAM_VEC
                .with_label_values(&[cmd.tag()])
                .start_coarse_timer(),
        }
    }

    fn on_schedule(&mut self) {
        self.latch_timer.take();
    }
}

struct SchedulerInner<E: Engine> {
    // cid -> Task
    pending_tasks: Vec<Mutex<HashMap<u64, Task>>>,

    // cid -> TaskContext
    task_contexts: Vec<Mutex<HashMap<u64, TaskContext>>>,

    // cmd id generator
    id_alloc: AtomicU64,

    // write concurrency control
    latches: Latches,

    // TODO: Dynamically calculate this value according to processing
    // speed of recent write requests.
    sched_pending_write_threshold: AtomicUsize,

    // worker pool
    worker_scheduler: threadpool::Scheduler<SchedContext<E>>,

    // high priority commands will be delivered to this pool
    high_priority_scheduler: threadpool::Scheduler<SchedContext<E>>,

    // used to control write flow
    running_write_bytes: AtomicUsize,
}

#[inline]
fn id_index(cid: u64) -> usize {
    cid as usize % TASKS_SLOTS_NUM
}

impl<E: Engine> SchedulerInner<E> {
    /// Generates the next command ID.
    fn gen_id(&self) -> u64 {
        let id = self.id_alloc.fetch_add(1, Ordering::AcqRel);
        id + 1
    }

    fn dequeue_task(&self, cid: u64) -> Task {
        let task = self.pending_tasks[id_index(cid)]
            .lock()
            .unwrap()
            .remove(&cid)
            .unwrap();
        assert_eq!(task.cid, cid);
        task
    }

    fn enqueue_task(&self, task: Task, callback: StorageCb) {
        let cid = task.cid;

        let tctx = {
            let cmd = task.cmd();
            let lock = self.gen_lock(cmd);
            TaskContext::new(lock, callback, cmd)
        };

        let running_write_bytes = self
            .running_write_bytes
            .fetch_add(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes + tctx.write_bytes as i64);

        if self.pending_tasks[id_index(cid)]
            .lock()
            .unwrap()
            .insert(cid, task)
            .is_some()
        {
            panic!("command cid={} shouldn't exist", cid);
        }
        SCHED_CONTEX_GAUGE.set(self.pending_tasks.len() as i64);
        if self.task_contexts[id_index(cid)]
            .lock()
            .unwrap()
            .insert(cid, tctx)
            .is_some()
        {
            panic!("TaskContext cid={} shouldn't exist", cid);
        }
    }

    fn dequeue_task_context(&self, cid: u64) -> TaskContext {
        let tctx = self.task_contexts[id_index(cid)]
            .lock()
            .unwrap()
            .remove(&cid)
            .unwrap();

        let running_write_bytes = self
            .running_write_bytes
            .fetch_sub(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes - tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.set(self.pending_tasks.len() as i64);

        tctx
    }

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire)
            >= self.sched_pending_write_threshold.load(Ordering::Acquire)
    }

    /// Generates the lock for a command.
    ///
    /// Basically, read-only commands require no latches, write commands require latches hashed
    /// by the referenced keys.
    fn gen_lock(&self, cmd: &Command) -> Lock {
        gen_command_lock(&self.latches, cmd)
    }

    /// Tries to acquire all the required latches for a command.
    ///
    /// Returns `Some(TaskContext)` if successful; returns `None` otherwise.
    fn acquire_lock(&self, cid: u64) -> bool {
        let mut task_contexts = self.task_contexts[id_index(cid)].lock().unwrap();
        let tctx = task_contexts.get_mut(&cid).unwrap();
        let acquired = self.latches.acquire(&mut tctx.lock, cid);
        tctx.on_schedule();
        acquired
    }
}

#[derive(Clone)]
struct InnerWrapper<E: Engine> {
    engine: E,
    inner: Arc<SchedulerInner<E>>,
}

/// Scheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct Scheduler<E: Engine> {
    inner: InnerWrapper<E>,
    // worker pool
    worker_pool: Arc<Mutex<ThreadPool<SchedContext<E>>>>,

    // high priority commands will be delivered to this pool
    high_priority_pool: Arc<Mutex<ThreadPool<SchedContext<E>>>>,
}

impl<E: Engine> Scheduler<E> {
    /// Creates a scheduler.
    pub fn new(
        engine: E,
        concurrency: usize,
        worker_pool_size: usize,
        sched_pending_write_threshold: usize,
    ) -> Self {
        let factory = SchedContextFactory::new(engine.clone());
        let mut pending_tasks = Vec::with_capacity(TASKS_SLOTS_NUM);
        let mut task_contexts = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            pending_tasks.push(Mutex::new(Default::default()));
            task_contexts.push(Mutex::new(Default::default()));
        }
        let worker_pool = ThreadPoolBuilder::new(thd_name!("sched-worker-pool"), factory.clone())
            .thread_count(worker_pool_size)
            .build();
        let high_priority_pool =
            ThreadPoolBuilder::new(thd_name!("sched-high-pri-pool"), factory).build();

        let inner = Arc::new(SchedulerInner {
            pending_tasks,
            task_contexts,
            id_alloc: AtomicU64::new(0),
            latches: Latches::new(concurrency),
            running_write_bytes: AtomicUsize::new(0),
            sched_pending_write_threshold: AtomicUsize::new(sched_pending_write_threshold),
            worker_scheduler: worker_pool.scheduler(),
            high_priority_scheduler: high_priority_pool.scheduler(),
        });

        Scheduler {
            inner: InnerWrapper { engine, inner },
            worker_pool: Arc::new(Mutex::new(worker_pool)),
            high_priority_pool: Arc::new(Mutex::new(high_priority_pool)),
        }
    }

    pub fn run_cmd(&self, cmd: Command, callback: StorageCb) {
        self.inner.on_receive_new_cmd(cmd, callback);
    }

    pub fn shutdown(&mut self) {
        if let Err(e) = self.worker_pool.lock().unwrap().stop() {
            error!("scheduler run err when worker pool stop:{:?}", e);
        }
        if let Err(e) = self.high_priority_pool.lock().unwrap().stop() {
            error!("scheduler run err when high priority pool stop:{:?}", e);
        }
        info!("scheduler stopped");
    }
}

impl<E: Engine> InnerWrapper<E> {
    pub fn fetch_executor(&self, priority: CommandPri) -> Executor<E, Self> {
        let scheduler = match priority {
            CommandPri::Low | CommandPri::Normal => self.inner.worker_scheduler.clone(),
            CommandPri::High => self.inner.high_priority_scheduler.clone(),
        };
        Executor::new(self.clone(), scheduler)
    }

    /// Releases all the latches held by a command.
    fn release_lock(&self, lock: &Lock, cid: u64) {
        let wakeup_list = self.inner.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.try_to_wake_up(wcid);
        }
    }

    fn schedule_command(&self, cmd: Command, callback: StorageCb) {
        let cid = self.inner.gen_id();
        debug!("received new command, cid={}, cmd={}", cid, cmd);

        let tag = cmd.tag();
        let priority_tag = cmd.priority_tag();
        let task = Task::new(cid, cmd);
        // TODO: enqueue_task should return an reference of the tctx.
        self.inner.enqueue_task(task, callback);
        self.try_to_wake_up(cid);

        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[tag, "new"])
            .inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC
            .with_label_values(&[priority_tag])
            .inc();
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for furthur processing.
    fn try_to_wake_up(&self, cid: u64) {
        let wake = self.inner.acquire_lock(cid);
        if wake {
            self.get_snapshot(cid);
        }
    }

    fn on_receive_new_cmd(&self, cmd: Command, callback: StorageCb) {
        // write flow control
        if cmd.need_flow_control() && self.inner.too_busy() {
            SCHED_TOO_BUSY_COUNTER_VEC
                .with_label_values(&[cmd.tag()])
                .inc();
            execute_callback(
                callback,
                ProcessResult::Failed {
                    err: StorageError::SchedTooBusy,
                },
            );
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
        let executor = self.fetch_executor(task.priority());

        let cb = Box::new(move |(cb_ctx, snapshot)| {
            executor.execute(cb_ctx, snapshot, task);
        });
        if let Err(e) = self.engine.async_snapshot(&ctx, cb) {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "async_snapshot_err"])
                .inc();

            error!("engine async_snapshot failed, err: {:?}", e);
            self.finish_with_err(cid, e.into());
        } else {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "snapshot"])
                .inc();
        }
    }

    /// Calls the callback with an error.
    fn finish_with_err(&self, cid: u64, err: Error) {
        debug!("command cid={}, finished with error", cid);
        let tctx = self.inner.dequeue_task_context(cid);

        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[tctx.tag, "error"])
            .inc();

        let pr = ProcessResult::Failed {
            err: StorageError::from(err),
        };
        execute_callback(tctx.cb, pr);

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: &str) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[tag, "read_finish"])
            .inc();

        debug!("read command(cid={}) finished", cid);
        let tctx = self.inner.dequeue_task_context(cid);
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "next_cmd"])
                .inc();
            self.schedule_command(cmd, tctx.cb);
        } else {
            execute_callback(tctx.cb, pr);
        }

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the success of write.
    fn on_write_finished(&self, cid: u64, pr: ProcessResult, result: EngineResult<()>, tag: &str) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[tag, "write_finish"])
            .inc();

        debug!("write finished for command, cid={}", cid);
        let tctx = self.inner.dequeue_task_context(cid);
        let pr = match result {
            Ok(()) => pr,
            Err(e) => ProcessResult::Failed {
                err: StorageError::from(e),
            },
        };
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[tag, "next_cmd"])
                .inc();
            self.schedule_command(cmd, tctx.cb);
        } else {
            execute_callback(tctx.cb, pr);
        }

        self.release_lock(&tctx.lock, cid);
    }
}

impl<E: Engine> MsgScheduler for InnerWrapper<E> {
    fn on_msg(&self, task: Msg) -> ::std::result::Result<(), ScheduleError<Msg>> {
        match task {
            Msg::ReadFinished { cid, tag, pr } => self.on_read_finished(cid, pr, tag),
            Msg::WriteFinished {
                cid,
                tag,
                pr,
                result,
            } => self.on_write_finished(cid, pr, result, tag),
            Msg::FinishedWithErr { cid, err, .. } => self.finish_with_err(cid, err),
            _ => unreachable!(),
        }
        Ok(())
    }
}

fn gen_command_lock(latches: &Latches, cmd: &Command) -> Lock {
    match *cmd {
        Command::Prewrite { ref mutations, .. } => {
            let keys: Vec<&Key> = mutations.iter().map(|x| x.key()).collect();
            latches.gen_lock(&keys)
        }
        Command::ResolveLock { ref key_locks, .. } => {
            let keys: Vec<&Key> = key_locks.iter().map(|x| &x.0).collect();
            latches.gen_lock(&keys)
        }
        Command::Commit { ref keys, .. } | Command::Rollback { ref keys, .. } => {
            latches.gen_lock(keys)
        }
        Command::Cleanup { ref key, .. } => latches.gen_lock(&[key]),
        Command::Pause { ref keys, .. } => latches.gen_lock(keys),
        _ => Lock::new(vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc;
    use crate::storage::txn::latch::*;
    use crate::storage::{Command, Key, Mutation, Options};
    use kvproto::kvrpcpb::Context;
    use tikv_util::collections::HashMap;

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10, 20);
        let readonly_cmds = vec![
            Command::ScanLock {
                ctx: Context::new(),
                max_ts: 5,
                start_key: None,
                limit: 0,
            },
            Command::ResolveLock {
                ctx: Context::new(),
                txn_status: temp_map.clone(),
                scan_key: None,
                key_locks: vec![],
            },
            Command::MvccByKey {
                ctx: Context::new(),
                key: Key::from_raw(b"k"),
            },
            Command::MvccByStartTs {
                ctx: Context::new(),
                start_ts: 25,
            },
        ];
        let write_cmds = vec![
            Command::Prewrite {
                ctx: Context::new(),
                mutations: vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                primary: b"k".to_vec(),
                start_ts: 10,
                options: Options::default(),
            },
            Command::Commit {
                ctx: Context::new(),
                keys: vec![Key::from_raw(b"k")],
                lock_ts: 10,
                commit_ts: 20,
            },
            Command::Cleanup {
                ctx: Context::new(),
                key: Key::from_raw(b"k"),
                start_ts: 10,
            },
            Command::Rollback {
                ctx: Context::new(),
                keys: vec![Key::from_raw(b"k")],
                start_ts: 10,
            },
            Command::ResolveLock {
                ctx: Context::new(),
                txn_status: temp_map.clone(),
                scan_key: None,
                key_locks: vec![(
                    Key::from_raw(b"k"),
                    mvcc::Lock::new(mvcc::LockType::Put, b"k".to_vec(), 10, 20, None),
                )],
            },
        ];

        let latches = Latches::new(1024);
        let write_locks: Vec<Lock> = write_cmds
            .into_iter()
            .enumerate()
            .map(|(id, cmd)| {
                let mut lock = gen_command_lock(&latches, &cmd);
                assert_eq!(latches.acquire(&mut lock, id as u64), id == 0);
                lock
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut lock = gen_command_lock(&latches, cmd);
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
