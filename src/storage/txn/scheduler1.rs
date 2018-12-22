// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
use std::u64;

use kvproto::kvrpcpb::CommandPri;
use prometheus::HistogramTimer;

use storage::engine::Result as EngineResult;
use storage::Key;
use storage::{Command, Engine, Error as StorageError, StorageCb};
use util::collections::HashMap;
use util::threadpool::{self, ThreadPool, ThreadPoolBuilder};
use util::worker::{self, Runnable};

use super::super::metrics::*;
use super::latch::{MutexLatches, MutexLock};
use super::process::MsgScheduler;
use super::process::{
    execute_callback, Executor, ProcessResult, SchedContext, SchedContextFactory, Task,
};
use super::scheduler::Msg;
use super::Error;
use util::worker::ScheduleError;

pub const CMD_BATCH_SIZE: usize = 256;

// It stores context of a task.
struct TaskContext {
    lock: MutexLock,
    cb: StorageCb,
    write_bytes: usize,
    tag: &'static str,
    // How long it waits on latches.
    latch_timer: Option<HistogramTimer>,
    // Total duration of a command.
    _cmd_timer: HistogramTimer,
}

impl TaskContext {
    fn new(lock: MutexLock, cb: StorageCb, cmd: &Command) -> TaskContext {
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

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const TASKS_SLOTS_NUM: usize = 1024;

struct SchedulerInner<E: Engine> {
    // cid -> Task
    pending_tasks: Vec<Mutex<HashMap<u64, Task>>>,

    // cid -> TaskContext
    task_contexts: Vec<Mutex<HashMap<u64, TaskContext>>>,

    // cmd id generator
    id_alloc: AtomicU64,

    // write concurrency control
    latches: MutexLatches,

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
        // SCHED_CONTEX_GAUGE.set(self.pending_tasks.len() as i64);
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
        // SCHED_CONTEX_GAUGE.set(self.pending_tasks.len() as i64);

        tctx
    }

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        // self.running_write_bytes.load(Ordering::Acquire)
        //     >= self.sched_pending_write_threshold.load(Ordering::Acquire)
        false
    }

    /// Generates the lock for a command.
    ///
    /// Basically, read-only commands require no latches, write commands require latches hashed
    /// by the referenced keys.
    fn gen_lock(&self, cmd: &Command) -> MutexLock {
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
            pending_tasks: pending_tasks,
            task_contexts: task_contexts,
            id_alloc: AtomicU64::new(0),
            latches: MutexLatches::new(concurrency),
            running_write_bytes: AtomicUsize::new(0),
            sched_pending_write_threshold: AtomicUsize::new(0),
            worker_scheduler: worker_pool.scheduler(),
            high_priority_scheduler: high_priority_pool.scheduler(),
        });

        Scheduler {
            inner: InnerWrapper {
                engine: engine,
                inner: inner,
            },
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
    fn release_lock(&self, lock: &MutexLock, cid: u64) {
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

        let cb = box move |(cb_ctx, snapshot)| {
            executor.execute(cb_ctx, snapshot, task);
        };
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
                err: ::storage::Error::from(e),
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

// impl<E: Engine> Runnable<Msg> for Scheduler<E> {
//     fn run_batch(&mut self, msgs: &mut Vec<Msg>) {
//         for msg in msgs.drain(..) {
//             match msg {
//                 Msg::Quit => {
//                     self.shutdown();
//                     return;
//                 }
//                 Msg::RawCmd { cmd, cb } => self.on_receive_new_cmd(cmd, cb),
//                 Msg::ReadFinished { cid, tag, pr } => self.on_read_finished(cid, pr, tag),
//                 Msg::WriteFinished {
//                     cid,
//                     tag,
//                     pr,
//                     result,
//                 } => self.on_write_finished(cid, pr, result, tag),
//                 Msg::FinishedWithErr { cid, err, .. } => self.finish_with_err(cid, err),
//             }
//         }
//     }

//     fn shutdown(&mut self) {
//         if let Err(e) = self.worker_pool.stop() {
//             error!("scheduler run err when worker pool stop:{:?}", e);
//         }
//         if let Err(e) = self.high_priority_pool.stop() {
//             error!("scheduler run err when high priority pool stop:{:?}", e);
//         }
//         info!("scheduler stopped");
//     }
// }

fn gen_command_lock(latches: &MutexLatches, cmd: &Command) -> MutexLock {
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
        _ => MutexLock::new(vec![]),
    }
}
