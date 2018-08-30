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

use kvproto::kvrpcpb::{CommandPri, Context};

use storage::engine::{Callback as EngineCallback, CbContext, Modify, Result as EngineResult};
use storage::Key;
use storage::{Command, Engine, Error as StorageError, StorageCb};
use util::collections::HashMap;
use util::threadpool::{ThreadPool, ThreadPoolBuilder};
use util::time::SlowTimer;
use util::worker::{self, Runnable, ScheduleError};

use super::super::metrics::*;
use super::latch::{Latches, Lock};
use super::process::{execute_callback, ProcessResult, SchedContext, Task};
use super::Error;

pub const CMD_BATCH_SIZE: usize = 256;

/// Message types for the scheduler event loop.
pub enum Msg<E: Engine> {
    Quit,
    RawCmd {
        cmd: Command,
        cb: StorageCb,
    },
    SnapshotFinished {
        cid: u64,
        cb_ctx: CbContext,
        snapshot: EngineResult<E::Snap>,
    },
    ReadFinished {
        cid: u64,
        pr: ProcessResult,
    },
    WritePrepareFinished {
        cid: u64,
        ctx: Context,
        tag: &'static str,
        pr: ProcessResult,
        to_be_write: Vec<Modify>,
        rows: usize,
    },
    WritePrepareFailed {
        cid: u64,
        err: Error,
    },
    WriteFinished {
        cid: u64,
        pr: ProcessResult,
        cb_ctx: CbContext,
        result: EngineResult<()>,
    },
}

/// Debug for messages.
impl<E: Engine> Debug for Msg<E> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Display for messages.
impl<E: Engine> Display for Msg<E> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(f, "Quit"),
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {:?}", cmd),
            Msg::SnapshotFinished { cid, .. } => write!(f, "SnapshotFinished [cid={}]", cid),
            Msg::ReadFinished { cid, .. } => write!(f, "ReadFinished [cid={}]", cid),
            Msg::WritePrepareFinished {
                cid, ref ctx, tag, ..
            } => write!(
                f,
                "WritePrepareFinished [cid={}, tag={}, ctx={:?}]",
                cid, tag, ctx
            ),
            Msg::WritePrepareFailed { cid, ref err } => {
                write!(f, "WritePrepareFailed [cid={}, err={:?}]", cid, err)
            }
            Msg::WriteFinished { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
        }
    }
}

/// Creates a callback to receive async results of write prepare from the storage engine.
fn make_engine_cb<E: Engine>(
    cmd: &'static str,
    cid: u64,
    pr: ProcessResult,
    scheduler: worker::Scheduler<Msg<E>>,
    rows: usize,
) -> EngineCallback<()> {
    Box::new(move |(cb_ctx, result)| {
        match scheduler.schedule(Msg::WriteFinished {
            cid,
            pr,
            cb_ctx,
            result,
        }) {
            Ok(_) => {
                KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                    .with_label_values(&[cmd])
                    .observe(rows as f64);
            }
            e @ Err(ScheduleError::Stopped(_)) => info!("scheduler worker stopped, {:?}", e),
            Err(e) => {
                panic!(
                    "schedule WriteFinished msg failed, cid={}, err:{:?}",
                    cid, e
                );
            }
        }
    })
}

/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler<E: Engine> {
    engine: E,

    // cid -> Task
    pub cmd_ctxs: HashMap<u64, Task>,

    // actual scheduler to schedule the execution of commands
    pub scheduler: worker::Scheduler<Msg<E>>,

    // cmd id generator
    id_alloc: u64,

    // write concurrency control
    latches: Latches,

    // TODO: Dynamically calculate this value according to processing
    // speed of recent write requests.
    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: ThreadPool<SchedContext>,

    // high priority commands will be delivered to this pool
    high_priority_pool: ThreadPool<SchedContext>,

    // used to control write flow
    running_write_bytes: usize,
}

impl<E: Engine> Scheduler<E> {
    /// Creates a scheduler.
    pub fn new(
        engine: E,
        scheduler: worker::Scheduler<Msg<E>>,
        concurrency: usize,
        worker_pool_size: usize,
        sched_pending_write_threshold: usize,
    ) -> Self {
        Scheduler {
            engine,
            cmd_ctxs: Default::default(),
            scheduler,
            id_alloc: 0,
            latches: Latches::new(concurrency),
            sched_pending_write_threshold,
            worker_pool: ThreadPoolBuilder::with_default_factory(thd_name!("sched-worker-pool"))
                .thread_count(worker_pool_size)
                .build(),
            high_priority_pool: ThreadPoolBuilder::with_default_factory(thd_name!(
                "sched-high-pri-pool"
            )).build(),
            running_write_bytes: 0,
        }
    }

    /// Generates the next command ID.
    fn gen_id(&mut self) -> u64 {
        self.id_alloc += 1;
        self.id_alloc
    }

    fn insert_ctx(&mut self, ctx: Task) {
        if ctx.lock.is_write_lock() {
            self.running_write_bytes += ctx.write_bytes;
        }
        let cid = ctx.cid;
        if self.cmd_ctxs.insert(cid, ctx).is_some() {
            panic!("command cid={} shouldn't exist", cid);
        }
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
        SCHED_CONTEX_GAUGE.set(self.cmd_ctxs.len() as i64);
    }

    fn remove_ctx(&mut self, cid: u64) -> Task {
        let ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        if ctx.lock.is_write_lock() {
            self.running_write_bytes -= ctx.write_bytes;
        }
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
        SCHED_CONTEX_GAUGE.set(self.cmd_ctxs.len() as i64);
        ctx
    }

    pub fn get_ctx_tag(&self, cid: u64) -> &'static str {
        let ctx = &self.cmd_ctxs[&cid];
        ctx.tag
    }

    pub fn fetch_worker_pool(&self, priority: CommandPri) -> &ThreadPool<SchedContext> {
        match priority {
            CommandPri::Low | CommandPri::Normal => &self.worker_pool,
            CommandPri::High => &self.high_priority_pool,
        }
    }

    /// Calls the callback with an error.
    pub fn finish_with_err(&mut self, cid: u64, err: Error) {
        debug!("command cid={}, finished with error", cid);
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.get_ctx_tag(cid), "error"])
            .inc();

        let mut ctx = self.remove_ctx(cid);
        let cb = ctx.callback.take().unwrap();
        let pr = ProcessResult::Failed {
            err: StorageError::from(err),
        };
        execute_callback(cb, pr);

        self.release_lock(&ctx.lock, cid);
    }

    /// Extracts the context of a command.
    fn extract_context(&self, cid: u64) -> &Context {
        let ctx = &self.cmd_ctxs[&cid];
        assert_eq!(ctx.cid, cid);
        ctx.cmd.as_ref().unwrap().get_context()
    }

    /// Event handler for new command.
    ///
    /// This method will try to acquire all the necessary latches. If all the necessary latches are
    /// acquired, the method initiates a get snapshot operation for furthur processing; otherwise,
    /// the method adds the command to the waiting queue(s). The command will be handled later in
    /// `lock_and_get_snapshot` when its turn comes.
    ///
    /// Note that once a command is ready to execute, the snapshot is always up-to-date during the
    /// execution because 1) all the conflicting commands (if any) must be in the waiting queues;
    /// 2) there may be non-conflicitng commands running concurrently, but it doesn't matter.
    fn schedule_command(&mut self, cmd: Command, callback: StorageCb) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[cmd.tag(), "new"])
            .inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC
            .with_label_values(&[cmd.priority_tag()])
            .inc();
        let cid = self.gen_id();
        debug!("received new command, cid={}, cmd={}", cid, cmd);
        let lock = gen_command_lock(&self.latches, &cmd);
        let ctx = Task::new(cid, cmd, lock, callback);
        self.insert_ctx(ctx);
        self.lock_and_get_snapshot(cid);
    }

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes >= self.sched_pending_write_threshold
    }

    fn on_receive_new_cmd(&mut self, cmd: Command, callback: StorageCb) {
        // write flow control
        if cmd.need_flow_control() && self.too_busy() {
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

    /// Tries to acquire all the required latches for a command.
    ///
    /// Returns true if successful; returns false otherwise.
    fn acquire_lock(&mut self, cid: u64) -> bool {
        let ctx = &mut self.cmd_ctxs.get_mut(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        let ok = self.latches.acquire(&mut ctx.lock, cid);
        if ok {
            ctx.latch_timer.take();
            ctx.slow_timer = Some(SlowTimer::new());
        }
        ok
    }

    /// Initiates an async operation to get a snapshot from the storage engine, then posts a
    /// `SnapshotFinished` message back to the event loop when it finishes.
    fn get_snapshot(&mut self, cid: u64) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.get_ctx_tag(cid), "snapshot"])
            .inc();
        let scheduler = self.scheduler.clone();
        let cb = box move |(cb_ctx, snapshot)| match scheduler.schedule(Msg::SnapshotFinished {
            cid,
            cb_ctx,
            snapshot,
        }) {
            Ok(_) => {}
            e @ Err(ScheduleError::Stopped(_)) => info!("scheduler worker stopped, err {:?}", e),
            Err(e) => panic!("schedule SnapshotFinish msg failed, err {:?}", e),
        };

        if let Err(e) = self.engine.async_snapshot(self.extract_context(cid), cb) {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[self.get_ctx_tag(cid), "async_snapshot_err"])
                .inc();
            self.finish_with_err(cid, Error::from(e));
        }
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&mut self, cid: u64, pr: ProcessResult) {
        debug!("read command(cid={}) finished", cid);
        let mut ctx = self.remove_ctx(cid);
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[ctx.tag, "read_finish"])
            .inc();
        let cb = ctx.callback.take().unwrap();
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[ctx.tag, "next_cmd"])
                .inc();
            self.schedule_command(cmd, cb);
        } else {
            execute_callback(cb, pr);
        }

        self.release_lock(&ctx.lock, cid);
    }

    /// Event handler for the failure of write prepare.
    ///
    /// Write prepare failure typically means conflicting transactions are detected. Delivers the
    /// error to the callback, and releases the latches.
    fn on_write_prepare_failed(&mut self, cid: u64, e: Error) {
        debug!("write command(cid={}) failed at prewrite.", cid);
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.get_ctx_tag(cid), "prepare_write_err"])
            .inc();
        self.finish_with_err(cid, e);
    }

    /// Event handler for the success of write prepare.
    ///
    /// Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
    /// message when it finishes.
    fn on_write_prepare_finished(
        &mut self,
        cid: u64,
        cmd_ctx: Context,
        cmd_tag: &'static str,
        pr: ProcessResult,
        to_be_write: Vec<Modify>,
        rows: usize,
    ) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.get_ctx_tag(cid), "write"])
            .inc();
        if to_be_write.is_empty() {
            return self.on_write_finished(cid, pr, Ok(()));
        }
        let engine_cb = make_engine_cb(cmd_tag, cid, pr, self.scheduler.clone(), rows);
        if let Err(e) = self.engine.async_write(&cmd_ctx, to_be_write, engine_cb) {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[self.get_ctx_tag(cid), "async_write_err"])
                .inc();
            self.finish_with_err(cid, Error::from(e));
        }
    }

    /// Event handler for the success of write.
    fn on_write_finished(&mut self, cid: u64, pr: ProcessResult, result: EngineResult<()>) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.get_ctx_tag(cid), "write_finish"])
            .inc();
        debug!("write finished for command, cid={}", cid);
        let mut ctx = self.remove_ctx(cid);
        let cb = ctx.callback.take().unwrap();
        let pr = match result {
            Ok(()) => pr,
            Err(e) => ProcessResult::Failed {
                err: ::storage::Error::from(e),
            },
        };
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[ctx.tag, "next_cmd"])
                .inc();
            self.schedule_command(cmd, cb);
        } else {
            execute_callback(cb, pr);
        }

        self.release_lock(&ctx.lock, cid);
    }

    /// Releases all the latches held by a command.
    fn release_lock(&mut self, lock: &Lock, cid: u64) {
        let wakeup_list = self.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.lock_and_get_snapshot(wcid);
        }
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for furthur processing.
    fn lock_and_get_snapshot(&mut self, cid: u64) {
        if self.acquire_lock(cid) {
            self.get_snapshot(cid);
        }
    }
}

impl<E: Engine> Runnable<Msg<E>> for Scheduler<E> {
    fn run(&mut self, _: Msg<E>) {
        panic!("Shouldn't call Scheduler::run directly");
    }

    fn run_batch(&mut self, msgs: &mut Vec<Msg<E>>) {
        for msg in msgs.drain(..) {
            match msg {
                Msg::Quit => {
                    self.shutdown();
                    return;
                }
                Msg::RawCmd { cmd, cb } => self.on_receive_new_cmd(cmd, cb),
                Msg::SnapshotFinished {
                    cid,
                    cb_ctx,
                    snapshot,
                } => self.on_snapshot_finished(cid, cb_ctx, snapshot),
                Msg::ReadFinished { cid, pr } => self.on_read_finished(cid, pr),
                Msg::WritePrepareFinished {
                    cid,
                    ctx,
                    tag,
                    pr,
                    to_be_write,
                    rows,
                } => self.on_write_prepare_finished(cid, ctx, tag, pr, to_be_write, rows),
                Msg::WritePrepareFailed { cid, err } => self.on_write_prepare_failed(cid, err),
                Msg::WriteFinished {
                    cid, pr, result, ..
                } => self.on_write_finished(cid, pr, result),
            }
        }
    }

    fn shutdown(&mut self) {
        if let Err(e) = self.worker_pool.stop() {
            error!("scheduler run err when worker pool stop:{:?}", e);
        }
        if let Err(e) = self.high_priority_pool.stop() {
            error!("scheduler run err when high priority pool stop:{:?}", e);
        }
        info!("scheduler stopped");
    }
}

/// Generates the lock for a command.
///
/// Basically, read-only commands require no latches, write commands require latches hashed
/// by the referenced keys.
pub fn gen_command_lock(latches: &Latches, cmd: &Command) -> Lock {
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
        _ => Lock::new(vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kvproto::kvrpcpb::Context;
    use storage::mvcc;
    use storage::txn::latch::*;
    use storage::{Command, Key, Mutation, Options};
    use util::collections::HashMap;

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

        let mut latches = Latches::new(1024);

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
