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
use std::hash::{Hash, Hasher};
use std::mem;
use std::thread;
use std::time::Duration;
use std::u64;

use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};
use prometheus::local::{LocalHistogramVec, LocalIntCounter};
use prometheus::HistogramTimer;

use storage::engine::{
    self, Callback as EngineCallback, CbContext, Error as EngineError, Modify,
    Result as EngineResult,
};
use storage::mvcc::{
    Error as MvccError, Lock as MvccLock, MvccReader, MvccTxn, Write, MAX_TXN_WRITE_SIZE,
};
use storage::{
    Command, Engine, Error as StorageError, Result as StorageResult, ScanMode, Snapshot,
    Statistics, StatisticsSummary, StorageCb,
};
use storage::{Key, KvPair, MvccInfo, Value, CMD_TAG_GC};
use util::collections::HashMap;
use util::threadpool::{Context as ThreadContext, ThreadPool, ThreadPoolBuilder};
use util::time::SlowTimer;
use util::worker::{self, Runnable, ScheduleError};

use super::super::metrics::*;
use super::latch::{Latches, Lock};
use super::Error;
use super::Result;

pub const CMD_BATCH_SIZE: usize = 256;
// TODO: make it configurable.
pub const GC_BATCH_SIZE: usize = 512;

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

/// After the GC scan of a key, output a message to the log if there are at least this many
/// versions of the key.
const GC_LOG_FOUND_VERSION_THRESHOLD: usize = 30;

/// After the GC delete versions of a key, output a message to the log if at least this many
/// versions are deleted.
const GC_LOG_DELETED_VERSION_THRESHOLD: usize = 30;

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<StorageResult<()>> },
    MultiKvpairs { pairs: Vec<StorageResult<KvPair>> },
    MvccKey { mvcc: MvccInfo },
    MvccStartTs { mvcc: Option<(Key, MvccInfo)> },
    Value { value: Option<Value> },
    Locks { locks: Vec<LockInfo> },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
}

type SnapshotResult = (Vec<u64>, CbContext, EngineResult<Box<Snapshot>>);

/// Message types for the scheduler event loop.
pub enum Msg {
    Quit,
    RawCmd {
        cmd: Command,
        cb: StorageCb,
    },
    RetryGetSnapshots(Vec<(Context, Vec<u64>)>),
    SnapshotFinished {
        cids: Vec<u64>,
        cb_ctx: CbContext,
        snapshot: EngineResult<Box<Snapshot>>,
    },
    BatchSnapshotFinished {
        batch: Vec<SnapshotResult>,
    },
    ReadFinished {
        cid: u64,
        pr: ProcessResult,
    },
    WritePrepareFinished {
        cid: u64,
        cmd: Command,
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
impl Debug for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(f, "Quit"),
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {:?}", cmd),
            Msg::RetryGetSnapshots(ref tasks) => write!(f, "RetryGetSnapshots {:?}", tasks),
            Msg::SnapshotFinished { ref cids, .. } => {
                write!(f, "SnapshotFinished [cids={:?}]", cids)
            }
            Msg::BatchSnapshotFinished { ref batch } => {
                let ids: Vec<&Vec<_>> = batch.iter().map(|&(ref ids, _, _)| ids).collect();
                write!(f, "BatchSnapshotFinished cids: {:?}", ids)
            }
            Msg::ReadFinished { cid, .. } => write!(f, "ReadFinished [cid={}]", cid),
            Msg::WritePrepareFinished { cid, ref cmd, .. } => {
                write!(f, "WritePrepareFinished [cid={}, cmd={:?}]", cid, cmd)
            }
            Msg::WritePrepareFailed { cid, ref err } => {
                write!(f, "WritePrepareFailed [cid={}, err={:?}]", cid, err)
            }
            Msg::WriteFinished { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
        }
    }
}

/// Display for messages.
impl Display for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(f, "Quit"),
            Msg::RawCmd { ref cmd, .. } => write!(f, "RawCmd {:?}", cmd),
            Msg::RetryGetSnapshots(ref tasks) => write!(f, "RetryGetSnapshots {:?}", tasks),
            Msg::SnapshotFinished { ref cids, .. } => {
                write!(f, "SnapshotFinished [cids={:?}]", cids)
            }
            Msg::BatchSnapshotFinished { ref batch } => {
                let ids: Vec<&Vec<_>> = batch.iter().map(|&(ref ids, _, _)| ids).collect();
                write!(f, "BatchSnapshotFinished cids: {:?}", ids)
            }
            Msg::ReadFinished { cid, .. } => write!(f, "ReadFinished [cid={}]", cid),
            Msg::WritePrepareFinished { cid, ref cmd, .. } => {
                write!(f, "WritePrepareFinished [cid={}, cmd={:?}]", cid, cmd)
            }
            Msg::WritePrepareFailed { cid, ref err } => {
                write!(f, "WritePrepareFailed [cid={}, err={:?}]", cid, err)
            }
            Msg::WriteFinished { cid, .. } => write!(f, "WriteFinished [cid={}]", cid),
        }
    }
}

/// Delivers the process result of a command to the storage callback.
fn execute_callback(callback: StorageCb, pr: ProcessResult) {
    match callback {
        StorageCb::Boolean(cb) => match pr {
            ProcessResult::Res => cb(Ok(())),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::Booleans(cb) => match pr {
            ProcessResult::MultiRes { results } => cb(Ok(results)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::SingleValue(cb) => match pr {
            ProcessResult::Value { value } => cb(Ok(value)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::KvPairs(cb) => match pr {
            ProcessResult::MultiKvpairs { pairs } => cb(Ok(pairs)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::MvccInfoByKey(cb) => match pr {
            ProcessResult::MvccKey { mvcc } => cb(Ok(mvcc)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::MvccInfoByStartTs(cb) => match pr {
            ProcessResult::MvccStartTs { mvcc } => cb(Ok(mvcc)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
        StorageCb::Locks(cb) => match pr {
            ProcessResult::Locks { locks } => cb(Ok(locks)),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
    }
}

/// Context for a running command.
pub struct RunningCtx {
    cid: u64,
    cmd: Option<Command>,
    write_bytes: usize,
    lock: Lock,
    callback: Option<StorageCb>,
    tag: &'static str,
    ts: u64,
    region_id: u64,
    latch_timer: Option<HistogramTimer>,
    _timer: HistogramTimer,
    slow_timer: Option<SlowTimer>,
}

impl RunningCtx {
    /// Creates a context for a running command.
    pub fn new(cid: u64, cmd: Command, lock: Lock, cb: StorageCb) -> RunningCtx {
        let tag = cmd.tag();
        let ts = cmd.ts();
        let region_id = cmd.get_context().get_region_id();
        let write_bytes = cmd.write_bytes();
        RunningCtx {
            cid,
            cmd: Some(cmd),
            write_bytes,
            lock,
            callback: Some(cb),
            tag,
            ts,
            region_id,
            latch_timer: Some(
                SCHED_LATCH_HISTOGRAM_VEC
                    .with_label_values(&[tag])
                    .start_coarse_timer(),
            ),
            _timer: SCHED_HISTOGRAM_VEC
                .with_label_values(&[tag])
                .start_coarse_timer(),
            slow_timer: None,
        }
    }
}

impl Drop for RunningCtx {
    fn drop(&mut self) {
        if let Some(ref mut timer) = self.slow_timer {
            slow_log!(
                timer,
                "[region {}] scheduler handle command: {}, ts: {}",
                self.region_id,
                self.tag,
                self.ts
            );
        }
    }
}

/// Creates a callback to receive async results of write prepare from the storage engine.
fn make_engine_cb(
    cmd: &'static str,
    cid: u64,
    pr: ProcessResult,
    scheduler: worker::Scheduler<Msg>,
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

#[derive(Clone)]
struct HashableContext(Context);

impl PartialEq for HashableContext {
    fn eq(&self, other: &HashableContext) -> bool {
        // k1 == k2 ⇒ hash(k1) == hash(k2)
        self.0.get_region_id() == other.0.get_region_id()
            && self.0.get_region_epoch().get_version() == other.0.get_region_epoch().get_version()
            && self.0.get_peer().get_id() == other.0.get_peer().get_id()
    }
}

impl Hash for HashableContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let key = {
            let ctx = &self.0;
            (
                ctx.get_region_id(),
                ctx.get_region_epoch().get_version(),
                ctx.get_peer().get_id(),
            )
        };
        Hash::hash(&key, state);
    }
}

impl Eq for HashableContext {}

/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler {
    engine: Box<Engine>,

    // cid -> RunningCtx
    cmd_ctxs: HashMap<u64, RunningCtx>,
    // Context -> cids
    grouped_cmds: Option<HashMap<HashableContext, Vec<u64>>>,

    // actual scheduler to schedule the execution of commands
    scheduler: worker::Scheduler<Msg>,

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

    has_gc_command: bool,

    // used to control write flow
    running_write_bytes: usize,
}

// Make clippy happy.
type MultipleReturnValue = (Option<MvccLock>, Vec<(u64, Write)>, Vec<(u64, Value)>);

fn find_mvcc_infos_by_key(
    reader: &mut MvccReader,
    key: &Key,
    mut ts: u64,
) -> Result<MultipleReturnValue> {
    let mut writes = vec![];
    let mut values = vec![];
    let lock = reader.load_lock(key)?;
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                ts = commit_ts - 1;
                writes.push((commit_ts, write));
            }
            None => break,
        };
    }
    for (ts, v) in reader.scan_values_in_default(key)? {
        values.push((ts, v));
    }
    Ok((lock, writes, values))
}

impl Scheduler {
    /// Creates a scheduler.
    pub fn new(
        engine: Box<Engine>,
        scheduler: worker::Scheduler<Msg>,
        concurrency: usize,
        worker_pool_size: usize,
        sched_pending_write_threshold: usize,
    ) -> Scheduler {
        Scheduler {
            engine,
            cmd_ctxs: Default::default(),
            grouped_cmds: Some(HashMap::with_capacity_and_hasher(
                CMD_BATCH_SIZE,
                Default::default(),
            )),
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
            has_gc_command: false,
            running_write_bytes: 0,
        }
    }
}

/// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
/// event loop.
fn process_read(
    sched_ctx: &mut SchedContext,
    cid: u64,
    mut cmd: Command,
    scheduler: worker::Scheduler<Msg>,
    snapshot: Box<Snapshot>,
) -> Statistics {
    fail_point!("txn_before_process_read");
    debug!("process read cmd(cid={}) in worker pool.", cid);
    let tag = cmd.tag();

    let mut statistics = Statistics::default();

    let pr = match cmd {
        Command::MvccByKey { ref ctx, ref key } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let res = match find_mvcc_infos_by_key(&mut reader, key, u64::MAX) {
                Ok((lock, writes, values)) => ProcessResult::MvccKey {
                    mvcc: MvccInfo {
                        lock,
                        writes,
                        values,
                    },
                },
                Err(e) => ProcessResult::Failed { err: e.into() },
            };
            statistics.add(reader.get_statistics());
            res
        }
        Command::MvccByStartTs { ref ctx, start_ts } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let res = match reader.seek_ts(start_ts).map_err(StorageError::from) {
                Err(e) => ProcessResult::Failed { err: e },
                Ok(opt) => match opt {
                    Some(key) => match find_mvcc_infos_by_key(&mut reader, &key, u64::MAX) {
                        Ok((lock, writes, values)) => ProcessResult::MvccStartTs {
                            mvcc: Some((
                                key,
                                MvccInfo {
                                    lock,
                                    writes,
                                    values,
                                },
                            )),
                        },
                        Err(e) => ProcessResult::Failed { err: e.into() },
                    },
                    None => ProcessResult::MvccStartTs { mvcc: None },
                },
            };
            statistics.add(reader.get_statistics());
            res
        }
        // Scans locks with timestamp <= `max_ts`
        Command::ScanLock {
            ref ctx,
            max_ts,
            ref mut start_key,
            limit,
            ..
        } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let res = reader
                .scan_lock(start_key.take(), |lock| lock.ts <= max_ts, limit)
                .map_err(Error::from)
                .and_then(|(v, _)| {
                    let mut locks = vec![];
                    for (key, lock) in v {
                        let mut lock_info = LockInfo::new();
                        lock_info.set_primary_lock(lock.primary);
                        lock_info.set_lock_version(lock.ts);
                        lock_info.set_key(key.raw()?);
                        locks.push(lock_info);
                    }
                    sched_ctx
                        .command_keyread_duration
                        .with_label_values(&[tag])
                        .observe(locks.len() as f64);
                    Ok(locks)
                });
            statistics.add(reader.get_statistics());
            match res {
                Ok(locks) => ProcessResult::Locks { locks },
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        Command::ResolveLock {
            ref ctx,
            ref mut txn_status,
            ref mut scan_key,
            ..
        } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let res = reader
                .scan_lock(
                    scan_key.take(),
                    |lock| txn_status.contains_key(&lock.ts),
                    RESOLVE_LOCK_BATCH_SIZE,
                )
                .map_err(Error::from)
                .and_then(|(v, next_scan_key)| {
                    let key_locks: Vec<_> = v.into_iter().map(|x| x).collect();
                    sched_ctx
                        .command_keyread_duration
                        .with_label_values(&[tag])
                        .observe(key_locks.len() as f64);
                    if key_locks.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(Command::ResolveLock {
                            ctx: ctx.clone(),
                            txn_status: mem::replace(txn_status, Default::default()),
                            scan_key: next_scan_key,
                            key_locks,
                        }))
                    }
                });
            statistics.add(reader.get_statistics());
            match res {
                Ok(Some(cmd)) => ProcessResult::NextCommand { cmd },
                Ok(None) => ProcessResult::Res,
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        // Collects garbage.
        Command::Gc {
            ref ctx,
            safe_point,
            ratio_threshold,
            ref mut scan_key,
            ..
        } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            // scan_key is used as start_key here,and Range start gc with scan_key=none.
            let is_range_start_gc = scan_key.is_none();
            // This is an optimization to skip gc before scanning all data.
            let need_gc = if is_range_start_gc {
                reader.need_gc(safe_point, ratio_threshold)
            } else {
                true
            };
            let res = if !need_gc {
                sched_ctx.command_gc_skipped_counter.inc();
                Ok(None)
            } else {
                reader
                    .scan_keys(scan_key.take(), GC_BATCH_SIZE)
                    .map_err(Error::from)
                    .and_then(|(keys, next_start)| {
                        sched_ctx
                            .command_keyread_duration
                            .with_label_values(&[tag])
                            .observe(keys.len() as f64);
                        if keys.is_empty() {
                            // empty range
                            if is_range_start_gc {
                                sched_ctx.command_gc_empty_range_counter.inc();
                            }
                            Ok(None)
                        } else {
                            Ok(Some(Command::Gc {
                                ctx: ctx.clone(),
                                safe_point,
                                ratio_threshold,
                                scan_key: next_start,
                                keys,
                            }))
                        }
                    })
            };
            statistics.add(reader.get_statistics());
            match res {
                Ok(Some(cmd)) => ProcessResult::NextCommand { cmd },
                Ok(None) => ProcessResult::Res,
                Err(e) => ProcessResult::Failed { err: e.into() },
            }
        }
        Command::Pause { duration, .. } => {
            thread::sleep(Duration::from_millis(duration));
            ProcessResult::Res
        }
        _ => panic!("unsupported read command"),
    };

    if let Err(e) = scheduler.schedule(Msg::ReadFinished { cid, pr }) {
        // Todo: if this happens we need to clean up command's context
        panic!("schedule ReadFinished msg failed, cid={}, err={:?}", cid, e);
    }
    statistics
}

/// Processes a write command within a worker thread, then posts either a `WritePrepareFinished`
/// message if successful or a `WritePrepareFailed` message back to the event loop.
fn process_write(
    cid: u64,
    cmd: Command,
    scheduler: worker::Scheduler<Msg>,
    snapshot: Box<Snapshot>,
) -> Statistics {
    fail_point!("txn_before_process_write");
    let mut statistics = Statistics::default();
    if let Err(e) = process_write_impl(cid, cmd, scheduler.clone(), snapshot, &mut statistics) {
        if let Err(err) = scheduler.schedule(Msg::WritePrepareFailed { cid, err: e }) {
            // Todo: if this happens, lock will hold for ever
            panic!(
                "schedule WritePrepareFailed msg failed. cid={}, err={:?}",
                cid, err
            );
        }
    }
    statistics
}

fn process_write_impl(
    cid: u64,
    mut cmd: Command,
    scheduler: worker::Scheduler<Msg>,
    snapshot: Box<Snapshot>,
    statistics: &mut Statistics,
) -> Result<()> {
    let (pr, modifies, rows) = match cmd {
        Command::Prewrite {
            ref ctx,
            ref mutations,
            ref primary,
            start_ts,
            ref options,
            ..
        } => {
            let mut txn = MvccTxn::new(
                snapshot,
                start_ts,
                None,
                ctx.get_isolation_level(),
                !ctx.get_not_fill_cache(),
            );
            let mut locks = vec![];
            let rows = mutations.len();
            for m in mutations {
                match txn.prewrite(m.clone(), primary, options) {
                    Ok(_) => {}
                    e @ Err(MvccError::KeyIsLocked { .. }) => {
                        locks.push(e.map_err(Error::from).map_err(StorageError::from));
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(txn.get_statistics());
            if locks.is_empty() {
                let pr = ProcessResult::MultiRes { results: vec![] };
                let modifies = txn.into_modifies();
                (pr, modifies, rows)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::MultiRes { results: locks };
                (pr, vec![], 0)
            }
        }
        Command::Commit {
            ref ctx,
            ref keys,
            lock_ts,
            commit_ts,
            ..
        } => {
            if commit_ts <= lock_ts {
                return Err(Error::InvalidTxnTso {
                    start_ts: lock_ts,
                    commit_ts,
                });
            }
            let mut txn = MvccTxn::new(
                snapshot,
                lock_ts,
                None,
                ctx.get_isolation_level(),
                !ctx.get_not_fill_cache(),
            );
            let rows = keys.len();
            for k in keys {
                txn.commit(k, commit_ts)?;
            }

            statistics.add(txn.get_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows)
        }
        Command::Cleanup {
            ref ctx,
            ref key,
            start_ts,
            ..
        } => {
            let mut txn = MvccTxn::new(
                snapshot,
                start_ts,
                None,
                ctx.get_isolation_level(),
                !ctx.get_not_fill_cache(),
            );
            txn.rollback(key)?;

            statistics.add(txn.get_statistics());
            (ProcessResult::Res, txn.into_modifies(), 1)
        }
        Command::Rollback {
            ref ctx,
            ref keys,
            start_ts,
            ..
        } => {
            let mut txn = MvccTxn::new(
                snapshot,
                start_ts,
                None,
                ctx.get_isolation_level(),
                !ctx.get_not_fill_cache(),
            );
            let rows = keys.len();
            for k in keys {
                txn.rollback(k)?;
            }

            statistics.add(txn.get_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows)
        }
        Command::ResolveLock {
            ref ctx,
            ref mut txn_status,
            ref mut scan_key,
            ref key_locks,
        } => {
            let mut scan_key = scan_key.take();
            let mut modifies: Vec<Modify> = vec![];
            let mut write_size = 0;
            let rows = key_locks.len();
            for &(ref current_key, ref current_lock) in key_locks {
                let mut txn = MvccTxn::new(
                    snapshot.clone(),
                    current_lock.ts,
                    None,
                    ctx.get_isolation_level(),
                    !ctx.get_not_fill_cache(),
                );
                let status = txn_status.get(&current_lock.ts);
                let commit_ts = match status {
                    Some(ts) => *ts,
                    None => panic!("txn status {} not found.", current_lock.ts),
                };
                if commit_ts > 0 {
                    if current_lock.ts >= commit_ts {
                        return Err(Error::InvalidTxnTso {
                            start_ts: current_lock.ts,
                            commit_ts,
                        });
                    }
                    txn.commit(current_key, commit_ts)?;
                } else {
                    txn.rollback(current_key)?;
                }
                write_size += txn.write_size();

                statistics.add(txn.get_statistics());
                modifies.append(&mut txn.into_modifies());

                if write_size >= MAX_TXN_WRITE_SIZE {
                    scan_key = Some(current_key.to_owned());
                    break;
                }
            }
            let pr = if scan_key.is_none() {
                ProcessResult::Res
            } else {
                ProcessResult::NextCommand {
                    cmd: Command::ResolveLock {
                        ctx: ctx.clone(),
                        txn_status: mem::replace(txn_status, Default::default()),
                        scan_key: scan_key.take(),
                        key_locks: vec![],
                    },
                }
            };
            (pr, modifies, rows)
        }
        Command::Gc {
            ref ctx,
            safe_point,
            ratio_threshold,
            ref mut scan_key,
            ref keys,
        } => {
            let mut scan_key = scan_key.take();
            let mut txn = MvccTxn::new(
                snapshot,
                0,
                Some(ScanMode::Forward),
                ctx.get_isolation_level(),
                !ctx.get_not_fill_cache(),
            );
            let rows = keys.len();
            for k in keys {
                let gc_info = txn.gc(k, safe_point)?;

                if gc_info.found_versions >= GC_LOG_FOUND_VERSION_THRESHOLD {
                    info!(
                        "[region {}] GC found at least {} versions for key {}",
                        ctx.get_region_id(),
                        gc_info.found_versions,
                        k
                    );
                }
                // TODO: we may delete only part of the versions in a batch, which may not beyond
                // the logging threshold `GC_LOG_DELETED_VERSION_THRESHOLD`.
                if gc_info.deleted_versions as usize >= GC_LOG_DELETED_VERSION_THRESHOLD {
                    info!(
                        "[region {}] GC deleted {} versions for key {}",
                        ctx.get_region_id(),
                        gc_info.deleted_versions,
                        k
                    );
                }

                if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                    scan_key = Some(k.to_owned());
                    break;
                }
            }

            statistics.add(txn.get_statistics());
            let pr = if scan_key.is_none() {
                ProcessResult::Res
            } else {
                ProcessResult::NextCommand {
                    cmd: Command::Gc {
                        ctx: ctx.clone(),
                        safe_point,
                        ratio_threshold,
                        scan_key: scan_key.take(),
                        keys: vec![],
                    },
                }
            };
            (pr, txn.into_modifies(), rows)
        }
        _ => panic!("unsupported write command"),
    };

    box_try!(scheduler.schedule(Msg::WritePrepareFinished {
        cid,
        cmd,
        pr,
        to_be_write: modifies,
        rows,
    }));

    Ok(())
}

struct SchedContext {
    stats: HashMap<&'static str, StatisticsSummary>,
    processing_read_duration: LocalHistogramVec,
    processing_write_duration: LocalHistogramVec,
    command_keyread_duration: LocalHistogramVec,
    command_gc_skipped_counter: LocalIntCounter,
    command_gc_empty_range_counter: LocalIntCounter,
}

impl Default for SchedContext {
    fn default() -> SchedContext {
        SchedContext {
            stats: HashMap::default(),
            processing_read_duration: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            processing_write_duration: SCHED_PROCESSING_WRITE_HISTOGRAM_VEC.local(),
            command_keyread_duration: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            command_gc_skipped_counter: KV_COMMAND_GC_SKIPPED_COUNTER.local(),
            command_gc_empty_range_counter: KV_COMMAND_GC_EMPTY_RANGE_COUNTER.local(),
        }
    }
}

impl SchedContext {
    fn add_statistics(&mut self, cmd_tag: &'static str, stat: &Statistics) {
        let entry = self.stats.entry(cmd_tag).or_insert_with(Default::default);
        entry.add_statistics(stat);
    }
}

impl ThreadContext for SchedContext {
    fn on_tick(&mut self) {
        for (cmd, stat) in self.stats.drain() {
            for (cf, details) in stat.stat.details() {
                for (tag, count) in details {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, cf, tag])
                        .inc_by(count as i64);
                }
            }
        }
        self.processing_read_duration.flush();
        self.processing_write_duration.flush();
        self.command_keyread_duration.flush();
        self.command_gc_skipped_counter.flush();
        self.command_gc_empty_range_counter.flush();
    }
}

impl Scheduler {
    /// Generates the next command ID.
    fn gen_id(&mut self) -> u64 {
        self.id_alloc += 1;
        self.id_alloc
    }

    fn insert_ctx(&mut self, ctx: RunningCtx) {
        if ctx.lock.is_write_lock() {
            self.running_write_bytes += ctx.write_bytes;
        }
        if ctx.tag == CMD_TAG_GC {
            self.has_gc_command = true;
        }
        let cid = ctx.cid;
        if self.cmd_ctxs.insert(cid, ctx).is_some() {
            panic!("command cid={} shouldn't exist", cid);
        }
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
        SCHED_CONTEX_GAUGE.set(self.cmd_ctxs.len() as i64);
    }

    fn remove_ctx(&mut self, cid: u64) -> RunningCtx {
        let ctx = self.cmd_ctxs.remove(&cid).unwrap();
        assert_eq!(ctx.cid, cid);
        if ctx.lock.is_write_lock() {
            self.running_write_bytes -= ctx.write_bytes;
        }
        if ctx.tag == CMD_TAG_GC {
            self.has_gc_command = false;
        }
        SCHED_WRITING_BYTES_GAUGE.set(self.running_write_bytes as i64);
        SCHED_CONTEX_GAUGE.set(self.cmd_ctxs.len() as i64);
        ctx
    }

    fn get_ctx_tag(&self, cid: u64) -> &'static str {
        let ctx = &self.cmd_ctxs[&cid];
        ctx.tag
    }

    fn fetch_worker_pool(&self, priority: CommandPri) -> &ThreadPool<SchedContext> {
        match priority {
            CommandPri::Low | CommandPri::Normal => &self.worker_pool,
            CommandPri::High => &self.high_priority_pool,
        }
    }

    /// Delivers a command to a worker thread for processing.
    fn process_by_worker(&mut self, cid: u64, cb_ctx: CbContext, snapshot: Box<Snapshot>) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[self.get_ctx_tag(cid), "process"])
            .inc();
        debug!(
            "process cmd with snapshot, cid={}, cb_ctx={:?}",
            cid, cb_ctx
        );
        let mut cmd = {
            let ctx = &mut self.cmd_ctxs.get_mut(&cid).unwrap();
            assert_eq!(ctx.cid, cid);
            ctx.cmd.take().unwrap()
        };
        if let Some(term) = cb_ctx.term {
            cmd.mut_context().set_term(term);
        }
        let readcmd = cmd.readonly();
        let worker_pool = self.fetch_worker_pool(cmd.priority());
        let tag = cmd.tag();
        let scheduler = self.scheduler.clone();
        if readcmd {
            worker_pool.execute(move |ctx: &mut SchedContext| {
                let _processing_read_timer = ctx
                    .processing_read_duration
                    .with_label_values(&[tag])
                    .start_coarse_timer();

                let s = process_read(ctx, cid, cmd, scheduler, snapshot);
                ctx.add_statistics(tag, &s);
            });
        } else {
            worker_pool.execute(move |ctx: &mut SchedContext| {
                let _processing_write_timer = ctx
                    .processing_write_duration
                    .with_label_values(&[tag])
                    .start_coarse_timer();

                let s = process_write(cid, cmd, scheduler, snapshot);
                ctx.add_statistics(tag, &s);
            });
        }
    }

    /// Calls the callback with an error.
    fn finish_with_err(&mut self, cid: u64, err: Error) {
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
    /// acquired,  the method initiates a get snapshot operation for furthur processing; otherwise,
    /// the method adds the command to the waiting queue(s).   The command will be handled later in
    /// `lock_and_register_get_snapshot` when its turn comes.
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
        let ctx = RunningCtx::new(cid, cmd, lock, callback);
        self.insert_ctx(ctx);
        self.lock_and_register_get_snapshot(cid);
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
        // Allow 1 GC command at the same time.
        if cmd.tag() == CMD_TAG_GC && self.has_gc_command {
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
    fn get_snapshot(&mut self, ctx: &Context, cids: Vec<u64>) {
        for cid in &cids {
            SCHED_STAGE_COUNTER_VEC
                .with_label_values(&[self.get_ctx_tag(*cid), "snapshot"])
                .inc();
        }
        let cids1 = cids.clone();
        let scheduler = self.scheduler.clone();
        let cb = box move |(cb_ctx, snapshot)| match scheduler.schedule(Msg::SnapshotFinished {
            cids: cids1,
            cb_ctx,
            snapshot,
        }) {
            Ok(_) => {}
            e @ Err(ScheduleError::Stopped(_)) => info!("scheduler worker stopped, err {:?}", e),
            Err(e) => panic!("schedule SnapshotFinish msg failed, err {:?}", e),
        };

        if let Err(e) = self.engine.async_snapshot(ctx, cb) {
            for cid in cids {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.get_ctx_tag(cid), "async_snapshot_err"])
                    .inc();

                let e = e.maybe_clone().unwrap_or_else(|| {
                    error!("async snapshot failed for cid={}, error {:?}", cid, e);
                    EngineError::Other(box_err!("{:?}", e))
                });
                self.finish_with_err(cid, Error::from(e));
            }
        }
    }

    /// Initiates an async operation to batch get snapshot from the storage engine, then posts a
    /// `BatchSnapshotFinished` message back to the event loop when it finishes, also it posts a
    /// `RetryGetSnapshots` message if there are any `None` responses.
    fn batch_get_snapshot(&mut self, batch: Vec<(Context, Vec<u64>)>) {
        let mut all_cids = Vec::with_capacity(batch.iter().map(|&(_, ref cids)| cids.len()).sum());
        for &(_, ref cids) in &batch {
            for cid in cids {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.get_ctx_tag(*cid), "batch_snapshot"])
                    .inc();
            }
            all_cids.extend(cids);
        }

        let scheduler = self.scheduler.clone();
        let batch1 = batch.iter().map(|&(ref ctx, _)| ctx.clone()).collect();
        let on_finished: engine::BatchCallback<Box<Snapshot>> = box move |results: Vec<_>| {
            let mut ready = Vec::with_capacity(results.len());
            let mut retry = Vec::new();
            for ((ctx, cids), snapshot) in batch.into_iter().zip(results) {
                match snapshot {
                    Some((cb_ctx, snapshot)) => {
                        ready.push((cids, cb_ctx, snapshot));
                    }
                    None => {
                        retry.push((ctx, cids));
                    }
                }
            }
            if !ready.is_empty() {
                match scheduler.schedule(Msg::BatchSnapshotFinished { batch: ready }) {
                    Ok(_) => {}
                    e @ Err(ScheduleError::Stopped(_)) => {
                        info!("scheduler worker stopped, err {:?}", e);
                    }
                    Err(e) => {
                        panic!("schedule BatchSnapshotFinish msg failed, err {:?}", e);
                    }
                }
            }
            if !retry.is_empty() {
                BATCH_COMMANDS
                    .with_label_values(&["retry"])
                    .observe(retry.len() as f64);
                match scheduler.schedule(Msg::RetryGetSnapshots(retry)) {
                    Ok(_) => {}
                    e @ Err(ScheduleError::Stopped(_)) => {
                        info!("scheduler worker stopped, err {:?}", e);
                    }
                    Err(e) => {
                        panic!("schedule RetryGetSnapshots msg failed, err {:?}", e);
                    }
                }
            }
        };

        if let Err(e) = self.engine.async_batch_snapshot(batch1, on_finished) {
            for cid in all_cids {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.get_ctx_tag(cid), "async_batch_snapshot_err"])
                    .inc();
                let e = e.maybe_clone().unwrap_or_else(|| {
                    error!("async snapshot failed for cid={}, error {:?}", cid, e);
                    EngineError::Other(box_err!("{:?}", e))
                });

                self.finish_with_err(cid, Error::from(e));
            }
        }
    }

    fn on_retry_get_snapshots(&mut self, batch: Vec<(Context, Vec<u64>)>) {
        for (ctx, cids) in batch {
            for cid in &cids {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.get_ctx_tag(*cid), "snapshot_retry"])
                    .inc();
            }
            self.get_snapshot(&ctx, cids);
        }
    }

    /// Event handler for the completion of get snapshot.
    ///
    /// Delivers the command along with the snapshot to a worker thread to execute.
    fn on_snapshot_finished(
        &mut self,
        cids: Vec<u64>,
        cb_ctx: CbContext,
        snapshot: EngineResult<Box<Snapshot>>,
    ) {
        fail_point!("scheduler_async_snapshot_finish");
        debug!(
            "receive snapshot finish msg for cids={:?}, cb_ctx={:?}",
            cids, cb_ctx
        );

        match snapshot {
            Ok(snapshot) => for cid in cids {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[self.get_ctx_tag(cid), "snapshot_ok"])
                    .inc();
                self.process_by_worker(cid, cb_ctx.clone(), snapshot.clone());
            },
            Err(ref e) => {
                error!("get snapshot failed for cids={:?}, error {:?}", cids, e);
                for cid in cids {
                    SCHED_STAGE_COUNTER_VEC
                        .with_label_values(&[self.get_ctx_tag(cid), "snapshot_err"])
                        .inc();
                    let e = e
                        .maybe_clone()
                        .unwrap_or_else(|| EngineError::Other(box_err!("{:?}", e)));

                    self.finish_with_err(cid, Error::from(e));
                }
            }
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
        cmd: Command,
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
        let engine_cb = make_engine_cb(cmd.tag(), cid, pr, self.scheduler.clone(), rows);
        if let Err(e) = self
            .engine
            .async_write(cmd.get_context(), to_be_write, engine_cb)
        {
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
            self.lock_and_register_get_snapshot(wcid);
        }
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for furthur processing.
    fn lock_and_register_get_snapshot(&mut self, cid: u64) {
        if self.acquire_lock(cid) {
            let ctx = self.extract_context(cid).clone();
            let group = self
                .grouped_cmds
                .as_mut()
                .unwrap()
                .entry(HashableContext(ctx))
                .or_insert_with(Vec::new);
            group.push(cid);
        }
    }
}

impl Runnable<Msg> for Scheduler {
    fn run(&mut self, msg: Msg) {
        match msg {
            Msg::Quit => {
                self.shutdown();
                return;
            }
            Msg::RawCmd { cmd, cb } => self.on_receive_new_cmd(cmd, cb),
            Msg::RetryGetSnapshots(batch) => self.on_retry_get_snapshots(batch),
            Msg::SnapshotFinished {
                cids,
                cb_ctx,
                snapshot,
            } => self.on_snapshot_finished(cids, cb_ctx, snapshot),
            Msg::BatchSnapshotFinished { batch } => for (cids, cb_ctx, snapshot) in batch {
                self.on_snapshot_finished(cids, cb_ctx, snapshot)
            },
            Msg::ReadFinished { cid, pr } => self.on_read_finished(cid, pr),
            Msg::WritePrepareFinished {
                cid,
                cmd,
                pr,
                to_be_write,
                rows,
            } => self.on_write_prepare_finished(cid, cmd, pr, to_be_write, rows),
            Msg::WritePrepareFailed { cid, err } => self.on_write_prepare_failed(cid, err),
            Msg::WriteFinished {
                cid, pr, result, ..
            } => self.on_write_finished(cid, pr, result),
        }
    }

    fn run_batch(&mut self, msgs: &mut Vec<Msg>) {
        for msg in msgs.drain(..) {
            self.run(msg);
        }
    }

    fn on_tick(&mut self) {
        if self.grouped_cmds.as_ref().unwrap().is_empty() {
            return;
        }

        if let Some(cmds) = self.grouped_cmds.take() {
            self.grouped_cmds = Some(HashMap::with_capacity_and_hasher(
                CMD_BATCH_SIZE,
                Default::default(),
            ));
            let batch = cmds.into_iter().map(|(hash_ctx, cids)| {
                BATCH_COMMANDS
                    .with_label_values(&["all"])
                    .observe(cids.len() as f64);
                (hash_ctx.0, cids)
            });
            self.batch_get_snapshot(batch.collect());
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
    use storage::{make_key, Command, Mutation, Options};
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
            Command::Gc {
                ctx: Context::new(),
                safe_point: 5,
                ratio_threshold: 0.0,
                scan_key: None,
                keys: vec![make_key(b"k")],
            },
            Command::MvccByKey {
                ctx: Context::new(),
                key: make_key(b"k"),
            },
            Command::MvccByStartTs {
                ctx: Context::new(),
                start_ts: 25,
            },
        ];
        let write_cmds = vec![
            Command::Prewrite {
                ctx: Context::new(),
                mutations: vec![Mutation::Put((make_key(b"k"), b"v".to_vec()))],
                primary: b"k".to_vec(),
                start_ts: 10,
                options: Options::default(),
            },
            Command::Commit {
                ctx: Context::new(),
                keys: vec![make_key(b"k")],
                lock_ts: 10,
                commit_ts: 20,
            },
            Command::Cleanup {
                ctx: Context::new(),
                key: make_key(b"k"),
                start_ts: 10,
            },
            Command::Rollback {
                ctx: Context::new(),
                keys: vec![make_key(b"k")],
                start_ts: 10,
            },
            Command::ResolveLock {
                ctx: Context::new(),
                txn_status: temp_map.clone(),
                scan_key: None,
                key_locks: vec![(
                    make_key(b"k"),
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
