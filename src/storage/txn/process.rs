// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;
use std::thread;
use std::time::Duration;
use std::u64;

use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};
use prometheus::local::LocalHistogramVec;

use crate::storage::kv::{CbContext, Modify, Result as EngineResult};
use crate::storage::mvcc::{
    Error as MvccError, Lock as MvccLock, MvccReader, MvccTxn, Write, MAX_TXN_WRITE_SIZE,
};
use crate::storage::{
    Command, Engine, Error as StorageError, Result as StorageResult, ScanMode, Snapshot,
    Statistics, StatisticsSummary, StorageCb,
};
use crate::storage::{Key, MvccInfo, Value};
use tikv_util::collections::HashMap;
use tikv_util::threadpool::{
    self, Context as ThreadContext, ContextFactory as ThreadContextFactory,
};
use tikv_util::time::SlowTimer;
use tikv_util::worker::{self, ScheduleError};

use super::super::metrics::*;
use super::scheduler::Msg;
use super::{Error, Result};
use crate::storage::lock_manager::{self, DetectorScheduler, WaiterMgrScheduler};

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

/// Process result of a command.
pub enum ProcessResult {
    Res,
    MultiRes { results: Vec<StorageResult<()>> },
    MvccKey { mvcc: MvccInfo },
    MvccStartTs { mvcc: Option<(Key, MvccInfo)> },
    Locks { locks: Vec<LockInfo> },
    NextCommand { cmd: Command },
    Failed { err: StorageError },
}

/// Delivers the process result of a command to the storage callback.
pub fn execute_callback(callback: StorageCb, pr: ProcessResult) {
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

/// Task is a running command.
pub struct Task {
    pub cid: u64,
    pub tag: &'static str,

    cmd: Command,
    ts: u64,
    region_id: u64,
}

impl Task {
    /// Creates a task for a running command.
    pub fn new(cid: u64, cmd: Command) -> Task {
        Task {
            cid,
            tag: cmd.tag(),
            region_id: cmd.get_context().get_region_id(),
            ts: cmd.ts(),
            cmd,
        }
    }

    pub fn cmd(&self) -> &Command {
        &self.cmd
    }

    pub fn priority(&self) -> CommandPri {
        self.cmd.priority()
    }

    pub fn context(&self) -> &Context {
        self.cmd.get_context()
    }
}

pub struct Executor<E: Engine> {
    // We put time consuming tasks to the thread pool.
    pool: Option<threadpool::Scheduler<SchedContext<E>>>,
    // And the tasks completes we post a completion to the `Scheduler`.
    scheduler: Option<worker::Scheduler<Msg>>,
    // If the task releases some locks, we wake up waiters waiting for them.
    waiter_mgr_scheduler: Option<WaiterMgrScheduler>,
    detector_scheduler: Option<DetectorScheduler>,
}

impl<E: Engine> Executor<E> {
    pub fn new(
        pool: threadpool::Scheduler<SchedContext<E>>,
        scheduler: worker::Scheduler<Msg>,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        detector_scheduler: DetectorScheduler,
    ) -> Self {
        Executor {
            pool: Some(pool),
            scheduler: Some(scheduler),
            waiter_mgr_scheduler: Some(waiter_mgr_scheduler),
            detector_scheduler: Some(detector_scheduler),
        }
    }

    fn take_pool(&mut self) -> threadpool::Scheduler<SchedContext<E>> {
        self.pool.take().unwrap()
    }

    fn take_scheduler(&mut self) -> worker::Scheduler<Msg> {
        self.scheduler.take().unwrap()
    }

    fn take_waiter_mgr_scheduler(&mut self) -> WaiterMgrScheduler {
        self.waiter_mgr_scheduler.take().unwrap()
    }

    fn take_detector_scheduler(&mut self) -> DetectorScheduler {
        self.detector_scheduler.take().unwrap()
    }

    /// Start the execution of the task.
    pub fn execute(mut self, cb_ctx: CbContext, snapshot: EngineResult<E::Snap>, task: Task) {
        debug!(
            "receive snapshot finish msg";
            "cid" => task.cid, "cb_ctx" => ?cb_ctx
        );

        match snapshot {
            Ok(snapshot) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[task.tag, "snapshot_ok"])
                    .inc();

                self.process_by_worker(cb_ctx, snapshot, task);
            }
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[task.tag, "snapshot_err"])
                    .inc();

                error!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                notify_scheduler(
                    self.take_scheduler(),
                    Msg::FinishedWithErr {
                        cid: task.cid,
                        err: Error::from(err),
                        tag: task.tag,
                    },
                );
            }
        }
    }

    /// Delivers a command to a worker thread for processing.
    fn process_by_worker(mut self, cb_ctx: CbContext, snapshot: E::Snap, mut task: Task) {
        SCHED_STAGE_COUNTER_VEC
            .with_label_values(&[task.tag, "process"])
            .inc();
        debug!(
            "process cmd with snapshot";
            "cid" => task.cid, "cb_ctx" => ?cb_ctx
        );
        let tag = task.tag;
        if let Some(term) = cb_ctx.term {
            task.cmd.mut_context().set_term(term);
        }
        let pool = self.take_pool();
        let readonly = task.cmd.readonly();
        pool.schedule(move |ctx: &mut SchedContext<E>| {
            fail_point!("scheduler_async_snapshot_finish");

            let _processing_read_timer = ctx
                .processing_read_duration
                .with_label_values(&[tag])
                .start_coarse_timer();

            let region_id = task.region_id;
            let ts = task.ts;
            let timer = SlowTimer::new();

            let statistics = if readonly {
                self.process_read(ctx, snapshot, task)
            } else {
                self.process_write(ctx, snapshot, task)
            };
            ctx.add_statistics(tag, &statistics);
            slow_log!(
                timer,
                "[region {}] scheduler handle command: {}, ts: {}",
                region_id,
                tag,
                ts
            );
        });
    }

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
    fn process_read(
        mut self,
        sched_ctx: &mut SchedContext<E>,
        snapshot: E::Snap,
        task: Task,
    ) -> Statistics {
        fail_point!("txn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);
        let tag = task.tag;
        let cid = task.cid;
        let mut statistics = Statistics::default();
        let pr = match process_read_impl(sched_ctx, task.cmd, snapshot, &mut statistics) {
            Err(e) => ProcessResult::Failed { err: e.into() },
            Ok(pr) => pr,
        };
        notify_scheduler(self.take_scheduler(), Msg::ReadFinished { cid, pr, tag });
        statistics
    }

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    fn process_write(
        mut self,
        sched_ctx: &SchedContext<E>,
        snapshot: E::Snap,
        task: Task,
    ) -> Statistics {
        fail_point!("txn_before_process_write");
        let tag = task.tag;
        let cid = task.cid;
        let ts = task.ts;
        let mut statistics = Statistics::default();
        let scheduler = self.take_scheduler();
        let waiter_mgr_scheduler = self.take_waiter_mgr_scheduler();
        let detector_scheduler = self.take_detector_scheduler();
        let msg = match process_write_impl(
            task.cmd,
            snapshot,
            waiter_mgr_scheduler,
            detector_scheduler,
            &mut statistics,
        ) {
            // Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
            // message when it finishes.
            Ok(WriteResult {
                ctx,
                to_be_write,
                rows,
                pr,
                lock_info,
            }) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[tag, "write"])
                    .inc();

                if lock_info.is_some() {
                    let (lock, for_update_ts, is_first_lock) = lock_info.unwrap();
                    Msg::PessimisticLockWait {
                        cid,
                        start_ts: ts,
                        for_update_ts,
                        pr,
                        lock,
                        is_first_lock,
                    }
                } else if to_be_write.is_empty() {
                    Msg::WriteFinished {
                        cid,
                        pr,
                        result: Ok(()),
                        tag,
                    }
                } else {
                    let sched = scheduler.clone();
                    // The callback to receive async results of write prepare from the storage engine.
                    let engine_cb = Box::new(move |(_, result)| {
                        if notify_scheduler(
                            sched,
                            Msg::WriteFinished {
                                cid,
                                pr,
                                result,
                                tag,
                            },
                        ) {
                            KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                                .with_label_values(&[tag])
                                .observe(rows as f64);
                        }
                    });

                    if let Err(e) = sched_ctx.engine.async_write(&ctx, to_be_write, engine_cb) {
                        SCHED_STAGE_COUNTER_VEC
                            .with_label_values(&[tag, "async_write_err"])
                            .inc();

                        error!("engine async_write failed"; "cid" => cid, "err" => ?e);
                        let err = e.into();
                        Msg::FinishedWithErr { cid, err, tag }
                    } else {
                        return statistics;
                    }
                }
            }
            // Write prepare failure typically means conflicting transactions are detected. Delivers the
            // error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC
                    .with_label_values(&[tag, "prepare_write_err"])
                    .inc();

                debug!("write command failed at prewrite"; "cid" => cid);
                Msg::FinishedWithErr { cid, err, tag }
            }
        };
        notify_scheduler(scheduler, msg);
        statistics
    }
}

fn process_read_impl<E: Engine>(
    sched_ctx: &mut SchedContext<E>,
    mut cmd: Command,
    snapshot: E::Snap,
    statistics: &mut Statistics,
) -> Result<ProcessResult> {
    let tag = cmd.tag();
    match cmd {
        Command::MvccByKey { ref ctx, ref key } => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                None,
                None,
                ctx.get_isolation_level(),
            );
            let result = find_mvcc_infos_by_key(&mut reader, key, u64::MAX);
            statistics.add(reader.get_statistics());
            let (lock, writes, values) = result?;
            Ok(ProcessResult::MvccKey {
                mvcc: MvccInfo {
                    lock,
                    writes,
                    values,
                },
            })
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
            match reader.seek_ts(start_ts)? {
                Some(key) => {
                    let result = find_mvcc_infos_by_key(&mut reader, &key, u64::MAX);
                    statistics.add(reader.get_statistics());
                    let (lock, writes, values) = result?;
                    Ok(ProcessResult::MvccStartTs {
                        mvcc: Some((
                            key,
                            MvccInfo {
                                lock,
                                writes,
                                values,
                            },
                        )),
                    })
                }
                None => Ok(ProcessResult::MvccStartTs { mvcc: None }),
            }
        }
        // Scans locks with timestamp <= `max_ts`
        Command::ScanLock {
            ref ctx,
            max_ts,
            ref start_key,
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
            let result = reader.scan_locks(start_key.as_ref(), |lock| lock.ts <= max_ts, limit);
            statistics.add(reader.get_statistics());
            let (kv_pairs, _) = result?;
            let mut locks = Vec::with_capacity(kv_pairs.len());
            for (key, lock) in kv_pairs {
                let mut lock_info = LockInfo::new();
                lock_info.set_primary_lock(lock.primary);
                lock_info.set_lock_version(lock.ts);
                lock_info.set_key(key.into_raw()?);
                locks.push(lock_info);
            }
            sched_ctx
                .command_keyread_duration
                .with_label_values(&[tag])
                .observe(locks.len() as f64);
            Ok(ProcessResult::Locks { locks })
        }
        Command::ResolveLock {
            ref ctx,
            ref mut txn_status,
            ref scan_key,
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
            let result = reader.scan_locks(
                scan_key.as_ref(),
                |lock| txn_status.contains_key(&lock.ts),
                RESOLVE_LOCK_BATCH_SIZE,
            );
            statistics.add(reader.get_statistics());
            let (kv_pairs, has_remain) = result?;
            sched_ctx
                .command_keyread_duration
                .with_label_values(&[tag])
                .observe(kv_pairs.len() as f64);
            if kv_pairs.is_empty() {
                Ok(ProcessResult::Res)
            } else {
                let next_scan_key = if has_remain {
                    // There might be more locks.
                    kv_pairs.last().map(|(k, _lock)| k.clone())
                } else {
                    // All locks are scanned
                    None
                };
                Ok(ProcessResult::NextCommand {
                    cmd: Command::ResolveLock {
                        ctx: ctx.clone(),
                        txn_status: mem::replace(txn_status, Default::default()),
                        scan_key: next_scan_key,
                        key_locks: kv_pairs,
                    },
                })
            }
        }
        _ => panic!("unsupported read command"),
    }
}

struct WriteResult {
    ctx: Context,
    to_be_write: Vec<Modify>,
    rows: usize,
    pr: ProcessResult,
    lock_info: Option<(lock_manager::Lock, u64, bool)>,
}

fn process_write_impl<S: Snapshot>(
    cmd: Command,
    snapshot: S,
    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: DetectorScheduler,
    statistics: &mut Statistics,
) -> Result<WriteResult> {
    let (pr, to_be_write, rows, ctx, lock_info) = match cmd {
        Command::Prewrite {
            ctx,
            mutations,
            primary,
            start_ts,
            mut options,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let mut locks = vec![];
            let rows = mutations.len();
            for (i, m) in mutations.into_iter().enumerate() {
                options.prewrite_pessimistic_lock = if options.is_pessimistic_lock.is_empty() {
                    false
                } else {
                    options.is_pessimistic_lock[i]
                };
                match txn.prewrite(m, &primary, &options) {
                    Ok(_) => {}
                    e @ Err(MvccError::KeyIsLocked { .. }) => {
                        locks.push(e.map_err(Error::from).map_err(StorageError::from));
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            if locks.is_empty() {
                let pr = ProcessResult::MultiRes { results: vec![] };
                let modifies = txn.into_modifies();
                (pr, modifies, rows, ctx, None)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::MultiRes { results: locks };
                (pr, vec![], 0, ctx, None)
            }
        }
        Command::PessimisticLock {
            ctx,
            keys,
            primary,
            start_ts,
            for_update_ts,
            options,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let mut locks = vec![];
            let rows = keys.len();
            for k in keys {
                match txn.acquire_pessimistic_lock(k, &primary, for_update_ts, &options) {
                    Ok(_) => {}
                    e @ Err(MvccError::KeyIsLocked { .. }) => {
                        locks.push(e.map_err(Error::from).map_err(StorageError::from));
                        break;
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            // no conflict
            if locks.is_empty() {
                let pr = ProcessResult::MultiRes { results: vec![] };
                let modifies = txn.into_modifies();
                (pr, modifies, rows, ctx, None)
            } else {
                let lock = lock_manager::extract_lock_from_result(&locks[0]);
                let pr = ProcessResult::MultiRes { results: locks };
                // Wait for lock released
                (
                    pr,
                    vec![],
                    0,
                    ctx,
                    Some((lock, for_update_ts, options.is_first_lock)),
                )
            }
        }
        Command::Commit {
            ctx,
            keys,
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
            let mut txn = MvccTxn::new(snapshot, lock_ts, !ctx.get_not_fill_cache())?;
            let key_hashes = lock_manager::gen_key_hashes(&keys);
            let rows = keys.len();
            for k in keys {
                txn.commit(k, commit_ts)?;
            }
            waiter_mgr_scheduler.wake_up(lock_ts, key_hashes, commit_ts);
            detector_scheduler.clean_up(lock_ts);
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx, None)
        }
        Command::Cleanup {
            ctx, key, start_ts, ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let key_hash = lock_manager::gen_key_hash(&key);
            txn.rollback(key)?;

            waiter_mgr_scheduler.wake_up(start_ts, vec![key_hash], 0);
            detector_scheduler.clean_up(start_ts);
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), 1, ctx, None)
        }
        Command::Rollback {
            ctx,
            keys,
            start_ts,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let key_hashes = lock_manager::gen_key_hashes(&keys);
            let rows = keys.len();
            for k in keys {
                txn.rollback(k)?;
            }

            waiter_mgr_scheduler.wake_up(start_ts, key_hashes, 0);
            detector_scheduler.clean_up(start_ts);
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx, None)
        }
        Command::ResolveLock {
            ctx,
            txn_status,
            mut scan_key,
            key_locks,
        } => {
            let mut scan_key = scan_key.take();
            let mut modifies: Vec<Modify> = vec![];
            let mut write_size = 0;
            let rows = key_locks.len();
            let mut ts_to_keys = HashMap::new();
            for (current_key, current_lock) in key_locks {
                ts_to_keys
                    .entry(current_lock.ts)
                    .or_insert(vec![])
                    .push(lock_manager::gen_key_hash(&current_key));

                let mut txn =
                    MvccTxn::new(snapshot.clone(), current_lock.ts, !ctx.get_not_fill_cache())?;
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
                    txn.commit(current_key.clone(), commit_ts)?;
                } else {
                    txn.rollback(current_key.clone())?;
                }
                write_size += txn.write_size();

                statistics.add(&txn.take_statistics());
                modifies.append(&mut txn.into_modifies());

                if write_size >= MAX_TXN_WRITE_SIZE {
                    scan_key = Some(current_key);
                    break;
                }
            }
            ts_to_keys.into_iter().for_each(|(ts, hashes)| {
                waiter_mgr_scheduler.wake_up(ts, hashes, 0);
                detector_scheduler.clean_up(ts);
            });

            let pr = if scan_key.is_none() {
                ProcessResult::Res
            } else {
                ProcessResult::NextCommand {
                    cmd: Command::ResolveLock {
                        ctx: ctx.clone(),
                        txn_status,
                        scan_key: scan_key.take(),
                        key_locks: vec![],
                    },
                }
            };

            (pr, modifies, rows, ctx, None)
        }
        Command::Pause { ctx, duration, .. } => {
            thread::sleep(Duration::from_millis(duration));
            (ProcessResult::Res, vec![], 0, ctx, None)
        }
        _ => panic!("unsupported write command"),
    };

    Ok(WriteResult {
        ctx,
        to_be_write,
        rows,
        pr,
        lock_info,
    })
}

fn notify_scheduler(scheduler: worker::Scheduler<Msg>, msg: Msg) -> bool {
    match scheduler.schedule(msg) {
        Ok(_) => true,
        e @ Err(ScheduleError::Stopped(_)) => {
            info!("scheduler stopped"; "err" => ?e);
            false
        }
        Err(e) => {
            panic!("schedule msg failed, err:{:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct SchedContextFactory<E: Clone> {
    engine: E,
}

impl<E: Clone> SchedContextFactory<E> {
    pub fn new(engine: E) -> SchedContextFactory<E> {
        SchedContextFactory { engine }
    }
}

impl<E: Engine + Clone> ThreadContextFactory<SchedContext<E>> for SchedContextFactory<E> {
    fn create(&self) -> SchedContext<E> {
        SchedContext {
            stats: HashMap::default(),
            processing_read_duration: SCHED_PROCESSING_READ_HISTOGRAM_VEC.local(),
            processing_write_duration: SCHED_PROCESSING_WRITE_HISTOGRAM_VEC.local(),
            command_keyread_duration: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            engine: self.engine.clone(),
        }
    }
}

pub struct SchedContext<E: Engine> {
    stats: HashMap<&'static str, StatisticsSummary>,
    processing_read_duration: LocalHistogramVec,
    processing_write_duration: LocalHistogramVec,
    command_keyread_duration: LocalHistogramVec,
    engine: E,
}

impl<E: Engine> SchedContext<E> {
    fn add_statistics(&mut self, cmd_tag: &'static str, stat: &Statistics) {
        let entry = self.stats.entry(cmd_tag).or_insert_with(Default::default);
        entry.add_statistics(stat);
    }
}

impl<E: Engine> ThreadContext for SchedContext<E> {
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
    }
}

// Make clippy happy.
type MultipleReturnValue = (Option<MvccLock>, Vec<(u64, Write)>, Vec<(u64, Value)>);

fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
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
