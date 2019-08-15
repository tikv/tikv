// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::time::Duration;
use std::{mem, thread, u64};

use futures::future;
use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};

use crate::storage::kv::with_tls_engine;
use crate::storage::kv::{CbContext, Modify, Result as EngineResult};
use crate::storage::lock_manager::{self, LockMgr};
use crate::storage::mvcc::{
    Error as MvccError, Lock as MvccLock, MvccReader, MvccTxn, Write, MAX_TXN_WRITE_SIZE,
};
use crate::storage::txn::{sched_pool::*, scheduler::Msg, Error, Result};
use crate::storage::{
    metrics::*, Command, Engine, Error as StorageError, Key, MvccInfo, Result as StorageResult,
    ScanMode, Snapshot, Statistics, StorageCb, Value,
};
use tikv_util::collections::HashMap;
use tikv_util::time::{Instant, SlowTimer};

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
    TxnStatus { lock_ttl: u64, commit_ts: u64 },
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
        StorageCb::TxnStatus(cb) => match pr {
            ProcessResult::TxnStatus {
                lock_ttl,
                commit_ts,
            } => cb(Ok((lock_ttl, commit_ts))),
            ProcessResult::Failed { err } => cb(Err(err)),
            _ => panic!("process result mismatch"),
        },
    }
}

/// Task is a running command.
pub struct Task {
    pub cid: u64,
    pub tag: CommandKind,

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

pub trait MsgScheduler: Clone + Send + 'static {
    fn on_msg(&self, task: Msg);
}

pub struct Executor<E: Engine, S: MsgScheduler, L: LockMgr> {
    // We put time consuming tasks to the thread pool.
    sched_pool: Option<SchedPool>,
    // And the tasks completes we post a completion to the `Scheduler`.
    scheduler: Option<S>,
    // If the task releases some locks, we wake up waiters waiting for them.
    lock_mgr: Option<L>,

    _phantom: PhantomData<E>,
}

impl<E: Engine, S: MsgScheduler, L: LockMgr> Executor<E, S, L> {
    pub fn new(scheduler: S, pool: SchedPool, lock_mgr: Option<L>) -> Self {
        Executor {
            sched_pool: Some(pool),
            scheduler: Some(scheduler),
            lock_mgr,
            _phantom: Default::default(),
        }
    }

    fn take_pool(&mut self) -> SchedPool {
        self.sched_pool.take().unwrap()
    }

    fn clone_pool(&mut self) -> SchedPool {
        self.sched_pool.clone().unwrap()
    }

    fn take_scheduler(&mut self) -> S {
        self.scheduler.take().unwrap()
    }

    fn take_lock_mgr(&mut self) -> Option<L> {
        self.lock_mgr.take()
    }

    /// Start the execution of the task.
    pub fn execute(mut self, cb_ctx: CbContext, snapshot: EngineResult<E::Snap>, task: Task) {
        debug!(
            "receive snapshot finish msg";
            "cid" => task.cid, "cb_ctx" => ?cb_ctx
        );

        match snapshot {
            Ok(snapshot) => {
                SCHED_STAGE_COUNTER_VEC.get(task.tag).snapshot_ok.inc();

                self.process_by_worker(cb_ctx, snapshot, task);
            }
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(task.tag).snapshot_err.inc();

                info!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                self.take_pool()
                    .pool
                    .spawn(move || {
                        notify_scheduler(
                            self.take_scheduler(),
                            Msg::FinishedWithErr {
                                cid: task.cid,
                                err: Error::from(err),
                                tag: task.tag,
                            },
                        );
                        future::ok::<_, ()>(())
                    })
                    .unwrap();
            }
        }
    }

    /// Delivers a command to a worker thread for processing.
    fn process_by_worker(mut self, cb_ctx: CbContext, snapshot: E::Snap, mut task: Task) {
        SCHED_STAGE_COUNTER_VEC.get(task.tag).process.inc();
        debug!(
            "process cmd with snapshot";
            "cid" => task.cid, "cb_ctx" => ?cb_ctx
        );
        let tag = task.tag;
        if let Some(term) = cb_ctx.term {
            task.cmd.mut_context().set_term(term);
        }
        let sched_pool = self.clone_pool();
        let readonly = task.cmd.readonly();
        sched_pool
            .pool
            .spawn(move || {
                fail_point!("scheduler_async_snapshot_finish");

                let read_duration = Instant::now_coarse();

                let region_id = task.region_id;
                let ts = task.ts;
                let timer = SlowTimer::new();

                let statistics = if readonly {
                    self.process_read(snapshot, task)
                } else {
                    with_tls_engine(|engine| self.process_write(engine, snapshot, task))
                };
                tls_collect_scan_details(tag.get_str(), &statistics);
                slow_log!(
                    timer,
                    "[region {}] scheduler handle command: {}, ts: {}",
                    region_id,
                    tag,
                    ts
                );

                tls_collect_read_duration(tag.get_str(), read_duration.elapsed());
                future::ok::<_, ()>(())
            })
            .unwrap();
    }

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
    fn process_read(mut self, snapshot: E::Snap, task: Task) -> Statistics {
        fail_point!("txn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);
        let tag = task.tag;
        let cid = task.cid;
        let mut statistics = Statistics::default();
        let pr = match process_read_impl::<E>(task.cmd, snapshot, &mut statistics) {
            Err(e) => ProcessResult::Failed { err: e.into() },
            Ok(pr) => pr,
        };
        notify_scheduler(self.take_scheduler(), Msg::ReadFinished { cid, pr, tag });
        statistics
    }

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    fn process_write(mut self, engine: &E, snapshot: E::Snap, task: Task) -> Statistics {
        fail_point!("txn_before_process_write");
        let tag = task.tag;
        let cid = task.cid;
        let ts = task.ts;
        let mut statistics = Statistics::default();
        let scheduler = self.take_scheduler();
        let lock_mgr = self.take_lock_mgr();
        let msg = match process_write_impl(task.cmd, snapshot, lock_mgr, &mut statistics) {
            // Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
            // message when it finishes.
            Ok(WriteResult {
                ctx,
                to_be_write,
                rows,
                pr,
                lock_info,
            }) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

                if lock_info.is_some() {
                    let (lock, is_first_lock) = lock_info.unwrap();
                    Msg::WaitForLock {
                        cid,
                        start_ts: ts,
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
                    let sched_pool = self.take_pool();
                    // The callback to receive async results of write prepare from the storage engine.
                    let engine_cb = Box::new(move |(_, result)| {
                        sched_pool
                            .pool
                            .spawn(move || {
                                notify_scheduler(
                                    sched,
                                    Msg::WriteFinished {
                                        cid,
                                        pr,
                                        result,
                                        tag,
                                    },
                                );
                                KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                                    .get(tag)
                                    .observe(rows as f64);
                                future::ok::<_, ()>(())
                            })
                            .unwrap()
                    });

                    if let Err(e) = engine.async_write(&ctx, to_be_write, engine_cb) {
                        SCHED_STAGE_COUNTER_VEC.get(tag).async_write_err.inc();

                        info!("engine async_write failed"; "cid" => cid, "err" => ?e);
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
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();

                debug!("write command failed at prewrite"; "cid" => cid);
                Msg::FinishedWithErr { cid, err, tag }
            }
        };
        notify_scheduler(scheduler, msg);
        statistics
    }
}

fn process_read_impl<E: Engine>(
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
                let mut lock_info = LockInfo::default();
                lock_info.set_primary_lock(lock.primary);
                lock_info.set_lock_version(lock.ts);
                lock_info.set_key(key.into_raw()?);
                locks.push(lock_info);
            }

            tls_collect_keyread_histogram_vec(tag.get_str(), locks.len() as f64);

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
            tls_collect_keyread_histogram_vec(tag.get_str(), kv_pairs.len() as f64);

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

// If lock_mgr has waiters, there may be some transactions waiting for these keys,
// so calculates keys' hashes to wake up them.
fn gen_key_hashes_if_needed<L: LockMgr>(lock_mgr: &Option<L>, keys: &[Key]) -> Option<Vec<u64>> {
    lock_mgr.as_ref().and_then(|lm| {
        if lm.has_waiter() {
            Some(lock_manager::gen_key_hashes(keys))
        } else {
            None
        }
    })
}

// Wake up pessimistic transactions that waiting for these locks
fn wake_up_waiters_if_needed<L: LockMgr>(
    lock_mgr: &Option<L>,
    lock_ts: u64,
    key_hashes: Option<Vec<u64>>,
    commit_ts: u64,
    is_pessimistic_txn: bool,
) {
    if let Some(lm) = lock_mgr {
        lm.wake_up(lock_ts, key_hashes, commit_ts, is_pessimistic_txn);
    }
}

struct WriteResult {
    ctx: Context,
    to_be_write: Vec<Modify>,
    rows: usize,
    pr: ProcessResult,
    // (lock, is_first_lock)
    lock_info: Option<(lock_manager::Lock, bool)>,
}

fn process_write_impl<S: Snapshot, L: LockMgr>(
    cmd: Command,
    snapshot: S,
    lock_mgr: Option<L>,
    statistics: &mut Statistics,
) -> Result<WriteResult> {
    let (pr, to_be_write, rows, ctx, lock_info) = match cmd {
        Command::Prewrite {
            ctx,
            mutations,
            primary,
            start_ts,
            options,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let mut locks = vec![];
            let rows = mutations.len();

            // If `options.for_update_ts` is 0, the transaction is optimistic
            // or else pessimistic.
            if options.for_update_ts == 0 {
                for m in mutations {
                    match txn.prewrite(m, &primary, &options) {
                        Ok(_) => {}
                        e @ Err(MvccError::KeyIsLocked { .. }) => {
                            locks.push(e.map_err(Error::from).map_err(StorageError::from));
                        }
                        Err(e) => return Err(Error::from(e)),
                    }
                }
            } else {
                for (i, m) in mutations.into_iter().enumerate() {
                    match txn.pessimistic_prewrite(
                        m,
                        &primary,
                        options.is_pessimistic_lock[i],
                        &options,
                    ) {
                        Ok(_) => {}
                        e @ Err(MvccError::KeyIsLocked { .. }) => {
                            locks.push(e.map_err(Error::from).map_err(StorageError::from));
                        }
                        Err(e) => return Err(Error::from(e)),
                    }
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
        Command::AcquirePessimisticLock {
            ctx,
            keys,
            primary,
            start_ts,
            options,
            ..
        } => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let mut locks = vec![];
            let rows = keys.len();
            for (k, should_not_exist) in keys {
                match txn.acquire_pessimistic_lock(k, &primary, should_not_exist, &options) {
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
                (pr, vec![], 0, ctx, Some((lock, options.is_first_lock)))
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
            // Pessimistic txn needs key_hashes to wake up waiters
            let key_hashes = gen_key_hashes_if_needed(&lock_mgr, &keys);

            let mut txn = MvccTxn::new(snapshot, lock_ts, !ctx.get_not_fill_cache())?;
            let mut is_pessimistic_txn = false;
            let rows = keys.len();
            for k in keys {
                is_pessimistic_txn = txn.commit(k, commit_ts)?;
            }

            wake_up_waiters_if_needed(
                &lock_mgr,
                lock_ts,
                key_hashes,
                commit_ts,
                is_pessimistic_txn,
            );
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx, None)
        }
        Command::Cleanup {
            ctx, key, start_ts, ..
        } => {
            let mut keys = vec![key];
            let key_hashes = gen_key_hashes_if_needed(&lock_mgr, &keys);

            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let is_pessimistic_txn = txn.rollback(keys.pop().unwrap())?;

            wake_up_waiters_if_needed(&lock_mgr, start_ts, key_hashes, 0, is_pessimistic_txn);
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), 1, ctx, None)
        }
        Command::Rollback {
            ctx,
            keys,
            start_ts,
            ..
        } => {
            let key_hashes = gen_key_hashes_if_needed(&lock_mgr, &keys);

            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let mut is_pessimistic_txn = false;
            let rows = keys.len();
            for k in keys {
                is_pessimistic_txn = txn.rollback(k)?;
            }

            wake_up_waiters_if_needed(&lock_mgr, start_ts, key_hashes, 0, is_pessimistic_txn);
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx, None)
        }
        Command::PessimisticRollback {
            ctx,
            keys,
            start_ts,
            for_update_ts,
        } => {
            assert!(lock_mgr.is_some());
            let key_hashes = gen_key_hashes_if_needed(&lock_mgr, &keys);

            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let rows = keys.len();
            for k in keys {
                txn.pessimistic_rollback(k, for_update_ts)?;
            }

            wake_up_waiters_if_needed(&lock_mgr, start_ts, key_hashes, 0, true);
            statistics.add(&txn.take_statistics());
            (
                ProcessResult::MultiRes { results: vec![] },
                txn.into_modifies(),
                rows,
                ctx,
                None,
            )
        }
        Command::ResolveLock {
            ctx,
            txn_status,
            mut scan_key,
            key_locks,
        } => {
            // Map (txn's start_ts, is_pessimistic_txn) => Option<key_hashes>
            let (mut txn_to_keys, has_waiter) = if let Some(lm) = lock_mgr.as_ref() {
                (Some(HashMap::default()), lm.has_waiter())
            } else {
                (None, false)
            };

            let mut scan_key = scan_key.take();
            let mut modifies: Vec<Modify> = vec![];
            let mut write_size = 0;
            let rows = key_locks.len();
            for (current_key, current_lock) in key_locks {
                if let Some(txn_to_keys) = txn_to_keys.as_mut() {
                    txn_to_keys
                        .entry((current_lock.ts, current_lock.for_update_ts != 0))
                        .and_modify(|key_hashes: &mut Option<Vec<u64>>| {
                            if let Some(key_hashes) = key_hashes {
                                key_hashes.push(lock_manager::gen_key_hash(&current_key));
                            }
                        })
                        .or_insert_with(|| {
                            if has_waiter {
                                Some(vec![lock_manager::gen_key_hash(&current_key)])
                            } else {
                                None
                            }
                        });
                }

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
            if let Some(txn_to_keys) = txn_to_keys {
                txn_to_keys
                    .into_iter()
                    .for_each(|((ts, is_pessimistic_txn), key_hashes)| {
                        wake_up_waiters_if_needed(&lock_mgr, ts, key_hashes, 0, is_pessimistic_txn);
                    });
            }

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
        Command::ResolveLockLite {
            ctx,
            start_ts,
            commit_ts,
            resolve_keys,
        } => {
            let key_hashes = gen_key_hashes_if_needed(&lock_mgr, &resolve_keys);

            let mut txn = MvccTxn::new(snapshot.clone(), start_ts, !ctx.get_not_fill_cache())?;
            let rows = resolve_keys.len();
            let mut is_pessimistic_txn = false;
            // ti-client guarantees the size of resolve_keys will not too large, so no necessary
            // to control the write_size as ResolveLock.
            for key in resolve_keys {
                if commit_ts > 0 {
                    is_pessimistic_txn = txn.commit(key, commit_ts)?;
                } else {
                    is_pessimistic_txn = txn.rollback(key)?;
                }
            }

            wake_up_waiters_if_needed(
                &lock_mgr,
                start_ts,
                key_hashes,
                commit_ts,
                is_pessimistic_txn,
            );
            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, ctx, None)
        }
        Command::TxnHeartBeat {
            ctx,
            primary_key,
            start_ts,
            advise_ttl,
        } => {
            // TxnHeartBeat never remove locks. No need to wake up waiters.
            let mut txn = MvccTxn::new(snapshot.clone(), start_ts, !ctx.get_not_fill_cache())?;
            let lock_ttl = txn.txn_heart_beat(primary_key, advise_ttl)?;

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus {
                lock_ttl,
                commit_ts: 0,
            };
            (pr, txn.into_modifies(), 1, ctx, None)
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

pub fn notify_scheduler<S: MsgScheduler>(scheduler: S, msg: Msg) {
    scheduler.on_msg(msg);
}

type LockWritesVals = (Option<MvccLock>, Vec<(u64, Write)>, Vec<(u64, Value)>);

fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    mut ts: u64,
) -> Result<LockWritesVals> {
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

#[cfg(test)]
mod test {
    use super::{Executor, MsgScheduler, Task};
    use crate::storage::kv::CbContext;
    use crate::storage::kv::Engine;
    use crate::storage::kv::Result as KvResult;
    use crate::storage::kv::{BTreeEngine, BTreeEngineSnapshot};
    use crate::storage::lock_manager::deadlock::{DetectType, Task as DetectTask};
    use crate::storage::lock_manager::DetectorScheduler;
    use crate::storage::lock_manager::{WaiterMgrScheduler, WaiterTask};
    use crate::storage::mvcc::{Lock, LockType};
    use crate::storage::txn::sched_pool::SchedPool;
    use crate::storage::txn::scheduler::Msg;
    use crate::storage::types::Key;
    use crate::storage::{Command, Mutation, Options};
    use engine::{CF_LOCK, CF_WRITE};
    use futures::sync::mpsc as fmpsc;
    use futures::{stream, Stream};
    use kvproto::kvrpcpb::Context as KvContext;
    use std::iter;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use tikv_util::worker::FutureScheduler;

    #[derive(Clone)]
    struct MockMsgScheduler(MockMsgSchedulerInner);
    type MockMsgSchedulerInner = Arc<Mutex<Option<Box<dyn FnMut(Msg) + Send + 'static>>>>;

    impl MsgScheduler for MockMsgScheduler {
        fn on_msg(&self, task: Msg) {
            let maybe_fn = self.0.lock().unwrap().take();
            if let Some(mut f) = maybe_fn {
                f(task);
            }
        }
    }

    impl MockMsgScheduler {
        fn set_on_msg(&self, f: Box<dyn FnMut(Msg) + Send + 'static>) {
            let mut cb = self.0.lock().unwrap();
            *cb = Some(f);
        }
    }

    fn assert_good_write_msg(msg: Msg) {
        match msg {
            Msg::WriteFinished { .. } => {}
            Msg::FinishedWithErr { err, .. } => {
                println!("{:?}", err);
                panic!("wrong msg");
            }
            err => {
                println!("{:?}", err);
                panic!("wrong msg");
            }
        }
    }

    struct Fixture {
        engine: BTreeEngine,
        pool: SchedPool,
        dead_rx: fmpsc::UnboundedReceiver<Option<DetectTask>>,
        dead_sched: DetectorScheduler,
        _wait_rx: fmpsc::UnboundedReceiver<Option<WaiterTask>>,
        wait_sched: WaiterMgrScheduler,
    }

    // Set up a testing engine and return a test fixture
    fn setup_engine() -> Fixture {
        // Set up the engine and the deadlock detector scheduler
        let cfs = &[CF_LOCK, CF_WRITE];
        let engine = BTreeEngine::new(cfs);
        let pool = SchedPool::new(engine.clone(), 1, "pool");
        // The deadlock detector tasks will arrive here
        let (dead_tx, dead_rx) = fmpsc::unbounded();
        let dead_future_sched = FutureScheduler::new("dead-sched", dead_tx);
        let dead_sched = DetectorScheduler::new(dead_future_sched);
        let (wait_tx, wait_rx) = fmpsc::unbounded();
        let wait_future_sched = FutureScheduler::new("wait-sched", wait_tx);
        let wait_sched = WaiterMgrScheduler::new(wait_future_sched);

        Fixture {
            engine,
            pool,
            dead_rx,
            dead_sched,
            _wait_rx: wait_rx,
            wait_sched,
        }
    }

    // Retrieve a DB snapshot for the command to run against
    fn make_snap(fx: &Fixture) -> BTreeEngineSnapshot {
        let snap: Arc<Mutex<Option<BTreeEngineSnapshot>>> = Arc::new(Mutex::new(None));
        let snap_clone = snap.clone();
        let snap_cb = Box::new(
            move |(_ctxt, new_snap): (CbContext, KvResult<BTreeEngineSnapshot>)| {
                let mut snap = snap_clone.lock().unwrap();
                *snap = Some(new_snap.unwrap());
            },
        );

        let kv_ctxt = KvContext::new();
        fx.engine.async_snapshot(&kv_ctxt, snap_cb).unwrap();

        #[allow(clippy::let_and_return)]
        // It's not possible to do what clippy says here. rust-clippy#1524
        let snap = snap.lock().unwrap().take().unwrap();
        snap
    }

    // Execute a comand and assert that it completes successfully
    fn must_ex_cmd(fx: &Fixture, cmd: Command) {
        // Set up a channel to introspect the results of tasks
        let mock_sched = MockMsgScheduler(Arc::new(Mutex::new(None)));
        let (msg_tx, msg_rx) = mpsc::channel();
        mock_sched.set_on_msg(Box::new(move |msg| {
            msg_tx.send(msg).unwrap();
        }));

        let cmd_task = Task::new(0, cmd);

        // Run the task
        let cb_ctxt = CbContext::new();
        let e: Executor<BTreeEngine, _> = Executor::new(
            mock_sched,
            fx.pool.clone(),
            Some(fx.wait_sched.clone()),
            Some(fx.dead_sched.clone()),
        );
        e.execute(cb_ctxt, KvResult::Ok(make_snap(&fx)), cmd_task);

        // Assert that it completed successfully
        let msg = msg_rx.recv().expect("process sched msg");
        assert_good_write_msg(msg);
    }

    fn must_acquire_pessimistic_lock(fx: &Fixture, keys: &[Key]) {
        let kv_ctxt = KvContext::new();
        let mut opts = Options::default();
        opts.for_update_ts = 1;
        let cmd = Command::AcquirePessimisticLock {
            ctx: kv_ctxt,
            keys: keys.to_owned().into_iter().map(|k| (k, true)).collect(),
            primary: keys[0].as_encoded().clone(),
            start_ts: 1,
            options: opts,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_prewrite_put_pessimistic(fx: &Fixture, kvs: &[(Key, Vec<u8>)]) {
        let kv_ctxt = KvContext::new();
        let mut opts = Options::default();
        opts.for_update_ts = 1;
        opts.is_pessimistic_lock = iter::repeat(true).take(kvs.len()).collect();
        let cmd = Command::Prewrite {
            ctx: kv_ctxt,
            mutations: kvs
                .to_owned()
                .into_iter()
                .map(|kv| Mutation::Put(kv))
                .collect(),
            primary: kvs[0].0.as_encoded().clone(),
            start_ts: 1,
            options: opts,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_commit(fx: &Fixture, keys: &[Key]) {
        let kv_ctxt = KvContext::new();
        let cmd = Command::Commit {
            ctx: kv_ctxt,
            keys: keys.to_owned(),
            lock_ts: 1,
            commit_ts: 2,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_cleanup(fx: &Fixture, key: &Key) {
        let kv_ctxt = KvContext::new();
        let cmd = Command::Cleanup {
            ctx: kv_ctxt,
            key: key.clone(),
            start_ts: 1,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_rollback(fx: &Fixture, keys: &[Key]) {
        let kv_ctxt = KvContext::new();
        let cmd = Command::Rollback {
            ctx: kv_ctxt,
            keys: keys.to_owned(),
            start_ts: 1,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_pessimistic_rollback(fx: &Fixture, keys: &[Key]) {
        let kv_ctxt = KvContext::new();
        let cmd = Command::PessimisticRollback {
            ctx: kv_ctxt,
            keys: keys.to_owned(),
            start_ts: 1,
            for_update_ts: 1,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_pessimistic_resolve_lock(fx: &Fixture, keys: &[Key]) {
        let key_locks = keys
            .iter()
            .map(|k| {
                let lock = Lock::new(LockType::Put, k.to_raw().unwrap(), 1, 10, None, 1, 0);
                (k.clone(), lock)
            })
            .collect();

        let kv_ctxt = KvContext::new();
        let cmd = Command::ResolveLock {
            ctx: kv_ctxt,
            txn_status: [(1, 0)].iter().cloned().collect(),
            scan_key: None,
            key_locks,
        };
        must_ex_cmd(fx, cmd);
    }

    fn must_pessimistic_resolve_lock_lite(fx: &Fixture, keys: &[Key]) {
        let kv_ctxt = KvContext::new();
        let cmd = Command::ResolveLockLite {
            ctx: kv_ctxt,
            start_ts: 1,
            commit_ts: 2,
            resolve_keys: keys.to_owned(),
        };
        must_ex_cmd(fx, cmd);
    }

    fn assert_cleanup_task(dead_task: DetectTask) {
        match dead_task {
            DetectTask::Detect { tp, .. } => {
                assert_eq!(tp, DetectType::CleanUp);
            }
            _ => panic!("wrong task"),
        }
    }

    // Dropping the fixture drops the DetectorSchedule and its tx
    // handle, preventing deadlock
    fn drop_fixture(fx: Fixture) -> fmpsc::UnboundedReceiver<Option<DetectTask>> {
        fx.dead_rx
    }

    fn next_dead_task(
        dead_iter: &mut stream::Wait<fmpsc::UnboundedReceiver<Option<DetectTask>>>,
    ) -> DetectTask {
        let dead_task = dead_iter.next().expect("detector msg");
        dead_task
            .expect("detector task ok")
            .expect("detector task some")
    }

    #[test]
    fn notify_deadlock_detector_on_pessimistic_commit() {
        let fx = setup_engine();

        let keys = vec![Key::from_raw(b"k")];
        let kvs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|k| (k, b"v".to_vec()))
            .collect();

        must_acquire_pessimistic_lock(&fx, &keys);
        must_prewrite_put_pessimistic(&fx, &kvs);
        must_commit(&fx, &keys);

        let mut dead_iter = drop_fixture(fx).wait();
        let dead_task = next_dead_task(&mut dead_iter);

        assert_cleanup_task(dead_task);
        assert!(dead_iter.next().is_none());
    }

    #[test]
    fn notify_deadlock_detector_on_pessimistic_cleanup() {
        let fx = setup_engine();

        let keys = vec![Key::from_raw(b"k")];
        let kvs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|k| (k, b"v".to_vec()))
            .collect();

        must_acquire_pessimistic_lock(&fx, &keys);
        must_prewrite_put_pessimistic(&fx, &kvs);
        must_cleanup(&fx, &keys[0]);

        let mut dead_iter = drop_fixture(fx).wait();
        let dead_task = next_dead_task(&mut dead_iter);

        assert_cleanup_task(dead_task);
        assert!(dead_iter.next().is_none());
    }

    #[test]
    fn notify_deadlock_detector_on_rollback_if_pessimistic() {
        let fx = setup_engine();

        let keys = vec![Key::from_raw(b"k")];
        let kvs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|k| (k, b"v".to_vec()))
            .collect();

        must_acquire_pessimistic_lock(&fx, &keys);
        must_prewrite_put_pessimistic(&fx, &kvs);
        must_rollback(&fx, &keys);

        let mut dead_iter = drop_fixture(fx).wait();
        let dead_task = next_dead_task(&mut dead_iter);

        assert_cleanup_task(dead_task);
        assert!(dead_iter.next().is_none());
    }

    #[test]
    fn notify_deadlock_detector_on_pessimistic_rollback() {
        let fx = setup_engine();

        let keys = vec![Key::from_raw(b"k")];
        let kvs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|k| (k, b"v".to_vec()))
            .collect();

        must_acquire_pessimistic_lock(&fx, &keys);
        must_prewrite_put_pessimistic(&fx, &kvs);
        must_pessimistic_rollback(&fx, &keys);

        let mut dead_iter = drop_fixture(fx).wait();
        let dead_task = next_dead_task(&mut dead_iter);

        assert_cleanup_task(dead_task);
        assert!(dead_iter.next().is_none());
    }

    #[test]
    fn notify_deadlock_detector_on_pessimistic_resolve_lock() {
        let fx = setup_engine();

        let keys = vec![Key::from_raw(b"k")];
        let kvs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|k| (k, b"v".to_vec()))
            .collect();

        must_acquire_pessimistic_lock(&fx, &keys);
        must_prewrite_put_pessimistic(&fx, &kvs);
        must_pessimistic_resolve_lock(&fx, &keys);

        let mut dead_iter = drop_fixture(fx).wait();
        let dead_task = next_dead_task(&mut dead_iter);

        assert_cleanup_task(dead_task);
        assert!(dead_iter.next().is_none());
    }

    #[test]
    fn notify_deadlock_detector_on_pessimistic_resolve_lock_lite() {
        let fx = setup_engine();

        let keys = vec![Key::from_raw(b"k")];
        let kvs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|k| (k, b"v".to_vec()))
            .collect();

        must_acquire_pessimistic_lock(&fx, &keys);
        must_prewrite_put_pessimistic(&fx, &kvs);
        must_pessimistic_resolve_lock_lite(&fx, &keys);

        let mut dead_iter = drop_fixture(fx).wait();
        let dead_task = next_dead_task(&mut dead_iter);

        assert_cleanup_task(dead_task);
        assert!(dead_iter.next().is_none());
    }

}
