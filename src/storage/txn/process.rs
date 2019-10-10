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
                    // Safety: `self.sched_pool` ensures a TLS engine exists.
                    unsafe { with_tls_engine(|engine| self.process_write(engine, snapshot, task)) }
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

                if let Some(lock_info) = lock_info {
                    let (lock, is_first_lock) = lock_info;
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
            ctx,
            key,
            start_ts,
            current_ts,
            ..
        } => {
            let mut keys = vec![key];
            let key_hashes = gen_key_hashes_if_needed(&lock_mgr, &keys);

            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache())?;
            let is_pessimistic_txn = txn.cleanup(keys.pop().unwrap(), current_ts)?;

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
mod tests {
    use crate::storage::lock_manager::{gen_key_hash, gen_key_hashes, Lock};
    use crate::storage::txn::ProcessResult;
    use crate::storage::*;
    use kvproto::kvrpcpb::Context;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::*,
        Arc,
    };
    use std::time::Duration;

    pub enum Msg {
        WaitFor {
            start_ts: u64,
            cb: StorageCb,
            pr: ProcessResult,
            lock: Lock,
            is_first_lock: bool,
        },

        WakeUp {
            lock_ts: u64,
            hashes: Option<Vec<u64>>,
            commit_ts: u64,
            is_pessimistic_txn: bool,
        },
    }

    /// `ProxyLockMgr` sends all msgs it received to `Sender`.
    /// It's used to check the correctness of msgs.
    #[derive(Clone)]
    pub struct ProxyLockMgr {
        tx: Sender<Msg>,
        has_waiter: Arc<AtomicBool>,
    }

    impl ProxyLockMgr {
        pub fn new(tx: Sender<Msg>) -> Self {
            Self {
                tx,
                has_waiter: Arc::new(AtomicBool::new(false)),
            }
        }

        pub fn set_has_waiter(&mut self, has_waiter: bool) {
            self.has_waiter.store(has_waiter, Ordering::Relaxed);
        }
    }

    impl LockMgr for ProxyLockMgr {
        fn wait_for(
            &self,
            start_ts: u64,
            cb: StorageCb,
            pr: ProcessResult,
            lock: Lock,
            is_first_lock: bool,
        ) {
            self.tx
                .send(Msg::WaitFor {
                    start_ts,
                    cb,
                    pr,
                    lock,
                    is_first_lock,
                })
                .unwrap();
        }

        fn wake_up(
            &self,
            lock_ts: u64,
            hashes: Option<Vec<u64>>,
            commit_ts: u64,
            is_pessimistic_txn: bool,
        ) {
            self.tx
                .send(Msg::WakeUp {
                    lock_ts,
                    hashes,
                    commit_ts,
                    is_pessimistic_txn,
                })
                .unwrap();
        }

        fn has_waiter(&self) -> bool {
            self.has_waiter.load(Ordering::Relaxed)
        }
    }

    fn expect_ok_callback<T>(done: Sender<()>) -> Callback<T> {
        Box::new(move |x: Result<T>| {
            x.unwrap();
            done.send(()).unwrap();
        })
    }

    fn expect_no_key_hashes(msg: Msg) {
        match msg {
            Msg::WakeUp { hashes, .. } => assert_eq!(hashes, None),
            _ => panic!("unexpected msg"),
        }
    }

    #[test]
    fn test_no_key_hash_when_no_waiter() {
        let (msg_tx, msg_rx) = channel();
        let storage = TestStorageBuilder::from_engine(TestEngineBuilder::new().build().unwrap())
            .lock_mgr(ProxyLockMgr::new(msg_tx))
            .build()
            .unwrap();

        let key = Key::from_raw(b"a");
        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((key.clone(), b"v".to_vec()))],
                key.to_raw().unwrap(),
                10,
                Options::default(),
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_commit(
                Context::default(),
                vec![key.clone()],
                10,
                20,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_no_key_hashes(msg_rx.try_recv().unwrap());

        storage
            .async_cleanup(
                Context::default(),
                key.clone(),
                20,
                0,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_no_key_hashes(msg_rx.try_recv().unwrap());

        storage
            .async_rollback(
                Context::default(),
                vec![key.clone()],
                30,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_no_key_hashes(msg_rx.try_recv().unwrap());

        storage
            .async_pessimistic_rollback(
                Context::default(),
                vec![key.clone()],
                40,
                40,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_no_key_hashes(msg_rx.try_recv().unwrap());

        storage
            .async_prewrite(
                Context::default(),
                vec![
                    Mutation::Put((Key::from_raw(b"a"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"b"), b"foo".to_vec())),
                    Mutation::Put((Key::from_raw(b"c"), b"foo".to_vec())),
                ],
                b"a".to_vec(),
                50,
                Options::default(),
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_resolve_lock_lite(
                Context::default(),
                50,
                0,
                vec![Key::from_raw(b"b")],
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_no_key_hashes(msg_rx.try_recv().unwrap());
        let mut txn_status = HashMap::default();
        txn_status.insert(50, 0);
        storage
            .async_resolve_lock(
                Context::default(),
                txn_status,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        expect_no_key_hashes(msg_rx.try_recv().unwrap());
    }

    #[test]
    fn test_wait_for_validation() {
        let (msg_tx, msg_rx) = channel();
        let storage = TestStorageBuilder::from_engine(TestEngineBuilder::new().build().unwrap())
            .lock_mgr(ProxyLockMgr::new(msg_tx))
            .build()
            .unwrap();

        let key = Key::from_raw(b"a");
        let (tx, rx) = channel();
        storage
            .async_acquire_pessimistic_lock(
                Context::default(),
                vec![(key.clone(), false)],
                key.to_raw().unwrap(),
                10,
                Options::default(),
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        // No wait for msg
        assert!(msg_rx.try_recv().is_err());

        let mut options = Options::default();
        options.is_first_lock = true;
        storage
            .async_acquire_pessimistic_lock(
                Context::default(),
                vec![(key.clone(), false)],
                key.to_raw().unwrap(),
                20,
                options,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        // The transaction should be waiting for lock released so cb won't be called.
        rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
        let msg = msg_rx.try_recv().unwrap();
        // Check msg validation.
        match msg {
            Msg::WaitFor {
                start_ts,
                pr,
                lock,
                is_first_lock,
                ..
            } => {
                assert_eq!(start_ts, 20);
                assert_eq!(
                    lock,
                    Lock {
                        ts: 10,
                        hash: gen_key_hash(&key)
                    }
                );
                assert_eq!(is_first_lock, true);
                match pr {
                    ProcessResult::MultiRes { results } => {
                        assert_eq!(results.len(), 1);
                        match results[0] {
                            Err(Error::Txn(txn::Error::Mvcc(mvcc::Error::KeyIsLocked(_)))) => (),
                            _ => panic!("unexpected error"),
                        }
                    }
                    _ => panic!("unexpected process result"),
                };
            }

            _ => panic!("unexpected msg"),
        }
    }

    fn expected_wake_up_msg(
        msg: Msg,
        expected_lock_ts: u64,
        expected_hashes: Option<Vec<u64>>,
        expected_commit_ts: u64,
        expected_is_pessimistic_txn: bool,
    ) {
        match msg {
            Msg::WakeUp {
                lock_ts,
                hashes,
                commit_ts,
                is_pessimistic_txn,
            } => {
                assert_eq!(lock_ts, expected_lock_ts);
                assert_eq!(hashes, expected_hashes);
                assert_eq!(commit_ts, expected_commit_ts);
                assert_eq!(is_pessimistic_txn, expected_is_pessimistic_txn);
            }
            _ => panic!("unexpected msg"),
        }
    }

    #[test]
    fn test_wake_up_validation() {
        let (msg_tx, msg_rx) = channel();
        let mut lock_mgr = ProxyLockMgr::new(msg_tx);
        lock_mgr.set_has_waiter(true);

        let storage = TestStorageBuilder::from_engine(TestEngineBuilder::new().build().unwrap())
            .lock_mgr(lock_mgr.clone())
            .build()
            .unwrap();

        let keys = vec![
            Key::from_raw(b"a"),
            Key::from_raw(b"b"),
            Key::from_raw(b"c"),
        ];
        let key_hashes = Some(gen_key_hashes(&keys));

        let (tx, rx) = channel();
        storage
            .async_prewrite(
                Context::default(),
                keys.iter()
                    .map(|key| Mutation::Put((key.clone(), b"v".to_vec())))
                    .collect(),
                keys[0].to_raw().unwrap(),
                10,
                Options::default(),
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .async_commit(
                Context::default(),
                keys.clone(),
                10,
                20,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let msg = msg_rx.try_recv().unwrap();
        expected_wake_up_msg(msg, 10, key_hashes.clone(), 20, false);

        storage
            .async_cleanup(
                Context::default(),
                keys[0].clone(),
                30,
                0,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let msg = msg_rx.try_recv().unwrap();
        expected_wake_up_msg(msg, 30, Some(vec![gen_key_hash(&keys[0])]), 0, false);

        storage
            .async_rollback(
                Context::default(),
                keys.clone(),
                40,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let msg = msg_rx.try_recv().unwrap();
        expected_wake_up_msg(msg, 40, key_hashes.clone(), 0, false);

        storage
            .async_pessimistic_rollback(
                Context::default(),
                keys.clone(),
                50,
                50,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let msg = msg_rx.try_recv().unwrap();
        expected_wake_up_msg(msg, 50, key_hashes.clone(), 0, true);

        storage
            .async_resolve_lock_lite(
                Context::default(),
                60,
                0,
                keys.clone(),
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let msg = msg_rx.try_recv().unwrap();
        expected_wake_up_msg(msg, 60, Some(gen_key_hashes(&keys)), 0, false);

        storage
            .async_prewrite(
                Context::default(),
                vec![Mutation::Put((keys[0].clone(), b"v".to_vec()))],
                keys[0].to_raw().unwrap(),
                70,
                Options::default(),
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut options = Options::default();
        options.for_update_ts = 80;
        storage
            .async_acquire_pessimistic_lock(
                Context::default(),
                vec![(keys[1].clone(), false)],
                keys[1].to_raw().unwrap(),
                80,
                options,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        // Commit txn-70 and rollback txn-80
        let mut txn_status = HashMap::default();
        txn_status.insert(70, 71);
        txn_status.insert(80, 0);
        storage
            .async_resolve_lock(
                Context::default(),
                txn_status,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut msg1 = msg_rx.try_recv().unwrap();
        let mut msg2 = msg_rx.try_recv().unwrap();
        // Make sure msgs1 has lock_ts-70.
        match &msg1 {
            Msg::WakeUp { lock_ts, .. } => {
                if *lock_ts != 70 {
                    assert_eq!(*lock_ts, 80);
                    std::mem::swap(&mut msg1, &mut msg2);
                }
            }
            _ => panic!("unexpected msg"),
        }
        expected_wake_up_msg(msg1, 70, Some(vec![gen_key_hash(&keys[0])]), 0, false);
        expected_wake_up_msg(msg2, 80, Some(vec![gen_key_hash(&keys[1])]), 0, true);

        // After setting has_waiter to false, no key hashes.
        lock_mgr.set_has_waiter(false);
        storage
            .async_cleanup(
                Context::default(),
                keys[0].clone(),
                30,
                0,
                expect_ok_callback(tx.clone()),
            )
            .unwrap();
        rx.recv().unwrap();
        let msg = msg_rx.try_recv().unwrap();
        expect_no_key_hashes(msg);
    }
}
