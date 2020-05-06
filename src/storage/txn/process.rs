// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::time::Duration;
use std::{mem, thread, u64};

use futures::future;
use kvproto::kvrpcpb::{CommandPri, Context, LockInfo};
use txn_types::{Key, Value};

use crate::storage::kv::{
    with_tls_engine, CbContext, Engine, Modify, Result as EngineResult, ScanMode, Snapshot,
    Statistics,
};
use crate::storage::lock_manager::{self, Lock, LockManager, WaitTimeout};
use crate::storage::mvcc::{
    has_data_in_range, Error as MvccError, ErrorInner as MvccErrorInner, Lock as MvccLock,
    MvccReader, MvccTxn, ReleasedLock, TimeStamp, Write, MAX_TXN_WRITE_SIZE,
};
use crate::storage::txn::{
    commands::{
        AcquirePessimisticLock, CheckTxnStatus, Cleanup, Command, CommandKind, Commit, MvccByKey,
        MvccByStartTs, Pause, PessimisticRollback, Prewrite, PrewritePessimistic, ResolveLock,
        ResolveLockLite, Rollback, ScanLock, TxnHeartBeat,
    },
    sched_pool::*,
    scheduler::Msg,
    Error, ErrorInner, ProcessResult, Result,
};
use crate::storage::{
    metrics::{self, KV_COMMAND_KEYWRITE_HISTOGRAM_VEC, SCHED_STAGE_COUNTER_VEC},
    types::{MvccInfo, PessimisticLockRes, TxnStatus},
    Error as StorageError, ErrorInner as StorageErrorInner, Result as StorageResult,
};
use engine_traits::CF_WRITE;
use tikv_util::collections::HashMap;
use tikv_util::time::Instant;

pub const FORWARD_MIN_MUTATIONS_NUM: usize = 12;

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

/// Task is a running command.
pub struct Task {
    pub cid: u64,
    pub tag: metrics::CommandKind,

    cmd: Command,
    ts: TimeStamp,
    region_id: u64,
}

impl Task {
    /// Creates a task for a running command.
    pub fn new(cid: u64, cmd: Command) -> Task {
        Task {
            cid,
            tag: cmd.tag(),
            region_id: cmd.ctx.get_region_id(),
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
        &self.cmd.ctx
    }
}

pub trait MsgScheduler: Clone + Send + 'static {
    fn on_msg(&self, task: Msg);
}

pub struct Executor<E: Engine, S: MsgScheduler, L: LockManager> {
    // We put time consuming tasks to the thread pool.
    sched_pool: Option<SchedPool>,
    // And the tasks completes we post a completion to the `Scheduler`.
    scheduler: Option<S>,
    // If the task releases some locks, we wake up waiters waiting for them.
    lock_mgr: Option<L>,

    pipelined_pessimistic_lock: bool,

    _phantom: PhantomData<E>,
}

impl<E: Engine, S: MsgScheduler, L: LockManager> Executor<E, S, L> {
    pub fn new(
        scheduler: S,
        pool: SchedPool,
        lock_mgr: Option<L>,
        pipelined_pessimistic_lock: bool,
    ) -> Self {
        Executor {
            sched_pool: Some(pool),
            scheduler: Some(scheduler),
            lock_mgr,
            pipelined_pessimistic_lock,
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
                    .spawn(async move {
                        notify_scheduler(
                            self.take_scheduler(),
                            Msg::FinishedWithErr {
                                cid: task.cid,
                                err: Error::from(err),
                                tag: task.tag,
                            },
                        );
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
            task.cmd.ctx.set_term(term);
        }
        let sched_pool = self.clone_pool();
        let readonly = task.cmd.readonly();
        sched_pool
            .pool
            .spawn(async move {
                fail_point!("scheduler_async_snapshot_finish");

                let read_duration = Instant::now_coarse();

                let region_id = task.region_id;
                let ts = task.ts;
                let timer = Instant::now_coarse();

                let statistics = if readonly {
                    self.process_read(snapshot, task)
                } else {
                    // Safety: `self.sched_pool` ensures a TLS engine exists.
                    unsafe { with_tls_engine(|engine| self.process_write(engine, snapshot, task)) }
                };
                tls_collect_scan_details(tag.get_str(), &statistics);
                slow_log!(
                    timer.elapsed(),
                    "[region {}] scheduler handle command: {}, ts: {}",
                    region_id,
                    tag,
                    ts
                );

                tls_collect_read_duration(tag.get_str(), read_duration.elapsed());
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
        let pipelined = self.pipelined_pessimistic_lock && task.cmd.can_pipelined();
        let msg = match process_write_impl(
            task.cmd,
            snapshot,
            lock_mgr,
            &mut statistics,
            self.pipelined_pessimistic_lock,
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
                SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

                if let Some(lock_info) = lock_info {
                    let (lock, is_first_lock, wait_timeout) = lock_info;
                    Msg::WaitForLock {
                        cid,
                        start_ts: ts,
                        pr,
                        lock,
                        is_first_lock,
                        wait_timeout,
                    }
                } else if to_be_write.is_empty() {
                    Msg::WriteFinished {
                        cid,
                        pr,
                        result: Ok(()),
                        pipelined: false,
                        tag,
                    }
                } else {
                    let sched = scheduler.clone();
                    let sched_pool = self.take_pool();
                    let (write_finished_pr, pipelined_write_pr) = if pipelined {
                        (pr.maybe_clone().unwrap(), pr)
                    } else {
                        (pr, ProcessResult::Res)
                    };
                    // The callback to receive async results of write prepare from the storage engine.
                    let engine_cb = Box::new(move |(_, result)| {
                        sched_pool
                            .pool
                            .spawn(async move {
                                notify_scheduler(
                                    sched,
                                    Msg::WriteFinished {
                                        cid,
                                        pr: write_finished_pr,
                                        result,
                                        pipelined,
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
                    } else if pipelined {
                        // The write task is scheduled to engine successfully.
                        // Respond to client early.
                        Msg::PipelinedWrite {
                            cid,
                            pr: pipelined_write_pr,
                            tag,
                        }
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
    match cmd.kind {
        CommandKind::MvccByKey(MvccByKey { ref key }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !cmd.ctx.get_not_fill_cache(),
                cmd.ctx.get_isolation_level(),
            );
            let result = find_mvcc_infos_by_key(&mut reader, key, TimeStamp::max());
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
        CommandKind::MvccByStartTs(MvccByStartTs { start_ts }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !cmd.ctx.get_not_fill_cache(),
                cmd.ctx.get_isolation_level(),
            );
            match reader.seek_ts(start_ts)? {
                Some(key) => {
                    let result = find_mvcc_infos_by_key(&mut reader, &key, TimeStamp::max());
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
        CommandKind::ScanLock(ScanLock {
            max_ts,
            ref start_key,
            limit,
            ..
        }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !cmd.ctx.get_not_fill_cache(),
                cmd.ctx.get_isolation_level(),
            );
            let result = reader.scan_locks(start_key.as_ref(), |lock| lock.ts <= max_ts, limit);
            statistics.add(reader.get_statistics());
            let (kv_pairs, _) = result?;
            let mut locks = Vec::with_capacity(kv_pairs.len());
            for (key, lock) in kv_pairs {
                let mut lock_info = LockInfo::default();
                lock_info.set_primary_lock(lock.primary);
                lock_info.set_lock_version(lock.ts.into_inner());
                lock_info.set_key(key.into_raw()?);
                lock_info.set_lock_ttl(lock.ttl);
                lock_info.set_txn_size(lock.txn_size);
                locks.push(lock_info);
            }

            tls_collect_keyread_histogram_vec(tag.get_str(), locks.len() as f64);

            Ok(ProcessResult::Locks { locks })
        }
        CommandKind::ResolveLock(ResolveLock {
            ref mut txn_status,
            ref scan_key,
            ..
        }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !cmd.ctx.get_not_fill_cache(),
                cmd.ctx.get_isolation_level(),
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
                    cmd: ResolveLock::new(
                        mem::replace(txn_status, Default::default()),
                        next_scan_key,
                        kv_pairs,
                        cmd.ctx.clone(),
                    )
                    .into(),
                })
            }
        }
        _ => panic!("unsupported read command"),
    }
}

#[derive(Default)]
struct ReleasedLocks {
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    hashes: Vec<u64>,
    pessimistic: bool,
}

impl ReleasedLocks {
    fn new(start_ts: TimeStamp, commit_ts: TimeStamp) -> Self {
        Self {
            start_ts,
            commit_ts,
            ..Default::default()
        }
    }

    fn push(&mut self, lock: Option<ReleasedLock>) {
        if let Some(lock) = lock {
            self.hashes.push(lock.hash);
            if !self.pessimistic {
                self.pessimistic = lock.pessimistic;
            }
        }
    }

    // Wake up pessimistic transactions that waiting for these locks.
    fn wake_up<L: LockManager>(self, lock_mgr: Option<&L>) {
        if let Some(lm) = lock_mgr {
            lm.wake_up(self.start_ts, self.hashes, self.commit_ts, self.pessimistic);
        }
    }
}

fn extract_lock_from_result<T>(res: &StorageResult<T>) -> Lock {
    match res {
        Err(StorageError(box StorageErrorInner::Txn(Error(box ErrorInner::Mvcc(MvccError(
            box MvccErrorInner::KeyIsLocked(info),
        )))))) => Lock {
            ts: info.get_lock_version().into(),
            hash: Key::from_raw(info.get_key()).gen_hash(),
        },
        _ => panic!("unexpected mvcc error"),
    }
}

struct WriteResult {
    ctx: Context,
    to_be_write: Vec<Modify>,
    rows: usize,
    pr: ProcessResult,
    // (lock, is_first_lock, wait_timeout)
    lock_info: Option<(lock_manager::Lock, bool, Option<WaitTimeout>)>,
}

fn process_write_impl<S: Snapshot, L: LockManager>(
    cmd: Command,
    snapshot: S,
    lock_mgr: Option<L>,
    statistics: &mut Statistics,
    pipelined_pessimistic_lock: bool,
) -> Result<WriteResult> {
    let (pr, to_be_write, rows, ctx, lock_info) = match cmd.kind {
        CommandKind::Prewrite(Prewrite {
            mut mutations,
            primary,
            start_ts,
            lock_ttl,
            mut skip_constraint_check,
            txn_size,
            min_commit_ts,
        }) => {
            let mut scan_mode = None;
            let rows = mutations.len();
            if rows > FORWARD_MIN_MUTATIONS_NUM {
                mutations.sort_by(|a, b| a.key().cmp(b.key()));
                let left_key = mutations.first().unwrap().key();
                let right_key = mutations
                    .last()
                    .unwrap()
                    .key()
                    .clone()
                    .append_ts(TimeStamp::zero());
                if !has_data_in_range(
                    snapshot.clone(),
                    CF_WRITE,
                    left_key,
                    &right_key,
                    &mut statistics.write,
                )? {
                    // If there is no data in range, we could skip constraint check, and use Forward seek for CF_LOCK.
                    // Because in most instances, there won't be more than one transaction write the same key. Seek
                    // operation could skip nonexistent key in CF_LOCK.
                    skip_constraint_check = true;
                    scan_mode = Some(ScanMode::Forward)
                }
            }
            let mut txn = if scan_mode.is_some() {
                MvccTxn::for_scan(snapshot, scan_mode, start_ts, !cmd.ctx.get_not_fill_cache())
            } else {
                MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache())
            };

            let mut locks = vec![];
            for m in mutations {
                match txn.prewrite(
                    m,
                    &primary,
                    skip_constraint_check,
                    lock_ttl,
                    txn_size,
                    min_commit_ts,
                ) {
                    Ok(_) => {}
                    e @ Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                        locks.push(e.map_err(Error::from).map_err(StorageError::from));
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            if locks.is_empty() {
                let pr = ProcessResult::MultiRes { results: vec![] };
                let modifies = txn.into_modifies();
                (pr, modifies, rows, cmd.ctx, None)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::MultiRes { results: locks };
                (pr, vec![], 0, cmd.ctx, None)
            }
        }
        CommandKind::PrewritePessimistic(PrewritePessimistic {
            mutations,
            primary,
            start_ts,
            lock_ttl,
            for_update_ts,
            txn_size,
            min_commit_ts,
        }) => {
            let rows = mutations.len();
            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());

            let mut locks = vec![];
            for (m, is_pessimistic_lock) in mutations.into_iter() {
                match txn.pessimistic_prewrite(
                    m,
                    &primary,
                    is_pessimistic_lock,
                    lock_ttl,
                    for_update_ts,
                    txn_size,
                    min_commit_ts,
                    pipelined_pessimistic_lock,
                ) {
                    Ok(_) => {}
                    e @ Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                        locks.push(e.map_err(Error::from).map_err(StorageError::from));
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            if locks.is_empty() {
                let pr = ProcessResult::MultiRes { results: vec![] };
                let modifies = txn.into_modifies();
                (pr, modifies, rows, cmd.ctx, None)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::MultiRes { results: locks };
                (pr, vec![], 0, cmd.ctx, None)
            }
        }
        CommandKind::AcquirePessimisticLock(AcquirePessimisticLock {
            keys,
            primary,
            start_ts,
            lock_ttl,
            is_first_lock,
            for_update_ts,
            wait_timeout,
            return_values,
            min_commit_ts,
            ..
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());
            let rows = keys.len();
            let mut res = if return_values {
                Ok(PessimisticLockRes::Values(vec![]))
            } else {
                Ok(PessimisticLockRes::Empty)
            };
            for (k, should_not_exist) in keys {
                match txn.acquire_pessimistic_lock(
                    k,
                    &primary,
                    should_not_exist,
                    lock_ttl,
                    for_update_ts,
                    return_values,
                    min_commit_ts,
                ) {
                    Ok(val) => {
                        if return_values {
                            res.as_mut().unwrap().push(val);
                        }
                    }
                    Err(e @ MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                        res = Err(e).map_err(Error::from).map_err(StorageError::from);
                        break;
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            // no conflict
            if res.is_ok() {
                let pr = ProcessResult::PessimisticLockRes { res };
                let modifies = txn.into_modifies();
                (pr, modifies, rows, cmd.ctx, None)
            } else {
                let lock = extract_lock_from_result(&res);
                let pr = ProcessResult::PessimisticLockRes { res };
                let lock_info = Some((lock, is_first_lock, wait_timeout));
                // Wait for lock released
                (pr, vec![], 0, cmd.ctx, lock_info)
            }
        }
        CommandKind::Commit(Commit {
            keys,
            lock_ts,
            commit_ts,
            ..
        }) => {
            if commit_ts <= lock_ts {
                return Err(Error::from(ErrorInner::InvalidTxnTso {
                    start_ts: lock_ts,
                    commit_ts,
                }));
            }
            let mut txn = MvccTxn::new(snapshot, lock_ts, !cmd.ctx.get_not_fill_cache());

            let rows = keys.len();
            // Pessimistic txn needs key_hashes to wake up waiters
            let mut released_locks = ReleasedLocks::new(lock_ts, commit_ts);
            for k in keys {
                released_locks.push(txn.commit(k, commit_ts)?);
            }
            released_locks.wake_up(lock_mgr.as_ref());

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus {
                txn_status: TxnStatus::committed(commit_ts),
            };
            (pr, txn.into_modifies(), rows, cmd.ctx, None)
        }
        CommandKind::Cleanup(Cleanup {
            key,
            start_ts,
            current_ts,
            ..
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());

            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            // The rollback must be protected, see more on
            // [issue #7364](https://github.com/tikv/tikv/issues/7364)
            released_locks.push(txn.cleanup(key, current_ts, true)?);
            released_locks.wake_up(lock_mgr.as_ref());

            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), 1, cmd.ctx, None)
        }
        CommandKind::Rollback(Rollback { keys, start_ts, .. }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());

            let rows = keys.len();
            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            for k in keys {
                released_locks.push(txn.rollback(k)?);
            }
            released_locks.wake_up(lock_mgr.as_ref());

            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, cmd.ctx, None)
        }
        CommandKind::PessimisticRollback(PessimisticRollback {
            keys,
            start_ts,
            for_update_ts,
        }) => {
            assert!(lock_mgr.is_some());

            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());

            let rows = keys.len();
            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            for k in keys {
                released_locks.push(txn.pessimistic_rollback(k, for_update_ts)?);
            }
            released_locks.wake_up(lock_mgr.as_ref());

            statistics.add(&txn.take_statistics());
            (
                ProcessResult::MultiRes { results: vec![] },
                txn.into_modifies(),
                rows,
                cmd.ctx,
                None,
            )
        }
        CommandKind::ResolveLock(ResolveLock {
            txn_status,
            mut scan_key,
            key_locks,
        }) => {
            let mut txn = MvccTxn::new(snapshot, TimeStamp::zero(), !cmd.ctx.get_not_fill_cache());

            let mut scan_key = scan_key.take();
            let rows = key_locks.len();
            // Map txn's start_ts to ReleasedLocks
            let mut released_locks = HashMap::default();
            for (current_key, current_lock) in key_locks {
                txn.set_start_ts(current_lock.ts);
                let commit_ts = *txn_status
                    .get(&current_lock.ts)
                    .expect("txn status not found");

                let released = if commit_ts.is_zero() {
                    txn.rollback(current_key.clone())?
                } else if commit_ts > current_lock.ts {
                    txn.commit(current_key.clone(), commit_ts)?
                } else {
                    return Err(Error::from(ErrorInner::InvalidTxnTso {
                        start_ts: current_lock.ts,
                        commit_ts,
                    }));
                };
                released_locks
                    .entry(current_lock.ts)
                    .or_insert_with(|| ReleasedLocks::new(current_lock.ts, commit_ts))
                    .push(released);

                if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                    scan_key = Some(current_key);
                    break;
                }
            }
            released_locks
                .into_iter()
                .for_each(|(_, released_locks)| released_locks.wake_up(lock_mgr.as_ref()));

            statistics.add(&txn.take_statistics());
            let pr = if scan_key.is_none() {
                ProcessResult::Res
            } else {
                ProcessResult::NextCommand {
                    cmd: ResolveLock::new(txn_status, scan_key.take(), vec![], cmd.ctx.clone())
                        .into(),
                }
            };
            (pr, txn.into_modifies(), rows, cmd.ctx, None)
        }
        CommandKind::ResolveLockLite(ResolveLockLite {
            start_ts,
            commit_ts,
            resolve_keys,
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());

            let rows = resolve_keys.len();
            // ti-client guarantees the size of resolve_keys will not too large, so no necessary
            // to control the write_size as ResolveLock.
            let mut released_locks = ReleasedLocks::new(start_ts, commit_ts);
            for key in resolve_keys {
                released_locks.push(if !commit_ts.is_zero() {
                    txn.commit(key, commit_ts)?
                } else {
                    txn.rollback(key)?
                });
            }
            released_locks.wake_up(lock_mgr.as_ref());

            statistics.add(&txn.take_statistics());
            (ProcessResult::Res, txn.into_modifies(), rows, cmd.ctx, None)
        }
        CommandKind::TxnHeartBeat(TxnHeartBeat {
            primary_key,
            start_ts,
            advise_ttl,
        }) => {
            // TxnHeartBeat never remove locks. No need to wake up waiters.
            let mut txn = MvccTxn::new(snapshot, start_ts, !cmd.ctx.get_not_fill_cache());
            let lock_ttl = txn.txn_heart_beat(primary_key, advise_ttl)?;

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus {
                txn_status: TxnStatus::uncommitted(lock_ttl, TimeStamp::zero()),
            };
            (pr, txn.into_modifies(), 1, cmd.ctx, None)
        }
        CommandKind::CheckTxnStatus(CheckTxnStatus {
            primary_key,
            lock_ts,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
        }) => {
            let mut txn = MvccTxn::new(snapshot, lock_ts, !cmd.ctx.get_not_fill_cache());

            let mut released_locks = ReleasedLocks::new(lock_ts, TimeStamp::zero());
            let (txn_status, released) = txn.check_txn_status(
                primary_key,
                caller_start_ts,
                current_ts,
                rollback_if_not_exist,
            )?;
            released_locks.push(released);
            // The lock is released here only when the `check_txn_status` returns `TtlExpire`.
            if let TxnStatus::TtlExpire = txn_status {
                released_locks.wake_up(lock_mgr.as_ref());
            }

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus { txn_status };
            (pr, txn.into_modifies(), 1, cmd.ctx, None)
        }
        CommandKind::Pause(Pause { duration, .. }) => {
            thread::sleep(Duration::from_millis(duration));
            (ProcessResult::Res, vec![], 0, cmd.ctx, None)
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

type LockWritesVals = (
    Option<MvccLock>,
    Vec<(TimeStamp, Write)>,
    Vec<(TimeStamp, Value)>,
);

fn find_mvcc_infos_by_key<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    mut ts: TimeStamp,
) -> Result<LockWritesVals> {
    let mut writes = vec![];
    let mut values = vec![];
    let lock = reader.load_lock(key)?;
    loop {
        let opt = reader.seek_write(key, ts)?;
        match opt {
            Some((commit_ts, write)) => {
                ts = commit_ts.prev();
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
    use super::*;
    use crate::storage::kv::{Snapshot, TestEngineBuilder};
    use crate::storage::{mvcc::Mutation, DummyLockManager};

    #[test]
    fn test_extract_lock_from_result() {
        let raw_key = b"key".to_vec();
        let key = Key::from_raw(&raw_key);
        let ts = 100;
        let mut info = LockInfo::default();
        info.set_key(raw_key);
        info.set_lock_version(ts);
        info.set_lock_ttl(100);
        let case = StorageError::from(StorageErrorInner::Txn(Error::from(ErrorInner::Mvcc(
            MvccError::from(MvccErrorInner::KeyIsLocked(info)),
        ))));
        let lock = extract_lock_from_result::<()>(&Err(case));
        assert_eq!(lock.ts, ts.into());
        assert_eq!(lock.hash, key.gen_hash());
    }

    fn inner_test_prewrite_skip_constraint_check(pri_key_number: u8, write_num: usize) {
        let mut mutations = Vec::default();
        let pri_key = &[pri_key_number];
        for i in 0..write_num {
            mutations.push(Mutation::Insert((
                Key::from_raw(&[i as u8]),
                b"100".to_vec(),
            )));
        }
        let mut statistic = Statistics::default();
        let engine = TestEngineBuilder::new().build().unwrap();
        prewrite(
            &engine,
            &mut statistic,
            vec![Mutation::Put((
                Key::from_raw(&[pri_key_number]),
                b"100".to_vec(),
            ))],
            pri_key.to_vec(),
            99,
        )
        .unwrap();
        assert_eq!(1, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            100,
        )
        .err()
        .unwrap();
        assert_eq!(2, statistic.write.seek);
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::KeyIsLocked(_)))) => (),
            _ => panic!("error type not match"),
        }
        commit(
            &engine,
            &mut statistic,
            vec![Key::from_raw(&[pri_key_number])],
            99,
            102,
        )
        .unwrap();
        assert_eq!(2, statistic.write.seek);
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            101,
        )
        .err()
        .unwrap();
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::WriteConflict {
                ..
            }))) => (),
            _ => panic!("error type not match"),
        }
        let e = prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
        )
        .err()
        .unwrap();
        match e {
            Error(box ErrorInner::Mvcc(MvccError(box MvccErrorInner::AlreadyExist { .. }))) => (),
            _ => panic!("error type not match"),
        }

        statistic.write.seek = 0;
        let ctx = Context::default();
        engine
            .delete_cf(
                &ctx,
                CF_WRITE,
                Key::from_raw(&[pri_key_number]).append_ts(102.into()),
            )
            .unwrap();
        prewrite(
            &engine,
            &mut statistic,
            mutations.clone(),
            pri_key.to_vec(),
            104,
        )
        .unwrap();
        // All keys are prewrited successful with only one seek operations.
        assert_eq!(1, statistic.write.seek);
        let keys: Vec<Key> = mutations.iter().map(|m| m.key().clone()).collect();
        commit(&engine, &mut statistic, keys.clone(), 104, 105).unwrap();
        let snap = engine.snapshot(&ctx).unwrap();
        for k in keys {
            let v = snap.get_cf(CF_WRITE, &k.append_ts(105.into())).unwrap();
            assert!(v.is_some());
        }
    }

    #[test]
    fn test_prewrite_skip_constraint_check() {
        inner_test_prewrite_skip_constraint_check(0, FORWARD_MIN_MUTATIONS_NUM + 1);
        inner_test_prewrite_skip_constraint_check(5, FORWARD_MIN_MUTATIONS_NUM + 1);
        inner_test_prewrite_skip_constraint_check(
            FORWARD_MIN_MUTATIONS_NUM as u8,
            FORWARD_MIN_MUTATIONS_NUM + 1,
        );
    }

    fn prewrite<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        mutations: Vec<Mutation>,
        primary: Vec<u8>,
        start_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let cmd = Prewrite::with_defaults(mutations, primary, TimeStamp::from(start_ts)).into();
        let m = DummyLockManager {};
        let ret = process_write_impl(cmd, snap, Some(m), statistics, false)?;
        if let ProcessResult::MultiRes { results } = ret.pr {
            if !results.is_empty() {
                let info = LockInfo::default();
                return Err(Error::from(ErrorInner::Mvcc(MvccError::from(
                    MvccErrorInner::KeyIsLocked(info),
                ))));
            }
        }
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }

    fn commit<E: Engine>(
        engine: &E,
        statistics: &mut Statistics,
        keys: Vec<Key>,
        lock_ts: u64,
        commit_ts: u64,
    ) -> Result<()> {
        let ctx = Context::default();
        let snap = engine.snapshot(&ctx)?;
        let cmd = Commit::new(
            keys,
            TimeStamp::from(lock_ts),
            TimeStamp::from(commit_ts),
            ctx,
        );
        let m = DummyLockManager {};
        let ret = process_write_impl(cmd.into(), snap, Some(m), statistics, false)?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }
}
