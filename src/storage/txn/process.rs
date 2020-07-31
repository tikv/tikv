// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Duration;
use std::{mem, thread, u64};

use engine_traits::CF_WRITE;
use kvproto::kvrpcpb::{Context, ExtraOp, LockInfo};
use pd_client::PdClient;
use tikv_util::collections::HashMap;
use txn_types::{Key, Value};

use crate::storage::kv::{Engine, ScanMode, Snapshot, Statistics, WriteData};
use crate::storage::lock_manager::{self, Lock, LockManager, WaitTimeout};
use crate::storage::mvcc::{
    has_data_in_range, Error as MvccError, ErrorInner as MvccErrorInner, Lock as MvccLock,
    MvccReader, MvccTxn, ReleasedLock, SecondaryLockStatus, TimeStamp, Write, MAX_TXN_WRITE_SIZE,
};
use crate::storage::txn::{
    commands::{
        AcquirePessimisticLock, CheckSecondaryLocks, CheckTxnStatus, Cleanup, Command, Commit,
        MvccByKey, MvccByStartTs, Pause, PessimisticRollback, Prewrite, PrewritePessimistic,
        ResolveLock, ResolveLockLite, ResolveLockReadPhase, Rollback, ScanLock, TxnHeartBeat,
    },
    sched_pool::*,
    Error, ErrorInner, ProcessResult, Result,
};
use crate::storage::{
    types::{MvccInfo, PessimisticLockRes, PrewriteResult, TxnStatus},
    Error as StorageError, ErrorInner as StorageErrorInner, Result as StorageResult,
    SecondaryLocksStatus,
};

// To resolve a key, the write size is about 100~150 bytes, depending on key and value length.
// The write batch will be around 32KB if we scan 256 keys each time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;

const FORWARD_MIN_MUTATIONS_NUM: usize = 12;

pub(super) fn process_read_impl<E: Engine>(
    mut cmd: Command,
    snapshot: E::Snap,
    statistics: &mut Statistics,
) -> Result<ProcessResult> {
    let tag = cmd.tag();
    match cmd {
        Command::MvccByKey(MvccByKey { ref key, ref ctx }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                ctx.get_isolation_level(),
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
        Command::MvccByStartTs(MvccByStartTs { start_ts, ctx }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                ctx.get_isolation_level(),
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
        Command::ScanLock(ScanLock {
            max_ts,
            ref start_key,
            limit,
            ref ctx,
            ..
        }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
                ctx.get_isolation_level(),
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
        Command::ResolveLockReadPhase(ResolveLockReadPhase {
            ref mut txn_status,
            ref scan_key,
            ref ctx,
            ..
        }) => {
            let mut reader = MvccReader::new(
                snapshot,
                Some(ScanMode::Forward),
                !ctx.get_not_fill_cache(),
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
                    cmd: ResolveLock::new(
                        mem::take(txn_status),
                        next_scan_key,
                        kv_pairs,
                        ctx.clone(),
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
    fn wake_up<L: LockManager>(self, lock_mgr: &L) {
        lock_mgr.wake_up(self.start_ts, self.hashes, self.commit_ts, self.pessimistic);
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

pub(super) struct WriteResult {
    pub ctx: Context,
    pub to_be_write: WriteData,
    pub rows: usize,
    pub pr: ProcessResult,
    // (lock, is_first_lock, wait_timeout)
    pub lock_info: Option<(lock_manager::Lock, bool, Option<WaitTimeout>)>,
}

pub(super) fn process_write_impl<S: Snapshot, L: LockManager, P: PdClient + 'static>(
    cmd: Command,
    snapshot: S,
    lock_mgr: &L,
    pd_client: Arc<P>,
    extra_op: ExtraOp,
    statistics: &mut Statistics,
    pipelined_pessimistic_lock: bool,
) -> Result<WriteResult> {
    let (pr, to_be_write, rows, ctx, lock_info) = match cmd {
        Command::Prewrite(Prewrite {
            mut mutations,
            primary,
            start_ts,
            lock_ttl,
            mut skip_constraint_check,
            txn_size,
            min_commit_ts,
            ctx,
            secondary_keys,
            ..
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
                MvccTxn::for_scan(
                    snapshot,
                    scan_mode,
                    start_ts,
                    !ctx.get_not_fill_cache(),
                    pd_client,
                )
            } else {
                MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client)
            };

            // Set extra op here for getting the write record when check write conflict in prewrite.
            txn.extra_op = extra_op;

            let primary_key = Key::from_raw(&primary);
            let mut locks = vec![];
            let mut async_commit_ts = TimeStamp::zero();
            for m in mutations {
                let mut secondaries = &secondary_keys.as_ref().map(|_| vec![]);

                if m.key() == &primary_key {
                    secondaries = &secondary_keys
                }
                match txn.prewrite(
                    m,
                    &primary,
                    secondaries,
                    skip_constraint_check,
                    lock_ttl,
                    txn_size,
                    min_commit_ts,
                ) {
                    Ok(ts) => {
                        if secondaries.is_some() {
                            async_commit_ts = ts;
                        }
                    }
                    e @ Err(MvccError(box MvccErrorInner::KeyIsLocked { .. })) => {
                        locks.push(
                            e.map(|_| ())
                                .map_err(Error::from)
                                .map_err(StorageError::from),
                        );
                    }
                    Err(e) => return Err(Error::from(e)),
                }
            }

            statistics.add(&txn.take_statistics());
            if locks.is_empty() {
                let pr = ProcessResult::PrewriteResult {
                    result: PrewriteResult {
                        locks: vec![],
                        min_commit_ts: async_commit_ts,
                    },
                };
                let txn_extra = txn.take_extra();
                let write_data = WriteData::new(txn.into_modifies(), txn_extra);
                (pr, write_data, rows, ctx, None)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::PrewriteResult {
                    result: PrewriteResult {
                        locks,
                        min_commit_ts: async_commit_ts,
                    },
                };
                (pr, WriteData::default(), 0, ctx, None)
            }
        }
        Command::PrewritePessimistic(PrewritePessimistic {
            mutations,
            primary,
            start_ts,
            lock_ttl,
            for_update_ts,
            txn_size,
            min_commit_ts,
            ctx,
        }) => {
            let rows = mutations.len();
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);
            // Althrough pessimistic prewrite doesn't read the write record for checking conflict, we still set extra op here
            // for getting the written keys.
            txn.extra_op = extra_op;

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
                let pr = ProcessResult::PrewriteResult {
                    result: PrewriteResult {
                        locks: vec![],
                        min_commit_ts: TimeStamp::zero(),
                    },
                };
                let txn_extra = txn.take_extra();
                let write_data = WriteData::new(txn.into_modifies(), txn_extra);
                (pr, write_data, rows, ctx, None)
            } else {
                // Skip write stage if some keys are locked.
                let pr = ProcessResult::PrewriteResult {
                    result: PrewriteResult {
                        locks,
                        min_commit_ts: TimeStamp::zero(),
                    },
                };
                (pr, WriteData::default(), 0, ctx, None)
            }
        }
        Command::AcquirePessimisticLock(AcquirePessimisticLock {
            keys,
            primary,
            start_ts,
            lock_ttl,
            is_first_lock,
            for_update_ts,
            wait_timeout,
            return_values,
            min_commit_ts,
            ctx,
            ..
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);
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
                let write_data = WriteData::from_modifies(txn.into_modifies());
                (pr, write_data, rows, ctx, None)
            } else {
                let lock = extract_lock_from_result(&res);
                let pr = ProcessResult::PessimisticLockRes { res };
                let lock_info = Some((lock, is_first_lock, wait_timeout));
                // Wait for lock released
                (pr, WriteData::default(), 0, ctx, lock_info)
            }
        }
        Command::Commit(Commit {
            keys,
            lock_ts,
            commit_ts,
            ctx,
            ..
        }) => {
            if commit_ts <= lock_ts {
                return Err(Error::from(ErrorInner::InvalidTxnTso {
                    start_ts: lock_ts,
                    commit_ts,
                }));
            }
            let mut txn = MvccTxn::new(snapshot, lock_ts, !ctx.get_not_fill_cache(), pd_client);

            let rows = keys.len();
            // Pessimistic txn needs key_hashes to wake up waiters
            let mut released_locks = ReleasedLocks::new(lock_ts, commit_ts);
            for k in keys {
                released_locks.push(txn.commit(k, commit_ts)?);
            }
            released_locks.wake_up(lock_mgr);

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus {
                txn_status: TxnStatus::committed(commit_ts),
            };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, rows, ctx, None)
        }
        Command::Cleanup(Cleanup {
            key,
            start_ts,
            current_ts,
            ctx,
            ..
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);

            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            // The rollback must be protected, see more on
            // [issue #7364](https://github.com/tikv/tikv/issues/7364)
            released_locks.push(txn.cleanup(key, current_ts, true)?);
            released_locks.wake_up(lock_mgr);

            statistics.add(&txn.take_statistics());
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (ProcessResult::Res, write_data, 1, ctx, None)
        }
        Command::Rollback(Rollback {
            keys,
            start_ts,
            ctx,
            ..
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);

            let rows = keys.len();
            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            for k in keys {
                released_locks.push(txn.rollback(k)?);
            }
            released_locks.wake_up(lock_mgr);

            statistics.add(&txn.take_statistics());
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (ProcessResult::Res, write_data, rows, ctx, None)
        }
        Command::PessimisticRollback(PessimisticRollback {
            keys,
            start_ts,
            for_update_ts,
            ctx,
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);

            let rows = keys.len();
            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            for k in keys {
                released_locks.push(txn.pessimistic_rollback(k, for_update_ts)?);
            }
            released_locks.wake_up(lock_mgr);

            statistics.add(&txn.take_statistics());
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (
                ProcessResult::MultiRes { results: vec![] },
                write_data,
                rows,
                ctx,
                None,
            )
        }
        Command::ResolveLock(ResolveLock {
            txn_status,
            mut scan_key,
            key_locks,
            ctx,
        }) => {
            let mut txn = MvccTxn::new(
                snapshot,
                TimeStamp::zero(),
                !ctx.get_not_fill_cache(),
                pd_client,
            );

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
                .for_each(|(_, released_locks)| released_locks.wake_up(lock_mgr));

            statistics.add(&txn.take_statistics());
            let pr = if scan_key.is_none() {
                ProcessResult::Res
            } else {
                ProcessResult::NextCommand {
                    cmd: ResolveLockReadPhase::new(txn_status, scan_key.take(), ctx.clone()).into(),
                }
            };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, rows, ctx, None)
        }
        Command::ResolveLockLite(ResolveLockLite {
            start_ts,
            commit_ts,
            resolve_keys,
            ctx,
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);

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
            released_locks.wake_up(lock_mgr);

            statistics.add(&txn.take_statistics());
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (ProcessResult::Res, write_data, rows, ctx, None)
        }
        Command::TxnHeartBeat(TxnHeartBeat {
            primary_key,
            start_ts,
            advise_ttl,
            ctx,
        }) => {
            // TxnHeartBeat never remove locks. No need to wake up waiters.
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);
            let lock_ttl = txn.txn_heart_beat(primary_key, advise_ttl)?;

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus {
                txn_status: TxnStatus::uncommitted(lock_ttl, TimeStamp::zero(), false, vec![]),
            };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, 1, ctx, None)
        }
        Command::CheckTxnStatus(CheckTxnStatus {
            primary_key,
            lock_ts,
            caller_start_ts,
            current_ts,
            rollback_if_not_exist,
            ctx,
        }) => {
            let mut txn = MvccTxn::new(snapshot, lock_ts, !ctx.get_not_fill_cache(), pd_client);

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
                released_locks.wake_up(lock_mgr);
            }

            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::TxnStatus { txn_status };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, 1, ctx, None)
        }
        Command::CheckSecondaryLocks(CheckSecondaryLocks {
            keys,
            start_ts,
            ctx,
        }) => {
            let mut txn = MvccTxn::new(snapshot, start_ts, !ctx.get_not_fill_cache(), pd_client);
            let mut released_locks = ReleasedLocks::new(start_ts, TimeStamp::zero());
            let mut result = SecondaryLocksStatus::Locked(Vec::new());

            for key in keys {
                let (status, released) = txn.check_secondary_lock(&key, start_ts)?;
                released_locks.push(released);
                match status {
                    SecondaryLockStatus::Locked(lock) => {
                        result.push(lock.into_lock_info(key.to_raw()?));
                    }
                    SecondaryLockStatus::Committed(commit_ts) => {
                        result = SecondaryLocksStatus::Committed(commit_ts);
                        break;
                    }
                    SecondaryLockStatus::RolledBack => {
                        result = SecondaryLocksStatus::RolledBack;
                        break;
                    }
                }
            }

            let mut rows = 0;
            if let SecondaryLocksStatus::RolledBack = &result {
                // Lock is only released when result is `RolledBack`.
                released_locks.wake_up(lock_mgr);
                // One row is mutated only when a secondary lock is rolled back.
                rows = 1;
            }
            statistics.add(&txn.take_statistics());
            let pr = ProcessResult::SecondaryLocksStatus { status: result };
            let write_data = WriteData::from_modifies(txn.into_modifies());
            (pr, write_data, rows, ctx, None)
        }
        Command::Pause(Pause { duration, ctx, .. }) => {
            thread::sleep(Duration::from_millis(duration));
            (ProcessResult::Res, WriteData::default(), 0, ctx, None)
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
    use pd_client::DummyPdClient;

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
        let ret = process_write_impl(
            cmd,
            snap,
            &DummyLockManager {},
            Arc::new(DummyPdClient::new()),
            ExtraOp::Noop,
            statistics,
            false,
        )?;
        if let ProcessResult::PrewriteResult {
            result: PrewriteResult { locks, .. },
        } = ret.pr
        {
            if !locks.is_empty() {
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

        let ret = process_write_impl(
            cmd.into(),
            snap,
            &DummyLockManager {},
            Arc::new(DummyPdClient::new()),
            ExtraOp::Noop,
            statistics,
            false,
        )?;
        let ctx = Context::default();
        engine.write(&ctx, ret.to_be_write).unwrap();
        Ok(())
    }
}
