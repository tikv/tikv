// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{MvccReader, MvccTxn, MAX_TXN_WRITE_SIZE};
use crate::storage::txn::commands::{
    Command, CommandExt, ReleasedLocks, TypedCommand, WriteCommand, WriteContext, WriteResult,
};
use crate::storage::txn::{Error, ErrorInner, Result};
use crate::storage::{ProcessResult, ScanMode, Snapshot};
use tikv_util::collections::HashMap;
use txn_types::{Key, TimeStamp};

command! {
    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before safe point.
    /// This should follow after a `ResolveLockReadPhase`.
    ResolveLockScan:
        cmd_ty => (),
        display => "kv::resolve_lock_scan", (),
        content => {
            /// Maps lock_ts to commit_ts. If a transaction was rolled back, it is mapped to 0.
            ///
            /// For example, let `txn_status` be `{ 100: 101, 102: 0 }`, then it means that the transaction
            /// whose start_ts is 100 was committed with commit_ts `101`, and the transaction whose
            /// start_ts is 102 was rolled back. If there are these keys in the db:
            ///
            /// * "k1", lock_ts = 100
            /// * "k2", lock_ts = 102
            /// * "k3", lock_ts = 104
            /// * "k4", no lock
            ///
            /// Here `"k1"`, `"k2"` and `"k3"` each has a not-yet-committed version, because they have
            /// locks. After calling resolve_lock, `"k1"` will be committed with commit_ts = 101 and `"k2"`
            /// will be rolled back.  `"k3"` will not be affected, because its lock_ts is not contained in
            /// `txn_status`. `"k4"` will not be affected either, because it doesn't have a non-committed
            /// version.
            txn_status: HashMap<TimeStamp, TimeStamp>,
            first_scan_key: Option<Key>,
        }
}

impl CommandExt for ResolveLockScan {
    ctx!();
    tag!(resolve_lock);

    command_method!(readonly, bool, false);
    command_method!(is_sys_cmd, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(first_scan_key: multiple); // it is not multiple actually, but Option is also IntoIterator
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for ResolveLockScan {
    fn process_write(self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (ctx, txn_status) = (self.ctx, self.txn_status);
        let lock_mgr = context.lock_mgr;
        let mut reader = MvccReader::new(
            snapshot.clone(),
            Some(ScanMode::Forward),
            !ctx.get_not_fill_cache(),
            ctx.get_isolation_level(),
        );
        let mut txn = MvccTxn::new(
            snapshot,
            TimeStamp::zero(),
            !ctx.get_not_fill_cache(),
            context.concurrency_manager,
        );
        let iter = reader.scan_locks_iter(self.first_scan_key.as_ref())?;
        let mut rows = 0;
        let mut next_first_scan_key = None;
        let mut released_locks = HashMap::default();
        for item in iter {
            let (current_key, current_lock) = item?;
            if !txn_status.contains_key(&current_lock.ts) {
                continue;
            }
            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                next_first_scan_key = Some((current_key, current_lock));
                break;
            }
            let mut lock = context.latches.gen_lock(std::iter::once(&current_key));
            if context.latches.acquire(&mut lock, context.cid) {
                // when latch is acquired successfully, we can resolve the lock
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
                rows += 1;
                context.latches.release(&lock, context.cid);
            } else {
                // else "spawn" another `ResolveLockScan` command which will wait for this (key, lock) group
                next_first_scan_key = Some((current_key, current_lock));
                break;
            }
        }
        reader.statistics.lock.processed += released_locks.len();
        released_locks
            .into_iter()
            .for_each(|(_, released_locks)| released_locks.wake_up(lock_mgr));
        let pr = if next_first_scan_key.is_none() {
            ProcessResult::Res
        } else {
            ProcessResult::NextCommand {
                cmd: ResolveLockScan::new(
                    txn_status,
                    next_first_scan_key.map(|it| it.0).take(),
                    ctx.clone(),
                )
                    .into(),
            }
        };

        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
            lock_guards: vec![],
        })
    }
}
