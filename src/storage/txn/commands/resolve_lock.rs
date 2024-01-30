// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use collections::HashMap;
use txn_types::{Key, Lock, TimeStamp};

use crate::storage::{
    kv::WriteData,
    lock_manager::LockManager,
    mvcc::{
        Error as MvccError, ErrorInner as MvccErrorInner, MvccTxn, SnapshotReader,
        MAX_TXN_WRITE_SIZE,
    },
    txn::{
        cleanup,
        commands::{
            Command, CommandExt, ReaderWithStats, ReleasedLocks, ResolveLockReadPhase,
            ResponsePolicy, TypedCommand, WriteCommand, WriteContext, WriteResult,
        },
        commit, Error, ErrorInner, Result,
    },
    ProcessResult, Snapshot,
};

command! {
    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before safe point.
    /// This should follow after a `ResolveLockReadPhase`.
    ResolveLock:
        cmd_ty => (),
        display => {
            "kv::resolve_lock {:?} scan_key({:?}) key_locks({:?})",
            (txn_status, scan_key, key_locks),
        }
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
            scan_key: Option<Key>,
            key_locks: Vec<(Key, Lock)>,
        }
}

impl CommandExt for ResolveLock {
    ctx!();
    tag!(resolve_lock);
    request_type!(KvResolveLock);
    property!(is_sys_cmd);

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(key_locks: multiple(|(key, _)| key));
}

impl<S: Snapshot, L: LockManager> WriteCommand<S, L> for ResolveLock {
    fn process_write(mut self, snapshot: S, context: WriteContext<'_, L>) -> Result<WriteResult> {
        let (ctx, txn_status, key_locks) = (self.ctx, self.txn_status, self.key_locks);

        let mut txn = MvccTxn::new(TimeStamp::zero(), context.concurrency_manager);
        let mut reader = ReaderWithStats::new(
            SnapshotReader::new_with_ctx(TimeStamp::zero(), snapshot, &ctx),
            context.statistics,
        );

        let mut scan_key = self.scan_key.take();
        let rows = key_locks.len();
        let mut released_locks = ReleasedLocks::new();
        let mut known_txn_status = vec![];
        for (current_key, current_lock) in key_locks {
            txn.start_ts = current_lock.ts;
            reader.start_ts = current_lock.ts;
            let commit_ts = *txn_status
                .get(&current_lock.ts)
                .expect("txn status not found");

            let released = if commit_ts.is_zero() {
                cleanup(
                    &mut txn,
                    &mut reader,
                    current_key.clone(),
                    TimeStamp::zero(),
                    false,
                )?
            } else if commit_ts > current_lock.ts {
                // Continue to resolve locks if the not found committed locks are pessimistic
                // type. They could be left if the transaction is finally committed and
                // pessimistic conflict retry happens during execution.
                match commit(&mut txn, &mut reader, current_key.clone(), commit_ts) {
                    Ok(res) => {
                        known_txn_status.push((current_lock.ts, commit_ts));
                        res
                    }
                    Err(MvccError(box MvccErrorInner::TxnLockNotFound { .. }))
                        if current_lock.is_pessimistic_lock() =>
                    {
                        None
                    }
                    Err(err) => return Err(err.into()),
                }
            } else {
                return Err(Error::from(ErrorInner::InvalidTxnTso {
                    start_ts: current_lock.ts,
                    commit_ts,
                }));
            };
            released_locks.push(released);

            if txn.write_size() >= MAX_TXN_WRITE_SIZE {
                scan_key = Some(current_key);
                break;
            }
        }

        known_txn_status.sort();
        known_txn_status.dedup();

        let pr = if scan_key.is_none() {
            ProcessResult::Res
        } else {
            let next_cmd = ResolveLockReadPhase {
                ctx: ctx.clone(),
                deadline: self.deadline,
                txn_status,
                scan_key,
            };
            ProcessResult::NextCommand {
                cmd: Command::ResolveLockReadPhase(next_cmd),
            }
        };
        let new_acquired_locks = txn.take_new_locks();
        let mut write_data = WriteData::from_modifies(txn.into_modifies());
        write_data.set_allowed_on_disk_almost_full();
        Ok(WriteResult {
            ctx,
            to_be_write: write_data,
            rows,
            pr,
            lock_info: vec![],
            released_locks,
            new_acquired_locks,
            lock_guards: vec![],
            response_policy: ResponsePolicy::OnApplied,
            known_txn_status,
        })
    }
}

// To resolve a key, the write size is about 100~150 bytes, depending on key and
// value length. The write batch will be around 32KB if we scan 256 keys each
// time.
pub const RESOLVE_LOCK_BATCH_SIZE: usize = 256;
