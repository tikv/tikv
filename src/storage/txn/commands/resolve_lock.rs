// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::kv::WriteData;
use crate::storage::lock_manager::LockManager;
use crate::storage::mvcc::{MvccTxn, MAX_TXN_WRITE_SIZE};
use crate::storage::txn::commands::{
    Command, CommandExt, ResolveLockReadPhase, TypedCommand, WriteCommand,
};
use crate::storage::txn::process::{ReleasedLocks, WriteResult};
use crate::storage::txn::Result;
use crate::storage::txn::{Error, ErrorInner};
use crate::storage::{ProcessResult, Snapshot, Statistics};
use kvproto::kvrpcpb::ExtraOp;
use pd_client::PdClient;
use std::sync::Arc;
use tikv_util::collections::HashMap;
use txn_types::{Key, Lock, TimeStamp};

command! {
    /// Resolve locks according to `txn_status`.
    ///
    /// During the GC operation, this should be called to clean up stale locks whose timestamp is
    /// before safe point.
    /// This should follow after a `ResolveLockReadPhase`.
    ResolveLock:
        cmd_ty => (),
        display => "kv::resolve_lock", (),
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

    command_method!(readonly, bool, false);
    command_method!(is_sys_cmd, bool, true);

    fn write_bytes(&self) -> usize {
        self.key_locks
            .iter()
            .map(|(key, _)| key.as_encoded().len())
            .sum()
    }

    gen_lock!(key_locks: multiple(|(key, _)| key));
}

impl<S: Snapshot, L: LockManager, P: PdClient + 'static> WriteCommand<S, L, P> for ResolveLock {
    fn process_write(
        &mut self,
        snapshot: S,
        lock_mgr: &L,
        pd_client: Arc<P>,
        _extra_op: ExtraOp,
        statistics: &mut Statistics,
        _pipelined_pessimistic_lock: bool,
    ) -> Result<WriteResult> {
        let mut txn = MvccTxn::new(
            snapshot,
            TimeStamp::zero(),
            !self.ctx.get_not_fill_cache(),
            pd_client,
        );

        let mut scan_key = self.scan_key.take();
        let rows = self.key_locks.len();
        // Map txn's start_ts to ReleasedLocks
        let mut released_locks = HashMap::default();
        for (current_key, current_lock) in self.key_locks.clone() {
            txn.set_start_ts(current_lock.ts);
            let commit_ts = *self
                .txn_status
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
                cmd: ResolveLockReadPhase::new(
                    self.txn_status.clone(),
                    scan_key.take(),
                    self.ctx.clone(),
                )
                .into(),
            }
        };
        let write_data = WriteData::from_modifies(txn.into_modifies());
        Ok(WriteResult {
            ctx: self.ctx.clone(),
            to_be_write: write_data,
            rows,
            pr,
            lock_info: None,
        })
    }
}
