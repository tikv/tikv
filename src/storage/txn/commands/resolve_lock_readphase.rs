// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use collections::HashMap;
use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{Command, CommandExt, ReadCommand, ResolveLock, TypedCommand},
        sched_pool::tls_collect_keyread_histogram_vec,
        ProcessResult, Result, RESOLVE_LOCK_BATCH_SIZE,
    },
    ScanMode, Snapshot, Statistics,
};

command! {
    /// Scan locks for resolving according to `txn_status`.
    ///
    /// During the GC operation, this should be called to find out stale locks whose timestamp is
    /// before safe point.
    /// This should followed by a `ResolveLock`.
    ResolveLockReadPhase:
        cmd_ty => (),
        display => "kv::resolve_lock_readphase", (),
        content => {
            /// Maps lock_ts to commit_ts. See ./resolve_lock.rs for details.
            txn_status: HashMap<TimeStamp, TimeStamp>,
            scan_key: Option<Key>,
        }
}

impl CommandExt for ResolveLockReadPhase {
    ctx!();
    tag!(resolve_lock);
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for ResolveLockReadPhase {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let tag = self.tag();
        let (ctx, txn_status) = (self.ctx, self.txn_status);
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &ctx);
        let result = reader.scan_locks(
            self.scan_key.as_ref(),
            None,
            |lock| txn_status.contains_key(&lock.ts),
            RESOLVE_LOCK_BATCH_SIZE,
        );
        statistics.add(&reader.statistics);
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
            let next_cmd = ResolveLock {
                ctx,
                deadline: self.deadline,
                txn_status,
                scan_key: next_scan_key,
                key_locks: kv_pairs,
            };
            Ok(ProcessResult::NextCommand {
                cmd: Command::ResolveLock(next_cmd),
            })
        }
    }
}
