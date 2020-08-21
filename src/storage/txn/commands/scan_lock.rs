// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::mvcc::MvccReader;
use crate::storage::txn::commands::{Command, CommandExt, ReadCommand, TypedCommand};
use crate::storage::txn::sched_pool::tls_collect_keyread_histogram_vec;
use crate::storage::txn::{LockInfo, ProcessResult, Result};
use crate::storage::{ScanMode, Snapshot, Statistics};
use txn_types::{Key, TimeStamp};

command! {
    /// Scan locks from `start_key`, and find all locks whose timestamp is before `max_ts`.
    ScanLock:
        cmd_ty => Vec<LockInfo>,
        display => "kv::scan_lock {:?} {} @ {} | {:?}", (start_key, limit, max_ts, ctx),
        content => {
            /// The maximum transaction timestamp to scan.
            max_ts: TimeStamp,
            /// The key to start from. (`None` means start from the very beginning.)
            start_key: Option<Key>,
            /// The result limit.
            limit: usize,
        }
}

impl CommandExt for ScanLock {
    ctx!();
    tag!(scan_lock);
    ts!(max_ts);
    command_method!(readonly, bool, true);
    command_method!(is_sys_cmd, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for ScanLock {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new(
            snapshot,
            Some(ScanMode::Forward),
            !self.ctx.get_not_fill_cache(),
            self.ctx.get_isolation_level(),
        );
        let result = reader.scan_locks(
            self.start_key.as_ref(),
            |lock| lock.ts <= self.max_ts,
            self.limit,
        );
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

        tls_collect_keyread_histogram_vec(self.tag().get_str(), locks.len() as f64);

        Ok(ProcessResult::Locks { locks })
    }
}
