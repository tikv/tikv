// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use tikv_util::collections::HashMap;
use txn_types::{Key, TimeStamp};

command! {
    /// Scan locks for resolving according to `txn_status`.
    ///
    /// During the GC operation, this should be called to find out stale locks whose timestamp is
    /// before safe point.
    /// This should follow by a `ResolveLock`
    ScanStaleLocks:
        cmd_ty => (),
        display => "kv::scan_stale_locks", (),
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
        }
}

impl CommandExt for ScanStaleLocks {
    ctx!();
    tag!(resolve_lock);
    command_method!(readonly, bool, true);
    command_method!(write_bytes, usize, 0);
    gen_lock!(empty);
}
