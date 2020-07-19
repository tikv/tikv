// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use tikv_util::collections::HashMap;
use txn_types::{Key, TimeStamp};

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
    command_method!(readonly, bool, true);
    command_method!(write_bytes, usize, 0);
    gen_lock!(empty);
}
