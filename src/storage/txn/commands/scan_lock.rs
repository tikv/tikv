// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::LockInfo;
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
