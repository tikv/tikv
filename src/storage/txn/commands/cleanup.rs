// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use txn_types::{Key, TimeStamp};

command! {
    /// Rollback mutations on a single key.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Cleanup:
        cmd_ty => (),
        display => "kv::command::cleanup {} @ {} | {:?}", (key, start_ts, ctx),
        content => {
            key: Key,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The approximate current ts when cleanup request is invoked, which is used to check the
            /// lock's TTL. 0 means do not check TTL.
            current_ts: TimeStamp,
        }
}

impl CommandExt for Cleanup {
    ctx!();
    tag!(cleanup);
    ts!(start_ts);
    write_bytes!(key);
    gen_lock!(key);
}
