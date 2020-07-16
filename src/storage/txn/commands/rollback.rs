// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use txn_types::{Key, TimeStamp};

command! {
    /// Rollback from the transaction that was started at `start_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite) on the given key.
    Rollback:
        cmd_ty => (),
        display => "kv::command::rollback keys({}) @ {} | {:?}", (keys.len, start_ts, ctx),
        content => {
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
        }
}

impl CommandExt for Rollback {
    ctx!();
    tag!(rollback);
    ts!(start_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}
