use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::{Context, TimeStamp};
use crate::{command, ctx, gen_lock, tag, ts, write_bytes};
use std::fmt::{self, Debug, Display, Formatter};
use std::iter;
use txn_types::Key;

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
