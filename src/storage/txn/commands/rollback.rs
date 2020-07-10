use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::{Context, TimeStamp};
use crate::{command, ctx, gen_lock, tag, ts, write_bytes};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Key;

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
