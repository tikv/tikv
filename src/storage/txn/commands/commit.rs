use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::{Context, TimeStamp, TxnStatus};
use crate::{command, ctx, gen_lock, tag, ts, write_bytes};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Key;

command! {
    /// Commit the transaction that started at `lock_ts`.
    ///
    /// This should be following a [`Prewrite`](Command::Prewrite).
    Commit:
        cmd_ty => TxnStatus,
        display => "kv::command::commit {} {} -> {} | {:?}", (keys.len, lock_ts, commit_ts, ctx),
        content => {
            /// The keys affected.
            keys: Vec<Key>,
            /// The lock timestamp.
            lock_ts: TimeStamp,
            /// The commit timestamp.
            commit_ts: TimeStamp,
        }
}

impl CommandExt for Commit {
    ctx!();
    tag!(commit);
    ts!(commit_ts);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}
