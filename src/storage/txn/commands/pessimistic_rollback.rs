use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::{Context, Result, TimeStamp};
use crate::{command, command_method, ctx, gen_lock, tag, ts, write_bytes};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Key;

command! {
    /// Rollback pessimistic locks identified by `start_ts` and `for_update_ts`.
    ///
    /// This can roll back an [`AcquirePessimisticLock`](Command::AcquirePessimisticLock) command.
    PessimisticRollback:
        cmd_ty => Vec<Result<()>>,
        display => "kv::command::pessimistic_rollback keys({}) @ {} {} | {:?}", (keys.len, start_ts, for_update_ts, ctx),
        content => {
            /// The keys to be rolled back.
            keys: Vec<Key>,
            /// The transaction timestamp.
            start_ts: TimeStamp,
            for_update_ts: TimeStamp,
        }
}

impl CommandExt for PessimisticRollback {
    ctx!();
    tag!(pessimistic_rollback);
    ts!(start_ts);
    command_method!(requires_pessimistic_txn, bool, true);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}
