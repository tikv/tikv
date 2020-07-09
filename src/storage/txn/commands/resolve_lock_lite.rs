use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::{Context, TimeStamp};
use crate::{command, command_method, ctx, gen_lock, tag, ts, write_bytes};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Key;

command! {
    /// Resolve locks on `resolve_keys` according to `start_ts` and `commit_ts`.
    ResolveLockLite:
        cmd_ty => (),
        display => "kv::resolve_lock_lite", (),
        content => {
            /// The transaction timestamp.
            start_ts: TimeStamp,
            /// The transaction commit timestamp.
            commit_ts: TimeStamp,
            /// The keys to resolve.
            resolve_keys: Vec<Key>,
        }
}

impl CommandExt for ResolveLockLite {
    ctx!();
    tag!(resolve_lock_lite);
    ts!(start_ts);
    command_method!(is_sys_cmd, bool, true);
    write_bytes!(resolve_keys: multiple);
    gen_lock!(resolve_keys: multiple);
}
