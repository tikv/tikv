use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::Context;
use crate::{command, ctx, gen_lock, tag, write_bytes};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Key;

command! {
    /// **Testing functionality:** Latch the given keys for given duration.
    ///
    /// This means other write operations that involve these keys will be blocked.
    Pause:
        cmd_ty => (),
        display => "kv::command::pause keys:({}) {} ms | {:?}", (keys.len, duration, ctx),
        content => {
            /// The keys to hold latches on.
            keys: Vec<Key>,
            /// The amount of time in milliseconds to latch for.
            duration: u64,
        }
}

impl CommandExt for Pause {
    ctx!();
    tag!(pause);
    write_bytes!(keys: multiple);
    gen_lock!(keys: multiple);
}
