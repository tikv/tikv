use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::types::MvccInfo;
use crate::storage::Context;
use crate::{command, command_method, ctx, gen_lock, tag};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::Key;

command! {
    /// Retrieve MVCC information for the given key.
    MvccByKey:
        cmd_ty => MvccInfo,
        display => "kv::command::mvccbykey {:?} | {:?}", (key, ctx),
        content => {
            key: Key,
        }
}

impl CommandExt for MvccByKey {
    ctx!();
    tag!(key_mvcc);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}
