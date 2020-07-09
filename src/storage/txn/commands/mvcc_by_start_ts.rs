use crate::storage::metrics::{self, KV_COMMAND_COUNTER_VEC_STATIC};
use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::txn::latch::{self, Latches};
use crate::storage::types::MvccInfo;
use crate::storage::Context;
use crate::{command, command_method, ctx, gen_lock, tag, ts};
use std::fmt::{self, Debug, Display, Formatter};
use txn_types::{Key, TimeStamp};

command! {
    /// Retrieve MVCC info for the first committed key which `start_ts == ts`.
    MvccByStartTs:
        cmd_ty => Option<(Key, MvccInfo)>,
        display => "kv::command::mvccbystartts {:?} | {:?}", (start_ts, ctx),
        content => {
            start_ts: TimeStamp,
        }
}

impl CommandExt for MvccByStartTs {
    ctx!();
    tag!(start_ts_mvcc);
    ts!(start_ts);
    command_method!(readonly, bool, true);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}
