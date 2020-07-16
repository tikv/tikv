// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::types::MvccInfo;
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
