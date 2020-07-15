// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::txn::commands::{Command, CommandExt, TypedCommand};
use crate::storage::types::MvccInfo;
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
