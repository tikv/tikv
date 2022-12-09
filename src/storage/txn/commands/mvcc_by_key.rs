// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use txn_types::{Key, TimeStamp};

use crate::storage::{
    mvcc::MvccReader,
    txn::{
        commands::{find_mvcc_infos_by_key, Command, CommandExt, ReadCommand, TypedCommand},
        ProcessResult, Result,
    },
    types::MvccInfo,
    Snapshot, Statistics,
};

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
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for MvccByKey {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new_with_ctx(snapshot, None, &self.ctx);
        let result = find_mvcc_infos_by_key(&mut reader, &self.key, TimeStamp::max());
        statistics.add(&reader.statistics);
        let (lock, writes, values) = result?;
        Ok(ProcessResult::MvccKey {
            mvcc: MvccInfo {
                lock,
                writes,
                values,
            },
        })
    }
}
