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
    ScanMode, Snapshot, Statistics,
};

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
    property!(readonly);

    fn write_bytes(&self) -> usize {
        0
    }

    gen_lock!(empty);
}

impl<S: Snapshot> ReadCommand<S> for MvccByStartTs {
    fn process_read(self, snapshot: S, statistics: &mut Statistics) -> Result<ProcessResult> {
        let mut reader = MvccReader::new_with_ctx(snapshot, Some(ScanMode::Forward), &self.ctx);
        match reader.seek_ts(self.start_ts)? {
            Some(key) => {
                let result = find_mvcc_infos_by_key(&mut reader, &key, TimeStamp::max());
                statistics.add(&reader.statistics);
                let (lock, writes, values) = result?;
                Ok(ProcessResult::MvccStartTs {
                    mvcc: Some((
                        key,
                        MvccInfo {
                            lock,
                            writes,
                            values,
                        },
                    )),
                })
            }
            None => Ok(ProcessResult::MvccStartTs { mvcc: None }),
        }
    }
}
