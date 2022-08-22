// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::storage::{
    mvcc::{Key, MvccReader, MvccTxn, Result as MvccResult, TimeStamp},
    Snapshot,
};

const BATCH_SIZE: usize = 1024;

pub fn flashback_to_version<S: Snapshot>(
    txn: &mut MvccTxn,
    reader: &mut MvccReader<S>,
    version: TimeStamp,
    start_key: Key,
    end_key: Key,
) -> MvccResult<()> {
    Ok(())
}
