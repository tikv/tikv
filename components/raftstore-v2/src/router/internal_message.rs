// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use raftstore::store::fsm::ApplyMetrics;

use crate::operation::{AdminCmdResult, CommittedEntries, DataTrace, GenSnapTask};

#[derive(Debug)]
pub enum ApplyTask {
    CommittedEntries(CommittedEntries),
    Snapshot(GenSnapTask),
    /// Writes that doesn't care consistency.
    UnsafeWrite(Box<[u8]>),
    ManualFlush,
}

#[derive(Debug, Default)]
pub struct ApplyRes {
    pub applied_index: u64,
    pub applied_term: u64,
    pub admin_result: Box<[AdminCmdResult]>,
    pub modifications: DataTrace,
    pub metrics: ApplyMetrics,
}
