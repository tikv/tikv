// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::operation::{AdminCmdResult, CommittedEntries, DataTrace, GenSnapTask};

#[derive(Debug)]
pub enum ApplyTask {
    CommittedEntries(CommittedEntries),
    Snapshot(GenSnapTask),
}

#[derive(Debug, Default)]
pub struct ApplyRes {
    pub applied_index: u64,
    pub applied_term: u64,
    pub admin_result: Box<[AdminCmdResult]>,
    pub modifications: DataTrace,
}
