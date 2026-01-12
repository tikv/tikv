// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::{BucketMeta, BucketStat};
use raftstore::store::fsm::ApplyMetrics;

use super::message::CaptureChange;
use crate::operation::{AdminCmdResult, CommittedEntries, DataTrace, GenSnapTask};

#[derive(Debug)]
pub enum ApplyTask {
    CommittedEntries(CommittedEntries),
    Snapshot(GenSnapTask),
    /// Writes that doesn't care consistency.
    UnsafeWrite(Box<[u8]>),
    ManualFlush,
    RefreshBucketStat(std::sync::Arc<BucketMeta>),
    CaptureApply(CaptureChange),
}

#[derive(Debug, Default)]
pub struct ApplyRes {
    pub applied_index: u64,
    pub applied_term: u64,
    pub admin_result: Box<[AdminCmdResult]>,
    pub modifications: DataTrace,
    pub metrics: ApplyMetrics,
    pub bucket_stat: Option<BucketStat>,
    pub sst_applied_index: Vec<SstApplyIndex>,
}

#[derive(Copy, Clone, Debug)]
pub struct SstApplyIndex {
    pub cf_index: usize,
    pub index: u64,
}
