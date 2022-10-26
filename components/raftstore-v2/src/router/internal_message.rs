// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Arc,
    },
};

use engine_traits::KvEngine;
use raftstore::store::fsm::ChangePeer;

use crate::operation::{AdminCmdResult, CommittedEntries, GenSnapTask};

#[derive(Debug)]
pub enum ApplyTask {
    CommittedEntries(CommittedEntries),
    Snapshot(GenSnapTask),
}

#[derive(Debug, Default)]
pub struct ApplyRes {
    pub applied_index: u64,
    pub applied_term: u64,
    pub admin_result: Vec<AdminCmdResult>,
}
