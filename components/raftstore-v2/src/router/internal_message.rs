// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    mpsc::SyncSender,
    Arc,
};

use raftstore::store::fsm::GenSnapTask;

pub enum ApplyTask {
    Snapshot(GenSnapTask),
}

#[derive(Debug)]
pub enum ApplyRes {}
