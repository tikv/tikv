// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        mpsc::SyncSender,
        Arc,
    },
};

use raft::eraftpb::Snapshot as RaftSnapshot;

/// Region snapshot related task
#[derive(Debug)]
pub enum Task {
    Gen {
        region_id: u64,
        tablet_suffix: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    },
    Apply {
        region_id: u64,
        status: Arc<AtomicUsize>,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        tablet_suffix: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snapshot gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy {
                region_id,
                tablet_suffix,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {}_{} [{}, {})",
                region_id,
                tablet_suffix,
                log_wrappers::Value::key(start_key),
                log_wrappers::Value::key(end_key)
            ),
        }
    }
}
