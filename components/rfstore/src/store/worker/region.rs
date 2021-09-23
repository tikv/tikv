// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

/// Region related task
#[derive(Debug)]
pub enum Task {
    ApplyChangeSet {
        change: kvenginepb::ChangeSet,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl Task {
    pub fn destroy(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Task {
        Task::Destroy {
            region_id,
            start_key,
            end_key,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Task::ApplyChangeSet { change } => write!(
                f,
                "Snap apply for {}:{}",
                change.get_shard_id(),
                change.get_shard_ver()
            ),
            Task::Destroy {
                region_id,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {} [{}, {})",
                region_id,
                log_wrappers::Value::key(&start_key),
                log_wrappers::Value::key(&end_key)
            ),
        }
    }
}
