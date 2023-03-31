// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use engine_traits::{KvEngine, TabletRegistry};
use tikv_util::worker::Runnable;

pub enum Task {
    // Compact {},
    CheckAndCompact {
        // Column families need to compact
        cf_names: Vec<String>,
        // Ranges need to check
        ranges: Vec<Key>,
        // The minimum RocksDB tombstones a range that need compacting has
        tombstones_num_threshold: u64,
        tombstones_percent_threshold: u64,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::CheckAndCompact {
                ref cf_names,
                ref ranges,
                tombstones_num_threshold,
                tombstones_percent_threshold,
            } => f
                .debug_struct("CheckAndCompact")
                .field("cf_names", cf_names)
                .field(
                    "ranges",
                    &(
                        ranges.first().as_ref().map(|k| log_wrappers::Value::key(k)),
                        ranges.last().as_ref().map(|k| log_wrappers::Value::key(k)),
                    ),
                )
                .field("tombstones_num_threshold", &tombstones_num_threshold)
                .field(
                    "tombstones_percent_threshold",
                    &tombstones_percent_threshold,
                )
                .finish(),
        }
    }
}

pub struct Runner<E> {
    tablet_registry: TabletRegistry<E>,
}

impl Runner<E>
where
    E: KvEngine,
{
    pub fn new(tablet_registry: TabletRegistry<E>) -> Runner<E> {
        Runner { tablet_registry }
    }
}

impl<E> Runnable for Runner<E>
where
    E: KvEngine,
{
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::CheckAndCompact {
                cf_names,
                ranges,
                tombstones_num_threshold,
                tombstones_percent_threshold,
            } => {
                
            }
        }
    }
}
