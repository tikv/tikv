// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, path::PathBuf, time::Duration};

use engine_traits::{Checkpointer, KvEngine, TabletRegistry};
use futures::channel::oneshot::Sender;
use raftstore::store::RAFT_INIT_LOG_INDEX;
use slog::Logger;
use tikv_util::{slog_panic, time::Instant, worker::Runnable};

use crate::operation::SPLIT_PREFIX;

pub enum Task<EK: KvEngine> {
    Checkpoint {
        // it is only used to assert
        log_index: u64,
        parent_region: u64,
        split_regions: Vec<u64>,
        tablet: EK,
        sender: Sender<Duration>,
    },
}

impl<EK: KvEngine> Display for Task<EK> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::Checkpoint {
                log_index,
                parent_region,
                split_regions,
                ..
            } => write!(
                f,
                "create checkpoint for batch split, parent region_id {}, source region_ids {:?}, log_index {}",
                parent_region, split_regions, log_index,
            ),
        }
    }
}

pub struct Runner<EK: KvEngine> {
    logger: Logger,
    tablet_registry: TabletRegistry<EK>,
}

pub fn temp_split_path<EK>(registry: &TabletRegistry<EK>, region_id: u64) -> PathBuf {
    let tablet_name = registry.tablet_name(SPLIT_PREFIX, region_id, RAFT_INIT_LOG_INDEX);
    registry.tablet_root().join(tablet_name)
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(logger: Logger, tablet_registry: TabletRegistry<EK>) -> Self {
        Self {
            logger,
            tablet_registry,
        }
    }

    fn checkpoint(
        &self,
        parent_region: u64,
        split_regions: Vec<u64>,
        log_index: u64,
        tablet: EK,
        sender: Sender<Duration>,
    ) {
        let now = Instant::now();
        let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
            slog_panic!(
                self.logger,
                "fails to create checkpoint object";
                "region_id" => parent_region,
                "error" => ?e
            )
        });

        for id in split_regions {
            let split_temp_path = temp_split_path(&self.tablet_registry, id);
            checkpointer
                .create_at(&split_temp_path, None, 0)
                .unwrap_or_else(|e| {
                    slog_panic!(
                        self.logger,
                        "fails to create checkpoint";
                        "region_id" => parent_region,
                        "path" => %split_temp_path.display(),
                        "error" => ?e
                    )
                });
        }

        let derived_path = self.tablet_registry.tablet_path(parent_region, log_index);

        // If it's recovered from restart, it's possible the target path exists already.
        // And because checkpoint is atomic, so we don't need to worry about corruption.
        // And it's also wrong to delete it and remake as it may has applied and flushed
        // some data to the new checkpoint before being restarted.
        if !derived_path.exists() {
            checkpointer
                .create_at(&derived_path, None, 0)
                .unwrap_or_else(|e| {
                    slog_panic!(
                        self.logger,
                        "fails to create checkpoint";
                        "region_id" => parent_region,
                        "path" => %derived_path.display(),
                        "error" => ?e
                    )
                });
        }

        sender.send(now.saturating_elapsed()).unwrap();
    }
}

impl<EK: KvEngine> Runnable for Runner<EK> {
    type Task = Task<EK>;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::Checkpoint {
                log_index,
                parent_region,
                split_regions,
                tablet,
                sender,
            } => {
                self.checkpoint(parent_region, split_regions, log_index, tablet, sender);
            }
        }
    }
}
