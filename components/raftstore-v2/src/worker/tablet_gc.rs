// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    path::PathBuf,
    time::Duration,
};

use engine_traits::{KvEngine, RaftEngine, TabletContext, TabletRegistry};
use slog::{warn, Logger};
use tikv_util::worker::{Runnable, RunnableWithTimer};

pub enum Task {
    /// Deletes all states associated with smaller apply_index.
    GcRaftStates { region: u64, index: u64 },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TabletGcTask")
    }
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    tablet_registry: TabletRegistry<EK>,
    raft_engine: ER,
    logger: Logger,

    pending_tombstone_tablets: Vec<PathBuf>,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(tablet_registry: TabletRegistry<EK>, raft_engine: ER, logger: Logger) -> Self {
        Self {
            tablet_registry,
            raft_engine,
            logger,
            pending_tombstone_tablets: Vec::new(),
        }
    }

    fn gc_tombstone_tablets(&mut self) {
        for tablet in self.tablet_registry.take_tombstone_tablets() {
            self.pending_tombstone_tablets
                .push(PathBuf::from(tablet.path()));
        }
        self.pending_tombstone_tablets.retain(|tablet_path| {
            match EK::locked(tablet_path.to_str().unwrap()) {
                Err(e) => warn!(
                    self.logger,
                    "failed to check whether the tablet path is locked";
                    "err" => ?e,
                    "path" => tablet_path.display(),
                ),
                Ok(false) => {
                    // TODO: use a meaningful table context.
                    let _ = self
                        .tablet_registry
                        .tablet_factory()
                        .destroy_tablet(TabletContext::with_infinite_region(0, None), tablet_path)
                        .map_err(|e| {
                            warn!(
                                self.logger,
                                "failed to destroy tablet";
                                "err" => ?e,
                                "path" => tablet_path.display(),
                            )
                        });
                    return false;
                }
                _ => {}
            }
            true
        });
    }

    fn gc_raft_states(&mut self, region: u64, index: u64) {
        // TODO: batch.
        let mut lb = self.raft_engine.log_batch(128);
        if let Err(e) = self
            .raft_engine
            .delete_all_states_before(region, index, &mut lb)
        {
            warn!(self.logger, "failed to get states for deletion"; "err" => ?e, "region_id" => region);
        } else if let Err(e) = self.raft_engine.consume(&mut lb, false) {
            warn!(self.logger, "failed to delete raft states"; "err" => ?e, "region_id" => region);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::GcRaftStates { region, index } => self.gc_raft_states(region, index),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> RunnableWithTimer for Runner<EK, ER> {
    fn on_timeout(&mut self) {
        self.gc_tombstone_tablets();
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(2)
    }
}
