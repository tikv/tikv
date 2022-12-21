// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    path::PathBuf,
    time::Duration,
};

use engine_traits::{KvEngine, TabletContext, TabletRegistry};
use slog::{warn, Logger};
use tikv_util::worker::{Runnable, RunnableWithTimer};

pub struct Task;

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TabletGcTask")
    }
}

pub struct Runner<EK: KvEngine> {
    tablet_registry: TabletRegistry<EK>,
    logger: Logger,

    pending_tombstone_tablets: Vec<PathBuf>,
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(tablet_registry: TabletRegistry<EK>, logger: Logger) -> Self {
        Self {
            tablet_registry,
            logger,
            pending_tombstone_tablets: Vec::new(),
        }
    }
}

impl<EK> Runnable for Runner<EK>
where
    EK: KvEngine,
{
    type Task = Task;

    fn run(&mut self, _task: Task) {}
}

impl<EK> RunnableWithTimer for Runner<EK>
where
    EK: KvEngine,
{
    fn on_timeout(&mut self) {
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

    fn get_interval(&self) -> Duration {
        Duration::from_secs(2)
    }
}
