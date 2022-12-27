// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    time::Duration,
};

use collections::HashMap;
use engine_traits::{DeleteStrategy, KvEngine, Range, TabletContext, TabletRegistry};
use slog::{warn, Logger};
use tikv_util::worker::{Runnable, RunnableWithTimer};

pub enum Task<EK> {
    Trim {
        tablet: EK,
        start_key: Box<[u8]>,
        end_key: Box<[u8]>,
    },
    PrepareDestroy {
        region_id: u64,
        tablet: EK,
        wait_for_persisted: u64,
    },
    Destroy {
        region_id: u64,
        persisted_index: u64,
    },
}

impl<EK> Display for Task<EK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO
        write!(f, "TabletGcTask")
    }
}

pub struct Runner<EK: KvEngine> {
    tablet_registry: TabletRegistry<EK>,
    logger: Logger,

    pending_trim_tasks: Vec<InternalTrimTask<EK>>,
    // region_id -> [(tablet_path, wait_for_persisted)].
    waiting_destroy_tasks: HashMap<u64, Vec<(PathBuf, u64)>>,
    pending_destroy_tasks: Vec<PathBuf>,
}

struct InternalTrimTask<EK> {
    tablet: EK,
    start_key: Box<[u8]>,
    end_key: Box<[u8]>,
    arrival_seqno: u64,
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(tablet_registry: TabletRegistry<EK>, logger: Logger) -> Self {
        Self {
            tablet_registry,
            logger,
            pending_trim_tasks: Vec::new(),
            waiting_destroy_tasks: HashMap::default(),
            pending_destroy_tasks: Vec::new(),
        }
    }

    /// Returns true if task is consumed. Failure is considered consumed.
    fn process_trim_task(task: &InternalTrimTask<EK>) -> bool {
        if task
            .tablet
            .get_oldest_snapshot_sequence_number()
            .map_or(true, |s| s >= task.arrival_seqno)
        {
            let range1 = Range::new(&[], &task.start_key);
            let range2 = Range::new(&task.end_key, &[0xFF, 0xFF]);
            task.tablet
                .delete_ranges_cfs(DeleteStrategy::DeleteFiles, &[range1, range2])
                .unwrap();
            task.tablet
                .delete_ranges_cfs(DeleteStrategy::DeleteByRange, &[range1, range2])
                .unwrap();
            return true;
        }
        false
    }

    fn prepare_destroy(&mut self, region_id: u64, tablet: EK, wait_for_persisted: u64) {
        // TODO: pause background work?
        self.waiting_destroy_tasks
            .entry(region_id)
            .or_default()
            .push((PathBuf::from(tablet.path()), wait_for_persisted));
    }

    fn destroy(&mut self, region_id: u64, persisted: u64) {
        if let Some(v) = self.waiting_destroy_tasks.get_mut(&region_id) {
            v.retain(|(path, wait)| {
                if *wait <= persisted {
                    if !Self::process_destroy_task(&self.logger, &self.tablet_registry, path) {
                        self.pending_destroy_tasks.push(path.clone());
                    }
                    return false;
                }
                true
            });
        }
    }

    /// Returns true if task is consumed. Failure is considered consumed.
    fn process_destroy_task(logger: &Logger, registry: &TabletRegistry<EK>, path: &Path) -> bool {
        match EK::locked(path.to_str().unwrap()) {
            Err(e) => warn!(
                logger,
                "failed to check whether the tablet path is locked";
                "err" => ?e,
                "path" => path.display(),
            ),
            Ok(false) => {
                // TODO: use a meaningful table context.
                let _ = registry
                    .tablet_factory()
                    .destroy_tablet(TabletContext::with_infinite_region(0, None), path)
                    .map_err(|e| {
                        warn!(
                            logger,
                            "failed to destroy tablet";
                            "err" => ?e,
                            "path" => path.display(),
                        )
                    });
                return true;
            }
            _ => {}
        }
        false
    }
}

impl<EK> Runnable for Runner<EK>
where
    EK: KvEngine,
{
    type Task = Task<EK>;

    fn run(&mut self, task: Task<EK>) {
        match task {
            Task::Trim {
                tablet,
                start_key,
                end_key,
            } => {
                let arrival_seqno = tablet.get_latest_sequence_number();
                self.pending_trim_tasks.push(InternalTrimTask {
                    tablet,
                    start_key,
                    end_key,
                    arrival_seqno,
                });
            }
            Task::PrepareDestroy {
                region_id,
                tablet,
                wait_for_persisted,
            } => self.prepare_destroy(region_id, tablet, wait_for_persisted),
            Task::Destroy {
                region_id,
                persisted_index,
            } => self.destroy(region_id, persisted_index),
        }
    }
}

impl<EK> RunnableWithTimer for Runner<EK>
where
    EK: KvEngine,
{
    fn on_timeout(&mut self) {
        self.pending_trim_tasks
            .retain(|task| !Self::process_trim_task(task));
        self.pending_destroy_tasks
            .retain(|task| !Self::process_destroy_task(&self.logger, &self.tablet_registry, task));
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(2)
    }
}
