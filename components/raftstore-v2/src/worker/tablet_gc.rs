// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    time::Duration,
};

use collections::HashMap;
use engine_traits::{DeleteStrategy, KvEngine, Range, TabletContext, TabletRegistry};
use kvproto::metapb::Region;
use slog::{error, warn, Logger};
use tikv_util::worker::{Runnable, RunnableWithTimer};

pub enum Task<EK> {
    Trim {
        tablet: EK,
        start_key: Box<[u8]>,
        end_key: Box<[u8]>,
    },
    PrepareDestroy {
        tablet: EK,
        region_id: u64,
        wait_for_persisted: u64,
    },
    Destroy {
        region_id: u64,
        persisted_index: u64,
    },
}

impl<EK> Display for Task<EK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Trim {
                ref start_key,
                ref end_key,
                ..
            } => write!(
                f,
                "trim tablet for start_key {}, end_key {}",
                log_wrappers::Value::key(start_key),
                log_wrappers::Value::key(end_key),
            ),
            Task::PrepareDestroy {
                region_id,
                wait_for_persisted,
                ..
            } => write!(
                f,
                "prepare destroy tablet for region_id {}, wait_for_persisted {}",
                region_id, wait_for_persisted,
            ),
            Task::Destroy {
                region_id,
                persisted_index,
            } => write!(
                f,
                "destroy tablet for region_id {} persisted_index {}",
                region_id, persisted_index,
            ),
        }
    }
}

impl<EK> Task<EK> {
    #[inline]
    pub fn trim(tablet: EK, region: &Region) -> Self {
        Task::Trim {
            tablet,
            start_key: region.get_start_key().into(),
            end_key: region.get_end_key().into(),
        }
    }

    #[inline]
    pub fn prepare_destroy(tablet: EK, region_id: u64, wait_for_persisted: u64) -> Self {
        Task::PrepareDestroy {
            tablet,
            region_id,
            wait_for_persisted,
        }
    }

    #[inline]
    pub fn destroy(region_id: u64, persisted_index: u64) -> Self {
        Task::Destroy {
            region_id,
            persisted_index,
        }
    }
}

pub struct Runner<EK: KvEngine> {
    tablet_registry: TabletRegistry<EK>,
    logger: Logger,

    // region_id -> [(tablet_path, wait_for_persisted)].
    waiting_destroy_tasks: HashMap<u64, Vec<(PathBuf, u64)>>,
    pending_destroy_tasks: Vec<PathBuf>,
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(tablet_registry: TabletRegistry<EK>, logger: Logger) -> Self {
        Self {
            tablet_registry,
            logger,
            waiting_destroy_tasks: HashMap::default(),
            pending_destroy_tasks: Vec::new(),
        }
    }

    fn trim(tablet: &EK, start_key: &[u8], end_key: &[u8]) -> engine_traits::Result<()> {
        let start_key = keys::data_key(start_key);
        let end_key = keys::data_end_key(end_key);
        let range1 = Range::new(&[], &start_key);
        let range2 = Range::new(&end_key, keys::DATA_MAX_KEY);
        tablet.delete_ranges_cfs(DeleteStrategy::DeleteFiles, &[range1, range2])?;
        // TODO: Avoid this after compaction filter is ready.
        tablet.delete_ranges_cfs(DeleteStrategy::DeleteByRange, &[range1, range2])?;
        for r in [range1, range2] {
            tablet.compact_range(Some(r.start_key), Some(r.end_key), false, 1)?;
        }
        Ok(())
    }

    fn prepare_destroy(&mut self, region_id: u64, tablet: EK, wait_for_persisted: u64) {
        let _ = tablet.pause_background_work();
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
                if let Err(e) = Self::trim(&tablet, &start_key, &end_key) {
                    error!(
                        self.logger,
                        "failed to trim tablet";
                        "start_key" => log_wrappers::Value::key(&start_key),
                        "end_key" => log_wrappers::Value::key(&end_key),
                        "err" => %e,
                    );
                }
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
        self.pending_destroy_tasks
            .retain(|task| !Self::process_destroy_task(&self.logger, &self.tablet_registry, task));
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(2)
    }
}
