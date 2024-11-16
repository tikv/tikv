// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use collections::HashMap;
use engine_traits::{
    DeleteStrategy, KvEngine, ManualCompactionOptions, Range, TabletContext, TabletRegistry,
    DATA_CFS,
};
use fail::fail_point;
use kvproto::{import_sstpb::SstMeta, metapb::Region};
use slog::{debug, error, info, warn, Logger};
use sst_importer::SstImporter;
use tikv_util::{
    time::Instant,
    worker::{Runnable, RunnableWithTimer},
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
    Either,
};

const DEFAULT_BACKGROUND_POOL_SIZE: usize = 6;

pub enum Task<EK> {
    Trim {
        tablet: EK,
        start_key: Box<[u8]>,
        end_key: Box<[u8]>,
        cb: Box<dyn FnOnce() + Send>,
    },
    PrepareDestroy {
        // A path is passed only when the db is never opened.
        tablet: Either<EK, PathBuf>,
        region_id: u64,
        wait_for_persisted: u64,
    },
    Destroy {
        region_id: u64,
        persisted_index: u64,
    },
    /// Sometimes we know for sure a tablet can be destroyed directly.
    DirectDestroy { tablet: Either<EK, PathBuf> },
    /// Cleanup ssts.
    CleanupImportSst(Box<[SstMeta]>),
    /// Flush memtable before split
    ///
    /// cb is some iff the task is sent from leader, it is used to real propose
    /// split when flush finishes
    Flush {
        region_id: u64,
        cb: Option<Box<dyn FnOnce() + Send>>,
    },
}

impl<EK> Display for Task<EK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Trim {
                start_key, end_key, ..
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
            Task::DirectDestroy { .. } => {
                write!(f, "direct destroy tablet")
            }
            Task::CleanupImportSst(ssts) => {
                write!(f, "cleanup import ssts {:?}", ssts)
            }
            Task::Flush {
                region_id,
                cb: on_flush_finish,
            } => {
                write!(
                    f,
                    "flush tablet for region_id {}, is leader {}",
                    region_id,
                    on_flush_finish.is_some()
                )
            }
        }
    }
}

impl<EK> Task<EK> {
    #[inline]
    pub fn trim(tablet: EK, region: &Region, cb: impl FnOnce() + Send + 'static) -> Self {
        Task::Trim {
            tablet,
            start_key: region.get_start_key().into(),
            end_key: region.get_end_key().into(),
            cb: Box::new(cb),
        }
    }

    #[inline]
    pub fn prepare_destroy(tablet: EK, region_id: u64, wait_for_persisted: u64) -> Self {
        Task::PrepareDestroy {
            tablet: Either::Left(tablet),
            region_id,
            wait_for_persisted,
        }
    }

    #[inline]
    pub fn prepare_destroy_path(path: PathBuf, region_id: u64, wait_for_persisted: u64) -> Self {
        Task::PrepareDestroy {
            tablet: Either::Right(path),
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

    #[inline]
    pub fn direct_destroy(tablet: EK) -> Self {
        Task::DirectDestroy {
            tablet: Either::Left(tablet),
        }
    }

    #[inline]
    pub fn direct_destroy_path(path: PathBuf) -> Self {
        Task::DirectDestroy {
            tablet: Either::Right(path),
        }
    }
}

pub struct Runner<EK: KvEngine> {
    tablet_registry: TabletRegistry<EK>,
    sst_importer: Arc<SstImporter>,
    logger: Logger,

    // region_id -> [(tablet_path, wait_for_persisted)].
    waiting_destroy_tasks: HashMap<u64, Vec<(PathBuf, u64)>>,
    pending_destroy_tasks: Vec<PathBuf>,

    // An independent pool to run tasks that are time-consuming but doesn't take CPU resources,
    // such as waiting for RocksDB compaction.
    background_pool: FuturePool,
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(
        tablet_registry: TabletRegistry<EK>,
        sst_importer: Arc<SstImporter>,
        logger: Logger,
    ) -> Self {
        Self {
            tablet_registry,
            sst_importer,
            logger,
            waiting_destroy_tasks: HashMap::default(),
            pending_destroy_tasks: Vec::new(),
            background_pool: YatpPoolBuilder::new(DefaultTicker::default())
                .name_prefix("tablet-bg")
                .thread_count(
                    0,
                    DEFAULT_BACKGROUND_POOL_SIZE,
                    DEFAULT_BACKGROUND_POOL_SIZE,
                )
                .build_future_pool(),
        }
    }

    fn trim(&self, tablet: EK, start: Box<[u8]>, end: Box<[u8]>, cb: Box<dyn FnOnce() + Send>) {
        let start_key = keys::data_key(&start);
        let end_key = keys::data_end_key(&end);
        let range1 = Range::new(&[], &start_key);
        let range2 = Range::new(&end_key, keys::DATA_MAX_KEY);
        if let Err(e) = tablet.delete_ranges_cfs(DeleteStrategy::DeleteFiles, &[range1, range2]) {
            error!(
                self.logger,
                "failed to trim tablet";
                "start_key" => log_wrappers::Value::key(&start_key),
                "end_key" => log_wrappers::Value::key(&end_key),
                "err" => %e,
            );
            return;
        }
        let logger = self.logger.clone();
        self.background_pool
            .spawn(async move {
                let range1 = Range::new(&[], &start_key);
                let range2 = Range::new(&end_key, keys::DATA_MAX_KEY);
                for r in [range1, range2] {
                    // When compaction filter is present, trivial move is disallowed.
                    if let Err(e) = tablet.compact_range(
                        Some(r.start_key),
                        Some(r.end_key),
                        ManualCompactionOptions::new(false, 1, false),
                    ) {
                        if e.to_string().contains("Manual compaction paused") {
                            info!(
                                logger,
                                "tablet manual compaction is paused, skip trim";
                                "start_key" => log_wrappers::Value::key(&start_key),
                                "end_key" => log_wrappers::Value::key(&end_key),
                                "err" => %e,
                            );
                        } else {
                            error!(
                                logger,
                                "failed to trim tablet";
                                "start_key" => log_wrappers::Value::key(&start_key),
                                "end_key" => log_wrappers::Value::key(&end_key),
                                "err" => %e,
                            );
                        }
                        return;
                    }
                }
                // drop before callback.
                drop(tablet);
                fail_point!("tablet_trimmed_finished");
                cb();
            })
            .unwrap();
    }

    fn pause_background_work(&mut self, tablet: Either<EK, PathBuf>) -> PathBuf {
        match tablet {
            Either::Left(tablet) => {
                // The tablet is about to be deleted, flush is a waste and will block destroy.
                let _ = tablet.set_db_options(&[("avoid_flush_during_shutdown", "true")]);
                let _ = tablet.pause_background_work();
                PathBuf::from(tablet.path())
            }
            Either::Right(path) => path,
        }
    }

    fn prepare_destroy(
        &mut self,
        region_id: u64,
        tablet: Either<EK, PathBuf>,
        wait_for_persisted: u64,
    ) {
        let path = self.pause_background_work(tablet);
        self.waiting_destroy_tasks
            .entry(region_id)
            .or_default()
            .push((path, wait_for_persisted));
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

    fn direct_destroy(&mut self, tablet: Either<EK, PathBuf>) {
        let path = self.pause_background_work(tablet);
        if !Self::process_destroy_task(&self.logger, &self.tablet_registry, &path) {
            self.pending_destroy_tasks.push(path);
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
                let (_, region_id, tablet_index) =
                    registry.parse_tablet_name(path).unwrap_or(("", 0, 0));
                // TODO: use a meaningful table context.
                let _ = registry
                    .tablet_factory()
                    .destroy_tablet(
                        TabletContext::with_infinite_region(region_id, Some(tablet_index)),
                        path,
                    )
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
            Ok(true) => {
                debug!(logger, "ignore locked tablet"; "path" => path.display());
            }
        }
        false
    }

    fn cleanup_ssts(&self, ssts: Box<[SstMeta]>) {
        for sst in Vec::from(ssts) {
            if let Err(e) = self.sst_importer.delete(&sst) {
                warn!(self.logger, "failed to cleanup sst"; "err" => ?e, "sst" => ?sst);
            }
        }
    }

    fn flush_tablet(&self, region_id: u64, cb: Option<Box<dyn FnOnce() + Send>>) {
        let Some(Some(tablet)) = self
            .tablet_registry
            .get(region_id)
            .map(|mut cache| cache.latest().cloned()) else {return};

        // The callback `cb` being some means it's the task sent from
        // leader, we should sync flush memtables and call it after the flush complete
        // where the split will be proposed again with extra flag.
        if let Some(cb) = cb {
            let logger = self.logger.clone();
            let now = Instant::now();
            self.background_pool
                .spawn(async move {
                    // sync flush for leader to let the flush happend before later checkpoint.
                    tablet.flush_cfs(DATA_CFS, true).unwrap();
                    let elapsed = now.saturating_elapsed();
                    // to be removed after when it's stable
                    info!(
                        logger,
                        "flush memtable for leader";
                        "region_id" => region_id,
                        "duration" => ?elapsed,
                    );

                    drop(tablet);
                    cb();
                })
                .unwrap();
        } else {
            info!(
                self.logger,
                "flush memtable for follower";
                "region_id" => region_id,
            );

            tablet.flush_cfs(DATA_CFS, false).unwrap();
        }
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
                cb,
            } => self.trim(tablet, start_key, end_key, cb),
            Task::PrepareDestroy {
                region_id,
                tablet,
                wait_for_persisted,
            } => self.prepare_destroy(region_id, tablet, wait_for_persisted),
            Task::Destroy {
                region_id,
                persisted_index,
            } => self.destroy(region_id, persisted_index),
            Task::DirectDestroy { tablet, .. } => self.direct_destroy(tablet),
            Task::CleanupImportSst(ssts) => self.cleanup_ssts(ssts),
            Task::Flush { region_id, cb } => self.flush_tablet(region_id, cb),
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
        Duration::from_secs(10)
    }
}

#[cfg(test)]
mod tests {
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactory,
    };
    use engine_traits::{MiscExt, TabletContext, TabletRegistry};
    use tempfile::Builder;

    use super::*;
    use crate::operation::test_util::create_tmp_importer;

    #[test]
    fn test_race_between_destroy_and_trim() {
        let dir = Builder::new()
            .prefix("test_race_between_destroy_and_trim")
            .tempdir()
            .unwrap();
        let factory = Box::new(TestTabletFactory::new(
            DbOptions::default(),
            vec![("default", CfOptions::default())],
        ));
        let registry = TabletRegistry::new(factory, dir.path()).unwrap();
        let logger = slog_global::borrow_global().new(slog::o!());
        let (_dir, importer) = create_tmp_importer();
        let mut runner = Runner::new(registry.clone(), importer, logger);

        let mut region = Region::default();
        let rid = 1;
        region.set_id(rid);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"b".to_vec());
        let tablet = registry
            .load(TabletContext::new(&region, Some(1)), true)
            .unwrap()
            .latest()
            .unwrap()
            .clone();
        runner.run(Task::prepare_destroy(tablet.clone(), rid, 10));
        let (tx, rx) = std::sync::mpsc::channel();
        runner.run(Task::trim(tablet, &region, move || tx.send(()).unwrap()));
        rx.recv().unwrap();

        let rid = 2;
        region.set_id(rid);
        region.set_start_key(b"c".to_vec());
        region.set_end_key(b"d".to_vec());
        let tablet = registry
            .load(TabletContext::new(&region, Some(1)), true)
            .unwrap()
            .latest()
            .unwrap()
            .clone();
        registry.remove(rid);
        runner.run(Task::prepare_destroy(tablet.clone(), rid, 10));
        runner.run(Task::destroy(rid, 100));
        let path = PathBuf::from(tablet.path());
        assert!(path.exists());
        let (tx, rx) = std::sync::mpsc::channel();
        runner.run(Task::trim(tablet, &region, move || tx.send(()).unwrap()));
        rx.recv().unwrap();
        runner.on_timeout();
        assert!(!path.exists());
    }
}
