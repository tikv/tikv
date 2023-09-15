// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use collections::HashMap;
use engine_traits::{
    CfName, DeleteStrategy, KvEngine, Range, TabletContext, TabletRegistry, WriteOptions, DATA_CFS,
};
use fail::fail_point;
use kvproto::{import_sstpb::SstMeta, metapb::Region};
use raftstore::store::{TabletSnapKey, TabletSnapManager};
use slog::{debug, error, info, warn, Logger};
use sst_importer::SstImporter;
use tikv_util::{
    config::ReadableDuration,
    slog_panic,
    time::Instant,
    worker::{Runnable, RunnableWithTimer},
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
    Either,
};

const DEFAULT_HIGH_PRI_POOL_SIZE: usize = 2;
const DEFAULT_LOW_PRI_POOL_SIZE: usize = 6;

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
        cb: Option<Box<dyn FnOnce() + Send>>,
    },
    Destroy {
        region_id: u64,
        persisted_index: u64,
    },
    /// Sometimes we know for sure a tablet can be destroyed directly.
    DirectDestroy {
        tablet: Either<EK, PathBuf>,
    },
    /// Cleanup ssts.
    CleanupImportSst(Box<[SstMeta]>),
    /// Flush memtable.
    Flush {
        region_id: u64,
        reason: &'static str,
        high_priority: bool,
        /// Do not flush if the active memtable is just flushed within this
        /// threshold.
        threshold: Option<Duration>,
        /// Callback will be called regardless of whether the flush succeeds.
        cb: Option<Box<dyn FnOnce() + Send>>,
    },
    DeleteRange {
        region_id: u64,
        tablet: EK,
        cf: CfName,
        start_key: Box<[u8]>,
        end_key: Box<[u8]>,
        cb: Box<dyn FnOnce(bool) + Send>,
    },
    // Gc snapshot
    SnapGc(Box<[TabletSnapKey]>),
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
                "prepare destroy tablet for region_id {region_id}, wait_for_persisted {wait_for_persisted}",
            ),
            Task::Destroy {
                region_id,
                persisted_index,
            } => write!(
                f,
                "destroy tablet for region_id {region_id}, persisted_index {persisted_index}",
            ),
            Task::DirectDestroy { .. } => {
                write!(f, "direct destroy tablet")
            }
            Task::CleanupImportSst(ssts) => {
                write!(f, "cleanup import ssts {ssts:?}")
            }
            Task::Flush {
                region_id,
                reason,
                high_priority,
                threshold,
                cb: on_flush_finish,
            } => {
                write!(
                    f,
                    "flush tablet for region_id {region_id}, reason {reason}, high_priority \
                    {high_priority}, threshold {:?}, has_cb {}",
                    threshold,
                    on_flush_finish.is_some(),
                )
            }
            Task::DeleteRange {
                region_id,
                cf,
                start_key,
                end_key,
                ..
            } => {
                write!(
                    f,
                    "delete range cf {} [{}, {}) for region_id {}",
                    cf,
                    log_wrappers::Value::key(start_key),
                    log_wrappers::Value::key(end_key),
                    region_id,
                )
            }

            Task::SnapGc(snap_keys) => {
                write!(f, "gc snapshot {:?}", snap_keys)
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
            cb: None,
        }
    }

    #[inline]
    pub fn prepare_destroy_path(path: PathBuf, region_id: u64, wait_for_persisted: u64) -> Self {
        Task::PrepareDestroy {
            tablet: Either::Right(path),
            region_id,
            wait_for_persisted,
            cb: None,
        }
    }

    #[inline]
    pub fn prepare_destroy_path_callback(
        path: PathBuf,
        region_id: u64,
        wait_for_persisted: u64,
        cb: impl FnOnce() + Send + 'static,
    ) -> Self {
        Task::PrepareDestroy {
            tablet: Either::Right(path),
            region_id,
            wait_for_persisted,
            cb: Some(Box::new(cb)),
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

    pub fn delete_range(
        region_id: u64,
        tablet: EK,
        cf: CfName,
        start_key: Box<[u8]>,
        end_key: Box<[u8]>,
        cb: Box<dyn FnOnce(bool) + Send>,
    ) -> Self {
        Task::DeleteRange {
            region_id,
            tablet,
            cf,
            start_key,
            end_key,
            cb,
        }
    }
}

pub struct Runner<EK: KvEngine> {
    tablet_registry: TabletRegistry<EK>,
    sst_importer: Arc<SstImporter>,
    snap_mgr: TabletSnapManager,
    logger: Logger,

    // region_id -> [(tablet_path, wait_for_persisted, callback)].
    waiting_destroy_tasks: HashMap<u64, Vec<(PathBuf, u64, Option<Box<dyn FnOnce() + Send>>)>>,
    pending_destroy_tasks: Vec<(PathBuf, Option<Box<dyn FnOnce() + Send>>)>,

    // An independent pool to run tasks that are time-consuming but doesn't take CPU resources,
    // such as waiting for RocksDB compaction.
    high_pri_pool: FuturePool,
    low_pri_pool: FuturePool,
}

impl<EK: KvEngine> Runner<EK> {
    pub fn new(
        tablet_registry: TabletRegistry<EK>,
        sst_importer: Arc<SstImporter>,
        snap_mgr: TabletSnapManager,
        logger: Logger,
    ) -> Self {
        Self {
            tablet_registry,
            sst_importer,
            snap_mgr,
            logger,
            waiting_destroy_tasks: HashMap::default(),
            pending_destroy_tasks: Vec::new(),
            high_pri_pool: YatpPoolBuilder::new(DefaultTicker::default())
                .name_prefix("tablet-high")
                .thread_count(0, DEFAULT_HIGH_PRI_POOL_SIZE, DEFAULT_HIGH_PRI_POOL_SIZE)
                .build_future_pool(),
            low_pri_pool: YatpPoolBuilder::new(DefaultTicker::default())
                .name_prefix("tablet-bg")
                .thread_count(0, DEFAULT_LOW_PRI_POOL_SIZE, DEFAULT_LOW_PRI_POOL_SIZE)
                .build_future_pool(),
        }
    }

    fn trim(&self, tablet: EK, start: Box<[u8]>, end: Box<[u8]>, cb: Box<dyn FnOnce() + Send>) {
        let start_key = keys::data_key(&start);
        let end_key = keys::data_end_key(&end);
        let range1 = Range::new(&[], &start_key);
        let range2 = Range::new(&end_key, keys::DATA_MAX_KEY);
        let mut wopts = WriteOptions::default();
        wopts.set_disable_wal(true);
        if let Err(e) =
            tablet.delete_ranges_cfs(&wopts, DeleteStrategy::DeleteFiles, &[range1, range2])
        {
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
        self.low_pri_pool
            .spawn(async move {
                let range1 = Range::new(&[], &start_key);
                let range2 = Range::new(&end_key, keys::DATA_MAX_KEY);
                // Note: Refer to https://github.com/facebook/rocksdb/pull/11468. There's could be
                // some files missing from compaction if dynamic_level_bytes is off.
                for r in [range1, range2] {
                    // When compaction filter is present, trivial move is disallowed.
                    if let Err(e) =
                        tablet.compact_range(Some(r.start_key), Some(r.end_key), false, 1)
                    {
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
                if let Err(e) = tablet.check_in_range(Some(&start_key), Some(&end_key)) {
                    debug_assert!(false, "check_in_range failed {:?}, is titan enabled?", e);
                    error!(
                        logger,
                        "trim did not remove all dirty data";
                        "path" => tablet.path(),
                        "err" => %e,
                    );
                    return;
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
                // `pause_background_work` needs to wait for outstanding compactions.
                let path = PathBuf::from(tablet.path());
                self.low_pri_pool
                    .spawn(async move {
                        let _ = tablet.pause_background_work();
                    })
                    .unwrap();
                path
            }
            Either::Right(path) => path,
        }
    }

    fn prepare_destroy(
        &mut self,
        region_id: u64,
        tablet: Either<EK, PathBuf>,
        wait_for_persisted: u64,
        cb: Option<Box<dyn FnOnce() + Send>>,
    ) {
        let path = self.pause_background_work(tablet);
        let list = self.waiting_destroy_tasks.entry(region_id).or_default();
        if list.iter().any(|(p, ..)| p == &path) {
            return;
        }
        list.push((path, wait_for_persisted, cb));
    }

    fn destroy(&mut self, region_id: u64, persisted: u64) {
        if let Some(v) = self.waiting_destroy_tasks.get_mut(&region_id) {
            v.retain_mut(|(path, wait, cb)| {
                if *wait <= persisted {
                    let cb = cb.take();
                    if !Self::process_destroy_task(&self.logger, &self.tablet_registry, path) {
                        self.pending_destroy_tasks.push((path.clone(), cb));
                    } else if let Some(cb) = cb {
                        cb();
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
            self.pending_destroy_tasks.push((path, None));
        }
    }

    /// Returns true if task is consumed. Failure is considered consumed.
    fn process_destroy_task(logger: &Logger, registry: &TabletRegistry<EK>, path: &Path) -> bool {
        match EK::locked(path.to_str().unwrap()) {
            Err(e) if !e.to_string().contains("No such file or directory") => {
                warn!(
                    logger,
                    "failed to check whether the tablet path is locked";
                    "err" => ?e,
                    "path" => path.display(),
                );
                return false;
            }
            Err(_) => (),
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
            }
            Ok(true) => {
                debug!(logger, "ignore locked tablet"; "path" => path.display());
                return false;
            }
        }
        true
    }

    fn cleanup_ssts(&self, ssts: Box<[SstMeta]>) {
        for sst in Vec::from(ssts) {
            if let Err(e) = self.sst_importer.delete(&sst) {
                warn!(self.logger, "failed to cleanup sst"; "err" => ?e, "sst" => ?sst);
            }
        }
    }

    fn snap_gc(&self, keys: Box<[TabletSnapKey]>) {
        for key in Vec::from(keys) {
            if !self.snap_mgr.delete_snapshot(&key) {
                warn!(self.logger, "failed to gc snap"; "key" => ?key);
            }
        }
    }

    fn flush_tablet(
        &self,
        region_id: u64,
        reason: &'static str,
        high_priority: bool,
        threshold: Option<Duration>,
        cb: Option<Box<dyn FnOnce() + Send>>,
    ) {
        let Some(Some(tablet)) = self
            .tablet_registry
            .get(region_id)
            .map(|mut cache| cache.latest().cloned())
        else {
            warn!(
                self.logger,
                "flush memtable failed to acquire tablet";
                "region_id" => region_id,
                "reason" => reason,
            );
            if let Some(cb) = cb {
                cb();
            }
            return;
        };
        let threshold = threshold.map(|t| std::time::SystemTime::now() - t);
        // The callback `cb` being some means it's the task sent from
        // leader, we should sync flush memtables and call it after the flush complete
        // where the split will be proposed again with extra flag.
        if let Some(cb) = cb {
            let logger = self.logger.clone();
            let now = Instant::now();
            let pool = if high_priority
                && self.low_pri_pool.get_running_task_count() >= DEFAULT_LOW_PRI_POOL_SIZE - 2
            {
                &self.high_pri_pool
            } else {
                &self.low_pri_pool
            };
            pool.spawn(async move {
                // sync flush for leader to let the flush happen before later checkpoint.
                if threshold.is_none() || tablet.has_old_active_memtable(threshold.unwrap()) {
                    let r = tablet.flush_cfs(DATA_CFS, true);
                    let elapsed = now.saturating_elapsed();
                    if let Err(e) = r {
                        warn!(
                            logger,
                            "flush memtable for leader failed";
                            "region_id" => region_id,
                            "reason" => reason,
                            "err" => ?e,
                        );
                    } else {
                        info!(
                            logger,
                            "flush memtable for leader";
                            "region_id" => region_id,
                            "reason" => reason,
                            "duration" => %ReadableDuration(elapsed),
                        );
                    }
                } else {
                    info!(
                        logger,
                        "skipped flush memtable for leader";
                        "region_id" => region_id,
                        "reason" => reason,
                    );
                }
                drop(tablet);
                cb();
            })
            .unwrap();
        } else if threshold.is_none() || tablet.has_old_active_memtable(threshold.unwrap()) {
            if let Err(e) = tablet.flush_cfs(DATA_CFS, false) {
                warn!(
                    self.logger,
                    "flush memtable for follower failed";
                    "region_id" => region_id,
                    "reason" => reason,
                    "err" => ?e,
                );
            } else {
                info!(
                    self.logger,
                    "flush memtable for follower";
                    "region_id" => region_id,
                    "reason" => reason,
                );
            }
        } else {
            info!(
                self.logger,
                "skipped flush memtable for follower";
                "region_id" => region_id,
                "reason" => reason,
            );
        }
    }

    fn delete_range(&self, delete_range: Task<EK>) {
        let Task::DeleteRange {
            region_id,
            tablet,
            cf,
            start_key,
            end_key,
            cb,
        } = delete_range
        else {
            slog_panic!(self.logger, "unexpected task"; "task" => format!("{}", delete_range))
        };

        let range = vec![Range::new(&start_key, &end_key)];
        let fail_f = |e: engine_traits::Error, strategy: DeleteStrategy| {
            slog_panic!(
                self.logger,
                "failed to delete";
                "region_id" => region_id,
                "strategy" => ?strategy,
                "range_start" => log_wrappers::Value::key(&start_key),
                "range_end" => log_wrappers::Value::key(&end_key),
                "error" => ?e,
            )
        };
        let mut wopts = WriteOptions::default();
        wopts.set_disable_wal(true);
        let mut written = tablet
            .delete_ranges_cf(&wopts, cf, DeleteStrategy::DeleteFiles, &range)
            .unwrap_or_else(|e| fail_f(e, DeleteStrategy::DeleteFiles));

        let strategy = DeleteStrategy::DeleteByKey;
        // Delete all remaining keys.
        written |= tablet
            .delete_ranges_cf(&wopts, cf, strategy.clone(), &range)
            .unwrap_or_else(move |e| fail_f(e, strategy));

        // TODO: support titan?
        // tablet
        //     .delete_ranges_cf(&wopts, cf, DeleteStrategy::DeleteBlobs, &range)
        //     .unwrap_or_else(move |e| fail_f(e,
        // DeleteStrategy::DeleteBlobs));

        cb(written);
    }
}

#[cfg(test)]
impl<EK: KvEngine> Runner<EK> {
    pub fn get_running_task_count(&self) -> usize {
        self.low_pri_pool.get_running_task_count()
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
                cb,
            } => self.prepare_destroy(region_id, tablet, wait_for_persisted, cb),
            Task::Destroy {
                region_id,
                persisted_index,
            } => self.destroy(region_id, persisted_index),
            Task::DirectDestroy { tablet, .. } => self.direct_destroy(tablet),
            Task::CleanupImportSst(ssts) => self.cleanup_ssts(ssts),
            Task::Flush {
                region_id,
                reason,
                high_priority,
                threshold,
                cb,
            } => self.flush_tablet(region_id, reason, high_priority, threshold, cb),
            delete_range @ Task::DeleteRange { .. } => self.delete_range(delete_range),
            Task::SnapGc(keys) => self.snap_gc(keys),
        }
    }
}

impl<EK> RunnableWithTimer for Runner<EK>
where
    EK: KvEngine,
{
    fn on_timeout(&mut self) {
        self.pending_destroy_tasks.retain_mut(|(path, cb)| {
            let r = Self::process_destroy_task(&self.logger, &self.tablet_registry, path);
            if r && let Some(cb) = cb.take() {
                cb();
            }
            !r
        });
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
        let snap_dir = dir.path().join("snap");
        let snap_mgr = TabletSnapManager::new(snap_dir, None).unwrap();
        let mut runner = Runner::new(registry.clone(), importer, snap_mgr, logger);

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

    #[test]
    fn test_destroy_locked_tablet() {
        let dir = Builder::new()
            .prefix("test_destroy_locked_tablet")
            .tempdir()
            .unwrap();
        let factory = Box::new(TestTabletFactory::new(
            DbOptions::default(),
            vec![("default", CfOptions::default())],
        ));
        let registry = TabletRegistry::new(factory, dir.path()).unwrap();
        let logger = slog_global::borrow_global().new(slog::o!());
        let (_dir, importer) = create_tmp_importer();
        let snap_dir = dir.path().join("snap");
        let snap_mgr = TabletSnapManager::new(snap_dir, None).unwrap();
        let mut runner = Runner::new(registry.clone(), importer, snap_mgr, logger);

        let mut region = Region::default();
        let r_1 = 1;
        region.set_id(r_1);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"b".to_vec());
        let tablet1 = registry
            .load(TabletContext::new(&region, Some(1)), true)
            .unwrap()
            .latest()
            .unwrap()
            .clone();
        let path1 = PathBuf::from(tablet1.path());
        let r_2 = 2;
        region.set_id(r_2);
        region.set_start_key(b"c".to_vec());
        region.set_end_key(b"d".to_vec());
        let tablet2 = registry
            .load(TabletContext::new(&region, Some(1)), true)
            .unwrap()
            .latest()
            .unwrap()
            .clone();
        let path2 = PathBuf::from(tablet2.path());

        // both tablets are locked.
        runner.run(Task::prepare_destroy(tablet1, r_1, 10));
        runner.run(Task::prepare_destroy(tablet2, r_2, 10));
        runner.run(Task::destroy(r_1, 100));
        runner.run(Task::destroy(r_2, 100));
        assert!(path1.exists());
        assert!(path2.exists());

        registry.remove(r_1);
        runner.on_timeout();
        assert!(!path1.exists());
        assert!(path2.exists());

        registry.remove(r_2);
        runner.on_timeout();
        assert!(!path2.exists());
    }

    #[test]
    fn test_destroy_missing() {
        let dir = Builder::new()
            .prefix("test_destroy_missing")
            .tempdir()
            .unwrap();
        let factory = Box::new(TestTabletFactory::new(
            DbOptions::default(),
            vec![("default", CfOptions::default())],
        ));
        let registry = TabletRegistry::new(factory, dir.path()).unwrap();
        let logger = slog_global::borrow_global().new(slog::o!());
        let (_dir, importer) = create_tmp_importer();
        let snap_dir = dir.path().join("snap");
        let snap_mgr = TabletSnapManager::new(snap_dir, None).unwrap();
        let mut runner = Runner::new(registry.clone(), importer, snap_mgr, logger);

        let mut region = Region::default();
        let r_1 = 1;
        region.set_id(r_1);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"b".to_vec());
        let tablet = registry
            .load(TabletContext::new(&region, Some(1)), true)
            .unwrap()
            .latest()
            .unwrap()
            .clone();
        let path = PathBuf::from(tablet.path());
        // submit for destroy twice.
        runner.run(Task::prepare_destroy(tablet.clone(), r_1, 10));
        runner.run(Task::destroy(r_1, 100));
        runner.run(Task::prepare_destroy(tablet, r_1, 10));
        runner.run(Task::destroy(r_1, 100));
        assert!(path.exists());
        registry.remove(r_1);
        // waiting for async `pause_background_work` to be finished,
        // this task can block tablet's destroy.
        for _i in 0..100 {
            if runner.get_running_task_count() == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        runner.on_timeout();
        assert!(!path.exists());
        assert!(runner.pending_destroy_tasks.is_empty());

        // submit a non-existing path.
        runner.run(Task::prepare_destroy_path(
            dir.path().join("missing"),
            r_1,
            200,
        ));
        runner.run(Task::destroy(r_1, 500));
        runner.on_timeout();
        assert!(runner.pending_destroy_tasks.is_empty());
    }
}
