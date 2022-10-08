// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    fmt,
    marker::PhantomData,
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_traits::KvEngine;
use error_code::ErrorCodeExt;
use futures::FutureExt;
use kvproto::{
    brpb::{StreamBackupError, StreamBackupTaskInfo},
    metapb::Region,
};
use online_config::ConfigChange;
use pd_client::PdClient;
use raftstore::{
    coprocessor::{CmdBatch, ObserveHandle, RegionInfoProvider},
    router::RaftStoreRouter,
};
use tikv::config::BackupStreamConfig;
use tikv_util::{
    box_err,
    config::ReadableDuration,
    debug, defer, info,
    sys::thread::ThreadBuildWrapper,
    time::{Instant, Limiter},
    warn,
    worker::{Runnable, Scheduler},
    HandyRwLock,
};
use tokio::{
    io::Result as TokioResult,
    runtime::{Handle, Runtime},
    sync::oneshot,
};
use tokio_stream::StreamExt;
use txn_types::TimeStamp;

use super::metrics::HANDLE_EVENT_DURATION_HISTOGRAM;
use crate::{
    annotate,
    checkpoint_manager::{
        BasicFlushObserver, CheckpointManager, CheckpointV2FlushObserver,
        CheckpointV3FlushObserver, FlushObserver, GetCheckpointResult, RegionIdWithVersion,
    },
    errors::{Error, Result},
    event_loader::{InitialDataLoader, PendingMemoryQuota},
    future,
    metadata::{store::MetaStore, MetadataClient, MetadataEvent, StreamTask},
    metrics::{self, TaskStatus},
    observer::BackupStreamObserver,
    router::{ApplyEvents, Router, TaskSelector},
    subscription_manager::{RegionSubscriptionManager, ResolvedRegions},
    subscription_track::SubscriptionTracer,
    try_send,
    utils::{self, CallbackWaitGroup, StopWatch, Work},
};

const SLOW_EVENT_THRESHOLD: f64 = 120.0;
/// CHECKPOINT_SAFEPOINT_TTL_IF_ERROR specifies the safe point TTL(24 hour) if
/// task has fatal error.
const CHECKPOINT_SAFEPOINT_TTL_IF_ERROR: u64 = 24;
/// The timeout for tick updating the checkpoint.
/// Generally, it would take ~100ms.
/// 5s would be enough for it.
const TICK_UPDATE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct Endpoint<S, R, E, RT, PDC> {
    // Note: those fields are more like a shared context between components.
    // For now, we copied them everywhere, maybe we'd better extract them into a
    // context type.
    pub(crate) meta_client: MetadataClient<S>,
    pub(crate) scheduler: Scheduler<Task>,
    pub(crate) store_id: u64,
    pub(crate) regions: R,
    pub(crate) engine: PhantomData<E>,
    pub(crate) router: RT,
    pub(crate) pd_client: Arc<PDC>,
    pub(crate) subs: SubscriptionTracer,
    pub(crate) concurrency_manager: ConcurrencyManager,

    range_router: Router,
    observer: BackupStreamObserver,
    pool: Runtime,
    initial_scan_memory_quota: PendingMemoryQuota,
    initial_scan_throughput_quota: Limiter,
    region_operator: RegionSubscriptionManager<S, R, PDC>,
    failover_time: Option<Instant>,
    config: BackupStreamConfig,
    checkpoint_mgr: CheckpointManager,
}

impl<S, R, E, RT, PDC> Endpoint<S, R, E, RT, PDC>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
    S: MetaStore + 'static,
{
    pub fn new(
        store_id: u64,
        store: S,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
        pd_client: Arc<PDC>,
        concurrency_manager: ConcurrencyManager,
    ) -> Self {
        crate::metrics::STREAM_ENABLED.inc();
        let pool = create_tokio_runtime((config.num_threads / 2).max(1), "backup-stream")
            .expect("failed to create tokio runtime for backup stream worker.");

        let meta_client = MetadataClient::new(store, store_id);
        let range_router = Router::new(
            PathBuf::from(config.temp_path.clone()),
            scheduler.clone(),
            config.file_size_limit.0,
            config.max_flush_interval.0,
        );

        // spawn a worker to watch task changes from etcd periodically.
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();
        // TODO build a error handle mechanism #error 2
        pool.spawn(async {
            if let Err(err) = Self::start_and_watch_tasks(meta_client_clone, scheduler_clone).await
            {
                err.report("failed to start watch tasks");
            }
        });

        pool.spawn(Self::starts_flush_ticks(range_router.clone()));

        let initial_scan_memory_quota =
            PendingMemoryQuota::new(config.initial_scan_pending_memory_quota.0 as _);
        let limit = if config.initial_scan_rate_limit.0 > 0 {
            config.initial_scan_rate_limit.0 as f64
        } else {
            f64::INFINITY
        };
        let initial_scan_throughput_quota = Limiter::new(limit);
        info!("the endpoint of stream backup started"; "path" => %config.temp_path);
        let subs = SubscriptionTracer::default();
        let (region_operator, op_loop) = RegionSubscriptionManager::start(
            InitialDataLoader::new(
                router.clone(),
                accessor.clone(),
                range_router.clone(),
                subs.clone(),
                scheduler.clone(),
                initial_scan_memory_quota.clone(),
                pool.handle().clone(),
                initial_scan_throughput_quota.clone(),
            ),
            observer.clone(),
            meta_client.clone(),
            pd_client.clone(),
            ((config.num_threads + 1) / 2).max(1),
        );
        pool.spawn(op_loop);
        Endpoint {
            meta_client,
            range_router,
            scheduler,
            observer,
            pool,
            store_id,
            regions: accessor,
            engine: PhantomData,
            router,
            pd_client,
            subs,
            concurrency_manager,
            initial_scan_memory_quota,
            initial_scan_throughput_quota,
            region_operator,
            failover_time: None,
            config,
            checkpoint_mgr: Default::default(),
        }
    }
}

impl<S, R, E, RT, PDC> Endpoint<S, R, E, RT, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
{
    fn get_meta_client(&self) -> MetadataClient<S> {
        self.meta_client.clone()
    }

    fn on_fatal_error(&self, select: TaskSelector, err: Box<Error>) {
        err.report_fatal();
        let tasks = self
            .pool
            .block_on(self.range_router.select_task(select.reference()));
        warn!("fatal error reporting"; "selector" => ?select, "selected" => ?tasks, "err" => %err);
        for task in tasks {
            // Let's pause the task first.
            self.unload_task(&task);
            metrics::update_task_status(TaskStatus::Error, &task);

            let meta_cli = self.get_meta_client();
            let pdc = self.pd_client.clone();
            let store_id = self.store_id;
            let sched = self.scheduler.clone();
            let safepoint_name = self.pause_guard_id_for_task(&task);
            let safepoint_ttl = self.pause_guard_duration();
            let code = err.error_code().code.to_owned();
            let msg = err.to_string();
            self.pool.block_on(async move {
                let err_fut = async {
                    let safepoint = meta_cli.global_progress_of_task(&task).await?;
                    pdc.update_service_safe_point(
                        safepoint_name,
                        TimeStamp::new(safepoint - 1),
                        safepoint_ttl,
                    )
                    .await?;
                    meta_cli.pause(&task).await?;
                    let mut last_error = StreamBackupError::new();
                    last_error.set_error_code(code);
                    last_error.set_error_message(msg.clone());
                    last_error.set_store_id(store_id);
                    last_error.set_happen_at(TimeStamp::physical_now());
                    meta_cli.report_last_error(&task, last_error).await?;
                    Result::Ok(())
                };
                if let Err(err_report) = err_fut.await {
                    err_report.report(format_args!("failed to upload error {}", err_report));
                    // Let's retry reporting after 5s.
                    tokio::task::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        try_send!(
                            sched,
                            Task::FatalError(
                                TaskSelector::ByName(task.to_owned()),
                                Box::new(annotate!(err_report, "origin error: {}", msg))
                            )
                        );
                    });
                }
            });
        }
    }

    async fn starts_flush_ticks(router: Router) {
        loop {
            // check every 5s.
            // TODO: maybe use global timer handle in the `tikv_utils::timer` (instead of
            // enabling timing in the current runtime)?
            tokio::time::sleep(Duration::from_secs(5)).await;
            debug!("backup stream trigger flush tick");
            router.tick().await;
        }
    }

    // TODO find a proper way to exit watch tasks
    async fn start_and_watch_tasks(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
    ) -> Result<()> {
        let tasks = meta_client.get_tasks().await?;
        for task in tasks.inner {
            info!("backup stream watch task"; "task" => ?task);
            if task.is_paused {
                continue;
            }
            // We have meet task upon store start, we must in a failover.
            scheduler.schedule(Task::MarkFailover(Instant::now()))?;
            // move task to schedule
            scheduler.schedule(Task::WatchTask(TaskOp::AddTask(task)))?;
        }

        let revision = tasks.revision;
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();

        Handle::current().spawn(async move {
            if let Err(err) =
                Self::starts_watch_task(meta_client_clone, scheduler_clone, revision).await
            {
                err.report("failed to start watch tasks");
            }
        });

        Handle::current().spawn(async move {
            if let Err(err) = Self::starts_watch_pause(meta_client, scheduler, revision).await {
                err.report("failed to start watch pause");
            }
        });

        Ok(())
    }

    async fn starts_watch_task(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
        revision: i64,
    ) -> Result<()> {
        let mut revision_new = revision;
        loop {
            let watcher = meta_client.events_from(revision_new).await;
            let mut watcher = match watcher {
                Ok(w) => w,
                Err(e) => {
                    e.report("failed to start watch pause");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            loop {
                if let Some(event) = watcher.stream.next().await {
                    info!("backup stream watch event from etcd"; "event" => ?event);

                    let revision = meta_client.get_reversion().await;
                    if let Ok(r) = revision {
                        revision_new = r;
                    }

                    match event {
                        MetadataEvent::AddTask { task } => {
                            scheduler.schedule(Task::WatchTask(TaskOp::AddTask(task)))?;
                        }
                        MetadataEvent::RemoveTask { task } => {
                            scheduler.schedule(Task::WatchTask(TaskOp::RemoveTask(task)))?;
                        }
                        MetadataEvent::Error { err } => {
                            err.report("metadata client watch meet error");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            break;
                        }
                        _ => warn!("BUG: invalid event"; "event" => ?event),
                    }
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
    }

    async fn starts_watch_pause(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
        revision: i64,
    ) -> Result<()> {
        let mut revision_new = revision;

        loop {
            let watcher = meta_client.events_from_pause(revision_new).await;
            let mut watcher = match watcher {
                Ok(w) => w,
                Err(e) => {
                    e.report("failed to start watch pause");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            loop {
                if let Some(event) = watcher.stream.next().await {
                    info!("backup stream watch event from etcd"; "event" => ?event);
                    let revision = meta_client.get_reversion().await;
                    if let Ok(r) = revision {
                        revision_new = r;
                    }

                    match event {
                        MetadataEvent::PauseTask { task } => {
                            scheduler.schedule(Task::WatchTask(TaskOp::PauseTask(task)))?;
                        }
                        MetadataEvent::ResumeTask { task } => {
                            scheduler.schedule(Task::WatchTask(TaskOp::ResumeTask(task)))?;
                        }
                        MetadataEvent::Error { err } => {
                            err.report("metadata client watch meet error");
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            break;
                        }
                        _ => warn!("BUG: invalid event"; "event" => ?event),
                    }
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
    }

    fn flush_observer(&self) -> Box<dyn FlushObserver> {
        let basic = BasicFlushObserver::new(self.pd_client.clone(), self.store_id);
        if self.config.use_checkpoint_v3 {
            Box::new(CheckpointV3FlushObserver::new(
                self.scheduler.clone(),
                self.meta_client.clone(),
                self.subs.clone(),
                basic,
            ))
        } else {
            Box::new(CheckpointV2FlushObserver::new(
                self.meta_client.clone(),
                self.make_flush_guard(),
                self.subs.clone(),
                basic,
            ))
        }
    }

    /// Convert a batch of events to the cmd batch, and update the resolver
    /// status.
    fn record_batch(subs: SubscriptionTracer, batch: CmdBatch) -> Option<ApplyEvents> {
        let region_id = batch.region_id;
        let mut resolver = match subs.get_subscription_of(region_id) {
            Some(rts) => rts,
            None => {
                debug!("the region isn't registered (no resolver found) but sent to backup_batch, maybe stale."; "region_id" => %region_id);
                return None;
            }
        };
        // Stale data is acceptable, while stale locks may block the checkpoint
        // advancing.
        // ```text
        // Let L be the instant some key locked, U be the instant it unlocked,
        // +---------*-------L-----------U--*-------------+
        //           ^   ^----(1)----^      ^ We get the snapshot for initial scanning at here.
        //           +- If we issue refresh resolver at here, and the cmd batch (1) is the last cmd batch of the first observing.
        //              ...the background initial scanning may keep running, and the lock would be sent to the scanning.
        //              ...note that (1) is the last cmd batch of first observing, so the unlock event would never be sent to us.
        //              ...then the lock would get an eternal life in the resolver :|
        //                 (Before we refreshing the resolver for this region again)
        // ```
        if batch.pitr_id != resolver.value().handle.id {
            debug!("stale command"; "region_id" => %region_id, "now" => ?resolver.value().handle.id, "remote" => ?batch.pitr_id);
            return None;
        }

        let kvs = ApplyEvents::from_cmd_batch(batch, resolver.value_mut().resolver());
        Some(kvs)
    }

    fn backup_batch(&self, batch: CmdBatch, work: Work) {
        let mut sw = StopWatch::new();

        let router = self.range_router.clone();
        let sched = self.scheduler.clone();
        let subs = self.subs.clone();
        self.pool.spawn(async move {
            let region_id = batch.region_id;
            let kvs = Self::record_batch(subs, batch);
            if kvs.as_ref().map(|x| x.is_empty()).unwrap_or(true) {
                return;
            }
            let kvs = kvs.unwrap();

            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["to_stream_event"])
                .observe(sw.lap().as_secs_f64());
            let kv_count = kvs.len();
            let total_size = kvs.size();
            metrics::HEAP_MEMORY
                .add(total_size as _);
            utils::handle_on_event_result(&sched, router.on_events(kvs).await);
            metrics::HEAP_MEMORY
                .sub(total_size as _);
            let time_cost = sw.lap().as_secs_f64();
            if time_cost > SLOW_EVENT_THRESHOLD {
                warn!("write to temp file too slow."; "time_cost" => ?time_cost, "region_id" => %region_id, "len" => %kv_count);
            }
            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["save_to_temp_file"])
                .observe(time_cost);
            drop(work)
        });
    }

    /// Make an initial data loader using the resource of the endpoint.
    pub fn make_initial_loader(&self) -> InitialDataLoader<E, R, RT> {
        InitialDataLoader::new(
            self.router.clone(),
            self.regions.clone(),
            self.range_router.clone(),
            self.subs.clone(),
            self.scheduler.clone(),
            self.initial_scan_memory_quota.clone(),
            self.pool.handle().clone(),
            self.initial_scan_throughput_quota.clone(),
        )
    }

    pub fn handle_watch_task(&self, op: TaskOp) {
        match op {
            TaskOp::AddTask(task) => {
                self.on_register(task);
            }
            TaskOp::RemoveTask(task_name) => {
                self.on_unregister(&task_name);
            }
            TaskOp::PauseTask(task_name) => {
                self.on_pause(&task_name);
            }
            TaskOp::ResumeTask(task) => {
                self.on_resume(task);
            }
        }
    }

    async fn observe_and_scan_region(
        &self,
        init: InitialDataLoader<E, R, RT>,
        task: &StreamTask,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Result<()> {
        let start = Instant::now_coarse();
        let success = self
            .observer
            .ranges
            .wl()
            .add((start_key.clone(), end_key.clone()));
        if !success {
            warn!("backup stream task ranges overlapped, which hasn't been supported for now";
                "task" => ?task,
                "start_key" => utils::redact(&start_key),
                "end_key" => utils::redact(&end_key),
            );
        }
        // Assuming the `region info provider` would read region info form `StoreMeta`
        // directly and this would be fast. If this gets slow, maybe make it async
        // again. (Will that bring race conditions? say `Start` handled after
        // `ResfreshResolver` of some region.)
        let range_init_result = init.initialize_range(start_key.clone(), end_key.clone());
        match range_init_result {
            Ok(()) => {
                info!("backup stream success to initialize"; 
                        "start_key" => utils::redact(&start_key),
                        "end_key" => utils::redact(&end_key),
                        "take" => ?start.saturating_elapsed(),)
            }
            Err(e) => {
                e.report("backup stream initialize failed");
            }
        }
        Ok(())
    }

    // register task ranges
    pub fn on_register(&self, task: StreamTask) {
        let name = task.info.name.clone();
        let start_ts = task.info.start_ts;
        self.load_task(task);

        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[name.as_str()])
            .set(start_ts as _);
    }

    /// Load the task into memory: this would make the endpint start to observe.
    fn load_task(&self, task: StreamTask) {
        let cli = self.meta_client.clone();
        let init = self.make_initial_loader();
        let range_router = self.range_router.clone();
        let use_v3 = self.config.use_checkpoint_v3;

        info!(
            "register backup stream task";
            "task" => ?task,
        );

        let task_name = task.info.get_name().to_owned();
        // clean the safepoint created at pause(if there is)
        self.pool.spawn(
            self.pd_client
                .update_service_safe_point(
                    self.pause_guard_id_for_task(task.info.get_name()),
                    TimeStamp::zero(),
                    Duration::new(0, 0),
                )
                .map(|r| {
                    r.map_err(|err| Error::from(err).report("removing safe point for pausing"))
                }),
        );
        self.pool.block_on(async move {
            let task_clone = task.clone();
            let run = async move {
                let task_name = task.info.get_name();
                if !use_v3 {
                    cli.init_task(&task.info).await?;
                }
                let ranges = cli.ranges_of_task(task_name).await?;
                info!(
                    "register backup stream ranges";
                    "task" => ?task,
                    "ranges-count" => ranges.inner.len(),
                );
                let ranges = ranges
                    .inner
                    .into_iter()
                    .map(|(start_key, end_key)| {
                        (utils::wrap_key(start_key), utils::wrap_key(end_key))
                    })
                    .collect::<Vec<_>>();
                range_router
                    .register_task(task.clone(), ranges.clone(), self.config.file_size_limit.0)
                    .await?;

                for (start_key, end_key) in ranges {
                    let init = init.clone();

                    self.observe_and_scan_region(init, &task, start_key, end_key)
                        .await?
                }
                info!(
                    "finish register backup stream ranges";
                    "task" => ?task,
                );
                Result::Ok(())
            };
            if let Err(e) = run.await {
                e.report(format!(
                    "failed to register backup stream task {} to router: ranges not found",
                    task_clone.info.get_name()
                ));
            }
        });
        metrics::update_task_status(TaskStatus::Running, &task_name);
    }

    fn pause_guard_id_for_task(&self, task: &str) -> String {
        format!("{}-{}-pause-guard", task, self.store_id)
    }

    fn pause_guard_duration(&self) -> Duration {
        ReadableDuration::hours(CHECKPOINT_SAFEPOINT_TTL_IF_ERROR).0
    }

    pub fn on_pause(&self, task: &str) {
        self.unload_task(task);

        metrics::update_task_status(TaskStatus::Paused, task);
    }

    pub fn on_resume(&self, task_name: String) {
        let task = self.pool.block_on(self.meta_client.get_task(&task_name));
        match task {
            Ok(Some(stream_task)) => self.load_task(stream_task),
            Ok(None) => {
                info!("backup stream task not existed"; "task" => %task_name);
            }
            Err(err) => {
                err.report(format!("failed to resume backup stream task {}", task_name));
                let sched = self.scheduler.clone();
                tokio::task::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    sched
                        .schedule(Task::WatchTask(TaskOp::ResumeTask(task_name)))
                        .unwrap();
                });
            }
        }
    }

    pub fn on_unregister(&self, task: &str) -> Option<StreamBackupTaskInfo> {
        let info = self.unload_task(task);
        self.remove_metrics_after_unregister(task);
        info
    }

    fn remove_metrics_after_unregister(&self, task: &str) {
        // remove metrics of the task so it won't mislead the metrics.
        let _ = metrics::STORE_CHECKPOINT_TS
            .remove_label_values(&[task])
            .map_err(
                |err| info!("failed to remove checkpoint ts metric"; "task" => task, "err" => %err),
            );
        let _ = metrics::remove_task_status_metric(task).map_err(
            |err| info!("failed to remove checkpoint ts metric"; "task" => task, "err" => %err),
        );
    }

    /// unload a task from memory: this would stop observe the changes required
    /// by the task temporarily.
    fn unload_task(&self, task: &str) -> Option<StreamBackupTaskInfo> {
        let router = self.range_router.clone();

        // for now, we support one concurrent task only.
        // so simply clear all info would be fine.
        self.observer.ranges.wl().clear();
        self.subs.clear();
        self.pool.block_on(router.unregister_task(task))
    }

    /// Make a guard for checking whether we can flush the checkpoint ts.
    fn make_flush_guard(&self) -> impl FnOnce() -> bool + Send {
        let failover = self.failover_time;
        let flush_duration = self.config.max_flush_interval;
        move || {
            if failover
                .as_ref()
                .map(|failover_t| failover_t.saturating_elapsed() < flush_duration.0 * 2)
                .unwrap_or(false)
            {
                warn!("during failover, skipping advancing resolved ts"; 
                    "failover_time_ago" => ?failover.map(|failover_t| failover_t.saturating_elapsed()));
                return false;
            }
            let in_flight = crate::observer::IN_FLIGHT_START_OBSERVE_MESSAGE.load(Ordering::SeqCst);
            if in_flight > 0 {
                warn!("inflight leader detected, skipping advancing resolved ts"; "in_flight" => %in_flight);
                return false;
            }
            true
        }
    }

    fn prepare_min_ts(&self) -> future![TimeStamp] {
        let pd_cli = self.pd_client.clone();
        let cm = self.concurrency_manager.clone();
        async move {
            let pd_tso = pd_cli
                .get_tso()
                .await
                .map_err(|err| Error::from(err).report("failed to get tso from pd"))
                .unwrap_or_default();
            cm.update_max_ts(pd_tso);
            let min_ts = cm.global_min_lock_ts().unwrap_or(TimeStamp::max());
            Ord::min(pd_tso, min_ts)
        }
    }

    fn get_resolved_regions(&self, min_ts: TimeStamp) -> future![Result<ResolvedRegions>] {
        let (tx, rx) = oneshot::channel();
        let op = self.region_operator.clone();
        async move {
            let req = ObserveOp::ResolveRegions {
                callback: Box::new(move |rs| {
                    let _ = tx.send(rs);
                }),
                min_ts,
            };
            op.request(req).await;
            rx.await
                .map_err(|err| annotate!(err, "failed to send request for resolve regions"))
        }
    }

    fn do_flush(&self, task: String, min_ts: TimeStamp) -> future![Result<()>] {
        let get_rts = self.get_resolved_regions(min_ts);
        let router = self.range_router.clone();
        let store_id = self.store_id;
        let mut flush_ob = self.flush_observer();
        async move {
            let mut resolved = get_rts.await?;
            let mut new_rts = resolved.global_checkpoint();
            fail::fail_point!("delay_on_flush");
            flush_ob.before(resolved.take_region_checkpoints()).await;
            if let Some(rewritten_rts) = flush_ob.rewrite_resolved_ts(&task).await {
                info!("rewriting resolved ts"; "old" => %new_rts, "new" => %rewritten_rts);
                new_rts = rewritten_rts.min(new_rts);
            }
            if let Some(rts) = router.do_flush(&task, store_id, new_rts).await {
                info!("flushing and refreshing checkpoint ts.";
                    "checkpoint_ts" => %rts,
                    "task" => %task,
                );
                if rts == 0 {
                    // We cannot advance the resolved ts for now.
                    return Ok(());
                }
                flush_ob.after(&task, rts).await?
            }
            Ok(())
        }
    }

    pub fn on_force_flush(&self, task: String) {
        self.pool.block_on(async move {
            let info = self.range_router.get_task_info(&task).await;
            // This should only happen in testing, it would be to unwrap...
            let _ = info.unwrap().set_flushing_status_cas(false, true);
            let mts = self.prepare_min_ts().await;
            try_send!(self.scheduler, Task::FlushWithMinTs(task, mts));
        });
    }

    pub fn on_flush(&self, task: String) {
        self.pool.block_on(async move {
            let mts = self.prepare_min_ts().await;
            info!("min_ts prepared for flushing"; "min_ts" => %mts);
            try_send!(self.scheduler, Task::FlushWithMinTs(task, mts));
        })
    }

    fn on_flush_with_min_ts(&self, task: String, min_ts: TimeStamp) {
        self.pool.spawn(self.do_flush(task, min_ts).map(|r| {
            if let Err(err) = r {
                err.report("during updating flush status")
            }
        }));
    }

    fn update_global_checkpoint(&self, task: String) -> future![()] {
        let meta_client = self.meta_client.clone();
        let router = self.range_router.clone();
        let store_id = self.store_id;
        async move {
            #[cfg(feature = "failpoints")]
            {
                // fail-rs doesn't support async code blocks now.
                // let's borrow the feature name and do it ourselves :3
                if std::env::var("LOG_BACKUP_UGC_SLEEP_AND_RETURN").is_ok() {
                    tokio::time::sleep(Duration::from_secs(100)).await;
                    return;
                }
            }
            let ts = meta_client.global_progress_of_task(&task).await;
            match ts {
                Ok(global_checkpoint) => {
                    let r = router
                        .update_global_checkpoint(&task, global_checkpoint, store_id)
                        .await;
                    match r {
                        Ok(true) => {
                            if let Err(err) = meta_client
                                .set_storage_checkpoint(&task, global_checkpoint)
                                .await
                            {
                                warn!("backup stream failed to set global checkpoint.";
                                    "task" => ?task,
                                    "global-checkpoint" => global_checkpoint,
                                    "err" => ?err,
                                );
                            }
                        }
                        Ok(false) => {
                            debug!("backup stream no need update global checkpoint.";
                                "task" => ?task,
                                "global-checkpoint" => global_checkpoint,
                            );
                        }
                        Err(e) => {
                            warn!("backup stream failed to update global checkpoint.";
                                "task" => ?task,
                                "err" => ?e
                            );
                        }
                    }
                }
                Err(e) => {
                    warn!("backup stream failed to get global checkpoint.";
                        "task" => ?task,
                        "err" => ?e
                    );
                }
            }
        }
    }

    fn on_update_global_checkpoint(&self, task: String) {
        let _guard = self.pool.handle().enter();
        let result = self.pool.block_on(tokio::time::timeout(
            TICK_UPDATE_TIMEOUT,
            self.update_global_checkpoint(task),
        ));
        if let Err(err) = result {
            warn!("log backup update global checkpoint timed out"; "err" => %err)
        }
    }

    /// Modify observe over some region.
    /// This would register the region to the RaftStore.
    pub fn on_modify_observe(&self, op: ObserveOp) {
        self.pool.block_on(self.region_operator.request(op));
    }

    pub fn run_task(&mut self, task: Task) {
        debug!("run backup stream task"; "task" => ?task, "store_id" => %self.store_id);
        let now = Instant::now_coarse();
        let label = task.label();
        defer! {
            metrics::INTERNAL_ACTOR_MESSAGE_HANDLE_DURATION.with_label_values(&[label])
                .observe(now.saturating_elapsed_secs())
        }
        match task {
            Task::WatchTask(op) => self.handle_watch_task(op),
            Task::BatchEvent(events) => self.do_backup(events),
            Task::Flush(task) => self.on_flush(task),
            Task::ModifyObserve(op) => self.on_modify_observe(op),
            Task::ForceFlush(task) => self.on_force_flush(task),
            Task::FatalError(task, err) => self.on_fatal_error(task, err),
            Task::ChangeConfig(_) => {
                warn!("change config online isn't supported for now.")
            }
            Task::Sync(cb, mut cond) => {
                if cond(&self.range_router) {
                    cb()
                } else {
                    let sched = self.scheduler.clone();
                    self.pool.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        sched.schedule(Task::Sync(cb, cond)).unwrap();
                    });
                }
            }
            Task::MarkFailover(t) => self.failover_time = Some(t),
            Task::FlushWithMinTs(task, min_ts) => self.on_flush_with_min_ts(task, min_ts),
            Task::RegionCheckpointsOp(s) => self.handle_region_checkpoints_op(s),
            Task::UpdateGlobalCheckpoint(task) => self.on_update_global_checkpoint(task),
        }
    }

    pub fn handle_region_checkpoints_op(&mut self, op: RegionCheckpointOperation) {
        match op {
            RegionCheckpointOperation::Update(u) => {
                // Let's clear all stale checkpoints first.
                // Or they may slow down the global checkpoint.
                self.checkpoint_mgr.clear();
                for (region, checkpoint) in u {
                    debug!("setting region checkpoint"; "region" => %region.get_id(), "ts" => %checkpoint);
                    self.checkpoint_mgr
                        .update_region_checkpoint(&region, checkpoint)
                }
            }
            RegionCheckpointOperation::Get(g, cb) => {
                let _guard = self.pool.handle().enter();
                match g {
                    RegionSet::Universal => cb(self
                        .checkpoint_mgr
                        .get_all()
                        .into_iter()
                        .map(|c| GetCheckpointResult::ok(c.region.clone(), c.checkpoint))
                        .collect()),
                    RegionSet::Regions(rs) => cb(rs
                        .iter()
                        .map(|(id, version)| {
                            self.checkpoint_mgr
                                .get_from_region(RegionIdWithVersion::new(*id, *version))
                        })
                        .collect()),
                }
            }
        }
    }

    pub fn do_backup(&self, events: Vec<CmdBatch>) {
        let wg = CallbackWaitGroup::new();
        for batch in events {
            self.backup_batch(batch, wg.clone().work());
        }
        self.pool.block_on(wg.wait())
    }
}

/// Create a standard tokio runtime
/// (which allows io and time reactor, involve thread memory accessor),
fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
    info!("create tokio runtime for backup stream"; "thread_name" => thread_name, "thread-count" => thread_count);

    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        // Maybe make it more configurable?
        // currently, blocking threads would be used for tokio local I/O.
        // (`File` API in `tokio::io` would use this pool.)
        .max_blocking_threads(thread_count * 8)
        .worker_threads(thread_count)
        .enable_io()
        .enable_time()
        .after_start_wrapper(|| {
            tikv_alloc::add_thread_memory_accessor();
        })
        .before_stop_wrapper(|| {
            tikv_alloc::remove_thread_memory_accessor();
        })
        .build()
}

#[derive(Debug)]
pub enum RegionSet {
    /// The universal set.
    Universal,
    /// A subset.
    Regions(HashSet<(u64, u64)>),
}

pub enum RegionCheckpointOperation {
    Update(Vec<(Region, TimeStamp)>),
    Get(RegionSet, Box<dyn FnOnce(Vec<GetCheckpointResult>) + Send>),
}

impl fmt::Debug for RegionCheckpointOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Update(arg0) => f.debug_tuple("Update").field(arg0).finish(),
            Self::Get(arg0, _) => f.debug_tuple("Get").field(arg0).finish(),
        }
    }
}

pub enum Task {
    WatchTask(TaskOp),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(ConfigChange),
    /// Change the observe status of some region.
    ModifyObserve(ObserveOp),
    /// Convert status of some task into `flushing` and do flush then.
    ForceFlush(String),
    /// FatalError pauses the task and set the error.
    FatalError(TaskSelector, Box<Error>),
    /// Run the callback when see this message. Only for test usage.
    /// NOTE: Those messages for testing are not guarded by `#[cfg(test)]` for
    /// now, because the integration test would not enable test config when
    /// compiling (why?)
    Sync(
        // Run the closure if ...
        Box<dyn FnOnce() + Send>,
        // This returns `true`.
        Box<dyn FnMut(&Router) -> bool + Send>,
    ),
    /// Mark the store as a failover store.
    /// This would prevent store from updating its checkpoint ts for a while.
    /// Because we are not sure whether the regions in the store have new leader
    /// -- we keep a safe checkpoint so they can choose a safe `from_ts` for
    /// initial scanning.
    MarkFailover(Instant),
    /// Flush the task with name.
    Flush(String),
    /// Execute the flush with the calculated `min_ts`.
    /// This is an internal command only issued by the `Flush` task.
    FlushWithMinTs(String, TimeStamp),
    /// The command for getting region checkpoints.
    RegionCheckpointsOp(RegionCheckpointOperation),
    /// update global-checkpoint-ts to storage.
    UpdateGlobalCheckpoint(String),
}

#[derive(Debug)]
pub enum TaskOp {
    AddTask(StreamTask),
    RemoveTask(String),
    PauseTask(String),
    ResumeTask(String),
}

/// The callback for resolving region.
type ResolveRegionsCallback = Box<dyn FnOnce(ResolvedRegions) + 'static + Send>;

pub enum ObserveOp {
    Start {
        region: Region,
    },
    Stop {
        region: Region,
    },
    /// Destroy the region subscription.
    /// Unlike `Stop`, this will assume the region would never go back.
    /// For now, the effect of "never go back" is that we won't try to hint
    /// other store the checkpoint ts of this region.
    Destroy {
        region: Region,
    },
    RefreshResolver {
        region: Region,
    },
    NotifyFailToStartObserve {
        region: Region,
        handle: ObserveHandle,
        err: Box<Error>,
    },
    ResolveRegions {
        callback: ResolveRegionsCallback,
        min_ts: TimeStamp,
    },
}

impl std::fmt::Debug for ObserveOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start { region } => f.debug_struct("Start").field("region", region).finish(),
            Self::Stop { region } => f.debug_struct("Stop").field("region", region).finish(),
            Self::Destroy { region } => f.debug_struct("Destroy").field("region", region).finish(),
            Self::RefreshResolver { region } => f
                .debug_struct("RefreshResolver")
                .field("region", region)
                .finish(),
            Self::NotifyFailToStartObserve {
                region,
                handle,
                err,
            } => f
                .debug_struct("NotifyFailToStartObserve")
                .field("region", region)
                .field("handle", handle)
                .field("err", err)
                .finish(),
            Self::ResolveRegions { min_ts, .. } => f
                .debug_struct("ResolveRegions")
                .field("min_ts", min_ts)
                .field("callback", &format_args!("fn {{ .. }}"))
                .finish(),
        }
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WatchTask(arg0) => f.debug_tuple("WatchTask").field(arg0).finish(),
            Self::BatchEvent(arg0) => f
                .debug_tuple("BatchEvent")
                .field(&format!("[{} events...]", arg0.len()))
                .finish(),
            Self::ChangeConfig(arg0) => f.debug_tuple("ChangeConfig").field(arg0).finish(),
            Self::Flush(arg0) => f.debug_tuple("Flush").field(arg0).finish(),
            Self::ModifyObserve(op) => f.debug_tuple("ModifyObserve").field(op).finish(),
            Self::ForceFlush(arg0) => f.debug_tuple("ForceFlush").field(arg0).finish(),
            Self::FatalError(task, err) => {
                f.debug_tuple("FatalError").field(task).field(err).finish()
            }
            Self::Sync(..) => f.debug_tuple("Sync").finish(),
            Self::MarkFailover(t) => f
                .debug_tuple("MarkFailover")
                .field(&format_args!("{:?} ago", t.saturating_elapsed()))
                .finish(),
            Self::FlushWithMinTs(arg0, arg1) => f
                .debug_tuple("FlushWithMinTs")
                .field(arg0)
                .field(arg1)
                .finish(),
            Self::RegionCheckpointsOp(s) => f.debug_tuple("GetRegionCheckpoints").field(s).finish(),
            Self::UpdateGlobalCheckpoint(task) => {
                f.debug_tuple("UpdateGlobalCheckpoint").field(task).finish()
            }
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Task {
    fn label(&self) -> &'static str {
        match self {
            Task::WatchTask(w) => match w {
                TaskOp::AddTask(_) => "watch_task.add",
                TaskOp::RemoveTask(_) => "watch_task.remove",
                TaskOp::PauseTask(_) => "watch_task.pause",
                TaskOp::ResumeTask(_) => "watch_task.resume",
            },
            Task::BatchEvent(_) => "batch_event",
            Task::ChangeConfig(_) => "change_config",
            Task::Flush(_) => "flush",
            Task::ModifyObserve(o) => match o {
                ObserveOp::Start { .. } => "modify_observe.start",
                ObserveOp::Stop { .. } => "modify_observe.stop",
                ObserveOp::Destroy { .. } => "modify_observe.destroy",
                ObserveOp::RefreshResolver { .. } => "modify_observe.refresh_resolver",
                ObserveOp::NotifyFailToStartObserve { .. } => "modify_observe.retry",
                ObserveOp::ResolveRegions { .. } => "modify_observe.resolve",
            },
            Task::ForceFlush(_) => "force_flush",
            Task::FatalError(..) => "fatal_error",
            Task::Sync(..) => "sync",
            Task::MarkFailover(_) => "mark_failover",
            Task::FlushWithMinTs(..) => "flush_with_min_ts",
            Task::RegionCheckpointsOp(..) => "get_checkpoints",
            Task::UpdateGlobalCheckpoint(..) => "update_global_checkpoint",
        }
    }
}

impl<S, R, E, RT, PDC> Runnable for Endpoint<S, R, E, RT, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        self.run_task(task)
    }
}
