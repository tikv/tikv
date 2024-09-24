// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    any::Any,
    collections::HashSet,
    fmt,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use encryption::BackupEncryptionManager;
use engine_traits::KvEngine;
use error_code::ErrorCodeExt;
use futures::{stream::AbortHandle, FutureExt, TryFutureExt};
use kvproto::{
    brpb::{StreamBackupError, StreamBackupTaskInfo},
    metapb::{Region, RegionEpoch},
};
use pd_client::PdClient;
use raft::StateRole;
use raftstore::{
    coprocessor::{CmdBatch, ObserveHandle, RegionInfoProvider},
    router::CdcHandle,
};
use resolved_ts::{resolve_by_raft, LeadershipResolver};
use tikv::config::{BackupStreamConfig, ResolvedTsConfig};
use tikv_util::{
    box_err,
    config::ReadableDuration,
    debug, defer, info,
    memory::MemoryQuota,
    sys::thread::ThreadBuildWrapper,
    time::{Instant, Limiter},
    warn,
    worker::{Runnable, Scheduler},
    HandyRwLock,
};
use tokio::{
    io::Result as TokioResult,
    runtime::{Handle, Runtime},
    sync::{mpsc::Sender, Semaphore},
};
use tokio_stream::StreamExt;
use tracing::instrument;
use tracing_active_tree::root;
use txn_types::TimeStamp;

use super::metrics::HANDLE_EVENT_DURATION_HISTOGRAM;
use crate::{
    annotate,
    checkpoint_manager::{
        BasicFlushObserver, CheckpointManager, CheckpointV3FlushObserver, FlushObserver,
        GetCheckpointResult, RegionIdWithVersion, Subscription,
    },
    errors::{Error, ReportableResult, Result},
    event_loader::InitialDataLoader,
    future,
    metadata::{store::MetaStore, MetadataClient, MetadataEvent, StreamTask},
    metrics::{self, TaskStatus},
    observer::BackupStreamObserver,
    router::{self, ApplyEvents, Router, TaskSelector},
    subscription_manager::{RegionSubscriptionManager, ResolvedRegions},
    subscription_track::{Ref, RefMut, ResolveResult, SubscriptionTracer},
    try_send,
    utils::{self, FutureWaitGroup, StopWatch, Work},
};

const SLOW_EVENT_THRESHOLD: f64 = 120.0;
/// CHECKPOINT_SAFEPOINT_TTL_IF_ERROR specifies the safe point TTL(24 hour) if
/// task has fatal error.
const CHECKPOINT_SAFEPOINT_TTL_IF_ERROR: u64 = 24;

pub struct Endpoint<S, R, E: KvEngine, PDC> {
    // Note: those fields are more like a shared context between components.
    // For now, we copied them everywhere, maybe we'd better extract them into a
    // context type.
    pub(crate) meta_client: MetadataClient<S>,
    pub(crate) scheduler: Scheduler<Task>,
    pub(crate) store_id: u64,
    pub(crate) regions: R,
    pub(crate) engine: PhantomData<E>,
    pub(crate) pd_client: Arc<PDC>,
    pub(crate) subs: SubscriptionTracer,
    pub(crate) concurrency_manager: ConcurrencyManager,

    // Note: some of fields are public so test cases are able to access them.
    pub range_router: Router,
    observer: BackupStreamObserver,
    pool: Runtime,
    region_operator: Sender<ObserveOp>,
    failover_time: Option<Instant>,
    config: BackupStreamConfig,
    pub checkpoint_mgr: CheckpointManager,

    // Runtime status:
    /// The handle to abort last save storage safe point.
    /// This is used for simulating an asynchronous background worker.
    /// Each time we spawn a task, once time goes by, we abort that task.
    pub abort_last_storage_save: Option<AbortHandle>,
    pub initial_scan_semaphore: Arc<Semaphore>,
}

impl<S, R, E, PDC> Endpoint<S, R, E, PDC>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    PDC: PdClient + 'static,
    S: MetaStore + 'static,
{
    pub fn new<RT: CdcHandle<E> + 'static>(
        store_id: u64,
        store: S,
        config: BackupStreamConfig,
        resolved_ts_config: ResolvedTsConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
        pd_client: Arc<PDC>,
        concurrency_manager: ConcurrencyManager,
        resolver: BackupStreamResolver<RT, E>,
        backup_encryption_manager: BackupEncryptionManager,
    ) -> Self {
        crate::metrics::STREAM_ENABLED.inc();
        let pool = create_tokio_runtime((config.num_threads / 2).max(1), "backup-stream")
            .expect("failed to create tokio runtime for backup stream worker.");

        let meta_client = MetadataClient::new(store, store_id);
        let conf = router::Config::from(config.clone());
        let range_router = Router::new(scheduler.clone(), conf, backup_encryption_manager.clone());

        // spawn a worker to watch task changes from etcd periodically.
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();
        // TODO build a error handle mechanism #error 2
        pool.spawn(root!("flush_ticker"; Self::starts_flush_ticks(range_router.clone())));
        pool.spawn(root!("start_watch_tasks"; async {
            if let Err(err) = Self::start_and_watch_tasks(meta_client_clone, scheduler_clone).await
            {
                err.report("failed to start watch tasks");
            }
            info!("started task watcher!");
        }));

        let initial_scan_memory_quota = Arc::new(MemoryQuota::new(
            config.initial_scan_pending_memory_quota.0 as _,
        ));
        let limit = if config.initial_scan_rate_limit.0 > 0 {
            config.initial_scan_rate_limit.0 as f64
        } else {
            f64::INFINITY
        };
        let initial_scan_throughput_quota = Limiter::new(limit);
        info!("the endpoint of stream backup started"; "path" => %config.temp_path);
        let subs = SubscriptionTracer::default();

        let initial_scan_semaphore = Arc::new(Semaphore::new(config.initial_scan_concurrency));
        let (region_operator, op_loop) = RegionSubscriptionManager::start(
            InitialDataLoader::new(
                range_router.clone(),
                subs.clone(),
                scheduler.clone(),
                initial_scan_memory_quota,
                initial_scan_throughput_quota,
                // NOTE: in fact we can get rid of the `Arc`. Just need to warp the router when the
                // scanner pool is created. But at that time the handle has been sealed in the
                // `InitialScan` trait -- we cannot do that.
                Arc::new(Mutex::new(router)),
                Arc::clone(&initial_scan_semaphore),
            ),
            accessor.clone(),
            meta_client.clone(),
            ((config.num_threads + 1) / 2).max(1),
            resolver,
            resolved_ts_config.advance_ts_interval.0,
        );
        pool.spawn(root!(op_loop));
        let mut checkpoint_mgr = CheckpointManager::default();
        pool.spawn(root!(checkpoint_mgr.spawn_subscription_mgr()));
        let ep = Endpoint {
            initial_scan_semaphore,
            meta_client,
            range_router,
            scheduler,
            observer,
            pool,
            store_id,
            regions: accessor,
            engine: PhantomData,
            pd_client,
            subs,
            concurrency_manager,
            region_operator,
            failover_time: None,
            config,
            checkpoint_mgr,
            abort_last_storage_save: None,
        };
        ep.pool.spawn(root!(ep.min_ts_worker()));
        ep
    }
}

impl<S, R, E, PDC> Endpoint<S, R, E, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    PDC: PdClient + 'static,
{
    fn get_meta_client(&self) -> MetadataClient<S> {
        self.meta_client.clone()
    }

    fn on_fatal_error_of_task(&self, task: &str, err: &Error) -> future![()] {
        metrics::update_task_status(TaskStatus::Error, task);
        let meta_cli = self.get_meta_client();
        let pdc = self.pd_client.clone();
        let store_id = self.store_id;
        let sched = self.scheduler.clone();
        let safepoint_name = self.pause_guard_id_for_task(task);
        let safepoint_ttl = self.pause_guard_duration();
        let code = err.error_code().code.to_owned();
        let msg = err.to_string();
        let t = task.to_owned();
        let f = async move {
            let err_fut = async {
                let safepoint = meta_cli.global_progress_of_task(&t).await?;
                pdc.update_service_safe_point(
                    safepoint_name,
                    TimeStamp::new(safepoint.saturating_sub(1)),
                    safepoint_ttl,
                )
                .await
                .or_else(|err| match err {
                    pd_client::Error::UnsafeServiceGcSafePoint { .. } => {
                        warn!("gc safe point exceeds the task checkpoint. skipping uploading"; "err" => %err);
                        Ok(())
                    }
                    _ => Err(err),
                })?;
                meta_cli.pause(&t).await?;
                let mut last_error = StreamBackupError::new();
                last_error.set_error_code(code);
                last_error.set_error_message(msg.clone());
                last_error.set_store_id(store_id);
                last_error.set_happen_at(TimeStamp::physical_now());
                meta_cli.report_last_error(&t, last_error).await?;
                Result::Ok(())
            };
            if let Err(err_report) = err_fut.await {
                err_report.report(format_args!("failed to upload error {}", err_report));
                let name = t.to_owned();
                // Let's retry reporting after 5s.
                tokio::task::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    try_send!(
                        sched,
                        Task::FatalError(
                            TaskSelector::ByName(name),
                            Box::new(annotate!(err_report, "origin error: {}", msg))
                        )
                    );
                });
            }
        };
        tracing_active_tree::frame!("on_fatal_error_of_task"; f; %err, %task)
    }

    fn on_fatal_error(&self, select: TaskSelector, err: Box<Error>) {
        err.report_fatal();
        let tasks = self.range_router.select_task(select.reference());
        warn!("fatal error reporting"; "selector" => ?select, "selected" => ?tasks, "err" => %err);
        for task in tasks {
            // Let's pause the task first.
            self.unload_task(&task);
            self.pool.block_on(self.on_fatal_error_of_task(&task, &err));
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
    #[instrument(skip_all)]
    async fn start_and_watch_tasks(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
    ) -> Result<()> {
        let tasks;
        loop {
            let r = meta_client.get_tasks().await;
            match r {
                Ok(t) => {
                    tasks = t;
                    break;
                }
                Err(e) => {
                    e.report("failed to get backup stream task");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            }
        }

        for task in tasks.inner {
            info!("backup stream watch task"; "task" => ?task);
            if task.is_paused {
                continue;
            }
            // move task to schedule
            scheduler.schedule(Task::WatchTask(TaskOp::AddTask(task)))?;
        }

        let revision = tasks.revision;
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();

        Handle::current().spawn(root!("task_watcher"; async move {
            if let Err(err) =
                Self::starts_watch_task(meta_client_clone, scheduler_clone, revision).await
            {
                err.report("failed to start watch tasks");
            }
        }));

        Handle::current().spawn(root!("pause_watcher"; async move {
            if let Err(err) = Self::starts_watch_pause(meta_client, scheduler, revision).await {
                err.report("failed to start watch pause");
            }
        }));

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
                    e.report("failed to start watch task");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            info!("start watching the task changes."; "from_rev" => %revision_new);

            loop {
                if let Some(event) = watcher.stream.next().await {
                    info!("backup stream watch task from etcd"; "event" => ?event);

                    let revision = meta_client.get_reversion().await;
                    if let Ok(r) = revision {
                        revision_new = r;
                        info!("update the revision"; "revision" => revision_new);
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
            info!("start watching the pausing events."; "from_rev" => %revision_new);

            loop {
                if let Some(event) = watcher.stream.next().await {
                    info!("backup stream watch pause from etcd"; "event" => ?event);
                    let revision = meta_client.get_reversion().await;
                    if let Ok(r) = revision {
                        revision_new = r;
                        info!("update the revision"; "revision" => revision_new);
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

    fn flush_observer(&self) -> impl FlushObserver {
        let basic = BasicFlushObserver::new(self.pd_client.clone(), self.store_id);
        CheckpointV3FlushObserver::new(self.scheduler.clone(), self.meta_client.clone(), basic)
    }

    /// Convert a batch of events to the cmd batch, and update the resolver
    /// status.
    fn record_batch(subs: SubscriptionTracer, batch: CmdBatch) -> Result<ApplyEvents> {
        let region_id = batch.region_id;
        let mut resolver = match subs.get_subscription_of(region_id) {
            Some(rts) => rts,
            None => {
                debug!("the region isn't registered (no resolver found) but sent to backup_batch, maybe stale."; "region_id" => %region_id);
                // Sadly, we know nothing about the epoch in this context. Thankfully this is a
                // local error and won't be sent to outside.
                return Err(Error::ObserveCanceled(region_id, RegionEpoch::new()));
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
            return Err(Error::ObserveCanceled(region_id, RegionEpoch::new()));
        }

        let kvs = ApplyEvents::from_cmd_batch(batch, resolver.value_mut().resolver())?;
        Ok(kvs)
    }

    fn backup_batch(&self, batch: CmdBatch, work: Work) {
        let mut sw = StopWatch::by_now();

        let router = self.range_router.clone();
        let sched = self.scheduler.clone();
        let subs = self.subs.clone();
        let region_op = self.region_operator.clone();
        let region = batch.region_id;
        let from_idx = batch.cmds.first().map(|c| c.index).unwrap_or(0);
        let (to_idx, term) = batch
            .cmds
            .last()
            .map(|c| (c.index, c.term))
            .unwrap_or((0, 0));
        self.pool.spawn(root!("backup_batch"; async move {
            let region_id = batch.region_id;
            let kvs = Self::record_batch(subs, batch);
            let kvs = match kvs {
                Err(Error::OutOfQuota { region_id }) => {
                    region_op.send(ObserveOp::HighMemUsageWarning { region_id }).await
                        .map_err(|err| Error::Other(box_err!("failed to send, are we shutting down? {}", err)))
                        .report_if_err("");
                    return
                }
                Err(Error::ObserveCanceled(..)) => {
                    return;
                }
                Err(err) => {
                    err.report(format_args!("unexpected error during handing region event for {}.", region_id));
                    return;
                }
                Ok(batch) => {
                    if batch.is_empty() {
                        return
                    }
                    batch
                }
            };

            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["to_stream_event"])
                .observe(sw.lap().as_secs_f64());
            let kv_count = kvs.len();
            let total_size = kvs.size();
            metrics::HEAP_MEMORY
                .add(total_size as _);
            #[cfg(feature = "failpoints")]
            tokio::time::sleep(Duration::from_millis((|| {
                fail::fail_point!("log_backup_batch_delay", |val| val.and_then( |x| x.parse::<u64>().ok()).unwrap_or(0));
                0
            })())).await;
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
        }; from_idx, to_idx, region, current_term = term));
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

    async fn observe_regions_in_range(
        &self,
        task: &StreamTask,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) {
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
        let range_init_result = self
            .initialize_range(start_key.clone(), end_key.clone())
            .await;
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
    }

    /// initialize a range: it simply scan the regions with leader role and send
    /// them to [`initialize_region`].
    pub async fn initialize_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        // Generally we will be very very fast to consume.
        // Directly clone the initial data loader to the background thread looks a
        // little heavier than creating a new channel. TODO: Perhaps we need a
        // handle to the `InitialDataLoader`. Making it a `Runnable` worker might be a
        // good idea.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.regions
            .seek_region(
                &start_key,
                Box::new(move |i| {
                    // Ignore the error, this can only happen while the server is shutting down, the
                    // future has been canceled.
                    let _ = i
                        .filter(|r| r.role == StateRole::Leader)
                        .take_while(|r| r.region.start_key < end_key)
                        .try_for_each(|r| {
                            tx.blocking_send(ObserveOp::Start {
                                region: r.region.clone(),
                                handle: ObserveHandle::new(),
                            })
                        });
                }),
            )
            .map_err(|err| {
                Error::Other(box_err!(
                    "failed to seek region for start key {}: {}",
                    utils::redact(&start_key),
                    err
                ))
            })?;
        // Don't reschedule this command: or once the endpoint's mailbox gets
        // full, the system might deadlock.
        while let Some(cmd) = rx.recv().await {
            self.region_op(cmd).await;
        }
        Ok(())
    }

    /// send an operation request to the manager.
    /// the returned future would be resolved after send is success.
    /// the operation would be executed asynchronously.
    async fn region_op(&self, cmd: ObserveOp) {
        self.region_operator
            .send(cmd)
            .await
            .map_err(|err| {
                Error::Other(
                    format!("cannot send to region operator, are we shutting down? ({err})").into(),
                )
            })
            .report_if_err("send region cmd")
    }

    // register task ranges
    pub fn on_register(&self, task: StreamTask) {
        let name = task.info.name.clone();
        let start_ts = task.info.start_ts;
        self.register_stream_task(task);

        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[name.as_str()])
            .set(start_ts as _);
    }

    /// Load the task into memory: this would make the endpoint start to
    /// observe.
    fn register_stream_task(&self, task: StreamTask) {
        let cli = self.meta_client.clone();
        let range_router = self.range_router.clone();

        info!(
            "register backup stream task";
            "task" => ?task,
        );

        let task_name = task.info.get_name().to_owned();
        self.clean_pause_guard_id_for_task(&task_name);
        self.pool.block_on(async move {
            let task_name_clone = task.info.get_name().to_owned();
            let run = async move {
                let ranges = cli.ranges_of_task(task.info.get_name()).await?;
                fail::fail_point!("load_task::error_when_fetching_ranges", |_| {
                    Err(Error::Other("what range? no such thing, go away.".into()))
                });
                info!(
                    "register backup stream ranges";
                    "task" => ?task,
                    "ranges_count" => ranges.inner.len(),
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
                    self.observe_regions_in_range(&task, start_key, end_key)
                        .await
                }
                info!(
                    "finish register backup stream ranges";
                    "task" => ?task,
                );
                Ok(())
            };
            if let Err(e) = run.await {
                self.on_fatal_error_of_task(&task_name_clone, &Box::new(e))
                    .await;
            }
        });
        metrics::update_task_status(TaskStatus::Running, &task_name);
    }

    // clean the safepoint created at pause(if there is)
    fn clean_pause_guard_id_for_task(&self, task_name: &str) {
        self.pool.spawn(root!("unregister_task";
        self.pd_client
            .update_service_safe_point(
                self.pause_guard_id_for_task(task_name),
                TimeStamp::zero(),
                Duration::new(0, 0),
            )
            .map(|r| {
                r.map_err(|err| Error::from(err).report("removing safe point for pausing"))
            })
        ));
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
            Ok(Some(stream_task)) => self.register_stream_task(stream_task),
            Ok(None) => {
                info!("backup stream task not existed"; "task" => %task_name);
            }
            Err(err) => {
                err.report(format!("failed to resume backup stream task {}", task_name));
                let sched = self.scheduler.clone();
                self.pool.spawn(root!("retry_resume"; async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    sched
                        .schedule(Task::WatchTask(TaskOp::ResumeTask(task_name)))
                        .unwrap();
                }));
            }
        }
    }

    pub fn on_unregister(&self, task_name: &str) -> Option<StreamBackupTaskInfo> {
        let info = self.unload_task(task_name);
        self.clean_pause_guard_id_for_task(task_name);
        self.remove_metrics_after_unregister(task_name);
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
        router.unregister_task(task)
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

    fn do_flush(&self, task: String, mut resolved: ResolvedRegions) -> future![Result<()>] {
        let router = self.range_router.clone();
        let store_id = self.store_id;
        let mut flush_ob = self.flush_observer();
        async move {
            let mut new_rts = resolved.global_checkpoint();
            fail::fail_point!("delay_on_flush");
            flush_ob.before(resolved.take_resolve_result()).await;
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
            let handler_res = self.range_router.get_task_handler(&task);
            // This should only happen in testing, it would be to unwrap...
            let _ = handler_res.unwrap().set_flushing_status_cas(false, true);
            let mts = self.prepare_min_ts().await;
            let sched = self.scheduler.clone();
            self.region_op(ObserveOp::ResolveRegions {
                callback: Box::new(move |res| {
                    try_send!(sched, Task::ExecFlush(task, res));
                }),
                min_ts: mts,
            })
            .await;
        });
    }

    pub fn on_flush(&self, task: String) {
        self.pool.block_on(async move {
            let mts = self.prepare_min_ts().await;
            let sched = self.scheduler.clone();
            info!("min_ts prepared for flushing"; "min_ts" => %mts);
            self.region_op(ObserveOp::ResolveRegions {
                callback: Box::new(move |res| {
                    try_send!(sched, Task::ExecFlush(task, res));
                }),
                min_ts: mts,
            })
            .await
        })
    }

    fn on_exec_flush(&mut self, task: String, resolved: ResolvedRegions) {
        self.checkpoint_mgr.freeze();
        self.pool
            .spawn(root!("flush"; self.do_flush(task, resolved).map(|r| {
                if let Err(err) = r {
                    err.report("during updating flush status")
                }
            })));
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
                                    "global_checkpoint" => global_checkpoint,
                                    "err" => ?err,
                                );
                            }
                        }
                        Ok(false) => {
                            debug!("backup stream no need update global checkpoint.";
                                "task" => ?task,
                                "global_checkpoint" => global_checkpoint,
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

    fn on_update_global_checkpoint(&mut self, task: String) {
        if let Some(handle) = self.abort_last_storage_save.take() {
            handle.abort();
        }
        let (fut, handle) = futures::future::abortable(self.update_global_checkpoint(task));
        self.pool.spawn(root!("update_global_checkpoint"; fut));
        self.abort_last_storage_save = Some(handle);
    }

    fn on_update_change_config(&mut self, cfg: BackupStreamConfig) {
        let concurrency_diff =
            cfg.initial_scan_concurrency as isize - self.config.initial_scan_concurrency as isize;
        info!(
            "update log backup config";
             "config" => ?cfg,
             "concurrency_diff" => concurrency_diff,
        );
        self.range_router.update_config(&cfg);
        self.update_semaphore_capacity(&self.initial_scan_semaphore, concurrency_diff);

        self.config = cfg;
    }

    /// Modify observe over some region.
    /// This would register the region to the RaftStore.
    pub fn on_modify_observe(&self, op: ObserveOp) {
        self.pool
            .block_on(self.region_operator.send(op))
            .map_err(|err| {
                Error::Other(box_err!(
                    "cannot send to region operator, are we shutting down? ({})",
                    err
                ))
            })
            .report_if_err("during on_modify_observe");
    }

    fn update_semaphore_capacity(&self, sema: &Arc<Semaphore>, diff: isize) {
        use std::cmp::Ordering::*;
        match diff.cmp(&0) {
            Less => {
                self.pool.spawn(root!(
                    Arc::clone(sema)
                    .acquire_many_owned(-diff as _)
                    // It is OK to trivially ignore the Error case (semaphore has been closed, we are shutting down the server.)
                    .map_ok(|p| p.forget())
                ));
            }
            Equal => {}
            Greater => {
                sema.add_permits(diff as _);
            }
        }
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
            Task::ChangeConfig(cfg) => {
                self.on_update_change_config(cfg);
            }
            Task::Sync(cb, mut cond) => {
                if cond(self) {
                    cb()
                } else {
                    let sched = self.scheduler.clone();
                    self.pool.spawn(root!(async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        sched.schedule(Task::Sync(cb, cond)).unwrap();
                    }));
                }
            }
            Task::MarkFailover(t) => self.failover_time = Some(t),
            Task::ExecFlush(task, min_ts) => self.on_exec_flush(task, min_ts),
            Task::RegionCheckpointsOp(s) => self.handle_region_checkpoints_op(s),
            Task::UpdateGlobalCheckpoint(task) => self.on_update_global_checkpoint(task),
        }
    }

    fn min_ts_worker(&self) -> future![()] {
        let sched = self.scheduler.clone();
        let interval = self.config.min_ts_interval.0;
        async move {
            loop {
                tokio::time::sleep(interval).await;
                try_send!(
                    sched,
                    Task::RegionCheckpointsOp(RegionCheckpointOperation::PrepareMinTsForResolve)
                );
            }
        }
    }

    pub fn handle_region_checkpoints_op(&mut self, op: RegionCheckpointOperation) {
        match op {
            RegionCheckpointOperation::Resolved {
                checkpoints,
                start_time,
            } => {
                self.checkpoint_mgr.resolve_regions(checkpoints);
                metrics::MIN_TS_RESOLVE_DURATION.observe(start_time.saturating_elapsed_secs());
            }
            RegionCheckpointOperation::FlushWith(checkpoints) => {
                self.checkpoint_mgr.flush_and_notify(checkpoints);
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
            RegionCheckpointOperation::Subscribe(sub) => {
                let fut = self.checkpoint_mgr.add_subscriber(sub);
                self.pool.spawn(root!(async move {
                    if let Err(err) = fut.await {
                        err.report("adding subscription");
                    }
                }));
            }
            RegionCheckpointOperation::PrepareMinTsForResolve => {
                if self.observer.is_hibernating() {
                    metrics::MISC_EVENTS.skip_resolve_no_subscription.inc();
                    return;
                }
                let min_ts = self.pool.block_on(self.prepare_min_ts());
                let start_time = Instant::now();
                // We need to reschedule the `Resolve` task to queue, because the subscription
                // is asynchronous -- there may be transactions committed before
                // the min_ts we prepared but haven't been observed yet.
                try_send!(
                    self.scheduler,
                    Task::RegionCheckpointsOp(RegionCheckpointOperation::Resolve {
                        min_ts,
                        start_time
                    })
                );
            }
            #[allow(clippy::blocks_in_conditions)]
            RegionCheckpointOperation::Resolve { min_ts, start_time } => {
                let sched = self.scheduler.clone();
                try_send!(
                    self.scheduler,
                    Task::ModifyObserve(ObserveOp::ResolveRegions {
                        callback: Box::new(move |mut resolved| {
                            let t =
                                Task::RegionCheckpointsOp(RegionCheckpointOperation::Resolved {
                                    checkpoints: resolved.take_resolve_result(),
                                    start_time,
                                });
                            try_send!(sched, t);
                        }),
                        min_ts
                    })
                );
            }
        }
    }

    pub fn do_backup(&self, events: Vec<CmdBatch>) {
        let wg = FutureWaitGroup::new();
        for batch in events {
            self.backup_batch(batch, wg.clone().work());
        }
        self.pool.block_on(wg.wait())
    }
}

/// Create a standard tokio runtime
/// (which allows io and time reactor, involve thread memory accessor),
fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
    info!("create tokio runtime for backup stream"; "thread_name" => thread_name, "thread_count" => thread_count);

    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        // Maybe make it more configurable?
        // currently, blocking threads would be used for tokio local I/O.
        // (`File` API in `tokio::io` would use this pool.)
        .max_blocking_threads(thread_count * 8)
        .worker_threads(thread_count)
        .with_sys_hooks()
        .enable_io()
        .enable_time()
        .build()
}

pub enum BackupStreamResolver<RT, EK> {
    // for raftstore-v1, we use LeadershipResolver to check leadership of a region.
    V1(LeadershipResolver),
    // for raftstore-v2, it has less regions. we use CDCHandler to check leadership of a region.
    V2(RT, PhantomData<EK>),
    #[cfg(test)]
    // for some test cases, it is OK to don't check leader.
    Nop,
}

impl<RT, EK> BackupStreamResolver<RT, EK>
where
    RT: CdcHandle<EK> + 'static,
    EK: KvEngine,
{
    pub async fn resolve(
        &mut self,
        regions: Vec<u64>,
        min_ts: TimeStamp,
        timeout: Option<Duration>,
    ) -> Vec<u64> {
        match self {
            BackupStreamResolver::V1(x) => x.resolve(regions, min_ts, timeout).await,
            BackupStreamResolver::V2(x, _) => {
                let x = x.clone();
                resolve_by_raft(regions, min_ts, x).await
            }
            #[cfg(test)]
            BackupStreamResolver::Nop => regions,
        }
    }
}

#[derive(Debug)]
pub enum RegionSet {
    /// The universal set.
    Universal,
    /// A subset.
    Regions(HashSet<(u64, u64)>),
}

pub enum RegionCheckpointOperation {
    FlushWith(Vec<ResolveResult>),
    PrepareMinTsForResolve,
    Resolve {
        min_ts: TimeStamp,
        start_time: Instant,
    },
    Resolved {
        checkpoints: Vec<ResolveResult>,
        start_time: Instant,
    },
    Get(RegionSet, Box<dyn FnOnce(Vec<GetCheckpointResult>) + Send>),
    Subscribe(Subscription),
}

impl fmt::Debug for RegionCheckpointOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FlushWith(checkpoints) => f.debug_tuple("FlushWith").field(checkpoints).finish(),
            Self::Get(arg0, _) => f.debug_tuple("Get").field(arg0).finish(),

            Self::Subscribe(_) => f.debug_tuple("Subscription").finish(),
            Self::Resolved { checkpoints, .. } => {
                f.debug_tuple("Resolved").field(checkpoints).finish()
            }
            Self::PrepareMinTsForResolve => f.debug_tuple("PrepareMinTsForResolve").finish(),
            Self::Resolve { min_ts, .. } => {
                f.debug_struct("Resolve").field("min_ts", min_ts).finish()
            }
        }
    }
}

pub enum Task {
    WatchTask(TaskOp),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(BackupStreamConfig),
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
        // The argument should be `self`, but there are too many generic argument for `self`...
        // So let the caller in test cases downcast this to the type they need manually...
        Box<dyn FnMut(&mut dyn Any) -> bool + Send>,
    ),
    /// Mark the store as a failover store.
    /// This would prevent store from updating its checkpoint ts for a while.
    /// Because we are not sure whether the regions in the store have new leader
    /// -- we keep a safe checkpoint so they can choose a safe `from_ts` for
    /// initial scanning.
    MarkFailover(Instant),
    /// Flush the task with name.
    Flush(String),
    /// Execute the flush with the calculated resolved result.
    /// This is an internal command only issued by the `Flush` task.
    ExecFlush(String, ResolvedRegions),
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
        handle: ObserveHandle,
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
    NotifyStartObserveResult {
        region: Region,
        handle: ObserveHandle,
        err: Option<Box<Error>>,
    },
    ResolveRegions {
        callback: ResolveRegionsCallback,
        min_ts: TimeStamp,
    },
    HighMemUsageWarning {
        region_id: u64,
    },
}

impl std::fmt::Debug for ObserveOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start { region, handle } => f
                .debug_struct("Start")
                .field("region", &utils::debug_region(region))
                .field("handle", &handle)
                .finish(),
            Self::Stop { region } => f
                .debug_struct("Stop")
                .field("region", &utils::debug_region(region))
                .finish(),
            Self::Destroy { region } => f
                .debug_struct("Destroy")
                .field("region", &utils::debug_region(region))
                .finish(),
            Self::RefreshResolver { region } => f
                .debug_struct("RefreshResolver")
                .field("region", &utils::debug_region(region))
                .finish(),
            Self::NotifyStartObserveResult {
                region,
                handle,
                err,
            } => f
                .debug_struct("NotifyStartObserveResult")
                .field("region", &utils::debug_region(region))
                .field("handle", handle)
                .field("err", err)
                .finish(),
            Self::ResolveRegions { min_ts, .. } => f
                .debug_struct("ResolveRegions")
                .field("min_ts", min_ts)
                .field("callback", &format_args!("fn {{ .. }}"))
                .finish(),
            Self::HighMemUsageWarning {
                region_id: inconsistent_region_id,
            } => f
                .debug_struct("HighMemUsageWarning")
                .field("inconsistent_region", &inconsistent_region_id)
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
            Self::ExecFlush(arg0, arg1) => f
                .debug_tuple("ExecFlush")
                .field(arg0)
                .field(&arg1.global_checkpoint())
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
                ObserveOp::NotifyStartObserveResult { .. } => "modify_observe.retry",
                ObserveOp::ResolveRegions { .. } => "modify_observe.resolve",
                ObserveOp::HighMemUsageWarning { .. } => "modify_observe.high_mem",
            },
            Task::ForceFlush(..) => "force_flush",
            Task::FatalError(..) => "fatal_error",
            Task::Sync(..) => "sync",
            Task::MarkFailover(_) => "mark_failover",
            Task::ExecFlush(..) => "flush_with_min_ts",
            Task::RegionCheckpointsOp(..) => "get_checkpoints",
            Task::UpdateGlobalCheckpoint(..) => "update_global_checkpoint",
        }
    }
}

impl<S, R, E, PDC> Runnable for Endpoint<S, R, E, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    PDC: PdClient + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        self.run_task(task)
    }
}
