// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::AsRef;
use std::fmt;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use concurrency_manager::ConcurrencyManager;

use engine_traits::KvEngine;

use error_code::ErrorCodeExt;
use kvproto::brpb::StreamBackupError;
use kvproto::metapb::Region;
use pd_client::PdClient;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::ChangeObserver;

use tikv_util::time::Instant;

use tokio::io::Result as TokioResult;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use txn_types::TimeStamp;

use crate::errors::Error;
use crate::event_loader::InitialDataLoader;
use crate::metadata::store::{EtcdStore, MetaStore};
use crate::metadata::{MetadataClient, MetadataEvent, StreamTask};
use crate::router::{ApplyEvents, Router, FLUSH_STORAGE_INTERVAL};
use crate::subscription_track::SubscriptionTracer;
use crate::utils::{self, StopWatch};
use crate::{errors::Result, observer::BackupStreamObserver};
use crate::{metrics, try_send};

use online_config::ConfigChange;
use raftstore::coprocessor::{CmdBatch, ObserveHandle, RegionInfoProvider};
use tikv::config::BackupStreamConfig;

use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{debug, error, info};
use tikv_util::{warn, HandyRwLock};

use super::metrics::{HANDLE_EVENT_DURATION_HISTOGRAM, HANDLE_KV_HISTOGRAM};

const SLOW_EVENT_THRESHOLD: f64 = 120.0;

pub struct Endpoint<S: MetaStore + 'static, R, E, RT, PDC> {
    #[allow(dead_code)]
    config: BackupStreamConfig,
    meta_client: Option<MetadataClient<S>>,
    range_router: Router,
    #[allow(dead_code)]
    scheduler: Scheduler<Task>,
    #[allow(dead_code)]
    observer: BackupStreamObserver,
    pool: Runtime,
    store_id: u64,
    regions: R,
    engine: PhantomData<E>,
    router: RT,
    pd_client: Arc<PDC>,
    subs: SubscriptionTracer,
    concurrency_manager: ConcurrencyManager,
}

impl<S, R, E, RT, PDC> Endpoint<S, R, E, RT, PDC>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
    S: MetaStore + 'static,
{
    pub fn with_client(
        store_id: u64,
        cli: MetadataClient<S>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
        pd_client: Arc<PDC>,
        cm: ConcurrencyManager,
    ) -> Self {
        let pool = create_tokio_runtime(config.num_threads, "br-stream")
            .expect("failed to create tokio runtime for backup stream worker.");

        // TODO consider TLS?
        let meta_client = Some(cli);
        let range_router = Router::new(
            PathBuf::from(config.temp_path.clone()),
            scheduler.clone(),
            config.temp_file_size_limit_per_task.0,
        );

        if let Some(meta_client) = meta_client.as_ref() {
            // spawn a worker to watch task changes from etcd periodically.
            let meta_client_clone = meta_client.clone();
            let scheduler_clone = scheduler.clone();
            // TODO build a error handle mechanism #error 2
            pool.spawn(async {
                if let Err(err) =
                    Self::start_and_watch_tasks(meta_client_clone, scheduler_clone).await
                {
                    err.report("failed to start watch tasks");
                }
            });
            pool.spawn(Self::starts_flush_ticks(range_router.clone()));
        }

        info!("the endpoint of backup stream started"; "path" => %config.temp_path);
        Endpoint {
            config,
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
            subs: Default::default(),
            concurrency_manager: cm,
        }
    }
}

impl<R, E, RT, PDC> Endpoint<EtcdStore, R, E, RT, PDC>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
    PDC: PdClient + 'static,
{
    pub fn new<S: AsRef<str>>(
        store_id: u64,
        endpoints: &dyn AsRef<[S]>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
        pd_client: Arc<PDC>,
        concurrency_manager: ConcurrencyManager,
    ) -> Endpoint<EtcdStore, R, E, RT, PDC> {
        let pool = create_tokio_runtime(config.num_threads, "backup-stream")
            .expect("failed to create tokio runtime for backup stream worker.");

        // TODO consider TLS?
        let meta_client = match pool.block_on(etcd_client::Client::connect(&endpoints, None)) {
            Ok(c) => {
                let meta_store = EtcdStore::from(c);
                Some(MetadataClient::new(meta_store, store_id))
            }
            Err(e) => {
                error!("failed to create etcd client for backup stream worker"; "error" => ?e);
                None
            }
        };

        let range_router = Router::new(
            PathBuf::from(config.temp_path.clone()),
            scheduler.clone(),
            config.temp_file_size_limit_per_task.0,
        );

        if let Some(meta_client) = meta_client.as_ref() {
            // spawn a worker to watch task changes from etcd periodically.
            let meta_client_clone = meta_client.clone();
            let scheduler_clone = scheduler.clone();
            // TODO build a error handle mechanism #error 2
            pool.spawn(async {
                if let Err(err) =
                    Self::start_and_watch_tasks(meta_client_clone, scheduler_clone).await
                {
                    err.report("failed to start watch tasks");
                }
            });

            pool.spawn(Self::starts_flush_ticks(range_router.clone()));
        }

        info!("the endpoint of stream backup started"; "path" => %config.temp_path);
        Endpoint {
            config,
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
            subs: Default::default(),
            concurrency_manager,
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
        self.meta_client.as_ref().unwrap().clone()
    }

    fn on_fatal_error(&self, task: String, err: Box<Error>) {
        // Let's pause the task locally first.
        self.on_unregister(&task);

        let meta_cli = self.get_meta_client();
        let store_id = self.store_id;
        let sched = self.scheduler.clone();
        self.pool.block_on(async move {
            // TODO: also pause the task using the meta client.
            let err_fut = async {
                meta_cli.pause(&task).await?;
                let mut last_error = StreamBackupError::new();
                last_error.set_error_code(err.error_code().code.to_owned());
                last_error.set_error_message(err.to_string());
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
                    try_send!(sched, Task::FatalError(task, err));
                });
            }
        })
    }

    async fn starts_flush_ticks(router: Router) {
        loop {
            // wait 1min to trigger tick
            tokio::time::sleep(Duration::from_secs(FLUSH_STORAGE_INTERVAL / 5)).await;
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
        let mut watcher = meta_client.events_from(revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("backup stream watch event from etcd"; "event" => ?event);
                match event {
                    MetadataEvent::AddTask { task } => {
                        scheduler.schedule(Task::WatchTask(TaskOp::AddTask(task)))?;
                    }
                    MetadataEvent::RemoveTask { task } => {
                        scheduler.schedule(Task::WatchTask(TaskOp::RemoveTask(task)))?;
                    }
                    MetadataEvent::Error { err } => err.report("metadata client watch meet error"),
                    _ => panic!("BUG: invalid event {:?}", event),
                }
            }
        }
    }

    async fn starts_watch_pause(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
        revision: i64,
    ) -> Result<()> {
        let mut watcher = meta_client.events_from_pause(revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("backup stream watch event from etcd"; "event" => ?event);
                match event {
                    MetadataEvent::PauseTask { task } => {
                        scheduler.schedule(Task::WatchTask(TaskOp::PauseTask(task)))?;
                    }
                    MetadataEvent::ResumeTask { task } => {
                        let task = meta_client.get_task(&task).await?;
                        scheduler.schedule(Task::WatchTask(TaskOp::ResumeTask(task)))?;
                    }
                    MetadataEvent::Error { err } => err.report("metadata client watch meet error"),
                    _ => panic!("BUG: invalid event {:?}", event),
                }
            }
        }
    }

    fn backup_batch(&self, batch: CmdBatch) {
        let mut sw = StopWatch::new();
        let region_id = batch.region_id;
        let mut resolver = match self.subs.get_subscription_of(region_id) {
            Some(rts) => rts,
            None => {
                warn!("BUG: the region isn't registered (no resolver found) but sent to backup_batch."; "region_id" => %region_id);
                return;
            }
        };

        let kvs = ApplyEvents::from_cmd_batch(batch, resolver.value_mut().resolver());
        drop(resolver);
        if kvs.len() == 0 {
            return;
        }

        HANDLE_EVENT_DURATION_HISTOGRAM
            .with_label_values(&["to_stream_event"])
            .observe(sw.lap().as_secs_f64());
        let router = self.range_router.clone();
        self.pool.spawn(async move {
            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["get_router_lock"])
                .observe(sw.lap().as_secs_f64());
            let kv_count = kvs.len();
            let total_size = kvs.size();
            metrics::HEAP_MEMORY
                .with_label_values(&["alloc"])
                .inc_by(total_size as f64);
            if let Err(err) = router.on_events(kvs).await {
                err.report("failed to send event.");
            }
            metrics::HEAP_MEMORY
                .with_label_values(&["free"])
                .inc_by(total_size as f64);
            HANDLE_KV_HISTOGRAM.observe(kv_count as _);
            let time_cost = sw.lap().as_secs_f64();
            if time_cost > SLOW_EVENT_THRESHOLD {
                warn!("write to temp file too slow."; "time_cost" => ?time_cost, "region_id" => %region_id, "len" => %kv_count);
            }
            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["save_to_temp_file"])
                .observe(time_cost)
        });
    }

    /// Make an initial data loader using the resource of the endpoint.
    pub fn make_initial_loader(&self) -> InitialDataLoader<E, R, RT> {
        InitialDataLoader::new(
            self.router.clone(),
            self.regions.clone(),
            self.range_router.clone(),
            self.subs.clone(),
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
                self.on_unregister(&task_name);
            }
            TaskOp::ResumeTask(task) => {
                self.on_register(task);
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
        let mut start_ts = task.info.get_start_ts();
        // Should scan from checkpoint_ts rather than start_ts if checkpoint_ts exists in Metadata.
        if let Some(cli) = &self.meta_client {
            let checkpoint_ts = cli.progress_of_task(task.info.get_name()).await?;
            start_ts = start_ts.max(checkpoint_ts);
        }
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
        tokio::task::spawn_blocking(move || {
            let range_init_result =
                init.initialize_range(start_key.clone(), end_key.clone(), TimeStamp::new(start_ts));
            match range_init_result {
                Ok(stat) => {
                    info!("backup stream success to do initial scanning"; "stat" => ?stat,
                        "start_key" => utils::redact(&start_key),
                        "end_key" => utils::redact(&end_key),
                        "take" => ?start.saturating_elapsed(),)
                }
                Err(e) => {
                    e.report("backup stream do initial scanning failed");
                }
            }
        });
        Ok(())
    }

    // register task ranges
    pub fn on_register(&self, task: StreamTask) {
        if let Some(cli) = self.meta_client.as_ref() {
            let cli = cli.clone();
            let init = self.make_initial_loader();
            let range_router = self.range_router.clone();

            info!(
                "register backup stream task";
                "task" => ?task,
            );

            self.pool.block_on(async move {
                let task_name = task.info.get_name();
                match cli.ranges_of_task(task_name).await {
                    Ok(ranges) => {
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
                        if let Err(err) = range_router
                            .register_task(task.clone(), ranges.clone())
                            .await
                        {
                            err.report(format!(
                                "failed to register backup stream task {}",
                                task.info.name
                            ));
                            return;
                        }

                        for (start_key, end_key) in ranges {
                            let init = init.clone();

                            self.observe_and_scan_region(init, &task, start_key, end_key)
                                .await
                                .unwrap();
                        }
                        info!(
                            "finish register backup stream ranges";
                            "task" => ?task,
                        );
                    }
                    Err(e) => {
                        e.report(format!(
                            "failed to register backup stream task {} to router: ranges not found",
                            task.info.get_name()
                        ));
                        // TODO build a error handle mechanism #error 5
                    }
                }
            });
        };
    }

    pub fn on_unregister(&self, task: &str) {
        let router = self.range_router.clone();

        self.pool.block_on(async move {
            router.unregister_task(task).await;
        });
    }

    /// try advance the resolved ts by the pd tso.
    async fn try_resolve(
        cm: &ConcurrencyManager,
        pd_client: Arc<PDC>,
        resolvers: SubscriptionTracer,
    ) -> TimeStamp {
        let pd_tso = pd_client
            .get_tso()
            .await
            .map_err(|err| Error::from(err).report("failed to get tso from pd"))
            .unwrap_or_default();
        let min_ts = cm.global_min_lock_ts().unwrap_or(TimeStamp::max());
        let tso = Ord::min(pd_tso, min_ts);
        let ts = resolvers.resolve_with(tso);
        resolvers.warn_if_gap_too_huge(ts);
        ts
    }

    async fn flush_for_task(
        task: String,
        store_id: u64,
        router: Router,
        pd_cli: Arc<PDC>,
        resolvers: SubscriptionTracer,
        meta_cli: MetadataClient<S>,
        concurrency_manager: ConcurrencyManager,
    ) {
        let start = Instant::now_coarse();
        // NOTE: Maybe push down the resolve step to the router?
        //       Or if there are too many duplicated `Flush` command, we may do some useless works.
        let new_rts = Self::try_resolve(&concurrency_manager, pd_cli.clone(), resolvers).await;
        metrics::FLUSH_DURATION
            .with_label_values(&["resolve_by_now"])
            .observe(start.saturating_elapsed_secs());
        if let Some(rts) = router.do_flush(&task, store_id, new_rts).await {
            info!("flushing and refreshing checkpoint ts.";
                "checkpoint_ts" => %rts,
                "task" => %task,
            );
            if rts == 0 {
                // We cannot advance the resolved ts for now.
                return;
            }
            concurrency_manager.update_max_ts(TimeStamp::new(rts));
            if let Err(err) = pd_cli
                .update_service_safe_point(
                    format!("backup-stream-{}-{}", task, store_id),
                    TimeStamp::new(rts),
                    Duration::from_secs(600),
                )
                .await
            {
                Error::from(err).report("failed to update service safe point!");
                // don't give up?
            }
            if let Err(err) = meta_cli.step_task(&task, rts).await {
                err.report(format!("on flushing task {}", task));
                // we can advance the progress at next time.
                // return early so we won't be mislead by the metrics.
                return;
            }
            metrics::STORE_CHECKPOINT_TS
                // Currently, we only support one task at the same time,
                // so use the task as label would be ok.
                .with_label_values(&[task.as_str()])
                .set(rts as _)
        }
    }

    pub fn on_force_flush(&self, task: String, store_id: u64) {
        let router = self.range_router.clone();
        let cli = self
            .meta_client
            .as_ref()
            .expect("on_flush: executed from an endpoint without cli")
            .clone();
        let pd_cli = self.pd_client.clone();
        let resolvers = self.subs.clone();
        let cm = self.concurrency_manager.clone();
        self.pool.spawn(async move {
            let info = router.get_task_info(&task).await;
            // This should only happen in testing, it would be to unwrap...
            let _ = info.unwrap().set_flushing_status_cas(false, true);
            Self::flush_for_task(task, store_id, router, pd_cli, resolvers, cli, cm).await;
        });
    }

    pub fn on_flush(&self, task: String, store_id: u64) {
        let router = self.range_router.clone();
        let cli = self
            .meta_client
            .as_ref()
            .expect("on_flush: executed from an endpoint without cli")
            .clone();
        let pd_cli = self.pd_client.clone();
        let resolvers = self.subs.clone();
        let cm = self.concurrency_manager.clone();
        self.pool.spawn(Self::flush_for_task(
            task, store_id, router, pd_cli, resolvers, cli, cm,
        ));
    }

    /// Start observe over some region.
    /// This would modify some internal state, and delegate the task to InitialLoader::observe_over.
    fn observe_over(&self, region: &Region) -> Result<()> {
        let init = self.make_initial_loader();
        let handle = ObserveHandle::new();
        let region_id = region.get_id();
        self.subs.register_region(&region, handle.clone(), None);
        init.observe_over_with_retry(region, || {
            ChangeObserver::from_cdc(region_id, handle.clone())
        })?;
        Ok(())
    }

    fn observe_over_with_initial_data_from_checkpoint(
        &self,
        region: &Region,
        task: String,
    ) -> Result<()> {
        let init = self.make_initial_loader();

        let handle = ObserveHandle::new();
        let meta_cli = self.meta_client.as_ref().unwrap().clone();
        let last_checkpoint = TimeStamp::new(
            self.pool
                .block_on(meta_cli.global_progress_of_task(&task))?,
        );
        self.subs
            .register_region(&region, handle.clone(), Some(last_checkpoint));

        let region_id = region.get_id();
        let snap = init.observe_over_with_retry(region, move || {
            ChangeObserver::from_cdc(region_id, handle.clone())
        })?;
        let region = region.clone();

        // Note: Even we did the initial scanning, if the next_backup_ts was updated by periodic flushing,
        //       before the initial scanning done, there is still possibility of losing data:
        //       if the server crashes immediately, and data of this scanning hasn't been sent to sink,
        //       those data would be permanently lost.
        // Maybe we need block the next_backup_ts from advancing before all initial scanning done(Or just for the region, via disabling the resolver)?
        self.pool.spawn_blocking(move || {
            match init.do_initial_scan(&region, last_checkpoint, snap) {
                Ok(stat) => {
                    info!("initial scanning of leader transforming finished!"; "statistics" => ?stat, "region" => %region.get_id(), "from_ts" => %last_checkpoint);
                }
                Err(err) => err.report(format!("during initial scanning of region {:?}", region)),
            }
        });
        Ok(())
    }

    fn find_task_by_region(&self, r: &Region) -> Option<String> {
        self.range_router
            .find_task_by_range(&r.start_key, &r.end_key)
    }

    /// Modify observe over some region.
    /// This would register the region to the RaftStore.
    pub fn on_modify_observe(&self, op: ObserveOp) {
        info!("backup stream: on_modify_observe"; "op" => ?op);
        match op {
            ObserveOp::Start {
                region,
                needs_initial_scanning,
            } => {
                let result = if needs_initial_scanning {
                    let for_task = self.find_task_by_region(&region).unwrap_or_else(|| {
                        panic!(
                            "BUG: the region {:?} is register to no task but being observed",
                            region
                        )
                    });
                    self.observe_over_with_initial_data_from_checkpoint(&region, for_task)
                } else {
                    self.observe_over(&region)
                };
                if let Err(err) = result {
                    err.report(format!(
                        "during doing initial scanning for region {:?}",
                        region
                    ));
                }
            }
            ObserveOp::Stop { ref region } => {
                self.subs.deregister_region(region, |_, _| true);
            }
            ObserveOp::CheckEpochAndStop { ref region } => {
                self.subs.deregister_region(region, |old, new| {
                    raftstore::store::util::compare_region_epoch(
                        old.get_region_epoch(),
                        new,
                        true,
                        true,
                        false,
                    )
                    .map_err(|err| warn!("check epoch and stop failed."; "err" => %err))
                    .is_ok()
                });
            }
            ObserveOp::RefreshResolver { ref region } => {
                let need_refresh_all = !self.subs.try_update_region(&region);

                if need_refresh_all {
                    let canceled = self.subs.deregister_region(region, |_, _| true);
                    if canceled {
                        let for_task = self.find_task_by_region(&region).unwrap_or_else(|| {
                            panic!(
                                "BUG: the region {:?} is register to no task but being observed",
                                region
                            )
                        });
                        if let Err(e) =
                            self.observe_over_with_initial_data_from_checkpoint(&region, for_task)
                        {
                            e.report(format!(
                                "register region {} to raftstore when refreshing",
                                region.get_id()
                            ));
                        }
                    }
                }
            }
        }
    }

    pub fn run_task(&self, task: Task) {
        debug!("run backup stream task"; "task" => ?task);
        match task {
            Task::WatchTask(op) => self.handle_watch_task(op),
            Task::BatchEvent(events) => self.do_backup(events),
            Task::Flush(task) => self.on_flush(task, self.store_id),
            Task::ModifyObserve(op) => self.on_modify_observe(op),
            Task::ForceFlush(task) => self.on_force_flush(task, self.store_id),
            Task::FatalError(task, err) => self.on_fatal_error(task, err),
            _ => (),
        }
    }

    pub fn do_backup(&self, events: Vec<CmdBatch>) {
        for batch in events {
            self.backup_batch(batch)
        }
    }
}

/// Create a standard tokio runtime
/// (which allows io and time reactor, involve thread memory accessor),
fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        // Maybe make it more configurable?
        // currently, blocking threads would be used for incremental scanning.
        .max_blocking_threads(thread_count)
        .worker_threads(thread_count)
        .enable_io()
        .enable_time()
        .on_thread_start(|| {
            tikv_alloc::add_thread_memory_accessor();
        })
        .on_thread_stop(|| {
            tikv_alloc::remove_thread_memory_accessor();
        })
        .build()
}

pub enum Task {
    WatchTask(TaskOp),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(ConfigChange),
    /// Flush the task with name.
    Flush(String),
    /// Change the observe status of some region.
    ModifyObserve(ObserveOp),
    /// Convert status of some task into `flushing` and do flush then.
    ForceFlush(String),
    /// FatalError pauses the task and set the error.
    FatalError(String, Box<Error>),
}

#[derive(Debug)]
pub enum TaskOp {
    AddTask(StreamTask),
    RemoveTask(String),
    PauseTask(String),
    ResumeTask(StreamTask),
}

#[derive(Debug)]
pub enum ObserveOp {
    Start {
        region: Region,
        // if `true`, would scan and sink change from the global checkpoint ts.
        // Note: maybe we'd better make it Option<TimeStamp> to make it more generic,
        //       but that needs the `observer` know where the checkpoint is, which is a little dirty...
        needs_initial_scanning: bool,
    },
    Stop {
        region: Region,
    },
    CheckEpochAndStop {
        region: Region,
    },
    RefreshResolver {
        region: Region,
    },
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
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
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
