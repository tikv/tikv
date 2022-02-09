// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::AsRef;
use std::fmt;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use engine_traits::KvEngine;

use kvproto::metapb::Region;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::ChangeObserver;
use resolved_ts::Resolver;
use tikv_util::time::Instant;

use crossbeam_channel::tick;
use tokio::io::Result as TokioResult;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use txn_types::TimeStamp;

use crate::event_loader::InitialDataLoader;
use crate::metadata::store::{EtcdStore, MetaStore};
use crate::metadata::{MetadataClient, MetadataEvent, StreamTask};
use crate::metrics;
use crate::router::{ApplyEvent, Router};
use crate::utils::{self, StopWatch};
use crate::{errors::Result, observer::BackupStreamObserver};

use online_config::ConfigChange;
use raftstore::coprocessor::{CmdBatch, ObserveHandle, RegionInfoProvider};
use tikv::config::BackupStreamConfig;

use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{debug, error, info};
use tikv_util::{warn, HandyRwLock};

use super::metrics::{HANDLE_EVENT_DURATION_HISTOGRAM, HANDLE_KV_HISTOGRAM};

const SLOW_EVENT_THRESHOLD: f64 = 120.0;

pub struct Endpoint<S: MetaStore + 'static, R, E, RT> {
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
    resolvers: Arc<DashMap<u64, Resolver>>,
}

impl<R, E, RT> Endpoint<EtcdStore, R, E, RT>
where
    R: RegionInfoProvider + 'static + Clone,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
{
    pub fn new<S: AsRef<str>>(
        store_id: u64,
        endpoints: &dyn AsRef<[S]>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
        accessor: R,
        router: RT,
    ) -> Endpoint<EtcdStore, R, E, RT> {
        let pool = create_tokio_runtime(config.num_threads, "br-stream")
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
            PathBuf::from(config.streaming_path.clone()),
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
                    Endpoint::<_, R, E, RT>::starts_watch_tasks(meta_client_clone, scheduler_clone)
                        .await
                {
                    err.report("failed to start watch tasks");
                }
            });
            pool.spawn(Endpoint::<_, R, E, RT>::starts_flush_ticks(
                meta_client.clone(),
                scheduler.clone(),
                range_router.clone(),
            ));
        }

        info!("the endpoint of stream backup started"; "path" => %config.streaming_path);
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
            resolvers: Default::default(),
        }
    }
}

impl<S, R, E, RT> Endpoint<S, R, E, RT>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
{
    async fn starts_flush_ticks(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
        router: Router,
    ) -> Result<()> {
        let ticker = tick(Duration::from_secs(10));
        loop {
            // wait 10s to trigger tick
            let _ = ticker.recv().unwrap();
            debug!("backup stream trigger flush tick");
            match meta_client.get_tasks().await {
                Ok(tasks) => {
                    for task in tasks.inner {
                        debug!("backup stream get task in flush tick"; "task" => ?task);
                        router.get_task_info(&task.info.name).await.and_then(|task_info| {
                            if task_info.should_flush() && !task_info.is_flushing() && task_info.set_flushing_status_cas(false, true).is_ok() {
                                info!("backup stream trigger flush task by tick"; "task" => ?task);
                                scheduler.schedule(Task::Flush(task.info.name.to_string())).map_err(|e| {
                                    error!("backup stream schedule task failed"; "error" => ?e);
                                    e.into()
                                })
                            } else {
                                Ok(())
                            }
                        })?;
                    }
                }
                Err(e) => error!("backup stream get tasks failed"; "error" => ?e),
            }
        }
    }

    // TODO find a proper way to exit watch tasks
    async fn starts_watch_tasks(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
    ) -> Result<()> {
        let tasks = meta_client.get_tasks().await?;
        for task in tasks.inner {
            info!("backup stream watch task"; "task" => ?task);
            // move task to schedule
            scheduler.schedule(Task::WatchTask(task))?;
        }

        let mut watcher = meta_client.events_from(tasks.revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("backup stream watch event from etcd"; "event" => ?event);
                match event {
                    MetadataEvent::AddTask { task } => {
                        let t = meta_client.get_task(&task).await?;
                        scheduler.schedule(Task::WatchTask(t))?;
                    }
                    MetadataEvent::RemoveTask { task: _ } => {
                        // TODO implement remove task
                    }
                    MetadataEvent::Error { err } => err.report("metadata client watch meet error"),
                }
            }
        }
    }

    fn backup_batch(&self, batch: CmdBatch) {
        let mut sw = StopWatch::new();
        let region_id = batch.region_id;
        let mut resolver = match self.resolvers.as_ref().get_mut(&region_id) {
            Some(rts) => rts,
            None => {
                warn!("BUG: the region isn't registered (no resolver found) but sent to backup_batch."; "region_id" => %region_id);
                return;
            }
        };

        let kvs = ApplyEvent::from_cmd_batch(batch, resolver.value_mut());
        drop(resolver);

        HANDLE_EVENT_DURATION_HISTOGRAM
            .with_label_values(&["to_stream_event"])
            .observe(sw.lap().as_secs_f64());
        let router = self.range_router.clone();
        self.pool.spawn(async move {
            HANDLE_EVENT_DURATION_HISTOGRAM
                .with_label_values(&["get_router_lock"])
                .observe(sw.lap().as_secs_f64());
            let mut kv_count = 0;
            let total_size = kvs.as_slice().iter().fold(0usize, |init, kv| init + kv.size());
            metrics::HEAP_MEMORY
                .with_label_values(&["alloc"])
                .inc_by(total_size as f64);
            for kv in kvs {
                let size = kv.size();
                // TODO build a error handle mechanism #error 6
                if kv.should_record() {
                    if let Err(err) = router.on_event(kv).await {
                        err.report("failed to send event.");
                    }
                    metrics::HEAP_MEMORY
                        .with_label_values(&["free"])
                        .inc_by(size as f64);
                    kv_count += 1;
                }
            }
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
            self.store_id,
        )
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
                            err.report(format!("failed to register task {}", task.info.name))
                        }
                        for (start_key, end_key) in ranges {
                            let init = init.clone();
                            let start_key = start_key;
                            let end_key = end_key;
                            let start = Instant::now_coarse();
                            let start_ts = task.info.get_start_ts();
                            let ob = self.observer.clone();
                            let rs = self.resolvers.clone();
                            let success = self
                                .observer
                                .ranges
                                .wl()
                                .add((start_key.clone(), end_key.clone()));
                            if !success {
                                warn!("task ranges overlapped, which hasn't been supported for now";
                                    "task" => ?task,
                                    "start_key" => utils::redact(&start_key),
                                    "end_key" => utils::redact(&end_key),
                                );
                                continue;
                            }
                            tokio::task::spawn_blocking(move || {
                                let range_init_result = init.initialize_range(
                                    start_key.clone(),
                                    end_key.clone(),
                                    TimeStamp::new(start_ts),
                                    |region_id, handle| {
                                        // Note: maybe we'd better schedule a "register region" here?
                                        ob.subs.register_region(region_id, handle);
                                        rs.insert(region_id, Resolver::new(region_id));
                                    },
                                );
                                match range_init_result {
                                    Ok(stat) => {
                                        info!("success to do initial scanning"; "stat" => ?stat, 
                                            "start_key" => utils::redact(&start_key),
                                            "end_key" => utils::redact(&end_key),
                                            "take" => ?start.saturating_elapsed(),)
                                    }
                                    Err(e) => {
                                        e.report("failed to initialize regions");
                                    }
                                }
                            });
                        }
                        info!(
                            "finish register backup stream ranges";
                            "task" => ?task,
                        );
                    }
                    Err(e) => {
                        e.report(format!(
                            "failed to register task {} to router: ranges not found",
                            task.info.get_name()
                        ));
                        // TODO build a error handle mechanism #error 5
                    }
                }
            });
        };
    }

    pub fn on_flush(&self, task: String, store_id: u64) {
        let router = self.range_router.clone();
        let cli = self
            .meta_client
            .as_ref()
            .expect("on_flush: executed from an endpoint without cli")
            .clone();
        self.pool.spawn(async move {
            if let Some(rts) = router.do_flush(&task, store_id).await {
                if let Err(err) = cli.step_task(&task, rts).await {
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
        });
    }

    /// Start observe over some region.
    /// This would modify some internal state, and delegate the task to InitialLoader::observe_over.
    fn observe_over(&self, region: &Region) -> Result<()> {
        let init = self.make_initial_loader();
        let handle = ObserveHandle::new();
        let region_id = region.get_id();
        let ob = ChangeObserver::from_cdc(region_id, handle.clone());
        init.observe_over(region, ob)?;
        self.observer.subs.register_region(region_id, handle);
        self.resolvers.insert(region.id, Resolver::new(region.id));
        Ok(())
    }

    /// Modify observe over some region.
    /// This would register the region to the RaftStore.
    ///
    /// > Note: If using this to start observe, this won't trigger a incremental scanning.
    /// >      When the follower progress faster than leader and then be elected,
    /// >      there is a risk of losing data.
    pub fn on_modify_observe(&self, op: ObserveOp) {
        match op {
            ObserveOp::Start { region } => {
                if let Err(e) = self.observe_over(&region) {
                    e.report(format!("register region {} to raftstore", region.get_id()));
                }
            }
            ObserveOp::Stop { region } => {
                self.observer.subs.deregister_region(region.id);
                self.resolvers.as_ref().remove(&region.id);
            }
            ObserveOp::RefreshResolver { region } => {
                self.observer.subs.deregister_region(region.id);
                self.resolvers.as_ref().remove(&region.id);
                if let Err(e) = self.observe_over(&region) {
                    e.report(format!(
                        "register region {} to raftstore when refreshing",
                        region.get_id()
                    ));
                }
            }
        }
    }

    pub fn do_backup(&mut self, events: Vec<CmdBatch>) {
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
    WatchTask(StreamTask),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(ConfigChange),
    /// Flush the task with name.
    Flush(String),
    /// Change the observe status of some region.
    ModifyObserve(ObserveOp),
}

#[derive(Debug)]
pub enum ObserveOp {
    Start {
        region: Region,
        // Note: Maybe add the request for initial scanning too.
        // needs_initial_scanning: bool
    },
    Stop {
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
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<S, R, E, RT> Runnable for Endpoint<S, R, E, RT>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    E: KvEngine,
    RT: RaftStoreRouter<E> + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run backup stream task"; "task" => ?task);
        match task {
            Task::WatchTask(task) => self.on_register(task),
            Task::BatchEvent(events) => self.do_backup(events),
            Task::Flush(task) => self.on_flush(task, self.store_id),
            Task::ModifyObserve(op) => self.on_modify_observe(op),
            _ => (),
        }
    }
}
