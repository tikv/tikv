// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::AsRef;
use std::fmt;
use std::marker::PhantomData;
use std::path::PathBuf;


use engine_traits::KvEngine;

use kvproto::metapb::Region;
use raftstore::router::RaftStoreRouter;

use raftstore::store::fsm::ChangeObserver;
use tikv_util::time::Instant;
use tokio::io::Result as TokioResult;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;
use txn_types::TimeStamp;

use crate::event_loader::InitialDataLoader;
use crate::metadata::store::{EtcdStore, MetaStore};
use crate::metadata::{MetadataClient, MetadataEvent, Task as MetaTask};
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
        let cli = match pool.block_on(etcd_client::Client::connect(&endpoints, None)) {
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

        if cli.is_none() {
            // unable to connect to etcd
            // may we should retry connect later
            // TODO build a error handle mechanism #error 1
            return Endpoint {
                config,
                meta_client: None,
                range_router,
                scheduler,
                observer,
                pool,
                store_id,
                regions: accessor,
                engine: PhantomData,
                router,
            };
        }

        let meta_client = cli.unwrap();
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
        Endpoint {
            config,
            meta_client: Some(meta_client),
            range_router,
            scheduler,
            observer,
            pool,
            store_id,
            regions: accessor,
            engine: PhantomData,
            router,
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
        let kvs = ApplyEvent::from_cmd_batch(batch, /* TODO */ 0);
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
                        err.report(format!("failed to send event."));
                    }
                    metrics::HEAP_MEMORY
                        .with_label_values(&["free"])
                        .inc_by(size as f64);
                    kv_count += 1;
                }
            }
            HANDLE_KV_HISTOGRAM.observe(kv_count as _);
            let time_cost = sw.lap().as_secs_f64();
            if time_cost > 120.0 {
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
    pub fn on_register(&self, task: MetaTask) {
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
                        range_router.register_ranges(task_name, ranges.clone());
                        for (start_key, end_key) in ranges {
                            let init = init.clone();
                            let start_key = start_key;
                            let end_key = end_key;
                            let start = Instant::now_coarse();
                            let start_ts = task.info.get_start_ts();
                            let ob = self.observer.clone();
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
                                    |region_id, handle| ob.subs.register_region(region_id, handle),
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

    pub async fn do_flush(router: Router, task: String) -> Result<()> {
        let temp_files = router.take_temporary_files(&task).await?;
        let meta = temp_files.generate_metadata().await?;
        // TODO flush the files to external storage
        info!("flushing data to external storage"; "local_file_simple" => ?meta.files.get(0));
        Ok(())
    }

    pub fn on_flush(&self, task: String) {
        let router = self.range_router.clone();
        self.pool.spawn(async move {
            // TODO handle the error
            let _ = Self::do_flush(router, task).await;
        });
    }

    /// Start observe over some region.
    /// This would register the region to the RaftStore.
    ///
    /// > Note: This won't trigger a incremental scanning.
    /// >      When the follower progress faster than leader and then be elected,
    /// >      there is a risk of losing data.
    pub fn on_observe_region(&self, region: Region) {
        let init = self.make_initial_loader();
        let handle = ObserveHandle::new();
        let region_id = region.get_id();
        let ob = ChangeObserver::from_cdc(region_id, handle.clone());
        if let Err(e) = init.observe_over(&region, ob) {
            e.report(format!("register region {} to raftstore", region.get_id()));
        }
        self.observer.subs.register_region(region_id, handle);
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
    WatchTask(MetaTask),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(ConfigChange),
    /// Flush the task with name.
    Flush(String),
    /// Start observe over the region.
    ObserverRegion {
        region: Region,
        // Note: Maybe add the request for initial scanning too.
        // needs_initial_scanning: bool
    },
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WatchTask(arg0) => f.debug_tuple("WatchTask").field(arg0).finish(),
            Self::BatchEvent(arg0) => f.debug_tuple("BatchEvent").field(arg0).finish(),
            Self::ChangeConfig(arg0) => f.debug_tuple("ChangeConfig").field(arg0).finish(),
            Self::Flush(arg0) => f.debug_tuple("Flush").field(arg0).finish(),
            Self::ObserverRegion { region } => f
                .debug_struct("ObserverRegion")
                .field("region", region)
                .finish(),
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
            Task::Flush(task) => self.on_flush(task),
            Task::ObserverRegion { region } => self.on_observe_region(region),
            _ => (),
        }
    }
}
