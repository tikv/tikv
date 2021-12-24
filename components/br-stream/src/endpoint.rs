// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::path::PathBuf;

use tokio::io::Result as TokioResult;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;

use crate::metadata::store::{EtcdStore, MetaStore};
use crate::metadata::{MetadataClient, MetadataEvent, Task as MetaTask};
use crate::router::{ApplyEvent, Router};
use crate::utils;
use crate::{errors::Result, observer::BackupStreamObserver};

use online_config::ConfigChange;
use raftstore::coprocessor::CmdBatch;
use tikv::config::BackupStreamConfig;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{debug, error, info};

pub struct Endpoint<S: MetaStore + 'static> {
    #[allow(dead_code)]
    config: BackupStreamConfig,
    meta_client: Option<MetadataClient<S>>,
    range_router: Router,
    #[allow(dead_code)]
    scheduler: Scheduler<Task>,
    #[allow(dead_code)]
    observer: BackupStreamObserver,
    pool: Runtime,
}

impl Endpoint<EtcdStore> {
    pub fn new<E: AsRef<str>>(
        store_id: u64,
        endpoints: &dyn AsRef<[E]>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
    ) -> Endpoint<EtcdStore> {
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
            };
        }

        let meta_client = cli.unwrap();
        // spawn a worker to watch task changes from etcd periodically.
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();
        // TODO build a error handle mechanism #error 2
        pool.spawn(Endpoint::starts_watch_tasks(
            meta_client_clone,
            scheduler_clone,
        ));
        Endpoint {
            config,
            meta_client: Some(meta_client),
            range_router,
            scheduler,
            observer,
            pool,
        }
    }
}

impl<S> Endpoint<S>
where
    S: MetaStore + 'static,
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
            if let Err(e) = scheduler.schedule(Task::WatchTask(task)) {
                // TODO build a error handle mechanism #error 3
                error!("backup stream schedule task failed"; "error" => ?e);
            }
        }

        let mut watcher = meta_client.events_from(tasks.revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("backup stream watch event from etcd"; "event" => ?event);
                match event {
                    MetadataEvent::AddTask { task } => {
                        let t = meta_client.get_task(&task).await?;
                        if let Err(e) = scheduler.schedule(Task::WatchTask(t)) {
                            error!("backup stream schedule task failed"; "error" => ?e);
                        }
                    }
                    MetadataEvent::RemoveTask { task: _ } => {
                        // TODO implement remove task
                    }
                    MetadataEvent::Error { .. } => {
                        // TODO implement error
                    }
                }
            }
        }
    }
    // TODO use a more efficent encode kv event format
    // TODO move this function to a indepentent module.
    pub fn encode_event(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut buf = vec![];
        let key_len = (key.len() as u32).to_ne_bytes();
        let val_len = value.len().to_ne_bytes();
        buf.extend_from_slice(&key_len);
        buf.extend_from_slice(key);
        buf.extend_from_slice(&val_len);
        buf.extend_from_slice(value);
        buf
    }

    fn backup_batch(&self, batch: CmdBatch) {
        let kvs = ApplyEvent::from_cmd_batch(batch, /* TODO */ 0);
        let router = self.range_router.clone();
        self.pool.spawn(async move {
            let mut router = router.lock().await;
            for kv in kvs {
                // TODO build a error handle mechanism #error 6
                if kv.should_record() {
                    if let Err(err) = router.on_event(kv).await {
                        error!("backup stream failed in backup batch"; "error" => ?err);
                    }
                }
            }
        });
    }

    // register task ranges
    pub fn on_register(&self, task: MetaTask) {
        if let Some(cli) = self.meta_client.as_ref() {
            let cli = cli.clone();
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
                        // TODO implement register ranges
                        range_router.lock().await.register_ranges(
                            task_name,
                            ranges
                                .inner
                                .into_iter()
                                .map(|(start_key, end_key)| {
                                    (utils::wrap_key(start_key), utils::wrap_key(end_key))
                                })
                                .collect(),
                        );
                    }
                    Err(e) => {
                        error!("backup stream get tasks failed"; "error" => ?e);
                        // TODO build a error handle mechanism #error 5
                    }
                }
            });
        };
    }

    pub async fn do_flush(router: Router, task: String) -> Result<()> {
        let temp_files = {
            let mut router = router.lock().await;
            router.take_temporary_files(&task)?
        };
        let meta = temp_files.generate_metadata().await?;
        // TODO flush the files to external storage
        info!("flushing data to external storage"; "local_files" => ?meta);
        Ok(())
    }

    pub fn on_flush(&self, task: String) {
        let router = self.range_router.clone();
        self.pool.spawn(async move {
            // TODO handle the error
            let _ = Self::do_flush(router, task).await;
        });
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
        .enable_io()
        .enable_time()
        .on_thread_start(|| {
            tikv_alloc::add_thread_memory_accessor();
        })
        .on_thread_stop(|| {
            tikv_alloc::remove_thread_memory_accessor();
        })
        .worker_threads(thread_count)
        .build()
}

pub enum Task {
    WatchTask(MetaTask),
    BatchEvent(Vec<CmdBatch>),
    ChangeConfig(ConfigChange),
    /// Flush the task with name.
    Flush(String),
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("BackupStreamTask");
        match self {
            Task::WatchTask(t) => de
                .field("name", &t.info.name)
                .field("table_filter", &t.info.table_filter)
                .field("start_ts", &t.info.start_ts)
                .field("end_ts", &t.info.end_ts)
                .finish(),
            Task::BatchEvent(_) => de.field("name", &"batch_event").finish(),
            Task::ChangeConfig(change) => de
                .field("name", &"change_config")
                .field("change", change)
                .finish(),
            Task::Flush(task) => de.field("name", &"flush").field("task", &task).finish(),
        }
    }
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<S> Runnable for Endpoint<S>
where
    S: MetaStore,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run backup stream task"; "task" => ?task);
        match task {
            Task::WatchTask(task) => self.on_register(task),
            Task::BatchEvent(events) => self.do_backup(events),
            Task::Flush(task) => self.on_flush(task),
            _ => (),
        }
    }
}
