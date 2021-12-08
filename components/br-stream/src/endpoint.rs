// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use tokio::io::Result as TokioResult;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;

use crate::metadata::store::MetaStore;
use crate::metadata::{MetadataClient, MetadataEvent, Task as MetaTask};
use crate::{errors::Result, observer::BackupStreamObserver};
use online_config::ConfigChange;
use raftstore::coprocessor::CmdBatch;
use tikv::config::BackupStreamConfig;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{debug, error, info};

pub struct Endpoint<S: MetaStore + 'static> {
    #[allow(dead_code)]
    config: BackupStreamConfig,
    meta_client: MetadataClient<S>,
    #[allow(dead_code)]
    scheduler: Scheduler<Task>,
    #[allow(dead_code)]
    observer: BackupStreamObserver,
    pool: Runtime,
}

impl<S> Endpoint<S>
where
    S: MetaStore + 'static,
{
    pub fn new(
        endpoints: AsRef<E>,
        meta_client: MetadataClient<S>,
        config: BackupStreamConfig,
        scheduler: Scheduler<Task>,
        observer: BackupStreamObserver,
    ) -> Endpoint<S> {
        let pool = create_tokio_runtime(config.num_threads, "br-stream")
            .expect("failed to create tokio runtime for backup worker.");

        if let Ok(cli) = pool.block_on(etcd_client::Client::connect(&endpoints, None)) {

        }

        // spawn a worker to watch task changes from etcd periodically.
        let meta_client_clone = meta_client.clone();
        let scheduler_clone = scheduler.clone();
        pool.spawn(async move { Endpoint::starts_watch_tasks(meta_client_clone, scheduler_clone) });
        Endpoint {
            config,
            meta_client,
            scheduler,
            observer,
            pool,
        }
    }

    // TODO find a proper way to exit watch tasks
    async fn starts_watch_tasks(
        meta_client: MetadataClient<S>,
        scheduler: Scheduler<Task>,
    ) -> Result<()> {
        let tasks = meta_client.get_tasks().await?;
        let mut watcher = meta_client.events_from(tasks.revision).await?;
        for task in tasks.inner {
            info!("starts watch task {:?} from backup stream", task);
            // move task to schedule
            if let Err(e) = scheduler.schedule(Task::WatchTask(task)) {
                error!("backup stream schedule task failed"; "error" => ?e);
            }
        }
        loop {
            if let Some(event) = watcher.stream.next().await {
                debug!("backup stream received {:?} from etcd", event);
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

    #[allow(dead_code)]
    // keep ranges in memory to filter kv events not in these ranges.
    fn register_ranges(_ranges: Vec<(Vec<u8>, Vec<u8>)>) {
        // TODO reigister ranges to filter kv event
        // register ranges has two main purpose.
        // 1. filter kv event that no need to backup
        // 2. route kv event to the corresponding file.
        unimplemented!();
    }

    // register task ranges
    pub fn on_register(&self, task: MetaTask) {
        let cli = self.meta_client.clone();
        self.pool.spawn(async move {
            match cli.ranges_of_task(task.info.get_name()).await {
                Ok(_ranges) => {
                    debug!("backup stream register ranges to observer");
                    // TODO implement register ranges
                    // Endpoint::register_ranges(ranges.inner);
                }
                Err(e) => error!("backup stream register task failed"; "error" => ?e),
            }
        });
    }

    pub fn do_backup(&self, _events: Vec<CmdBatch>) {
        // TODO append events to local storage.
        unimplemented!();
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
        debug!("run backup-stream task"; "task" => ?task);
        match task {
            Task::WatchTask(task) => self.on_register(task),
            Task::BatchEvent(events) => self.do_backup(events),
            _ => (),
        }
    }
}
