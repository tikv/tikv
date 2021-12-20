// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::{fmt, io::Write};

use engine_traits::CF_LOCK;

use tokio::io::Result as TokioResult;
use tokio::runtime::Runtime;
use tokio_stream::StreamExt;

use crate::metadata::store::{EtcdStore, MetaStore};
use crate::metadata::{MetadataClient, MetadataEvent, Task as MetaTask};
use crate::router::Router;
use crate::{errors::Result, observer::BackupStreamObserver};
use kvproto::raft_cmdpb::{CmdType, Request};
use online_config::ConfigChange;
use raftstore::coprocessor::{Cmd, CmdBatch};
use tidb_query_datatype::codec::table::decode_table_id;
use tikv::config::BackupStreamConfig;
use tikv_util::worker::{Runnable, Scheduler};
use tikv_util::{debug, error, info};

pub struct Endpoint<S: MetaStore + 'static> {
    #[allow(dead_code)]
    config: BackupStreamConfig,
    meta_client: Option<MetadataClient<S>>,
    range_router: Arc<RwLock<Router>>,
    #[allow(dead_code)]
    scheduler: Scheduler<Task>,
    #[allow(dead_code)]
    observer: BackupStreamObserver,
    pool: Runtime,
    store_id: u64,
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

        let range_router = Arc::new(RwLock::new(Router::new()));

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
            store_id,
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
            info!("watch task"; "task" => ?task);
            // move task to schedule
            if let Err(e) = scheduler.schedule(Task::WatchTask(task)) {
                // TODO build a error handle mechanism #error 3
                error!("backup stream schedule task failed"; "error" => ?e);
            }
        }

        let mut watcher = meta_client.events_from(tasks.revision).await?;
        loop {
            if let Some(event) = watcher.stream.next().await {
                info!("watch event from etcd"; "event" => ?event);
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
    fn encode_event(&self, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut buf = vec![];
        let key_len = (key.len() as u32).to_ne_bytes();
        let val_len = value.len().to_ne_bytes();
        buf.extend_from_slice(&key_len);
        buf.extend_from_slice(key);
        buf.extend_from_slice(&val_len);
        buf.extend_from_slice(value);
        buf
    }

    // TODO improve the bakcup file name
    fn backup_file_name(
        &self,
        task_name: String,
        store_id: u64,
        table_id: u64,
        region_id: u64,
        cf: &str,
        t: &str,
    ) -> String {
        format!(
            "{}-{}-{}-{}-{}-{}.log",
            task_name, store_id, table_id, region_id, cf, t,
        )
    }

    // backup kv event to file.
    fn backup_file(&self, t: CmdType, key: Vec<u8>, value: Vec<u8>, cf: String) {
        if cf == CF_LOCK {
            return;
        }
        if !self.range_router.read().unwrap().key_in_ranges(&key) {
            // drop the key not in filter
            return;
        }
        debug!(
            "backup kv";
            "cmdtype" => ?t,
            "cf" => ?cf,
            "key" => &log_wrappers::Value::key(&key),
        );
        if let Some(task) = self.range_router.read().unwrap().get_task_by_key(&key) {
            let cmd_type = if t == CmdType::Put { "put" } else { "delete" };
            let table_id = decode_table_id(&key).unwrap_or(0) as u64;
            let name = self.backup_file_name(task, self.store_id, table_id, 0, &cf, cmd_type);
            let file = PathBuf::from(self.config.streaming_path.clone()).join(name);
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(file)
                .unwrap();
            let bytes = self.encode_event(&key, &value);
            if let Err(e) = file.write_all(&bytes) {
                error!("backup stream write file failed"; "error" => ?e);
            }
        } else {
            // TODO handle this error
            error!(
                "backup stream not found task by given key failed";
                "key" => &log_wrappers::Value::key(&key)
            );
        }
    }

    fn backup_data(&mut self, requests: Vec<Request>) -> Result<()> {
        for mut req in requests {
            match req.get_cmd_type() {
                CmdType::Put => {
                    let mut put = req.take_put();
                    self.backup_file(req.get_cmd_type(), put.take_key(), put.take_value(), put.cf);
                }
                CmdType::Delete => {
                    let mut del = req.take_delete();
                    self.backup_file(req.get_cmd_type(), del.take_key(), Vec::new(), del.cf);
                }
                _ => {
                    debug!(
                        "backup stream skip other command";
                        "command" => ?req,
                    );
                }
            };
        }
        Ok(())
    }

    fn backup_batch(&mut self, batch: CmdBatch) -> Result<()> {
        let region_id = batch.region_id;
        for cmd in batch.into_iter(region_id) {
            let Cmd {
                index: _,
                request,
                mut response,
            } = cmd;
            if response.get_header().has_error() {
                let err_header = response.mut_header().take_error();
                error!("backup stream parse batch cmd failed"; "error" => ?err_header);
                // TODO find a proper way to handle all related error
            }
            if !request.has_admin_request() {
                self.backup_data(request.requests.into())?;
            } else {
                error!("backup stream ignore amdin request for now");
            }
        }
        Ok(())
    }

    // register task ranges
    pub fn on_register(&self, task: MetaTask) {
        if let Some(cli) = self.meta_client.as_ref() {
            let cli = cli.clone();
            let range_router = Arc::clone(&self.range_router);

            info!("register task {:?}", task);

            self.pool.block_on(async move {
                let task_name = task.info.get_name();
                match cli.ranges_of_task(task_name).await {
                    Ok(ranges) => {
                        debug!(
                            "backup stream {:?} ranges len {:?}",
                            task,
                            ranges.inner.len()
                        );
                        // TODO implement register ranges
                        range_router
                            .write()
                            .unwrap()
                            .register_ranges(task_name, ranges.inner);
                    }
                    Err(e) => {
                        error!("backup stream get tasks failed"; "error" => ?e);
                        // TODO build a error handle mechanism #error 5
                    }
                }
            });
        };
    }

    pub fn do_backup(&mut self, events: Vec<CmdBatch>) {
        for batch in events {
            if let Err(e) = self.backup_batch(batch) {
                // TODO build a error handle mechanism #error 6
                error!("backup stream failed in backup batch"; "error" => ?e);
            }
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
