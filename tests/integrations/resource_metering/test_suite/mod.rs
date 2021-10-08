// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod mock_receiver_server;

use std::collections::HashMap;
use std::sync::Arc;

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::channel::oneshot;
use futures::{select, FutureExt};
use grpcio::{Environment, Server};
use kvproto::kvrpcpb::Context;
use kvproto::resource_usage_agent::CpuTimeRecord;
use mock_receiver_server::MockReceiverServer;
use resource_metering::cpu::recorder::{init_recorder, TEST_TAG_PREFIX};
use resource_metering::reporter::{ResourceMeteringReporter, Task};
use resource_metering::{Config, ConfigManager};
use tempfile::TempDir;
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv::storage::lock_manager::DummyLockManager;
use tikv::storage::{RocksEngine, Storage, TestEngineBuilder, TestStorageBuilder};
use tikv_util::config::ReadableDuration;
use tikv_util::worker::LazyWorker;
use tokio::runtime::{self, Runtime};
use txn_types::{Key, TimeStamp};

pub struct TestSuite {
    receiver_server: Option<Server>,

    storage: Storage<RocksEngine, DummyLockManager>,
    reporter: Option<Box<LazyWorker<Task>>>,
    cfg_controller: ConfigController,

    tx: Sender<Vec<CpuTimeRecord>>,
    rx: Receiver<Vec<CpuTimeRecord>>,

    env: Arc<Environment>,
    rt: Runtime,
    cancel_workload: Option<oneshot::Sender<()>>,
    wait_for_cancel: Option<oneshot::Receiver<()>>,

    _dir: TempDir,
}

impl TestSuite {
    pub fn new() -> Self {
        fail::cfg("cpu-record-test-filter", "return").unwrap();
        let engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilder::from_engine_and_lock_mgr(engine, DummyLockManager {})
            .build()
            .unwrap();

        let mut reporter = Box::new(LazyWorker::new("resource-metering-reporter"));
        let scheduler = reporter.scheduler();

        let (mut tikv_cfg, dir) = TiKvConfig::with_tmp().unwrap();
        tikv_cfg.resource_metering.report_receiver_interval = ReadableDuration::secs(5);

        let resource_metering_cfg = tikv_cfg.resource_metering.clone();
        let cfg_controller = ConfigController::new(tikv_cfg);
        cfg_controller.register(
            Module::ResourceMetering,
            Box::new(ConfigManager::new(
                resource_metering_cfg.clone(),
                scheduler.clone(),
                init_recorder(),
            )),
        );
        let env = Arc::new(Environment::new(2));
        reporter.start_with_timer(ResourceMeteringReporter::new(
            resource_metering_cfg,
            scheduler.clone(),
            env.clone(),
        ));

        let (tx, rx) = unbounded();

        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        Self {
            receiver_server: None,
            storage,
            reporter: Some(reporter),
            cfg_controller,
            tx,
            rx,
            env,
            rt,
            cancel_workload: None,
            wait_for_cancel: None,
            _dir: dir,
        }
    }

    pub fn cfg_enabled(&mut self, enabled: bool) {
        self.cfg_controller
            .update_config("resource-metering.enabled", &enabled.to_string())
            .unwrap();
    }

    pub fn cfg_receiver_address(&self, addr: impl Into<String>) {
        let addr = addr.into();
        self.cfg_controller
            .update_config("resource-metering.receiver-address", &addr)
            .unwrap();
    }

    pub fn cfg_precision(&self, precision: impl Into<String>) {
        let precision = precision.into();
        self.cfg_controller
            .update_config("resource-metering.precision", &precision)
            .unwrap();
    }

    pub fn cfg_report_receiver_interval(&self, interval: impl Into<String>) {
        let interval = interval.into();
        self.cfg_controller
            .update_config("resource-metering.report-receiver-interval", &interval)
            .unwrap();
    }

    pub fn cfg_max_resource_groups(&self, max_resource_groups: u64) {
        self.cfg_controller
            .update_config(
                "resource-metering.max-resource-groups",
                &max_resource_groups.to_string(),
            )
            .unwrap();
    }

    pub fn get_current_cfg(&self) -> Config {
        self.cfg_controller.get_current().resource_metering.clone()
    }

    pub fn start_receiver_at(&mut self, port: u16) {
        assert!(self.receiver_server.is_none());

        let mut receiver_server =
            MockReceiverServer::new(self.tx.clone()).build_server(port, self.env.clone());
        receiver_server.start();
        self.receiver_server = Some(receiver_server);
    }

    pub fn shutdown_receiver(&mut self) {
        if let Some(mut receiver) = self.receiver_server.take() {
            self.rt.block_on(receiver.shutdown()).unwrap();
        }
    }

    pub fn setup_workload(&mut self, tags: Vec<impl Into<String>>) {
        assert!(self.cancel_workload.is_none(), "Workload has been set");
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        let (wait_tx, wait_rx) = oneshot::channel();
        self.cancel_workload = Some(cancel_tx);
        self.wait_for_cancel = Some(wait_rx);
        let tags = tags.into_iter().map(|s| s.into()).collect::<Vec<_>>();
        let storage = self.storage.clone();
        self.rt.spawn(async move {
            loop {
                let mut workload = futures::future::join_all(tags.iter().map(|tag| {
                    let mut ctx = Context::default();
                    ctx.set_resource_group_tag({
                        let mut t = Vec::from(TEST_TAG_PREFIX);
                        let tag = tag.clone();
                        t.extend_from_slice(tag.as_bytes());
                        t
                    });
                    storage.get(ctx, Key::from_raw(b""), TimeStamp::new(0))
                }))
                .fuse();

                select! {
                    _ = workload => {}
                    _ = cancel_rx => break,
                }
            }

            wait_tx.send(()).unwrap();
        });
    }

    pub fn cancel_workload(&mut self) {
        if let Some(tx) = self.cancel_workload.take() {
            tx.send(()).unwrap();
            self.rt
                .block_on(self.wait_for_cancel.take().unwrap())
                .unwrap();
        }
    }

    pub fn fetch_reported_cpu_time(&self) -> HashMap<String, (Vec<u64>, Vec<u32>)> {
        let mut res = HashMap::new();
        self.rx
            .try_iter()
            .flat_map(|r| r.into_iter())
            .for_each(|r| {
                let tag = String::from_utf8_lossy(
                    (!r.get_resource_group_tag().is_empty())
                        .then(|| r.resource_group_tag.split_at(TEST_TAG_PREFIX.len()).1)
                        .unwrap_or(b""),
                )
                .into_owned();
                let (ts, cpu_time) = res.entry(tag).or_insert((vec![], vec![]));
                ts.extend(&r.record_list_timestamp_sec);
                cpu_time.extend(&r.record_list_cpu_time_ms);
            });

        res
    }

    pub fn flush_receiver(&self) {
        let _ = self
            .rx
            .recv_timeout(self.get_current_cfg().report_receiver_interval.0);
    }

    pub fn reset(&mut self) {
        self.cfg_enabled(false);
        self.cfg_receiver_address("");
        self.cancel_workload();
        self.flush_receiver();
        self.cfg_precision("1s");
        self.cfg_report_receiver_interval("5s");
        self.cfg_max_resource_groups(5000);
        self.shutdown_receiver();
    }
}

impl Drop for TestSuite {
    fn drop(&mut self) {
        self.reset();
        self.reporter.take().unwrap().stop_worker();
        fail::remove("cpu-record-test-filter");
    }
}
