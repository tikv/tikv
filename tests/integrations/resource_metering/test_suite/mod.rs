// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod mock_agent_server;

use std::collections::HashMap;
use std::sync::Arc;

use crossbeam::channel::{unbounded, Receiver, Sender};
use grpcio::{Environment, Server};
use kvproto::kvrpcpb::Context;
use kvproto::resource_usage_agent::ReportCpuTimeRequest;
use mock_agent_server::MockAgentServer;
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
    agent_server: Option<Server>,

    storage: Storage<RocksEngine, DummyLockManager>,
    reporter: Option<Box<LazyWorker<Task>>>,
    cfg_controller: ConfigController,

    tx: Sender<ReportCpuTimeRequest>,
    rx: Receiver<ReportCpuTimeRequest>,

    env: Arc<Environment>,
    rt: Runtime,

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
        tikv_cfg.resource_metering.report_agent_interval = ReadableDuration::secs(5);

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
            agent_server: None,
            storage,
            reporter: Some(reporter),
            cfg_controller,
            tx,
            rx,
            env,
            rt,
            _dir: dir,
        }
    }

    pub fn cfg_failpoint_op_duration(&self, ms: u64) {
        if ms == 0 {
            fail::remove("storage::get");
        } else {
            fail::cfg("storage::get", &format!("delay({})", ms)).unwrap();
        }
    }

    pub fn cfg_enabled(&mut self, enabled: bool) {
        self.cfg_controller
            .update_config("resource-metering.enabled", &enabled.to_string())
            .unwrap();
    }

    pub fn cfg_agent_address(&self, addr: impl Into<String>) {
        let addr = addr.into();
        self.cfg_controller
            .update_config("resource-metering.agent-address", &addr)
            .unwrap();
    }

    pub fn cfg_precision(&self, precision: impl Into<String>) {
        let precision = precision.into();
        self.cfg_controller
            .update_config("resource-metering.precision", &precision)
            .unwrap();
    }

    pub fn cfg_report_agent_interval(&self, interval: impl Into<String>) {
        let interval = interval.into();
        self.cfg_controller
            .update_config("resource-metering.report-agent-interval", &interval)
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

    pub fn start_agent_at(&mut self, port: u16) {
        assert!(self.agent_server.is_none());

        let mut agent_server =
            MockAgentServer::new(self.tx.clone()).build_server(port, self.env.clone());
        agent_server.start();
        self.agent_server = Some(agent_server);
    }

    pub fn shutdown_agent(&mut self) {
        if let Some(mut agent) = self.agent_server.take() {
            self.rt.block_on(agent.shutdown()).unwrap();
        }
    }

    pub fn execute_ops(&mut self, tags: Vec<impl Into<String>>) {
        self.rt
            .spawn(futures::future::join_all(tags.into_iter().map(|tag| {
                let mut ctx = Context::default();
                ctx.set_resource_group_tag({
                    let mut t = Vec::from(TEST_TAG_PREFIX);
                    let tag = tag.into();
                    t.extend_from_slice(tag.as_bytes());
                    t
                });
                self.storage.get(ctx, Key::from_raw(b""), TimeStamp::new(0))
            })));
    }

    pub fn fetch_reported_cpu_time(&self) -> HashMap<String, (Vec<u64>, Vec<u32>)> {
        self.rx
            .try_iter()
            .map(|r| {
                (
                    String::from_utf8_lossy(
                        (!r.get_resource_group_tag().is_empty())
                            .then(|| r.resource_group_tag.split_at(TEST_TAG_PREFIX.len()).1)
                            .unwrap_or(b""),
                    )
                    .into_owned(),
                    (r.record_list_timestamp_sec, r.record_list_cpu_time_ms),
                )
            })
            .collect()
    }

    pub fn reset(&mut self) {
        self.cfg_failpoint_op_duration(0);
        self.cfg_enabled(false);
        self.cfg_agent_address("");
        self.cfg_precision("1s");
        self.cfg_report_agent_interval("5s");
        self.cfg_max_resource_groups(5000);
        self.shutdown_agent();
    }
}

impl Drop for TestSuite {
    fn drop(&mut self) {
        self.reset();
        self.reporter.take().unwrap().stop_worker();
        fail::remove("cpu-record-test-filter");
    }
}
