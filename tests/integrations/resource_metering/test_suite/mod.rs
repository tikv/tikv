// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod mock_pubsub;
mod mock_receiver_server;

pub use mock_receiver_server::MockReceiverServer;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver, Sender};
use futures::channel::oneshot;
use futures::{select, FutureExt};
use grpcio::{ChannelBuilder, ClientSStreamReceiver, Environment};
use kvproto::kvrpcpb::{ApiVersion, Context};
use kvproto::resource_usage_agent::{
    ResourceMeteringPubSubClient, ResourceMeteringRequest, ResourceUsageRecord,
};
use resource_metering::Config;
use tempfile::TempDir;
use test_util::alloc_port;
use tikv::config::{ConfigController, TiKvConfig};
use tikv::storage::lock_manager::DummyLockManager;
use tikv::storage::{RocksEngine, Storage, TestEngineBuilder, TestStorageBuilder};
use tokio::runtime::{self, Runtime};
use txn_types::{Key, TimeStamp};

pub struct TestSuite {
    pubsub_server_port: u16,
    receiver_server: Option<MockReceiverServer>,

    storage: Storage<RocksEngine, DummyLockManager>,
    cfg_controller: ConfigController,

    tx: Sender<Vec<ResourceUsageRecord>>,
    rx: Receiver<Vec<ResourceUsageRecord>>,

    env: Arc<Environment>,
    pub rt: Runtime,
    cancel_workload: Option<oneshot::Sender<()>>,
    wait_for_cancel: Option<oneshot::Receiver<()>>,

    _dir: TempDir,
    stop_workers: Option<Box<dyn FnOnce()>>,
}

impl TestSuite {
    pub fn new(cfg: resource_metering::Config) -> Self {
        let (mut tikv_cfg, dir) = TiKvConfig::with_tmp().unwrap();
        tikv_cfg.resource_metering = cfg.clone();
        let cfg_controller = ConfigController::new(tikv_cfg);

        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(cfg.precision.as_millis());
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(cfg.clone(), collector_reg_handle);
        let env = Arc::new(Environment::new(2));
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            cfg.receiver_address.clone(),
            env.clone(),
            data_sink_reg_handle.clone(),
        );
        let pubsub_server_port = alloc_port();
        let mut pubsub_server = mock_pubsub::MockPubSubServer::new(
            pubsub_server_port,
            env.clone(),
            data_sink_reg_handle,
        );
        pubsub_server.start();

        let cfg_manager = resource_metering::ConfigManager::new(
            cfg,
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );

        let engine = TestEngineBuilder::new().build().unwrap();
        let storage = TestStorageBuilder::from_engine_and_lock_mgr(
            engine,
            DummyLockManager {},
            ApiVersion::V1,
        )
        .set_resource_tag_factory(resource_tag_factory)
        .build()
        .unwrap();

        let (tx, rx) = unbounded();

        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        Self {
            pubsub_server_port,
            receiver_server: None,
            storage,
            cfg_controller,
            tx,
            rx,
            env,
            rt,
            cancel_workload: None,
            wait_for_cancel: None,
            _dir: dir,
            stop_workers: Some(Box::new(move || {
                futures::executor::block_on(pubsub_server.shutdown()).unwrap();
                single_target_worker.stop_worker();
                reporter_worker.stop_worker();
                recorder_worker.stop_worker();
            })),
        }
    }

    pub fn subscribe(
        &self,
    ) -> (
        ResourceMeteringPubSubClient,
        ClientSStreamReceiver<ResourceUsageRecord>,
    ) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone());
            cb.connect(&format!("127.0.0.1:{}", self.pubsub_server_port))
        };
        let client = ResourceMeteringPubSubClient::new(channel);
        let receiver = client
            .subscribe(&ResourceMeteringRequest::default())
            .unwrap();
        (client, receiver)
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
        self.cfg_controller.get_current().resource_metering
    }

    pub fn start_receiver_at(&mut self, port: u16) {
        assert!(self.receiver_server.is_none());

        let mut receiver_server = MockReceiverServer::new(self.tx.clone());
        receiver_server.start_server(port, self.env.clone());
        self.receiver_server = Some(receiver_server);
    }

    pub fn shutdown_receiver(&mut self) {
        if let Some(mut receiver) = self.receiver_server.take() {
            self.rt.block_on(receiver.shutdown_server());
        }
    }

    pub fn block_receiver(&mut self) {
        self.receiver_server.as_ref().unwrap().block();
    }

    pub fn unblock_receiver(&mut self) {
        self.receiver_server.as_ref().unwrap().unblock();
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
                    ctx.set_resource_group_tag(tag.as_bytes().to_vec());
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

    pub fn nonblock_receiver_all(&self) -> HashMap<String, (Vec<u64>, Vec<u32>)> {
        let mut res = HashMap::new();
        if let Ok(r) = self.rx.try_recv() {
            Self::merge_records(&mut res, r);
        }
        res
    }

    pub fn block_receive_one(&self) -> HashMap<String, (Vec<u64>, Vec<u32>)> {
        let records = self.rx.recv().unwrap();
        let mut res = HashMap::new();
        Self::merge_records(&mut res, records);
        res
    }

    fn merge_records(
        map: &mut HashMap<String, (Vec<u64>, Vec<u32>)>,
        records: Vec<ResourceUsageRecord>,
    ) {
        for r in records {
            let tag = String::from_utf8_lossy(r.get_record().get_resource_group_tag()).into_owned();
            let (ts, cpu_time) = map.entry(tag).or_insert((vec![], vec![]));
            ts.extend(
                r.get_record()
                    .get_items()
                    .iter()
                    .map(|item| item.timestamp_sec),
            );
            cpu_time.extend(
                r.get_record()
                    .get_items()
                    .iter()
                    .map(|item| item.cpu_time_ms),
            );
        }
    }

    pub fn flush_receiver(&self) {
        while self.rx.try_recv().is_ok() {}
        let _ = self.rx.recv_timeout(
            self.get_current_cfg().report_receiver_interval.0 + Duration::from_millis(500),
        );
    }
}

impl Drop for TestSuite {
    fn drop(&mut self) {
        self.stop_workers.take().unwrap()();
    }
}
