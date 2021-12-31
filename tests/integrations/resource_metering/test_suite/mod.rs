// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod mock_pubsub;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use futures::{select, FutureExt, StreamExt};
use futures_timer::Delay;
use grpcio::{ChannelBuilder, ClientSStreamReceiver, Environment};
use kvproto::kvrpcpb::{ApiVersion, Context};
use kvproto::resource_usage_agent::{
    GroupTagRecordItem, ResourceMeteringPubSubClient, ResourceMeteringRequest, ResourceUsageRecord,
};
use tempfile::TempDir;
use test_util::alloc_port;
use tikv::config::{ConfigController, TiKvConfig};
use tikv::storage::lock_manager::DummyLockManager;
use tikv::storage::{RocksEngine, Storage, TestEngineBuilder, TestStorageBuilder};
use tokio::runtime::{self, Runtime};
use txn_types::{Key, TimeStamp};

pub struct TestSuite {
    pubsub_server_port: u16,

    storage: Storage<RocksEngine, DummyLockManager>,
    cfg_controller: ConfigController,

    env: Arc<Environment>,
    rt: Runtime,
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
            resource_metering::init_reporter(cfg.clone(), collector_reg_handle.clone());
        let env = Arc::new(Environment::new(2));
        let pubsub_server_port = alloc_port();
        let mut pubsub_server = mock_pubsub::MockPubSubServer::new(
            pubsub_server_port,
            env.clone(),
            data_sink_reg_handle,
        );
        pubsub_server.start();

        let cfg_manager =
            resource_metering::ConfigManager::new(cfg, recorder_notifier, reporter_notifier);
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

        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();

        Self {
            pubsub_server_port,
            storage,
            cfg_controller,
            env,
            rt,
            cancel_workload: None,
            wait_for_cancel: None,
            _dir: dir,
            stop_workers: Some(Box::new(move || {
                futures::executor::block_on(pubsub_server.shutdown()).unwrap();
                reporter_worker.stop_worker();
                recorder_worker.stop_worker();
            })),
        }
    }

    pub fn subscribe(&self) -> Subscriber {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone());
            cb.connect(&format!("127.0.0.1:{}", self.pubsub_server_port))
        };
        let client = ResourceMeteringPubSubClient::new(channel);
        let receiver = client
            .subscribe(&ResourceMeteringRequest::default())
            .unwrap();
        Subscriber::new(client, receiver)
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
}

impl Drop for TestSuite {
    fn drop(&mut self) {
        self.cancel_workload();
        self.stop_workers.take().unwrap()();
    }
}

pub struct Subscriber {
    _client: ResourceMeteringPubSubClient,
    stream: ClientSStreamReceiver<ResourceUsageRecord>,
}

impl Subscriber {
    pub fn from_client(client: ResourceMeteringPubSubClient) -> Self {
        let stream = client
            .subscribe(&ResourceMeteringRequest::default())
            .unwrap();
        Self {
            _client: client,
            stream,
        }
    }

    pub fn new(
        client: ResourceMeteringPubSubClient,
        stream: ClientSStreamReceiver<ResourceUsageRecord>,
    ) -> Self {
        Self {
            _client: client,
            stream,
        }
    }

    pub fn next_batch_tags(&mut self, max_delay: Duration) -> HashSet<String> {
        let mut set = HashSet::default();
        for (k, _) in self.next_batch_records(max_delay) {
            set.insert(k);
        }
        set
    }

    pub fn next_batch_records(
        &mut self,
        max_delay: Duration,
    ) -> HashMap<String, Vec<GroupTagRecordItem>> {
        let f = async {
            let mut map = HashMap::default();

            let dl = Delay::new(max_delay);
            let res = select! {
                r = self.stream.next().fuse() => r.map(|r| r.ok()).flatten(),
                _ = dl.fuse() => None,
            };

            if res.is_none() {
                return map;
            }

            let record = res.as_ref().unwrap().get_record();
            let tag = record.get_resource_group_tag();
            map.insert(
                String::from_utf8_lossy(tag).into_owned(),
                record.get_items().into(),
            );

            loop {
                let dl = Delay::new(Duration::from_millis(50));
                let res = select! {
                    r = self.stream.next().fuse() => r.map(|r| r.ok()).flatten(),
                    _ = dl.fuse() => None,
                };

                if res.is_none() {
                    break;
                }

                let record = res.as_ref().unwrap().get_record();
                let tag = record.get_resource_group_tag();
                map.insert(
                    String::from_utf8_lossy(tag).into_owned(),
                    record.get_items().into(),
                );
            }

            map
        };

        futures::executor::block_on(f)
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.stream.cancel();
    }
}
