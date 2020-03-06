// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use engine::rocks;
use engine_rocks::RocksEngine;
use kvproto::raft_serverpb::RaftMessage;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::config::{Config, RaftstoreConfigManager};
use raftstore::store::fsm::StoreMeta;
use raftstore::store::fsm::*;
use raftstore::store::{SnapManager, StoreMsg, Transport};
use raftstore::Result;
use tikv::config::{ConfigController, ConfigHandler, Module, TiKvConfig};
use tikv::import::SSTImporter;

use engine::Engines;
use engine_traits::ALL_CFS;
use pd_client::ConfigClient;
use pd_client::PdClient;
use tempfile::{Builder, TempDir};
use tikv_util::config::VersionTrack;
use tikv_util::worker::{FutureWorker, Worker};

#[derive(Clone)]
struct MockTransport;
impl Transport for MockTransport {
    fn send(&mut self, _: RaftMessage) -> Result<()> {
        unimplemented!()
    }
    fn flush(&mut self) {
        unimplemented!()
    }
}

struct MockPdClient;
impl PdClient for MockPdClient {}
impl ConfigClient for MockPdClient {}

fn create_tmp_engine(path: &str) -> (TempDir, Engines) {
    let path = Builder::new().prefix(path).tempdir().unwrap();
    let db = Arc::new(
        rocks::util::new_engine(
            path.path().join("db").to_str().unwrap(),
            None,
            ALL_CFS,
            None,
        )
        .unwrap(),
    );
    let raft_db = Arc::new(
        rocks::util::new_engine(path.path().join("raft").to_str().unwrap(), None, &[], None)
            .unwrap(),
    );
    let shared_block_cache = false;
    (path, Engines::new(db, raft_db, shared_block_cache))
}

fn start_raftstore(
    cfg: TiKvConfig,
) -> (
    ConfigController,
    RaftRouter<RocksEngine>,
    ApplyRouter,
    RaftBatchSystem,
) {
    let (raft_router, mut system) = create_raft_batch_system(&cfg.raft_store);
    let (_, engines) = create_tmp_engine("store-config");
    let host = CoprocessorHost::default();
    let importer = {
        let dir = Builder::new().prefix("store-config").tempdir().unwrap();
        Arc::new(SSTImporter::new(dir.path()).unwrap())
    };
    let snap_mgr = {
        let tmp = Builder::new().prefix("store-config").tempdir().unwrap();
        SnapManager::new(tmp.path().to_str().unwrap(), Some(raft_router.clone()))
    };
    let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
    let cfg_track = Arc::new(VersionTrack::new(cfg.raft_store.clone()));
    let mut cfg_controller = ConfigController::new(cfg, Default::default());
    cfg_controller.register(
        Module::Raftstore,
        Box::new(RaftstoreConfigManager(cfg_track.clone())),
    );
    let pd_worker = FutureWorker::new("store-config");
    let config_client =
        ConfigHandler::start(String::new(), Default::default(), pd_worker.scheduler()).unwrap();

    system
        .spawn(
            Default::default(),
            cfg_track,
            engines,
            MockTransport,
            Arc::new(MockPdClient),
            snap_mgr,
            pd_worker,
            store_meta,
            host,
            importer,
            Worker::new("split"),
            Box::new(config_client),
        )
        .unwrap();
    (cfg_controller, raft_router, system.apply_router(), system)
}

fn validate_store<F>(router: &RaftRouter<RocksEngine>, f: F)
where
    F: FnOnce(&Config) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    router
        .send_control(StoreMsg::Validate(Box::new(move |cfg: &Config| {
            f(cfg);
            tx.send(()).unwrap();
        })))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

fn validate_apply<F>(router: &ApplyRouter, region_id: u64, validate: F)
where
    F: FnOnce(bool) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.schedule_task(
        region_id,
        ApplyTask::Validate(
            region_id,
            Box::new(move |(_, sync_log): (_, bool)| {
                validate(sync_log);
                tx.send(()).unwrap();
            }),
        ),
    );
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_update_raftstore_config() {
    let mut config = TiKvConfig::default();
    config.validate().unwrap();
    let (mut cfg_controller, router, _, mut system) = start_raftstore(config.clone());

    let incoming = config.clone();
    let raft_store = incoming.raft_store.clone();
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();

    // config should not change
    assert_eq!(rollback.right(), Some(false));
    validate_store(&router, move |cfg: &Config| {
        assert_eq!(cfg, &raft_store);
    });

    // dispatch updated config
    let mut raft_store = config.raft_store.clone();
    raft_store.messages_per_tick = 12345;
    raft_store.raft_log_gc_threshold = 54321;
    let mut incoming = config;
    incoming.raft_store = raft_store.clone();
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();

    // config should be updated
    assert_eq!(rollback.right(), Some(true));
    validate_store(&router, move |cfg: &Config| {
        assert_eq!(cfg, &raft_store);
    });

    system.shutdown();
}

#[test]
fn test_update_apply_store_config() {
    let mut config = TiKvConfig::default();
    config.raft_store.sync_log = true;
    config.validate().unwrap();
    let (mut cfg_controller, raft_router, apply_router, mut system) =
        start_raftstore(config.clone());

    // register region
    let region_id = 1;
    let mut reg = Registration::default();
    reg.region.set_id(region_id);
    apply_router.schedule_task(region_id, ApplyTask::Registration(reg));

    let rollback = cfg_controller.update_or_rollback(config.clone()).unwrap();

    // config should not change
    assert_eq!(rollback.right(), Some(false));
    validate_store(&raft_router, move |cfg: &Config| {
        assert_eq!(cfg.sync_log, true);
    });
    validate_apply(&apply_router, region_id, |sync_log| {
        assert_eq!(sync_log, true);
    });

    // dispatch updated config
    let mut incoming = config;
    incoming.raft_store.sync_log = false;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();

    // both configs should be updated
    assert_eq!(rollback.right(), Some(true));
    validate_store(&raft_router, move |cfg: &Config| {
        assert_eq!(cfg.sync_log, false);
    });
    validate_apply(&apply_router, region_id, |sync_log| {
        assert_eq!(sync_log, false);
    });

    system.shutdown();
}
