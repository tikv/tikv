// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use engine_rocks::RocksEngine;
use kvproto::raft_serverpb::RaftMessage;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::config::{Config, RaftstoreConfigManager};
use raftstore::store::fsm::StoreMeta;
use raftstore::store::fsm::*;
use raftstore::store::{AutoSplitController, SnapManager, StoreMsg, Transport};
use raftstore::Result;
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv::import::SSTImporter;

use concurrency_manager::ConcurrencyManager;
use engine_traits::{Engines, ALL_CFS};
use tempfile::TempDir;
use test_raftstore::TestPdClient;
use tikv_util::config::VersionTrack;
use tikv_util::worker::{dummy_scheduler, FutureWorker, Worker};

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

fn create_tmp_engine(dir: &TempDir) -> Engines<RocksEngine, RocksEngine> {
    let db = Arc::new(
        engine_rocks::raw_util::new_engine(
            dir.path().join("db").to_str().unwrap(),
            None,
            ALL_CFS,
            None,
        )
        .unwrap(),
    );
    let raft_db = Arc::new(
        engine_rocks::raw_util::new_engine(
            dir.path().join("raft").to_str().unwrap(),
            None,
            &[],
            None,
        )
        .unwrap(),
    );
    Engines::new(RocksEngine::from_db(db), RocksEngine::from_db(raft_db))
}

fn start_raftstore(
    cfg: TiKvConfig,
    dir: &TempDir,
) -> (
    ConfigController,
    RaftRouter<RocksEngine, RocksEngine>,
    ApplyRouter<RocksEngine>,
    RaftBatchSystem<RocksEngine, RocksEngine>,
) {
    let (raft_router, mut system) = create_raft_batch_system(&cfg.raft_store);
    let engines = create_tmp_engine(dir);
    let host = CoprocessorHost::default();
    let importer = {
        let p = dir
            .path()
            .join("store-config-importer")
            .as_path()
            .display()
            .to_string();
        Arc::new(SSTImporter::new(&p, None).unwrap())
    };
    let snap_mgr = {
        let p = dir
            .path()
            .join("store-config-snp")
            .as_path()
            .display()
            .to_string();
        SnapManager::new(p)
    };
    let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
    let cfg_track = Arc::new(VersionTrack::new(cfg.raft_store.clone()));
    let cfg_controller = ConfigController::new(cfg);
    cfg_controller.register(
        Module::Raftstore,
        Box::new(RaftstoreConfigManager(cfg_track.clone())),
    );
    let pd_worker = FutureWorker::new("store-config");
    let (split_check_scheduler, _) = dummy_scheduler();

    system
        .spawn(
            Default::default(),
            cfg_track,
            engines,
            MockTransport,
            Arc::new(TestPdClient::new(0, true)),
            snap_mgr,
            pd_worker,
            store_meta,
            host,
            importer,
            split_check_scheduler,
            Worker::new("split"),
            AutoSplitController::default(),
            Arc::default(),
            ConcurrencyManager::new(1.into()),
        )
        .unwrap();
    (cfg_controller, raft_router, system.apply_router(), system)
}

fn validate_store<F>(router: &RaftRouter<RocksEngine, RocksEngine>, f: F)
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

#[test]
fn test_update_raftstore_config() {
    let (mut config, _dir) = TiKvConfig::with_tmp().unwrap();
    config.validate().unwrap();
    let (cfg_controller, router, _, mut system) = start_raftstore(config.clone(), &_dir);

    // dispatch updated config
    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert("raftstore.messages-per-tick".to_owned(), "12345".to_owned());
        m.insert(
            "raftstore.raft-log-gc-threshold".to_owned(),
            "54321".to_owned(),
        );
        m
    };
    cfg_controller.update(change).unwrap();

    // config should be updated
    let mut raft_store = config.raft_store;
    raft_store.messages_per_tick = 12345;
    raft_store.raft_log_gc_threshold = 54321;
    validate_store(&router, move |cfg: &Config| {
        assert_eq!(cfg, &raft_store);
    });

    system.shutdown();
}
