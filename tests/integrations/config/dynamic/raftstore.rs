// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    iter::FromIterator,
    sync::{mpsc, Arc, Mutex},
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksEngine;
use engine_traits::{Engines, ALL_CFS};
use kvproto::raft_serverpb::RaftMessage;
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{
        config::{Config, RaftstoreConfigManager},
        fsm::{StoreMeta, *},
        AutoSplitController, SnapManager, StoreMsg, Transport,
    },
    Result,
};
use resource_metering::CollectorRegHandle;
use tempfile::TempDir;
use test_raftstore::TestPdClient;
use tikv::{
    config::{ConfigController, Module, TiKvConfig},
    import::SstImporter,
};
use tikv_util::{
    config::{ReadableSize, VersionTrack},
    worker::{dummy_scheduler, LazyWorker, Worker},
};

#[derive(Clone)]
struct MockTransport;
impl Transport for MockTransport {
    fn send(&mut self, _: RaftMessage) -> Result<()> {
        unimplemented!()
    }
    fn set_store_allowlist(&mut self, _: Vec<u64>) {
        unimplemented!();
    }
    fn need_flush(&self) -> bool {
        false
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
        Arc::new(SstImporter::new(&cfg.import, &p, None, cfg.storage.api_version()).unwrap())
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
    let pd_worker = LazyWorker::new("store-config");
    let (split_check_scheduler, _) = dummy_scheduler();

    system
        .spawn(
            Default::default(),
            cfg_track.clone(),
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
            CollectorRegHandle::new_for_test(),
            None,
        )
        .unwrap();

    let cfg_controller = ConfigController::new(cfg);
    cfg_controller.register(
        Module::Raftstore,
        Box::new(RaftstoreConfigManager::new(
            system.refresh_config_scheduler(),
            cfg_track,
        )),
    );

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

    let new_changes = |cfgs: Vec<(&str, &str)>| {
        std::collections::HashMap::from_iter(
            cfgs.into_iter()
                .map(|kv| (kv.0.to_owned(), kv.1.to_owned())),
        )
    };

    // dispatch updated config
    let change = new_changes(vec![
        ("raftstore.messages-per-tick", "12345"),
        ("raftstore.raft-log-gc-threshold", "54321"),
        ("raftstore.raft-max-size-per-msg", "128MiB"),
        ("raftstore.apply-max-batch-size", "1234"),
        ("raftstore.store-max-batch-size", "4321"),
        ("raftstore.raft-entry-max-size", "32MiB"),
    ]);

    cfg_controller.update(change).unwrap();

    // config should be updated
    let mut raft_store = config.raft_store;
    raft_store.messages_per_tick = 12345;
    raft_store.raft_log_gc_threshold = 54321;
    raft_store.apply_batch_system.max_batch_size = Some(1234);
    raft_store.store_batch_system.max_batch_size = Some(4321);
    raft_store.raft_max_size_per_msg = ReadableSize::mb(128);
    raft_store.raft_entry_max_size = ReadableSize::mb(32);
    let validate_store_cfg = |raft_cfg: &Config| {
        let raftstore_cfg = raft_cfg.clone();
        validate_store(&router, move |cfg: &Config| {
            assert_eq!(cfg, &raftstore_cfg);
        });
    };
    validate_store_cfg(&raft_store);

    let invalid_cfgs = vec![
        ("raftstore.apply-max-batch-size", "10241"),
        ("raftstore.store-max-batch-size", "10241"),
        ("raftstore.apply-max-batch-size", "0"),
        ("raftstore.store-max-batch-size", "0"),
        ("raftstore.raft-entry-max-size", "0KiB"),
        ("raftstore.raft-entry-max-size", "4GiB"),
    ];
    for cfg in invalid_cfgs {
        let change = new_changes(vec![cfg]);
        assert!(cfg_controller.update(change).is_err());

        // update failed, original config should not be changed.
        validate_store_cfg(&raft_store);
    }

    let max_cfg = vec![
        ("raftstore.apply-max-batch-size", "10240"),
        ("raftstore.store-max-batch-size", "10240"),
        ("raftstore.raft-entry-max-size", "3GiB"),
    ];
    cfg_controller.update(new_changes(max_cfg)).unwrap();
    raft_store.apply_batch_system.max_batch_size = Some(10240);
    raft_store.store_batch_system.max_batch_size = Some(10240);
    raft_store.raft_entry_max_size = ReadableSize::gb(3);
    validate_store_cfg(&raft_store);

    let min_cfg = vec![
        ("raftstore.apply-max-batch-size", "1"),
        ("raftstore.store-max-batch-size", "1"),
        ("raftstore.raft-entry-max-size", "1"),
    ];
    cfg_controller.update(new_changes(min_cfg)).unwrap();
    raft_store.apply_batch_system.max_batch_size = Some(1);
    raft_store.store_batch_system.max_batch_size = Some(1);
    raft_store.raft_entry_max_size = ReadableSize(1);
    validate_store_cfg(&raft_store);

    system.shutdown();
}
