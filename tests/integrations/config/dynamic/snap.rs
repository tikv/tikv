// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::channel, Arc},
    time::Duration,
};

use engine_rocks::RocksEngine;
use grpcio::EnvBuilder;
use raft_log_engine::RaftLogEngine;
use raftstore::store::{fsm::create_raft_batch_system, SnapManager};
use security::SecurityManager;
use tempfile::TempDir;
use tikv::{
    config::{ConfigController, TiKvConfig},
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        snap::{Runner as SnapHandler, Task as SnapTask},
    },
};
use tikv_util::{
    config::{ReadableSize, VersionTrack},
    worker::{LazyWorker, Scheduler, Worker},
};

fn start_server(
    cfg: TiKvConfig,
    dir: &TempDir,
) -> (ConfigController, LazyWorker<SnapTask>, SnapManager) {
    let snap_mgr = {
        let p = dir
            .path()
            .join("store-config-snp")
            .as_path()
            .display()
            .to_string();
        SnapManager::new(p)
    };

    let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
    let env = Arc::new(
        EnvBuilder::new()
            .cq_count(2)
            .name_prefix(thd_name!("test-server"))
            .build(),
    );
    let (raft_router, _) = create_raft_batch_system::<RocksEngine, RaftLogEngine>(&cfg.raft_store);
    let mut snap_worker = Worker::new("snap-handler").lazy_build("snap-handler");
    let snap_worker_scheduler = snap_worker.scheduler();
    let server_config = Arc::new(VersionTrack::new(cfg.server.clone()));
    let cfg_controller = ConfigController::new(cfg);
    cfg_controller.register(
        tikv::config::Module::Server,
        Box::new(ServerConfigManager::new(
            snap_worker_scheduler,
            server_config.clone(),
        )),
    );
    let snap_runner = SnapHandler::new(
        Arc::clone(&env),
        snap_mgr.clone(),
        raft_router,
        security_mgr,
        Arc::clone(&server_config),
    );
    snap_worker.start(snap_runner);

    (cfg_controller, snap_worker, snap_mgr)
}

fn validate<F>(scheduler: &Scheduler<SnapTask>, f: F)
where
    F: FnOnce(&ServerConfig) + Send + 'static,
{
    let (tx, rx) = channel();
    scheduler
        .schedule(SnapTask::Validate(Box::new(move |cfg: &ServerConfig| {
            f(cfg);
            tx.send(()).unwrap();
        })))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_update_server_config() {
    let (mut config, _dir) = TiKvConfig::with_tmp().unwrap();
    config.validate().unwrap();
    let (cfg_controller, snap_worker, snap_mgr) = start_server(config.clone(), &_dir);
    let mut svr_cfg = config.server.clone();
    // dispatch updated config
    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "server.snap-max-write-bytes-per-sec".to_owned(),
            "512MB".to_owned(),
        );
        m.insert(
            "server.concurrent-send-snap-limit".to_owned(),
            "100".to_owned(),
        );
        m
    };
    cfg_controller.update(change).unwrap();

    svr_cfg.snap_max_write_bytes_per_sec = ReadableSize::mb(512);
    svr_cfg.concurrent_send_snap_limit = 100;
    // config should be updated
    assert_eq!(snap_mgr.get_speed_limit() as u64, 536870912);
    validate(&snap_worker.scheduler(), move |cfg: &ServerConfig| {
        assert_eq!(cfg, &svr_cfg);
    });
}
