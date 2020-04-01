// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::{self, sync_channel};
use std::sync::Arc;
use std::time::Duration;

use engine::{rocks, DB};
use engine_rocks::Compat;
use raftstore::coprocessor::{
    config::{Config, SplitCheckConfigManager},
    CoprocessorHost,
};
use raftstore::store::{SplitCheckRunner as Runner, SplitCheckTask as Task};
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv_util::worker::{Scheduler, Worker};

use tempfile::Builder;

fn tmp_engine() -> Arc<DB> {
    let path = Builder::new().prefix("test-config").tempdir().unwrap();
    Arc::new(
        rocks::util::new_engine(
            path.path().join("db").to_str().unwrap(),
            None,
            &["split-check-config"],
            None,
        )
        .unwrap(),
    )
}

fn setup(cfg: TiKvConfig, engine: Arc<DB>) -> (ConfigController, Worker<Task>) {
    let (router, _) = sync_channel(1);
    let runner = Runner::new(
        engine.c().clone(),
        router.clone(),
        CoprocessorHost::new(router),
        cfg.coprocessor.clone(),
    );
    let mut worker: Worker<Task> = Worker::new("split-check-config");
    worker.start(runner).unwrap();

    let mut cfg_controller = ConfigController::new(cfg, Default::default(), false);
    cfg_controller.register(
        Module::Coprocessor,
        Box::new(SplitCheckConfigManager(worker.scheduler())),
    );

    (cfg_controller, worker)
}

fn validate<F>(scheduler: &Scheduler<Task>, f: F)
where
    F: FnOnce(&Config) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    scheduler
        .schedule(Task::Validate(Box::new(move |cfg: &Config| {
            f(cfg);
            tx.send(()).unwrap();
        })))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn test_update_split_check_config() {
    let mut cfg = TiKvConfig::default();
    cfg.validate().unwrap();
    let engine = tmp_engine();
    let (mut cfg_controller, mut worker) = setup(cfg.clone(), engine);
    let scheduler = worker.scheduler();

    let cop_config = cfg.coprocessor.clone();
    let mut incoming = cfg.clone();
    incoming.raft_store.raft_log_gc_threshold = 2000;
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    // update of other module's config should not effect split check config
    assert_eq!(rollback.right(), Some(true));
    validate(&scheduler, move |cfg: &Config| {
        assert_eq!(cfg, &cop_config);
    });

    let cop_config = {
        let mut cop_config = cfg.coprocessor.clone();
        cop_config.split_region_on_table = true;
        cop_config.batch_split_limit = 123;
        cop_config.region_split_keys = 12345;
        cop_config
    };
    let mut incoming = cfg;
    incoming.coprocessor = cop_config.clone();
    let rollback = cfg_controller.update_or_rollback(incoming).unwrap();
    // config should be updated
    assert_eq!(rollback.right(), Some(true));
    validate(&scheduler, move |cfg: &Config| {
        assert_eq!(cfg, &cop_config);
    });

    worker.stop().unwrap().join().unwrap();
}
