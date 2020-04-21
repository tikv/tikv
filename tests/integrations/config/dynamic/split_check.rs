// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
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

fn tmp_engine<P: AsRef<Path>>(path: P) -> Arc<DB> {
    Arc::new(
        rocks::util::new_engine(
            path.as_ref().to_str().unwrap(),
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

    let mut cfg_controller = ConfigController::new(cfg);
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
    let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
    cfg.validate().unwrap();
    let engine = tmp_engine(&cfg.storage.data_dir);
    let (mut cfg_controller, mut worker) = setup(cfg.clone(), engine);
    let scheduler = worker.scheduler();

    let cop_config = cfg.coprocessor.clone();
    // update of other module's config should not effect split check config
    cfg_controller
        .update_config("raftstore.raft-log-gc-threshold", "2000")
        .unwrap();
    validate(&scheduler, move |cfg: &Config| {
        assert_eq!(cfg, &cop_config);
    });

    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "coprocessor.split_region_on_table".to_owned(),
            "true".to_owned(),
        );
        m.insert("coprocessor.batch_split_limit".to_owned(), "123".to_owned());
        m.insert(
            "coprocessor.region_split_keys".to_owned(),
            "12345".to_owned(),
        );
        m
    };
    cfg_controller.update(change).unwrap();

    // config should be updated
    let cop_config = {
        let mut cop_config = cfg.coprocessor;
        cop_config.split_region_on_table = true;
        cop_config.batch_split_limit = 123;
        cop_config.region_split_keys = 12345;
        cop_config
    };
    validate(&scheduler, move |cfg: &Config| {
        assert_eq!(cfg, &cop_config);
    });

    worker.stop().unwrap().join().unwrap();
}
