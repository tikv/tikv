// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::{channel, Receiver, Sender};

use resource_metering::cpu::recorder::RecorderHandle;
use resource_metering::reporter::Task;
use resource_metering::ConfigManager;
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv_util::config::ReadableDuration;
use tikv_util::worker::{LazyWorker, Runnable};

pub struct MockResourceMeteringReporter {
    tx: Sender<Task>,
}

impl MockResourceMeteringReporter {
    fn new(tx: Sender<Task>) -> Self {
        MockResourceMeteringReporter { tx }
    }
}

impl Runnable for MockResourceMeteringReporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        self.tx.send(task).unwrap();
    }
}

fn setup_cfg_manager(
    config: TiKvConfig,
) -> (ConfigController, Receiver<Task>, Box<LazyWorker<Task>>) {
    let mut worker = Box::new(LazyWorker::new("resource-metering-reporter"));
    let scheduler = worker.scheduler();

    let (tx, rx) = channel();

    let resource_metering_config = config.resource_metering.clone();
    let cfg_controller = ConfigController::new(config);
    cfg_controller.register(
        Module::ResourceMetering,
        Box::new(ConfigManager::new(
            resource_metering_config,
            scheduler,
            RecorderHandle::default(),
        )),
    );

    worker.start(MockResourceMeteringReporter::new(tx));
    (cfg_controller, rx, worker)
}

#[test]
fn test_update_resource_metering_agent_config() {
    let (mut config, _dir) = TiKvConfig::with_tmp().unwrap();
    config.validate().unwrap();

    let (cfg_controller, rx, worker) = setup_cfg_manager(config);

    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert("resource-metering.enabled".to_owned(), "true".to_owned());
        m.insert(
            "resource-metering.agent-address".to_owned(),
            "localhost:8888".to_owned(),
        );
        m.insert("resource-metering.precision".to_owned(), "20s".to_owned());
        m.insert(
            "resource-metering.report-agent-interval".to_owned(),
            "80s".to_owned(),
        );
        m.insert(
            "resource-metering.max-resource-groups".to_owned(),
            "3000".to_owned(),
        );
        m
    };
    cfg_controller.update(change).unwrap();

    let new_config = cfg_controller.get_current().resource_metering;
    assert!(new_config.enabled);
    assert_eq!(new_config.agent_address, "localhost:8888".to_string());
    assert_eq!(new_config.precision, ReadableDuration::secs(20));
    assert_eq!(new_config.report_agent_interval, ReadableDuration::secs(80));
    assert_eq!(new_config.max_resource_groups, 3000);

    let task = rx.recv().unwrap();

    match task {
        Task::ConfigChange(config) => {
            assert_eq!(config, new_config)
        }
        _ => unreachable!(),
    }

    worker.stop_worker();
}
