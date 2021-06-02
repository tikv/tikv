// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::{channel, Receiver, Sender};

use resource_metering::agent::{ConfigManager, Task};
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv_util::worker::{LazyWorker, Runnable};

pub struct MockResourceMeteringAgent {
    tx: Sender<Task>,
}

impl MockResourceMeteringAgent {
    fn new(tx: Sender<Task>) -> Self {
        MockResourceMeteringAgent { tx }
    }
}

impl Runnable for MockResourceMeteringAgent {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        self.tx.send(task).unwrap();
    }
}

fn setup_cfg_manager(
    config: TiKvConfig,
) -> (ConfigController, Receiver<Task>, Box<LazyWorker<Task>>) {
    let mut worker = Box::new(LazyWorker::new("resource-metering-agent"));
    let scheduler = worker.scheduler();

    let (tx, rx) = channel();

    let resource_metering_config = config.resource_metering_agent.clone();
    let cfg_controller = ConfigController::new(config);
    cfg_controller.register(
        Module::ResourceMeteringAgent,
        Box::new(ConfigManager::new(resource_metering_config, scheduler)),
    );

    worker.start(MockResourceMeteringAgent::new(tx));
    (cfg_controller, rx, worker)
}

#[test]
fn test_update_resource_metering_agent_config() {
    let (mut config, _dir) = TiKvConfig::with_tmp().unwrap();
    config.resource_metering_agent.agent_address = "localhost:8888".to_owned();
    config.validate().unwrap();

    let (cfg_controller, rx, worker) = setup_cfg_manager(config);

    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "resource-metering-agent.enabled".to_owned(),
            "true".to_owned(),
        );
        m.insert(
            "resource-metering-agent.precision-seconds".to_owned(),
            "20".to_owned(),
        );
        m.insert(
            "resource-metering-agent.max-resource-groups".to_owned(),
            "3000".to_owned(),
        );
        m
    };
    cfg_controller.update(change).unwrap();

    let new_config = cfg_controller.get_current().resource_metering_agent;
    assert!(new_config.enabled);
    assert_eq!(new_config.agent_address, "localhost:8888".to_string());
    assert_eq!(new_config.precision_seconds, 20);
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
