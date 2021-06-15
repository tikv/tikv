// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use resource_metering::ConfigManagerBuilder;
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv_util::config::ReadableDuration;

#[test]
fn test_update_resource_metering_agent_config() {
    let (mut config, _dir) = TiKvConfig::with_tmp().unwrap();
    config.validate().unwrap();

    let cfg_controller = ConfigController::new(config.clone());

    let config = Arc::new(Mutex::new(config.resource_metering.clone()));
    let cfg_mgr = {
        let mut cfg_mgr_builder = ConfigManagerBuilder::new();

        let cfg = config.clone();
        cfg_mgr_builder.on_change_max_resource_groups(move |m| {
            let mut c = cfg.lock().unwrap();
            c.max_resource_groups = m;
        });

        let cfg = config.clone();
        cfg_mgr_builder.on_change_agent_address(move |a| {
            let mut c = cfg.lock().unwrap();
            c.agent_address = a.to_owned();
        });

        let cfg = config.clone();
        cfg_mgr_builder.on_change_enabled(move |e| {
            let mut c = cfg.lock().unwrap();
            c.enabled = e;
        });

        let cfg = config.clone();
        cfg_mgr_builder.on_change_precision(move |p| {
            let mut c = cfg.lock().unwrap();
            c.precision = p;
        });

        let cfg = config.clone();
        cfg_mgr_builder.on_change_report_agent_interval(move |r| {
            let mut c = cfg.lock().unwrap();
            c.report_agent_interval = r;
        });

        cfg_mgr_builder.build()
    };

    cfg_controller.register(Module::ResourceMetering, Box::new(cfg_mgr));

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

    assert_eq!(&*config.lock().unwrap(), &new_config);
}
