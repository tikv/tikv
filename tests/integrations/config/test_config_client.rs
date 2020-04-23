// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use configuration::{ConfigChange, Configuration};
use raftstore::store::Config as RaftstoreConfig;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tikv::config::*;

fn change(name: &str, value: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(name.to_owned(), value.to_owned());
    m
}

#[test]
fn test_update_config() {
    let (cfg, _dir) = TiKvConfig::with_tmp().unwrap();
    let mut cfg_controller = ConfigController::new(cfg);
    let mut cfg = cfg_controller.get_current().clone();

    // normal update
    cfg_controller
        .update(change("raftstore.raft-log-gc-threshold", "2000"))
        .unwrap();
    cfg.raft_store.raft_log_gc_threshold = 2000;
    assert_eq!(cfg_controller.get_current(), &cfg);

    // update not support config
    let res = cfg_controller.update(change("server.addr", "localhost:3000"));
    assert!(res.is_err());
    assert_eq!(cfg_controller.get_current(), &cfg);

    // update to invalid config
    let res = cfg_controller.update(change("raftstore.raft-log-gc-threshold", "0"));
    assert!(res.is_err());
    assert_eq!(cfg_controller.get_current(), &cfg);

    // bad update request
    let res = cfg_controller.update(change("xxx.yyy", "0"));
    assert!(res.is_err());
    let res = cfg_controller.update(change("raftstore.xxx", "0"));
    assert!(res.is_err());
    let res = cfg_controller.update(change("raftstore.raft-log-gc-threshold", "10MB"));
    assert!(res.is_err());
    let res = cfg_controller.update(change("raft-log-gc-threshold", "10MB"));
    assert!(res.is_err());
    assert_eq!(cfg_controller.get_current(), &cfg);
}

#[test]
fn test_dispatch_change() {
    use configuration::ConfigManager;
    use std::error::Error;
    use std::result::Result;

    #[derive(Clone)]
    struct CfgManager(Arc<Mutex<RaftstoreConfig>>);

    impl ConfigManager for CfgManager {
        fn dispatch(&mut self, c: ConfigChange) -> Result<(), Box<dyn Error>> {
            self.0.lock().unwrap().update(c);
            Ok(())
        }
    }

    let (cfg, _dir) = TiKvConfig::with_tmp().unwrap();
    let mut cfg_controller = ConfigController::new(cfg);
    let mut cfg = cfg_controller.get_current().clone();
    let mgr = CfgManager(Arc::new(Mutex::new(cfg.raft_store.clone())));
    cfg_controller.register(Module::Raftstore, Box::new(mgr.clone()));

    cfg_controller
        .update(change("raftstore.raft-log-gc-threshold", "2000"))
        .unwrap();

    // config update
    cfg.raft_store.raft_log_gc_threshold = 2000;
    assert_eq!(cfg_controller.get_current(), &cfg);

    // config change should also dispatch to raftstore config manager
    assert_eq!(mgr.0.lock().unwrap().raft_log_gc_threshold, 2000);
}
