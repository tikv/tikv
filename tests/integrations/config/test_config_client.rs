// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kvproto::configpb::*;

use configuration::{ConfigChange, Configuration};
use pd_client::errors::Result;
use pd_client::PdClient;
use tikv::config::*;
use tikv::raftstore::store::Config as RaftstoreConfig;
use tikv_util::config::ReadableDuration;
use tikv_util::worker::FutureWorker;

struct MockPdClient {
    configs: Mutex<HashMap<String, Config>>,
}

#[derive(Clone)]
struct Config {
    version: Version,
    content: String,
    update: Vec<ConfigEntry>,
}

impl Config {
    fn new(version: Version, content: String, update: Vec<ConfigEntry>) -> Self {
        Config {
            version,
            content,
            update,
        }
    }
}

impl MockPdClient {
    fn new() -> Self {
        MockPdClient {
            configs: Mutex::new(HashMap::new()),
        }
    }

    fn register(self: Arc<Self>, id: &str, cfg: TiKvConfig) -> ConfigHandler {
        let (version, cfg) = ConfigHandler::create(id.to_owned(), self, cfg).unwrap();
        ConfigHandler::start(
            id.to_owned(),
            ConfigController::new(cfg),
            version,
            FutureWorker::new("test-pd-worker").scheduler(),
        )
        .unwrap()
    }

    fn update_cfg<F>(&self, id: &str, f: F)
    where
        F: Fn(&mut TiKvConfig),
    {
        let mut configs = self.configs.lock().unwrap();
        let cfg = configs.get_mut(id).unwrap();
        let mut config: TiKvConfig = toml::from_str(&cfg.content).unwrap();
        f(&mut config);
        cfg.content = toml::to_string(&config).unwrap();
        cfg.version.local += 1;
    }

    fn update_raw<F>(&self, id: &str, f: F)
    where
        F: Fn(&mut String),
    {
        let mut configs = self.configs.lock().unwrap();
        let cfg = configs.get_mut(id).unwrap();
        f(&mut cfg.content);
        cfg.version.local += 1;
    }

    fn get(&self, id: &str) -> Config {
        self.configs.lock().unwrap().get(id).unwrap().clone()
    }
}

impl PdClient for MockPdClient {
    fn register_config(&self, id: String, v: Version, cfg: String) -> Result<CreateResponse> {
        let old = self
            .configs
            .lock()
            .unwrap()
            .insert(id.clone(), Config::new(v.clone(), cfg.clone(), Vec::new()));
        assert!(old.is_none(), format!("id {} already be registered", id));

        let mut status = Status::default();
        status.set_code(StatusCode::Ok);
        let mut resp = CreateResponse::default();
        resp.set_status(status);
        resp.set_config(cfg);
        resp.set_version(v);
        Ok(resp)
    }

    fn get_config(&self, id: String, version: Version) -> Result<GetResponse> {
        let mut resp = GetResponse::default();
        let mut status = Status::default();
        let configs = self.configs.lock().unwrap();
        if let Some(cfg) = configs.get(&id) {
            match cmp_version(&cfg.version, &version) {
                Ordering::Equal => status.set_code(StatusCode::NotChange),
                _ => {
                    resp.set_config(cfg.content.clone());
                    status.set_code(StatusCode::WrongVersion);
                }
            }
            resp.set_version(cfg.version.clone());
        } else {
            status.set_code(StatusCode::Unknown);
        }
        resp.set_status(status);
        Ok(resp)
    }

    fn update_config(
        &self,
        id: String,
        version: Version,
        mut entries: Vec<ConfigEntry>,
    ) -> Result<UpdateResponse> {
        let mut resp = UpdateResponse::default();
        let mut status = Status::default();
        if let Some(cfg) = self.configs.lock().unwrap().get_mut(&id) {
            match cmp_version(&cfg.version, &version) {
                Ordering::Equal => {
                    cfg.update.append(&mut entries);
                    cfg.version.local += 1;
                    status.set_code(StatusCode::Ok);
                }
                _ => status.set_code(StatusCode::WrongVersion),
            }
            resp.set_version(cfg.version.clone());
        } else {
            status.set_code(StatusCode::Unknown);
        }
        resp.set_status(status);
        Ok(resp)
    }
}

fn validated_cfg() -> TiKvConfig {
    let mut cfg = TiKvConfig::default();
    cfg.validate().unwrap();
    cfg
}

#[test]
fn test_update_config() {
    let pd_client = Arc::new(MockPdClient::new());
    let id = "localhost:1080";

    // register config
    let mut cfg_handler = pd_client.clone().register(id, validated_cfg());

    // refresh local config
    cfg_handler.refresh_config(pd_client.clone()).unwrap();

    // nothing change if there are no update on pd side
    assert_eq!(cfg_handler.get_config(), &validated_cfg());

    // update config on pd side
    pd_client.update_cfg(id, |cfg| {
        cfg.refresh_config_interval = ReadableDuration::hours(12);
    });

    // refresh local config
    cfg_handler.refresh_config(pd_client.clone()).unwrap();

    // config update
    let mut cfg = validated_cfg();
    cfg.refresh_config_interval = ReadableDuration::hours(12);
    assert_eq!(cfg_handler.get_config(), &cfg);
}

#[test]
fn test_update_not_support_config() {
    let pd_client = Arc::new(MockPdClient::new());
    let id = "localhost:1080";

    // register config
    let mut cfg_handler = pd_client.clone().register(id, validated_cfg());

    // update not support config on pd side
    pd_client.update_cfg(id, |cfg| {
        cfg.server.addr = "localhost:3000".to_owned();
    });

    // refresh local config
    cfg_handler.refresh_config(pd_client.clone()).unwrap();

    // nothing change
    assert_eq!(cfg_handler.get_config(), &validated_cfg());
}

#[test]
fn test_update_to_invalid() {
    let pd_client = Arc::new(MockPdClient::new());
    let id = "localhost:1080";

    let mut cfg = validated_cfg();
    cfg.raft_store.raft_log_gc_threshold = 2000;

    // register config
    let mut cfg_handler = pd_client.clone().register(id, cfg);

    // update invalid config on pd side
    pd_client.update_cfg(id, |cfg| {
        cfg.raft_store.raft_log_gc_threshold = 0;
    });

    // refresh local config
    cfg_handler.refresh_config(pd_client.clone()).unwrap();

    // local config should not change
    assert_eq!(
        cfg_handler.get_config().raft_store.raft_log_gc_threshold,
        2000
    );

    // config on pd side should be rollbacked to valid config
    let cfg = pd_client.get(id);
    assert_eq!(cfg.update.len(), 1);
    assert_eq!(cfg.update[0].name, "raftstore.raft-log-gc-threshold");
    assert_eq!(cfg.update[0].value, toml::to_string(&2000).unwrap());
}

#[test]
fn test_compatible_config() {
    let pd_client = Arc::new(MockPdClient::new());
    let id = "localhost:1080";

    // register config
    let mut cfg_handler = pd_client.clone().register(id, validated_cfg());

    // update config on pd side with misssing config, new config and exist config
    pd_client.update_raw(id, |cfg| {
        *cfg = "
            [new.config]
            xyz = 1
            [raftstore]
            raft-log-gc-threshold = 2048
        "
        .to_owned();
    });

    // refresh local config
    cfg_handler.refresh_config(pd_client.clone()).unwrap();

    let mut new_cfg = validated_cfg();
    new_cfg.raft_store.raft_log_gc_threshold = 2048;
    assert_eq!(cfg_handler.get_config(), &new_cfg);
}

#[test]
fn test_dispatch_change() {
    use std::error::Error;
    use std::result::Result;
    use tikv::config::ConfigManager;

    #[derive(Clone)]
    struct CfgManager(Arc<Mutex<RaftstoreConfig>>);

    impl ConfigManager for CfgManager {
        fn dispatch(&mut self, c: ConfigChange) -> Result<(), Box<dyn Error>> {
            self.0.lock().unwrap().update(c);
            Ok(())
        }
    }

    let pd_client = Arc::new(MockPdClient::new());
    let id = "localhost:1080";
    let cfg = validated_cfg();
    let mgr = CfgManager(Arc::new(Mutex::new(Default::default())));

    // register config and raftstore config manager
    let mut cfg_handler = {
        let (version, cfg) = ConfigHandler::create(id.to_owned(), pd_client.clone(), cfg).unwrap();
        *mgr.0.lock().unwrap() = cfg.raft_store.clone();
        let mut controller = ConfigController::new(cfg);
        controller.register("raft_store", Box::new(mgr.clone()));
        ConfigHandler::start(
            id.to_owned(),
            controller,
            version,
            FutureWorker::new("test-pd-worker").scheduler(),
        )
        .unwrap()
    };

    pd_client.update_cfg(id, |cfg| {
        cfg.raft_store.raft_log_gc_threshold = 2000;
    });

    // refresh local config
    cfg_handler.refresh_config(pd_client.clone()).unwrap();

    // config update
    assert_eq!(
        cfg_handler.get_config().raft_store.raft_log_gc_threshold,
        2000
    );
    // config change should also dispatch to raftstore config manager
    assert_eq!(mgr.0.lock().unwrap().raft_log_gc_threshold, 2000);
}
