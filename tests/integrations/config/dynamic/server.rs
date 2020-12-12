use std::sync::{Arc, RwLock};
use tikv::config::{ConfigController, Module, TiKvConfig};
use tikv::server::config::ServerConfigManager;
use tikv_util::config::ReadableDuration;

#[test]
fn test_update_server_config() {
    let (mut cfg, _dir) = TiKvConfig::with_tmp().unwrap();
    cfg.validate().unwrap();
    let cfg_track = Arc::new(RwLock::new(cfg.server.clone()));
    let cfg_controller = ConfigController::new(cfg);
    cfg_controller.register(Module::Server, Box::new(ServerConfigManager(cfg_track)));

    // skipped config should not be changed
    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert("server.addr".to_owned(), "localhost:3000".to_owned());
        m
    };
    let res = cfg_controller.update(change);
    assert!(res.is_err());

    // supported configs should be changeable.
    let change = {
        let mut m = std::collections::HashMap::new();
        m.insert(
            "server.end-point-slow-log-threshold".to_owned(),
            "15s".to_owned(),
        );
        m
    };
    cfg_controller.update(change).unwrap();
    assert_eq!(
        cfg_controller
            .get_current()
            .server
            .end_point_slow_log_threshold,
        ReadableDuration::secs(15)
    );
}
