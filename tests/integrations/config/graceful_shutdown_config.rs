// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use tikv::config::TikvConfig;

#[test]
fn test_graceful_shutdown_config_defaults() {
    let config = TikvConfig::default();

    // Test default values
    assert_eq!(
        config.server.graceful_shutdown_timeout.0,
        Duration::from_secs(20)
    );
}

#[test]
fn test_graceful_shutdown_config_serialization() {
    let mut config = TikvConfig::default();
    config.server.graceful_shutdown_timeout = tikv_util::config::ReadableDuration::secs(25);

    // Test TOML serialization (basic structure test)
    let toml_value = toml::Value::try_from(&config.server).unwrap();
    let server_table = toml_value.as_table().unwrap();

    assert!(server_table.contains_key("graceful-shutdown-timeout"));
}
