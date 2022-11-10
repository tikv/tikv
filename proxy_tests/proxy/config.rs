// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use clap::{App, Arg};
use proxy_server::{
    config::{
        address_proxy_config, ensure_no_common_unrecognized_keys, get_last_config,
        memory_limit_for_cf, setup_default_tikv_config, validate_and_persist_config,
        TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR, TIFLASH_DEFAULT_LISTENING_ADDR,
        TIFLASH_DEFAULT_STATUS_ADDR,
    },
    proxy::{gen_proxy_config, gen_tikv_config},
    setup::overwrite_config_with_cmd_args,
};
use tikv_util::sys::SysQuota;

use crate::proxy::*;

/// We test here if we can use proxy's default value without given file.
/// Normally, we only need to add config tests in
/// `test_config_proxy_default_no_config_item`.
#[test]
fn test_config_proxy_default_no_config_file() {
    let args: Vec<&str> = vec![];
    let matches = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .get_matches_from(args);
    let mut v: Vec<String> = vec![];
    let mut config = gen_tikv_config(&None, false, &mut v);
    let mut proxy_config = gen_proxy_config(&None, false, &mut v);
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);

    assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
    assert_eq!(config.server.status_addr, TIFLASH_DEFAULT_STATUS_ADDR);
    assert_eq!(
        config.server.advertise_status_addr,
        TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR
    );
    assert_eq!(
        config.raft_store.region_worker_tick_interval.as_millis(),
        500
    );
}

/// We test here if we can use proxy's default value with given file,
/// but without given field.
/// Add assertion in this function, if we add some new items in
/// `ProxyConfig`.
#[test]
fn test_config_proxy_default_no_config_item() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let text = "z=4\n[rocksdb]\nmax-open-files=56\n";
    write!(file, "{}", text).unwrap();
    let path = file.path();
    let cpath = Some(path.as_os_str());
    let args = vec![format!("-C{}", path.to_str().unwrap())];
    let matches = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .get_matches_from(args);
    let mut v: Vec<String> = vec![];
    let mut config = gen_tikv_config(&cpath, false, &mut v);
    let mut proxy_config = gen_proxy_config(&cpath, false, &mut v);
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);

    let total_mem = SysQuota::memory_limit_in_bytes();
    let cpu_num = SysQuota::cpu_cores_quota();
    assert_eq!(config.rocksdb.max_open_files, 56);
    assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
    assert_eq!(config.server.status_addr, TIFLASH_DEFAULT_STATUS_ADDR);
    assert_eq!(
        config.server.advertise_status_addr,
        TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR
    );
    assert_eq!(
        config.raft_store.region_worker_tick_interval.as_millis(),
        500
    );
    assert_eq!(
        ProxyConfig::default()
            .raft_store
            .apply_low_priority_pool_size,
        config.raft_store.apply_batch_system.low_priority_pool_size
    );
    assert_eq!(
        config.raftdb.defaultcf.block_cache_size,
        memory_limit_for_cf(true, CF_DEFAULT, total_mem)
    );
    assert_eq!(
        config.rocksdb.defaultcf.block_cache_size,
        memory_limit_for_cf(false, CF_DEFAULT, total_mem)
    );
    assert_eq!(
        config.rocksdb.writecf.block_cache_size,
        memory_limit_for_cf(false, CF_WRITE, total_mem)
    );
    assert_eq!(
        config.rocksdb.lockcf.block_cache_size,
        memory_limit_for_cf(false, CF_LOCK, total_mem)
    );
    assert_eq!(config.storage.reserve_space, ReadableSize::gb(1));

    let background_thread_count = std::cmp::min(4, cpu_num as usize);
    assert_eq!(
        config.server.background_thread_count,
        background_thread_count
    );

    assert_eq!(config.import.num_threads, 4);
}

/// We test if the engine-label is set properly.
#[test]
fn test_config_proxy_engine_label() {
    // case-1: If engine-label not specified in arguments, use default value.
    let args: Vec<&str> = vec![];
    let matches = App::new("RaftStore Proxy").get_matches_from(args);
    let mut v: Vec<String> = vec![];
    let mut config = gen_tikv_config(&None, false, &mut v);
    let mut proxy_config = gen_proxy_config(&None, false, &mut v);
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);
    const DEFAULT_ENGINE_LABEL_KEY: &str = "engine";

    assert_eq!(
        config
            .server
            .labels
            .get(DEFAULT_ENGINE_LABEL_KEY)
            .unwrap()
            .as_str(),
        option_env!("ENGINE_LABEL_VALUE").unwrap()
    );

    // case-2: If engine-label specified in arguments, use it as engine-label.
    const EXPECTED_ENGINE_LABEL: &str = "tiflash_compute";
    let args = vec![
        "test_config_proxy_default1",
        "--engine-label",
        EXPECTED_ENGINE_LABEL,
    ];
    let matches = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("engine-label")
                .long("engine-label")
                .help("Set engine label")
                .required(true)
                .takes_value(true),
        )
        .get_matches_from(args);
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);
    assert_eq!(
        config.server.labels.get(DEFAULT_ENGINE_LABEL_KEY).unwrap(),
        EXPECTED_ENGINE_LABEL
    );
}

// We test whether Proxy will overwrite TiKV's value,
// when a config item which is both defined by ProxyConfig and TikvConfig.
// We only nned to add tests to this function when the logic is different.
#[test]
fn test_config_proxy_overwrite() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    write!(
        file,
        "
[raftstore]
apply-low-priority-pool-size = 41
    "
    )
    .unwrap();
    let path = file.path();

    let mut v: Vec<String> = vec![];
    let cpath = Some(path.as_os_str());
    let mut config = gen_tikv_config(&cpath, false, &mut v);
    let proxy_config = gen_proxy_config(&cpath, false, &mut v);
    address_proxy_config(&mut config, &proxy_config);

    // When raftstore.apply-low-priority-pool-size is specified, its value
    // should be used.
    assert_eq!(
        41,
        config.raft_store.apply_batch_system.low_priority_pool_size
    );
}
