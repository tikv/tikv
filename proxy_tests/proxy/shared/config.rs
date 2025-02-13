// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use clap::{App, Arg};
use proxy_server::{
    config::{
        address_proxy_config, memory_limit_for_cf, TIFLASH_DEFAULT_ADVERTISE_LISTENING_ADDR,
        TIFLASH_DEFAULT_LISTENING_ADDR, TIFLASH_DEFAULT_STATUS_ADDR,
    },
    proxy::{gen_proxy_config, gen_tikv_config},
    setup::overwrite_config_with_cmd_args,
};
use tikv::config::MEMORY_USAGE_LIMIT_RATE;
use tikv_util::sys::SysQuota;

use crate::utils::v1::*;

/// We test here if we can use proxy's default value without given file.
/// Normally, we only need to add config tests in
/// `test_default_no_config_item`.
#[test]
fn test_default_no_config_file() {
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
fn test_default_no_config_item() {
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
    use serde_json::Value;
    let json_format_proxy = serde_json::to_string(&proxy_config).unwrap();
    let parsed_json_proxy: Value = serde_json::from_str(json_format_proxy.as_str()).unwrap();
    {
        let raft_store = parsed_json_proxy
            .as_object()
            .expect("fail")
            .get("raftstore")
            .unwrap();
        let snap_handle_pool_size = raft_store.get("snap-handle-pool-size").unwrap();
        assert_eq!(
            proxy_config.raft_store.snap_handle_pool_size as u64,
            snap_handle_pool_size.as_u64().unwrap()
        );
    }

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
        Some(memory_limit_for_cf(true, CF_DEFAULT, total_mem))
    );
    assert_eq!(
        config.rocksdb.defaultcf.block_cache_size,
        Some(memory_limit_for_cf(false, CF_DEFAULT, total_mem))
    );
    assert_eq!(
        config.rocksdb.writecf.block_cache_size,
        Some(memory_limit_for_cf(false, CF_WRITE, total_mem))
    );
    assert_eq!(
        config.rocksdb.lockcf.block_cache_size,
        Some(memory_limit_for_cf(false, CF_LOCK, total_mem))
    );
    assert_eq!(config.storage.reserve_space, ReadableSize::gb(1));

    let background_thread_count = std::cmp::min(4, cpu_num as usize);
    assert_eq!(
        config.server.background_thread_count,
        background_thread_count
    );

    assert_eq!(
        config.import.num_threads,
        std::cmp::max(4, (cpu_num * 2.0) as usize)
    );
    assert_eq!(config.server.status_thread_pool_size, 2);

    assert_eq!(config.raft_store.evict_cache_on_memory_ratio, 0.1);
    assert_eq!(config.memory_usage_high_water, 0.9);
    assert_eq!(config.server.reject_messages_on_memory_ratio, 0.05);

    assert_eq!(config.raft_store.enable_v2_compatible_learner, true);
}

#[test]
fn test_cmdline_overwrite() {
    let args = vec!["test_cmdline_overwrite1", "--unips-enabled", "1"];
    let matches = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("unips-enabled")
                .long("unips-enabled")
                .required(true)
                .takes_value(true),
        )
        .get_matches_from(args);
    let mut v: Vec<String> = vec![];
    let mut config = gen_tikv_config(&None, false, &mut v);
    let mut proxy_config = gen_proxy_config(&None, false, &mut v);
    proxy_config.engine_store.enable_unips = false;
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);
    assert_eq!(proxy_config.engine_store.enable_unips, true);
}

/// We test if the engine-label is set properly.
#[test]
fn test_engine_label() {
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

#[test]
fn test_config_proxy_engine_role_label() {
    let mut v: Vec<String> = vec![];
    let mut config = gen_tikv_config(&None, false, &mut v);
    let mut proxy_config = gen_proxy_config(&None, false, &mut v);
    let app = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .alias("label")
                .takes_value(true)
                .value_name("KEY=VALUE")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets server labels")
                .long_help(
                    "Set the server labels. Uses `,` to separate kv pairs, like \
                `zone=cn,disk=ssd`",
                ),
        )
        .arg(
            Arg::with_name("engine-label")
                .long("engine-label")
                .help("Set engine label")
                .required(true)
                .takes_value(true),
        );
    // case-1: If engine-role label specified in neither the argument `--labels` nor
    // proxy's config file, it's none.
    let args = vec!["test_config_proxy_default1", "--engine-label", "tiflash"];
    let matches = app.clone().get_matches_from(args);
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);

    const DEFAULT_ENGINE_ROLE_LABEL_KEY: &str = "engine_role";

    assert_eq!(
        config
            .server
            .labels
            .get(DEFAULT_ENGINE_ROLE_LABEL_KEY)
            .is_none(),
        true
    );

    // case-2: If engine-role label specified in the argument `--label`, use it as
    // engine-role label.
    let args = vec![
        "test_config_proxy_default1",
        "--engine-label",
        "tiflash",
        "--labels",
        "engine_role=write",
    ];
    let matches = app.clone().get_matches_from(args);
    overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
    address_proxy_config(&mut config, &proxy_config);
    assert_eq!(
        config
            .server
            .labels
            .get(DEFAULT_ENGINE_ROLE_LABEL_KEY)
            .unwrap(),
        "write"
    );
}

// We test whether Proxy will overwrite TiKV's value,
// when a config item which is both defined by ProxyConfig and TikvConfig.
// We only need to add tests to this function when the logic is different.
#[test]
fn test_overwrite() {
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

#[test]
fn test_owned_config() {
    test_util::init_log_for_test();
    let mut file = tempfile::NamedTempFile::new().unwrap();
    write!(
        file,
        "
[engine-store]
enable-fast-add-peer = true
    "
    )
    .unwrap();
    let path = file.path();

    let mut v: Vec<String> = vec![];
    let cpath = Some(path.as_os_str());
    let proxy_config = gen_proxy_config(&cpath, false, &mut v);

    info!("using proxy config"; "config" => ?proxy_config);
    assert_eq!(true, proxy_config.engine_store.enable_fast_add_peer);
}

#[test]
fn test_memory_limit_overwrite() {
    let app = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("memory-limit-size")
                .long("memory-limit-size")
                .help("Used as the maximum memory we can consume, in bytes")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("memory-limit-ratio")
                .long("memory-limit-ratio")
                .help("Used as the maximum memory we can consume, in percentage")
                .takes_value(true),
        );

    let bootstrap = |args: Vec<&str>| {
        let mut v: Vec<String> = vec![];
        let matches = app.clone().get_matches_from(args);
        let mut config = gen_tikv_config(&None, false, &mut v);
        let mut proxy_config = gen_proxy_config(&None, false, &mut v);
        proxy_config.raftdb.defaultcf.block_cache_size = Some(ReadableSize(0));
        proxy_config.rocksdb.defaultcf.block_cache_size = Some(ReadableSize(0));
        proxy_config.rocksdb.lockcf.block_cache_size = Some(ReadableSize(0));
        proxy_config.rocksdb.writecf.block_cache_size = Some(ReadableSize(0));
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);
        config.compatible_adjust();
        config
    };

    {
        let args = vec![
            "test_memory_limit_overwrite1",
            "--memory-limit-size",
            "12345",
        ];
        let mut config = bootstrap(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(12345)));
    }

    {
        let args = vec![
            "test_memory_limit_overwrite2",
            "--memory-limit-size",
            "12345",
            "--memory-limit-ratio",
            "0.9",
        ];
        let mut config = bootstrap(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(12345)));
    }

    let total = SysQuota::memory_limit_in_bytes();
    {
        let args = vec![
            "test_memory_limit_overwrite3",
            "--memory-limit-ratio",
            "0.800000",
        ];
        let mut config = bootstrap(args);
        assert!(config.validate().is_ok());
        let limit = (total as f64 * 0.8) as u64;
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(limit)));
    }

    let default_limit = (total as f64 * MEMORY_USAGE_LIMIT_RATE) as u64;
    {
        let args = vec![
            "test_memory_limit_overwrite4",
            "--memory-limit-ratio",
            "7.9",
        ];
        let mut config = bootstrap(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(default_limit)));
    }

    {
        let args = vec![
            "test_memory_limit_overwrite5",
            "--memory-limit-ratio",
            "'-0.9'",
        ];
        let mut config = bootstrap(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(default_limit)));
    }

    {
        let args = vec!["test_memory_limit_overwrite6"];
        let mut config = bootstrap(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(default_limit)));
    }

    let bootstrap2 = |args: Vec<&str>| {
        let mut v: Vec<String> = vec![];
        let matches = app.clone().get_matches_from(args);
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "
memory-usage-limit = 42
            "
        )
        .unwrap();
        let path = file.path();
        let cpath = Some(path.as_os_str());
        let mut config = gen_tikv_config(&cpath, false, &mut v);
        let mut proxy_config = gen_proxy_config(&cpath, false, &mut v);
        proxy_config.raftdb.defaultcf.block_cache_size = Some(ReadableSize(0));
        proxy_config.rocksdb.defaultcf.block_cache_size = Some(ReadableSize(0));
        proxy_config.rocksdb.lockcf.block_cache_size = Some(ReadableSize(0));
        proxy_config.rocksdb.writecf.block_cache_size = Some(ReadableSize(0));
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);
        config.compatible_adjust();
        config
    };

    {
        let args = vec![
            "test_memory_limit_nooverwrite3",
            "--memory-limit-ratio",
            "0.800000",
        ];
        let mut config = bootstrap2(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(42)));
    }

    {
        let args = vec![
            "test_memory_limit_nooverwrite1",
            "--memory-limit-size",
            "12345",
        ];
        let mut config = bootstrap2(args);
        assert!(config.validate().is_ok());
        assert_eq!(config.memory_usage_limit, Some(ReadableSize(42)));
    }
}

#[test]
fn test_label_overwrite() {
    let app = App::new("RaftStore Proxy")
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .alias("label")
                .takes_value(true)
                .value_name("KEY=VALUE")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets server labels")
                .long_help(
                    "Set the server labels. Uses `,` to separate kv pairs, like \
                `zone=cn,disk=ssd`",
                ),
        )
        .arg(
            Arg::with_name("engine-label")
                .long("engine-label")
                .help("Set engine label")
                .required(true)
                .takes_value(true),
        );

    let get_config = |args: Vec<&str>, labels: Option<&str>| {
        let mut v: Vec<String> = vec![];
        // args
        let matches = app.clone().get_matches_from(args);
        // Gen tiflash_tikv.toml
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "
[server]
[server.labels]
{}
            ",
            labels.unwrap_or_default()
        )
        .unwrap();
        let cpath = Some(file.path().as_os_str());
        let mut config = gen_tikv_config(&cpath, false, &mut v);
        let mut proxy_config = gen_proxy_config(&cpath, false, &mut v);
        overwrite_config_with_cmd_args(&mut config, &mut proxy_config, &matches);
        address_proxy_config(&mut config, &proxy_config);
        config.compatible_adjust();
        config
    };

    {
        let args = vec!["test_label", "--engine-label", "tiflash"];
        let mut config = get_config(args, None);
        assert!(config.validate().is_ok());
        let expected_labels = [("engine".to_owned(), "tiflash".to_owned())]
            .iter()
            .cloned()
            .collect();
        assert!(config.server.labels == expected_labels);
    }
    {
        let args = vec!["test_label", "--engine-label", "tiflash_compute"];
        let mut config = get_config(args, None);
        assert!(config.validate().is_ok());
        let expected_labels = [("engine".to_owned(), "tiflash_compute".to_owned())]
            .iter()
            .cloned()
            .collect();
        assert!(config.server.labels == expected_labels);
    }

    {
        let args = vec!["test_label", "--labels", "k=v", "--engine-label", "tiflash"];
        let mut config = get_config(args, None);
        assert!(config.validate().is_ok());
        let expected_labels = [
            ("engine".to_owned(), "tiflash".to_owned()),
            ("k".to_owned(), "v".to_owned()),
        ]
        .iter()
        .cloned()
        .collect();
        assert!(config.server.labels == expected_labels);
    }

    {
        let args = vec![
            "test_label",
            "--labels",
            "k=v,exclusive=no-data",
            "--engine-label",
            "tiflash_compute",
        ];
        let mut config = get_config(args, None);
        assert!(config.validate().is_ok());
        let expected_labels = [
            ("engine".to_owned(), "tiflash_compute".to_owned()),
            ("k".to_owned(), "v".to_owned()),
            ("exclusive".to_owned(), "no-data".to_owned()),
        ]
        .iter()
        .cloned()
        .collect();
        assert!(config.server.labels == expected_labels);
    }

    {
        let args = vec!["test_label", "--engine-label", "tiflash"];
        // server.labels define in tiflash_tikv.toml
        let labels = Some(
            r#"
        k="v2"
        abc="def"
        "#,
        );
        let mut config = get_config(args, labels);
        assert!(config.validate().is_ok());
        let expected_labels = [
            ("engine".to_owned(), "tiflash".to_owned()),
            ("k".to_owned(), "v2".to_owned()),
            ("abc".to_owned(), "def".to_owned()),
        ]
        .iter()
        .cloned()
        .collect();
        assert!(config.server.labels == expected_labels);
    }

    {
        let args = vec![
            "test_label",
            "--labels",
            "k=v,a=b",
            "--engine-label",
            "tiflash",
        ];
        // server.labels define in tiflash_tikv.toml
        let labels = Some(
            r#"
        k="v2"
        abc="def"
        "#,
        );
        let mut config = get_config(args, labels);
        assert!(config.validate().is_ok());
        // merge `server.labels` in tiflash_tikv.toml and `--labels` in command line
        // "k=v" in `--labels` should be overwritten by "k=v2" in tiflash_tikv.toml
        let expected_labels = [
            ("engine".to_owned(), "tiflash".to_owned()),
            ("k".to_owned(), "v2".to_owned()),
            ("a".to_owned(), "b".to_owned()),
            ("abc".to_owned(), "def".to_owned()),
        ]
        .iter()
        .cloned()
        .collect();
        assert!(config.server.labels == expected_labels);
    }
}
