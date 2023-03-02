// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(proc_macro_hygiene)]

use std::{
    path::{Path, PathBuf},
    process,
    sync::Arc,
};

use clap::{crate_authors, App, Arg};
use encryption_export::data_key_manager_from_config;
use engine_traits::Peekable;
use kvproto::raft_serverpb::StoreIdent;
use raft_engine::ReadableSize as RaftEngineCfgSize;
use serde_json::{Map, Value};
use server::setup::{ensure_no_unrecognized_config, validate_and_persist_config};
use tikv::{
    config::{to_flatten_config_info, TikvConfig},
    server::KvEngineFactoryBuilder,
    storage::config::EngineType,
};
use tikv_util::config::{ReadableDuration, ReadableSize};

fn main() {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    let version_info = tikv::tikv_version_info(build_timestamp);

    let matches = App::new("TiKV agent")
        .about("Start a TiKV instance copy at agent-dir")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
        .arg(
            Arg::with_name("agent-dir")
                .required(true)
                .long("agent-dir")
                .takes_value(true)
                .help("agent directory, will be cleaned if exists"),
            )
        .arg(
            Arg::with_name("skip-build-agent-dir")
                .long("skip-build-agent-dir")
                .takes_value(false)
                .help("whether to skip building the agent directory"),
            )
        .arg(
            Arg::with_name("show-cluster-id")
                .long("show-cluster-id")
                .takes_value(false)
                .help("show cluster id"),
        )
        .arg(
            Arg::with_name("omit-logs")
                .long("omit-logs")
                .takes_value(false)
                .help("whether to omit logs"),
            )
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("config-check")
                .required(false)
                .long("config-check")
                .takes_value(false)
                .help("Check config file validity and exit"),
        )
        .arg(
            Arg::with_name("config-info")
                .required(false)
                .long("config-info")
                .takes_value(true)
                .value_name("FORMAT")
                .possible_values(&["json"])
                .help("print configuration information with specified format")
        )
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .alias("log")
                .takes_value(true)
                .value_name("LEVEL")
                .possible_values(&[
                    "trace", "debug", "info", "warn", "warning", "error", "critical", "fatal",
                ])
                .help("Set the log level"),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true)
                .value_name("FILE")
                .help("Sets log file")
                .long_help("Set the log file path. If not set, logs will output to stderr"),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the listening address"),
        )
        .arg(
            Arg::with_name("advertise-addr")
                .long("advertise-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for client communication"),
        )
        .arg(
            Arg::with_name("status-addr")
                .long("status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the HTTP listening address for the status report service"),
        )
        .arg(
            Arg::with_name("advertise-status-addr")
                .long("advertise-status-addr")
                .takes_value(true)
                .value_name("IP:PORT")
                .help("Set the advertise listening address for the client communication of status report service"),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory used to store data"),
        )
        .arg(
            Arg::with_name("capacity")
                .long("capacity")
                .takes_value(true)
                .value_name("CAPACITY")
                .help("Set the store capacity")
                .long_help("Set the store capacity to use. If not set, use entire partition"),
        )
        .arg(
            Arg::with_name("pd-endpoints")
                .long("pd-endpoints")
                .aliases(&["pd", "pd-endpoint"])
                .takes_value(true)
                .value_name("PD_URL")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets PD endpoints")
                .long_help("Set the PD endpoints to use. Use `,` to separate multiple PDs"),
        )
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
            Arg::with_name("print-sample-config")
                .long("print-sample-config")
                .help("Print a sample config to stdout"),
        )
        .arg(
            Arg::with_name("metrics-addr")
                .long("metrics-addr")
                .value_name("IP:PORT")
                .hidden(true)
                .help("Sets Prometheus Pushgateway address")
                .long_help(
                    "Sets push address to the Prometheus Pushgateway, \
                     leaves it empty will disable Prometheus push",
                ),
        )
        .get_matches();

    if matches.is_present("print-sample-config") {
        let config = TikvConfig::default();
        println!("{}", toml::to_string_pretty(&config).unwrap());
        process::exit(0);
    }

    let mut unrecognized_keys = Vec::new();
    let is_config_check = matches.is_present("config-check");

    let mut config = matches
        .value_of_os("config")
        .map_or_else(TikvConfig::default, |path| {
            let path = Path::new(path);
            TikvConfig::from_file(
                path,
                if is_config_check {
                    Some(&mut unrecognized_keys)
                } else {
                    None
                },
            )
            .unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    path.display(),
                    e
                );
            })
        });

    server::setup::overwrite_config_with_cmd_args(&mut config, &matches);
    config.logger_compatible_adjust();

    if is_config_check {
        validate_and_persist_config(&mut config, false);
        ensure_no_unrecognized_config(&unrecognized_keys);
        println!("config check successful");
        process::exit(0)
    }

    let is_config_info = matches.is_present("config-info");
    if is_config_info {
        let config_infos = to_flatten_config_info(&config);
        let mut result = Map::new();
        result.insert("Component".into(), "TiKV Server".into());
        result.insert("Version".into(), tikv::tikv_build_version().into());
        result.insert("Parameters".into(), Value::Array(config_infos));
        println!("{}", serde_json::to_string_pretty(&result).unwrap());
        process::exit(0);
    }

    let agent_dir = matches.value_of("agent-dir").unwrap();

    if !matches.is_present("skip-build-agent-dir") {
        if let Err(e) = std::fs::remove_dir_all(agent_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!("remove {} fail: {:?}", agent_dir, e);
                process::exit(-1);
            } else {
                println!("remove {} success", agent_dir);
            }
        }

        if let Err(e) = std::fs::create_dir(agent_dir) {
            eprintln!("create {} fail: {:?}", agent_dir, e);
            process::exit(-1);
        }
        println!("create {} success", agent_dir);

        match check_import_empty(&config) {
            Ok(false) => {
                let dir = PathBuf::from(&config.storage.data_dir).join("import");
                eprintln!("import directory {} should be empty", dir.display());
                process::exit(-1);
            }
            Err(e) => {
                eprintln!("check import directory empty fail: {}", e);
                process::exit(-1);
            }
            _ => {}
        }

        // TODO: find a way to only copy necessary snapshot files.

        // Symlink files of kv engine and raft engine to the given agent dir.
        if let Err(e) = symlink_kv_engine_files(&config, agent_dir) {
            eprintln!("symlink_kv_engine_files: {}", e);
            process::exit(-1);
        }
        if let Err(e) = symlink_raft_engine_files(&config, agent_dir) {
            eprintln!("symlink_raft_engine_files: {}", e);
            process::exit(-1);
        }

        // Some files should be copied instead of symlinked.
        if let Err(e) = replace_kv_engine_symlinks(&config, agent_dir) {
            eprintln!("replace_kv_engine_symlinks: {}", e);
            process::exit(-1);
        }
        if let Err(e) = replace_raft_engine_symlinks(&config, agent_dir) {
            eprintln!("replace_raft_engine_symlinks: {}", e);
            process::exit(-1);
        }
    }

    config.storage.data_dir = agent_dir.to_owned();
    config.rocksdb.wal_dir = "".to_owned();
    config.raftdb.wal_dir = "".to_owned();
    config.raft_engine.mut_config().dir = "".to_owned();

    if matches.is_present("show-cluster-id") {
        println!("cluster-id: {:?}", get_cluster_id(&config));
        process::exit(0);
    }

    if matches.is_present("omit-logs") {
        // Avoid logs and place holders take memory.
        config.log.file.filename = "/dev/null".to_owned();
        config.log.file.max_backups = 0;
        config.slow_log_file = "/dev/null".to_owned();
        config.rocksdb.info_log_dir = "/tmp/".to_owned();
        config.raftdb.info_log_dir = "/tmp/".to_owned();
    }

    // Try to avoid generating new disk files.
    config.storage.reserve_space = ReadableSize(0);
    config.storage.reserve_raft_space = ReadableSize(0);
    config.rocksdb.defaultcf.disable_auto_compactions = true;
    config.rocksdb.writecf.disable_auto_compactions = true;
    config.rocksdb.lockcf.disable_auto_compactions = true;
    config.rocksdb.raftcf.disable_auto_compactions = true;
    if config.raft_engine.enable {
        config.raft_engine.mut_config().purge_threshold = RaftEngineCfgSize::gb(1);
        config.raft_engine.mut_config().purge_rewrite_garbage_ratio = 0.99;
        config.raft_engine.mut_config().enable_log_recycle = false;
    } else {
        config.raftdb.defaultcf.disable_auto_compactions = true;
    }

    // To limit memory components.
    config.raft_store.raft_entry_cache_life_time = ReadableDuration::secs(1);
    config.storage.block_cache.capacity = Some(ReadableSize::gb(1));

    match config.storage.engine {
        EngineType::RaftKv => server::server::run_tikv(config),
        EngineType::RaftKv2 => server::server2::run_tikv(config),
    }
}

fn check_import_empty(config: &TikvConfig) -> std::io::Result<bool> {
    let dir = PathBuf::from(&config.storage.data_dir).join("import");
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let fname = entry.file_name().to_str().unwrap().to_owned();
        if fname == ".clone" || fname == ".temp" {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

fn symlink_kv_engine_files(config: &TikvConfig, agent_dir: &str) -> std::io::Result<()> {
    let mut tmp_config = TikvConfig::default();
    tmp_config.storage.data_dir = agent_dir.to_owned();
    let kvdb = tmp_config.infer_kv_engine_path(None).unwrap();
    std::fs::create_dir(&kvdb)?;

    let source_kvdb = config.infer_kv_engine_path(None).unwrap();
    symlink_stuffs(&source_kvdb, &kvdb, &["LOCK"])?;

    if config.rocksdb.wal_dir != "" {
        symlink_stuffs(&config.rocksdb.wal_dir, &kvdb, &[])?;
    }

    Ok(())
}

fn symlink_raft_engine_files(config: &TikvConfig, agent_dir: &str) -> std::io::Result<()> {
    if config.raft_engine.enable {
        let mut tmp_config = TikvConfig::default();
        tmp_config.storage.data_dir = agent_dir.to_owned();
        let raftdb = tmp_config.infer_raft_engine_path(None).unwrap();
        std::fs::create_dir(&raftdb)?;

        let source_raftdb = config.infer_raft_engine_path(None).unwrap();
        symlink_stuffs(&source_raftdb, &raftdb, &["LOCK"])?;
    } else {
        let mut tmp_config = TikvConfig::default();
        tmp_config.storage.data_dir = agent_dir.to_owned();
        let raftdb = tmp_config.infer_raft_db_path(None).unwrap();
        std::fs::create_dir(&raftdb)?;

        let source_raftdb = config.infer_raft_db_path(None).unwrap();
        symlink_stuffs(&source_raftdb, &raftdb, &["LOCK"])?;

        if config.raftdb.wal_dir != "" {
            symlink_stuffs(&config.raftdb.wal_dir, &raftdb, &[])?;
        }
    }

    Ok(())
}

fn symlink_stuffs(source_dir: &str, target_dir: &str, ignored: &[&str]) -> std::io::Result<()> {
    for entry in std::fs::read_dir(&source_dir)? {
        let entry = entry?;
        let fname = entry.file_name().to_str().unwrap().to_owned();
        if !ignored.contains(&fname.as_ref()) {
            let source = entry.path().canonicalize()?;
            let target = PathBuf::from(target_dir).join(fname);
            std::os::unix::fs::symlink(&source, &target)?;
            println!(
                "symlinking from {} to {}",
                source.display(),
                target.display()
            );
        }
    }
    Ok(())
}

fn replace_kv_engine_symlinks(config: &TikvConfig, agent_dir: &str) -> std::io::Result<()> {
    let mut tmp_config = TikvConfig::default();
    tmp_config.storage.data_dir = agent_dir.to_owned();
    let kvdb = tmp_config.infer_kv_engine_path(None).unwrap();

    let selector = rocksdb_files_should_copy;
    if config.rocksdb.wal_dir != "" {
        replace_symlink_with_copy(&config.rocksdb.wal_dir, &kvdb, selector)
    } else {
        let source_kvdb = config.infer_kv_engine_path(None).unwrap();
        replace_symlink_with_copy(&source_kvdb, &kvdb, selector)
    }
}

fn replace_raft_engine_symlinks(config: &TikvConfig, agent_dir: &str) -> std::io::Result<()> {
    if config.raft_engine.enable {
        let selector = raft_engine_files_should_copy;

        let mut tmp_config = TikvConfig::default();
        tmp_config.storage.data_dir = agent_dir.to_owned();
        let raftdb = tmp_config.infer_raft_engine_path(None).unwrap();

        let source_raftdb = config.infer_raft_engine_path(None).unwrap();
        replace_symlink_with_copy(&source_raftdb, &raftdb, selector)
    } else {
        let mut tmp_config = TikvConfig::default();
        tmp_config.storage.data_dir = agent_dir.to_owned();
        let raftdb = tmp_config.infer_raft_db_path(None).unwrap();

        let selector = rocksdb_files_should_copy;
        if config.raftdb.wal_dir != "" {
            replace_symlink_with_copy(&config.raftdb.wal_dir, &raftdb, selector)
        } else {
            let source_raftdb = config.infer_raft_db_path(None).unwrap();
            replace_symlink_with_copy(&source_raftdb, &raftdb, selector)
        }
    }
}

fn replace_symlink_with_copy<F>(
    source_dir: &str,
    target_dir: &str,
    selector: F,
) -> std::io::Result<()>
where
    F: Fn(&mut dyn Iterator<Item = String>) -> Vec<String>,
{
    let mut names = Vec::new();
    for entry in std::fs::read_dir(target_dir)? {
        let entry = entry?;
        let fname = entry.file_name().to_str().unwrap().to_owned();
        names.push(fname);
    }

    let kvdb_preifx = PathBuf::from(&target_dir);
    let src_prefix = PathBuf::from(source_dir);
    for name in (selector)(&mut names.into_iter()) {
        let src = src_prefix.join(&name);
        let dst = kvdb_preifx.join(&name);
        std::fs::remove_file(&dst)?;
        std::fs::copy(&src, &dst)?;
        let mut pmt = std::fs::metadata(&dst)?.permissions();
        pmt.set_readonly(false);
        std::fs::set_permissions(&dst, pmt)?;
        println!(
            "copy instead of symlink from {} to {}",
            src.display(),
            dst.display()
        );
    }

    Ok(())
}

fn rocksdb_files_should_copy(iter: &mut dyn Iterator<Item = String>) -> Vec<String> {
    let mut names = Vec::new();
    for name in iter {
        if name.ends_with(".log") {
            names.push(name);
        }
    }
    names.sort();
    if let Some(last) = names.pop() {
        names = vec![last];
    }
    names
}

fn raft_engine_files_should_copy(iter: &mut dyn Iterator<Item = String>) -> Vec<String> {
    let (mut raftlogs, mut rewrites) = (Vec::new(), Vec::new());
    for name in iter {
        if name.ends_with(".raftlog") {
            raftlogs.push(name);
        } else if name.ends_with(".rewrite") {
            rewrites.push(name);
        }
    }
    raftlogs.sort();
    rewrites.sort();

    let mut names = Vec::new();
    if let Some(last) = raftlogs.pop() {
        names.push(last);
    }
    if let Some(last) = rewrites.pop() {
        names.push(last);
    }
    names
}

fn get_cluster_id(config: &TikvConfig) -> Result<u64, String> {
    let enc_mgr =
        data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .map_err(|e| format!("data_key_manager_from_config fail: {}", e))?
            .map(Arc::new);
    let env = config
        .build_shared_rocks_env(enc_mgr, None)
        .map_err(|e| format!("build_shared_rocks_env fail: {}", e))?;
    let cache = config
        .storage
        .block_cache
        .build_shared_cache(config.storage.engine);
    let kv_engine = KvEngineFactoryBuilder::new(env, config, cache)
        .build()
        .create_shared_db(&config.storage.data_dir)
        .map_err(|e| format!("create_shared_db fail: {}", e))?;
    let ident = kv_engine
        .get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
        .unwrap()
        .unwrap();
    Ok(ident.cluster_id)
}
