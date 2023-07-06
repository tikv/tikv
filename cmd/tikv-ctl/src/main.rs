// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(once_cell)]
#![feature(let_chains)]

#[macro_use]
extern crate log;

mod cmd;
mod executor;
mod fork_readonly_tikv;
mod util;

use std::{
    borrow::ToOwned,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read},
    path::Path,
    process, str,
    string::ToString,
    sync::Arc,
    thread,
    time::Duration,
    u64,
};

use collections::HashMap;
use encryption_export::{
    create_backend, data_key_manager_from_config, from_engine_encryption_method, DataKeyManager,
    DecrypterReader, Iv,
};
use engine_rocks::get_env;
use engine_traits::{EncryptionKeyManager, Peekable};
use file_system::calc_crc32;
use futures::{executor::block_on, future::try_join_all};
use gag::BufferRedirect;
use grpcio::{CallOption, ChannelBuilder, Environment};
use kvproto::{
    debugpb::{Db as DbType, *},
    encryptionpb::EncryptionMethod,
    kvrpcpb::SplitRegionRequest,
    raft_serverpb::{SnapshotMeta, StoreIdent},
    tikvpb::TikvClient,
};
use pd_client::{Config as PdConfig, PdClient, RpcClient};
use protobuf::Message;
use raft_engine::RecoveryMode;
use raft_log_engine::ManagedFileSystem;
use raftstore::store::util::build_key_range;
use regex::Regex;
use security::{SecurityConfig, SecurityManager};
use structopt::{clap::ErrorKind, StructOpt};
use tikv::{
    config::TikvConfig,
    server::{debug::BottommostLevelCompaction, KvEngineFactoryBuilder},
    storage::config::EngineType,
};
use tikv_util::{escape, run_and_wait_child_process, sys::thread::StdThreadBuildWrapper, unescape};
use txn_types::Key;

use crate::{cmd::*, executor::*, util::*};

fn main() {
    let opt = Opt::from_args();

    // Initialize logger.
    init_ctl_logger(&opt.log_level);

    // Initialize configuration and security manager.
    let cfg_path = opt.config.as_ref();
    let mut cfg = cfg_path.map_or_else(
        || {
            let mut cfg = TikvConfig::default();
            cfg.log.level = tikv_util::logger::get_level_by_string("warn")
                .unwrap()
                .into();
            cfg
        },
        |path| {
            let s = fs::read_to_string(path).unwrap();
            toml::from_str(&s).unwrap()
        },
    );
    let mgr = new_security_mgr(&opt);

    let cmd = match opt.cmd {
        Some(cmd) => cmd,
        None => {
            // Deal with arguments about key utils.
            if let Some(hex) = opt.hex_to_escaped.as_deref() {
                println!("{}", escape(&from_hex(hex).unwrap()));
            } else if let Some(escaped) = opt.escaped_to_hex.as_deref() {
                println!("{}", hex::encode_upper(unescape(escaped)));
            } else if let Some(encoded) = opt.decode.as_deref() {
                match Key::from_encoded(unescape(encoded)).into_raw() {
                    Ok(k) => println!("{}", escape(&k)),
                    Err(e) => println!("decode meets error: {}", e),
                };
            } else if let Some(decoded) = opt.encode.as_deref() {
                println!("{}", Key::from_raw(&unescape(decoded)));
            } else {
                Opt::clap().print_help().ok();
            }
            return;
        }
    };

    match cmd {
        Cmd::External(args) => {
            // Bypass the ldb and sst dump command to RocksDB.
            match args[0].as_str() {
                "ldb" => run_ldb_command(args, &cfg),
                "sst_dump" => run_sst_dump_command(args, &cfg),
                _ => Opt::clap().print_help().unwrap(),
            }
        }
        Cmd::RaftEngineCtl { args } => {
            let key_manager =
                data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
                    .expect("data_key_manager_from_config should success");
            let file_system = Arc::new(ManagedFileSystem::new(
                key_manager.map(|m| Arc::new(m)),
                None,
            ));
            raft_engine_ctl::run_command(args, file_system);
        }
        Cmd::BadSsts { manifest, pd } => {
            let data_dir = opt.data_dir.as_deref();
            assert!(data_dir.is_some(), "--data-dir must be specified");
            let data_dir = data_dir.expect("--data-dir must be specified");
            let pd_client = get_pd_rpc_client(Some(pd), Arc::clone(&mgr));
            print_bad_ssts(data_dir, manifest.as_deref(), pd_client, &cfg);
        }
        Cmd::DumpSnapMeta { file } => {
            let path = file.as_ref();
            dump_snap_meta_file(path);
        }
        Cmd::DecryptFile { file, out_file } => {
            let message =
                "This action will expose sensitive data as plaintext on persistent storage";
            if !warning_prompt(message) {
                return;
            }
            let infile = &file;
            let outfile = &out_file;
            println!("infile: {}, outfile: {}", infile, outfile);

            let key_manager =
                match data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
                    .expect("data_key_manager_from_config should success")
                {
                    Some(mgr) => mgr,
                    None => {
                        println!("Encryption is disabled");
                        println!("crc32: {}", calc_crc32(infile).unwrap());
                        return;
                    }
                };

            let infile1 = Path::new(infile).canonicalize().unwrap();
            let file_info = key_manager.get_file(infile1.to_str().unwrap()).unwrap();

            let mthd = from_engine_encryption_method(file_info.method);
            if mthd == EncryptionMethod::Plaintext {
                println!(
                    "{} is not encrypted, skip to decrypt it into {}",
                    infile, outfile
                );
                println!("crc32: {}", calc_crc32(infile).unwrap());
                return;
            }

            let mut outf = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(outfile)
                .unwrap();

            let iv = Iv::from_slice(&file_info.iv).unwrap();
            let f = File::open(infile).unwrap();
            let mut reader = DecrypterReader::new(f, mthd, &file_info.key, iv).unwrap();

            io::copy(&mut reader, &mut outf).unwrap();
            println!("crc32: {}", calc_crc32(outfile).unwrap());
        }
        Cmd::EncryptionMeta { cmd: subcmd } => match subcmd {
            EncryptionMetaCmd::DumpKey { ids } => {
                let message = "This action will expose encryption key(s) as plaintext. Do not output the \
                    result in file on disk.";
                if !warning_prompt(message) {
                    return;
                }
                DataKeyManager::dump_key_dict(
                    create_backend(&cfg.security.encryption.master_key)
                        .expect("encryption-meta master key creation"),
                    &cfg.storage.data_dir,
                    ids,
                )
                .unwrap();
            }
            EncryptionMetaCmd::DumpFile { path } => {
                let path =
                    path.map(|path| fs::canonicalize(path).unwrap().to_str().unwrap().to_owned());
                DataKeyManager::dump_file_dict(&cfg.storage.data_dir, path.as_deref()).unwrap();
            }
        },
        Cmd::CleanupEncryptionMeta {} => {
            let key_manager =
                match data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
                    .expect("data_key_manager_from_config should success")
                {
                    Some(mgr) => mgr,
                    None => {
                        println!("Encryption is disabled");
                        return;
                    }
                };
            key_manager.retain_encrypted_files(|fname| Path::new(fname).exists())
        }
        Cmd::CompactCluster {
            db,
            cf,
            from,
            to,
            threads,
            bottommost,
        } => {
            let pd_client = get_pd_rpc_client(opt.pd, Arc::clone(&mgr));
            let db_type = if db == "kv" { DbType::Kv } else { DbType::Raft };
            let cfs = cf.iter().map(|s| s.as_ref()).collect();
            let from_key = from.map(|k| unescape(&k));
            let to_key = to.map(|k| unescape(&k));
            let bottommost = BottommostLevelCompaction::from(Some(bottommost.as_ref()));
            compact_whole_cluster(
                &pd_client, &cfg, mgr, db_type, cfs, from_key, to_key, threads, bottommost,
            );
        }
        Cmd::SplitRegion {
            region: region_id,
            key,
        } => {
            let pd_client = get_pd_rpc_client(opt.pd, Arc::clone(&mgr));
            let key = unescape(&key);
            split_region(&pd_client, mgr, region_id, key);
        }
        Cmd::ShowClusterId { data_dir } => {
            if opt.config.is_none() {
                clap::Error {
                    message: String::from("(--config) must be specified"),
                    kind: ErrorKind::MissingRequiredArgument,
                    info: None,
                }
                .exit();
            }
            if data_dir.is_empty() {
                clap::Error {
                    message: String::from("(--data-dir) must be specified"),
                    kind: ErrorKind::MissingRequiredArgument,
                    info: None,
                }
                .exit();
            }
            cfg.storage.data_dir = data_dir;
            // Disable auto compactions and GCs to avoid modifications.
            cfg.rocksdb.defaultcf.disable_auto_compactions = true;
            cfg.rocksdb.writecf.disable_auto_compactions = true;
            cfg.rocksdb.lockcf.disable_auto_compactions = true;
            cfg.rocksdb.raftcf.disable_auto_compactions = true;
            cfg.raftdb.defaultcf.disable_auto_compactions = true;
            cfg.rocksdb.titan.disable_gc = true;
            match read_cluster_id(&cfg) {
                Ok(id) => {
                    println!("cluster-id: {}", id);
                    process::exit(0);
                }
                Err(e) => {
                    eprintln!("read cluster ID fail: {}", e);
                    process::exit(-1);
                }
            }
        }
        Cmd::ReuseReadonlyRemains {
            data_dir,
            agent_dir,
            snaps,
            rocksdb_files,
        } => {
            if opt.config.is_none() {
                clap::Error {
                    message: String::from("(--config) must be specified"),
                    kind: ErrorKind::MissingRequiredArgument,
                    info: None,
                }
                .exit();
            }
            if data_dir.is_empty() {
                clap::Error {
                    message: String::from("(--data-dir) must be specified"),
                    kind: ErrorKind::MissingRequiredArgument,
                    info: None,
                }
                .exit();
            }
            cfg.storage.data_dir = data_dir;
            if cfg.storage.engine == EngineType::RaftKv2 {
                clap::Error {
                    message: String::from("storage.engine can only be raftkv"),
                    kind: ErrorKind::InvalidValue,
                    info: None,
                }
                .exit();
            }
            if cfg.raft_engine.config().enable_log_recycle {
                clap::Error {
                    message: String::from("raft-engine.enable-log-recycle can only be false"),
                    kind: ErrorKind::InvalidValue,
                    info: None,
                }
                .exit();
            }
            if cfg.raft_engine.config().recovery_mode != RecoveryMode::TolerateTailCorruption {
                clap::Error {
                    message: String::from(
                        "raft-engine.recovery-mode can only be tolerate-tail-corruption",
                    ),
                    kind: ErrorKind::InvalidValue,
                    info: None,
                }
                .exit();
            }
            if snaps != fork_readonly_tikv::SYMLINK && snaps != fork_readonly_tikv::COPY {
                clap::Error {
                    message: String::from("(--snaps) can only be symlink or copy"),
                    kind: ErrorKind::InvalidValue,
                    info: None,
                }
                .exit();
            }
            if rocksdb_files != fork_readonly_tikv::SYMLINK
                && rocksdb_files != fork_readonly_tikv::COPY
            {
                clap::Error {
                    message: String::from("(--rocksdb_files) can only be symlink or copy"),
                    kind: ErrorKind::InvalidValue,
                    info: None,
                }
                .exit();
            }
            fork_readonly_tikv::run(&cfg, &agent_dir, &snaps, &rocksdb_files)
        }
        Cmd::Flashback {
            version,
            regions,
            start,
            end,
        } => {
            let start_key = from_hex(&start).unwrap();
            let end_key = from_hex(&end).unwrap();
            let pd_client = get_pd_rpc_client(opt.pd, Arc::clone(&mgr));
            flashback_whole_cluster(
                &pd_client,
                &cfg,
                Arc::clone(&mgr),
                regions.unwrap_or_default(),
                version,
                start_key,
                end_key,
            );
        }
        // Commands below requires either the data dir or the host.
        cmd => {
            let data_dir = opt.data_dir.as_deref();
            let host = opt.host.as_deref();

            if data_dir.is_none() && host.is_none() {
                clap::Error {
                    message: String::from("[host|data-dir] is not specified"),
                    kind: ErrorKind::MissingRequiredArgument,
                    info: None,
                }
                .exit();
            }

            cfg.rocksdb.paranoid_checks = Some(!opt.skip_paranoid_checks);
            let debug_executor = new_debug_executor(&cfg, data_dir, host, Arc::clone(&mgr));

            match cmd {
                Cmd::Print { cf, key } => {
                    let key = unescape(&key);
                    debug_executor.dump_value(&cf, key);
                }
                Cmd::Raft { cmd: subcmd } => match subcmd {
                    RaftCmd::Log {
                        region,
                        index,
                        key,
                        binary,
                    } => {
                        let (id, index) = if let Some(key) = key.as_deref() {
                            keys::decode_raft_log_key(&unescape(key)).unwrap()
                        } else {
                            let id = region.unwrap();
                            let index = index.unwrap();
                            (id, index)
                        };
                        debug_executor.dump_raft_log(id, index, binary);
                    }
                    RaftCmd::Region {
                        regions,
                        skip_tombstone,
                        start,
                        end,
                        limit,
                        ..
                    } => {
                        let start_key = from_hex(&start).unwrap();
                        let end_key = from_hex(&end).unwrap();
                        debug_executor.dump_region_info(
                            regions,
                            &start_key,
                            &end_key,
                            limit,
                            skip_tombstone,
                        );
                    }
                },
                Cmd::Size { region, cf } => {
                    let cfs = cf.iter().map(AsRef::as_ref).collect();
                    if let Some(id) = region {
                        debug_executor.dump_region_size(id, cfs);
                    } else {
                        debug_executor.dump_all_region_size(cfs);
                    }
                }
                Cmd::Scan {
                    from,
                    to,
                    limit,
                    show_cf,
                    start_ts,
                    commit_ts,
                } => {
                    let from = unescape(&from);
                    let to = to.map_or_else(Vec::new, |to| unescape(&to));
                    let limit = limit.unwrap_or(0);
                    if to.is_empty() && limit == 0 {
                        println!(r#"please pass "to" or "limit""#);
                        tikv_util::logger::exit_process_gracefully(-1);
                    }
                    let cfs = show_cf.iter().map(AsRef::as_ref).collect();
                    debug_executor.dump_mvccs_infos(from, to, limit, cfs, start_ts, commit_ts);
                }
                Cmd::RawScan {
                    from,
                    to,
                    limit,
                    cf,
                } => {
                    let from = unescape(&from);
                    let to = unescape(&to);
                    debug_executor.raw_scan(&from, &to, limit, &cf);
                }
                Cmd::Mvcc {
                    key,
                    show_cf,
                    start_ts,
                    commit_ts,
                } => {
                    let from = unescape(&key);
                    let cfs = show_cf.iter().map(AsRef::as_ref).collect();
                    debug_executor.dump_mvccs_infos(from, vec![], 0, cfs, start_ts, commit_ts);
                }
                Cmd::Diff {
                    region,
                    to_data_dir,
                    to_host,
                    to_config,
                    ..
                } => {
                    let to_data_dir = to_data_dir.as_deref();
                    let to_host = to_host.as_deref();
                    let to_config = to_config.map_or_else(TikvConfig::default, |path| {
                        let s = fs::read_to_string(path).unwrap();
                        toml::from_str(&s).unwrap()
                    });
                    debug_executor.diff_region(region, to_host, to_data_dir, &to_config, mgr);
                }
                Cmd::Compact {
                    region,
                    db,
                    cf,
                    from,
                    to,
                    threads,
                    bottommost,
                } => {
                    let db_type = if db == "kv" { DbType::Kv } else { DbType::Raft };
                    let from_key = from.map(|k| unescape(&k));
                    let to_key = to.map(|k| unescape(&k));
                    let bottommost = BottommostLevelCompaction::from(Some(bottommost.as_ref()));
                    if let Some(region) = region {
                        debug_executor
                            .compact_region(host, db_type, &cf, region, threads, bottommost);
                    } else {
                        debug_executor
                            .compact(host, db_type, &cf, from_key, to_key, threads, bottommost);
                    }
                }
                Cmd::Tombstone { regions, pd, force } => {
                    if let Some(pd_urls) = pd {
                        let cfg = PdConfig {
                            endpoints: pd_urls,
                            ..Default::default()
                        };
                        if let Err(e) = cfg.validate() {
                            panic!("invalid pd configuration: {:?}", e);
                        }
                        debug_executor.set_region_tombstone_after_remove_peer(mgr, &cfg, regions);
                    } else {
                        assert!(force);
                        debug_executor.set_region_tombstone_force(regions);
                    }
                }
                Cmd::RecoverMvcc {
                    read_only,
                    all,
                    threads,
                    regions,
                    pd: pd_urls,
                } => {
                    if all {
                        let threads = threads.unwrap();
                        if threads == 0 {
                            panic!("Number of threads can't be 0");
                        }
                        println!(
                            "Recover all, threads: {}, read_only: {}",
                            threads, read_only
                        );
                        debug_executor.recover_mvcc_all(threads, read_only);
                    } else {
                        let mut cfg = PdConfig::default();
                        println!(
                            "Recover regions: {:?}, pd: {:?}, read_only: {}",
                            regions, pd_urls, read_only
                        );
                        cfg.endpoints = pd_urls;
                        if let Err(e) = cfg.validate() {
                            panic!("invalid pd configuration: {:?}", e);
                        }
                        debug_executor.recover_regions_mvcc(mgr, &cfg, regions, read_only);
                    }
                }
                Cmd::UnsafeRecover { cmd: subcmd } => match subcmd {
                    UnsafeRecoverCmd::RemoveFailStores {
                        stores,
                        regions,
                        promote_learner,
                        ..
                    } => {
                        debug_executor.remove_fail_stores(stores, regions, promote_learner);
                    }
                    UnsafeRecoverCmd::DropUnappliedRaftlog { regions, .. } => {
                        debug_executor.drop_unapplied_raftlog(regions);
                    }
                },
                Cmd::RecreateRegion {
                    pd,
                    region: region_id,
                } => {
                    let pd_cfg = PdConfig {
                        endpoints: pd,
                        ..Default::default()
                    };
                    debug_executor.recreate_region(mgr, &pd_cfg, region_id);
                }
                Cmd::ConsistencyCheck { region } => {
                    debug_executor.check_region_consistency(region);
                }
                Cmd::BadRegions {} => {
                    debug_executor.print_bad_regions();
                }
                Cmd::ModifyTikvConfig {
                    config_name,
                    config_value,
                } => {
                    debug_executor.modify_tikv_config(&config_name, &config_value);
                }
                Cmd::Metrics { tag } => {
                    let tags = tag.iter().map(AsRef::as_ref).collect();
                    debug_executor.dump_metrics(tags)
                }
                Cmd::RegionProperties { region } => debug_executor.dump_region_properties(region),
                Cmd::RangeProperties { start, end } => {
                    let start_key = from_hex(&start).unwrap();
                    let end_key = from_hex(&end).unwrap();
                    debug_executor.dump_range_properties(start_key, end_key);
                }
                Cmd::Fail { cmd: subcmd } => {
                    if host.is_none() {
                        println!("command fail requires host");
                        tikv_util::logger::exit_process_gracefully(-1);
                    }
                    let client = new_debug_client(host.unwrap(), mgr);
                    match subcmd {
                        FailCmd::Inject { args, file } => {
                            let mut list = file.as_deref().map_or_else(Vec::new, read_fail_file);
                            for pair in args {
                                let mut parts = pair.split('=');
                                list.push((
                                    parts.next().unwrap().to_owned(),
                                    parts.next().unwrap_or("").to_owned(),
                                ))
                            }
                            for (name, actions) in list {
                                if actions.is_empty() {
                                    println!("No action for fail point {}", name);
                                    continue;
                                }
                                let mut inject_req = InjectFailPointRequest::default();
                                inject_req.set_name(name);
                                inject_req.set_actions(actions);

                                let option = CallOption::default().timeout(Duration::from_secs(10));
                                client.inject_fail_point_opt(&inject_req, option).unwrap();
                            }
                        }
                        FailCmd::Recover { args, file } => {
                            let mut list = file.as_deref().map_or_else(Vec::new, read_fail_file);
                            for fp in args {
                                list.push((fp.to_owned(), "".to_owned()))
                            }
                            for (name, _) in list {
                                let mut recover_req = RecoverFailPointRequest::default();
                                recover_req.set_name(name);
                                let option = CallOption::default().timeout(Duration::from_secs(10));
                                client.recover_fail_point_opt(&recover_req, option).unwrap();
                            }
                        }
                        FailCmd::List {} => {
                            let list_req = ListFailPointsRequest::default();
                            let option = CallOption::default().timeout(Duration::from_secs(10));
                            let resp = client.list_fail_points_opt(&list_req, option).unwrap();
                            println!("{:?}", resp.get_entries());
                        }
                    }
                }
                Cmd::Store {} => {
                    debug_executor.dump_store_info();
                }
                Cmd::Cluster {} => {
                    debug_executor.dump_cluster_info();
                }
                Cmd::ResetToVersion { version } => debug_executor.reset_to_version(version),
                _ => {
                    unreachable!()
                }
            }
        }
    }
}

fn new_security_mgr(opt: &Opt) -> Arc<SecurityManager> {
    let ca_path = opt.ca_path.as_ref();
    let cert_path = opt.cert_path.as_ref();
    let key_path = opt.key_path.as_ref();

    let mut cfg = SecurityConfig::default();
    if ca_path.is_some() || cert_path.is_some() || key_path.is_some() {
        cfg.ca_path = ca_path
            .expect("CA path should be set when cert path or key path is set.")
            .to_owned();
        cfg.cert_path = cert_path
            .expect("cert path should be set when CA path or key path is set.")
            .to_owned();
        cfg.key_path = key_path
            .expect("key path should be set when cert path or CA path is set.")
            .to_owned();
    }

    Arc::new(SecurityManager::new(&cfg).expect("failed to initialize security manager"))
}

fn dump_snap_meta_file(path: &str) {
    let content =
        fs::read(path).unwrap_or_else(|e| panic!("read meta file {} failed, error {:?}", path, e));

    let mut meta = SnapshotMeta::default();
    meta.merge_from_bytes(&content)
        .unwrap_or_else(|e| panic!("parse from bytes error {:?}", e));
    for cf_file in meta.get_cf_files() {
        println!(
            "cf {}, size {}, checksum: {}",
            cf_file.cf, cf_file.size, cf_file.checksum
        );
    }
}

fn get_pd_rpc_client(pd: Option<String>, mgr: Arc<SecurityManager>) -> RpcClient {
    let pd = pd.unwrap_or_else(|| {
        clap::Error {
            message: String::from("--pd is required for this command"),
            kind: ErrorKind::MissingRequiredArgument,
            info: None,
        }
        .exit();
    });
    let cfg = PdConfig::new(vec![pd]);
    cfg.validate().unwrap();
    RpcClient::new(&cfg, None, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e))
}

fn split_region(pd_client: &RpcClient, mgr: Arc<SecurityManager>, region_id: u64, key: Vec<u8>) {
    let region = block_on(pd_client.get_region_by_id(region_id))
        .expect("get_region_by_id should success")
        .expect("must have the region");

    let leader = pd_client
        .get_region_info(region.get_start_key())
        .expect("get_region_info should success")
        .leader
        .expect("region must have leader");

    let store = pd_client
        .get_store(leader.get_store_id())
        .expect("get_store should success");

    let tikv_client = {
        let cb = ChannelBuilder::new(Arc::new(Environment::new(1)));
        let channel = mgr.connect(cb, store.get_address());
        TikvClient::new(channel)
    };

    let mut req = SplitRegionRequest::default();
    req.mut_context().set_region_id(region_id);
    req.mut_context()
        .set_region_epoch(region.get_region_epoch().clone());
    req.set_split_key(key);

    let resp = tikv_client
        .split_region(&req)
        .expect("split_region should success");
    if resp.has_region_error() {
        println!("split_region internal error: {:?}", resp.get_region_error());
        return;
    }

    println!(
        "split region {} success, left: {}, right: {}",
        region_id,
        resp.get_left().get_id(),
        resp.get_right().get_id(),
    );
}

fn compact_whole_cluster(
    pd_client: &RpcClient,
    cfg: &TikvConfig,
    mgr: Arc<SecurityManager>,
    db_type: DbType,
    cfs: Vec<&str>,
    from: Option<Vec<u8>>,
    to: Option<Vec<u8>>,
    threads: u32,
    bottommost: BottommostLevelCompaction,
) {
    let stores = pd_client
        .get_all_stores(true) // Exclude tombstone stores.
        .unwrap_or_else(|e| perror_and_exit("Get all cluster stores from PD failed", e));

    let mut handles = Vec::new();
    for s in stores {
        let cfg = cfg.clone();
        let mgr = Arc::clone(&mgr);
        let addr = s.address.clone();
        let (from, to) = (from.clone(), to.clone());
        let cfs: Vec<String> = cfs.iter().map(|cf| cf.to_string()).collect();
        let h = thread::Builder::new()
            .name(format!("compact-{}", addr))
            .spawn_wrapper(move || {
                let debug_executor = new_debug_executor(&cfg, None, Some(&addr), mgr);
                for cf in cfs {
                    debug_executor.compact(
                        Some(&addr),
                        db_type,
                        cf.as_str(),
                        from.clone(),
                        to.clone(),
                        threads,
                        bottommost,
                    );
                }
            })
            .unwrap();
        handles.push(h);
    }

    handles.into_iter().for_each(|h| h.join().unwrap());
}

const FLASHBACK_TIMEOUT: u64 = 1800; // 1800s
const WAIT_APPLY_FLASHBACK_STATE: u64 = 100; // 100ms

fn flashback_whole_cluster(
    pd_client: &RpcClient,
    cfg: &TikvConfig,
    mgr: Arc<SecurityManager>,
    region_ids: Vec<u64>,
    version: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) {
    let pd_client = pd_client.clone();
    let cfg = cfg.clone();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("flashback")
        .enable_time()
        .build()
        .unwrap();

    block_on(runtime.spawn(async move {
        // Prepare all regions for flashback.
        let start_ts = pd_client.get_tso().await.unwrap();
        let mut stores_leader = load_leaders_to_each_store(&pd_client, start_key, end_key);
        // Need to retry if all regions are not finish prepare.
        loop {
            let mut futures = Vec::default();
            stores_leader
                .clone()
                .into_iter()
                .for_each(|(store_id, leaders)| {
                    let addr = pd_client.get_store(store_id).unwrap().address;
                    leaders
                        .into_iter()
                        .filter(|(_, region_id)| {
                            region_ids.is_empty() || region_ids.contains(region_id)
                        })
                        .for_each(|(key_range, region_id)| {
                            // Prepare flashback region version by key range.
                            let key_range = build_key_range(&key_range.0, &key_range.1, false);

                            let addr = addr.clone();
                            let cfg_inner = cfg.clone();
                            let mgr = Arc::clone(&mgr);
                            let f = async move {
                                let debug_executor =
                                    new_debug_executor(&cfg_inner, None, Some(&addr), mgr);
                                debug_executor.flashback_to_version(
                                    version,
                                    region_id,
                                    key_range,
                                    start_ts.into_inner(),
                                    0,
                                )
                            };
                            futures.push(f);
                        })
                });

            // Wait for finishing prepare flashback.
            match tokio::time::timeout(
                Duration::from_secs(FLASHBACK_TIMEOUT),
                try_join_all(futures),
            )
            .await
            {
                Ok(res) => {
                    if let Err(key_range) = res {
                        // Retry specific key range to prepare flashback.
                        let retry_stores_leader = load_leaders_to_each_store(
                            &pd_client,
                            key_range.get_start_key().to_vec(),
                            key_range.get_end_key().to_vec(),
                        );
                        // Need to update `stores_leader` to replace stale key range.
                        for (store_id, leaders) in retry_stores_leader {
                            let regions = stores_leader
                                .entry(store_id)
                                .or_insert_with(HashMap::default);
                            regions.extend(leaders);
                        }
                        thread::sleep(Duration::from_micros(WAIT_APPLY_FLASHBACK_STATE));
                        continue;
                    }
                    break;
                }
                Err(e) => {
                    println!(
                        "prepare flashback with start_ts {:?} timeout. err: {:?}",
                        start_ts, e
                    );
                    return;
                }
            }
        }

        // Start flashback for all regions.
        let commit_ts = pd_client.get_tso().await.unwrap();
        loop {
            let mut futures = Vec::default();
            stores_leader
                .clone()
                .into_iter()
                .for_each(|(store_id, leaders)| {
                    let addr = pd_client.get_store(store_id).unwrap().address;
                    leaders
                        .into_iter()
                        .filter(|(_, region_id)| {
                            region_ids.is_empty() || region_ids.contains(region_id)
                        })
                        .for_each(|(key_range, region_id)| {
                            // Flashback region version by key range.
                            let key_range = build_key_range(&key_range.0, &key_range.1, false);

                            let addr = addr.clone();
                            let cfg_inner = cfg.clone();
                            let mgr = Arc::clone(&mgr);
                            let f = async move {
                                let debug_executor =
                                    new_debug_executor(&cfg_inner, None, Some(&addr), mgr);
                                debug_executor.flashback_to_version(
                                    version,
                                    region_id,
                                    key_range,
                                    start_ts.into_inner(),
                                    commit_ts.into_inner(),
                                )
                            };
                            futures.push(f);
                        })
                });

            // Wait for finishing flashback to version.
            match tokio::time::timeout(
                Duration::from_secs(FLASHBACK_TIMEOUT),
                try_join_all(futures),
            )
            .await
            {
                Ok(res) => match res {
                    Ok(_) => break,
                    Err(_) => {
                        thread::sleep(Duration::from_micros(WAIT_APPLY_FLASHBACK_STATE));
                        continue;
                    }
                },
                Err(e) => {
                    println!(
                        "finish flashback with start_ts {:?}, commit_ts: {:?} timeout. err: {:?}",
                        e, start_ts, commit_ts
                    );
                    return;
                }
            }
        }
    }))
    .unwrap();

    println!("flashback all stores success!");
}

fn load_leaders_to_each_store(
    pd_client: &RpcClient,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
) -> HashMap<u64, HashMap<(Vec<u8>, Vec<u8>), u64>> {
    // Get all regions in the cluster.
    let res = pd_client.batch_load_regions(start_key, end_key);
    // Put all regions in right stores.
    let mut store_regions = HashMap::default();
    res.into_iter().for_each(|batch| {
        batch.into_iter().for_each(|r| {
            let store_id = r.get_leader().get_store_id();
            let regions = store_regions
                .entry(store_id)
                .or_insert_with(HashMap::default);
            let mut cur_region = r.get_region().clone();
            regions.insert(
                (cur_region.take_start_key(), cur_region.take_end_key()),
                cur_region.get_id(),
            );
        });
    });
    store_regions
}

fn read_fail_file(path: &str) -> Vec<(String, String)> {
    let f = File::open(path).unwrap();
    let f = BufReader::new(f);

    let mut list = vec![];
    for line in f.lines() {
        let line = line.unwrap();
        let mut parts = line.split('=');
        list.push((
            parts.next().unwrap().to_owned(),
            parts.next().unwrap_or("").to_owned(),
        ))
    }
    list
}

fn build_rocks_opts(cfg: &TikvConfig) -> engine_rocks::RocksDbOptions {
    let key_manager = data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
        .unwrap()
        .map(Arc::new);
    let env = get_env(key_manager, None /* io_rate_limiter */).unwrap();
    let resource = cfg.rocksdb.build_resources(env);
    cfg.rocksdb.build_opt(&resource, cfg.storage.engine)
}

fn run_ldb_command(args: Vec<String>, cfg: &TikvConfig) {
    engine_rocks::raw::run_ldb_tool(&args, &build_rocks_opts(cfg));
}

fn run_sst_dump_command(args: Vec<String>, cfg: &TikvConfig) {
    engine_rocks::raw::run_sst_dump_tool(&args, &build_rocks_opts(cfg));
}

fn print_bad_ssts(data_dir: &str, manifest: Option<&str>, pd_client: RpcClient, cfg: &TikvConfig) {
    let db = &cfg.infer_kv_engine_path(Some(data_dir)).unwrap();
    println!(
        "\nstart to print bad ssts; data_dir:{}; db:{}",
        data_dir, db
    );

    let mut args = vec![
        "sst_dump".to_string(),
        "--output_hex".to_string(),
        "--command=verify".to_string(),
    ];
    args.push(format!("--file={}", db));

    let stderr = BufferRedirect::stderr().unwrap();
    let stdout = BufferRedirect::stdout().unwrap();
    let opts = build_rocks_opts(cfg);

    match run_and_wait_child_process(|| engine_rocks::raw::run_sst_dump_tool(&args, &opts)) {
        Ok(code) => {
            if code != 0 {
                flush_std_buffer_to_log(
                    &format!("failed to run {}", args.join(" ")),
                    stderr,
                    stdout,
                );
                tikv_util::logger::exit_process_gracefully(code);
            }
        }
        Err(e) => {
            flush_std_buffer_to_log(
                &format!("failed to run {} and get error:{}", args.join(" "), e),
                stderr,
                stdout,
            );
            panic!();
        }
    }

    drop(stdout);
    let mut stderr_buf = stderr.into_inner();
    let mut buffer = Vec::new();
    stderr_buf.read_to_end(&mut buffer).unwrap();
    let corruptions = unsafe { String::from_utf8_unchecked(buffer) };

    for line in corruptions.lines() {
        println!("--------------------------------------------------------");
        // The corruption format may like this:
        // ```text
        // /path/to/db/057155.sst is corrupted: Corruption: block checksum mismatch: expected 3754995957, got 708533950  in /path/to/db/057155.sst offset 3126049 size 22724
        // ```
        println!("corruption info:\n{}", line);

        let r = Regex::new(r"/\w*\.sst").unwrap();
        let sst_file_number = match r.captures(line) {
            None => {
                println!("skip bad line format");
                continue;
            }
            Some(parts) => {
                if let Some(part) = parts.get(0) {
                    Path::new(&part.as_str()[1..])
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                } else {
                    println!("skip bad line format");
                    continue;
                }
            }
        };
        let mut args1 = vec![
            "ldb".to_string(),
            "--hex".to_string(),
            "manifest_dump".to_string(),
        ];
        args1.push(format!("--db={}", db));
        args1.push(format!("--sst_file_number={}", sst_file_number));
        if let Some(manifest_path) = manifest {
            args1.push(format!("--manifest={}", manifest_path));
        }

        let stdout = BufferRedirect::stdout().unwrap();
        let stderr = BufferRedirect::stderr().unwrap();
        match run_and_wait_child_process(|| engine_rocks::raw::run_ldb_tool(&args1, &opts)).unwrap()
        {
            0 => {}
            status => {
                let mut err = String::new();
                let mut stderr_buf = stderr.into_inner();
                drop(stdout);
                stderr_buf.read_to_string(&mut err).unwrap();
                println!(
                    "ldb process return status code {}, failed to run {}:\n{}",
                    status,
                    args1.join(" "),
                    err
                );
                continue;
            }
        };

        let mut stdout_buf = stdout.into_inner();
        drop(stderr);
        let mut output = String::new();
        stdout_buf.read_to_string(&mut output).unwrap();

        println!("\nsst meta:");
        // The output may like this:
        // ```text
        // --------------- Column family "write"  (ID 2) --------------
        // 63:132906243[3555338 .. 3555338]['7A311B40EFCC2CB4C5911ECF3937D728DED26AE53FA5E61BE04F23F2BE54EACC73' seq:3555338, type:1 .. '7A313030302E25CD5F57252E' seq:3555338, type:1] at level 0
        // ```
        let column_r = Regex::new(r"--------------- (.*) --------------\n(.*)").unwrap();
        if let Some(m) = column_r.captures(&output) {
            println!(
                "{} for {}",
                m.get(2).unwrap().as_str(),
                m.get(1).unwrap().as_str()
            );
            let r = Regex::new(r".*\n\d+:\d+\[\d+ .. \d+\]\['(\w*)' seq:\d+, type:\d+ .. '(\w*)' seq:\d+, type:\d+\] at level \d+").unwrap();
            let matches = match r.captures(&output) {
                None => {
                    println!("sst start key format is not correct: {}", output);
                    continue;
                }
                Some(v) => v,
            };
            let start = from_hex(matches.get(1).unwrap().as_str()).unwrap();
            let end = from_hex(matches.get(2).unwrap().as_str()).unwrap();

            println!("start key:{:?}; end key:{:?}", &start, &end);

            if start.starts_with(&[keys::DATA_PREFIX]) {
                print_overlap_region_and_suggestions(
                    &pd_client,
                    &start[1..],
                    &end[1..],
                    db,
                    data_dir,
                    sst_file_number,
                );
            } else if start.starts_with(&[keys::LOCAL_PREFIX]) {
                println!(
                    "it isn't easy to handle local data, start key:{}",
                    log_wrappers::Value(&start)
                );

                // consider the case that include both meta and user data
                if end.starts_with(&[keys::DATA_PREFIX]) {
                    println!("WARNING: the range includes both meta and user data.");
                    print_overlap_region_and_suggestions(
                        &pd_client,
                        &[],
                        &end[1..],
                        db,
                        data_dir,
                        sst_file_number,
                    );
                }
            } else {
                println!("unexpected key {}", log_wrappers::Value(&start));
            }
        } else {
            // it is expected when the sst is output of a compaction and the sst isn't added
            // to manifest yet.
            println!(
                "sst {} is not found in manifest: {}",
                sst_file_number, output
            );
        }
    }
    println!("--------------------------------------------------------");
    println!("corruption analysis has completed");
}

fn print_overlap_region_and_suggestions(
    pd_client: &RpcClient,
    start: &[u8],
    end: &[u8],
    db: &str,
    data_dir: &str,
    sst_file_number: &str,
) {
    let mut key = start.to_vec();
    let mut regions_to_print = vec![];
    println!("\noverlap region:");
    loop {
        let region = match pd_client.get_region_info(&key) {
            Err(e) => {
                println!(
                    "can not get the region of key {}: {}",
                    log_wrappers::Value(start),
                    e
                );
                return;
            }
            Ok(r) => r,
        };
        regions_to_print.push(region.clone());
        println!("{:?}", region);
        if region.get_end_key() > end || region.get_end_key().is_empty() {
            break;
        }
        key = region.get_end_key().to_vec();
    }

    println!("\nrefer operations:");
    println!(
        "tikv-ctl ldb --db={} unsafe_remove_sst_file {}",
        db, sst_file_number
    );
    for region in regions_to_print {
        println!(
            "tikv-ctl --data-dir={} tombstone -r {} -p <endpoint>",
            data_dir, region.id
        );
    }
}

fn flush_std_buffer_to_log(
    msg: &str,
    mut err_buffer: BufferRedirect,
    mut out_buffer: BufferRedirect,
) {
    let mut err = String::new();
    let mut out = String::new();
    err_buffer.read_to_string(&mut err).unwrap();
    out_buffer.read_to_string(&mut out).unwrap();
    println!("{}, err redirect:{}, out redirect:{}", msg, err, out);
}

fn read_cluster_id(config: &TikvConfig) -> Result<u64, String> {
    let key_manager =
        data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .unwrap()
            .map(Arc::new);
    let env = get_env(key_manager.clone(), None /* io_rate_limiter */).unwrap();
    let cache = config
        .storage
        .block_cache
        .build_shared_cache(config.storage.engine);
    let kv_engine = KvEngineFactoryBuilder::new(env, config, cache, key_manager)
        .build()
        .create_shared_db(&config.storage.data_dir)
        .map_err(|e| format!("create_shared_db fail: {}", e))?;
    let ident = kv_engine
        .get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
        .unwrap()
        .unwrap();
    Ok(ident.cluster_id)
}
