// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.
#![feature(once_cell)]

#[macro_use]
extern crate log;

use clap::{crate_authors, AppSettings};
use encryption_export::{
    create_backend, data_key_manager_from_config, encryption_method_from_db_encryption_method,
    DataKeyManager, DecrypterReader, Iv,
};
use engine_rocks::get_env;
use engine_rocks::raw_util::new_engine_opt;
use engine_rocks::RocksEngine;
use engine_traits::{
    EncryptionKeyManager, Engines, Error as EngineError, RaftEngine, ALL_CFS, CF_DEFAULT, CF_LOCK,
    CF_WRITE,
};
use file_system::calc_crc32;
use futures::{executor::block_on, future, stream, Stream, StreamExt, TryStreamExt};
use gag::BufferRedirect;
use grpcio::{CallOption, ChannelBuilder, Environment};
use kvproto::debugpb::{Db as DBType, *};
use kvproto::encryptionpb::EncryptionMethod;
use kvproto::kvrpcpb::{MvccInfo, SplitRegionRequest};
use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::{PeerState, SnapshotMeta};
use kvproto::tikvpb::TikvClient;
use pd_client::{Config as PdConfig, PdClient, RpcClient};
use protobuf::Message;
use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use raft_log_engine::RaftLogEngine;
use raftstore::store::INIT_EPOCH_CONF_VER;
use regex::Regex;
use security::{SecurityConfig, SecurityManager};
use serde_json::json;
use server::setup::initial_logger;
use std::borrow::ToOwned;
use std::cmp::Ordering;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read};
use std::lazy::SyncLazy;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::string::ToString;
use std::sync::Arc;
use std::time::Duration;
use std::{process, str, thread, u64};
use structopt::StructOpt;
use tikv::config::{ConfigController, TiKvConfig, DEFAULT_ROCKSDB_SUB_DIR};
use tikv::server::debug::{BottommostLevelCompaction, Debugger, RegionInfo};
use tikv_util::config::canonicalize_sub_path;
use tikv_util::{escape, run_and_wait_child_process, unescape};
use txn_types::Key;

const METRICS_PROMETHEUS: &str = "prometheus";
const METRICS_ROCKSDB_KV: &str = "rocksdb_kv";
const METRICS_ROCKSDB_RAFT: &str = "rocksdb_raft";
const METRICS_JEMALLOC: &str = "jemalloc";

const LOCK_FILE_ERROR: &str = "IO error: While lock file";

type MvccInfoStream = Pin<Box<dyn Stream<Item = Result<(Vec<u8>, MvccInfo), String>>>>;

fn perror_and_exit<E: Error>(prefix: &str, e: E) -> ! {
    println!("{}: {}", prefix, e);
    process::exit(-1);
}

fn init_ctl_logger(level: &str) {
    let mut cfg = TiKvConfig::default();
    cfg.log_level = slog::Level::from_str(level).unwrap();
    cfg.rocksdb.info_log_dir = "./ctl-engine-info-log".to_owned();
    cfg.raftdb.info_log_dir = "./ctl-engine-info-log".to_owned();
    initial_logger(&cfg);
}

fn handle_engine_error(err: EngineError) -> ! {
    error!("error while open kvdb: {}", err);
    if let EngineError::Engine(msg) = err {
        if msg.starts_with(LOCK_FILE_ERROR) {
            error!(
                "LOCK file conflict indicates TiKV process is running. \
                Do NOT delete the LOCK file and force the command to run. \
                Doing so could cause data corruption."
            );
        }
    }
    process::exit(-1);
}

fn new_debug_executor(
    cfg: &TiKvConfig,
    data_dir: Option<&str>,
    skip_paranoid_checks: bool,
    host: Option<&str>,
    mgr: Arc<SecurityManager>,
) -> Box<dyn DebugExecutor> {
    if let Some(remote) = host {
        return Box::new(new_debug_client(remote, mgr)) as Box<dyn DebugExecutor>;
    }

    let data_dir = data_dir.unwrap();
    let kv_path = canonicalize_sub_path(data_dir, DEFAULT_ROCKSDB_SUB_DIR).unwrap();

    let key_manager = data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
        .unwrap()
        .map(Arc::new);

    let cache = cfg.storage.block_cache.build_shared_cache();
    let shared_block_cache = cache.is_some();
    let env = get_env(key_manager, None /*io_rate_limiter*/).unwrap();

    let mut kv_db_opts = cfg.rocksdb.build_opt();
    kv_db_opts.set_env(env.clone());
    kv_db_opts.set_paranoid_checks(!skip_paranoid_checks);
    let kv_cfs_opts = cfg
        .rocksdb
        .build_cf_opts(&cache, None, cfg.storage.enable_ttl);
    let kv_path = PathBuf::from(kv_path).canonicalize().unwrap();
    let kv_path = kv_path.to_str().unwrap();
    let kv_db = match new_engine_opt(kv_path, kv_db_opts, kv_cfs_opts) {
        Ok(db) => db,
        Err(e) => handle_engine_error(e),
    };
    let mut kv_db = RocksEngine::from_db(Arc::new(kv_db));
    kv_db.set_shared_block_cache(shared_block_cache);

    let cfg_controller = ConfigController::default();
    if !cfg.raft_engine.enable {
        let mut raft_db_opts = cfg.raftdb.build_opt();
        raft_db_opts.set_env(env);
        let raft_db_cf_opts = cfg.raftdb.build_cf_opts(&cache);
        let raft_path = canonicalize_sub_path(data_dir, &cfg.raft_store.raftdb_path).unwrap();
        let raft_db = match new_engine_opt(&raft_path, raft_db_opts, raft_db_cf_opts) {
            Ok(db) => db,
            Err(e) => handle_engine_error(e),
        };
        let mut raft_db = RocksEngine::from_db(Arc::new(raft_db));
        raft_db.set_shared_block_cache(shared_block_cache);
        let debugger = Debugger::new(Engines::new(kv_db, raft_db), cfg_controller);
        Box::new(debugger) as Box<dyn DebugExecutor>
    } else {
        let mut config = cfg.raft_engine.config();
        config.dir = canonicalize_sub_path(data_dir, &config.dir).unwrap();
        let raft_db = RaftLogEngine::new(config).unwrap();
        let debugger = Debugger::new(Engines::new(kv_db, raft_db), cfg_controller);
        Box::new(debugger) as Box<dyn DebugExecutor>
    }
}

fn new_debug_client(host: &str, mgr: Arc<SecurityManager>) -> DebugClient {
    let env = Arc::new(Environment::new(1));
    let cb = ChannelBuilder::new(env)
        .max_receive_message_len(1 << 30) // 1G.
        .max_send_message_len(1 << 30)
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3));

    let channel = mgr.connect(cb, host);
    DebugClient::new(channel)
}

trait DebugExecutor {
    fn dump_value(&self, cf: &str, key: Vec<u8>) {
        let value = self.get_value_by_key(cf, key);
        println!("value: {}", escape(&value));
    }

    fn dump_region_size(&self, region: u64, cfs: Vec<&str>) -> usize {
        let sizes = self.get_region_size(region, cfs);
        let mut total_size = 0;
        println!("region id: {}", region);
        for (cf, size) in sizes {
            println!("cf {} region size: {}", cf, convert_gbmb(size as u64));
            total_size += size;
        }
        total_size
    }

    fn dump_all_region_size(&self, cfs: Vec<&str>) {
        let regions = self.get_all_regions_in_store();
        let regions_number = regions.len();
        let mut total_size = 0;
        for region in regions {
            total_size += self.dump_region_size(region, cfs.clone());
        }
        println!("total region number: {}", regions_number);
        println!("total region size: {}", convert_gbmb(total_size as u64));
    }

    fn dump_region_info(&self, region_ids: Option<Vec<u64>>, skip_tombstone: bool) {
        let region_ids = region_ids.unwrap_or_else(|| self.get_all_regions_in_store());
        let mut region_objects = serde_json::map::Map::new();
        for region_id in region_ids {
            let r = self.get_region_info(region_id);
            if skip_tombstone {
                let region_state = r.region_local_state.as_ref();
                if region_state.map_or(false, |s| s.get_state() == PeerState::Tombstone) {
                    return;
                }
            }
            let region_object = json!({
                "region_id": region_id,
                "region_local_state": r.region_local_state.map(|s| {
                    let r = s.get_region();
                    let region_epoch = r.get_region_epoch();
                    let peers = r.get_peers();
                    json!({
                        "region": json!({
                            "id": r.get_id(),
                            "start_key": hex::encode_upper(r.get_start_key()),
                            "end_key": hex::encode_upper(r.get_end_key()),
                            "region_epoch": json!({
                                "conf_ver": region_epoch.get_conf_ver(),
                                "version": region_epoch.get_version()
                            }),
                            "peers": peers.iter().map(|p| json!({
                                "id": p.get_id(),
                                "store_id": p.get_store_id(),
                                "role": format!("{:?}", p.get_role()),
                            })).collect::<Vec<_>>(),
                        }),
                    })
                }),
                "raft_local_state": r.raft_local_state.map(|s| {
                    let hard_state = s.get_hard_state();
                    json!({
                        "hard_state": json!({
                            "term": hard_state.get_term(),
                            "vote": hard_state.get_vote(),
                            "commit": hard_state.get_commit(),
                        }),
                        "last_index": s.get_last_index(),
                    })
                }),
                "raft_apply_state": r.raft_apply_state.map(|s| {
                    let truncated_state = s.get_truncated_state();
                    json!({
                        "applied_index": s.get_applied_index(),
                        "commit_index": s.get_commit_index(),
                        "commit_term": s.get_commit_term(),
                        "truncated_state": json!({
                            "index": truncated_state.get_index(),
                            "term": truncated_state.get_term(),
                        })
                    })
                })
            });
            region_objects.insert(region_id.to_string(), region_object);
        }

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "region_infos": region_objects })).unwrap()
        );
    }

    fn dump_raft_log(&self, region: u64, index: u64) {
        let idx_key = keys::raft_log_key(region, index);
        println!("idx_key: {}", escape(&idx_key));
        println!("region: {}", region);
        println!("log index: {}", index);

        let mut entry = self.get_raft_log(region, index);
        let data = entry.take_data();
        println!("entry {:?}", entry);
        println!("msg len: {}", data.len());

        if data.is_empty() {
            return;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                let mut msg = RaftCmdRequest::default();
                msg.merge_from_bytes(&data).unwrap();
                println!("Normal: {:#?}", msg);
            }
            EntryType::EntryConfChange => {
                let mut msg = ConfChange::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                println!("ConfChange: {:?}", msg);
                let mut cmd = RaftCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                println!("ConfChange.RaftCmdRequest: {:#?}", cmd);
            }
            EntryType::EntryConfChangeV2 => {
                let mut msg = ConfChangeV2::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                println!("ConfChangeV2: {:?}", msg);
                let mut cmd = RaftCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                println!("ConfChangeV2.RaftCmdRequest: {:#?}", cmd);
            }
        }
    }

    /// Dump mvcc infos for a given key range. The given `from` and `to` must
    /// be raw key with `z` prefix. Both `to` and `limit` are empty value means
    /// what we want is point query instead of range scan.
    fn dump_mvccs_infos(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        mut limit: u64,
        cfs: Vec<&str>,
        start_ts: Option<u64>,
        commit_ts: Option<u64>,
    ) {
        if !from.starts_with(b"z") || (!to.is_empty() && !to.starts_with(b"z")) {
            println!("from and to should start with \"z\"");
            process::exit(-1);
        }
        if !to.is_empty() && to < from {
            println!("\"to\" must be greater than \"from\"");
            process::exit(-1);
        }

        let point_query = to.is_empty() && limit == 0;
        if point_query {
            limit = 1;
        }

        let scan_future =
            self.get_mvcc_infos(from.clone(), to, limit)
                .try_for_each(move |(key, mvcc)| {
                    if point_query && key != from {
                        println!("no mvcc infos for {}", escape(&from));
                        return future::err::<(), String>("no mvcc infos".to_owned());
                    }

                    println!("key: {}", escape(&key));
                    if cfs.contains(&CF_LOCK) && mvcc.has_lock() {
                        let lock_info = mvcc.get_lock();
                        if start_ts.map_or(true, |ts| lock_info.get_start_ts() == ts) {
                            println!("\tlock cf value: {:?}", lock_info);
                        }
                    }
                    if cfs.contains(&CF_DEFAULT) {
                        for value_info in mvcc.get_values() {
                            if commit_ts.map_or(true, |ts| value_info.get_start_ts() == ts) {
                                println!("\tdefault cf value: {:?}", value_info);
                            }
                        }
                    }
                    if cfs.contains(&CF_WRITE) {
                        for write_info in mvcc.get_writes() {
                            if start_ts.map_or(true, |ts| write_info.get_start_ts() == ts)
                                && commit_ts.map_or(true, |ts| write_info.get_commit_ts() == ts)
                            {
                                println!("\t write cf value: {:?}", write_info);
                            }
                        }
                    }
                    println!();
                    future::ok::<(), String>(())
                });
        if let Err(e) = block_on(scan_future) {
            println!("{}", e);
            process::exit(-1);
        }
    }

    fn raw_scan(&self, from_key: &[u8], to_key: &[u8], limit: usize, cf: &str) {
        if !ALL_CFS.contains(&cf) {
            eprintln!("CF \"{}\" doesn't exist.", cf);
            process::exit(-1);
        }
        if !to_key.is_empty() && from_key >= to_key {
            eprintln!(
                "to_key should be greater than from_key, but got from_key: \"{}\", to_key: \"{}\"",
                escape(from_key),
                escape(to_key)
            );
            process::exit(-1);
        }
        if limit == 0 {
            eprintln!("limit should be greater than 0");
            process::exit(-1);
        }

        self.raw_scan_impl(from_key, to_key, limit, cf);
    }

    fn diff_region(
        &self,
        region: u64,
        to_host: Option<&str>,
        to_data_dir: Option<&str>,
        to_config: &TiKvConfig,
        mgr: Arc<SecurityManager>,
    ) {
        let rhs_debug_executor = new_debug_executor(to_config, to_data_dir, false, to_host, mgr);

        let r1 = self.get_region_info(region);
        let r2 = rhs_debug_executor.get_region_info(region);
        println!("region id: {}", region);
        println!("db1 region state: {:?}", r1.region_local_state);
        println!("db2 region state: {:?}", r2.region_local_state);
        println!("db1 apply state: {:?}", r1.raft_apply_state);
        println!("db2 apply state: {:?}", r2.raft_apply_state);

        match (r1.region_local_state, r2.region_local_state) {
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                println!("db1 and db2 don't have same region local_state");
            }
            (Some(region_local_1), Some(region_local_2)) => {
                let region1 = region_local_1.get_region();
                let region2 = region_local_2.get_region();
                if region1 != region2 {
                    println!("db1 and db2 have different region:");
                    println!("db1 region: {:?}", region1);
                    println!("db2 region: {:?}", region2);
                    return;
                }
                let start_key = keys::data_key(region1.get_start_key());
                let end_key = keys::data_key(region1.get_end_key());
                let mut mvcc_infos_1 = self.get_mvcc_infos(start_key.clone(), end_key.clone(), 0);
                let mut mvcc_infos_2 = rhs_debug_executor.get_mvcc_infos(start_key, end_key, 0);

                let mut has_diff = false;
                let mut key_counts = [0; 2];

                let mut take_item = |i: usize| -> Option<(Vec<u8>, MvccInfo)> {
                    let wait = match i {
                        1 => block_on(future::poll_fn(|cx| mvcc_infos_1.poll_next_unpin(cx))),
                        _ => block_on(future::poll_fn(|cx| mvcc_infos_2.poll_next_unpin(cx))),
                    };
                    match wait? {
                        Err(e) => {
                            println!("db{} scan data in region {} fail: {}", i, region, e);
                            process::exit(-1);
                        }
                        Ok(s) => Some(s),
                    }
                };

                let show_only = |i: usize, k: &[u8]| {
                    println!("only db{} has: {}", i, escape(k));
                };

                let (mut item1, mut item2) = (take_item(1), take_item(2));
                while item1.is_some() && item2.is_some() {
                    key_counts[0] += 1;
                    key_counts[1] += 1;
                    let t1 = item1.take().unwrap();
                    let t2 = item2.take().unwrap();
                    match t1.0.cmp(&t2.0) {
                        Ordering::Less => {
                            show_only(1, &t1.0);
                            has_diff = true;
                            item1 = take_item(1);
                            item2 = Some(t2);
                            key_counts[1] -= 1;
                        }
                        Ordering::Greater => {
                            show_only(2, &t2.0);
                            has_diff = true;
                            item1 = Some(t1);
                            item2 = take_item(2);
                            key_counts[0] -= 1;
                        }
                        _ => {
                            if t1.1 != t2.1 {
                                println!("diff mvcc on key: {}", escape(&t1.0));
                                has_diff = true;
                            }
                            item1 = take_item(1);
                            item2 = take_item(2);
                        }
                    }
                }
                let mut item = item1.map(|t| (1, t)).or_else(|| item2.map(|t| (2, t)));
                while let Some((i, (key, _))) = item.take() {
                    key_counts[i - 1] += 1;
                    show_only(i, &key);
                    has_diff = true;
                    item = take_item(i).map(|t| (i, t));
                }
                if !has_diff {
                    println!("db1 and db2 have same data in region: {}", region);
                }
                println!(
                    "db1 has {} keys, db2 has {} keys",
                    key_counts[0], key_counts[1]
                );
            }
        }
    }

    fn compact(
        &self,
        address: Option<&str>,
        db: DBType,
        cf: &str,
        from: Option<Vec<u8>>,
        to: Option<Vec<u8>>,
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let from = from.unwrap_or_default();
        let to = to.unwrap_or_default();
        self.do_compaction(db, cf, &from, &to, threads, bottommost);
        println!(
            "store:{:?} compact db:{:?} cf:{} range:[{:?}, {:?}) success!",
            address.unwrap_or("local"),
            db,
            cf,
            from,
            to
        );
    }

    fn compact_region(
        &self,
        address: Option<&str>,
        db: DBType,
        cf: &str,
        region_id: u64,
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let region_local = self.get_region_info(region_id).region_local_state.unwrap();
        let r = region_local.get_region();
        let from = keys::data_key(r.get_start_key());
        let to = keys::data_end_key(r.get_end_key());
        self.do_compaction(db, cf, &from, &to, threads, bottommost);
        println!(
            "store:{:?} compact_region db:{:?} cf:{} range:[{:?}, {:?}) success!",
            address.unwrap_or("local"),
            db,
            cf,
            from,
            to
        );
    }

    fn print_bad_regions(&self);

    fn set_region_tombstone_after_remove_peer(
        &self,
        mgr: Arc<SecurityManager>,
        cfg: &PdConfig,
        region_ids: Vec<u64>,
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(cfg, None, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let regions = region_ids
            .into_iter()
            .map(|region_id| {
                if let Some(region) = block_on(rpc_client.get_region_by_id(region_id))
                    .unwrap_or_else(|e| perror_and_exit("Get region id from PD", e))
                {
                    return region;
                }
                println!("no such region in pd: {}", region_id);
                process::exit(-1);
            })
            .collect();
        self.set_region_tombstone(regions);
    }

    fn set_region_tombstone_force(&self, region_ids: Vec<u64>) {
        self.check_local_mode();
        self.set_region_tombstone_by_id(region_ids);
    }

    /// Recover the cluster when given `store_ids` are failed.
    fn remove_fail_stores(
        &self,
        store_ids: Vec<u64>,
        region_ids: Option<Vec<u64>>,
        promote_learner: bool,
    );

    fn drop_unapplied_raftlog(&self, region_ids: Option<Vec<u64>>);

    /// Recreate the region with metadata from pd, but alloc new id for it.
    fn recreate_region(&self, sec_mgr: Arc<SecurityManager>, pd_cfg: &PdConfig, region_id: u64);

    fn check_region_consistency(&self, _: u64);

    fn check_local_mode(&self);

    fn recover_regions_mvcc(
        &self,
        mgr: Arc<SecurityManager>,
        cfg: &PdConfig,
        region_ids: Vec<u64>,
        read_only: bool,
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(cfg, None, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let regions = region_ids
            .into_iter()
            .map(|region_id| {
                if let Some(region) = block_on(rpc_client.get_region_by_id(region_id))
                    .unwrap_or_else(|e| perror_and_exit("Get region id from PD", e))
                {
                    return region;
                }
                println!("no such region in pd: {}", region_id);
                process::exit(-1);
            })
            .collect();
        self.recover_regions(regions, read_only);
    }

    fn recover_mvcc_all(&self, threads: usize, read_only: bool) {
        self.check_local_mode();
        self.recover_all(threads, read_only);
    }

    fn get_all_regions_in_store(&self) -> Vec<u64>;

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8>;

    fn get_region_size(&self, region: u64, cfs: Vec<&str>) -> Vec<(String, usize)>;

    fn get_region_info(&self, region: u64) -> RegionInfo;

    fn get_raft_log(&self, region: u64, index: u64) -> Entry;

    fn get_mvcc_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream;

    fn raw_scan_impl(&self, from_key: &[u8], end_key: &[u8], limit: usize, cf: &str);

    fn do_compaction(
        &self,
        db: DBType,
        cf: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    );

    fn set_region_tombstone(&self, regions: Vec<Region>);

    fn set_region_tombstone_by_id(&self, regions: Vec<u64>);

    fn recover_regions(&self, regions: Vec<Region>, read_only: bool);

    fn recover_all(&self, threads: usize, read_only: bool);

    fn modify_tikv_config(&self, config_name: &str, config_value: &str);

    fn dump_metrics(&self, tags: Vec<&str>);

    fn dump_region_properties(&self, region_id: u64);

    fn dump_range_properties(&self, start: Vec<u8>, end: Vec<u8>);

    fn dump_store_info(&self);

    fn dump_cluster_info(&self);
}

impl DebugExecutor for DebugClient {
    fn check_local_mode(&self) {
        println!("This command is only for local mode");
        process::exit(-1);
    }

    fn get_all_regions_in_store(&self) -> Vec<u64> {
        DebugClient::get_all_regions_in_store(self, &GetAllRegionsInStoreRequest::default())
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_all_regions_in_store", e))
            .take_regions()
    }

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8> {
        let mut req = GetRequest::default();
        req.set_db(DBType::Kv);
        req.set_cf(cf.to_owned());
        req.set_key(key);
        self.get(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get", e))
            .take_value()
    }

    fn get_region_size(&self, region: u64, cfs: Vec<&str>) -> Vec<(String, usize)> {
        let cfs = cfs.into_iter().map(ToOwned::to_owned).collect::<Vec<_>>();
        let mut req = RegionSizeRequest::default();
        req.set_cfs(cfs.into());
        req.set_region_id(region);
        self.region_size(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::region_size", e))
            .take_entries()
            .into_iter()
            .map(|mut entry| (entry.take_cf(), entry.get_size() as usize))
            .collect()
    }

    fn get_region_info(&self, region: u64) -> RegionInfo {
        let mut req = RegionInfoRequest::default();
        req.set_region_id(region);
        let mut resp = self
            .region_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::region_info", e));

        let mut region_info = RegionInfo::default();
        if resp.has_raft_local_state() {
            region_info.raft_local_state = Some(resp.take_raft_local_state());
        }
        if resp.has_raft_apply_state() {
            region_info.raft_apply_state = Some(resp.take_raft_apply_state());
        }
        if resp.has_region_local_state() {
            region_info.region_local_state = Some(resp.take_region_local_state());
        }
        region_info
    }

    fn get_raft_log(&self, region: u64, index: u64) -> Entry {
        let mut req = RaftLogRequest::default();
        req.set_region_id(region);
        req.set_log_index(index);
        self.raft_log(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::raft_log", e))
            .take_entry()
    }

    fn get_mvcc_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream {
        let mut req = ScanMvccRequest::default();
        req.set_from_key(from);
        req.set_to_key(to);
        req.set_limit(limit);
        Box::pin(
            self.scan_mvcc(&req)
                .unwrap()
                .map_err(|e| e.to_string())
                .map_ok(|mut resp| (resp.take_key(), resp.take_info())),
        )
    }

    fn raw_scan_impl(&self, _: &[u8], _: &[u8], _: usize, _: &str) {
        unimplemented!();
    }

    fn do_compaction(
        &self,
        db: DBType,
        cf: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        let mut req = CompactRequest::default();
        req.set_db(db);
        req.set_cf(cf.to_owned());
        req.set_from_key(from.to_owned());
        req.set_to_key(to.to_owned());
        req.set_threads(threads);
        req.set_bottommost_level_compaction(bottommost.into());
        self.compact(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::compact", e));
    }

    fn dump_metrics(&self, tags: Vec<&str>) {
        let mut req = GetMetricsRequest::default();
        req.set_all(true);
        if tags.len() == 1 && tags[0] == METRICS_PROMETHEUS {
            req.set_all(false);
        }
        let mut resp = self
            .get_metrics(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::metrics", e));
        for tag in tags {
            println!("tag:{}", tag);
            let metrics = match tag {
                METRICS_ROCKSDB_KV => resp.take_rocksdb_kv(),
                METRICS_ROCKSDB_RAFT => resp.take_rocksdb_raft(),
                METRICS_JEMALLOC => resp.take_jemalloc(),
                METRICS_PROMETHEUS => resp.take_prometheus(),
                _ => String::from(
                    "unsupported tag, should be one of prometheus/jemalloc/rocksdb_raft/rocksdb_kv",
                ),
            };
            println!("{}", metrics);
        }
    }

    fn set_region_tombstone(&self, _: Vec<Region>) {
        unimplemented!("only available for local mode");
    }

    fn set_region_tombstone_by_id(&self, _: Vec<u64>) {
        unimplemented!("only available for local mode");
    }

    fn recover_regions(&self, _: Vec<Region>, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn recover_all(&self, _: usize, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn print_bad_regions(&self) {
        unimplemented!("only available for local mode");
    }

    fn remove_fail_stores(&self, _: Vec<u64>, _: Option<Vec<u64>>, _: bool) {
        unimplemented!("only available for local mode");
    }

    fn drop_unapplied_raftlog(&self, _: Option<Vec<u64>>) {
        unimplemented!("only available for local mode");
    }

    fn recreate_region(&self, _: Arc<SecurityManager>, _: &PdConfig, _: u64) {
        unimplemented!("only available for local mode");
    }

    fn check_region_consistency(&self, region_id: u64) {
        let mut req = RegionConsistencyCheckRequest::default();
        req.set_region_id(region_id);
        self.check_region_consistency(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::check_region_consistency", e));
        println!("success!");
    }

    fn modify_tikv_config(&self, config_name: &str, config_value: &str) {
        let mut req = ModifyTikvConfigRequest::default();
        req.set_config_name(config_name.to_owned());
        req.set_config_value(config_value.to_owned());
        self.modify_tikv_config(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::modify_tikv_config", e));
        println!("success");
    }

    fn dump_region_properties(&self, region_id: u64) {
        let mut req = GetRegionPropertiesRequest::default();
        req.set_region_id(region_id);
        let resp = self
            .get_region_properties(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_region_properties", e));
        for prop in resp.get_props() {
            println!("{}: {}", prop.get_name(), prop.get_value());
        }
    }

    fn dump_range_properties(&self, _: Vec<u8>, _: Vec<u8>) {
        unimplemented!("only available for local mode");
    }

    fn dump_store_info(&self) {
        let req = GetStoreInfoRequest::default();
        let resp = self
            .get_store_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_store_info", e));
        println!("{}", resp.get_store_id())
    }

    fn dump_cluster_info(&self) {
        let req = GetClusterInfoRequest::default();
        let resp = self
            .get_cluster_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_cluster_info", e));
        println!("{}", resp.get_cluster_id())
    }
}

impl<ER: RaftEngine> DebugExecutor for Debugger<ER> {
    fn check_local_mode(&self) {}

    fn get_all_regions_in_store(&self) -> Vec<u64> {
        self.get_all_regions_in_store()
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_all_regions_in_store", e))
    }

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8> {
        self.get(DBType::Kv, cf, &key)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get", e))
    }

    fn get_region_size(&self, region: u64, cfs: Vec<&str>) -> Vec<(String, usize)> {
        self.region_size(region, cfs)
            .unwrap_or_else(|e| perror_and_exit("Debugger::region_size", e))
            .into_iter()
            .map(|(cf, size)| (cf.to_owned(), size as usize))
            .collect()
    }

    fn get_region_info(&self, region: u64) -> RegionInfo {
        self.region_info(region)
            .unwrap_or_else(|e| perror_and_exit("Debugger::region_info", e))
    }

    fn get_raft_log(&self, region: u64, index: u64) -> Entry {
        self.raft_log(region, index)
            .unwrap_or_else(|e| perror_and_exit("Debugger::raft_log", e))
    }

    fn get_mvcc_infos(&self, from: Vec<u8>, to: Vec<u8>, limit: u64) -> MvccInfoStream {
        let iter = self
            .scan_mvcc(&from, &to, limit)
            .unwrap_or_else(|e| perror_and_exit("Debugger::scan_mvcc", e));
        let stream = stream::iter(iter).map_err(|e| e.to_string());
        Box::pin(stream)
    }

    fn raw_scan_impl(&self, from_key: &[u8], end_key: &[u8], limit: usize, cf: &str) {
        let res = self
            .raw_scan(from_key, end_key, limit, cf)
            .unwrap_or_else(|e| perror_and_exit("Debugger::raw_scan_impl", e));

        for (k, v) in &res {
            println!("key: \"{}\", value: \"{}\"", escape(k), escape(v));
        }
        println!();
        println!("Total scanned keys: {}", res.len());
    }

    fn do_compaction(
        &self,
        db: DBType,
        cf: &str,
        from: &[u8],
        to: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) {
        self.compact(db, cf, from, to, threads, bottommost)
            .unwrap_or_else(|e| perror_and_exit("Debugger::compact", e));
    }

    fn set_region_tombstone(&self, regions: Vec<Region>) {
        let ret = self
            .set_region_tombstone(regions)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_region_tombstone", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (region_id, error) in ret {
            println!("region: {}, error: {}", region_id, error);
        }
    }

    fn set_region_tombstone_by_id(&self, region_ids: Vec<u64>) {
        let ret = self
            .set_region_tombstone_by_id(region_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_region_tombstone_by_id", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (region_id, error) in ret {
            println!("region: {}, error: {}", region_id, error);
        }
    }

    fn recover_regions(&self, regions: Vec<Region>, read_only: bool) {
        let ret = self
            .recover_regions(regions, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover regions", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (region_id, error) in ret {
            println!("region: {}, error: {}", region_id, error);
        }
    }

    fn recover_all(&self, threads: usize, read_only: bool) {
        Debugger::recover_all(self, threads, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover all", e));
    }

    fn print_bad_regions(&self) {
        let bad_regions = self
            .bad_regions()
            .unwrap_or_else(|e| perror_and_exit("Debugger::bad_regions", e));
        if !bad_regions.is_empty() {
            for (region_id, error) in bad_regions {
                println!("{}: {}", region_id, error);
            }
            return;
        }
        println!("all regions are healthy")
    }

    fn remove_fail_stores(
        &self,
        store_ids: Vec<u64>,
        region_ids: Option<Vec<u64>>,
        promote_learner: bool,
    ) {
        println!("removing stores {:?} from configurations...", store_ids);
        self.remove_failed_stores(store_ids, region_ids, promote_learner)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        println!("success");
    }

    fn drop_unapplied_raftlog(&self, region_ids: Option<Vec<u64>>) {
        println!("removing unapplied raftlog on region {:?} ...", region_ids);
        self.drop_unapplied_raftlog(region_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        println!("success");
    }

    fn recreate_region(&self, mgr: Arc<SecurityManager>, pd_cfg: &PdConfig, region_id: u64) {
        let rpc_client = RpcClient::new(pd_cfg, None, mgr)
            .unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let mut region = match block_on(rpc_client.get_region_by_id(region_id)) {
            Ok(Some(region)) => region,
            Ok(None) => {
                println!("no such region {} on PD", region_id);
                process::exit(-1)
            }
            Err(e) => perror_and_exit("RpcClient::get_region_by_id", e),
        };

        let new_region_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));
        let new_peer_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));

        let store_id = self.get_store_id().expect("get store id");

        region.set_id(new_region_id);
        let old_version = region.get_region_epoch().get_version();
        region.mut_region_epoch().set_version(old_version + 1);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        region.peers.clear();
        let mut peer = Peer::default();
        peer.set_id(new_peer_id);
        peer.set_store_id(store_id);
        region.mut_peers().push(peer);

        println!(
            "initing empty region {} with peer_id {}...",
            new_region_id, new_peer_id
        );
        self.recreate_region(region)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recreate_region", e));
        println!("success");
    }

    fn dump_metrics(&self, _tags: Vec<&str>) {
        unimplemented!("only available for online mode");
    }

    fn check_region_consistency(&self, _: u64) {
        println!("only support remote mode");
        process::exit(-1);
    }

    fn modify_tikv_config(&self, _: &str, _: &str) {
        println!("only support remote mode");
        process::exit(-1);
    }

    fn dump_region_properties(&self, region_id: u64) {
        let props = self
            .get_region_properties(region_id)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_region_properties", e));
        for (name, value) in props {
            println!("{}: {}", name, value);
        }
    }

    fn dump_range_properties(&self, start: Vec<u8>, end: Vec<u8>) {
        let props = self
            .get_range_properties(&start, &end)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_range_properties", e));
        for (name, value) in props {
            println!("{}: {}", name, value);
        }
    }

    fn dump_store_info(&self) {
        let store_id = self.get_store_id();
        if let Ok(id) = store_id {
            println!("store id: {}", id);
        }
    }

    fn dump_cluster_info(&self) {
        let cluster_id = self.get_cluster_id();
        if let Ok(id) = cluster_id {
            println!("cluster id: {}", id);
        }
    }
}

fn warning_prompt(message: &str) -> bool {
    const EXPECTED: &str = "I consent";
    println!("{}", message);
    let input: String = promptly::prompt(format!(
        "Type \"{}\" to continue, anything else to exit",
        EXPECTED
    ))
    .unwrap();
    if input == EXPECTED {
        true
    } else {
        println!("exit.");
        false
    }
}

static VERSION_INFO: SyncLazy<String> = SyncLazy::new(|| {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::tikv_version_info(build_timestamp)
});

const RAW_KEY_HINT: &str = "Raw key (generally starts with \"z\") in escaped form";

#[derive(StructOpt)]
#[structopt(
    name = "TiKV Control (tikv-ctl)",
    about = "A tool for interacting with TiKV deployments.",
    author = crate_authors!(),
    version = &**VERSION_INFO,
    long_version = &**VERSION_INFO,
    setting = AppSettings::DontCollapseArgsInUsage,
)]
struct Opt {
    #[structopt(long)]
    /// Set the address of pd
    pd: Option<String>,

    #[structopt(long, default_value = "info")]
    /// Set the log level
    log_level: String,

    #[structopt(long)]
    /// Set the remote host
    host: Option<String>,

    #[structopt(long)]
    /// Set the CA certificate path
    ca_path: Option<String>,

    #[structopt(long)]
    /// Set the certificate path
    cert_path: Option<String>,

    #[structopt(long)]
    /// Set the private key path
    key_path: Option<String>,

    #[structopt(long)]
    /// TiKV config path, by default it's <deploy-dir>/conf/tikv.toml
    config: Option<String>,

    #[structopt(long)]
    /// TiKV data-dir, check <deploy-dir>/scripts/run.sh to get it
    data_dir: Option<String>,

    #[structopt(long)]
    /// Skip paranoid checks when open rocksdb
    skip_paranoid_checks: bool,

    #[allow(dead_code)]
    #[structopt(
        long,
        validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the rocksdb path
    db: Option<String>,

    #[allow(dead_code)]
    #[structopt(
        long,
        validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the raft rocksdb path
    raftdb: Option<String>,

    #[structopt(conflicts_with = "escaped-to-hex", long = "to-escaped")]
    /// Convert a hex key to escaped key
    hex_to_escaped: Option<String>,

    #[structopt(conflicts_with = "hex-to-escaped", long = "to-hex")]
    /// Convert an escaped key to hex key
    escaped_to_hex: Option<String>,

    #[structopt(
        conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
        long,
    )]
    /// Decode a key in escaped format
    decode: Option<String>,

    #[structopt(
        conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
        long,
    )]
    /// Encode a key in escaped format
    encode: Option<String>,

    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(StructOpt)]
enum Cmd {
    /// Print a raft log entry
    Raft {
        #[structopt(subcommand)]
        cmd: RaftCmd,
    },
    /// Print region size
    Size {
        #[structopt(short = "r")]
        /// Set the region id, if not specified, print all regions
        region: Option<u64>,

        #[structopt(
            short = "c",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = "default,write,lock"
        )]
        /// Set the cf name, if not specified, print all cf
        cf: Vec<String>,
    },
    /// Print the range db range
    Scan {
        #[structopt(
            short = "f",
            long,
            help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(
            short = "t",
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(long)]
        /// Set the scan limit
        limit: Option<u64>,

        #[structopt(long)]
        /// Set the scan start_ts as filter
        start_ts: Option<u64>,

        #[structopt(long)]
        /// Set the scan commit_ts as filter
        commit_ts: Option<u64>,

        #[structopt(
            long,
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,
    },
    /// Print all raw keys in the range
    RawScan {
        #[structopt(
            short = "f",
            long,
            default_value = "",
            help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(
            short = "t",
            long,
            default_value = "",
            help = RAW_KEY_HINT,
        )]
        to: String,

        #[structopt(long, default_value = "30")]
        /// Limit the number of keys to scan
        limit: usize,

        #[structopt(
            long,
            default_value = "default",
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,
    },
    /// Print the raw value
    Print {
        #[structopt(
            short = "c",
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,

        #[structopt(
            short = "k",
            help = RAW_KEY_HINT,
        )]
        key: String,
    },
    /// Print the mvcc value
    Mvcc {
        #[structopt(
            short = "k",
            help = RAW_KEY_HINT,
        )]
        key: String,

        #[structopt(
            long,
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,

        #[structopt(long)]
        /// Set start_ts as filter
        start_ts: Option<u64>,

        #[structopt(long)]
        /// Set commit_ts as filter
        commit_ts: Option<u64>,
    },
    /// Calculate difference of region keys from different dbs
    Diff {
        #[structopt(short = "r")]
        /// Specify region id
        region: u64,

        #[allow(dead_code)]
        #[structopt(
            conflicts_with = "to_host",
            long,
            validator = |_| Err("DEPRECATED!!! Use --to-data-dir and --to-config instead".to_owned()),
        )]
        /// To which db path
        to_db: Option<String>,

        #[structopt(conflicts_with = "to_host", long)]
        /// data-dir of the target TiKV
        to_data_dir: Option<String>,

        #[structopt(conflicts_with = "to_host", long)]
        /// config of the target TiKV
        to_config: Option<String>,

        #[structopt(
            required_unless = "to_data_dir",
            conflicts_with = "to_db",
            long,
            conflicts_with = "to_db"
        )]
        /// To which remote host
        to_host: Option<String>,
    },
    /// Compact a column family in a specified range
    Compact {
        #[structopt(
            short = "d",
            default_value = "kv",
            possible_values = &["kv", "raft"],
        )]
        /// Which db to compact
        db: String,

        #[structopt(
            short = "c",
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,

        #[structopt(
            short = "f",
            long,
            help = RAW_KEY_HINT,
        )]
        from: Option<String>,

        #[structopt(
            short = "t",
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(short = "n", long, default_value = "8")]
        /// Number of threads in one compaction
        threads: u32,

        #[structopt(short = "r", long)]
        /// Set the region id
        region: Option<u64>,

        #[structopt(
            short = "b",
            long,
            default_value = "default",
            possible_values = &["skip", "force", "default"],
        )]
        /// Set how to compact the bottommost level
        bottommost: String,
    },
    /// Set some regions on the node to tombstone by manual
    Tombstone {
        #[structopt(
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// The target regions, separated with commas if multiple
        regions: Vec<u64>,

        #[structopt(
            short = "p",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// PD endpoints
        pd: Option<Vec<String>>,

        #[structopt(long)]
        /// force execute without pd
        force: bool,
    },
    /// Recover mvcc data on one node by deleting corrupted keys
    RecoverMvcc {
        #[structopt(short = "a", long)]
        /// Recover the whole db
        all: bool,

        #[structopt(
            required_unless = "all",
            conflicts_with = "all",
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// The target regions, separated with commas if multiple
        regions: Vec<u64>,

        #[structopt(
            required_unless = "all",
            short = "p",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// PD endpoints
        pd: Vec<String>,

        #[structopt(long, default_value_if("all", None, "4"), requires = "all")]
        /// The number of threads to do recover, only for --all mode
        threads: Option<usize>,

        #[structopt(short = "R", long)]
        /// Skip write RocksDB
        read_only: bool,
    },
    /// Unsafely recover when the store can not start normally, this recover may lose data
    UnsafeRecover {
        #[structopt(subcommand)]
        cmd: UnsafeRecoverCmd,
    },
    /// Recreate a region with given metadata, but alloc new id for it
    RecreateRegion {
        #[structopt(
            short = "p",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// PD endpoints
        pd: Vec<String>,

        #[structopt(short = "r")]
        /// The origin region id
        region: u64,
    },
    /// Print the metrics
    Metrics {
        #[structopt(
            short = "t",
            long,
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = METRICS_PROMETHEUS,
            possible_values = &["prometheus", "jemalloc", "rocksdb_raft", "rocksdb_kv"],
        )]
        /// Set the metrics tag, one of prometheus/jemalloc/rocksdb_raft/rocksdb_kv, if not specified, print prometheus
        tag: Vec<String>,
    },
    /// Force a consistency-check for a specified region
    ConsistencyCheck {
        #[structopt(short = "r")]
        /// The target region
        region: u64,
    },
    /// Get all regions with corrupt raft
    BadRegions {},
    /// Modify tikv config, eg. tikv-ctl --host ip:port modify-tikv-config -n rocksdb.defaultcf.disable-auto-compactions -v true
    ModifyTikvConfig {
        #[structopt(short = "n")]
        /// The config name are same as the name used on config file, eg. raftstore.messages-per-tick, raftdb.max-background-jobs
        config_name: String,

        #[structopt(short = "v")]
        /// The config value, eg. 8, true, 1h, 8MB
        config_value: String,
    },
    /// Dump snapshot meta file
    DumpSnapMeta {
        #[structopt(short = "f", long)]
        /// Output meta file path
        file: String,
    },
    /// Compact the whole cluster in a specified range in one or more column families
    CompactCluster {
        #[structopt(
            short = "d",
            default_value = "kv",
            possible_values = &["kv", "raft"],
        )]
        /// The db to use
        db: String,

        #[structopt(
            short = "c",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// Column family names, for kv db, combine from default/lock/write; for raft db, can only be default
        cf: Vec<String>,

        #[structopt(
            short = "f",
            long,
            help = RAW_KEY_HINT,
        )]
        from: Option<String>,

        #[structopt(
            short = "t",
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(short = "n", long, default_value = "8")]
        /// Number of threads in one compaction
        threads: u32,

        #[structopt(
            short = "b",
            long,
            default_value = "default",
            possible_values = &["skip", "force", "default"],
        )]
        /// How to compact the bottommost level
        bottommost: String,
    },
    /// Show region properties
    RegionProperties {
        #[structopt(short = "r")]
        /// The target region id
        region: u64,
    },
    /// Show range properties
    RangeProperties {
        #[structopt(long, default_value = "")]
        /// hex start key
        start: String,

        #[structopt(long, default_value = "")]
        /// hex end key
        end: String,
    },
    /// Split the region
    SplitRegion {
        #[structopt(short = "r")]
        /// The target region id
        region: u64,

        #[structopt(short = "k")]
        /// The key to split it, in unencoded escaped format
        key: String,
    },
    /// Inject failures to TiKV and recovery
    Fail {
        #[structopt(subcommand)]
        cmd: FailCmd,
    },
    /// Print the store id
    Store {},
    /// Print the cluster id
    Cluster {},
    /// Decrypt an encrypted file
    DecryptFile {
        #[structopt(long)]
        /// input file path
        file: String,

        #[structopt(long)]
        /// output file path
        out_file: String,
    },
    /// Dump encryption metadata
    EncryptionMeta {
        #[structopt(subcommand)]
        cmd: EncryptionMetaCmd,
    },
    /// Print bad ssts related infos
    BadSsts {
        #[structopt(long)]
        /// db directory.
        db: String,

        #[structopt(long)]
        /// specify manifest, if not set, it will look up manifest file in db path
        manifest: Option<String>,

        #[structopt(long, value_delimiter = ",")]
        /// PD endpoints
        pd: String,
    },
    #[structopt(external_subcommand)]
    External(Vec<String>),
}

#[derive(StructOpt)]
enum RaftCmd {
    /// Print the raft log entry info
    Log {
        #[structopt(required_unless = "key", conflicts_with = "key", short = "r")]
        /// Set the region id
        region: Option<u64>,

        #[structopt(required_unless = "key", conflicts_with = "key", short = "i")]
        /// Set the raft log index
        index: Option<u64>,

        #[structopt(
            required_unless_one = &["region", "index"],
            conflicts_with_all = &["region", "index"],
            short = "k",
            help = RAW_KEY_HINT,
        )]
        key: Option<String>,
    },
    /// print region info
    Region {
        #[structopt(
            short = "r",
            aliases = &["region"],
            required_unless = "all-regions",
            conflicts_with = "all-regions",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Print info for these regions
        regions: Option<Vec<u64>>,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[structopt(long, required_unless = "regions", conflicts_with = "regions")]
        /// Print info for all regions
        all_regions: bool,

        #[structopt(long)]
        /// Skip tombstone regions
        skip_tombstone: bool,
    },
}

#[derive(StructOpt)]
enum FailCmd {
    /// Inject failures
    Inject {
        /// Inject fail point and actions pairs. E.g. tikv-ctl fail inject a=off b=panic
        args: Vec<String>,

        #[structopt(short = "f")]
        /// Read a file of fail points and actions to inject
        file: Option<String>,
    },
    /// Recover failures
    Recover {
        /// Recover fail points. Eg. tikv-ctl fail recover a b
        args: Vec<String>,

        #[structopt(short = "f")]
        /// Recover from a file of fail points
        file: Option<String>,
    },
    /// List all fail points
    List {},
}

#[derive(StructOpt)]
enum EncryptionMetaCmd {
    /// Dump data keys
    DumpKey {
        #[structopt(long, use_delimiter = true)]
        /// List of data key ids. Dump all keys if not provided.
        ids: Option<Vec<u64>>,
    },
    /// Dump file encryption info
    DumpFile {
        #[structopt(long)]
        /// Path to the file. Dump for all files if not provided.
        path: Option<String>,
    },
}

#[derive(StructOpt)]
enum UnsafeRecoverCmd {
    /// Remove the failed machines from the peer list for the regions
    RemoveFailStores {
        #[structopt(
            short = "s",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Stores to be removed
        stores: Vec<u64>,

        #[structopt(
            required_unless = "all-regions",
            conflicts_with = "all-regions",
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Only for these regions
        regions: Option<Vec<u64>>,

        #[structopt(long)]
        /// Promote learner to voter
        promote_learner: bool,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[structopt(required_unless = "regions", conflicts_with = "regions", long)]
        /// Do the command for all regions
        all_regions: bool,
    },
    /// Remove unapplied raftlogs on the regions
    DropUnappliedRaftlog {
        #[structopt(
            required_unless = "all-regions",
            conflicts_with = "all-regions",
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Only for these regions
        regions: Option<Vec<u64>>,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[structopt(required_unless = "regions", conflicts_with = "regions", long)]
        /// Do the command for all regions
        all_regions: bool,
    },
}

fn main() {
    let opt = Opt::from_args();

    // Initialize logger.
    init_ctl_logger(&opt.log_level);

    // Initialize configuration and security manager.
    let cfg_path = opt.config.as_ref();
    let cfg = cfg_path.map_or_else(
        || {
            let mut cfg = TiKvConfig::default();
            cfg.log_level = tikv_util::logger::get_level_by_string("warn").unwrap();
            cfg
        },
        |path| {
            let s = fs::read_to_string(&path).unwrap();
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
                println!("{}", log_wrappers::hex_encode_upper(unescape(escaped)));
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

    // Bypass the ldb and sst dump command to RocksDB.
    if let Cmd::External(args) = cmd {
        match args[0].as_str() {
            "ldb" => run_ldb_command(args, &cfg),
            "sst_dump" => run_sst_dump_command(args, &cfg),
            _ => Opt::clap().print_help().unwrap(),
        }
        return;
    }

    if let Cmd::BadSsts { db, manifest, pd } = cmd {
        let pd_client = get_pd_rpc_client(&pd, Arc::clone(&mgr));
        print_bad_ssts(&db, manifest.as_deref(), pd_client, &cfg);
        return;
    }

    // Deal with subcommand dump-snap-meta. This subcommand doesn't require other args, so process
    // it before checking args.
    if let Cmd::DumpSnapMeta { file } = cmd {
        let path = file.as_ref();
        return dump_snap_meta_file(path);
    }

    // Deal with arguments about key utils.
    if let Some(hex) = opt.hex_to_escaped.as_deref() {
        println!("{}", escape(&from_hex(hex).unwrap()));
        return;
    } else if let Some(escaped) = opt.escaped_to_hex.as_deref() {
        println!("{}", hex::encode_upper(unescape(escaped)));
        return;
    } else if let Some(encoded) = opt.decode.as_deref() {
        match Key::from_encoded(unescape(encoded)).into_raw() {
            Ok(k) => println!("{}", escape(&k)),
            Err(e) => println!("decode meets error: {}", e),
        };
        return;
    } else if let Some(decoded) = opt.encode.as_deref() {
        println!("{}", Key::from_raw(&unescape(decoded)));
        return;
    }

    if let Cmd::DecryptFile { file, out_file } = cmd {
        let message = "This action will expose sensitive data as plaintext on persistent storage";
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

        let mthd = encryption_method_from_db_encryption_method(file_info.method);
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
        let f = File::open(&infile).unwrap();
        let mut reader = DecrypterReader::new(f, mthd, &file_info.key, iv).unwrap();

        io::copy(&mut reader, &mut outf).unwrap();
        println!("crc32: {}", calc_crc32(outfile).unwrap());
        return;
    }

    if let Cmd::EncryptionMeta { cmd: subcmd } = cmd {
        match subcmd {
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
        }
        return;
    }

    // Deal with all subcommands needs PD.
    if let Some(pd) = opt.pd.as_deref() {
        let pd_client = get_pd_rpc_client(pd, Arc::clone(&mgr));
        if let Cmd::CompactCluster {
            db,
            cf,
            from,
            to,
            threads,
            bottommost,
        } = cmd
        {
            let db_type = if db == "kv" { DBType::Kv } else { DBType::Raft };
            let cfs = cf.iter().map(|s| s.as_ref()).collect();
            let from_key = from.map(|k| unescape(&k));
            let to_key = to.map(|k| unescape(&k));
            let bottommost = BottommostLevelCompaction::from(Some(bottommost.as_ref()));
            return compact_whole_cluster(
                &pd_client, &cfg, mgr, db_type, cfs, from_key, to_key, threads, bottommost,
            );
        }
        if let Cmd::SplitRegion {
            region: region_id,
            key,
        } = cmd
        {
            let key = unescape(&key);
            return split_region(&pd_client, mgr, region_id, key);
        }

        Opt::clap().print_help().ok();
        return;
    }

    // Deal with all subcommands about db or host.
    let data_dir = opt.data_dir.as_deref();
    let skip_paranoid_checks = opt.skip_paranoid_checks;
    let host = opt.host.as_deref();

    let debug_executor =
        new_debug_executor(&cfg, data_dir, skip_paranoid_checks, host, Arc::clone(&mgr));

    if let Cmd::Print { cf, key } = cmd {
        let key = unescape(&key);
        debug_executor.dump_value(&cf, key);
    } else if let Cmd::Raft { cmd: subcmd } = cmd {
        match subcmd {
            RaftCmd::Log { region, index, key } => {
                let (id, index) = if let Some(key) = key.as_deref() {
                    keys::decode_raft_log_key(&unescape(key)).unwrap()
                } else {
                    let id = region.unwrap();
                    let index = index.unwrap();
                    (id, index)
                };
                debug_executor.dump_raft_log(id, index);
            }
            RaftCmd::Region {
                regions,
                skip_tombstone,
                ..
            } => {
                debug_executor.dump_region_info(regions, skip_tombstone);
            }
        }
    } else if let Cmd::Size { region, cf } = cmd {
        let cfs = cf.iter().map(AsRef::as_ref).collect();
        if let Some(id) = region {
            debug_executor.dump_region_size(id, cfs);
        } else {
            debug_executor.dump_all_region_size(cfs);
        }
    } else if let Cmd::Scan {
        from,
        to,
        limit,
        show_cf,
        start_ts,
        commit_ts,
    } = cmd
    {
        let from = unescape(&from);
        let to = to.map_or_else(Vec::new, |to| unescape(&to));
        let limit = limit.unwrap_or(0);
        if to.is_empty() && limit == 0 {
            println!(r#"please pass "to" or "limit""#);
            process::exit(-1);
        }
        let cfs = show_cf.iter().map(AsRef::as_ref).collect();
        debug_executor.dump_mvccs_infos(from, to, limit, cfs, start_ts, commit_ts);
    } else if let Cmd::RawScan {
        from,
        to,
        limit,
        cf,
    } = cmd
    {
        let from = unescape(&from);
        let to = unescape(&to);
        debug_executor.raw_scan(&from, &to, limit, &cf);
    } else if let Cmd::Mvcc {
        key,
        show_cf,
        start_ts,
        commit_ts,
    } = cmd
    {
        let from = unescape(&key);
        let cfs = show_cf.iter().map(AsRef::as_ref).collect();
        debug_executor.dump_mvccs_infos(from, vec![], 0, cfs, start_ts, commit_ts);
    } else if let Cmd::Diff {
        region,
        to_data_dir,
        to_host,
        to_config,
        ..
    } = cmd
    {
        let to_data_dir = to_data_dir.as_deref();
        let to_host = to_host.as_deref();
        let to_config = to_config.map_or_else(TiKvConfig::default, |path| {
            let s = fs::read_to_string(&path).unwrap();
            toml::from_str(&s).unwrap()
        });
        debug_executor.diff_region(region, to_host, to_data_dir, &to_config, mgr);
    } else if let Cmd::Compact {
        region,
        db,
        cf,
        from,
        to,
        threads,
        bottommost,
    } = cmd
    {
        let db_type = if db == "kv" { DBType::Kv } else { DBType::Raft };
        let from_key = from.map(|k| unescape(&k));
        let to_key = to.map(|k| unescape(&k));
        let bottommost = BottommostLevelCompaction::from(Some(bottommost.as_ref()));
        if let Some(region) = region {
            debug_executor.compact_region(host, db_type, &cf, region, threads, bottommost);
        } else {
            debug_executor.compact(host, db_type, &cf, from_key, to_key, threads, bottommost);
        }
    } else if let Cmd::Tombstone { regions, pd, force } = cmd {
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
    } else if let Cmd::RecoverMvcc {
        read_only,
        all,
        threads,
        regions,
        pd: pd_urls,
    } = cmd
    {
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
    } else if let Cmd::UnsafeRecover { cmd: subcmd } = cmd {
        match subcmd {
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
        }
    } else if let Cmd::RecreateRegion {
        pd,
        region: region_id,
    } = cmd
    {
        let pd_cfg = PdConfig {
            endpoints: pd,
            ..Default::default()
        };
        debug_executor.recreate_region(mgr, &pd_cfg, region_id);
    } else if let Cmd::ConsistencyCheck { region } = cmd {
        debug_executor.check_region_consistency(region);
    } else if let Cmd::BadRegions {} = cmd {
        debug_executor.print_bad_regions();
    } else if let Cmd::ModifyTikvConfig {
        config_name,
        config_value,
    } = cmd
    {
        debug_executor.modify_tikv_config(&config_name, &config_value);
    } else if let Cmd::Metrics { tag } = cmd {
        let tags = tag.iter().map(AsRef::as_ref).collect();
        debug_executor.dump_metrics(tags)
    } else if let Cmd::RegionProperties { region } = cmd {
        debug_executor.dump_region_properties(region)
    } else if let Cmd::RangeProperties { start, end } = cmd {
        let start_key = from_hex(&start).unwrap();
        let end_key = from_hex(&end).unwrap();
        debug_executor.dump_range_properties(start_key, end_key);
    } else if let Cmd::Fail { cmd: subcmd } = cmd {
        if host.is_none() {
            println!("command fail requires host");
            process::exit(-1);
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
    } else if let Cmd::Store {} = cmd {
        debug_executor.dump_store_info();
    } else if let Cmd::Cluster {} = cmd {
        debug_executor.dump_cluster_info();
    } else {
        Opt::clap().print_help().ok();
    }
}

fn from_hex(key: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if key.starts_with("0x") || key.starts_with("0X") {
        return hex::decode(&key[2..]);
    }
    hex::decode(key)
}

fn convert_gbmb(mut bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes < MIB {
        return format!("{}B", bytes);
    }
    let mb = if bytes % GIB == 0 {
        String::from("")
    } else {
        format!("{:.3}MiB", (bytes % GIB) as f64 / MIB as f64)
    };
    bytes /= GIB;
    let gb = if bytes == 0 {
        String::from("")
    } else {
        format!("{}GiB ", bytes)
    };
    format!("{}{}", gb, mb)
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

fn get_pd_rpc_client(pd: &str, mgr: Arc<SecurityManager>) -> RpcClient {
    let cfg = PdConfig::new(vec![pd.to_string()]);
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
    cfg: &TiKvConfig,
    mgr: Arc<SecurityManager>,
    db_type: DBType,
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
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let debug_executor = new_debug_executor(&cfg, None, false, Some(&addr), mgr);
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
                tikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap();
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
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

fn run_ldb_command(args: Vec<String>, cfg: &TiKvConfig) {
    let key_manager = data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
        .unwrap()
        .map(Arc::new);
    let env = get_env(key_manager, None /*io_rate_limiter*/).unwrap();
    let mut opts = cfg.rocksdb.build_opt();
    opts.set_env(env);

    engine_rocks::raw::run_ldb_tool(&args, &opts);
}

fn run_sst_dump_command(args: Vec<String>, cfg: &TiKvConfig) {
    let opts = cfg.rocksdb.build_opt();
    engine_rocks::raw::run_sst_dump_tool(&args, &opts);
}

fn print_bad_ssts(db: &str, manifest: Option<&str>, pd_client: RpcClient, cfg: &TiKvConfig) {
    let mut args = vec![
        "sst_dump".to_string(),
        "--output_hex".to_string(),
        "--command=verify".to_string(),
    ];
    args.push(format!("--file={}", db));

    let mut stderr = BufferRedirect::stderr().unwrap();
    let stdout = BufferRedirect::stdout().unwrap();
    let opts = cfg.rocksdb.build_opt();
    match run_and_wait_child_process(|| engine_rocks::raw::run_sst_dump_tool(&args, &opts)).unwrap()
    {
        0 => {}
        status => {
            let mut err = String::new();
            stderr.read_to_string(&mut err).unwrap();
            println!("failed to run {}:\n{}", args.join(" "), err);
            std::process::exit(status);
        }
    };

    let mut stderr_buf = stderr.into_inner();
    drop(stdout);
    let mut buffer = Vec::new();
    stderr_buf.read_to_end(&mut buffer).unwrap();
    let corruptions = unsafe { String::from_utf8_unchecked(buffer) };

    for line in corruptions.lines() {
        println!("--------------------------------------------------------");
        // The corruption format may like this:
        // /path/to/db/057155.sst is corrupted: Corruption: block checksum mismatch: expected 3754995957, got 708533950  in /path/to/db/057155.sst offset 3126049 size 22724
        println!("corruption info:\n{}", line);
        let parts = line.splitn(2, ':').collect::<Vec<_>>();
        let path = Path::new(parts[0]);
        match path.extension() {
            Some(ext) if ext.to_str().unwrap() == "sst" => {}
            _ => {
                println!("skip bad line format: {}", line);
                continue;
            }
        }
        let sst_file_number = path.file_stem().unwrap().to_str().unwrap();
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
        // --------------- Column family "write"  (ID 2) --------------
        // 63:132906243[3555338 .. 3555338]['7A311B40EFCC2CB4C5911ECF3937D728DED26AE53FA5E61BE04F23F2BE54EACC73' seq:3555338, type:1 .. '7A313030302E25CD5F57252E' seq:3555338, type:1] at level 0
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

            if start.starts_with(&[keys::DATA_PREFIX]) {
                print_overlap_region_and_suggestions(&pd_client, &start[1..], &end[1..], db, path);
            } else if start.starts_with(&[keys::LOCAL_PREFIX]) {
                println!(
                    "it isn't easy to handle local data, start key:{}",
                    log_wrappers::Value(&start)
                );

                // consider the case that include both meta and user data
                if end.starts_with(&[keys::DATA_PREFIX]) {
                    print_overlap_region_and_suggestions(&pd_client, &[], &end[1..], db, path);
                }
            } else {
                println!("unexpected key {}", log_wrappers::Value(&start));
            }
        } else {
            // it is expected when the sst is output of a compaction and the sst isn't added to manifest yet.
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
    sst_path: &Path,
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

    println!("\nsuggested operations:");
    println!(
        "tikv-ctl ldb --db={} unsafe_remove_sst_file {:?}",
        db, sst_path
    );
    for region in regions_to_print {
        println!(
            "tikv-ctl --db={} tombstone -r {} --pd <endpoint>",
            db, region.id
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_hex() {
        let result = vec![0x74];
        assert_eq!(from_hex("74").unwrap(), result);
        assert_eq!(from_hex("0x74").unwrap(), result);
        assert_eq!(from_hex("0X74").unwrap(), result);
    }
}
