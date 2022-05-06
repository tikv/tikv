// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::ToOwned, cmp::Ordering, path::PathBuf, pin::Pin, str, string::ToString, sync::Arc,
    time::Duration, u64,
};

use encryption_export::data_key_manager_from_config;
use engine_rocks::{
    raw_util::{db_exist, new_engine_opt},
    RocksEngine,
};
use engine_traits::{
    Engines, Error as EngineError, RaftEngine, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS,
};
use futures::{executor::block_on, future, stream, Stream, StreamExt, TryStreamExt};
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    debugpb::{Db as DBType, *},
    kvrpcpb::MvccInfo,
    metapb::{Peer, Region},
    raft_cmdpb::RaftCmdRequest,
    raft_serverpb::PeerState,
};
use pd_client::{Config as PdConfig, PdClient, RpcClient};
use protobuf::Message;
use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use raft_log_engine::RaftLogEngine;
use raftstore::store::INIT_EPOCH_CONF_VER;
use security::SecurityManager;
use serde_json::json;
use tikv::{
    config::{ConfigController, TiKvConfig},
    server::debug::{BottommostLevelCompaction, Debugger, RegionInfo},
};
use tikv_util::escape;

use crate::util::*;

pub const METRICS_PROMETHEUS: &str = "prometheus";
pub const METRICS_ROCKSDB_KV: &str = "rocksdb_kv";
pub const METRICS_ROCKSDB_RAFT: &str = "rocksdb_raft";
pub const METRICS_JEMALLOC: &str = "jemalloc";
pub const LOCK_FILE_ERROR: &str = "IO error: While lock file";

type MvccInfoStream = Pin<Box<dyn Stream<Item = Result<(Vec<u8>, MvccInfo), String>>>>;

pub fn new_debug_executor(
    cfg: &TiKvConfig,
    data_dir: Option<&str>,
    skip_paranoid_checks: bool,
    host: Option<&str>,
    mgr: Arc<SecurityManager>,
) -> Box<dyn DebugExecutor> {
    if let Some(remote) = host {
        return Box::new(new_debug_client(remote, mgr)) as Box<dyn DebugExecutor>;
    }

    // TODO: perhaps we should allow user skip specifying data path.
    let data_dir = data_dir.unwrap();
    let kv_path = cfg.infer_kv_engine_path(Some(data_dir)).unwrap();

    let key_manager = data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
        .unwrap()
        .map(Arc::new);

    let cache = cfg.storage.block_cache.build_shared_cache();
    let shared_block_cache = cache.is_some();
    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), None /*io_rate_limiter*/)
        .unwrap();

    let mut kv_db_opts = cfg.rocksdb.build_opt();
    kv_db_opts.set_env(env.clone());
    kv_db_opts.set_paranoid_checks(!skip_paranoid_checks);
    let kv_cfs_opts = cfg
        .rocksdb
        .build_cf_opts(&cache, None, cfg.storage.api_version());
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
        let raft_path = cfg.infer_raft_db_path(Some(data_dir)).unwrap();
        if !db_exist(&raft_path) {
            error!("raft db not exists: {}", raft_path);
            tikv_util::logger::exit_process_gracefully(-1);
        }
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
        config.dir = cfg.infer_raft_engine_path(Some(data_dir)).unwrap();
        if !RaftLogEngine::exists(&config.dir) {
            error!("raft engine not exists: {}", config.dir);
            tikv_util::logger::exit_process_gracefully(-1);
        }
        let raft_db = RaftLogEngine::new(config, key_manager, None /*io_rate_limiter*/).unwrap();
        let debugger = Debugger::new(Engines::new(kv_db, raft_db), cfg_controller);
        Box::new(debugger) as Box<dyn DebugExecutor>
    }
}

pub fn new_debug_client(host: &str, mgr: Arc<SecurityManager>) -> DebugClient {
    let env = Arc::new(Environment::new(1));
    let cb = ChannelBuilder::new(env)
        .max_receive_message_len(1 << 30) // 1G.
        .max_send_message_len(1 << 30)
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3));

    let channel = mgr.connect(cb, host);
    DebugClient::new(channel)
}

pub trait DebugExecutor {
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
            tikv_util::logger::exit_process_gracefully(-1);
        }
        if !to.is_empty() && to < from {
            println!("\"to\" must be greater than \"from\"");
            tikv_util::logger::exit_process_gracefully(-1);
        }

        cfs.iter().for_each(|cf| {
            if !DATA_CFS.contains(cf) {
                eprintln!("CF \"{}\" doesn't exist.", cf);
                tikv_util::logger::exit_process_gracefully(-1);
            }
        });

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
            tikv_util::logger::exit_process_gracefully(-1);
        }
    }

    fn raw_scan(&self, from_key: &[u8], to_key: &[u8], limit: usize, cf: &str) {
        if !ALL_CFS.contains(&cf) {
            eprintln!("CF \"{}\" doesn't exist.", cf);
            tikv_util::logger::exit_process_gracefully(-1);
        }
        if !to_key.is_empty() && from_key >= to_key {
            eprintln!(
                "to_key should be greater than from_key, but got from_key: \"{}\", to_key: \"{}\"",
                escape(from_key),
                escape(to_key)
            );
            tikv_util::logger::exit_process_gracefully(-1);
        }
        if limit == 0 {
            eprintln!("limit should be greater than 0");
            tikv_util::logger::exit_process_gracefully(-1);
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
                            tikv_util::logger::exit_process_gracefully(-1);
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
                tikv_util::logger::exit_process_gracefully(-1);
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
                tikv_util::logger::exit_process_gracefully(-1);
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

    fn reset_to_version(&self, version: u64);
}

impl DebugExecutor for DebugClient {
    fn check_local_mode(&self) {
        println!("This command is only for local mode");
        tikv_util::logger::exit_process_gracefully(-1);
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
        println!("store id: {}", resp.get_store_id());
        println!("api version: {:?}", resp.get_api_version())
    }

    fn dump_cluster_info(&self) {
        let req = GetClusterInfoRequest::default();
        let resp = self
            .get_cluster_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_cluster_info", e));
        println!("{}", resp.get_cluster_id())
    }

    fn reset_to_version(&self, version: u64) {
        let mut req = ResetToVersionRequest::default();
        req.set_ts(version);
        DebugClient::reset_to_version(self, &req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_cluster_info", e));
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
                tikv_util::logger::exit_process_gracefully(-1);
            }
            Err(e) => perror_and_exit("RpcClient::get_region_by_id", e),
        };

        let new_region_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));
        let new_peer_id = rpc_client
            .alloc_id()
            .unwrap_or_else(|e| perror_and_exit("RpcClient::alloc_id", e));

        let store_id = self.get_store_ident().expect("get store id").store_id;

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
        tikv_util::logger::exit_process_gracefully(-1);
    }

    fn modify_tikv_config(&self, _: &str, _: &str) {
        println!("only support remote mode");
        tikv_util::logger::exit_process_gracefully(-1);
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
        let store_ident_info = self.get_store_ident();
        if let Ok(ident) = store_ident_info {
            println!("store id: {}", ident.get_store_id());
            println!("api version: {:?}", ident.get_api_version());
        }
    }

    fn dump_cluster_info(&self) {
        let store_ident_info = self.get_store_ident();
        if let Ok(ident) = store_ident_info {
            println!("cluster id: {}", ident.get_cluster_id());
        }
    }

    fn reset_to_version(&self, version: u64) {
        Debugger::reset_to_version(self, version);
    }
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

    tikv_util::logger::exit_process_gracefully(-1);
}
