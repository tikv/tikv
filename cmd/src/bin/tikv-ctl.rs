// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate clap;
#[macro_use]
extern crate vlog;

use std::borrow::ToOwned;
use std::cmp::Ordering;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{process, str, u64};

use clap::{crate_authors, App, AppSettings, Arg, ArgMatches, SubCommand};
use futures::{executor::block_on, future, stream, Stream, StreamExt, TryStreamExt};
use grpcio::{CallOption, ChannelBuilder, Environment};
use protobuf::Message;

use encryption_export::{
    create_backend, data_key_manager_from_config, encryption_method_from_db_encryption_method,
    DataKeyManager, DecrypterReader, Iv,
};
use engine_rocks::encryption::get_env;
use engine_rocks::RocksEngine;
use engine_traits::{EncryptionKeyManager, Engines, RaftEngine};
use engine_traits::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE};
use file_system::calc_crc32;
use kvproto::debugpb::{Db as DBType, *};
use kvproto::encryptionpb::EncryptionMethod;
use kvproto::kvrpcpb::{MvccInfo, SplitRegionRequest};
use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::{PeerState, SnapshotMeta};
use kvproto::tikvpb::TikvClient;
use pd_client::{Config as PdConfig, PdClient, RpcClient};
use raft::eraftpb::{ConfChange, ConfChangeV2, Entry, EntryType};
use raft_log_engine::RaftLogEngine;
use raftstore::store::INIT_EPOCH_CONF_VER;
use security::{SecurityConfig, SecurityManager};
use std::pin::Pin;
use tikv::config::{ConfigController, TiKvConfig};
use tikv::server::debug::{BottommostLevelCompaction, Debugger, RegionInfo};
use tikv_util::{escape, unescape};
use txn_types::Key;

const METRICS_PROMETHEUS: &str = "prometheus";
const METRICS_ROCKSDB_KV: &str = "rocksdb_kv";
const METRICS_ROCKSDB_RAFT: &str = "rocksdb_raft";
const METRICS_JEMALLOC: &str = "jemalloc";

type MvccInfoStream = Pin<Box<dyn Stream<Item = Result<(Vec<u8>, MvccInfo), String>>>>;

fn perror_and_exit<E: Error>(prefix: &str, e: E) -> ! {
    ve1!("{}: {}", prefix, e);
    process::exit(-1);
}

fn new_debug_executor(
    db: Option<&str>,
    raft_db: Option<&str>,
    skip_paranoid_checks: bool,
    host: Option<&str>,
    cfg: &TiKvConfig,
    mgr: Arc<SecurityManager>,
) -> Box<dyn DebugExecutor> {
    match (host, db) {
        (None, Some(kv_path)) => {
            let key_manager =
                data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
                    .unwrap()
                    .map(Arc::new);
            let cache = cfg.storage.block_cache.build_shared_cache();
            let shared_block_cache = cache.is_some();
            let env = get_env(key_manager, None).unwrap();

            let mut kv_db_opts = cfg.rocksdb.build_opt();
            kv_db_opts.set_env(env.clone());
            kv_db_opts.set_paranoid_checks(!skip_paranoid_checks);
            let kv_cfs_opts = cfg.rocksdb.build_cf_opts(&cache, None);
            let kv_path = PathBuf::from(kv_path).canonicalize().unwrap();
            let kv_path = kv_path.to_str().unwrap();
            let kv_db =
                engine_rocks::raw_util::new_engine_opt(kv_path, kv_db_opts, kv_cfs_opts).unwrap();
            let mut kv_db = RocksEngine::from_db(Arc::new(kv_db));
            kv_db.set_shared_block_cache(shared_block_cache);

            let mut raft_path = raft_db
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("{}/../raft", kv_path));
            raft_path = PathBuf::from(raft_path)
                .canonicalize()
                .unwrap()
                .to_str()
                .map(ToString::to_string)
                .unwrap();

            let cfg_controller = ConfigController::default();
            if !cfg.raft_engine.enable {
                let mut raft_db_opts = cfg.raftdb.build_opt();
                raft_db_opts.set_env(env);
                let raft_db_cf_opts = cfg.raftdb.build_cf_opts(&cache);
                let raft_db = engine_rocks::raw_util::new_engine_opt(
                    &raft_path,
                    raft_db_opts,
                    raft_db_cf_opts,
                )
                .unwrap();
                let mut raft_db = RocksEngine::from_db(Arc::new(raft_db));
                raft_db.set_shared_block_cache(shared_block_cache);
                let debugger = Debugger::new(Engines::new(kv_db, raft_db), cfg_controller);
                Box::new(debugger) as Box<dyn DebugExecutor>
            } else {
                let config = cfg.raft_engine.config();
                let raft_db = RaftLogEngine::new(config);
                let debugger = Debugger::new(Engines::new(kv_db, raft_db), cfg_controller);
                Box::new(debugger) as Box<dyn DebugExecutor>
            }
        }
        (Some(remote), None) => Box::new(new_debug_client(remote, mgr)) as Box<dyn DebugExecutor>,
        _ => unreachable!(),
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
        v1!("value: {}", escape(&value));
    }

    fn dump_region_size(&self, region: u64, cfs: Vec<&str>) -> usize {
        let sizes = self.get_region_size(region, cfs);
        let mut total_size = 0;
        v1!("region id: {}", region);
        for (cf, size) in sizes {
            v1!("cf {} region size: {}", cf, convert_gbmb(size as u64));
            total_size += size;
        }
        total_size
    }

    fn dump_all_region_size(&self, cfs: Vec<&str>) {
        let regions = self.get_all_meta_regions();
        let regions_number = regions.len();
        let mut total_size = 0;
        for region in regions {
            total_size += self.dump_region_size(region, cfs.clone());
        }
        v1!("total region number: {}", regions_number);
        v1!("total region size: {}", convert_gbmb(total_size as u64));
    }

    fn dump_region_info(&self, region: u64, skip_tombstone: bool) {
        let r = self.get_region_info(region);
        if skip_tombstone {
            let region_state = r.region_local_state.as_ref();
            if region_state.map_or(false, |s| s.get_state() == PeerState::Tombstone) {
                return;
            }
        }
        let region_state_key = keys::region_state_key(region);
        let raft_state_key = keys::raft_state_key(region);
        let apply_state_key = keys::apply_state_key(region);
        v1!("region id: {}", region);
        v1!("region state key: {}", escape(&region_state_key));
        v1!("region state: {:?}", r.region_local_state);
        v1!("raft state key: {}", escape(&raft_state_key));
        v1!("raft state: {:?}", r.raft_local_state);
        v1!("apply state key: {}", escape(&apply_state_key));
        v1!("apply state: {:?}", r.raft_apply_state);
    }

    fn dump_all_region_info(&self, skip_tombstone: bool) {
        for region in self.get_all_meta_regions() {
            self.dump_region_info(region, skip_tombstone);
        }
    }

    fn dump_raft_log(&self, region: u64, index: u64) {
        let idx_key = keys::raft_log_key(region, index);
        v1!("idx_key: {}", escape(&idx_key));
        v1!("region: {}", region);
        v1!("log index: {}", index);

        let mut entry = self.get_raft_log(region, index);
        let data = entry.take_data();
        v1!("entry {:?}", entry);
        v1!("msg len: {}", data.len());

        if data.is_empty() {
            return;
        }

        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                let mut msg = RaftCmdRequest::default();
                msg.merge_from_bytes(&data).unwrap();
                v1!("Normal: {:#?}", msg);
            }
            EntryType::EntryConfChange => {
                let mut msg = ConfChange::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                v1!("ConfChange: {:?}", msg);
                let mut cmd = RaftCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                v1!("ConfChange.RaftCmdRequest: {:#?}", cmd);
            }
            EntryType::EntryConfChangeV2 => {
                let mut msg = ConfChangeV2::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                v1!("ConfChangeV2: {:?}", msg);
                let mut cmd = RaftCmdRequest::default();
                cmd.merge_from_bytes(&ctx).unwrap();
                v1!("ConfChangeV2.RaftCmdRequest: {:#?}", cmd);
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
            ve1!("from and to should start with \"z\"");
            process::exit(-1);
        }
        if !to.is_empty() && to < from {
            ve1!("\"to\" must be greater than \"from\"");
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
                        v1!("no mvcc infos for {}", escape(&from));
                        return future::err::<(), String>("no mvcc infos".to_owned());
                    }

                    v1!("key: {}", escape(&key));
                    if cfs.contains(&CF_LOCK) && mvcc.has_lock() {
                        let lock_info = mvcc.get_lock();
                        if start_ts.map_or(true, |ts| lock_info.get_start_ts() == ts) {
                            v1!("\tlock cf value: {:?}", lock_info);
                        }
                    }
                    if cfs.contains(&CF_DEFAULT) {
                        for value_info in mvcc.get_values() {
                            if commit_ts.map_or(true, |ts| value_info.get_start_ts() == ts) {
                                v1!("\tdefault cf value: {:?}", value_info);
                            }
                        }
                    }
                    if cfs.contains(&CF_WRITE) {
                        for write_info in mvcc.get_writes() {
                            if start_ts.map_or(true, |ts| write_info.get_start_ts() == ts)
                                && commit_ts.map_or(true, |ts| write_info.get_commit_ts() == ts)
                            {
                                v1!("\t write cf value: {:?}", write_info);
                            }
                        }
                    }
                    v1!("");
                    future::ok::<(), String>(())
                });
        if let Err(e) = block_on(scan_future) {
            ve1!("{}", e);
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
        db: Option<&str>,
        raft_db: Option<&str>,
        host: Option<&str>,
        cfg: &TiKvConfig,
        mgr: Arc<SecurityManager>,
    ) {
        let rhs_debug_executor = new_debug_executor(db, raft_db, false, host, cfg, mgr);

        let r1 = self.get_region_info(region);
        let r2 = rhs_debug_executor.get_region_info(region);
        v1!("region id: {}", region);
        v1!("db1 region state: {:?}", r1.region_local_state);
        v1!("db2 region state: {:?}", r2.region_local_state);
        v1!("db1 apply state: {:?}", r1.raft_apply_state);
        v1!("db2 apply state: {:?}", r2.raft_apply_state);

        match (r1.region_local_state, r2.region_local_state) {
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                v1!("db1 and db2 don't have same region local_state");
            }
            (Some(region_local_1), Some(region_local_2)) => {
                let region1 = region_local_1.get_region();
                let region2 = region_local_2.get_region();
                if region1 != region2 {
                    v1!("db1 and db2 have different region:");
                    v1!("db1 region: {:?}", region1);
                    v1!("db2 region: {:?}", region2);
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
                            v1!("db{} scan data in region {} fail: {}", i, region, e);
                            process::exit(-1);
                        }
                        Ok(s) => Some(s),
                    }
                };

                let show_only = |i: usize, k: &[u8]| {
                    v1!("only db{} has: {}", i, escape(k));
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
                                v1!("diff mvcc on key: {}", escape(&t1.0));
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
                    v1!("db1 and db2 have same data in region: {}", region);
                }
                v1!(
                    "db1 has {} keys, db2 has {} keys",
                    key_counts[0],
                    key_counts[1]
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
        v1!(
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
        v1!(
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
                ve1!("no such region in pd: {}", region_id);
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
    fn remove_fail_stores(&self, store_ids: Vec<u64>, region_ids: Option<Vec<u64>>);

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
                ve1!("no such region in pd: {}", region_id);
                process::exit(-1);
            })
            .collect();
        self.recover_regions(regions, read_only);
    }

    fn recover_mvcc_all(&self, threads: usize, read_only: bool) {
        self.check_local_mode();
        self.recover_all(threads, read_only);
    }

    fn get_all_meta_regions(&self) -> Vec<u64>;

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
        ve1!("This command is only for local mode");
        process::exit(-1);
    }

    fn get_all_meta_regions(&self) -> Vec<u64> {
        unimplemented!();
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
            v1!("tag:{}", tag);
            let metrics = match tag {
                METRICS_ROCKSDB_KV => resp.take_rocksdb_kv(),
                METRICS_ROCKSDB_RAFT => resp.take_rocksdb_raft(),
                METRICS_JEMALLOC => resp.take_jemalloc(),
                METRICS_PROMETHEUS => resp.take_prometheus(),
                _ => String::from(
                    "unsupported tag, should be one of prometheus/jemalloc/rocksdb_raft/rocksdb_kv",
                ),
            };
            v1!("{}", metrics);
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

    fn remove_fail_stores(&self, _: Vec<u64>, _: Option<Vec<u64>>) {
        self.check_local_mode();
    }

    fn recreate_region(&self, _: Arc<SecurityManager>, _: &PdConfig, _: u64) {
        self.check_local_mode();
    }

    fn check_region_consistency(&self, region_id: u64) {
        let mut req = RegionConsistencyCheckRequest::default();
        req.set_region_id(region_id);
        self.check_region_consistency(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::check_region_consistency", e));
        v1!("success!");
    }

    fn modify_tikv_config(&self, config_name: &str, config_value: &str) {
        let mut req = ModifyTikvConfigRequest::default();
        req.set_config_name(config_name.to_owned());
        req.set_config_value(config_value.to_owned());
        self.modify_tikv_config(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::modify_tikv_config", e));
        v1!("success");
    }

    fn dump_region_properties(&self, region_id: u64) {
        let mut req = GetRegionPropertiesRequest::default();
        req.set_region_id(region_id);
        let resp = self
            .get_region_properties(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_region_properties", e));
        for prop in resp.get_props() {
            v1!("{}: {}", prop.get_name(), prop.get_value());
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
        v1!("{}", resp.get_store_id())
    }

    fn dump_cluster_info(&self) {
        let req = GetClusterInfoRequest::default();
        let resp = self
            .get_cluster_info(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_cluster_info", e));
        v1!("{}", resp.get_cluster_id())
    }
}

impl<ER: RaftEngine> DebugExecutor for Debugger<ER> {
    fn check_local_mode(&self) {}

    fn get_all_meta_regions(&self) -> Vec<u64> {
        self.get_all_meta_regions()
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_all_meta_regions", e))
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
            v1!("success!");
            return;
        }
        for (region_id, error) in ret {
            ve1!("region: {}, error: {}", region_id, error);
        }
    }

    fn set_region_tombstone_by_id(&self, region_ids: Vec<u64>) {
        let ret = self
            .set_region_tombstone_by_id(region_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::set_region_tombstone_by_id", e));
        if ret.is_empty() {
            v1!("success!");
            return;
        }
        for (region_id, error) in ret {
            ve1!("region: {}, error: {}", region_id, error);
        }
    }

    fn recover_regions(&self, regions: Vec<Region>, read_only: bool) {
        let ret = self
            .recover_regions(regions, read_only)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover regions", e));
        if ret.is_empty() {
            v1!("success!");
            return;
        }
        for (region_id, error) in ret {
            ve1!("region: {}, error: {}", region_id, error);
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
                v1!("{}: {}", region_id, error);
            }
            return;
        }
        v1!("all regions are healthy")
    }

    fn remove_fail_stores(&self, store_ids: Vec<u64>, region_ids: Option<Vec<u64>>) {
        v1!("removing stores {:?} from configurations...", store_ids);
        self.remove_failed_stores(store_ids, region_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        v1!("success");
    }

    fn recreate_region(&self, mgr: Arc<SecurityManager>, pd_cfg: &PdConfig, region_id: u64) {
        let rpc_client = RpcClient::new(pd_cfg, None, mgr)
            .unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let mut region = match block_on(rpc_client.get_region_by_id(region_id)) {
            Ok(Some(region)) => region,
            Ok(None) => {
                ve1!("no such region {} on PD", region_id);
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

        v1!(
            "initing empty region {} with peer_id {}...",
            new_region_id,
            new_peer_id
        );
        self.recreate_region(region)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recreate_region", e));
        v1!("success");
    }

    fn dump_metrics(&self, _tags: Vec<&str>) {
        unimplemented!("only available for online mode");
    }

    fn check_region_consistency(&self, _: u64) {
        ve1!("only support remote mode");
        process::exit(-1);
    }

    fn modify_tikv_config(&self, _: &str, _: &str) {
        ve1!("only support remote mode");
        process::exit(-1);
    }

    fn dump_region_properties(&self, region_id: u64) {
        let props = self
            .get_region_properties(region_id)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_region_properties", e));
        for (name, value) in props {
            v1!("{}: {}", name, value);
        }
    }

    fn dump_range_properties(&self, start: Vec<u8>, end: Vec<u8>) {
        let props = self
            .get_range_properties(&start, &end)
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_range_properties", e));
        for (name, value) in props {
            v1!("{}: {}", name, value);
        }
    }

    fn dump_store_info(&self) {
        let store_id = self.get_store_id();
        if let Ok(id) = store_id {
            v1!("store id: {}", id);
        }
    }

    fn dump_cluster_info(&self) {
        let cluster_id = self.get_cluster_id();
        if let Ok(id) = cluster_id {
            v1!("cluster id: {}", id);
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

fn main() {
    vlog::set_verbosity_level(1);

    let raw_key_hint: &'static str = "Raw key (generally starts with \"z\") in escaped form";
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    let version_info = tikv::tikv_version_info(build_timestamp);

    let mut app = App::new("TiKV Control (tikv-ctl)")
        .about("A tool for interacting with TiKV deployments.")
        .author(crate_authors!())
        .version(version_info.as_ref())
        .long_version(version_info.as_ref())
        .setting(AppSettings::AllowExternalSubcommands)
        .arg(
            Arg::with_name("db")
                .long("db")
                .takes_value(true)
                .help("Set the rocksdb path"),
        )
        .arg(
            Arg::with_name("raftdb")
                .long("raftdb")
                .takes_value(true)
                .help("Set the raft rocksdb path"),
        )
        .arg(
            Arg::with_name("skip-paranoid-checks")
                .required(false)
                .long("skip-paranoid-checks")
                .takes_value(false)
                .help("Skip paranoid checks when open rocksdb"),
        )
        .arg(
            Arg::with_name("config")
                .long("config")
                .takes_value(true)
                .help("Set the config for rocksdb"),
        )
        .arg(
            Arg::with_name("host")
                .long("host")
                .takes_value(true)
                .help("Set the remote host"),
        )
        .arg(
            Arg::with_name("ca_path")
                .required(false)
                .long("ca-path")
                .takes_value(true)
                .help("Set the CA certificate path"),
        )
        .arg(
            Arg::with_name("cert_path")
                .required(false)
                .long("cert-path")
                .takes_value(true)
                .help("Set the certificate path"),
        )
        .arg(
            Arg::with_name("key_path")
                .required(false)
                .long("key-path")
                .takes_value(true)
                .help("Set the private key path"),
        )
        .arg(
            Arg::with_name("hex-to-escaped")
                .conflicts_with("escaped-to-hex")
                .long("to-escaped")
                .takes_value(true)
                .help("Convert a hex key to escaped key"),
        )
        .arg(
            Arg::with_name("escaped-to-hex")
                .conflicts_with("hex-to-escaped")
                .long("to-hex")
                .takes_value(true)
                .help("Convert an escaped key to hex key"),
        )
        .arg(
            Arg::with_name("decode")
                .conflicts_with_all(&["hex-to-escaped", "escaped-to-hex"])
                .long("decode")
                .takes_value(true)
                .help("Decode a key in escaped format"),
        )
        .arg(
            Arg::with_name("encode")
                .conflicts_with_all(&["hex-to-escaped", "escaped-to-hex"])
                .long("encode")
                .takes_value(true)
                .help("Encode a key in escaped format"),
        )
        .arg(
            Arg::with_name("pd")
                .long("pd")
                .takes_value(true)
                .help("Set the address of pd"),
        )
        .subcommand(
            SubCommand::with_name("raft")
                .about("Print a raft log entry")
                .subcommand(
                    SubCommand::with_name("log")
                        .about("Print the raft log entry info")
                        .arg(
                            Arg::with_name("region")
                                .required_unless("key")
                                .conflicts_with("key")
                                .short("r")
                                .takes_value(true)
                                .help("Set the region id"),
                        )
                        .arg(
                            Arg::with_name("index")
                                .required_unless("key")
                                .conflicts_with("key")
                                .short("i")
                                .takes_value(true)
                                .help("Set the raft log index"),
                        )
                        .arg(
                            Arg::with_name("key")
                                .required_unless_one(&["region", "index"])
                                .conflicts_with_all(&["region", "index"])
                                .short("k")
                                .takes_value(true)
                                .help(raw_key_hint)
                        ),
                )
                .subcommand(
                    SubCommand::with_name("region")
                        .about("print region info")
                        .arg(
                            Arg::with_name("region")
                                .short("r")
                                .takes_value(true)
                                .help("Set the region id, if not specified, print all regions"),
                        )
                        .arg(
                            Arg::with_name("skip-tombstone")
                                .long("skip-tombstone")
                                .takes_value(false)
                                .help("Skip tombstone regions"),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("size")
                .about("Print region size")
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .takes_value(true)
                        .help("Set the region id, if not specified, print all regions"),
                )
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value("default,write,lock")
                        .help("Set the cf name, if not specified, print all cf"),
                ),
        )
        .subcommand(
            SubCommand::with_name("scan")
                .about("Print the range db range")
                .arg(
                    Arg::with_name("from")
                        .required(true)
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("limit")
                        .long("limit")
                        .takes_value(true)
                        .help("Set the scan limit"),
                )
                .arg(
                    Arg::with_name("start_ts")
                        .long("start-ts")
                        .takes_value(true)
                        .help("Set the scan start_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .long("commit-ts")
                        .takes_value(true)
                        .help("Set the scan commit_ts as filter"),
                )
                .arg(
                    Arg::with_name("show-cf")
                        .long("show-cf")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(CF_DEFAULT)
                        .help("Column family names, combined from default/lock/write"),
                ),
        )
        .subcommand(
            SubCommand::with_name("raw-scan")
                .about("Print all raw keys in the range")
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .default_value("")
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .default_value("")
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("limit")
                        .long("limit")
                        .takes_value(true)
                        .default_value("30")
                        .help("Limit the number of keys to scan")
                )
                .arg(
                    Arg::with_name("cf")
                        .long("cf")
                        .takes_value(true)
                        .default_value("default")
                        .possible_values(&[
                            "default", "lock", "write"
                        ])
                        .help("The column family name.")
                )
        )
        .subcommand(
            SubCommand::with_name("print")
                .about("Print the raw value")
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .default_value(CF_DEFAULT)
                        .possible_values(&[
                            "default", "lock", "write"
                        ])
                        .help("The column family name.")
                )
                .arg(
                    Arg::with_name("key")
                        .required(true)
                        .short("k")
                        .takes_value(true)
                        .help(raw_key_hint)
                ),
        )
        .subcommand(
            SubCommand::with_name("mvcc")
                .about("Print the mvcc value")
                .arg(
                    Arg::with_name("key")
                        .required(true)
                        .short("k")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("show-cf")
                        .long("show-cf")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(CF_DEFAULT)
                        .help("Column family names, combined from default/lock/write"),
                )
                .arg(
                    Arg::with_name("start_ts")
                        .long("start-ts")
                        .takes_value(true)
                        .help("Set start_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .long("commit-ts")
                        .takes_value(true)
                        .help("Set commit_ts as filter"),
                ),
        )
        .subcommand(
            SubCommand::with_name("diff")
                .about("Calculate difference of region keys from different dbs")
                .arg(
                    Arg::with_name("region")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("Specify region id"),
                )
                .arg(
                    Arg::with_name("to_db")
                        .required_unless("to_host")
                        .conflicts_with("to_host")
                        .long("to-db")
                        .takes_value(true)
                        .help("To which db path"),
                )
                .arg(
                    Arg::with_name("to_host")
                        .required_unless("to_db")
                        .conflicts_with("to_db")
                        .long("to-host")
                        .takes_value(true)
                        .conflicts_with("to_db")
                        .help("To which remote host"),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact")
                .about("Compact a column family in a specified range")
                .arg(
                    Arg::with_name("db")
                        .short("d")
                        .takes_value(true)
                        .default_value("kv")
                        .possible_values(&[
                            "kv", "raft",
                        ])
                        .help("Which db to compact"),
                )
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .default_value(CF_DEFAULT)
                        .possible_values(&[
                            "default", "lock", "write"
                        ])
                        .help("The column family name"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("threads")
                        .short("n")
                        .long("threads")
                        .takes_value(true)
                        .default_value("8")
                        .help("Number of threads in one compaction")
                )
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .long("region")
                        .takes_value(true)
                        .help("Set the region id"),
                )
                .arg(
                    Arg::with_name("bottommost")
                        .short("b")
                        .long("bottommost")
                        .takes_value(true)
                        .default_value("default")
                        .possible_values(&["skip", "force", "default"])
                        .help("Set how to compact the bottommost level"),
                ),
        )
        .subcommand(
            SubCommand::with_name("tombstone")
                .about("Set some regions on the node to tombstone by manual")
                .arg(
                    Arg::with_name("regions")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("The target regions, separated with commas if multiple"),
                )
                .arg(
                    Arg::with_name("pd")
                        .short("p")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("PD endpoints"),
                )
                .arg(
                    Arg::with_name("force")
                        .long("force")
                        .takes_value(false)
                        .help("force execute without pd"),
                ),
        )
        .subcommand(
            SubCommand::with_name("recover-mvcc")
                .about("Recover mvcc data on one node by deleting corrupted keys")
                .arg(
                    Arg::with_name("all")
                        .short("a")
                        .long("all")
                        .takes_value(false)
                        .help("Recover the whole db"),
                )
                .arg(
                    Arg::with_name("regions")
                        .required_unless("all")
                        .conflicts_with("all")
                        .short("r")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("The target regions, separated with commas if multiple"),
                )
                .arg(
                    Arg::with_name("pd")
                        .required_unless("all")
                        .short("p")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("PD endpoints"),
                )
                .arg(
                    Arg::with_name("threads")
                        .long("threads")
                        .takes_value(true)
                        .default_value_if("all", None, "4")
                        .requires("all")
                        .help("The number of threads to do recover, only for --all mode"),
                )
                .arg(
                    Arg::with_name("read-only")
                        .short("R")
                        .long("read-only")
                        .help("Skip write RocksDB"),
                ),
        )
        .subcommand(
            SubCommand::with_name("unsafe-recover")
                .about("Unsafely recover the cluster when the majority replicas are failed")
                .subcommand(
                    SubCommand::with_name("remove-fail-stores")
                        .arg(
                            Arg::with_name("stores")
                                .required(true)
                                .short("s")
                                .takes_value(true)
                                .multiple(true)
                                .use_delimiter(true)
                                .require_delimiter(true)
                                .value_delimiter(",")
                                .help("Stores to be removed"),
                        )
                        .arg(
                            Arg::with_name("regions")
                                .required_unless("all-regions")
                                .conflicts_with("all-regions")
                                .takes_value(true)
                                .short("r")
                                .multiple(true)
                                .use_delimiter(true)
                                .require_delimiter(true)
                                .value_delimiter(",")
                                .help("Only for these regions"),
                        )
                        .arg(
                            Arg::with_name("all-regions")
                                .required_unless("regions")
                                .conflicts_with("regions")
                                .long("all-regions")
                                .takes_value(false)
                                .help("Do the command for all regions"),
                        )
                ),
        )
        .subcommand(
            SubCommand::with_name("recreate-region")
                .about("Recreate a region with given metadata, but alloc new id for it")
                .arg(
                    Arg::with_name("pd")
                        .required(true)
                        .short("p")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("PD endpoints"),
                )
                .arg(
                    Arg::with_name("region")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("The origin region id"),
                ),
        )
        .subcommand(
            SubCommand::with_name("metrics")
                .about("Print the metrics")
                .arg(
                    Arg::with_name("tag")
                        .short("t")
                        .long("tag")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(METRICS_PROMETHEUS)
                        .possible_values(&[
                            "prometheus", "jemalloc", "rocksdb_raft", "rocksdb_kv",
                        ])
                        .help(
                            "Set the metrics tag, one of prometheus/jemalloc/rocksdb_raft/rocksdb_kv, if not specified, print prometheus",
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("consistency-check")
                .about("Force a consistency-check for a specified region")
                .arg(
                    Arg::with_name("region")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("The target region"),
                ),
        )
        .subcommand(SubCommand::with_name("bad-regions").about("Get all regions with corrupt raft"))
        .subcommand(
            SubCommand::with_name("modify-tikv-config")
                .about("Modify tikv config, eg. tikv-ctl --host ip:port modify-tikv-config -n rocksdb.defaultcf.disable-auto-compactions -v true")
                .arg(
                    Arg::with_name("config_name")
                        .required(true)
                        .short("n")
                        .takes_value(true)
                        .help("The config name are same as the name used on config file, eg. raftstore.messages-per-tick, raftdb.max-background-jobs"),
                )
                .arg(
                    Arg::with_name("config_value")
                        .required(true)
                        .short("v")
                        .takes_value(true)
                        .help("The config value, eg. 8, true, 1h, 8MB"),
                ),
        )
        .subcommand(
            SubCommand::with_name("dump-snap-meta")
                .about("Dump snapshot meta file")
                .arg(
                    Arg::with_name("file")
                        .required(true)
                        .short("f")
                        .long("file")
                        .takes_value(true)
                        .help("Output meta file path"),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact-cluster")
                .about("Compact the whole cluster in a specified range in one or more column families")
                .arg(
                    Arg::with_name("db")
                        .short("d")
                        .takes_value(true)
                        .default_value("kv")
                        .possible_values(&["kv", "raft"])
                        .help("The db to use"),
                )
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .default_value(CF_DEFAULT)
                        .possible_values(&["default", "lock", "write"])
                        .help("Column family names, for kv db, combine from default/lock/write; for raft db, can only be default"),
                )
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .long("from")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .long("to")
                        .takes_value(true)
                        .help(raw_key_hint)
                )
                .arg(
                    Arg::with_name("threads")
                        .short("n")
                        .long("threads")
                        .takes_value(true)
                        .default_value("8")
                        .help("Number of threads in one compaction")
                )
                .arg(
                    Arg::with_name("bottommost")
                        .short("b")
                        .long("bottommost")
                        .takes_value(true)
                        .default_value("default")
                        .possible_values(&["skip", "force", "default"])
                        .help("How to compact the bottommost level"),
                ),
        )
        .subcommand(
            SubCommand::with_name("region-properties")
                .about("Show region properties")
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .required(true)
                        .takes_value(true)
                        .help("The target region id"),
                ),
        )
        .subcommand(
            SubCommand::with_name("range-properties")
                .about("Show range properties")
                .arg(
                    Arg::with_name("start")
                        .long("start")
                        .required(true)
                        .takes_value(true)
                        .default_value("")
                        .help("hex start key"),
                )
                .arg(
                    Arg::with_name("end")
                        .long("end")
                        .required(true)
                        .takes_value(true)
                        .default_value("")
                        .help("hex end key"),
                ),
        )
        .subcommand(
            SubCommand::with_name("split-region")
                .about("Split the region")
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .required(true)
                        .takes_value(true)
                        .help("The target region id")
                )
                .arg(
                    Arg::with_name("key")
                        .short("k")
                        .required(true)
                        .takes_value(true)
                        .help("The key to split it, in unencoded escaped format")
                ),
        )
        .subcommand(
            SubCommand::with_name("fail")
                .about("Inject failures to TiKV and recovery")
                .subcommand(
                    SubCommand::with_name("inject")
                        .about("Inject failures")
                        .arg(
                            Arg::with_name("args")
                                .multiple(true)
                                .takes_value(true)
                                .help(
                                    "Inject fail point and actions pairs.\
                                E.g. tikv-ctl fail inject a=off b=panic",
                                ),
                        )
                        .arg(
                            Arg::with_name("file")
                                .short("f")
                                .takes_value(true)
                                .help("Read a file of fail points and actions to inject"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("recover")
                        .about("Recover failures")
                        .arg(
                            Arg::with_name("args")
                                .multiple(true)
                                .takes_value(true)
                                .help("Recover fail points. Eg. tikv-ctl fail recover a b"),
                        )
                        .arg(
                            Arg::with_name("file")
                                .short("f")
                                .takes_value(true)
                                .help("Recover from a file of fail points"),
                        ),
                )
                .subcommand(SubCommand::with_name("list").about("List all fail points"))
        )
        .subcommand(
            SubCommand::with_name("store")
                .about("Print the store id"),
        )
        .subcommand(
            SubCommand::with_name("cluster")
                .about("Print the cluster id"),
        )
        .subcommand(
            SubCommand::with_name("decrypt-file")
                .about("Decrypt an encrypted file")
                .arg(
                    Arg::with_name("file")
                        .long("file")
                        .takes_value(true)
                        .required(true)
                        .help("input file path"),
                )
                .arg(
                    Arg::with_name("out-file")
                        .long("out-file")
                        .takes_value(true)
                        .required(true)
                        .help("output file path"),
                ),
        )
        .subcommand(
            SubCommand::with_name("encryption-meta")
                .about("Dump encryption metadata")
                .subcommand(
                    SubCommand::with_name("dump-key")
                        .about("Dump data keys")
                        .arg(
                            Arg::with_name("ids")
                                .long("ids")
                                .takes_value(true)
                                .use_delimiter(true)
                                .help("List of data key ids. Dump all keys if not provided."),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("dump-file")
                        .about("Dump file encryption info")
                        .arg(
                            Arg::with_name("path")
                                .long("path")
                                .takes_value(true)
                                .help("Path to the file. Dump for all files if not provided."),
                        ),
                ),
        );

    let matches = app.clone().get_matches();

    // Initialize configuration and security manager.
    let cfg_path = matches.value_of("config");
    let cfg = cfg_path.map_or_else(TiKvConfig::default, |path| {
        let s = fs::read_to_string(&path).unwrap();
        toml::from_str(&s).unwrap()
    });
    let mgr = new_security_mgr(&matches);

    // Bypass the ldb command to RocksDB.
    if let Some(cmd) = matches.subcommand_matches("ldb") {
        run_ldb_command(&cmd, &cfg);
        return;
    }

    // Deal with subcommand dump-snap-meta. This subcommand doesn't require other args, so process
    // it before checking args.
    if let Some(matches) = matches.subcommand_matches("dump-snap-meta") {
        let path = matches.value_of("file").unwrap();
        return dump_snap_meta_file(path);
    }

    if matches.args.is_empty() {
        let _ = app.print_help();
        v1!("");
        return;
    }

    // Deal with arguments about key utils.
    if let Some(hex) = matches.value_of("hex-to-escaped") {
        v1!("{}", escape(&from_hex(hex).unwrap()));
        return;
    } else if let Some(escaped) = matches.value_of("escaped-to-hex") {
        v1!("{}", log_wrappers::hex_encode_upper(unescape(escaped)));
        return;
    } else if let Some(encoded) = matches.value_of("decode") {
        match Key::from_encoded(unescape(encoded)).into_raw() {
            Ok(k) => v1!("{}", escape(&k)),
            Err(e) => ve1!("decode meets error: {}", e),
        };
        return;
    } else if let Some(decoded) = matches.value_of("encode") {
        v1!("{}", Key::from_raw(&unescape(decoded)));
        return;
    }

    if let Some(matches) = matches.subcommand_matches("decrypt-file") {
        let message = "This action will expose sensitive data as plaintext on persistent storage";
        if !warning_prompt(message) {
            return;
        }
        let infile = matches.value_of("file").unwrap();
        let outfile = matches.value_of("out-file").unwrap();
        v1!("infile: {}, outfile: {}", infile, outfile);

        let key_manager =
            match data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
                .expect("data_key_manager_from_config should success")
            {
                Some(mgr) => mgr,
                None => {
                    v1!("Encryption is disabled");
                    v1!("crc32: {}", calc_crc32(infile).unwrap());
                    return;
                }
            };

        let infile1 = Path::new(infile).canonicalize().unwrap();
        let file_info = key_manager.get_file(infile1.to_str().unwrap()).unwrap();

        let mthd = encryption_method_from_db_encryption_method(file_info.method);
        if mthd == EncryptionMethod::Plaintext {
            v1!(
                "{} is not encrypted, skip to decrypt it into {}",
                infile,
                outfile
            );
            v1!("crc32: {}", calc_crc32(infile).unwrap());
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
        v1!("crc32: {}", calc_crc32(outfile).unwrap());
        return;
    }

    if let Some(matches) = matches.subcommand_matches("encryption-meta") {
        match matches.subcommand() {
            ("dump-key", Some(matches)) => {
                let message =
                    "This action will expose encryption key(s) as plaintext. Do not output the \
                    result in file on disk.";
                if !warning_prompt(message) {
                    return;
                }
                DataKeyManager::dump_key_dict(
                    create_backend(&cfg.security.encryption.master_key)
                        .expect("encryption-meta master key creation"),
                    &cfg.storage.data_dir,
                    matches
                        .values_of("ids")
                        .map(|ids| ids.map(|id| id.parse::<u64>().unwrap()).collect()),
                )
                .unwrap();
            }
            ("dump-file", Some(matches)) => {
                let path = matches
                    .value_of("path")
                    .map(|path| fs::canonicalize(path).unwrap().to_str().unwrap().to_owned());
                DataKeyManager::dump_file_dict(&cfg.storage.data_dir, path.as_deref()).unwrap();
            }
            _ => ve1!("{}", matches.usage()),
        }
        return;
    }

    // Deal with all subcommands needs PD.
    if let Some(pd) = matches.value_of("pd") {
        let pd_client = get_pd_rpc_client(pd, Arc::clone(&mgr));
        if let Some(matches) = matches.subcommand_matches("compact-cluster") {
            let db = matches.value_of("db").unwrap();
            let db_type = if db == "kv" { DBType::Kv } else { DBType::Raft };
            let cfs = matches.values_of("cf").unwrap().collect();
            let from_key = matches.value_of("from").map(|k| unescape(k));
            let to_key = matches.value_of("to").map(|k| unescape(k));
            let threads = value_t_or_exit!(matches.value_of("threads"), u32);
            let bottommost = BottommostLevelCompaction::from(matches.value_of("bottommost"));
            return compact_whole_cluster(
                &pd_client, &cfg, mgr, db_type, cfs, from_key, to_key, threads, bottommost,
            );
        }
        if let Some(matches) = matches.subcommand_matches("split-region") {
            let region_id = value_t_or_exit!(matches.value_of("region"), u64);
            let key = unescape(matches.value_of("key").unwrap());
            return split_region(&pd_client, mgr, region_id, key);
        }

        let _ = app.print_help();
        return;
    }

    // Deal with all subcommands about db or host.
    let db = matches.value_of("db");
    let skip_paranoid_checks = matches.is_present("skip-paranoid-checks");
    let raft_db = matches.value_of("raftdb");
    let host = matches.value_of("host");

    let debug_executor = new_debug_executor(
        db,
        raft_db,
        skip_paranoid_checks,
        host,
        &cfg,
        Arc::clone(&mgr),
    );

    if let Some(matches) = matches.subcommand_matches("print") {
        let cf = matches.value_of("cf").unwrap();
        let key = unescape(matches.value_of("key").unwrap());
        debug_executor.dump_value(cf, key);
    } else if let Some(matches) = matches.subcommand_matches("raft") {
        if let Some(matches) = matches.subcommand_matches("log") {
            let (id, index) = if let Some(key) = matches.value_of("key") {
                keys::decode_raft_log_key(&unescape(key)).unwrap()
            } else {
                let id = matches.value_of("region").unwrap().parse().unwrap();
                let index = matches.value_of("index").unwrap().parse().unwrap();
                (id, index)
            };
            debug_executor.dump_raft_log(id, index);
        } else if let Some(matches) = matches.subcommand_matches("region") {
            let skip_tombstone = matches.is_present("skip-tombstone");
            if let Some(id) = matches.value_of("region") {
                debug_executor.dump_region_info(id.parse().unwrap(), skip_tombstone);
            } else {
                debug_executor.dump_all_region_info(skip_tombstone);
            }
        } else {
            let _ = app.print_help();
        }
    } else if let Some(matches) = matches.subcommand_matches("size") {
        let cfs = matches.values_of("cf").unwrap().collect();
        if let Some(id) = matches.value_of("region") {
            debug_executor.dump_region_size(id.parse().unwrap(), cfs);
        } else {
            debug_executor.dump_all_region_size(cfs);
        }
    } else if let Some(matches) = matches.subcommand_matches("scan") {
        let from = unescape(matches.value_of("from").unwrap());
        let to = matches
            .value_of("to")
            .map_or_else(Vec::new, |to| unescape(to));
        let limit = matches
            .value_of("limit")
            .map_or(0, |s| s.parse().expect("parse u64"));
        if to.is_empty() && limit == 0 {
            ve1!(r#"please pass "to" or "limit""#);
            process::exit(-1);
        }
        let cfs = matches.values_of("show-cf").unwrap().collect();
        let start_ts = matches.value_of("start_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        debug_executor.dump_mvccs_infos(from, to, limit, cfs, start_ts, commit_ts);
    } else if let Some(matches) = matches.subcommand_matches("raw-scan") {
        let from = unescape(matches.value_of("from").unwrap());
        let to = unescape(matches.value_of("to").unwrap());
        let limit: usize = matches.value_of("limit").unwrap().parse().unwrap();
        let cf = matches.value_of("cf").unwrap();
        debug_executor.raw_scan(&from, &to, limit, cf);
    } else if let Some(matches) = matches.subcommand_matches("mvcc") {
        let from = unescape(matches.value_of("key").unwrap());
        let cfs = matches.values_of("show-cf").unwrap().collect();
        let start_ts = matches.value_of("start_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        debug_executor.dump_mvccs_infos(from, vec![], 0, cfs, start_ts, commit_ts);
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let region = matches.value_of("region").unwrap().parse().unwrap();
        let to_db = matches.value_of("to_db");
        let to_host = matches.value_of("to_host");
        debug_executor.diff_region(region, to_db, None, to_host, &cfg, mgr);
    } else if let Some(matches) = matches.subcommand_matches("compact") {
        let db = matches.value_of("db").unwrap();
        let db_type = if db == "kv" { DBType::Kv } else { DBType::Raft };
        let cf = matches.value_of("cf").unwrap();
        let from_key = matches.value_of("from").map(|k| unescape(k));
        let to_key = matches.value_of("to").map(|k| unescape(k));
        let threads = value_t_or_exit!(matches.value_of("threads"), u32);
        let bottommost = BottommostLevelCompaction::from(matches.value_of("bottommost"));
        if let Some(region) = matches.value_of("region") {
            debug_executor.compact_region(
                host,
                db_type,
                cf,
                region.parse().unwrap(),
                threads,
                bottommost,
            );
        } else {
            debug_executor.compact(host, db_type, cf, from_key, to_key, threads, bottommost);
        }
    } else if let Some(matches) = matches.subcommand_matches("tombstone") {
        let regions = matches
            .values_of("regions")
            .unwrap()
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse regions fail");
        if let Some(pd_urls) = matches.values_of("pd") {
            let pd_urls = pd_urls.map(ToOwned::to_owned).collect();
            let cfg = PdConfig {
                endpoints: pd_urls,
                ..Default::default()
            };
            if let Err(e) = cfg.validate() {
                panic!("invalid pd configuration: {:?}", e);
            }
            debug_executor.set_region_tombstone_after_remove_peer(mgr, &cfg, regions);
        } else {
            assert!(matches.is_present("force"));
            debug_executor.set_region_tombstone_force(regions);
        }
    } else if let Some(matches) = matches.subcommand_matches("recover-mvcc") {
        let read_only = matches.is_present("read-only");
        if matches.is_present("all") {
            let threads = matches
                .value_of("threads")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            if threads == 0 {
                panic!("Number of threads can't be 0");
            }
            v1!(
                "Recover all, threads: {}, read_only: {}",
                threads,
                read_only
            );
            debug_executor.recover_mvcc_all(threads, read_only);
        } else {
            let regions = matches
                .values_of("regions")
                .unwrap()
                .map(str::parse)
                .collect::<Result<Vec<_>, _>>()
                .expect("parse regions fail");
            let pd_urls = matches
                .values_of("pd")
                .unwrap()
                .map(ToOwned::to_owned)
                .collect();
            let mut cfg = PdConfig::default();
            v1!(
                "Recover regions: {:?}, pd: {:?}, read_only: {}",
                regions,
                pd_urls,
                read_only
            );
            cfg.endpoints = pd_urls;
            if let Err(e) = cfg.validate() {
                panic!("invalid pd configuration: {:?}", e);
            }
            debug_executor.recover_regions_mvcc(mgr, &cfg, regions, read_only);
        }
    } else if let Some(matches) = matches.subcommand_matches("unsafe-recover") {
        if let Some(matches) = matches.subcommand_matches("remove-fail-stores") {
            let store_ids = values_t!(matches, "stores", u64).expect("parse stores fail");
            let region_ids = matches.values_of("regions").map(|ids| {
                ids.map(str::parse)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("parse regions fail")
            });
            debug_executor.remove_fail_stores(store_ids, region_ids);
        } else {
            ve1!("{}", matches.usage());
        }
    } else if let Some(matches) = matches.subcommand_matches("recreate-region") {
        let pd_cfg = PdConfig {
            endpoints: matches
                .values_of("pd")
                .unwrap()
                .map(ToOwned::to_owned)
                .collect(),
            ..Default::default()
        };
        let region_id = matches.value_of("region").unwrap().parse().unwrap();
        debug_executor.recreate_region(mgr, &pd_cfg, region_id);
    } else if let Some(matches) = matches.subcommand_matches("consistency-check") {
        let region_id = matches.value_of("region").unwrap().parse().unwrap();
        debug_executor.check_region_consistency(region_id);
    } else if matches.subcommand_matches("bad-regions").is_some() {
        debug_executor.print_bad_regions();
    } else if let Some(matches) = matches.subcommand_matches("modify-tikv-config") {
        let config_name = matches.value_of("config_name").unwrap();
        let config_value = matches.value_of("config_value").unwrap();
        debug_executor.modify_tikv_config(config_name, config_value);
    } else if let Some(matches) = matches.subcommand_matches("metrics") {
        let tags = matches.values_of("tag").unwrap().collect();
        debug_executor.dump_metrics(tags)
    } else if let Some(matches) = matches.subcommand_matches("region-properties") {
        let region_id = value_t_or_exit!(matches.value_of("region"), u64);
        debug_executor.dump_region_properties(region_id)
    } else if let Some(matches) = matches.subcommand_matches("range-properties") {
        let start_key = from_hex(matches.value_of("start").unwrap()).unwrap();
        let end_key = from_hex(matches.value_of("end").unwrap()).unwrap();
        debug_executor.dump_range_properties(start_key, end_key);
    } else if let Some(matches) = matches.subcommand_matches("fail") {
        if host.is_none() {
            ve1!("command fail requires host");
            process::exit(-1);
        }
        let client = new_debug_client(host.unwrap(), mgr);
        if let Some(matches) = matches.subcommand_matches("inject") {
            let mut list = matches
                .value_of("file")
                .map_or_else(Vec::new, read_fail_file);
            if let Some(ps) = matches.values_of("args") {
                for pair in ps {
                    let mut parts = pair.split('=');
                    list.push((
                        parts.next().unwrap().to_owned(),
                        parts.next().unwrap_or("").to_owned(),
                    ))
                }
            }
            for (name, actions) in list {
                if actions.is_empty() {
                    v1!("No action for fail point {}", name);
                    continue;
                }
                let mut inject_req = InjectFailPointRequest::default();
                inject_req.set_name(name);
                inject_req.set_actions(actions);

                let option = CallOption::default().timeout(Duration::from_secs(10));
                client.inject_fail_point_opt(&inject_req, option).unwrap();
            }
        } else if let Some(matches) = matches.subcommand_matches("recover") {
            let mut list = matches
                .value_of("file")
                .map_or_else(Vec::new, read_fail_file);
            if let Some(fps) = matches.values_of("args") {
                for fp in fps {
                    list.push((fp.to_owned(), "".to_owned()))
                }
            }
            for (name, _) in list {
                let mut recover_req = RecoverFailPointRequest::default();
                recover_req.set_name(name);
                let option = CallOption::default().timeout(Duration::from_secs(10));
                client.recover_fail_point_opt(&recover_req, option).unwrap();
            }
        } else if matches.is_present("list") {
            let list_req = ListFailPointsRequest::default();
            let option = CallOption::default().timeout(Duration::from_secs(10));
            let resp = client.list_fail_points_opt(&list_req, option).unwrap();
            v1!("{:?}", resp.get_entries());
        }
    } else if matches.subcommand_matches("store").is_some() {
        debug_executor.dump_store_info();
    } else if matches.subcommand_matches("cluster").is_some() {
        debug_executor.dump_cluster_info();
    } else {
        let _ = app.print_help();
    }
}

fn from_hex(key: &str) -> Result<Vec<u8>, hex::FromHexError> {
    if key.starts_with("0x") || key.starts_with("0X") {
        return hex::decode(&key[2..]);
    }
    hex::decode(key)
}

fn convert_gbmb(mut bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    if bytes < MB {
        return format!("{} B", bytes);
    }
    let mb = if bytes % GB == 0 {
        String::from("")
    } else {
        format!("{:.3} MB", (bytes % GB) as f64 / MB as f64)
    };
    bytes /= GB;
    let gb = if bytes == 0 {
        String::from("")
    } else {
        format!("{} GB ", bytes)
    };
    format!("{}{}", gb, mb)
}

fn new_security_mgr(matches: &ArgMatches<'_>) -> Arc<SecurityManager> {
    let ca_path = matches.value_of("ca_path");
    let cert_path = matches.value_of("cert_path");
    let key_path = matches.value_of("key_path");

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
        v1!(
            "cf {}, size {}, checksum: {}",
            cf_file.cf,
            cf_file.size,
            cf_file.checksum
        );
    }
}

fn get_pd_rpc_client(pd: &str, mgr: Arc<SecurityManager>) -> RpcClient {
    let mut cfg = PdConfig::default();
    cfg.endpoints.push(pd.to_owned());
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
        ve1!("split_region internal error: {:?}", resp.get_region_error());
        return;
    }

    v1!(
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
        let cfs: Vec<String> = cfs.iter().map(|cf| (*cf).to_string()).collect();
        let h = thread::Builder::new()
            .name(format!("compact-{}", addr))
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let debug_executor = new_debug_executor(None, None, false, Some(&addr), &cfg, mgr);
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

fn run_ldb_command(cmd: &ArgMatches<'_>, cfg: &TiKvConfig) {
    let mut args: Vec<String> = match cmd.values_of("") {
        Some(v) => v.map(ToOwned::to_owned).collect(),
        None => Vec::new(),
    };
    args.insert(0, "ldb".to_owned());
    let key_manager = data_key_manager_from_config(&cfg.security.encryption, &cfg.storage.data_dir)
        .unwrap()
        .map(Arc::new);
    let env = get_env(key_manager, None).unwrap();
    let mut opts = cfg.rocksdb.build_opt();
    opts.set_env(env);

    engine_rocks::raw::run_ldb_tool(&args, &opts);
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
