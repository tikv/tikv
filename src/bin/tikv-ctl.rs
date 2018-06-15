// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value, cyclomatic_complexity))]

#[macro_use]
extern crate clap;
extern crate futures;
extern crate grpcio;
extern crate kvproto;
extern crate protobuf;
extern crate raft;
extern crate rocksdb;
extern crate rustc_serialize;
extern crate tikv;
extern crate toml;

use rustc_serialize::hex::{FromHex, FromHexError, ToHex};
use std::cmp::Ordering;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::iter::FromIterator;
use std::sync::Arc;
use std::thread;
use std::{process, str, u64};

use clap::{App, Arg, ArgMatches, SubCommand};
use futures::{future, stream, Future, Stream};
use grpcio::{ChannelBuilder, Environment};
use protobuf::Message;
use protobuf::RepeatedField;

use kvproto::debugpb::DB as DBType;
use kvproto::debugpb::*;
use kvproto::debugpb_grpc::DebugClient;
use kvproto::kvrpcpb::MvccInfo;
use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::{PeerState, SnapshotMeta};
use raft::eraftpb::{ConfChange, Entry, EntryType};
use tikv::config::TiKvConfig;
use tikv::pd::{Config as PdConfig, PdClient, RpcClient};
use tikv::raftstore::store::{keys, Engines};
use tikv::server::debug::{Debugger, RegionInfo};
use tikv::storage::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use tikv::util::rocksdb as rocksdb_util;
use tikv::util::security::{SecurityConfig, SecurityManager};
use tikv::util::{escape, unescape};

const METRICS_PROMETHEUS: &str = "prometheus";
const METRICS_ROCKSDB_KV: &str = "rocksdb_kv";
const METRICS_ROCKSDB_RAFT: &str = "rocksdb_raft";
const METRICS_JEMALLOC: &str = "jemalloc";

fn perror_and_exit<E: Error>(prefix: &str, e: E) -> ! {
    eprintln!("{}: {}", prefix, e);
    process::exit(-1);
}

fn new_debug_executor(
    db: Option<&str>,
    raft_db: Option<&str>,
    host: Option<&str>,
    cfg_path: Option<&str>,
    mgr: Arc<SecurityManager>,
) -> Box<DebugExecutor> {
    match (host, db) {
        (None, Some(kv_path)) => {
            let cfg = cfg_path.map_or_else(TiKvConfig::default, |path| {
                File::open(&path)
                    .and_then(|mut f| {
                        let mut s = String::new();
                        f.read_to_string(&mut s).unwrap();
                        let c = toml::from_str(&s).unwrap();
                        Ok(c)
                    })
                    .unwrap()
            });
            let kv_db_opts = cfg.rocksdb.build_opt();
            let kv_cfs_opts = cfg.rocksdb.build_cf_opts();
            let kv_db = rocksdb_util::new_engine_opt(kv_path, kv_db_opts, kv_cfs_opts).unwrap();

            let raft_path = raft_db
                .map(|p| p.to_string())
                .unwrap_or_else(|| format!("{}/../raft", kv_path));
            let raft_db_opts = cfg.raftdb.build_opt();
            let raft_db_cf_opts = cfg.raftdb.build_cf_opts();
            let raft_db =
                rocksdb_util::new_engine_opt(&raft_path, raft_db_opts, raft_db_cf_opts).unwrap();

            Box::new(Debugger::new(Engines::new(
                Arc::new(kv_db),
                Arc::new(raft_db),
            ))) as Box<DebugExecutor>
        }
        (Some(remote), None) => {
            let env = Arc::new(Environment::new(1));
            let cb = ChannelBuilder::new(env);
            let channel = mgr.connect(cb, remote);
            let client = DebugClient::new(channel);
            Box::new(client) as Box<DebugExecutor>
        }
        _ => unreachable!(),
    }
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
        let regions = self.get_all_meta_regions();
        let regions_number = regions.len();
        let mut total_size = 0;
        for region in regions {
            total_size += self.dump_region_size(region, cfs.clone());
        }
        println!("total region number: {}", regions_number);
        println!("total region size: {}", convert_gbmb(total_size as u64));
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
        println!("region id: {}", region);
        println!("region state key: {}", escape(&region_state_key));
        println!("region state: {:?}", r.region_local_state);
        println!("raft state key: {}", escape(&raft_state_key));
        println!("raft state: {:?}", r.raft_local_state);
        println!("apply state key: {}", escape(&apply_state_key));
        println!("apply state: {:?}", r.raft_apply_state);
    }

    fn dump_all_region_info(&self, skip_tombstone: bool) {
        for region in self.get_all_meta_regions() {
            self.dump_region_info(region, skip_tombstone);
        }
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
                let mut msg = RaftCmdRequest::new();
                msg.merge_from_bytes(&data).unwrap();
                println!("Normal: {:#?}", msg);
            }
            EntryType::EntryConfChange => {
                let mut msg = ConfChange::new();
                msg.merge_from_bytes(&data).unwrap();
                let ctx = msg.take_context();
                println!("ConfChange: {:?}", msg);
                let mut cmd = RaftCmdRequest::new();
                cmd.merge_from_bytes(&ctx).unwrap();
                println!("ConfChange.RaftCmdRequest: {:#?}", cmd);
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
            eprintln!("from and to should start with \"z\"");
            process::exit(-1);
        }
        if !to.is_empty() && to < from {
            eprintln!("\"to\" must be greater than \"from\"");
            process::exit(-1);
        }

        let point_query = to.is_empty() && limit == 0;
        if point_query {
            limit = 1;
        }

        let scan_future = self.get_mvcc_infos(from.clone(), to, limit).for_each(
            move |(key, mvcc)| {
                if point_query && key != from {
                    println!("no mvcc infos for {}", escape(&from));
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
            },
        );
        if let Err(e) = scan_future.wait() {
            eprintln!("{}", e);
            process::exit(-1);
        }
    }

    fn diff_region(
        &self,
        region: u64,
        db: Option<&str>,
        raft_db: Option<&str>,
        host: Option<&str>,
        cfg_path: Option<&str>,
        mgr: Arc<SecurityManager>,
    ) {
        let rhs_debug_executor = new_debug_executor(db, raft_db, host, cfg_path, mgr);

        let r1 = self.get_region_info(region);
        let r2 = rhs_debug_executor.get_region_info(region);
        println!("region id: {}", region);
        println!("db1 region state: {:?}", r1.region_local_state);
        println!("db2 region state: {:?}", r2.region_local_state);
        println!("db1 apply state: {:?}", r1.raft_apply_state);
        println!("db2 apply state: {:?}", r2.raft_apply_state);

        match (r1.region_local_state, r2.region_local_state) {
            (None, None) => return,
            (Some(_), None) | (None, Some(_)) => {
                println!("db1 and db2 don't have same region local_state");
                return;
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
                        1 => future::poll_fn(|| mvcc_infos_1.poll()).wait(),
                        _ => future::poll_fn(|| mvcc_infos_2.poll()).wait(),
                    };
                    match wait {
                        Ok(item1) => item1,
                        Err(e) => {
                            println!("db{} scan data in region {} fail: {}", i, region, e);
                            process::exit(-1);
                        }
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
    ) {
        let from = from.unwrap_or_default();
        let to = to.unwrap_or_default();
        self.do_compaction(db, cf, &from, &to, threads);
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
    ) {
        let region_local = self.get_region_info(region_id).region_local_state.unwrap();
        let r = region_local.get_region();
        let from = keys::data_key(r.get_start_key());
        let to = keys::data_end_key(r.get_end_key());
        self.do_compaction(db, cf, &from, &to, threads);
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
            RpcClient::new(cfg, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let regions = region_ids
            .into_iter()
            .map(|region_id| {
                if let Some(region) = rpc_client
                    .get_region_by_id(region_id)
                    .wait()
                    .unwrap_or_else(|e| perror_and_exit("Get region id from PD", e))
                {
                    return region;
                }
                eprintln!("no such region in pd: {}", region_id);
                process::exit(-1);
            })
            .collect();
        self.set_region_tombstone(regions);
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
    ) {
        self.check_local_mode();
        let rpc_client =
            RpcClient::new(cfg, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let regions = region_ids
            .into_iter()
            .map(|region_id| {
                if let Some(region) = rpc_client
                    .get_region_by_id(region_id)
                    .wait()
                    .unwrap_or_else(|e| perror_and_exit("Get region id from PD", e))
                {
                    return region;
                }
                eprintln!("no such region in pd: {}", region_id);
                process::exit(-1);
            })
            .collect();
        self.recover_regions(regions);
    }

    fn get_all_meta_regions(&self) -> Vec<u64>;

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8>;

    fn get_region_size(&self, region: u64, cfs: Vec<&str>) -> Vec<(String, usize)>;

    fn get_region_info(&self, region: u64) -> RegionInfo;

    fn get_raft_log(&self, region: u64, index: u64) -> Entry;

    fn get_mvcc_infos(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u64,
    ) -> Box<Stream<Item = (Vec<u8>, MvccInfo), Error = String>>;

    fn do_compaction(&self, db: DBType, cf: &str, from: &[u8], to: &[u8], threads: u32);

    fn set_region_tombstone(&self, regions: Vec<Region>);

    fn recover_regions(&self, regions: Vec<Region>);

    fn modify_tikv_config(&self, module: MODULE, config_name: &str, config_value: &str);

    fn dump_metrics(&self, tags: Vec<&str>);

    fn dump_region_properties(&self, region_id: u64);
}

impl DebugExecutor for DebugClient {
    fn check_local_mode(&self) {
        eprintln!("This command is only for local mode");
        process::exit(-1);
    }

    fn get_all_meta_regions(&self) -> Vec<u64> {
        unimplemented!();
    }

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8> {
        let mut req = GetRequest::new();
        req.set_db(DBType::KV);
        req.set_cf(cf.to_owned());
        req.set_key(key);
        self.get(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get", e))
            .take_value()
    }

    fn get_region_size(&self, region: u64, cfs: Vec<&str>) -> Vec<(String, usize)> {
        let cfs = cfs.into_iter().map(|s| s.to_owned()).collect();
        let mut req = RegionSizeRequest::new();
        req.set_cfs(RepeatedField::from_vec(cfs));
        req.set_region_id(region);
        self.region_size(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::region_size", e))
            .take_entries()
            .into_iter()
            .map(|mut entry| (entry.take_cf(), entry.get_size() as usize))
            .collect()
    }

    fn get_region_info(&self, region: u64) -> RegionInfo {
        let mut req = RegionInfoRequest::new();
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
        let mut req = RaftLogRequest::new();
        req.set_region_id(region);
        req.set_log_index(index);
        self.raft_log(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::raft_log", e))
            .take_entry()
    }

    fn get_mvcc_infos(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u64,
    ) -> Box<Stream<Item = (Vec<u8>, MvccInfo), Error = String>> {
        let mut req = ScanMvccRequest::new();
        req.set_from_key(from);
        req.set_to_key(to);
        req.set_limit(limit);
        Box::new(
            self.scan_mvcc(&req)
                .unwrap()
                .map_err(|e| e.to_string())
                .map(|mut resp| (resp.take_key(), resp.take_info())),
        ) as Box<Stream<Item = (Vec<u8>, MvccInfo), Error = String>>
    }

    fn do_compaction(&self, db: DBType, cf: &str, from: &[u8], to: &[u8], threads: u32) {
        let mut req = CompactRequest::new();
        req.set_db(db);
        req.set_cf(cf.to_owned());
        req.set_from_key(from.to_owned());
        req.set_to_key(to.to_owned());
        req.set_threads(threads);
        self.compact(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::compact", e));
    }

    fn dump_metrics(&self, tags: Vec<&str>) {
        let mut req = GetMetricsRequest::new();
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
        unimplemented!("only avaliable for local mode");
    }

    fn recover_regions(&self, _: Vec<Region>) {
        unimplemented!("only avaliable for local mode");
    }

    fn print_bad_regions(&self) {
        unimplemented!("only avaliable for local mode");
    }

    fn remove_fail_stores(&self, _: Vec<u64>, _: Option<Vec<u64>>) {
        self.check_local_mode();
    }

    fn recreate_region(&self, _: Arc<SecurityManager>, _: &PdConfig, _: u64) {
        self.check_local_mode();
    }

    fn check_region_consistency(&self, region_id: u64) {
        let mut req = RegionConsistencyCheckRequest::new();
        req.set_region_id(region_id);
        self.check_region_consistency(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::check_region_consistency", e));
        println!("success!");
    }

    fn modify_tikv_config(&self, module: MODULE, config_name: &str, config_value: &str) {
        let mut req = ModifyTikvConfigRequest::new();
        req.set_module(module);
        req.set_config_name(config_name.to_owned());
        req.set_config_value(config_value.to_owned());
        self.modify_tikv_config(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::modify_tikv_config", e));
        println!("success");
    }

    fn dump_region_properties(&self, region_id: u64) {
        let mut req = GetRegionPropertiesRequest::new();
        req.set_region_id(region_id);
        let resp = self
            .get_region_properties(&req)
            .unwrap_or_else(|e| perror_and_exit("DebugClient::get_region_properties", e));
        for prop in resp.get_props() {
            println!("{}: {}", prop.get_name(), prop.get_value());
        }
    }
}

impl DebugExecutor for Debugger {
    fn check_local_mode(&self) {}

    fn get_all_meta_regions(&self) -> Vec<u64> {
        self.get_all_meta_regions()
            .unwrap_or_else(|e| perror_and_exit("Debugger::get_all_meta_regions", e))
    }

    fn get_value_by_key(&self, cf: &str, key: Vec<u8>) -> Vec<u8> {
        self.get(DBType::KV, cf, &key)
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

    fn get_mvcc_infos(
        &self,
        from: Vec<u8>,
        to: Vec<u8>,
        limit: u64,
    ) -> Box<Stream<Item = (Vec<u8>, MvccInfo), Error = String>> {
        let iter = self
            .scan_mvcc(&from, &to, limit)
            .unwrap_or_else(|e| perror_and_exit("Debugger::scan_mvcc", e));
        #[allow(deprecated)]
        let stream = stream::iter(iter).map_err(|e| e.to_string());
        Box::new(stream) as Box<Stream<Item = (Vec<u8>, MvccInfo), Error = String>>
    }

    fn do_compaction(&self, db: DBType, cf: &str, from: &[u8], to: &[u8], threads: u32) {
        self.compact(db, cf, from, to, threads)
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
            eprintln!("region: {}, error: {}", region_id, error);
        }
    }

    fn recover_regions(&self, regions: Vec<Region>) {
        let ret = self
            .recover_regions(regions)
            .unwrap_or_else(|e| perror_and_exit("Debugger::recover regions", e));
        if ret.is_empty() {
            println!("success!");
            return;
        }
        for (region_id, error) in ret {
            eprintln!("region: {}, error: {}", region_id, error);
        }
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

    fn remove_fail_stores(&self, store_ids: Vec<u64>, region_ids: Option<Vec<u64>>) {
        println!("removing stores {:?} from configrations...", store_ids);
        self.remove_failed_stores(store_ids, region_ids)
            .unwrap_or_else(|e| perror_and_exit("Debugger::remove_fail_stores", e));
        println!("success");
    }

    fn recreate_region(&self, mgr: Arc<SecurityManager>, pd_cfg: &PdConfig, region_id: u64) {
        let rpc_client =
            RpcClient::new(pd_cfg, mgr).unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

        let mut region = match rpc_client.get_region_by_id(region_id).wait() {
            Ok(Some(region)) => region,
            Ok(None) => {
                eprintln!("no such region {} on PD", region_id);
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
        region.mut_region_epoch().set_conf_ver(1);

        region.peers.clear();
        let mut peer = Peer::new();
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
        unimplemented!("only avaliable for online mode");
    }

    fn check_region_consistency(&self, _: u64) {
        eprintln!("only support remote mode");
        process::exit(-1);
    }

    fn modify_tikv_config(&self, _: MODULE, _: &str, _: &str) {
        eprintln!("only support remote mode");
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
}

fn main() {
    let raw_key_hint: &'static str = "raw key (generally starts with \"z\") in escaped form";

    let mut app = App::new("TiKV Ctl")
        .author("PingCAP")
        .about("Distributed transactional key value database powered by Rust and Raft")
        .arg(
            Arg::with_name("db")
                .required(true)
                .conflicts_with_all(&["host", "pd", "hex-to-escaped", "escaped-to-hex"])
                .long("db")
                .takes_value(true)
                .help("set rocksdb path"),
        )
        .arg(
            Arg::with_name("raftdb")
                .conflicts_with_all(&["host", "pd", "hex-to-escaped", "escaped-to-hex"])
                .long("raftdb")
                .takes_value(true)
                .help("set raft rocksdb path"),
        )
        .arg(
            Arg::with_name("config")
                .conflicts_with_all(&["host", "pd", "hex-to-escaped", "escaped-to-hex"])
                .long("config")
                .takes_value(true)
                .help("set config for rocksdb"),
        )
        .arg(
            Arg::with_name("host")
                .required(true)
                .conflicts_with_all(&["db", "pd", "raftdb", "hex-to-escaped", "escaped-to-hex", "config"])
                .long("host")
                .takes_value(true)
                .help("set remote host"),
        )
        .arg(
            Arg::with_name("ca_path")
                .required(false)
                .long("ca-path")
                .takes_value(true)
                .help("set CA certificate path"),
        )
        .arg(
            Arg::with_name("cert_path")
                .required(false)
                .long("cert-path")
                .takes_value(true)
                .help("set certificate path"),
        )
        .arg(
            Arg::with_name("key_path")
                .required(false)
                .long("key-path")
                .takes_value(true)
                .help("set private key path"),
        )
        .arg(
            Arg::with_name("hex-to-escaped")
                .conflicts_with("escaped-to-hex")
                .long("to-escaped")
                .takes_value(true)
                .help("convert hex key to escaped key"),
        )
        .arg(
            Arg::with_name("escaped-to-hex")
                .conflicts_with("hex-to-escaped")
                .long("to-hex")
                .takes_value(true)
                .help("convert escaped key to hex key"),
        )
        .arg(
            Arg::with_name("pd")
                .required(true)
                .long("pd")
                .conflicts_with_all(&["db", "raftdb", "host", "hex-to-escaped", "escaped-to-hex", "config"])
                .takes_value(true)
                .help("pd address"),
        )
        .subcommand(
            SubCommand::with_name("raft")
                .about("print raft log entry")
                .subcommand(
                    SubCommand::with_name("log")
                        .about("print the raft log entry info")
                        .arg(
                            Arg::with_name("region")
                                .required_unless("key")
                                .conflicts_with("key")
                                .short("r")
                                .takes_value(true)
                                .help("set the region id"),
                        )
                        .arg(
                            Arg::with_name("index")
                                .required_unless("key")
                                .conflicts_with("key")
                                .short("i")
                                .takes_value(true)
                                .help("set the raft log index"),
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
                                .help("set the region id, if not specified, print all regions."),
                        )
                        .arg(
                            Arg::with_name("skip-tombstone")
                                .long("skip-tombstone")
                                .takes_value(false)
                                .help("skip tombstone region."),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("size")
                .about("print region size")
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .takes_value(true)
                        .help("set the region id, if not specified, print all regions."),
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
                        .help("set the cf name, if not specified, print all cf."),
                ),
        )
        .subcommand(
            SubCommand::with_name("scan")
                .about("print the range db range")
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
                        .help("set the scan limit"),
                )
                .arg(
                    Arg::with_name("start_ts")
                        .long("start-ts")
                        .takes_value(true)
                        .help("set the scan start_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .long("commit-ts")
                        .takes_value(true)
                        .help("set the scan commit_ts as filter"),
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
                        .help("column family names, combined from default/lock/write"),
                ),
        )
        .subcommand(
            SubCommand::with_name("print")
                .about("print the raw value")
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .default_value(CF_DEFAULT)
                        .help("column family name"),
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
                .about("print the mvcc value")
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
                        .help("column family names, combined from default/lock/write"),
                )
                .arg(
                    Arg::with_name("start_ts")
                        .long("start-ts")
                        .takes_value(true)
                        .help("set start_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .long("commit-ts")
                        .takes_value(true)
                        .help("set commit_ts as filter"),
                ),
        )
        .subcommand(
            SubCommand::with_name("diff")
                .about("diff two region keys")
                .arg(
                    Arg::with_name("region")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("specify region id"),
                )
                .arg(
                    Arg::with_name("to_db")
                        .required_unless("to_host")
                        .conflicts_with("to_host")
                        .long("to-db")
                        .takes_value(true)
                        .help("to which db path"),
                )
                .arg(
                    Arg::with_name("to_host")
                        .required_unless("to_db")
                        .conflicts_with("to_db")
                        .long("to-host")
                        .takes_value(true)
                        .conflicts_with("to_db")
                        .help("to which remote host"),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact")
                .about("compact a column family in a specified range")
                .arg(
                    Arg::with_name("db")
                        .short("d")
                        .takes_value(true)
                        .default_value("kv")
                        .help("kv or raft"),
                )
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .default_value(CF_DEFAULT)
                        .help("column family name, only can be default/lock/write"),
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
                        .help("number of threads in one compaction")
                )
                .arg(
                    Arg::with_name("region")
                    .short("r")
                    .long("region")
                    .takes_value(true)
                    .help("set the region id"),
                ),
        )
        .subcommand(
            SubCommand::with_name("tombstone")
                .about("set a region on the node to tombstone by manual")
                .arg(
                    Arg::with_name("regions")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("the target region"),
                )
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
                ),
        )
        .subcommand(
            SubCommand::with_name("recover-mvcc")
                .about("recover mvcc data of regions on one node")
                .arg(
                    Arg::with_name("regions")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .help("the target region"),
                )
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
                ),
        )
        .subcommand(
            SubCommand::with_name("unsafe-recover")
                .about("unsafe recover the cluster when majority replicas are failed")
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
                            .help("failed store id list"),
                    )
                    .arg(
                        Arg::with_name("regions")
                            .takes_value(true)
                            .short("r")
                            .multiple(true)
                            .use_delimiter(true)
                            .require_delimiter(true)
                            .value_delimiter(",")
                            .help("only for these regions"),
                        )
                ),
        )
        .subcommand(
            SubCommand::with_name("recreate-region")
                .about("recreate a region with given metadata, but alloc new id for it")
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
                        .help("the origin region id"),
                        ),
        )
        .subcommand(
            SubCommand::with_name("metrics")
                .about("print the metrics")
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
                        .help(
                            "set the metrics tag, one of prometheus/jemalloc/rocksdb_raft/rocksdb_kv, if not specified, print prometheus",
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("consistency-check")
                .about("force consistency-check for a specified region")
                .arg(
                    Arg::with_name("region")
                        .required(true)
                        .short("r")
                        .takes_value(true)
                        .help("the target region"),
                ),
        )
        .subcommand(SubCommand::with_name("bad-regions").about("get all regions with corrupt raft"))
        .subcommand(
            SubCommand::with_name("modify-tikv-config")
                .about("modify tikv config, eg. ./tikv-ctl -h ip:port -m kvdb -n default.disable_auto_compactions -v true")
                .arg(
                    Arg::with_name("module")
                        .required(true)
                        .short("m")
                        .takes_value(true)
                        .help("module of the tikv, eg. kvdb or raftdb"),
                )
                .arg(
                    Arg::with_name("config_name")
                        .required(true)
                        .short("n")
                        .takes_value(true)
                        .help("config name of the module, for kvdb or raftdb, you can choose \
                            max_background_jobs to modify db options or default.disable_auto_compactions to modify column family(cf) options, \
                            and so on, default stands for default cf, \
                            for kvdb, default|write|lock|raft can be chosen, for raftdb, default can be chosen"),
                )
                .arg(
                    Arg::with_name("config_value")
                        .required(true)
                        .short("v")
                        .takes_value(true)
                        .help("config value of the module, eg. 8 for max_background_jobs or true for disable_auto_compactions"),
                ),
        )
        .subcommand(
            SubCommand::with_name("dump-snap-meta")
                .about("dump snapshot meta file")
                .arg(
                    Arg::with_name("file")
                        .required(true)
                        .short("f")
                        .long("file")
                        .takes_value(true)
                        .help("meta file path"),
                ),
        )
        .subcommand(
            SubCommand::with_name("compact-cluster")
                .about("compact the whole cluster in a specified range in one or more column families")
                .arg(
                    Arg::with_name("db")
                        .short("d")
                        .takes_value(true)
                        .default_value("kv")
                        .possible_values(&["kv", "raft"])
                        .help("kv or raft"),
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
                        .help("column family names, for kv db, combine from default/lock/write; for raft db, can only be default"),
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
                        .help("number of threads in one compaction")
                ),
        )
        .subcommand(
            SubCommand::with_name("region-properties")
                .about("show region properties")
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .required(true)
                        .takes_value(true)
                        .help("the target region id"),
                ),
        );

    let matches = app.clone().get_matches();

    let hex_key = matches.value_of("hex-to-escaped");
    let escaped_key = matches.value_of("escaped-to-hex");
    match (hex_key, escaped_key) {
        (Some(hex), None) => {
            println!("{}", escape(&from_hex(hex).unwrap()));
            return;
        }
        (None, Some(escaped)) => {
            println!("{}", &unescape(escaped).to_hex().to_uppercase());
            return;
        }
        (None, None) => {}
        _ => unreachable!(),
    };

    let mgr = new_security_mgr(&matches);

    if let Some(matches) = matches.subcommand_matches("dump-snap-meta") {
        let path = matches.value_of("file").unwrap();
        return dump_snap_meta_file(path);
    } else if let Some(sub_cmd) = matches.subcommand_matches("compact-cluster") {
        let pd = matches.value_of("pd").unwrap();
        let db = sub_cmd.value_of("db").unwrap();
        let db_type = if db == "kv" { DBType::KV } else { DBType::RAFT };
        let cfs = Vec::from_iter(sub_cmd.values_of("cf").unwrap());
        let from_key = sub_cmd.value_of("from").map(|k| unescape(k));
        let to_key = sub_cmd.value_of("to").map(|k| unescape(k));
        let threads = value_t_or_exit!(sub_cmd.value_of("threads"), u32);
        return compact_whole_cluster(pd, db_type, cfs, from_key, to_key, threads, mgr);
    }

    let db = matches.value_of("db");
    let raft_db = matches.value_of("raftdb");
    let host = matches.value_of("host");
    let cfg_path = matches.value_of("config");

    let debug_executor = new_debug_executor(db, raft_db, host, cfg_path, Arc::clone(&mgr));

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
        let cfs = Vec::from_iter(matches.values_of("cf").unwrap());
        if let Some(id) = matches.value_of("region") {
            debug_executor.dump_region_size(id.parse().unwrap(), cfs);
        } else {
            debug_executor.dump_all_region_size(cfs);
        }
    } else if let Some(matches) = matches.subcommand_matches("scan") {
        let from = unescape(matches.value_of("from").unwrap());
        let to = matches
            .value_of("to")
            .map_or_else(|| vec![], |to| unescape(to));
        let limit = matches
            .value_of("limit")
            .map_or(0, |s| s.parse().expect("parse u64"));
        if to.is_empty() && limit == 0 {
            eprintln!(r#"please pass "to" or "limit""#);
            process::exit(-1);
        }
        let cfs = Vec::from_iter(matches.values_of("show-cf").unwrap());
        let start_ts = matches.value_of("start_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        debug_executor.dump_mvccs_infos(from, to, limit, cfs, start_ts, commit_ts);
    } else if let Some(matches) = matches.subcommand_matches("mvcc") {
        let from = unescape(matches.value_of("key").unwrap());
        let cfs = Vec::from_iter(matches.values_of("show-cf").unwrap());
        let start_ts = matches.value_of("start_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        debug_executor.dump_mvccs_infos(from, vec![], 0, cfs, start_ts, commit_ts);
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let region = matches.value_of("region").unwrap().parse().unwrap();
        let to_db = matches.value_of("to_db");
        let to_host = matches.value_of("to_host");
        debug_executor.diff_region(region, to_db, None, to_host, cfg_path, mgr);
    } else if let Some(matches) = matches.subcommand_matches("compact") {
        let db = matches.value_of("db").unwrap();
        let db_type = if db == "kv" { DBType::KV } else { DBType::RAFT };
        let cf = matches.value_of("cf").unwrap();
        let from_key = matches.value_of("from").map(|k| unescape(k));
        let to_key = matches.value_of("to").map(|k| unescape(k));
        let threads = value_t_or_exit!(matches.value_of("threads"), u32);
        if let Some(region) = matches.value_of("region") {
            debug_executor.compact_region(host, db_type, cf, region.parse().unwrap(), threads);
        } else {
            debug_executor.compact(host, db_type, cf, from_key, to_key, threads);
        }
    } else if let Some(matches) = matches.subcommand_matches("tombstone") {
        let regions = matches
            .values_of("regions")
            .unwrap()
            .map(|r| r.parse())
            .collect::<Result<Vec<_>, _>>()
            .expect("parse regions fail");
        let pd_urls = Vec::from_iter(matches.values_of("pd").unwrap().map(|u| u.to_owned()));
        let mut cfg = PdConfig::default();
        cfg.endpoints = pd_urls;
        if let Err(e) = cfg.validate() {
            panic!("invalid pd configuration: {:?}", e);
        }
        debug_executor.set_region_tombstone_after_remove_peer(mgr, &cfg, regions);
    } else if let Some(matches) = matches.subcommand_matches("recover-mvcc") {
        let regions = matches
            .values_of("regions")
            .unwrap()
            .map(|r| r.parse())
            .collect::<Result<Vec<_>, _>>()
            .expect("parse regions fail");
        let pd_urls = Vec::from_iter(matches.values_of("pd").unwrap().map(|u| u.to_owned()));
        let mut cfg = PdConfig::default();
        cfg.endpoints = pd_urls;
        if let Err(e) = cfg.validate() {
            panic!("invalid pd configuration: {:?}", e);
        }
        debug_executor.recover_regions_mvcc(mgr, &cfg, regions);
    } else if let Some(matches) = matches.subcommand_matches("unsafe-recover") {
        if let Some(matches) = matches.subcommand_matches("remove-fail-stores") {
            let region_ids = matches.values_of("regions").map(|ids| {
                ids.map(|r| r.parse())
                    .collect::<Result<Vec<_>, _>>()
                    .expect("parse regions fail")
            });
            let stores = matches.values_of("stores").unwrap();

            match stores.map(|s| s.parse()).collect::<Result<Vec<u64>, _>>() {
                Ok(store_ids) => debug_executor.remove_fail_stores(store_ids, region_ids),
                Err(e) => perror_and_exit("parse store id list", e),
            }
        } else {
            eprintln!("{}", matches.usage());
        }
    } else if let Some(matches) = matches.subcommand_matches("recreate-region") {
        let mut pd_cfg = PdConfig::default();
        pd_cfg.endpoints = Vec::from_iter(matches.values_of("pd").unwrap().map(|u| u.to_owned()));
        let region_id = matches.value_of("region").unwrap().parse().unwrap();
        debug_executor.recreate_region(mgr, &pd_cfg, region_id);
    } else if let Some(matches) = matches.subcommand_matches("consistency-check") {
        let region_id = matches.value_of("region").unwrap().parse().unwrap();
        debug_executor.check_region_consistency(region_id);
    } else if matches.subcommand_matches("bad-regions").is_some() {
        debug_executor.print_bad_regions();
    } else if let Some(matches) = matches.subcommand_matches("modify-tikv-config") {
        let module = matches.value_of("module").unwrap();
        let config_name = matches.value_of("config_name").unwrap();
        let config_value = matches.value_of("config_value").unwrap();
        debug_executor.modify_tikv_config(get_module_type(module), config_name, config_value);
    } else if let Some(matches) = matches.subcommand_matches("metrics") {
        let tags = Vec::from_iter(matches.values_of("tag").unwrap());
        debug_executor.dump_metrics(tags)
    } else if let Some(matches) = matches.subcommand_matches("region-properties") {
        let region_id = value_t_or_exit!(matches.value_of("region"), u64);
        debug_executor.dump_region_properties(region_id)
    } else {
        let _ = app.print_help();
    }
}

fn get_module_type(module: &str) -> MODULE {
    match module {
        "kvdb" => MODULE::KVDB,
        "raftdb" => MODULE::RAFTDB,
        "readpool" => MODULE::READPOOL,
        "server" => MODULE::SERVER,
        "storage" => MODULE::STORAGE,
        "ps" => MODULE::PD,
        "metric" => MODULE::METRIC,
        "coprocessor" => MODULE::COPROCESSOR,
        "security" => MODULE::SECURITY,
        "import" => MODULE::IMPORT,
        _ => MODULE::UNUSED,
    }
}

fn from_hex(key: &str) -> Result<Vec<u8>, FromHexError> {
    const HEX_PREFIX: &str = "0x";
    if key.starts_with(HEX_PREFIX) {
        return key[2..].from_hex();
    }
    key.from_hex()
}

fn convert_gbmb(mut bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    if bytes < MB {
        return bytes.to_string();
    }
    let mb = if bytes % GB == 0 {
        String::from("")
    } else {
        format!("{:.3} MB ", (bytes % GB) as f64 / MB as f64)
    };
    bytes /= GB;
    let gb = if bytes == 0 {
        String::from("")
    } else {
        format!("{} GB ", bytes)
    };
    format!("{}{}", gb, mb)
}

fn new_security_mgr(matches: &ArgMatches) -> Arc<SecurityManager> {
    let ca_path = matches.value_of("ca_path");
    let cert_path = matches.value_of("cert_path");
    let key_path = matches.value_of("key_path");

    let mut cfg = SecurityConfig::default();
    if ca_path.is_none() && cert_path.is_none() && key_path.is_none() {
        return Arc::new(SecurityManager::new(&cfg).unwrap());
    }

    if ca_path.is_none() || cert_path.is_none() || key_path.is_none() {
        panic!("CA certificate and private key should all be set.");
    }
    cfg.ca_path = ca_path.unwrap().to_owned();
    cfg.cert_path = cert_path.unwrap().to_owned();
    cfg.key_path = key_path.unwrap().to_owned();
    Arc::new(SecurityManager::new(&cfg).expect("failed to initialize security manager"))
}

fn dump_snap_meta_file(path: &str) {
    let mut f =
        File::open(path).unwrap_or_else(|e| panic!("open file {} failed, error {:?}", path, e));
    let mut content = Vec::new();
    f.read_to_end(&mut content)
        .unwrap_or_else(|e| panic!("read meta file error {:?}", e));

    let mut meta = SnapshotMeta::new();
    meta.merge_from_bytes(&content)
        .unwrap_or_else(|e| panic!("parse from bytes error {:?}", e));
    for cf_file in meta.get_cf_files() {
        println!(
            "cf {}, size {}, checksum: {}",
            cf_file.cf, cf_file.size, cf_file.checksum
        );
    }
}

fn compact_whole_cluster(
    pd: &str,
    db_type: DBType,
    cfs: Vec<&str>,
    from: Option<Vec<u8>>,
    to: Option<Vec<u8>>,
    threads: u32,
    mgr: Arc<SecurityManager>,
) {
    let mut cfg = PdConfig::default();
    cfg.endpoints.push(pd.to_owned());
    if let Err(e) = cfg.validate() {
        panic!("invalid pd configuration: {:?}", e);
    }

    let pd_client = RpcClient::new(&cfg, Arc::clone(&mgr))
        .unwrap_or_else(|e| perror_and_exit("RpcClient::new", e));

    let stores = pd_client
        .get_all_stores()
        .unwrap_or_else(|e| perror_and_exit("Get all cluster stores from PD failed", e));

    let mut handles = Vec::new();
    for s in stores {
        let mgr = Arc::clone(&mgr);
        let addr = s.address.clone();
        let (from, to) = (from.clone(), to.clone());
        let cfs: Vec<String> = cfs.iter().map(|cf| cf.to_string().clone()).collect();
        let h = thread::spawn(move || {
            let debug_executor = new_debug_executor(None, None, Some(&addr), None, mgr);
            for cf in cfs {
                debug_executor.compact(
                    Some(&addr),
                    db_type,
                    cf.as_str(),
                    from.clone(),
                    to.clone(),
                    threads,
                );
            }
        });
        handles.push(h);
    }

    for h in handles {
        h.join().unwrap();
    }
}
