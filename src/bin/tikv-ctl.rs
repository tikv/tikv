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

#![feature(plugin)]
#![cfg_attr(feature = "dev", plugin(clippy))]
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]
#![allow(needless_pass_by_value)]

extern crate tikv;
extern crate clap;
extern crate protobuf;
extern crate kvproto;
extern crate rocksdb;
extern crate grpcio;
extern crate futures;
#[cfg(test)]
extern crate tempdir;
extern crate rustc_serialize;

use std::{process, str, u64};
use std::error::Error;
use std::sync::Arc;
use std::path::PathBuf;
use rustc_serialize::hex::{FromHex, ToHex};

use clap::{App, Arg, ArgMatches, SubCommand};
use protobuf::Message;
use futures::{future, Future, Stream};
use grpcio::{ChannelBuilder, Environment, Error as GrpcError};
use protobuf::RepeatedField;
use protobuf::text_format::print_to_string;

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::PeerState;
use kvproto::eraftpb;
use kvproto::kvrpcpb::MvccInfo;
use kvproto::debugpb::*;
use kvproto::debugpb::DB as DBType;
use kvproto::debugpb_grpc::DebugClient;
use tikv::util::{self, escape, unescape};
use tikv::raftstore::store::{keys, Engines};
use tikv::raftstore::store::debug::{Debugger, RegionInfo};
use tikv::storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};

enum DebugExecutor {
    Remote(DebugClient),
    Local(Debugger),
}

impl DebugExecutor {
    fn from_args(matches: &ArgMatches) -> Self {
        let remote = matches.value_of("host");
        let local = matches.value_of("db");
        match (remote, local) {
            (Some(_), Some(_)) => {
                eprintln!(r#""host" and "db" can not be passed together!"#);
                process::exit(1);
            }
            (None, None) => {
                eprintln!(r#"please pass "host" or "db""#);
                process::exit(1);
            }
            (None, Some(path)) => {
                let db = Arc::new(util::rocksdb::open(path, ALL_CFS).unwrap());
                let raft_db = if let Some(raftdb_path) = matches.value_of("raftdb") {
                    Arc::new(util::rocksdb::open(raftdb_path, &[CF_DEFAULT]).unwrap())
                } else {
                    let raftdb_path = PathBuf::from(path).join("../raft");
                    Arc::new(
                        util::rocksdb::open(raftdb_path.to_str().unwrap(), &[CF_DEFAULT]).unwrap(),
                    )
                };
                DebugExecutor::Local(Debugger::new(Engines::new(db, raft_db)))
            }
            (Some(remote), None) => {
                let env = Arc::new(Environment::new(1));
                let channel = ChannelBuilder::new(env).connect(remote);
                let client = DebugClient::new(channel);
                DebugExecutor::Remote(client)
            }
        }
    }

    fn print(&self, cf: &str, key: &str) {
        let db = DBType::KV;
        let key = unescape(key);
        let value = match *self {
            DebugExecutor::Remote(ref client) => {
                let mut get_req = GetRequest::new();
                get_req.set_db(db);
                get_req.set_cf(cf.to_owned());
                get_req.set_key(key);
                client
                    .get(get_req)
                    .map(|resp| escape(resp.get_value()))
                    .unwrap_or_else(Self::report_and_exit)
            }
            DebugExecutor::Local(ref debugger) => debugger
                .get(db, cf, &key)
                .map(|bytes| escape(&bytes))
                .unwrap_or_else(Self::report_and_exit),
        };
        println!("value: {}", value);
    }

    fn region_size(&self, region_id: u64, cfs: Vec<&str>) {
        let sizes = match *self {
            DebugExecutor::Remote(ref client) => {
                let mut req = RegionSizeRequest::new();
                let cfs = cfs.into_iter().map(|s| s.to_owned()).collect();
                req.set_cfs(RepeatedField::from_vec(cfs));
                req.set_region_id(region_id);
                client
                    .region_size(req)
                    .unwrap_or_else(Self::report_and_exit)
                    .take_entries()
                    .into_iter()
                    .map(|mut entry| (entry.take_cf(), entry.get_size() as usize))
                    .collect::<Vec<_>>()
            }
            DebugExecutor::Local(ref debugger) => debugger
                .region_size(region_id, cfs)
                .unwrap_or_else(Self::report_and_exit)
                .into_iter()
                .map(|(cf, size)| (cf.to_owned(), size as usize))
                .collect(),

        };
        Self::show_region_size(sizes);
    }

    fn all_region_size(&self, cfs: Vec<&str>) {
        let all_regions = self.get_all_regions(CF_RAFT);
        for region in all_regions {
            self.region_size(region, cfs.clone());
        }
    }

    fn region_info(&self, region_id: u64, skip_tombstone: bool) {
        let region_info = self.get_region_info(region_id);
        Self::show_region_info(region_id, region_info, skip_tombstone);
    }

    fn all_region_info(&self, skip_tombstone: bool) {
        let all_regions = self.get_all_regions(CF_RAFT);
        for region in all_regions {
            self.region_info(region, skip_tombstone);
        }
    }

    fn raft_log(&self, region_id: u64, log_index: u64) {
        let entry = match *self {
            DebugExecutor::Remote(ref client) => {
                let mut req = RaftLogRequest::new();
                req.set_region_id(region_id);
                req.set_log_index(log_index);
                client
                    .raft_log(req)
                    .map(|mut resp| resp.take_entry())
                    .unwrap_or_else(Self::report_and_exit)
            }
            DebugExecutor::Local(ref debugger) => debugger
                .raft_log(region_id, log_index)
                .unwrap_or_else(Self::report_and_exit),
        };
        Self::show_raft_log(region_id, log_index, entry);
    }

    fn scan_mvcc<S>(&self, from: Vec<u8>, to: Option<Vec<u8>>, limit: Option<u64>, show: S)
    where
        S: Fn(Vec<u8>, MvccInfo),
    {
        let to = to.unwrap_or_default();
        let limit = limit.unwrap_or_default();

        if to.is_empty() && limit == 0 {
            eprintln!(r#"please pass "to" or "limit""#);
            process::exit(-1);
        }
        if to < from {
            eprintln!("The region's start pos must greater than the end pos.");
            process::exit(-1);
        }

        match *self {
            DebugExecutor::Remote(ref client) => {
                let mut req = ScanMvccRequest::new();
                req.set_from_key(from);
                req.set_to_key(to);
                req.set_limit(limit);
                let future = client.scan_mvcc(req).for_each(
                    |mut resp: ScanMvccResponse| {
                        let key = resp.take_key();
                        let mvcc = resp.take_info();
                        show(key, mvcc);
                        future::ok::<_, GrpcError>(())
                    },
                );
                future.wait().unwrap_or_else(Self::report_and_exit);
            }
            DebugExecutor::Local(ref debugger) => {
                let iter = debugger
                    .scan_mvcc(&from, &to, limit)
                    .unwrap_or_else(Self::report_and_exit);
                for r in iter {
                    let (key, mvcc) = r.unwrap_or_else(Self::report_and_exit);
                    show(key, mvcc);
                }
            }
        }
    }

    fn diff_region(&self, region: u64, to: &str) {
        let region_info_1 = self.get_region_info(region);
        let another_executor = {
            let db = Arc::new(util::rocksdb::open(to, ALL_CFS).unwrap());
            let raft_to = to.to_string() + "../raft";
            let raft_db = Arc::new(util::rocksdb::open(&raft_to, &[CF_DEFAULT]).unwrap());
            DebugExecutor::Local(Debugger::new(Engines::new(db, raft_db)))
        };
        let region_info_2 = another_executor.get_region_info(region);
        Self::show_diff_region(region, region_info_1, region_info_2);
    }

    fn compact(&self, db: DBType, cf: &str, from: Option<Vec<u8>>, to: Option<Vec<u8>>) {
        let from = from.unwrap_or_default();
        let to = to.unwrap_or_default();
        match *self {
            DebugExecutor::Remote(ref client) => {
                let mut req = CompactRequest::new();
                req.set_db(db);
                req.set_cf(cf.to_owned());
                req.set_from_key(from);
                req.set_to_key(to);
                client.compact(req).unwrap_or_else(Self::report_and_exit);
                println!("success!");
            }
            DebugExecutor::Local(ref debugger) => {
                debugger
                    .compact(db, cf, &from, &to)
                    .unwrap_or_else(Self::report_and_exit);
                println!("success!");
            }
        }
    }

    fn tombstone(&self, region: u64, conf_ver: u64) {
        match *self {
            DebugExecutor::Remote(_) => {
                eprintln!("This command is only for local mode");
                process::exit(-1);
            }
            DebugExecutor::Local(ref debugger) => debugger
                .tombstone_region(region, conf_ver)
                .unwrap_or_else(Self::report_and_exit),
        }
    }

    fn report_and_exit<E: Error, T>(e: E) -> T {
        eprintln!("{}", e);
        process::exit(-1);
    }

    fn get_all_regions(&self, cf: &str) -> Vec<u64> {
        match *self {
            DebugExecutor::Remote(_) => unimplemented!(),
            DebugExecutor::Local(ref debugger) => debugger
                .get_all_regions(cf)
                .unwrap_or_else(Self::report_and_exit),
        }
    }

    fn get_region_info(&self, region_id: u64) -> RegionInfo {
        match *self {
            DebugExecutor::Remote(ref client) => {
                let resp_to_region_info = |mut resp: RegionInfoResponse| {
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
                };
                let mut req = RegionInfoRequest::new();
                req.set_region_id(region_id);
                client
                    .region_info(req)
                    .map(resp_to_region_info)
                    .unwrap_or_else(Self::report_and_exit)
            }
            DebugExecutor::Local(ref debugger) => debugger
                .region_info(region_id)
                .unwrap_or_else(Self::report_and_exit),
        }
    }

    fn show_region_size(sizes: Vec<(String, usize)>) {
        if sizes.len() > 1 {
            let total_size = sizes.iter().map(|t| t.1).sum::<usize>() as u64;
            println!("total region number: {}", sizes.len());
            println!("total region size: {}", convert_gbmb(total_size));
        }
        for (cf, size) in sizes {
            println!("cf: {}", cf);
            println!("region size: {}", convert_gbmb(size as u64));
        }
    }

    fn show_region_info(id: u64, r: RegionInfo, skip_tomb: bool) {
        if skip_tomb {
            let region_state = r.region_local_state.as_ref();
            if region_state.map_or(false, |s| s.get_state() == PeerState::Tombstone) {
                return;
            }
        }
        let region_state_key = keys::region_state_key(id);
        let raft_state_key = keys::raft_state_key(id);
        let apply_state_key = keys::apply_state_key(id);
        println!("region state key: {}", escape(&region_state_key));
        println!("region state: {:?}", r.region_local_state);
        println!("raft state key: {}", escape(&raft_state_key));
        println!("raft state: {:?}", r.raft_local_state);
        println!("apply state key: {}", escape(&apply_state_key));
        println!("apply state: {:?}", r.raft_apply_state);
    }

    fn show_diff_region(id: u64, r1: RegionInfo, r2: RegionInfo) {
        println!("region id: {}", id);
        println!("db1 region state: {:?}", r1.region_local_state);
        println!("db2 region state: {:?}", r2.region_local_state);
        println!("db1 apply state: {:?}", r1.raft_apply_state);
        println!("db2 apply state: {:?}", r2.raft_apply_state);
    }

    fn show_raft_log(id: u64, index: u64, mut entry: eraftpb::Entry) {
        let idx_key = keys::raft_log_key(id, index);
        println!("idx_key: {}", escape(&idx_key));
        println!("region: {}", id);
        println!("log index: {}", index);

        let data = entry.take_data();
        println!("entry {:?}", entry);
        println!("msg len: {}", data.len());

        let mut msg = RaftCmdRequest::new();
        msg.merge_from_bytes(&data).unwrap();
        println!("{:?}", msg);
    }

    fn show_mvcc_in_cf(
        key: Vec<u8>,
        mut mvcc: MvccInfo,
        cf: &str,
        start_ts: Option<u64>,
        commit_ts: Option<u64>,
    ) {
        let print = |k: &[u8], v: &[u8]| {
            println!("key: {}, value len: {}", escape(k), v.len());
            println!("{}", escape(v));
        };
        if cf == CF_DEFAULT {
            for mut value_info in mvcc.take_values().into_iter() {
                if commit_ts.map_or(true, |ts| value_info.get_ts() == ts) {
                    let value = escape(value_info.get_value()).into_bytes();
                    value_info.set_value(value);
                    print(&key, &print_to_string(&value_info).into_bytes());
                }
            }
        } else if cf == CF_WRITE {
            for write_info in mvcc.take_writes().into_iter() {
                if start_ts.map_or(true, |ts| write_info.get_start_ts() == ts) &&
                    commit_ts.map_or(true, |ts| write_info.get_commit_ts() == ts)
                {
                    // FIXME: short_value is lost in kvproto.
                    print(&key, &print_to_string(&write_info).into_bytes());
                }
            }
        } else if cf == CF_LOCK {
            if mvcc.has_lock() {
                let mut lock_info = mvcc.take_lock();
                if start_ts.map_or(true, |ts| lock_info.get_lock_version() == ts) {
                    // FIXME: lock type is lost in kvproto.
                    let pk = escape(lock_info.get_primary_lock()).into_bytes();
                    let k = escape(lock_info.get_key()).into_bytes();
                    lock_info.set_primary_lock(pk);
                    lock_info.set_key(k);
                    print(&key, &print_to_string(&lock_info).into_bytes());
                }
            }
        } else {
            eprintln!("invalid cf: {}", cf);
            process::exit(-1);
        }
    }
}

fn main() {
    let mut app = App::new("TiKV Ctl")
        .author("PingCAP")
        .about(
            "Distributed transactional key value database powered by Rust and Raft",
        )
        .arg(
            Arg::with_name("db")
                .short("d")
                .takes_value(true)
                .help("set rocksdb path"),
        )
        .arg(
            Arg::with_name("raftdb")
                .short("r")
                .takes_value(true)
                .help("set raft rocksdb path"),
        )
        .arg(
            Arg::with_name("hex-to-escaped")
                .short("h")
                .takes_value(true)
                .help("convert hex key to escaped key"),
        )
        .arg(
            Arg::with_name("escaped-to-hex")
                .short("e")
                .takes_value(true)
                .help("convert escaped key to hex key"),
        )
        .subcommand(
            SubCommand::with_name("raft")
                .about("print raft log entry")
                .subcommand(
                    SubCommand::with_name("log")
                        .about("print the raft log entry info")
                        .arg(
                            Arg::with_name("region")
                                .short("r")
                                .takes_value(true)
                                .help("set the region id"),
                        )
                        .arg(
                            Arg::with_name("index")
                                .short("i")
                                .takes_value(true)
                                .help("set the raft log index"),
                        )
                        .arg(
                            Arg::with_name("key")
                                .short("k")
                                .takes_value(true)
                                .help("set the raw key, in escaped form"),
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
                                .short("s")
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
                        .help("set the cf name, if not specified, print all cf."),
                ),
        )
        .subcommand(
            SubCommand::with_name("scan")
                .about("print the range db range")
                .arg(
                    Arg::with_name("from")
                        .short("f")
                        .takes_value(true)
                        .help("set the scan from raw key, in escaped format"),
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .takes_value(true)
                        .help("set the scan end raw key, in escaped format"),
                )
                .arg(
                    Arg::with_name("limit")
                        .short("l")
                        .takes_value(true)
                        .help("set the scan limit"),
                )
                .arg(
                    Arg::with_name("start_ts")
                        .short("s")
                        .takes_value(true)
                        .help("set the scan start_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .takes_value(true)
                        .help("set the scan commit_ts as filter"),
                )
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .default_value(CF_DEFAULT)
                        .help("column family name"),
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
                        .short("k")
                        .takes_value(true)
                        .help("set the query raw key, in escaped form"),
                ),
        )
        .subcommand(
            SubCommand::with_name("mvcc")
                .about("print the mvcc value")
                .arg(
                    Arg::with_name("key")
                        .short("key")
                        .takes_value(true)
                        .help("set the query raw key, in escaped form"),
                )
                .arg(
                    Arg::with_name("cf")
                        .short("c")
                        .takes_value(true)
                        .default_value(CF_DEFAULT)
                        .help("column family name, only can be default/lock/write"),
                )
                .arg(
                    Arg::with_name("start_ts")
                        .short("s")
                        .takes_value(true)
                        .help("set start_ts as filter"),
                )
                .arg(
                    Arg::with_name("commit_ts")
                        .takes_value(true)
                        .help("set commit_ts as filter"),
                ),
        )
        .subcommand(
            SubCommand::with_name("diff")
                .about("diff two region keys")
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .takes_value(true)
                        .help("to which db"),
                )
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .takes_value(true)
                        .help("specify region id"),
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
                        .takes_value(true)
                        .help("set the start raw key, in escaped form"),
                )
                .arg(
                    Arg::with_name("to")
                        .short("t")
                        .takes_value(true)
                        .help("set the end raw key, in escaped form"),
                ),
        )
        .subcommand(
            SubCommand::with_name("tombstone")
                .about("set a region on the node to tombstone by manual")
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .takes_value(true)
                        .help("the target region"),
                )
                .arg(
                    Arg::with_name("conf_ver")
                        .short("v")
                        .takes_value(true)
                        .help("the conf_ver set to the region"),
                ),
        );
    let matches = app.clone().get_matches();

    let hex_key = matches.value_of("hex-to-escaped");
    let escaped_key = matches.value_of("escaped-to-hex");
    match (hex_key, escaped_key) {
        (Some(_), Some(_)) => panic!("hex and escaped can not be passed together!"),
        (Some(hex), None) => {
            println!("{}", escape(&from_hex(hex)));
            return;
        }
        (None, Some(escaped)) => {
            println!("{}", &unescape(escaped).to_hex().to_uppercase());
            return;
        }
        (None, None) => {}
    };

    let debug_executor = DebugExecutor::from_args(&matches);

    if let Some(matches) = matches.subcommand_matches("print") {
        let cf = matches.value_of("cf").unwrap();
        let key = matches.value_of("key").unwrap();
        debug_executor.print(cf, key);
    } else if let Some(matches) = matches.subcommand_matches("raft") {
        if let Some(matches) = matches.subcommand_matches("log") {
            let (id, index) = if let Some(key) = matches.value_of("key") {
                keys::decode_raft_log_key(&unescape(key)).unwrap()
            } else {
                let id = matches.value_of("region").unwrap().parse().unwrap();
                let index = matches.value_of("index").unwrap().parse().unwrap();
                (id, index)
            };
            debug_executor.raft_log(id, index);
        } else if let Some(matches) = matches.subcommand_matches("region") {
            let skip_tombstone = matches.is_present("skip-tombstone");
            if let Some(id) = matches.value_of("region") {
                debug_executor.region_info(id.parse().unwrap(), skip_tombstone);
            } else {
                debug_executor.all_region_info(skip_tombstone);
            }
        } else {
            let _ = app.print_help();
        }
    } else if let Some(matches) = matches.subcommand_matches("size") {
        let cfs = matches
            .value_of("cf")
            .map_or(ALL_CFS.to_vec(), |cf| vec![cf]);
        if let Some(id) = matches.value_of("region") {
            debug_executor.region_size(id.parse().unwrap(), cfs);
        } else {
            debug_executor.all_region_size(cfs);
        }
    } else if let Some(matches) = matches.subcommand_matches("scan") {
        let from = unescape(matches.value_of("from").unwrap());
        let to = matches.value_of("to").map(|to| unescape(to));
        let limit = matches.value_of("limit").map(|s| s.parse().unwrap());
        let cf = matches.value_of("cf").unwrap();
        let start_ts = matches.value_of("start_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        let show_mvcc = |k: Vec<u8>, v: MvccInfo| {
            DebugExecutor::show_mvcc_in_cf(k, v, cf, start_ts, commit_ts);
        };
        debug_executor.scan_mvcc(from, to, limit, show_mvcc);
    } else if let Some(matches) = matches.subcommand_matches("mvcc") {
        let from = unescape(matches.value_of("key").unwrap());
        let cf = matches.value_of("cf").unwrap();
        let start_ts = matches.value_of("start_ts").map(|s| s.parse().unwrap());
        let commit_ts = matches.value_of("commit_ts").map(|s| s.parse().unwrap());
        let show_mvcc = |k: Vec<u8>, v: MvccInfo| {
            DebugExecutor::show_mvcc_in_cf(k, v, cf, start_ts, commit_ts);
        };
        debug_executor.scan_mvcc(from, None, Some(1), show_mvcc);
    } else if let Some(matches) = matches.subcommand_matches("diff") {
        let region = matches.value_of("region").unwrap().parse().unwrap();
        let to = matches.value_of("to").unwrap();
        debug_executor.diff_region(region, to);
    } else if let Some(matches) = matches.subcommand_matches("compact") {
        let db = matches.value_of("db").unwrap();
        let db_type = if db == "kv" { DBType::KV } else { DBType::RAFT };
        let cf = matches.value_of("cf").unwrap();
        let from_key = matches.value_of("from").map(|k| unescape(k));
        let to_key = matches.value_of("to").map(|k| unescape(k));
        debug_executor.compact(db_type, cf, from_key, to_key);
    } else if let Some(matches) = matches.subcommand_matches("tombstone") {
        let region = matches.value_of("region").unwrap().parse().unwrap();
        let conf_ver = matches.value_of("conf_ver").unwrap().parse().unwrap();
        debug_executor.tombstone(region, conf_ver);
    } else {
        let _ = app.print_help();
    }

}

fn from_hex(key: &str) -> Vec<u8> {
    const HEX_PREFIX: &str = "0x";
    let mut s = String::from(key);
    if s.starts_with(HEX_PREFIX) {
        let len = s.len();
        let new_len = len.saturating_sub(HEX_PREFIX.len());
        s.truncate(new_len);
    }
    s.as_str().from_hex().unwrap()
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
