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

extern crate tikv;
extern crate getopts;
extern crate protobuf;
extern crate kvproto;
extern crate rocksdb;

use std::{env, str, u64};
use getopts::Options;
use protobuf::Message;
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::{RaftLocalState, RegionLocalState, RaftApplyState};
use kvproto::eraftpb::Entry;
use rocksdb::DB;
use tikv::util::{self, escape, unescape};
use tikv::raftstore::store::keys;
use tikv::raftstore::store::engine::{Peekable, Iterable};
use tikv::storage::DEFAULT_CFS;
use tikv::storage::CF_RAFT;

/// # Message dump tool
///
/// A simple tool that dump the message from rocksdb directory. Very useful when you want
/// to take a deep look in the data.

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("", "db", "set rocksdb path, required", "");
    opts.optopt("r",
                "region-id",
                "set the region id",
                "required when getting raft message");
    opts.optflag("h", "help", "print this help menu");
    opts.optflag("", "info", "print the region info");
    opts.optopt("i", "index", "set the raft log index", "");
    opts.optopt("k", "key", "set the query raw key, in escape format", "");
    opts.optopt("f",
                "from",
                "set the scan from raw key, in escaped format",
                "");
    opts.optopt("t", "to", "set the scan end raw key, in escaped format", "");
    opts.optopt("l", "limit", "set the scan limit", "");
    opts.optopt("c",
                "cf",
                "column family name, only avialbe for dump-range",
                "");
    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let db_str = matches.opt_str("db").unwrap();
    let db = util::rocksdb::open(&db_str, DEFAULT_CFS).unwrap();
    let key = matches.opt_str("k");
    let from = matches.opt_str("f");
    let to = matches.opt_str("t");
    let limit = matches.opt_str("l").map(|s| s.parse().unwrap());
    let idx = matches.opt_str("i");
    let region = matches.opt_str("r");
    let cf = matches.opt_str("c");
    let cf_name = cf.as_ref().map_or("default", |s| s.as_str());
    if let Some(key) = key {
        dump_raw_value(db, cf_name, key);
    } else if let Some(idx) = idx {
        dump_raft_log_entry(db, region.unwrap(), idx);
    } else if matches.opt_present("info") {
        dump_region_info(db, region.unwrap());
    } else if let Some(from) = from {
        dump_range(db, from, to, limit, cf_name);
    } else {
        panic!("currently only random key-value and raft log entry query are supported.");
    }
}

fn dump_raw_value(db: DB, cf: &str, key: String) {
    let key = unescape(&key);
    let value = db.get_value_cf(cf, &key).unwrap();
    println!("value: {}", value.map_or("None".to_owned(), |v| escape(&v)));
}

fn dump_raft_log_entry(db: DB, region_id_str: String, idx_str: String) {
    let region_id = u64::from_str_radix(&region_id_str, 10).unwrap();
    let idx = u64::from_str_radix(&idx_str, 10).unwrap();

    let idx_key = keys::raft_log_key(region_id, idx);
    println!("idx_key: {}", escape(&idx_key));
    let mut ent: Entry = db.get_msg_cf(CF_RAFT, &idx_key).unwrap().unwrap();
    let data = ent.take_data();
    println!("entry {:?}", ent);
    let mut msg = RaftCmdRequest::new();
    msg.merge_from_bytes(&data).unwrap();
    println!("msg {:?}", msg);
}

fn dump_region_info(db: DB, region_id_str: String) {
    let region_id = u64::from_str_radix(&region_id_str, 10).unwrap();

    let region_state_key = keys::region_state_key(region_id);
    println!("region state key: {}", escape(&region_state_key));
    let region_state: Option<RegionLocalState> = db.get_msg(&region_state_key).unwrap();
    println!("region state: {:?}", region_state);

    let raft_state_key = keys::raft_state_key(region_id);
    println!("raft state key: {}", escape(&raft_state_key));
    let raft_state: Option<RaftLocalState> = db.get_msg_cf(CF_RAFT, &raft_state_key).unwrap();
    println!("raft state: {:?}", raft_state);

    let apply_state_key = keys::apply_state_key(region_id);
    println!("apply state key: {}", escape(&apply_state_key));
    let apply_state: Option<RaftApplyState> = db.get_msg_cf(CF_RAFT, &apply_state_key).unwrap();
    println!("apply state: {:?}", apply_state);
}

fn dump_range(db: DB, from: String, to: Option<String>, limit: Option<u64>, cf: &str) {
    let from = unescape(&from);
    let to = to.map_or_else(|| vec![0xff], |s| unescape(&s));
    let limit = limit.unwrap_or(u64::MAX);

    if limit == 0 {
        return;
    }

    let mut cnt = 0;
    db.scan_cf(cf,
                 &from,
                 &to,
                 &mut |k, v| {
                     println!("key: {}, value: {}", escape(k), escape(v));
                     cnt += 1;
                     Ok(cnt < limit)
                 })
        .unwrap()
}
