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
use kvproto::metapb::Region;
use kvproto::raftpb::Entry;
use rocksdb::DB;
use tikv::util::escape;
use tikv::raftstore::store::keys;
use tikv::raftstore::store::engine::Peekable;

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
    opts.optopt("k", "key", "set the query raw key, in hex format", "");
    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let db_str = matches.opt_str("db").unwrap();
    let db = DB::open_default(&db_str).unwrap();
    let key = matches.opt_str("k");
    let idx = matches.opt_str("i");
    let region = matches.opt_str("r");
    if key.is_some() {
        dump_raw_value(db, key.unwrap());
    } else if idx.is_some() {
        dump_raft_log_entry(db, region.unwrap(), idx.unwrap());
    } else if matches.opt_present("info") {
        dump_region_info(db, region.unwrap());
    } else {
        panic!("currently only random key-value and raft log entry query are supported.");
    }
}

fn dump_raw_value(db: DB, mut key_str: String) {
    key_str = key_str.to_lowercase();
    let mut key = key_str.trim();
    if key.starts_with("0x") {
        key = key.split_at(2).1;
    }
    let key_bytes = key.as_bytes();
    let mut key = Vec::with_capacity(key.len() / 2);
    for chunk in key_bytes.chunks(2) {
        let b = u8::from_str_radix(str::from_utf8(chunk).unwrap(), 16).unwrap();
        key.push(b);
    }
    let value = db.get_value(&key).unwrap();
    println!("value: {:?}", value.map(|v| escape(&v)));
}

fn dump_raft_log_entry(db: DB, region_id_str: String, idx_str: String) {
    let region_id = u64::from_str_radix(&region_id_str, 10).unwrap();
    let idx = u64::from_str_radix(&idx_str, 10).unwrap();

    let idx_key = keys::raft_log_key(region_id, idx);
    println!("idx_key: {}", escape(&idx_key));
    let mut ent: Entry = db.get_msg(&idx_key).unwrap().unwrap();
    let data = ent.take_data();
    println!("entry {:?}", ent);
    let mut msg = RaftCmdRequest::new();
    msg.merge_from_bytes(&data).unwrap();
    println!("msg {:?}", msg);
}

fn dump_region_info(db: DB, region_id_str: String) {
    let region_id = u64::from_str_radix(&region_id_str, 10).unwrap();
    let region_info_key = keys::region_info_key(region_id);
    println!("info_key: {}", escape(&region_info_key));
    let region: Option<Region> = db.get_msg(&region_info_key).unwrap();
    println!("info: {:?}", region);
}
