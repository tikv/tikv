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
extern crate clap;
extern crate protobuf;
extern crate kvproto;
extern crate rocksdb;

use std::{str, u64};
use clap::{Arg, App, SubCommand};
use protobuf::Message;
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::{RaftLocalState, RegionLocalState, RaftApplyState};
use kvproto::eraftpb::Entry;
use rocksdb::DB;
use tikv::util::{self, escape, unescape};
use tikv::util::codec::bytes::encode_bytes;
use tikv::raftstore::store::keys;
use tikv::raftstore::store::engine::{Peekable, Iterable};
use tikv::storage::{ALL_CFS, CF_RAFT};
use tikv::storage::mvcc::{Lock, Write};

fn main() {
    let mut app = App::new("TiKV Ctl")
        .author("PingCAP")
        .about("Distributed transactional key value database powered by Rust and Raft")
        .arg(Arg::with_name("db")
            .short("d")
            .takes_value(true)
            .help("set rocksdb path, required"))
        .subcommand(SubCommand::with_name("raft")
            .about("print raft log entry")
            .subcommand(SubCommand::with_name("log")
                .about("print the raft log entry info")
                .arg(Arg::with_name("region")
                    .short("r")
                    .takes_value(true)
                    .help("set the region id"))
                .arg(Arg::with_name("index")
                    .short("i")
                    .takes_value(true)
                    .help("set the raft log index")))
            .subcommand(SubCommand::with_name("region")
                .about("print region info")
                .arg(Arg::with_name("region")
                    .short("r")
                    .takes_value(true)
                    .help("set the region id"))))
        .subcommand(SubCommand::with_name("scan")
            .about("print the range db range")
            .arg(Arg::with_name("from")
                .short("f")
                .takes_value(true)
                .help("set the scan from raw key, in escaped format"))
            .arg(Arg::with_name("to")
                .short("t")
                .takes_value(true)
                .help("set the scan end raw key, in escaped format"))
            .arg(Arg::with_name("limit")
                .short("l")
                .takes_value(true)
                .help("set the scan limit"))
            .arg(Arg::with_name("cf")
                .short("c")
                .takes_value(true)
                .help("column family name")))
        .subcommand(SubCommand::with_name("print")
                .about("print the raw value")
                .arg(Arg::with_name("cf")
                     .short("c")
                     .takes_value(true)
                     .help("column family name"))
                .arg(Arg::with_name("key")
                     .short("k")
                     .takes_value(true)
                     .help("set the query raw key, in escaped form")))
        .subcommand(SubCommand::with_name("mvcc")
                .about("print the mvcc value")
                .arg(Arg::with_name("cf")
                     .short("c")
                     .takes_value(true)
                     .help("column family name, only can be default/lock/write"))
                .arg(Arg::with_name("key")
                     .short("key")
                     .takes_value(true)
                     .help("the query key")));
    let matches = app.clone().get_matches();

    let db_path = matches.value_of("db").unwrap();
    let db = util::rocksdb::open(db_path, ALL_CFS).unwrap();
    if let Some(matches) = matches.subcommand_matches("print") {
        let cf_name = matches.value_of("cf").unwrap_or("default");
        let key = String::from(matches.value_of("key").unwrap());
        dump_raw_value(db, cf_name, key);
    } else if let Some(matches) = matches.subcommand_matches("raft") {
        if let Some(matches) = matches.subcommand_matches("log") {
            let region = String::from(matches.value_of("region").unwrap());
            let index = String::from(matches.value_of("index").unwrap());
            dump_raft_log_entry(db, region, index);
        } else if let Some(matches) = matches.subcommand_matches("region") {
            let region = String::from(matches.value_of("region").unwrap());
            dump_region_info(db, region);
        } else {
            panic!("Currently only support raft log entry and scan.")
        }
    } else if let Some(matches) = matches.subcommand_matches("scan") {
        let from = String::from(matches.value_of("from").unwrap());
        let to = matches.value_of("to").map(String::from);
        let limit = matches.value_of("limit").map(|s| s.parse().unwrap());
        let cf_name = matches.value_of("cf").unwrap_or("default");
        if let Some(ref to) = to {
            if to > &from {
                panic!("The region's start pos must greater than the end pos.")
            }
        }
        dump_range(db, from, to, limit, cf_name);
    } else if let Some(matches) = matches.subcommand_matches("mvcc") {
        let cf_name = matches.value_of("cf").unwrap_or("default");
        let key = matches.value_of("key").unwrap();
        println!("You are searching Key {}: ", key);
        match cf_name {
            "default" => {
                let key_prefix = gen_key_prefix(key);
                dump_mvcc_default(db, key_prefix);
            }
            "lock" => {
                let key_prefix = gen_key_prefix(key);
                dump_mvcc_lock(db, key_prefix);
            }
            "write" => {
                let key_prefix = gen_key_prefix(key);
                dump_mvcc_write(db, key_prefix);
            }
            _ => {
                let _ = app.print_help();
            }
        }
    } else {
        let _ = app.print_help();
    }

}

fn gen_key_prefix(key: &str) -> Vec<u8> {
    let mut prefix = "t".as_bytes().to_vec();
    let mut encoded = encode_bytes(key.as_bytes());
    prefix.append(&mut encoded);
    prefix
}

fn compare_prefix(key: &[u8], prefix: &Vec<u8>) -> bool {
    let (pre, _) = key.split_at(prefix.len());
    pre == prefix.as_slice()
}

fn get_default_start_ts<'a>(key: &'a [u8], prefix: &Vec<u8>) -> &'a [u8] {
    let (_, rest) = key.split_at(prefix.len());
    rest
}

fn dump_mvcc_default(db: DB, key_prefix: Vec<u8>) {
    let mut iter = db.new_iterator(None);
    iter.seek(key_prefix.as_slice().into());
    if iter.valid() {
        if compare_prefix(iter.key(), &key_prefix) {
            println!("key: {:?}", iter.key());
            println!("value: {:?}", iter.value());
            println!("start_ts: {:?}", get_default_start_ts(iter.key(), &key_prefix));
        }
        while iter.next() && compare_prefix(iter.key(), &key_prefix) {
            println!("key: {:?}", iter.key());
            println!("value: {:?}", iter.value());
            println!("start_ts: {:?}", get_default_start_ts(iter.key(), &key_prefix));
        }
    } else {
        println!("No such record");
    }
}

fn dump_mvcc_lock(db: DB, key_prefix: Vec<u8>) {
    let mut iter = db.new_iterator(None);
    iter.seek(key_prefix.as_slice().into());
    if iter.valid() {
        let lock = Lock::parse(iter.value().clone()).unwrap();
        if compare_prefix(iter.key(), &key_prefix) {
            println!("Key: {:?}", iter.key());
            println!("Value: {:?}", lock.primary);
            println!("Type: {:?}", lock.lock_type);
            println!("Start_ts: {:?}", lock.ts);
        }
        while iter.next() && compare_prefix(iter.key(), &key_prefix) {
            let lock = Lock::parse(iter.value().clone()).unwrap();
            println!("Key: {:?}", iter.key());
            println!("Value: {:?}", lock.primary);
            println!("Type: {:?}", lock.lock_type);
            println!("Start_ts: {:?}", lock.ts);
        }
    } else {
        println!("No such record");
    }
}

fn dump_mvcc_write(db: DB, key_prefix: Vec<u8>) {
    let mut iter = db.new_iterator(None);
    iter.seek(key_prefix.as_slice().into());
    if iter.valid() {
        let write = Write::parse(iter.value().clone()).unwrap();
        if compare_prefix(iter.key(), &key_prefix) {
            println!("Key: {:?}", iter.key());
            println!("Value: {:?}", iter.value());
            println!("Type: {:?}", write.write_type);
            println!("Start_ts: {:?}", write.start_ts);
        }
        while iter.next() && compare_prefix(iter.key(), &key_prefix) {
            let write = Write::parse(iter.value().clone()).unwrap();
            println!("Key: {:?}", iter.key());
            println!("Value: {:?}", iter.value());
            println!("Type: {:?}", write.write_type);
            println!("Start_ts: {:?}", write.start_ts);
        }
    } else {
        println!("No such record!");
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
                 true,
                 &mut |k, v| {
                     println!("key: {}, value: {}", escape(k), escape(v));
                     cnt += 1;
                     Ok(cnt < limit)
                 })
        .unwrap()
}
