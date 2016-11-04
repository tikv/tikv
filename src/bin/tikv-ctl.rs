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
extern crate tempdir;

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
use tikv::storage::{ALL_CFS, CF_RAFT, CF_LOCK, CF_WRITE, CF_DEFAULT, CfName};
use tikv::storage::mvcc::{Lock, Write};
use tikv::storage::types::Key;

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
            CF_DEFAULT => {
                dump_mvcc_default(&db, key);
            }
            CF_LOCK => {
                dump_mvcc_lock(&db, key);
            }
            CF_WRITE => {
                dump_mvcc_write(&db, key);
            }
            "all" => {
                dump_mvcc_default(&db, key);
                dump_mvcc_lock(&db, key);
                dump_mvcc_write(&db, key);
            }
            _ => {
                println!("The cf: {} cannot be dumped", cf_name);
                let _ = app.print_help();
            }
        }
    } else {
        let _ = app.print_help();
    }

}

fn gen_key_prefix(key: &str) -> Vec<u8> {
    let encoded = encode_bytes(unescape(key).as_slice());
    keys::data_key(encoded.as_slice())
}

pub trait MvccDeserializable {
    fn deserialize(bytes: &[u8]) -> Self;
}

impl MvccDeserializable for Write {
    fn deserialize(bytes: &[u8]) -> Self {
        Write::parse(bytes).unwrap()
    }
}

impl MvccDeserializable for Lock {
    fn deserialize(bytes: &[u8]) -> Self {
        Lock::parse(bytes).unwrap()
    }
}

impl MvccDeserializable for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
}

pub struct MvccKv<T> {
    key: Key,
    value: T,
}

pub fn gen_mvcc_iter<T: MvccDeserializable>(db: &DB,
                                            key_prefix: &str,
                                            mvcc_type: CfName)
                                            -> Option<Vec<MvccKv<T>>> {
    let prefix = gen_key_prefix(key_prefix);
    let mut iter = db.new_iterator_cf(mvcc_type, None, false).unwrap();
    iter.seek(prefix.as_slice().into());
    if !iter.valid() {
        None
    } else {
        let kvs = iter.map(|s| {
            let key = &keys::origin_key(&s.0);
            MvccKv {
                key: Key::from_encoded(key.to_vec()),
                value: T::deserialize(&s.1),
            }
        });
        Some(kvs.take_while(|s| s.key.raw().unwrap().starts_with(key_prefix.as_bytes()))
            .collect())
    }
}


fn dump_mvcc_default(db: &DB, key: &str) {
    let kvs: Vec<MvccKv<Vec<u8>>> = gen_mvcc_iter(&db, key, CF_DEFAULT).unwrap();
    for kv in kvs {
        let ts = kv.key.decode_ts().unwrap();
        let key = kv.key.truncate_ts().unwrap();
        println!("Key: {:?}", escape(key.raw().unwrap().as_slice()));
        println!("Value: {:?}", escape(kv.value.as_slice()));
        println!("Start_ts: {:?}", ts);
        println!("");
    }
}

fn dump_mvcc_lock(db: &DB, key: &str) {
    let kvs: Vec<MvccKv<Lock>> = gen_mvcc_iter(&db, key, CF_LOCK).unwrap();
    for kv in kvs {
        let lock = &kv.value;
        println!("Key: {:?}", escape(kv.key.raw().unwrap().as_slice()));
        println!("Primary: {:?}", escape(lock.primary.as_slice()));
        println!("Type: {:?}", lock.lock_type);
        println!("Start_ts: {:?}", lock.ts);
        println!("");
    }
}

fn dump_mvcc_write(db: &DB, key: &str) {
    let kvs: Vec<MvccKv<Write>> = gen_mvcc_iter(&db, key, CF_WRITE).unwrap();
    for kv in kvs {
        let write = &kv.value;
        let commit_ts = kv.key.decode_ts().unwrap();
        let key = kv.key.truncate_ts().unwrap();
        println!("Key: {:?}", escape(key.raw().unwrap().as_slice()));
        println!("Type: {:?}", write.write_type);
        println!("Start_ts: {:?}", write.start_ts);
        println!("Commit_ts: {:?}", commit_ts);
        println!("");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str;
    use rocksdb::Writable;
    use tikv::util::codec::bytes::encode_bytes;
    use tikv::util::codec::number::NumberEncoder;
    use tikv::raftstore::store::keys;
    use tikv::storage::{ALL_CFS, CF_LOCK, CF_WRITE, CF_DEFAULT};
    use tikv::storage::mvcc::{Lock, Write, LockType, WriteType};
    use tempdir::TempDir;
    use tikv::util::rocksdb::new_engine;
    use tikv::storage::types::Key;

    const PREFIX: &'static [u8] = b"k";

    #[test]
    fn test_ctl_mvcc() {
        let tmp_dir = TempDir::new("mvcc_tmp").expect("create mvcc_tmp dir");
        //        let file_path = tmp_dir.path().join("tmp_db");
        let db = new_engine(tmp_dir.path().to_str().unwrap(), ALL_CFS).unwrap();
        let test_data = vec![(PREFIX, b"v", 5), (PREFIX, b"x", 10), (PREFIX, b"y", 15)];
        for &(k, v, ts) in &test_data {
            let key = keys::data_key(Key::from_raw(k).append_ts(ts).encoded().as_slice());
            db.put(key.as_slice(), v).unwrap();
        }
        let kvs_gen: Vec<MvccKv<Vec<u8>>> = gen_mvcc_iter(&db, "k", CF_DEFAULT).unwrap();
        let mut test_iter = test_data.clone();
        assert_eq!(test_iter.len(), kvs_gen.len());
        for kv in kvs_gen {
            let ts = kv.key.decode_ts().unwrap();
            let key = kv.key.truncate_ts().unwrap().raw().unwrap();
            let value = kv.value.as_slice();
            test_iter.retain(|&s| !(&s.0[..] == key.as_slice() && &s.1[..] == value && s.2 == ts));
        }
        assert_eq!(test_iter.len(), 0);

        // Test MVCC Lock
        let test_data_lock = vec![(b"kv", LockType::Put, b"v", 5),
                                  (b"kx", LockType::Lock, b"x", 10),
                                  (b"kz", LockType::Delete, b"y", 15)];
        let keys: Vec<_> = test_data_lock.iter()
            .map(|data| {
                let encoded = encode_bytes(&data.0[..]);
                keys::data_key(encoded.as_slice())
            })
            .collect();
        let lock_value: Vec<_> = test_data_lock.iter()
            .map(|data| Lock::new(data.1, data.2.to_vec(), data.3).to_bytes())
            .collect();
        let kvs = keys.iter().zip(lock_value.iter());
        let lock_cf = db.cf_handle(CF_LOCK).unwrap();
        for (k, v) in kvs {
            db.put_cf(lock_cf, k.as_slice(), v.as_slice()).unwrap();
        }
        fn assert_iter(kvs_gen: &[MvccKv<Lock>], test_data: (&[u8; 2], LockType, &[u8; 1], u64)) {
            assert_eq!(kvs_gen.len(), 1);
            let kv = &kvs_gen[0];
            let lock = &kv.value;
            let key = kv.key.raw().unwrap();
            assert!(&key[..] == test_data.0 && test_data.1 == lock.lock_type &&
                    &test_data.2[..] == &lock.primary[..] &&
                    test_data.3 == lock.ts);
        }
        assert_iter(&gen_mvcc_iter(&db, "kv", CF_LOCK).unwrap(),
                    test_data_lock[0]);
        assert_iter(&gen_mvcc_iter(&db, "kx", CF_LOCK).unwrap(),
                    test_data_lock[1]);
        assert_iter(&gen_mvcc_iter(&db, "kz", CF_LOCK).unwrap(),
                    test_data_lock[2]);

        // Test MVCC Write
        let test_data_write = vec![(PREFIX, WriteType::Delete, 5, 10),
                                   (PREFIX, WriteType::Lock, 15, 20),
                                   (PREFIX, WriteType::Put, 25, 30),
                                   (PREFIX, WriteType::Rollback, 35, 40)];
        let keys: Vec<_> = test_data_write.iter()
            .map(|data| {
                let encoded = encode_bytes(&data.0[..]);
                let mut d = keys::data_key(encoded.as_slice());
                let _ = d.encode_u64_desc(data.3);
                d
            })
            .collect();
        let write_value: Vec<_> = test_data_write.iter()
            .map(|data| Write::new(data.1, data.2).to_bytes())
            .collect();
        let kvs = keys.iter().zip(write_value.iter());
        let write_cf = db.cf_handle(CF_WRITE).unwrap();
        for (k, v) in kvs {
            db.put_cf(write_cf, k.as_slice(), v.as_slice()).unwrap();
        }
        let kvs_gen: Vec<MvccKv<Write>> = gen_mvcc_iter(&db, "k", CF_WRITE).unwrap();
        let mut test_iter = test_data_write.clone();
        assert_eq!(test_iter.len(), kvs_gen.len());
        for kv in kvs_gen {
            let write = &kv.value;
            let ts = kv.key.decode_ts().unwrap();
            let key = kv.key.truncate_ts().unwrap().raw().unwrap();
            test_iter.retain(|&s| {
                !(&s.0[..] == key.as_slice() && s.1 == write.write_type && s.2 == write.start_ts &&
                  s.3 == ts)
            });
        }
        assert_eq!(test_iter.len(), 0);
    }
}
