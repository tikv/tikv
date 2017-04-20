// Copyright 2017 PingCAP, Inc.
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

extern crate protobuf;

use std::collections::HashMap;
use test::Bencher;
use rand::{Rng, thread_rng};
use protobuf::Message;
use kvproto::eraftpb::Entry;
use kvproto::raft_cmdpb::{RaftCmdRequest, Request, CmdType};

#[inline]
fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut rand_str = vec![0; len];
    thread_rng().fill_bytes(&mut rand_str);
    rand_str
}

#[inline]
fn generate_requests(map: &HashMap<&[u8], &[u8]>) -> Vec<Request> {
    let mut reqs = vec![];
    for (key, value) in map {
        let mut r = Request::new();
        r.set_cmd_type(CmdType::Put);
        r.mut_put().set_cf("tikv".to_owned());
        r.mut_put().set_key(key.to_vec());
        r.mut_put().set_value(value.to_vec());
        reqs.push(r);
    }
    reqs
}

fn encode(map: &HashMap<&[u8], &[u8]>) -> Vec<u8> {
    let mut e = Entry::new();
    let mut cmd = RaftCmdRequest::new();
    let reqs = generate_requests(map);
    cmd.set_requests(protobuf::RepeatedField::from_vec(reqs));
    let cmd_msg = cmd.write_to_bytes().unwrap();
    e.set_data(cmd_msg);
    e.write_to_bytes().unwrap()
}

fn decode(data: &[u8]) {
    let mut entry = Entry::new();
    entry.merge_from_bytes(data).unwrap();
    let mut cmd = RaftCmdRequest::new();
    cmd.merge_from_bytes(entry.get_data()).unwrap();
}

#[bench]
fn bench_encode_one(b: &mut Bencher) {
    let key = gen_rand_str(30);
    let value = gen_rand_str(256);
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(&key, &value);
    b.iter(|| {
        encode(&map);
    });
}

#[bench]
fn bench_decode_one(b: &mut Bencher) {
    let key = gen_rand_str(30);
    let value = gen_rand_str(256);
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(&key, &value);
    let data = encode(&map);
    b.iter(|| {
        decode(&data);
    });
}

#[bench]
fn bench_encode_two(b: &mut Bencher) {
    let key_for_lock = gen_rand_str(30);
    let value_for_lock = gen_rand_str(10);
    let key_for_data = gen_rand_str(30);
    let value_for_data = gen_rand_str(256);
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(&key_for_lock, &value_for_lock);
    map.insert(&key_for_data, &value_for_data);
    b.iter(|| {
        encode(&map);
    });
}

#[bench]
fn bench_decode_two(b: &mut Bencher) {
    let key_for_lock = gen_rand_str(30);
    let value_for_lock = gen_rand_str(10);
    let key_for_data = gen_rand_str(30);
    let value_for_data = gen_rand_str(256);
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(&key_for_lock, &value_for_lock);
    map.insert(&key_for_data, &value_for_data);
    let data = encode(&map);
    b.iter(|| {
        decode(&data);
    });
}
