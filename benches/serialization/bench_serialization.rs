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

use std::string::String;
use std::collections::HashMap;
use test::Bencher;
use kvproto::eraftpb::Entry;
use kvproto::raft_cmdpb::{RaftCmdRequest, Request, CmdType};

fn generate_requests(map: &HashMap<&[u8], &[u8]>) -> Vec<Request> {
    let mut reqs = vec![];
    for (key, value) in map {
        let mut r = Request::new();
        r.set_cmd_type(CmdType::Put);
        r.mut_put().set_cf(String::from("tikv"));
        r.mut_put().set_key(key.to_vec());
        r.mut_put().set_value(value.to_vec());
        reqs.push(r);
    }
    reqs
}

fn encode(reqs: &[Request]) -> Vec<u8> {
    let mut e = Entry::new();
    let mut cmd = RaftCmdRequest::new();
    cmd.set_requests(protobuf::RepeatedField::from_vec(reqs.to_vec()));
    let cmd_msg = protobuf::Message::write_to_bytes(&cmd).unwrap();
    e.set_data(cmd_msg);
    protobuf::Message::write_to_bytes(&e).unwrap()
}

fn decode(data: &[u8]) {
    let entry = protobuf::parse_from_bytes::<Entry>(data).unwrap();
    protobuf::parse_from_bytes::<RaftCmdRequest>(entry.get_data()).unwrap();
}

fn gen_rand_str(len: usize) -> Vec<u8> {
    let mut bstr = Vec::new();
    for _ in 0..len {
        bstr.push(rand::random::<u8>());
    }
    bstr
}

#[bench]
fn bench_encode_one(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    let key = gen_rand_str(30);
    let value = gen_rand_str(256);
    map.insert(key.as_slice(), value.as_slice());
    let reqs = generate_requests(&map);
    b.iter(|| {
        encode(reqs.as_slice());
    });
}

#[bench]
fn bench_decode_one(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    let key = gen_rand_str(30);
    let value = gen_rand_str(256);
    map.insert(key.as_slice(), value.as_slice());
    let reqs = generate_requests(&map);
    let data = encode(&reqs);
    b.iter(|| {
        decode(data.as_slice());
    });
}

#[bench]
fn bench_encode_two(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    let key = gen_rand_str(30);
    let value_for_lock = gen_rand_str(10);
    let value_for_data = gen_rand_str(256);
    map.insert(key.as_slice(), value_for_lock.as_slice());
    map.insert(key.as_slice(), value_for_data.as_slice());
    let reqs = generate_requests(&map);
    b.iter(|| {
        encode(reqs.as_slice());
    });
}

#[bench]
fn bench_decode_two(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    let key = gen_rand_str(30);
    let value_for_lock = gen_rand_str(10);
    let value_for_data = gen_rand_str(256);
    map.insert(key.as_slice(), value_for_lock.as_slice());
    map.insert(key.as_slice(), value_for_data.as_slice());
    let reqs = generate_requests(&map);
    let data = encode(&reqs);
    b.iter(|| {
        decode(data.as_slice());
    });
}
