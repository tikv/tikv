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

extern crate protobuf;

use test::Bencher;
use kvproto::eraftpb::Entry;
use kvproto::raft_cmdpb::{RaftCmdRequest, Request, CmdType};
use std::string::String;
use std::collections::HashMap;

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

fn encode(reqs: &Vec<Request>) -> Vec<u8> {
    let mut e = Entry::new();
    let mut cmd = RaftCmdRequest::new();
    let cloned_reqs = reqs.clone();
    cmd.set_requests(protobuf::RepeatedField::from_vec(cloned_reqs));
    let cmd_msg = protobuf::Message::write_to_bytes(&cmd).unwrap();
    e.set_data(cmd_msg);
    protobuf::Message::write_to_bytes(&e).unwrap()
}

fn decode(data: &Vec<u8>) {
    let entry = protobuf::parse_from_bytes::<Entry>(data).unwrap();
    protobuf::parse_from_bytes::<RaftCmdRequest>(entry.get_data()).unwrap();
}

#[bench]
fn bench_encode_one(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(b"6ba952020fbc91bad64be1ea0650bf", b"6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c");
    let reqs = generate_requests(&map);
    b.iter(|| {
        encode(&reqs);
    });
}

#[bench]
fn bench_decode_one(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(b"6ba952020fbc91bad64be1ea0650bf", b"6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c");
    let reqs = generate_requests(&map);
    let data = encode(&reqs);
    b.iter(|| {
        decode(&data);
    });
}

#[bench]
fn bench_encode_two(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(b"7fd7b5b7fc628b3cd19c56daf84dbe", b"6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c");
    map.insert(b"6ba952020fbc91bad64be1ea0650bf", b"6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c");
    let reqs = generate_requests(&map);
    b.iter(|| {
        encode(&reqs);
    });
}

#[bench]
fn bench_decode_two(b: &mut Bencher) {
    let mut map: HashMap<&[u8], &[u8]> = HashMap::new();
    map.insert(b"7fd7b5b7fc628b3cd19c56daf84dbe", b"6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c");
    map.insert(b"6ba952020fbc91bad64be1ea0650bf", b"6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c");
    let reqs = generate_requests(&map);
    let data = encode(&reqs);
    b.iter(|| {
        decode(&data);
    });
}