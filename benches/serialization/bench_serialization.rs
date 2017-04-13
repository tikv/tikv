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

use test::Bencher;
extern crate kvproto;
extern crate protobuf;
use self::protobuf::Message;
use self::kvproto::eraftpb::Entry;
use self::kvproto::raft_cmdpb::{RaftCmdRequest, Request, CmdType};
use std::time::{Instant, Duration};
use std::string::String;

#[inline]
fn encode_one_cmd() -> Vec<u8> {
    let mut e = Entry::new();
    let mut cmd = RaftCmdRequest::new();
    let mut reqs = vec![];

    let mut r = Request::new();
    r.set_cmd_type(CmdType::Put);
    r.mut_put().set_cf(String::from("tidb"));
    let key = Vec::from(String::from("7fd7b5b7fc628b3cd19c56daf84dbe").as_bytes());
    r.mut_put().set_key(key);
    let value = Vec::from(String::from("6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c").as_bytes());
    r.mut_put().set_value(value);
    reqs.push(r);

    cmd.set_requests(protobuf::RepeatedField::from_vec(reqs));
    let cmd_msg = protobuf::Message::write_to_bytes(&cmd).unwrap();
    e.set_data(cmd_msg);
    protobuf::Message::write_to_bytes(&e).unwrap()
}

#[inline]
fn encode_two_cmd() -> Vec<u8> {
    let mut e = Entry::new();
    let mut cmd = RaftCmdRequest::new();
    let mut reqs = vec![];

    let mut r = Request::new();
    r.set_cmd_type(CmdType::Put);
    r.mut_put().set_cf(String::from("tidb"));
    let key = Vec::from(String::from("7fd7b5b7fc628b3cd19c56daf84dbe").as_bytes());
    r.mut_put().set_key(key);
    let value = Vec::from(String::from("0650bfba52").as_bytes());
    r.mut_put().set_value(value);
    reqs.push(r);

    let mut r = Request::new();
    r.set_cmd_type(CmdType::Put);
    r.mut_put().set_cf(String::from("tidb"));
    let key = Vec::from(String::from("6ba952020fbc91bad64be1ea0650bfba52e6aab4").as_bytes());
    r.mut_put().set_key(key);
    let value = Vec::from(String::from("6ba952020fbc91bad64be1ea0650bfba52e6aab4049b9e4e8067b998e4581d026b0bc6d1113ab9f507aaca3a0724000e735a558d4c23b600512346d9024aa9a345e92aa1926517c4d9b16bd83e74c10d6ba952020fbc91bad64be1ea0650bfba52e6aab4024aa9a345e92aa1926517c4d9b16bd83e74c10d7fd7b5b7fc628b3c").as_bytes());
    r.mut_put().set_value(value);
    reqs.push(r);

    cmd.set_requests(protobuf::RepeatedField::from_vec(reqs));
    let cmd_msg = protobuf::Message::write_to_bytes(&cmd).unwrap();
    e.set_data(cmd_msg);
    protobuf::Message::write_to_bytes(&e).unwrap()
}

fn decode(data: &Vec<u8>) {
    let entry = protobuf::parse_from_bytes::<Entry>(data).unwrap();
    let cmd = protobuf::parse_from_bytes::<RaftCmdRequest>(entry.get_data());
}

fn print_cost(s: &str, d: Duration, count: u32) {
    let cost = (d.as_secs() * 1000) as f64 + (d.subsec_nanos() / 1000_000) as f64;
    let cost = cost / count as f64;
    print!("{}", s);
    print!(" cost {}ms\t...\t", cost);
}

#[bench]
fn bench_encode_one_cmd(b: &mut Bencher) {
    let count = 1000000;
    let now = Instant::now();
    for _ in 0..count {
        encode_one_cmd();
    }
    print_cost("encode_one_cmd", now.elapsed(), count);
}

#[bench]
fn bench_decode_one_cmd(b: &mut Bencher) {
    let result = encode_one_cmd();
    let count = 1000000;
    let now = Instant::now();
    for _ in 0..count {
        decode(&result);
    }
    print_cost("decode_one_cmd", now.elapsed(), count);
}

#[bench]
fn bench_encode_two_cmd(b: &mut Bencher) {
    let count = 1000000;

    let now = Instant::now();
    for _ in 0..count{
        encode_two_cmd();
    }
    print_cost("encode_two_cmd", now.elapsed(), count);
}

#[bench]
fn bench_decode_two_cmd(b: &mut Bencher) {
    let result = encode_two_cmd();
    let count = 1000000;
    let now = Instant::now();
    for _ in 0..count {
        decode(&result);
    }
    print_cost("decode_two_cmd", now.elapsed(), count);
}
