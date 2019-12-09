// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, Request};
use raft::eraftpb::Entry;

use protobuf::{self, Message};
use rand::{thread_rng, RngCore};
use test::Bencher;

use tikv_util::collections::HashMap;

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
        let mut r = Request::default();
        r.set_cmd_type(CmdType::Put);
        r.mut_put().set_cf("tikv".to_owned());
        r.mut_put().set_key(key.to_vec());
        r.mut_put().set_value(value.to_vec());
        reqs.push(r);
    }
    reqs
}

fn encode(map: &HashMap<&[u8], &[u8]>) -> Vec<u8> {
    let mut e = Entry::default();
    let mut cmd = RaftCmdRequest::default();
    let reqs = generate_requests(map);
    cmd.set_requests(reqs.into());
    let cmd_msg = cmd.write_to_bytes().unwrap();
    e.set_data(cmd_msg);
    e.write_to_bytes().unwrap()
}

fn decode(data: &[u8]) {
    let mut entry = Entry::default();
    entry.merge_from_bytes(data).unwrap();
    let mut cmd = RaftCmdRequest::default();
    cmd.merge_from_bytes(entry.get_data()).unwrap();
}

#[bench]
fn bench_encode_one(b: &mut Bencher) {
    let key = gen_rand_str(30);
    let value = gen_rand_str(256);
    let mut map: HashMap<&[u8], &[u8]> = HashMap::default();
    map.insert(&key, &value);
    b.iter(|| {
        encode(&map);
    });
}

#[bench]
fn bench_decode_one(b: &mut Bencher) {
    let key = gen_rand_str(30);
    let value = gen_rand_str(256);
    let mut map: HashMap<&[u8], &[u8]> = HashMap::default();
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
    let mut map: HashMap<&[u8], &[u8]> = HashMap::default();
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
    let mut map: HashMap<&[u8], &[u8]> = HashMap::default();
    map.insert(&key_for_lock, &value_for_lock);
    map.insert(&key_for_data, &value_for_data);
    let data = encode(&map);
    b.iter(|| {
        decode(&data);
    });
}
