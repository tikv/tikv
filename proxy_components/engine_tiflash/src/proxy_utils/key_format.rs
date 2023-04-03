// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// Some data may be migrated from kv engine to raft engine in the future,
// so kv engine and raft engine may write and delete the same key in the code
// base. To distinguish data managed by kv engine and raft engine, we prepend an
// `0x02` to the key written by kv engine.
// So kv engine won't scan any key from raft engine, and vice versa.

const RAFT_ENGINE_PREFIX: u8 = 0x01;
const KV_ENGINE_PREFIX: u8 = 0x02;

#[inline]
fn add_prefix(key: &[u8], prefix: u8) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.len() + 1);
    v.push(prefix);
    v.extend_from_slice(key);
    v
}

pub fn add_raft_engine_prefix(key: &[u8]) -> Vec<u8> {
    add_prefix(key, RAFT_ENGINE_PREFIX)
}

pub fn add_kv_engine_prefix(key: &[u8]) -> Vec<u8> {
    add_prefix(key, KV_ENGINE_PREFIX)
}

#[inline]
pub fn remove_prefix(key: &[u8]) -> &[u8] {
    &key[1..]
}
