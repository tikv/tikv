// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ffi::interfaces_ffi::FastAddPeerRes;

#[derive(Default)]
pub struct ServerlessExtra {
    _shard_ver: u64,
    _inner_key: Vec<u8>,
    _enc_key: Vec<u8>,
    _txn_file_ref: Vec<u8>,
}

impl ServerlessExtra {
    pub fn new(_res: &FastAddPeerRes) -> Self {
        Default::default()
    }
}
