// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod custom;
pub mod request;

use bytes::Bytes;
pub use custom::*;
use kvproto::raft_cmdpb;
pub use request::*;

pub trait RaftLog {
    fn region_id(&self) -> u64;
    fn epoch(&self) -> Epoch;
    fn peer_id(&self) -> u64;
    fn store_id(&self) -> u64;
    fn term(&self) -> u64;
    fn marshal(&self) -> Bytes;
    fn get_raft_cmd_request(&self) -> Option<raft_cmdpb::RaftCmdRequest>;
}

#[derive(Debug, Clone, Copy)]
pub struct Epoch {
    ver: u32,
    conf_ver: u32,
}

impl Epoch {
    pub fn new(ver: u32, conf_ver: u32) -> Self {
        Self { ver, conf_ver }
    }

    pub fn version(&self) -> u32 {
        self.ver
    }

    pub fn conf_version(&self) -> u32 {
        self.conf_ver
    }
}

pub fn decode_log(entry_data: &[u8]) -> Box<dyn RaftLog> {
    todo!();
}
