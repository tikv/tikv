// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use kvproto::raft_cmdpb;

pub struct RequestRaftLog {
    req: raft_cmdpb::RaftCmdRequest,
}

impl RequestRaftLog {
    pub fn new(req: raft_cmdpb::RaftCmdRequest) -> Self {
        Self { req }
    }
}

impl RaftLog for RequestRaftLog {
    fn region_id(&self) -> u64 {
        todo!()
    }

    fn epoch(&self) -> Epoch {
        todo!()
    }

    fn peer_id(&self) -> u64 {
        todo!()
    }

    fn store_id(&self) -> u64 {
        todo!()
    }

    fn term(&self) -> u64 {
        todo!()
    }

    fn marshal(&self) -> Bytes {
        todo!()
    }

    fn get_raft_cmd_request(&self) -> Option<raft_cmdpb::RaftCmdRequest> {
        todo!()
    }
}
