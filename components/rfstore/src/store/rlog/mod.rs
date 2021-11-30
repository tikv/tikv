// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod custom;

use core::panicking::panic;
use bytes::{Buf, Bytes};
pub use custom::*;
use kvproto::raft_cmdpb;
use kvproto::raft_cmdpb::{RaftCmdRequest, Request, CmdType};
use protobuf::Message;

#[derive(Debug)]
pub enum RaftLog {
    Request(raft_cmdpb::RaftCmdRequest),
    Custom(CustomRaftLog),
}

impl RaftLog {
    pub fn is_conf_change(&self) -> bool {
        if let RaftLog::Request(cmd) = self {
            if cmd.has_admin_request() {
                let admin = cmd.get_admin_request();
                return admin.has_change_peer() || admin.has_change_peer_v2()
            }
        }
        false
    }

    pub fn get_epoch(&self) -> (u64, u64) {
        match &self {
            RaftLog::Request(cmd) => {
                let epoch = cmd.get_header().get_region_epoch();
                (epoch.get_version(), epoch.get_conf_ver())
            },
            RaftLog::Custom(custom) => {
                (custom.epoch.ver as u64, custom.epoch.conf_ver as u64)
            }
        }
    }

    pub fn get_store_id(&self) -> u64 {
        match &self {
            RaftLog::Request(cmd) => {
                cmd.get_header().get_peer().store_id
            },
            RaftLog::Custom(custom) => {
                custom.store_id
            }
        }
    }

    pub fn get_term(&self) -> u64 {
        match &self {
            RaftLog::Request(cmd) => {
                cmd.get_header().get_term()
            },
            RaftLog::Custom(_) => {
                0
            }
        }
    }

    pub fn get_region_id(&self) -> u64 {
        match &self {
            RaftLog::Request(cmd) => {
                cmd.get_header().get_region_id()
            },
            RaftLog::Custom(custom) => {
                custom.region_id
            }
        }
    }

    pub fn get_peer_id(&self) -> u64 {
        match &self {
            RaftLog::Request(cmd) => {
                cmd.get_header().get_peer().get_id()
            },
            RaftLog::Custom(custom) => {
                custom.peer_id
            }
        }
    }

    pub fn get_cmd_request(&self) -> Option<&RaftCmdRequest> {
        if let RaftLog::Request(cmd) = self {
            Some(cmd)
        } else {
            None
        }
    }

    pub fn take_cmd_request(self) -> RaftCmdRequest {
        match self {
            RaftLog::Request(cmd) => cmd,
            RaftLog::Custom(_) => panic!("invalid call")
        }
    }

    pub fn get_requests(&self) -> &[Request] {
        if let RaftLog::Request(cmd) = self {
            cmd.get_requests()
        } else {
            &[]
        }
    }

    pub fn get_replica_read(&self) -> bool {
        if let RaftLog::Request(cmd) = self {
            cmd.get_header().get_replica_read()
        } else {
            false
        }
    }

    pub fn has_read_and_write(&self) -> (bool, bool) {
        let mut has_read = false;
        let mut has_write = false;
        match &self {
            RaftLog::Request(cmd) => {
                for r in cmd.get_requests() {
                    match r.get_cmd_type() {
                        CmdType::Get | CmdType::Snap | CmdType::ReadIndex => has_read = true,
                        _ => {}
                    }
                }
            },
            RaftLog::Custom(_) => {
                has_write = true;
            }
        }
        (has_read, has_write)
    }

    pub fn is_read_quorum(&self) -> bool {
        if let RaftLog::Request(cmd) = self {
            cmd.get_header().get_read_quorum()
        } else {
            false
        }
    }
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

pub fn decode_log(entry_data: Bytes) -> RaftLog {
    let rlog: RaftLog;
    if entry_data[0] == CUSTOM_FLAG {
        rlog = RaftLog::Custom(CustomRaftLog::new_from_data(entry_data))
    } else {
        let mut req = raft_cmdpb::RaftCmdRequest::default();
        req.merge_from_bytes(entry_data.chunk());
        rlog = RaftLog::Request(req)
    }
    rlog
}
