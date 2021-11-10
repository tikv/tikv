// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::errors::*;
use kvproto::raft_cmdpb::RaftCmdRequest;
use time::Timespec;

#[derive(Debug)]
pub(crate) struct Proposal {
    pub(crate) is_conf_change: bool,
    pub(crate) index: u64,
    pub(crate) term: u64,
    pub(crate) cb: Callback,

    /// `propose_time` is set to the last time when a peer starts to renew lease.
    pub propose_time: Option<Timespec>,
    pub must_pass_epoch_check: bool,
}

pub(crate) struct ApplyMsgs {
    pub(crate) msgs: Vec<ApplyMsg>,
}

pub(crate) struct ApplyBatch {
    pub(crate) msgs: Vec<ApplyMsg>,
}

pub(crate) struct Applier {}

impl Applier {
    pub(crate) fn new_from_peer(peer: &PeerFSM) -> Self {
        todo!()
    }
}

pub fn is_conf_change_cmd(msg: &RaftCmdRequest) -> bool {
    if !msg.has_admin_request() {
        return false;
    }
    let req = msg.get_admin_request();
    req.has_change_peer() || req.has_change_peer_v2()
}

#[derive(Clone)]
pub(crate) struct ApplyRouter {}

pub(crate) const APPLY_STATE_KEY: &'static str = "apply_state";
