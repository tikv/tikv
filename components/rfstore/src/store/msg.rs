// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::raft_cmdpb::RaftCmdResponse;
use kvproto::{metapb, raft_cmdpb, raft_serverpb as rspb};

use super::{rlog, Peer, RaftApplyState};

pub struct PeerMsg {
    region_id: u64,
    payload: PeerMsgPayload,
}

impl PeerMsg {
    pub(crate) fn new(region_id: u64, payload: PeerMsgPayload) -> Self {
        Self { region_id, payload }
    }
}

pub(crate) enum PeerMsgPayload {
    RaftMessage(Box<rspb::RaftMessage>),
    RaftCommand(MsgRaftCommand),
    SplitRegion(MsgSplitRegion),
    ComputeResult(MsgComputeHashResult),
    Tick,
    Start,
    ApplyRes,
    ApplyRegistration(MsgRegistration),
}

pub(crate) enum StoreMsg {
    Tick,
    Start,
    RaftMessage(Box<rspb::RaftMessage>),
    GenerateEngineChangeSet(Box<kvenginepb::ChangeSet>),
}

pub struct MsgRaftCommand {
    send_time: Instant,
    request: Box<dyn rlog::RaftLog>,
    callback: Callback,
}

pub struct MsgSplitRegion {
    pub(crate) ver: u64,
    pub(crate) conf_ver: u64,

    pub(crate) split_keys: Vec<Bytes>,

    pub(crate) callback: Callback,
}

pub struct MsgComputeHashResult {
    pub index: u64,
    pub hash: Bytes,
}

pub struct MsgMergeResult {
    pub target_peer: Box<metapb::Peer>,
    pub state: bool,
}

pub struct MsgWaitFollowerSplitFiles {
    split_keys: Vec<Bytes>,
    callback: Callback,
}

pub struct MsgRegistration {
    peer: metapb::Peer,
    term: u64,
    apply_state: RaftApplyState,
    region: metapb::Region,
}

impl MsgRegistration {
    pub(crate) fn new(peer: &Peer) -> Self {
        Self {
            peer: peer.peer.clone(),
            term: peer.term(),
            apply_state: peer.get_store().apply_state(),
            region: peer.get_store().region().clone(),
        }
    }
}

#[derive(Debug)]
pub struct ReadResponse<'a> {
    pub response: RaftCmdResponse,
    pub snapshot: Option<Arc<kvengine::SnapAccess<'a>>>,
    pub txn_extra_op: TxnExtraOp,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,
}

pub type ReadCallback = Box<dyn FnOnce(ReadResponse) + Send>;
pub type WriteCallback = Box<dyn FnOnce(WriteResponse) + Send>;
pub type ExtCallback = Box<dyn FnOnce() + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callback for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
pub enum Callback {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback),
    /// Write callback.
    Write {
        cb: WriteCallback,
        /// `proposed_cb` is called after a request is proposed to the raft group successfully.
        /// It's used to notify the caller to move on early because it's very likely the request
        /// will be applied to the raftstore.
        proposed_cb: Option<ExtCallback>,
        /// `committed_cb` is called after a request is committed and before it's being applied, and
        /// it's guaranteed that the request will be successfully applied soon.
        committed_cb: Option<ExtCallback>,
    },
}

impl Callback {
    pub fn invoke_with_response(self, resp: RaftCmdResponse) {
        match self {
            Callback::None => (),
            Callback::Read(read) => {
                let resp = ReadResponse {
                    response: resp,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                };
                read(resp);
            }
            Callback::Write { cb, .. } => {
                let resp = WriteResponse { response: resp };
                cb(resp);
            }
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Callback::None)
    }
}

type PeerTick = i32;

const PEER_TICK_RAFT: PeerTick = 0;
const PEER_TICK_RAFTLOG_GC: PeerTick = 1;
const PEER_TICK_SPLIT_REGION_CHECK: PeerTick = 2;
const PEER_TICK_PD_HEARTBEAT: PeerTick = 3;
const PEER_TICK_CHECK_MERGE: PeerTick = 4;
const PEER_TICK_PEER_STALE_STATE: PeerTick = 5;

type StoreTick = i32;

const STORE_TICK_PD_HEARTBEAT: StoreTick = 0;
const STORE_TICK_CONSISTENT_CHECK: StoreTick = 1;
