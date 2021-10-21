// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::store::RegionSnapshot;
use bytes::Bytes;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::RegionEpoch;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::{metapb, raft_cmdpb, raft_serverpb as rspb};
use raftstore::store::util::KeysInfoFormatter;
use tikv_util::deadline::Deadline;
use tikv_util::escape;

use super::{rlog, Peer, RaftApplyState};

#[derive(Debug)]
pub struct PeerMsg {
    region_id: u64,
    payload: PeerMsgPayload,
}

impl PeerMsg {
    pub(crate) fn new(region_id: u64, payload: PeerMsgPayload) -> Self {
        Self { region_id, payload }
    }

    pub(crate) fn get_callback(&mut self) -> Callback {
        match &mut self.payload {
            PeerMsgPayload::RaftCommand(cmd) => {
                std::mem::replace(&mut cmd.callback, Callback::None)
            }
            _ => Callback::None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum PeerMsgPayload {
    RaftMessage(rspb::RaftMessage),
    RaftCommand(RaftCommand),
    SplitRegion(MsgSplitRegion),
    ComputeResult(MsgComputeHashResult),
    Tick,
    Start,
    ApplyRes,
    ApplyRegistration(MsgRegistration),
    CasualMessage(CasualMessage),
    /// Message that can't be lost but rarely created. If they are lost, real bad
    /// things happen like some peers will be considered dead in the group.
    SignificantMsg(SignificantMsg),
}

pub enum StoreMsg {
    Tick,
    Start,
    StoreUnreachable { store_id: u64 },
    RaftMessage(Box<rspb::RaftMessage>),
    GenerateEngineChangeSet(Box<kvenginepb::ChangeSet>),
}

#[derive(Debug)]
pub struct RaftCommand {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub callback: Callback,
    pub deadline: Option<Deadline>,
}

impl RaftCommand {
    pub fn new(request: RaftCmdRequest, cb: Callback) -> Self {
        Self {
            send_time: Instant::now(),
            request,
            callback: cb,
            deadline: None,
        }
    }
}

#[derive(Debug)]
pub struct MsgSplitRegion {
    pub(crate) ver: u64,
    pub(crate) conf_ver: u64,

    pub(crate) split_keys: Vec<Bytes>,

    pub(crate) callback: Callback,
}

#[derive(Debug)]
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

#[derive(Debug)]
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
pub struct ReadResponse {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot>,
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

    pub fn invoke_read(self, args: ReadResponse) {
        match self {
            Callback::Read(read) => read(args),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Callback::None)
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write { .. } => write!(fmt, "Callback::Write(..)"),
        }
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

/// Some significant messages sent to raftstore. Raftstore will dispatch these messages to Raft
/// groups to update some important internal status.
#[derive(Debug)]
pub enum SignificantMsg {
    StoreUnreachable {
        store_id: u64,
    },
    /// Reports `to_peer_id` is unreachable.
    Unreachable {
        region_id: u64,
        to_peer_id: u64,
    },
    StoreResolved {
        store_id: u64,
        group_id: u64,
    },
    LeaderCallback(Callback),
}

/// Message that will be sent to a peer.
///
/// These messages are not significant and can be dropped occasionally.
pub enum CasualMessage {
    /// Split the target region into several partitions.
    SplitRegion {
        region_epoch: RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback,
        source: Cow<'static, str>,
    },

    /// Hash result of ComputeHash command.
    ComputeHashResult {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },

    /// Approximate size of target region. This message can only be sent by split-check thread.
    RegionApproximateSize {
        size: u64,
    },

    /// Approximate key count of target region.
    RegionApproximateKeys {
        keys: u64,
    },
    CompactionDeclinedBytes {
        bytes: u64,
    },
    /// Clear region size cache.
    ClearRegionSize,
    /// Indicate a target region is overlapped.
    RegionOverlapped,
    /// Notifies that a new snapshot has been generated.
    SnapshotGenerated,

    /// Generally Raft leader keeps as more as possible logs for followers,
    /// however `ForceCompactRaftLogs` only cares the leader itself.
    ForceCompactRaftLogs,

    /// Region info from PD
    QueryRegionLeaderResp {
        region: metapb::Region,
        leader: metapb::Peer,
    },
}

impl fmt::Debug for CasualMessage {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CasualMessage::ComputeHashResult {
                index,
                context,
                ref hash,
            } => write!(
                fmt,
                "ComputeHashResult [index: {}, context: {}, hash: {}]",
                index,
                log_wrappers::Value::key(&context),
                escape(hash)
            ),
            CasualMessage::SplitRegion {
                ref split_keys,
                source,
                ..
            } => write!(
                fmt,
                "Split region with {} from {}",
                KeysInfoFormatter(split_keys.iter()),
                source,
            ),
            CasualMessage::RegionApproximateSize { size } => {
                write!(fmt, "Region's approximate size [size: {:?}]", size)
            }
            CasualMessage::RegionApproximateKeys { keys } => {
                write!(fmt, "Region's approximate keys [keys: {:?}]", keys)
            }
            CasualMessage::CompactionDeclinedBytes { bytes } => {
                write!(fmt, "compaction declined bytes {}", bytes)
            }
            CasualMessage::ClearRegionSize => write! {
                fmt,
                "clear region size"
            },
            CasualMessage::RegionOverlapped => write!(fmt, "RegionOverlapped"),
            CasualMessage::SnapshotGenerated => write!(fmt, "SnapshotGenerated"),
            CasualMessage::ForceCompactRaftLogs => write!(fmt, "ForceCompactRaftLogs"),
            CasualMessage::QueryRegionLeaderResp { .. } => write!(fmt, "QueryRegionLeaderResp"),
        }
    }
}
