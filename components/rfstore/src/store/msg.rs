// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use crate::store::{ApplyMetrics, ApplyResult, ExecResult, Proposal, RegionSnapshot, StoreTick};
use bytes::Bytes;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::{metapb, pdpb, raft_cmdpb, raft_serverpb as rspb};
use raft_proto::eraftpb;
use raftstore::store::util::KeysInfoFormatter;
use tikv_util::deadline::Deadline;
use tikv_util::escape;

use super::{rlog, Peer, RaftApplyState};

#[derive(Debug)]
pub(crate) enum PeerMsg {
    RaftMessage(rspb::RaftMessage),
    RaftCommand(RaftCommand),
    Tick,
    Start,
    ApplyRes(MsgApplyResult),
    DestroyRes{
        // ID of peer that has been destroyed.
        peer_id: u64,
        // Whether destroy request is from its target region's snapshot
        merge_from_snapshot: bool,
    },
    CasualMessage(CasualMessage),
    /// Message that can't be lost but rarely created. If they are lost, real bad
    /// things happen like some peers will be considered dead in the group.
    SignificantMsg(SignificantMsg),
    /// Ask region to report a heartbeat to PD.
    HeartbeatPd,
}

impl PeerMsg {
    pub(crate) fn take_callback(&mut self) -> Callback {
        match &mut self {
            PeerMsg::RaftCommand(cmd) => {
                std::mem::replace(&mut cmd.callback, Callback::None)
            }
            _ => Callback::None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ApplyMsg {
    Apply(MsgApply),
    Registration(MsgRegistration),
    Destroy {
        region_id: u64,
        merge_from_snapshot: bool,
    },
}

pub enum StoreMsg {
    Tick(StoreTick),
    Start { store: metapb::Store },
    StoreUnreachable { store_id: u64 },
    RaftMessage(rspb::RaftMessage),
    GenerateEngineChangeSet(kvenginepb::ChangeSet),
}

#[derive(Debug)]
pub enum MergeResultKind {
    /// Its target peer applys `CommitMerge` log.
    FromTargetLog,
    /// Its target peer receives snapshot.
    /// In step 1, this peer should mark `pending_move` is true and destroy its apply fsm.
    /// Then its target peer will remove this peer data and apply snapshot atomically.
    FromTargetSnapshotStep1,
    /// In step 2, this peer should destroy its peer fsm.
    FromTargetSnapshotStep2,
    /// This peer is no longer needed by its target peer so it can be destroyed by itself.
    /// It happens if and only if its target peer has been removed by conf change.
    Stale,
}

#[derive(Debug)]
pub struct RaftCommand {
    pub send_time: Instant,
    pub request: rlog::RaftLog,
    pub callback: Callback,
    pub deadline: Option<Deadline>,
}

impl RaftCommand {
    pub fn new(request: rlog::RaftLog, cb: Callback) -> Self {
        Self {
            send_time: Instant::now(),
            request,
            callback: cb,
            deadline: None,
        }
    }
}

#[derive(Debug)]
pub struct MsgApply {
    pub(crate) region_id: u64,
    pub(crate) term: u64,
    pub(crate) entries: Vec<eraftpb::Entry>,
    pub(crate) soft_state: Option<raft::SoftState>,
    pub(crate) cbs: Vec<Proposal>,
}

#[derive(Debug)]
pub(crate) struct MsgApplyResult {
    pub(crate) results: VecDeque<ExecResult>,
    pub(crate) apply_state: RaftApplyState,
    pub(crate) merged: bool,
    pub(crate) metrics: ApplyMetrics,
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
    pub(crate) peer: metapb::Peer,
    pub(crate) term: u64,
    pub(crate) apply_state: RaftApplyState,
    pub(crate) region: metapb::Region,
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
    pub fn write(cb: WriteCallback) -> Self {
        Self::write_ext(cb, None, None)
    }

    pub fn write_ext(
        cb: WriteCallback,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> Self {
        Callback::Write {
            cb,
            proposed_cb,
            committed_cb,
        }
    }

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
        region_epoch: metapb::RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback,
        source: Cow<'static, str>,
    },

    /// Half split the target region.
    HalfSplitRegion {
        region_epoch: metapb::RegionEpoch,
        policy: pdpb::CheckPolicy,
        source: &'static str,
    },

    /// Hash result of ComputeHash command.
    ComputeHashResult {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },

    /// Indicate a target region is overlapped.
    RegionOverlapped,

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
            CasualMessage::HalfSplitRegion { source, .. } => {
                write!(fmt, "Half Split from {}", source)
            }
            CasualMessage::RegionOverlapped => write!(fmt, "RegionOverlapped"),
            CasualMessage::ForceCompactRaftLogs => write!(fmt, "ForceCompactRaftLogs"),
            CasualMessage::QueryRegionLeaderResp { .. } => write!(fmt, "QueryRegionLeaderResp"),
        }
    }
}
