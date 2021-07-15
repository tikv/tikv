// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::time::Instant;

use engine_rocks::RocksEngine;
use engine_traits::KvEngine;
use kvproto::import_sstpb::SstMeta;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb;
use kvproto::metapb::RegionEpoch;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_serverpb::RaftMessage;
use raft::SnapshotStatus;
use std::borrow::Cow;
use txn_types::TxnExtra;

use crate::store::fsm::apply::TaskRes as ApplyTaskRes;
use crate::store::fsm::apply::{CatchUpLogs, ChangeCmd};
use crate::store::fsm::PeerFsm;
use crate::store::util::KeysInfoFormatter;
use crate::store::SnapKey;
use engine_rocks::CompactedEvent;
use tikv_util::escape;

use super::RegionSnapshot;

#[derive(Debug, Clone)]
pub struct ReadResponse<E: KvEngine> {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot<E>>,
    pub txn_extra_op: TxnExtraOp,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,
}

pub type ReadCallback<E> = Box<dyn FnOnce(ReadResponse<E>) + Send>;
pub type WriteCallback = Box<dyn FnOnce(WriteResponse) + Send>;
pub type ExtCallback = Box<dyn FnOnce() + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callbak for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
pub enum Callback<E: KvEngine> {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback<E>),
    /// Write callback.
    Write {
        cb: WriteCallback,
        /// `proposed_cb` is called after a request is proposed to the raft group successfully.
        /// It's used to notify the caller to move on early because it's very likely the request
        /// will be applied to the raftstore.
        proposed_cb: Option<ExtCallback>,
    },
}

impl<E> Callback<E>
where
    E: KvEngine,
{
    pub fn write(cb: WriteCallback) -> Self {
        Self::write_ext(cb, None)
    }

    pub fn write_ext(cb: WriteCallback, proposed_cb: Option<ExtCallback>) -> Self {
        Callback::Write { cb, proposed_cb }
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

    pub fn invoke_proposed(&mut self) {
        if let Callback::Write { proposed_cb, .. } = self {
            if let Some(cb) = proposed_cb.take() {
                cb()
            }
        }
    }

    pub fn invoke_read(self, args: ReadResponse<E>) {
        match self {
            Callback::Read(read) => read(args),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            Callback::None => true,
            _ => false,
        }
    }
}

impl<E> fmt::Debug for Callback<E>
where
    E: KvEngine,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write { .. } => write!(fmt, "Callback::Write(..)"),
        }
    }
}

bitflags! {
    pub struct PeerTicks: u8 {
        const RAFT                   = 0b00000001;
        const RAFT_LOG_GC            = 0b00000010;
        const SPLIT_REGION_CHECK     = 0b00000100;
        const PD_HEARTBEAT           = 0b00001000;
        const CHECK_MERGE            = 0b00010000;
        const CHECK_PEER_STALE_STATE = 0b00100000;
    }
}

impl PeerTicks {
    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTicks::RAFT => "raft",
            PeerTicks::RAFT_LOG_GC => "raft_log_gc",
            PeerTicks::SPLIT_REGION_CHECK => "split_region_check",
            PeerTicks::PD_HEARTBEAT => "pd_heartbeat",
            PeerTicks::CHECK_MERGE => "check_merge",
            PeerTicks::CHECK_PEER_STALE_STATE => "check_peer_stale_state",
            _ => unreachable!(),
        }
    }
    pub fn get_all_ticks() -> &'static [PeerTicks] {
        const TICKS: &[PeerTicks] = &[
            PeerTicks::RAFT,
            PeerTicks::RAFT_LOG_GC,
            PeerTicks::SPLIT_REGION_CHECK,
            PeerTicks::PD_HEARTBEAT,
            PeerTicks::CHECK_MERGE,
            PeerTicks::CHECK_PEER_STALE_STATE,
        ];
        TICKS
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StoreTick {
    CompactCheck,
    PdStoreHeartbeat,
    SnapGc,
    CompactLockCf,
    ConsistencyCheck,
    CleanupImportSST,
}

impl StoreTick {
    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            StoreTick::CompactCheck => "compact_check",
            StoreTick::PdStoreHeartbeat => "pd_store_heartbeat",
            StoreTick::SnapGc => "snap_gc",
            StoreTick::CompactLockCf => "compact_lock_cf",
            StoreTick::ConsistencyCheck => "consistency_check",
            StoreTick::CleanupImportSST => "cleanup_import_sst",
        }
    }
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

/// Some significant messages sent to raftstore. Raftstore will dispatch these messages to Raft
/// groups to update some important internal status.
#[derive(Debug)]
pub enum SignificantMsg {
    /// Reports whether the snapshot sending is successful or not.
    SnapshotStatus {
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    },
    StoreUnreachable {
        store_id: u64,
    },
    /// Reports `to_peer_id` is unreachable.
    Unreachable {
        region_id: u64,
        to_peer_id: u64,
    },
    /// Source region catch up logs for merging
    CatchUpLogs(CatchUpLogs),
    /// Result of the fact that the region is merged.
    MergeResult {
        target_region_id: u64,
        target: metapb::Peer,
        result: MergeResultKind,
    },
    /// Capture the changes of the region.
    CaptureChange {
        cmd: ChangeCmd,
        region_epoch: RegionEpoch,
        callback: Callback<RocksEngine>,
    },
    LeaderCallback(Callback<RocksEngine>),
}

/// Message that will be sent to a peer.
///
/// These messages are not significant and can be dropped occasionally.
pub enum CasualMessage<E: KvEngine> {
    /// Split the target region into several partitions.
    SplitRegion {
        region_epoch: RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback<E>,
        source: Cow<'static, str>,
    },

    /// Hash result of ComputeHash command.
    ComputeHashResult {
        index: u64,
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
    /// Half split the target region.
    HalfSplitRegion {
        region_epoch: RegionEpoch,
        policy: CheckPolicy,
        source: &'static str,
    },
    /// Remove snapshot files in `snaps`.
    GcSnap {
        snaps: Vec<(SnapKey, bool)>,
    },
    /// Clear region size cache.
    ClearRegionSize,
    /// Indicate a target region is overlapped.
    RegionOverlapped,
    /// Notifies that a new snapshot has been generated.
    SnapshotGenerated,

    /// A message to access peer's internal state.
    AccessPeer(Box<dyn FnOnce(&mut PeerFsm<E>) + Send + 'static>),

    /// Region info from PD
    QueryRegionLeaderResp {
        region: metapb::Region,
        leader: metapb::Peer,
    },
}

impl<E: KvEngine> fmt::Debug for CasualMessage<E> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CasualMessage::ComputeHashResult { index, ref hash } => write!(
                fmt,
                "ComputeHashResult [index: {}, hash: {}]",
                index,
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
            CasualMessage::HalfSplitRegion { source, .. } => {
                write!(fmt, "Half Split from {}", source)
            }
            CasualMessage::GcSnap { ref snaps } => write! {
                fmt,
                "gc snaps {:?}",
                snaps
            },
            CasualMessage::ClearRegionSize => write! {
                fmt,
                "clear region size"
            },
            CasualMessage::RegionOverlapped => write!(fmt, "RegionOverlapped"),
            CasualMessage::SnapshotGenerated => write!(fmt, "SnapshotGenerated"),
            CasualMessage::AccessPeer(_) => write!(fmt, "AccessPeer"),
            CasualMessage::QueryRegionLeaderResp { .. } => write!(fmt, "QueryRegionLeaderResp"),
        }
    }
}

/// Raft command is the command that is expected to be proposed by the
/// leader of the target raft group.
#[derive(Debug)]
pub struct RaftCommand<E: KvEngine> {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub callback: Callback<E>,
    pub txn_extra: TxnExtra,
}

impl<E: KvEngine> RaftCommand<E> {
    #[inline]
    pub fn with_txn_extra(
        request: RaftCmdRequest,
        callback: Callback<E>,
        txn_extra: TxnExtra,
    ) -> RaftCommand<E> {
        RaftCommand {
            request,
            callback,
            txn_extra,
            send_time: Instant::now(),
        }
    }

    pub fn new(request: RaftCmdRequest, callback: Callback<E>) -> RaftCommand<E> {
        Self::with_txn_extra(request, callback, TxnExtra::default())
    }
}

/// Message that can be sent to a peer.
pub enum PeerMsg<E: KvEngine> {
    /// Raft message is the message sent between raft nodes in the same
    /// raft group. Messages need to be redirected to raftstore if target
    /// peer doesn't exist.
    RaftMessage(RaftMessage),
    /// Raft command is the command that is expected to be proposed by the
    /// leader of the target raft group. If it's failed to be sent, callback
    /// usually needs to be called before dropping in case of resource leak.
    RaftCommand(RaftCommand<E>),
    /// Tick is periodical task. If target peer doesn't exist there is a potential
    /// that the raft node will not work anymore.
    Tick(PeerTicks),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes {
        res: ApplyTaskRes,
    },
    /// Message that can't be lost but rarely created. If they are lost, real bad
    /// things happen like some peers will be considered dead in the group.
    SignificantMsg(SignificantMsg),
    /// Start the FSM.
    Start,
    /// A message only used to notify a peer.
    Noop,
    /// Message that is not important and can be dropped occasionally.
    CasualMessage(CasualMessage<E>),
    /// Ask region to report a heartbeat to PD.
    HeartbeatPd,
    Destroy(u64),
}

impl<E: KvEngine> fmt::Debug for PeerMsg<E> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            PeerMsg::RaftCommand(_) => write!(fmt, "Raft Command"),
            PeerMsg::Tick(tick) => write! {
                fmt,
                "{:?}",
                tick
            },
            PeerMsg::SignificantMsg(msg) => write!(fmt, "{:?}", msg),
            PeerMsg::ApplyRes { res } => write!(fmt, "ApplyRes {:?}", res),
            PeerMsg::Start => write!(fmt, "Startup"),
            PeerMsg::Noop => write!(fmt, "Noop"),
            PeerMsg::CasualMessage(msg) => write!(fmt, "CasualMessage {:?}", msg),
            PeerMsg::HeartbeatPd => write!(fmt, "HeartbeatPd"),
            PeerMsg::Destroy(peer_id) => write!(fmt, "Destroy {}", peer_id),
        }
    }
}

pub enum StoreMsg {
    RaftMessage(RaftMessage),

    ValidateSSTResult {
        invalid_ssts: Vec<SstMeta>,
    },

    // Clear region size and keys for all regions in the range, so we can force them to re-calculate
    // their size later.
    ClearRegionSizeInRange {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
    StoreUnreachable {
        store_id: u64,
    },

    // Compaction finished event
    CompactedEvent(CompactedEvent),
    Tick(StoreTick),
    Start {
        store: metapb::Store,
    },

    /// Messge only used for test
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&crate::store::Config) + Send>),
}

impl fmt::Debug for StoreMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StoreMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            StoreMsg::StoreUnreachable { store_id } => {
                write!(fmt, "Store {}  is unreachable", store_id)
            }
            StoreMsg::CompactedEvent(ref event) => write!(fmt, "CompactedEvent cf {}", event.cf),
            StoreMsg::ValidateSSTResult { .. } => write!(fmt, "Validate SST Result"),
            StoreMsg::ClearRegionSizeInRange {
                ref start_key,
                ref end_key,
            } => write!(
                fmt,
                "Clear Region size in range {:?} to {:?}",
                start_key, end_key
            ),
            StoreMsg::Tick(tick) => write!(fmt, "StoreTick {:?}", tick),
            StoreMsg::Start { ref store } => write!(fmt, "Start store {:?}", store),
            #[cfg(any(test, feature = "testexport"))]
            StoreMsg::Validate(_) => write!(fmt, "Validate config"),
        }
    }
}

// TODO: remove this enum and utilize the actual message instead.
#[derive(Debug)]
pub enum Msg {
    PeerMsg(PeerMsg<RocksEngine>),
    StoreMsg(StoreMsg),
}
