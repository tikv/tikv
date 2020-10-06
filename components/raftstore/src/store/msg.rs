// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::time::Instant;

use engine_traits::{CompactedEvent, KvEngine, Snapshot};
use kvproto::import_sstpb::SstMeta;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb;
use kvproto::metapb::RegionEpoch;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::replication_modepb::ReplicationStatus;
use raft::SnapshotStatus;

use crate::store::fsm::apply::TaskRes as ApplyTaskRes;
use crate::store::fsm::apply::{CatchUpLogs, ChangeCmd};
use crate::store::metrics::RaftEventDurationType;
use crate::store::util::KeysInfoFormatter;
use crate::store::SnapKey;
use tikv_util::escape;

use super::{AbstractPeer, RegionSnapshot};

#[derive(Debug)]
pub struct ReadResponse<S: Snapshot> {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot<S>>,
    pub txn_extra_op: TxnExtraOp,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,
}

// This is only necessary because of seeming limitations in derive(Clone) w/r/t
// generics. If it can be deleted in the future in favor of derive, it should
// be.
impl<S> Clone for ReadResponse<S>
where
    S: Snapshot,
{
    fn clone(&self) -> ReadResponse<S> {
        ReadResponse {
            response: self.response.clone(),
            snapshot: self.snapshot.clone(),
            txn_extra_op: self.txn_extra_op,
        }
    }
}

pub type ReadCallback<S> = Box<dyn FnOnce(ReadResponse<S>) + Send>;
pub type WriteCallback = Box<dyn FnOnce(WriteResponse) + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callback for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
pub enum Callback<S: Snapshot> {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback<S>),
    /// Write callback.
    Write(WriteCallback),
}

impl<S> Callback<S>
where
    S: Snapshot,
{
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
            Callback::Write(write) => {
                let resp = WriteResponse { response: resp };
                write(resp);
            }
        }
    }

    pub fn invoke_read(self, args: ReadResponse<S>) {
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

impl<S> fmt::Debug for Callback<S>
where
    S: Snapshot,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write(_) => write!(fmt, "Callback::Write(..)"),
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
    RaftEnginePurge,
}

impl StoreTick {
    #[inline]
    pub fn tag(self) -> RaftEventDurationType {
        match self {
            StoreTick::CompactCheck => RaftEventDurationType::compact_check,
            StoreTick::PdStoreHeartbeat => RaftEventDurationType::pd_store_heartbeat,
            StoreTick::SnapGc => RaftEventDurationType::snap_gc,
            StoreTick::CompactLockCf => RaftEventDurationType::compact_lock_cf,
            StoreTick::ConsistencyCheck => RaftEventDurationType::consistency_check,
            StoreTick::CleanupImportSST => RaftEventDurationType::cleanup_import_sst,
            StoreTick::RaftEnginePurge => RaftEventDurationType::raft_engine_purge,
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
pub enum SignificantMsg<SK>
where
    SK: Snapshot,
{
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
    StoreResolved {
        store_id: u64,
        group_id: u64,
    },
    /// Capture the changes of the region.
    CaptureChange {
        cmd: ChangeCmd,
        region_epoch: RegionEpoch,
        callback: Callback<SK>,
    },
    LeaderCallback(Callback<SK>),
}

/// Message that will be sent to a peer.
///
/// These messages are not significant and can be dropped occasionally.
pub enum CasualMessage<EK: KvEngine> {
    /// Split the target region into several partitions.
    SplitRegion {
        region_epoch: RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback<EK::Snapshot>,
    },

    /// Hash result of ComputeHash command.
    ComputeHashResult {
        index: u64,
        context: Vec<u8>,
        hash: Vec<u8>,
    },

    /// Approximate size of target region.
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

    /// Generally Raft leader keeps as more as possible logs for followers,
    /// however `ForceCompactRaftLogs` only cares the leader itself.
    ForceCompactRaftLogs,

    /// A message to access peer's internal state.
    AccessPeer(Box<dyn FnOnce(&mut dyn AbstractPeer) + Send + 'static>),
}

impl<EK: KvEngine> fmt::Debug for CasualMessage<EK> {
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
            CasualMessage::SplitRegion { ref split_keys, .. } => write!(
                fmt,
                "Split region with {}",
                KeysInfoFormatter(split_keys.iter())
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
            CasualMessage::HalfSplitRegion { .. } => write!(fmt, "Half Split"),
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
            CasualMessage::ForceCompactRaftLogs => write!(fmt, "ForceCompactRaftLogs"),
            CasualMessage::AccessPeer(_) => write!(fmt, "AccessPeer"),
        }
    }
}

/// Raft command is the command that is expected to be proposed by the
/// leader of the target raft group.
#[derive(Debug)]
pub struct RaftCommand<S: Snapshot> {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub callback: Callback<S>,
}

impl<S: Snapshot> RaftCommand<S> {
    #[inline]
    pub fn new(request: RaftCmdRequest, callback: Callback<S>) -> RaftCommand<S> {
        RaftCommand {
            request,
            callback,
            send_time: Instant::now(),
        }
    }
}

/// Message that can be sent to a peer.
pub enum PeerMsg<EK: KvEngine> {
    /// Raft message is the message sent between raft nodes in the same
    /// raft group. Messages need to be redirected to raftstore if target
    /// peer doesn't exist.
    RaftMessage(RaftMessage),
    /// Raft command is the command that is expected to be proposed by the
    /// leader of the target raft group. If it's failed to be sent, callback
    /// usually needs to be called before dropping in case of resource leak.
    RaftCommand(RaftCommand<EK::Snapshot>),
    /// Tick is periodical task. If target peer doesn't exist there is a potential
    /// that the raft node will not work anymore.
    Tick(PeerTicks),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes { res: ApplyTaskRes<EK::Snapshot> },
    /// Message that can't be lost but rarely created. If they are lost, real bad
    /// things happen like some peers will be considered dead in the group.
    SignificantMsg(SignificantMsg<EK::Snapshot>),
    /// Start the FSM.
    Start,
    /// A message only used to notify a peer.
    Noop,
    /// Message that is not important and can be dropped occasionally.
    CasualMessage(CasualMessage<EK>),
    /// Ask region to report a heartbeat to PD.
    HeartbeatPd,
    /// Asks region to change replication mode.
    UpdateReplicationMode,
}

impl<EK: KvEngine> fmt::Debug for PeerMsg<EK> {
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
            PeerMsg::UpdateReplicationMode => write!(fmt, "UpdateReplicationMode"),
        }
    }
}

pub enum StoreMsg<EK>
where
    EK: KvEngine,
{
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
    CompactedEvent(EK::CompactedEvent),
    Tick(StoreTick),
    Start {
        store: metapb::Store,
    },

    /// Message only used for test.
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&crate::store::Config) + Send>),
    /// Asks the store to update replication mode.
    UpdateReplicationMode(ReplicationStatus),
}

impl<EK> fmt::Debug for StoreMsg<EK>
where
    EK: KvEngine,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StoreMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            StoreMsg::StoreUnreachable { store_id } => {
                write!(fmt, "Store {}  is unreachable", store_id)
            }
            StoreMsg::CompactedEvent(ref event) => write!(fmt, "CompactedEvent cf {}", event.cf()),
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
            StoreMsg::UpdateReplicationMode(_) => write!(fmt, "UpdateReplicationMode"),
        }
    }
}
