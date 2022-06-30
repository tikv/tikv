// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
#[cfg(any(test, feature = "testexport"))]
use std::sync::Arc;
use std::{borrow::Cow, fmt};

use collections::HashSet;
use engine_traits::{CompactedEvent, KvEngine, Snapshot};
use kvproto::{
    import_sstpb::SstMeta,
    kvrpcpb::{DiskFullOpt, ExtraOp as TxnExtraOp},
    metapb,
    metapb::RegionEpoch,
    pdpb::{self, CheckPolicy},
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
    replication_modepb::ReplicationStatus,
};
#[cfg(any(test, feature = "testexport"))]
use pd_client::BucketMeta;
use raft::{GetEntriesContext, SnapshotStatus};
use smallvec::{smallvec, SmallVec};
use tikv_util::{deadline::Deadline, escape, memory::HeapSize, time::Instant};
use tracker::{get_tls_tracker_token, GLOBAL_TRACKERS, INVALID_TRACKER_TOKEN};

use super::{local_metrics::TimeTracker, AbstractPeer, RegionSnapshot};
use crate::store::{
    fsm::apply::{CatchUpLogs, ChangeObserver, TaskRes as ApplyTaskRes},
    metrics::RaftEventDurationType,
    peer::{
        UnsafeRecoveryExecutePlanSyncer, UnsafeRecoveryFillOutReportSyncer,
        UnsafeRecoveryForceLeaderSyncer, UnsafeRecoveryWaitApplySyncer,
    },
    util::{KeysInfoFormatter, LatencyInspector},
    worker::{Bucket, BucketRange},
    RaftlogFetchResult, SnapKey,
};

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

// Peer's internal stat, for test purpose only
#[cfg(any(test, feature = "testexport"))]
#[derive(Debug)]
pub struct PeerInternalStat {
    pub buckets: Arc<BucketMeta>,
    pub bucket_ranges: Option<Vec<BucketRange>>,
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
pub type ExtCallback = Box<dyn FnOnce() + Send>;
#[cfg(any(test, feature = "testexport"))]
pub type TestCallback = Box<dyn FnOnce(PeerInternalStat) + Send>;

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
    Write {
        cb: WriteCallback,
        /// `proposed_cb` is called after a request is proposed to the raft group successfully.
        /// It's used to notify the caller to move on early because it's very likely the request
        /// will be applied to the raftstore.
        proposed_cb: Option<ExtCallback>,
        /// `committed_cb` is called after a request is committed and before it's being applied, and
        /// it's guaranteed that the request will be successfully applied soon.
        committed_cb: Option<ExtCallback>,
        trackers: SmallVec<[TimeTracker; 4]>,
    },
    #[cfg(any(test, feature = "testexport"))]
    /// Test purpose callback
    Test { cb: TestCallback },
}

impl<S: Snapshot> HeapSize for Callback<S> {}

impl<S> Callback<S>
where
    S: Snapshot,
{
    pub fn write(cb: WriteCallback) -> Self {
        Self::write_ext(cb, None, None)
    }

    pub fn write_ext(
        cb: WriteCallback,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> Self {
        let tracker_token = get_tls_tracker_token();
        let now = std::time::Instant::now();
        let tracker = if tracker_token == INVALID_TRACKER_TOKEN {
            TimeTracker::Instant(now)
        } else {
            GLOBAL_TRACKERS.with_tracker(tracker_token, |tracker| {
                tracker.metrics.write_instant = Some(now);
            });
            TimeTracker::Tracker(tracker_token)
        };

        Callback::Write {
            cb,
            proposed_cb,
            committed_cb,
            trackers: smallvec![tracker],
        }
    }

    pub fn get_trackers(&self) -> Option<&SmallVec<[TimeTracker; 4]>> {
        match self {
            Callback::Write { trackers, .. } => Some(trackers),
            _ => None,
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
            #[cfg(any(test, feature = "testexport"))]
            Callback::Test { .. } => (),
        }
    }

    pub fn has_proposed_cb(&mut self) -> bool {
        if let Callback::Write { proposed_cb, .. } = self {
            proposed_cb.is_some()
        } else {
            false
        }
    }

    pub fn invoke_proposed(&mut self) {
        if let Callback::Write { proposed_cb, .. } = self {
            if let Some(cb) = proposed_cb.take() {
                cb()
            }
        }
    }

    pub fn invoke_committed(&mut self) {
        if let Callback::Write { committed_cb, .. } = self {
            if let Some(cb) = committed_cb.take() {
                cb()
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
        matches!(self, Callback::None)
    }
}

impl<S> fmt::Debug for Callback<S>
where
    S: Snapshot,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write { .. } => write!(fmt, "Callback::Write(..)"),
            #[cfg(any(test, feature = "testexport"))]
            Callback::Test { .. } => write!(fmt, "Callback::Test(..)"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PeerTick {
    Raft = 0,
    RaftLogGc = 1,
    SplitRegionCheck = 2,
    PdHeartbeat = 3,
    CheckMerge = 4,
    CheckPeerStaleState = 5,
    EntryCacheEvict = 6,
    CheckLeaderLease = 7,
    ReactivateMemoryLock = 8,
    ReportBuckets = 9,
}

impl PeerTick {
    pub const VARIANT_COUNT: usize = Self::get_all_ticks().len();

    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTick::Raft => "raft",
            PeerTick::RaftLogGc => "raft_log_gc",
            PeerTick::SplitRegionCheck => "split_region_check",
            PeerTick::PdHeartbeat => "pd_heartbeat",
            PeerTick::CheckMerge => "check_merge",
            PeerTick::CheckPeerStaleState => "check_peer_stale_state",
            PeerTick::EntryCacheEvict => "entry_cache_evict",
            PeerTick::CheckLeaderLease => "check_leader_lease",
            PeerTick::ReactivateMemoryLock => "reactivate_memory_lock",
            PeerTick::ReportBuckets => "report_buckets",
        }
    }

    pub const fn get_all_ticks() -> &'static [PeerTick] {
        const TICKS: &[PeerTick] = &[
            PeerTick::Raft,
            PeerTick::RaftLogGc,
            PeerTick::SplitRegionCheck,
            PeerTick::PdHeartbeat,
            PeerTick::CheckMerge,
            PeerTick::CheckPeerStaleState,
            PeerTick::EntryCacheEvict,
            PeerTick::CheckLeaderLease,
            PeerTick::ReactivateMemoryLock,
            PeerTick::ReportBuckets,
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
    CleanupImportSst,
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
            StoreTick::CleanupImportSst => RaftEventDurationType::cleanup_import_sst,
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
        cmd: ChangeObserver,
        region_epoch: RegionEpoch,
        callback: Callback<SK>,
    },
    LeaderCallback(Callback<SK>),
    RaftLogGcFlushed,
    // Reports the result of asynchronous Raft logs fetching.
    RaftlogFetched {
        context: GetEntriesContext,
        res: Box<RaftlogFetchResult>,
    },
    EnterForceLeaderState {
        syncer: UnsafeRecoveryForceLeaderSyncer,
        failed_stores: HashSet<u64>,
    },
    ExitForceLeaderState,
    UnsafeRecoveryDemoteFailedVoters {
        syncer: UnsafeRecoveryExecutePlanSyncer,
        failed_voters: Vec<metapb::Peer>,
    },
    UnsafeRecoveryDestroy(UnsafeRecoveryExecutePlanSyncer),
    UnsafeRecoveryWaitApply(UnsafeRecoveryWaitApplySyncer),
    UnsafeRecoveryFillOutReport(UnsafeRecoveryFillOutReportSyncer),
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
    /// Half split the target region.
    HalfSplitRegion {
        region_epoch: RegionEpoch,
        policy: CheckPolicy,
        source: &'static str,
        cb: Callback<EK::Snapshot>,
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

    /// Region info from PD
    QueryRegionLeaderResp {
        region: metapb::Region,
        leader: metapb::Peer,
    },

    /// For drop raft messages at an upper layer.
    RejectRaftAppend {
        peer_id: u64,
    },
    RefreshRegionBuckets {
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
        cb: Callback<EK::Snapshot>,
    },

    // Try renew leader lease
    RenewLease,

    // Snapshot is applied
    SnapshotApplied,

    // Trigger raft to campaign which is used after exiting force leader
    Campaign,
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
                log_wrappers::Value::key(context),
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
            CasualMessage::ForceCompactRaftLogs => write!(fmt, "ForceCompactRaftLogs"),
            CasualMessage::AccessPeer(_) => write!(fmt, "AccessPeer"),
            CasualMessage::QueryRegionLeaderResp { .. } => write!(fmt, "QueryRegionLeaderResp"),
            CasualMessage::RejectRaftAppend { peer_id } => {
                write!(fmt, "RejectRaftAppend(peer_id={})", peer_id)
            }
            CasualMessage::RefreshRegionBuckets { .. } => write!(fmt, "RefreshRegionBuckets"),
            CasualMessage::RenewLease => write!(fmt, "RenewLease"),
            CasualMessage::SnapshotApplied => write!(fmt, "SnapshotApplied"),
            CasualMessage::Campaign => write!(fmt, "Campaign"),
        }
    }
}

/// control options for raftcmd.
#[derive(Debug, Default, Clone)]
pub struct RaftCmdExtraOpts {
    pub deadline: Option<Deadline>,
    pub disk_full_opt: DiskFullOpt,
}

/// Raft command is the command that is expected to be proposed by the
/// leader of the target raft group.
#[derive(Debug)]
pub struct RaftCommand<S: Snapshot> {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub callback: Callback<S>,
    pub extra_opts: RaftCmdExtraOpts,
}

impl<S: Snapshot> RaftCommand<S> {
    #[inline]
    pub fn new(request: RaftCmdRequest, callback: Callback<S>) -> RaftCommand<S> {
        RaftCommand {
            request,
            callback,
            send_time: Instant::now(),
            extra_opts: RaftCmdExtraOpts::default(),
        }
    }

    pub fn new_ext(
        request: RaftCmdRequest,
        callback: Callback<S>,
        extra_opts: RaftCmdExtraOpts,
    ) -> RaftCommand<S> {
        RaftCommand {
            request,
            callback,
            send_time: Instant::now(),
            extra_opts: RaftCmdExtraOpts {
                deadline: extra_opts.deadline,
                disk_full_opt: extra_opts.disk_full_opt,
            },
        }
    }
}

pub struct InspectedRaftMessage {
    pub heap_size: usize,
    pub msg: RaftMessage,
}

/// Message that can be sent to a peer.
pub enum PeerMsg<EK: KvEngine> {
    /// Raft message is the message sent between raft nodes in the same
    /// raft group. Messages need to be redirected to raftstore if target
    /// peer doesn't exist.
    RaftMessage(InspectedRaftMessage),
    /// Raft command is the command that is expected to be proposed by the
    /// leader of the target raft group. If it's failed to be sent, callback
    /// usually needs to be called before dropping in case of resource leak.
    RaftCommand(RaftCommand<EK::Snapshot>),
    /// Tick is periodical task. If target peer doesn't exist there is a potential
    /// that the raft node will not work anymore.
    Tick(PeerTick),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes {
        res: ApplyTaskRes<EK::Snapshot>,
    },
    /// Message that can't be lost but rarely created. If they are lost, real bad
    /// things happen like some peers will be considered dead in the group.
    SignificantMsg(SignificantMsg<EK::Snapshot>),
    /// Start the FSM.
    Start,
    /// A message only used to notify a peer.
    Noop,
    Persisted {
        peer_id: u64,
        ready_number: u64,
    },
    /// Message that is not important and can be dropped occasionally.
    CasualMessage(CasualMessage<EK>),
    /// Ask region to report a heartbeat to PD.
    HeartbeatPd,
    /// Asks region to change replication mode.
    UpdateReplicationMode,
    Destroy(u64),
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
            PeerMsg::Persisted {
                peer_id,
                ready_number,
            } => write!(
                fmt,
                "Persisted peer_id {}, ready_number {}",
                peer_id, ready_number
            ),
            PeerMsg::CasualMessage(msg) => write!(fmt, "CasualMessage {:?}", msg),
            PeerMsg::HeartbeatPd => write!(fmt, "HeartbeatPd"),
            PeerMsg::UpdateReplicationMode => write!(fmt, "UpdateReplicationMode"),
            PeerMsg::Destroy(peer_id) => write!(fmt, "Destroy {}", peer_id),
        }
    }
}

pub enum StoreMsg<EK>
where
    EK: KvEngine,
{
    RaftMessage(InspectedRaftMessage),

    ValidateSstResult {
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

    /// Asks the store to update replication mode.
    UpdateReplicationMode(ReplicationStatus),

    /// Inspect the latency of raftstore.
    LatencyInspect {
        send_time: Instant,
        inspector: LatencyInspector,
    },

    /// Message only used for test.
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&crate::store::Config) + Send>),

    UnsafeRecoveryReport(pdpb::StoreReport),
    UnsafeRecoveryCreatePeer {
        syncer: UnsafeRecoveryExecutePlanSyncer,
        create: metapb::Region,
    },

    GcSnapshotFinish,
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
            StoreMsg::ValidateSstResult { .. } => write!(fmt, "Validate SST Result"),
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
            StoreMsg::LatencyInspect { .. } => write!(fmt, "LatencyInspect"),
            StoreMsg::UnsafeRecoveryReport(..) => write!(fmt, "UnsafeRecoveryReport"),
            StoreMsg::UnsafeRecoveryCreatePeer { .. } => {
                write!(fmt, "UnsafeRecoveryCreatePeer")
            }
            StoreMsg::GcSnapshotFinish => write!(fmt, "GcSnapshotFinish"),
        }
    }
}
