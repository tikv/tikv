// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]

use kvproto::{
    metapb,
    raft_cmdpb::{RaftCmdRequest, RaftRequestHeader},
    raft_serverpb::RaftMessage,
};
use raftstore::store::{metrics::RaftEventDurationType, FetchedLogs, GenSnapRes};
use resource_control::ResourceMetered;
use tikv_util::time::Instant;

use super::{
    response_channel::{
        CmdResChannel, CmdResSubscriber, DebugInfoChannel, QueryResChannel, QueryResSubscriber,
    },
    ApplyRes,
};
use crate::operation::{RequestHalfSplit, RequestSplit, SimpleWriteBinary, SplitInit};

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
#[repr(u8)]
pub enum PeerTick {
    Raft = 0,
    CompactLog = 1,
    SplitRegionCheck = 2,
    PdHeartbeat = 3,
    CheckMerge = 4,
    CheckPeerStaleState = 5,
    EntryCacheEvict = 6,
    CheckLeaderLease = 7,
    ReactivateMemoryLock = 8,
    ReportBuckets = 9,
    CheckLongUncommitted = 10,
    GcPeer = 11,
}

impl PeerTick {
    pub const VARIANT_COUNT: usize = Self::all_ticks().len();

    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTick::Raft => "raft",
            PeerTick::CompactLog => "compact_log",
            PeerTick::SplitRegionCheck => "split_region_check",
            PeerTick::PdHeartbeat => "pd_heartbeat",
            PeerTick::CheckMerge => "check_merge",
            PeerTick::CheckPeerStaleState => "check_peer_stale_state",
            PeerTick::EntryCacheEvict => "entry_cache_evict",
            PeerTick::CheckLeaderLease => "check_leader_lease",
            PeerTick::ReactivateMemoryLock => "reactivate_memory_lock",
            PeerTick::ReportBuckets => "report_buckets",
            PeerTick::CheckLongUncommitted => "check_long_uncommitted",
            PeerTick::GcPeer => "gc_peer",
        }
    }

    pub const fn all_ticks() -> &'static [PeerTick] {
        const TICKS: &[PeerTick] = &[
            PeerTick::Raft,
            PeerTick::CompactLog,
            PeerTick::SplitRegionCheck,
            PeerTick::PdHeartbeat,
            PeerTick::CheckMerge,
            PeerTick::CheckPeerStaleState,
            PeerTick::EntryCacheEvict,
            PeerTick::CheckLeaderLease,
            PeerTick::ReactivateMemoryLock,
            PeerTick::ReportBuckets,
            PeerTick::CheckLongUncommitted,
            PeerTick::GcPeer,
        ];
        TICKS
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StoreTick {
    // No CompactLock and CompactCheck as they should be implemented by peer itself.
    PdStoreHeartbeat,
    SnapGc,
    ConsistencyCheck,
    CleanupImportSst,
}

impl StoreTick {
    #[inline]
    pub fn tag(self) -> RaftEventDurationType {
        match self {
            StoreTick::PdStoreHeartbeat => RaftEventDurationType::pd_store_heartbeat,
            StoreTick::SnapGc => RaftEventDurationType::snap_gc,
            StoreTick::ConsistencyCheck => RaftEventDurationType::consistency_check,
            StoreTick::CleanupImportSst => RaftEventDurationType::cleanup_import_sst,
        }
    }
}

/// Command that can be handled by raftstore.
#[derive(Debug)]
pub struct RaftRequest<C> {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub ch: C,
}

impl<C> RaftRequest<C> {
    pub fn new(request: RaftCmdRequest, ch: C) -> Self {
        RaftRequest {
            send_time: Instant::now(),
            request,
            ch,
        }
    }
}

#[derive(Debug)]
pub struct SimpleWrite {
    pub send_time: Instant,
    pub header: Box<RaftRequestHeader>,
    pub data: SimpleWriteBinary,
    pub ch: CmdResChannel,
}

#[derive(Debug)]
pub struct UnsafeWrite {
    pub send_time: Instant,
    pub data: SimpleWriteBinary,
}

/// Message that can be sent to a peer.
#[derive(Debug)]
pub enum PeerMsg {
    /// Raft message is the message sent between raft nodes in the same
    /// raft group. Messages need to be redirected to raftstore if target
    /// peer doesn't exist.
    RaftMessage(Box<RaftMessage>),
    /// Query won't change any state. A typical query is KV read. In most cases,
    /// it will be processed using lease or read index.
    RaftQuery(RaftRequest<QueryResChannel>),
    /// Command changes the inernal states. It will be transformed into logs and
    /// applied on all replicas.
    SimpleWrite(SimpleWrite),
    UnsafeWrite(UnsafeWrite),
    /// Command that contains admin requests.
    AdminCommand(RaftRequest<CmdResChannel>),
    /// Tick is periodical task. If target peer doesn't exist there is a
    /// potential that the raft node will not work anymore.
    Tick(PeerTick),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes(ApplyRes),
    LogsFetched(FetchedLogs),
    SnapshotGenerated(GenSnapRes),
    /// Start the FSM.
    Start,
    /// Messages from peer to peer in the same store
    SplitInit(Box<SplitInit>),
    SplitInitFinish(u64),
    /// A message only used to notify a peer.
    Noop,
    /// A message that indicates an asynchronous write has finished.
    Persisted {
        peer_id: u64,
        ready_number: u64,
    },
    QueryDebugInfo(DebugInfoChannel),
    DataFlushed {
        cf: &'static str,
        tablet_index: u64,
        flushed_index: u64,
    },
    PeerUnreachable {
        to_peer_id: u64,
    },
    StoreUnreachable {
        to_store_id: u64,
    },
    /// Reports whether the snapshot sending is successful or not.
    SnapshotSent {
        to_peer_id: u64,
        status: raft::SnapshotStatus,
    },
    RequestSplit {
        request: RequestSplit,
        ch: CmdResChannel,
    },
    RequestHalfSplit {
        request: RequestHalfSplit,
        ch: CmdResChannel,
    },
    UpdateRegionSize {
        size: u64,
    },
    UpdateRegionKeys {
        keys: u64,
    },
    ClearRegionSize,
    ForceCompactLog,
    TabletTrimmed {
        tablet_index: u64,
    },
    /// A message that used to check if a flush is happened.
    #[cfg(feature = "testexport")]
    WaitFlush(super::FlushChannel),
}

impl ResourceMetered for PeerMsg {}

impl PeerMsg {
    pub fn raft_query(req: RaftCmdRequest) -> (Self, QueryResSubscriber) {
        let (ch, sub) = QueryResChannel::pair();
        (PeerMsg::RaftQuery(RaftRequest::new(req, ch)), sub)
    }

    pub fn admin_command(req: RaftCmdRequest) -> (Self, CmdResSubscriber) {
        let (ch, sub) = CmdResChannel::pair();
        (PeerMsg::AdminCommand(RaftRequest::new(req, ch)), sub)
    }

    pub fn simple_write(
        header: Box<RaftRequestHeader>,
        data: SimpleWriteBinary,
    ) -> (Self, CmdResSubscriber) {
        let (ch, sub) = CmdResChannel::pair();
        (
            PeerMsg::SimpleWrite(SimpleWrite {
                send_time: Instant::now(),
                header,
                data,
                ch,
            }),
            sub,
        )
    }

    pub fn unsafe_write(data: SimpleWriteBinary) -> Self {
        PeerMsg::UnsafeWrite(UnsafeWrite {
            send_time: Instant::now(),
            data,
        })
    }

    pub fn request_split(
        epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: String,
    ) -> (Self, CmdResSubscriber) {
        let (ch, sub) = CmdResChannel::pair();
        (
            PeerMsg::RequestSplit {
                request: RequestSplit {
                    epoch,
                    split_keys,
                    source: source.into(),
                },
                ch,
            },
            sub,
        )
    }
}

#[derive(Debug)]
pub enum StoreMsg {
    RaftMessage(Box<RaftMessage>),
    SplitInit(Box<SplitInit>),
    Tick(StoreTick),
    Start,
    StoreUnreachable {
        to_store_id: u64,
    },
    /// A message that used to check if a flush is happened.
    #[cfg(feature = "testexport")]
    WaitFlush {
        region_id: u64,
        ch: super::FlushChannel,
    },
}

impl ResourceMetered for StoreMsg {}
