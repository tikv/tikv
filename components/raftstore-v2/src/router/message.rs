// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::fmt;

use engine_traits::{KvEngine, Snapshot};
use kvproto::{
    cdcpb::Event,
    metapb,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
};
use raftstore::store::{metrics::RaftEventDurationType, InspectedRaftMessage, RegionSnapshot};
use tikv_util::time::Instant;

use super::{
    response_channel::{ReadChannel, WriteChannel},
    ApplyRes,
};

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
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
    CheckLongUncommitted = 10,
}

impl PeerTick {
    pub const VARIANT_COUNT: usize = Self::all_ticks().len();

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
            PeerTick::CheckLongUncommitted => "check_long_uncommitted",
        }
    }

    pub const fn all_ticks() -> &'static [PeerTick] {
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
            PeerTick::CheckLongUncommitted,
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
pub struct RaftCommand {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
}

impl RaftCommand {
    pub fn new(request: RaftCmdRequest) -> Self {
        RaftCommand {
            send_time: Instant::now(),
            request,
        }
    }
}

pub struct ReadCommand {
    pub cmd: RaftCommand,
    pub ch: ReadChannel,
}

impl ReadCommand {
    #[inline]
    pub fn new(request: RaftCmdRequest, ch: ReadChannel) -> Self {
        Self {
            cmd: RaftCommand::new(request),
            ch,
        }
    }
}

pub struct WriteCommand {
    pub cmd: RaftCommand,
    pub ch: WriteChannel,
}

impl WriteCommand {
    #[inline]
    pub fn new(request: RaftCmdRequest, ch: WriteChannel) -> Self {
        Self {
            cmd: RaftCommand::new(request),
            ch,
        }
    }
}

/// Message that can be sent to a peer.
pub enum PeerMsg {
    /// Raft message is the message sent between raft nodes in the same
    /// raft group. Messages need to be redirected to raftstore if target
    /// peer doesn't exist.
    RaftMessage(InspectedRaftMessage),
    /// Read command only involves read operations, they are usually processed
    /// using lease or read index.
    ReadCommand(ReadCommand),
    /// Write command needs to be processed by all peers in a raft group. They
    /// will be transformed into logs and be proposed by the leader peer.
    WriteCommand(WriteCommand),
    /// Tick is periodical task. If target peer doesn't exist there is a
    /// potential that the raft node will not work anymore.
    Tick(PeerTick),
    /// Result of applying committed entries. The message can't be lost.
    ApplyRes(ApplyRes),
    /// Start the FSM.
    Start,
    /// A message only used to notify a peer.
    Noop,
    /// A message that indicates an asynchronous write has finished.
    Persisted { peer_id: u64, ready_number: u64 },
}

impl fmt::Debug for PeerMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            PeerMsg::ReadCommand(_) => write!(fmt, "Read Command"),
            PeerMsg::WriteCommand(_) => write!(fmt, "Write Command"),
            PeerMsg::Tick(tick) => write! {
                fmt,
                "{:?}",
                tick
            },
            PeerMsg::ApplyRes(res) => write!(fmt, "ApplyRes {:?}", res),
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
        }
    }
}

pub enum StoreMsg {
    RaftMessage(InspectedRaftMessage),
    Tick(StoreTick),
    Start { store: metapb::Store },
}

impl fmt::Debug for StoreMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            StoreMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            StoreMsg::Tick(tick) => write!(fmt, "StoreTick {:?}", tick),
            StoreMsg::Start { ref store } => write!(fmt, "Start store {:?}", store),
        }
    }
}
