// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{fmt, marker::PhantomData};

use engine_traits::{KvEngine, Snapshot};
use kvproto::{
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
};
use raftstore::store::{
    fsm::ApplyTaskRes, metrics::RaftEventDurationType, InspectedRaftMessage, RegionSnapshot,
};
use tikv_util::{memory::HeapSize, time::Instant};

pub struct WriteResponseChannel;

impl WriteResponseChannel {
    /// Called after a request is proposed to the raft group successfully. It's
    /// used to notify the caller to move on early because it's very likely the
    /// request will be applied to the raftstore.
    pub fn notify_proposed(&self) {}

    /// Called after a request is committed and before it's being applied, and
    /// it's guaranteed that the request will be successfully applied soon.
    pub fn notify_committed(&self) {}

    pub fn notify_applied(&self, _res: Result<(), RaftCmdResponse>) {}
}

pub struct ReadResponseChannel<S> {
    _snap: PhantomData<S>,
}

pub struct ReadResponse<S: Snapshot> {
    pub snapshot: RegionSnapshot<S>,
    // What is this?
    pub txn_extra_op: TxnExtraOp,
}

impl<S: Snapshot> ReadResponseChannel<S> {
    pub fn notify_read(&self, _res: Result<ReadResponse<S>, RaftCmdResponse>) {}
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
            snapshot: self.snapshot.clone(),
            txn_extra_op: self.txn_extra_op,
        }
    }
}

/// Variants of channels for `Msg`.
///  - `Read`: a channel for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a channel for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
/// Prefer channel rather than callback because:
/// 1. channel can be reused, hence reduce allocations.
/// 2. channel may not need dynamic dispatch.
/// 3. caller can use async fashion.
/// 4. there will be no callback leak.
pub enum ResponseChannel<S> {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadResponseChannel<S>),
    /// Write callback.
    Write(WriteResponseChannel),
}

impl<S: Snapshot> HeapSize for ResponseChannel<S> {}

impl<S> ResponseChannel<S>
where
    S: Snapshot,
{
    pub fn notify_applied(self, resp: RaftCmdResponse) {
        match self {
            ResponseChannel::None => (),
            ResponseChannel::Read(read) => {
                read.notify_read(Err(resp));
            }
            ResponseChannel::Write(write) => {
                write.notify_applied(Err(resp));
            }
        }
    }

    pub fn notify_proposed(&mut self) {
        if let ResponseChannel::Write(write) = self {
            write.notify_proposed();
        }
    }

    pub fn notify_committed(&mut self) {
        if let ResponseChannel::Write(write) = self {
            write.notify_committed();
        }
    }

    pub fn invoke_read(self, args: ReadResponse<S>) {
        match self {
            ResponseChannel::Read(read) => read.notify_read(Ok(args)),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, ResponseChannel::None)
    }
}

impl<S> fmt::Debug for ResponseChannel<S>
where
    S: Snapshot,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResponseChannel::None => write!(fmt, "Callback::None"),
            ResponseChannel::Read(_) => write!(fmt, "Callback::Read(..)"),
            ResponseChannel::Write { .. } => write!(fmt, "Callback::Write(..)"),
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

/// Raft command is the command that is expected to be proposed by the
/// leader of the target raft group.
#[derive(Debug)]
pub struct RaftCommand<S: Snapshot> {
    pub send_time: Instant,
    pub request: RaftCmdRequest,
    pub ch: ResponseChannel<S>,
}

impl<S: Snapshot> RaftCommand<S> {
    #[inline]
    pub fn new(request: RaftCmdRequest, ch: ResponseChannel<S>) -> RaftCommand<S> {
        RaftCommand {
            request,
            ch,
            send_time: Instant::now(),
        }
    }
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
    /// Start the FSM.
    Start,
    /// A message only used to notify a peer.
    Noop,
    Persisted {
        peer_id: u64,
        ready_number: u64,
    },
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
