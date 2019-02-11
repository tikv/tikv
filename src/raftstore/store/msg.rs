// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::boxed::FnBox;
use std::fmt;
use std::time::Instant;

use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb;
use kvproto::metapb::RegionEpoch;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_serverpb::RaftMessage;

use crate::raftstore::store::fsm::apply::TaskRes as ApplyTaskRes;
use crate::raftstore::store::util::KeysInfoFormatter;
use crate::raftstore::store::SnapKey;
use crate::util::escape;
use crate::util::rocksdb::CompactedEvent;
use raft::{SnapshotStatus, StateRole};

use super::RegionSnapshot;

#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot>,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,
}

#[derive(Debug)]
pub enum SeekRegionResult {
    Found(metapb::Region),
    LimitExceeded { next_key: Vec<u8> },
    Ended,
}

pub type ReadCallback = Box<FnBox(ReadResponse) + Send>;
pub type WriteCallback = Box<FnBox(WriteResponse) + Send>;

pub type SeekRegionCallback = Box<FnBox(SeekRegionResult) + Send>;
pub type SeekRegionFilter = Box<Fn(&metapb::Region, StateRole) -> bool + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callbak for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
pub enum Callback {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback),
    /// Write callback.
    Write(WriteCallback),
}

impl Callback {
    pub fn invoke_with_response(self, resp: RaftCmdResponse) {
        match self {
            Callback::None => (),
            Callback::Read(read) => {
                let resp = ReadResponse {
                    response: resp,
                    snapshot: None,
                };
                read(resp);
            }
            Callback::Write(write) => {
                let resp = WriteResponse { response: resp };
                write(resp);
            }
        }
    }

    pub fn invoke_read(self, args: ReadResponse) {
        match self {
            Callback::Read(read) => read(args),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write(_) => write!(fmt, "Callback::Write(..)"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeerTick {
    Raft,
    RaftLogGc,
    SplitRegionCheck,
    PdHeartbeat,
    CheckMerge,
    CheckPeerStaleState,
}

impl PeerTick {
    #[inline]
    pub fn tag(self) -> &'static str {
        match self {
            PeerTick::Raft => "raft",
            PeerTick::RaftLogGc => "raft_log_gc",
            PeerTick::SplitRegionCheck => "split_region_check",
            PeerTick::PdHeartbeat => "pd_heartbeat",
            PeerTick::CheckMerge => "check_merge",
            PeerTick::CheckPeerStaleState => "check_peer_stale_state",
        }
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

/// Some significant messages sent to raftstore. Raftstore will dispatch these messages to Raft
/// groups to update some important internal status.
#[derive(Debug, PartialEq)]
pub enum SignificantMsg {
    /// Reports whether the snapshot sending is successful or not.
    SnapshotStatus {
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    },
    /// Reports `to_peer_id` is unreachable.
    Unreachable { region_id: u64, to_peer_id: u64 },
}

pub enum PeerMsg {
    // For notify.
    RaftMessage(RaftMessage),

    RaftCmd {
        send_time: Instant,
        request: RaftCmdRequest,
        callback: Callback,
    },

    SplitRegion {
        region_id: u64,
        region_epoch: RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_keys: Vec<Vec<u8>>,
        callback: Callback,
    },

    // For consistency check
    ComputeHashResult {
        region_id: u64,
        index: u64,
        hash: Vec<u8>,
    },

    // For region size
    RegionApproximateSize {
        region_id: u64,
        size: u64,
    },

    // For region keys
    RegionApproximateKeys {
        region_id: u64,
        keys: u64,
    },
    CompactionDeclinedBytes {
        region_id: u64,
        bytes: u64,
    },
    HalfSplitRegion {
        region_id: u64,
        region_epoch: RegionEpoch,
        policy: CheckPolicy,
    },
    MergeResult {
        region_id: u64,
        target: metapb::Peer,
        stale: bool,
    },
    GcSnap {
        region_id: u64,
        snaps: Vec<(SnapKey, bool)>,
    },
    ClearRegionSize(u64),
    Tick(u64, PeerTick),
    SignificantMsg(SignificantMsg),
    Start(u64),
    ApplyRes {
        region_id: u64,
        res: ApplyTaskRes,
    },
    Noop(u64),
}

impl fmt::Debug for PeerMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            PeerMsg::RaftCmd { .. } => write!(fmt, "Raft Command"),
            PeerMsg::ComputeHashResult {
                region_id,
                index,
                ref hash,
            } => write!(
                fmt,
                "ComputeHashResult [region_id: {}, index: {}, hash: {}]",
                region_id,
                index,
                escape(hash)
            ),
            PeerMsg::SplitRegion {
                region_id,
                ref split_keys,
                ..
            } => write!(
                fmt,
                "Split region {} with {}",
                region_id,
                KeysInfoFormatter(&split_keys)
            ),
            PeerMsg::RegionApproximateSize { region_id, size } => write!(
                fmt,
                "Region's approximate size [region_id: {}, size: {:?}]",
                region_id, size
            ),
            PeerMsg::RegionApproximateKeys { region_id, keys } => write!(
                fmt,
                "Region's approximate keys [region_id: {}, keys: {:?}]",
                region_id, keys
            ),
            PeerMsg::CompactionDeclinedBytes { region_id, bytes } => write!(
                fmt,
                "[region {}] compaction declined bytes {}",
                region_id, bytes
            ),
            PeerMsg::HalfSplitRegion { ref region_id, .. } => {
                write!(fmt, "Half Split region {}", region_id)
            }
            PeerMsg::MergeResult {
                region_id,
                target,
                stale,
            } => write! {
                fmt,
                "[region {}] target: {:?}, successful: {}",
                region_id, target, stale
            },
            PeerMsg::GcSnap {
                region_id,
                ref snaps,
            } => write! {
                fmt,
                "[region {}] gc snaps {:?}",
                region_id, snaps
            },
            PeerMsg::ClearRegionSize(region_id) => write! {
                fmt,
                "[region {}] clear region size",
                region_id
            },
            PeerMsg::Tick(region_id, tick) => write! {
                fmt,
                "[region {}] {:?}",
                region_id,
                tick
            },
            PeerMsg::SignificantMsg(msg) => write!(fmt, "{:?}", msg),
            PeerMsg::ApplyRes { region_id, res } => {
                write!(fmt, "[region {}] ApplyRes {:?}", region_id, res)
            }
            PeerMsg::Start(region_id) => write!(fmt, "[region {}] Startup", region_id),
            PeerMsg::Noop(region_id) => write!(fmt, "[region {}] Noop", region_id),
        }
    }
}

impl Msg {
    pub fn new_raft_cmd(request: RaftCmdRequest, callback: Callback) -> Msg {
        Msg::PeerMsg(PeerMsg::RaftCmd {
            send_time: Instant::now(),
            request,
            callback,
        })
    }

    pub fn new_half_split_region(
        region_id: u64,
        region_epoch: RegionEpoch,
        policy: CheckPolicy,
    ) -> Msg {
        Msg::PeerMsg(PeerMsg::HalfSplitRegion {
            region_id,
            region_epoch,
            policy,
        })
    }
}

pub enum StoreMsg {
    RaftMessage(RaftMessage),
    // For snapshot stats.
    SnapshotStats,

    ValidateSSTResult {
        invalid_ssts: Vec<SSTMeta>,
    },

    // Clear region size and keys for all regions in the range, so we can force them to re-calculate
    // their size later.
    ClearRegionSizeInRange {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },

    // Compaction finished event
    CompactedEvent(CompactedEvent),
    Tick(StoreTick),
    Start {
        store: metapb::Store,
    },
}

impl fmt::Debug for StoreMsg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            StoreMsg::RaftMessage(_) => write!(fmt, "Raft Message"),
            StoreMsg::SnapshotStats => write!(fmt, "Snapshot stats"),
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
        }
    }
}

// TODO: remove this enum and utilize the actual message instead.
#[derive(Debug)]
pub enum Msg {
    PeerMsg(PeerMsg),
    StoreMsg(StoreMsg),
}
