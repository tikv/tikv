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
use kvproto::metapb::RegionEpoch;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::raft_serverpb::RaftMessage;

use raft::SnapshotStatus;
use raftstore::store::util::RegionApproximateStat;
use util::escape;
use util::rocksdb::CompactedEvent;

use super::RegionSnapshot;

#[derive(Debug)]
pub struct ReadResponse {
    pub response: RaftCmdResponse,
    pub snapshot: Option<RegionSnapshot>,
}

#[derive(Debug)]
pub struct WriteResponse {
    pub response: RaftCmdResponse,
}

pub type ReadCallback = Box<FnBox(ReadResponse) + Send>;
pub type WriteCallback = Box<FnBox(WriteResponse) + Send>;
pub type BatchReadCallback = Box<FnBox(Vec<Option<ReadResponse>>) + Send>;

/// Variants of callbacks for `Msg`.
///  - `Read`: a callbak for read only requests including `StatusRequest`,
///         `GetRequest` and `SnapRequest`
///  - `Write`: a callback for write only requests including `AdminRequest`
///          `PutRequest`, `DeleteRequest` and `DeleteRangeRequest`.
///  - `BatchRead`: callbacks for a batch read request.
pub enum Callback {
    /// No callback.
    None,
    /// Read callback.
    Read(ReadCallback),
    /// Write callback.
    Write(WriteCallback),
    /// Batch read callbacks.
    BatchRead(BatchReadCallback),
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
            Callback::BatchRead(_) => unreachable!(),
        }
    }

    pub fn invoke_read(self, args: ReadResponse) {
        match self {
            Callback::Read(read) => read(args),
            other => panic!("expect Callback::Read(..), got {:?}", other),
        }
    }

    pub fn invoke_batch_read(self, args: Vec<Option<ReadResponse>>) {
        match self {
            Callback::BatchRead(batch_read) => batch_read(args),
            other => panic!("expect Callback::BatchRead(..), got {:?}", other),
        }
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Callback::None => write!(fmt, "Callback::None"),
            Callback::Read(_) => write!(fmt, "Callback::Read(..)"),
            Callback::Write(_) => write!(fmt, "Callback::Write(..)"),
            Callback::BatchRead(_) => write!(fmt, "Callback::BatchRead(..)"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Tick {
    Raft,
    RaftLogGc,
    SplitRegionCheck,
    CompactCheck,
    PdHeartbeat,
    PdStoreHeartbeat,
    SnapGc,
    CompactLockCf,
    ConsistencyCheck,
    CheckMerge,
    CheckPeerStaleState,
    CleanupImportSST,
}

#[derive(Debug, PartialEq)]
pub enum SignificantMsg {
    SnapshotStatus {
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    },
    Unreachable {
        region_id: u64,
        to_peer_id: u64,
    },
}

pub enum Msg {
    Quit,

    // For notify.
    RaftMessage(RaftMessage),

    RaftCmd {
        send_time: Instant,
        request: RaftCmdRequest,
        callback: Callback,
    },

    BatchRaftSnapCmds {
        send_time: Instant,
        batch: Vec<RaftCmdRequest>,
        on_finished: Callback,
    },

    SplitRegion {
        region_id: u64,
        region_epoch: RegionEpoch,
        // It's an encoded key.
        // TODO: support meta key.
        split_key: Vec<u8>,
        callback: Callback,
    },

    // For snapshot stats.
    SnapshotStats,

    // For consistency check
    ComputeHashResult {
        region_id: u64,
        index: u64,
        hash: Vec<u8>,
    },

    // For region stat
    RegionApproximateStat {
        region_id: u64,
        stat: RegionApproximateStat,
    },

    // Compaction finished event
    CompactedEvent(CompactedEvent),
    HalfSplitRegion {
        region_id: u64,
        region_epoch: RegionEpoch,
    },
    MergeFail {
        region_id: u64,
    },

    ValidateSSTResult {
        invalid_ssts: Vec<SSTMeta>,
    },
}

impl fmt::Debug for Msg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(fmt, "Quit"),
            Msg::RaftMessage(_) => write!(fmt, "Raft Message"),
            Msg::RaftCmd { .. } => write!(fmt, "Raft Command"),
            Msg::BatchRaftSnapCmds { .. } => write!(fmt, "Batch Raft Commands"),
            Msg::SnapshotStats => write!(fmt, "Snapshot stats"),
            Msg::ComputeHashResult {
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
            Msg::SplitRegion {
                ref region_id,
                ref split_key,
                ..
            } => write!(fmt, "Split region {} at key {:?}", region_id, split_key),
            Msg::RegionApproximateStat {
                region_id,
                ref stat,
            } => write!(
                fmt,
                "Region's approximate stat [region_id: {}, stat: {:?}]",
                region_id, stat
            ),
            Msg::CompactedEvent(ref event) => write!(fmt, "CompactedEvent cf {}", event.cf),
            Msg::HalfSplitRegion { ref region_id, .. } => {
                write!(fmt, "Half Split region {}", region_id)
            }
            Msg::MergeFail { region_id } => write!(fmt, "MergeFail region_id {}", region_id),
            Msg::ValidateSSTResult { .. } => write!(fmt, "Validate SST Result"),
        }
    }
}

impl Msg {
    pub fn new_raft_cmd(request: RaftCmdRequest, callback: Callback) -> Msg {
        Msg::RaftCmd {
            send_time: Instant::now(),
            request,
            callback,
        }
    }

    pub fn new_batch_raft_snapshot_cmd(
        batch: Vec<RaftCmdRequest>,
        on_finished: BatchReadCallback,
    ) -> Msg {
        Msg::BatchRaftSnapCmds {
            send_time: Instant::now(),
            batch,
            on_finished: Callback::BatchRead(on_finished),
        }
    }

    pub fn new_half_split_region(region_id: u64, region_epoch: RegionEpoch) -> Msg {
        Msg::HalfSplitRegion {
            region_id,
            region_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use mio::{EventLoop, Handler};

    use super::*;
    use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
    use raftstore::Error;
    use util::transport::SendCh;

    fn call_command(
        sendch: &SendCh<Msg>,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse, Error> {
        wait_op!(
            |cb: Box<FnBox(RaftCmdResponse) + 'static + Send>| {
                let callback = Callback::Write(Box::new(move |write_resp: WriteResponse| {
                    cb(write_resp.response);
                }));
                sendch.try_send(Msg::new_raft_cmd(request, callback))
            },
            timeout
        ).ok_or_else(|| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    struct TestHandler;

    impl Handler for TestHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
            match msg {
                Msg::Quit => event_loop.shutdown(),
                Msg::RaftCmd {
                    callback, request, ..
                } => {
                    // a trick for test timeout.
                    if request.get_header().get_region_id() == u64::max_value() {
                        thread::sleep(Duration::from_millis(100));
                    }
                    callback.invoke_with_response(RaftCmdResponse::new());
                }
                // we only test above message types, others panic.
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sender() {
        let mut event_loop = EventLoop::new().unwrap();
        let sendch = &SendCh::new(event_loop.channel(), "test-sender");

        let t = thread::spawn(move || {
            event_loop.run(&mut TestHandler).unwrap();
        });

        let mut request = RaftCmdRequest::new();
        request.mut_header().set_region_id(u64::max_value());
        assert!(call_command(sendch, request.clone(), Duration::from_millis(500)).is_ok());
        match call_command(sendch, request, Duration::from_millis(10)) {
            Err(Error::Timeout(_)) => {}
            _ => panic!("should failed with timeout"),
        }

        sendch.try_send(Msg::Quit).unwrap();

        t.join().unwrap();
    }
}
