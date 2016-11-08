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

use std::boxed::{Box, FnBox};
use std::fmt;

use kvproto::eraftpb::Snapshot;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::metapb::RegionEpoch;
use raft::SnapshotStatus;

pub type Callback = Box<FnBox(RaftCmdResponse) + Send>;

#[derive(Debug)]
pub enum Tick {
    Raft,
    RaftLogGc,
    SplitRegionCheck,
    CompactCheck,
    PdHeartbeat,
    PdStoreHeartbeat,
    SnapGc,
    CompactLockCf,
}

pub enum Msg {
    Quit,

    // For notify.
    RaftMessage(RaftMessage),
    RaftCmd {
        request: RaftCmdRequest,
        callback: Callback,
    },

    // For split check
    SplitCheckResult {
        region_id: u64,
        epoch: RegionEpoch,
        split_key: Vec<u8>,
    },

    MergeCheckResult { region_id: u64, epoch: RegionEpoch },

    RollbackRegionMerge { into_region_id: u64 },

    ReportSnapshot {
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    },

    ReportUnreachable { region_id: u64, to_peer_id: u64 },

    ReportStorageGc { region_id: u64, peer_id: u64 },

    // For snapshot stats.
    SnapshotStats,
    SnapGenRes {
        region_id: u64,
        snap: Option<Snapshot>,
    },
}

impl fmt::Debug for Msg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(fmt, "Quit"),
            Msg::RaftMessage(_) => write!(fmt, "Raft Message"),
            Msg::RaftCmd { .. } => write!(fmt, "Raft Command"),
            Msg::SplitCheckResult { .. } => write!(fmt, "Split Check Result"),
            Msg::MergeCheckResult { .. } => write!(fmt, "Merge Check Result"),
            Msg::RollbackRegionMerge { ref into_region_id } => {
                write!(fmt,
                       "Rollback region merge for region id {:?}",
                       into_region_id)
            }
            Msg::ReportSnapshot { ref region_id, ref to_peer_id, ref status } => {
                write!(fmt,
                       "Send snapshot to {} for region {} {:?}",
                       to_peer_id,
                       region_id,
                       status)
            }
            Msg::ReportUnreachable { ref region_id, ref to_peer_id } => {
                write!(fmt,
                       "peer {} for region {} is unreachable",
                       to_peer_id,
                       region_id)
            }
            Msg::ReportStorageGc { ref region_id, ref peer_id } => {
                write!(fmt,
                       "storage gc for region {}, peer {} is done",
                       region_id,
                       peer_id)
            }
            Msg::SnapshotStats => write!(fmt, "Snapshot stats"),
            Msg::SnapGenRes { region_id, ref snap } => {
                write!(fmt,
                       "SnapGenRes [region_id: {}, is_success: {}]",
                       region_id,
                       snap.is_some())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::boxed::FnBox;
    use std::time::Duration;

    use mio::{EventLoop, Handler};

    use super::*;
    use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
    use raftstore::Error;
    use util::transport::SendCh;

    fn call_command(sendch: &SendCh<Msg>,
                    request: RaftCmdRequest,
                    timeout: Duration)
                    -> Result<RaftCmdResponse, Error> {
        wait_event!(|cb: Box<FnBox(RaftCmdResponse) + 'static + Send>| {
            sendch.try_send(Msg::RaftCmd {
                    request: request,
                    callback: cb,
                })
                .unwrap()
        },
                    timeout)
            .ok_or_else(|| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    struct TestHandler;

    impl Handler for TestHandler {
        type Timeout = ();
        type Message = Msg;

        fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
            match msg {
                Msg::Quit => event_loop.shutdown(),
                Msg::RaftCmd { callback, request } => {
                    // a trick for test timeout.
                    if request.get_header().get_region_id() == u64::max_value() {
                        thread::sleep(Duration::from_millis(100));
                    }
                    callback.call_box((RaftCmdResponse::new(),));
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
