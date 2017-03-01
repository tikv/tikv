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

use std::time::Instant;
use std::boxed::{Box, FnBox};
use std::fmt;
use uuid::Uuid;

use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::metapb::{self, RegionEpoch};
use raft::SnapshotStatus;
use util::escape;
use super::local_read::{PeerStatusChange, RangeChangeType};

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
    ConsistencyCheck,
    ReportRegionFlow,
}

pub struct SnapshotStatusMsg {
    pub region_id: u64,
    pub to_peer_id: u64,
    pub status: SnapshotStatus,
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

    // For split check
    SplitCheckResult {
        region_id: u64,
        epoch: RegionEpoch,
        split_key: Vec<u8>,
    },

    ReportUnreachable { region_id: u64, to_peer_id: u64 },

    // For snapshot stats.
    SnapshotStats,

    // For consistency check
    ComputeHashResult {
        region_id: u64,
        index: u64,
        hash: Vec<u8>,
    },

    RedirectRaftCmd { uuid: Uuid, request: RaftCmdRequest },

    RedirectRaftCmdResp { uuid: Uuid, resp: RaftCmdResponse },

    NewPeerStatus {
        region: metapb::Region,
        peer: metapb::Peer,
        leader_id: u64,
        term: u64,
        applied_index_term: u64,
    },

    RemovePeerStatus { region_id: u64 },

    UpdatePeerStatus(PeerStatusChange),

    RegionRangeChange {
        change_type: RangeChangeType,
        region_id: u64,
        end_key: Vec<u8>,
    },
}

impl fmt::Debug for Msg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(fmt, "Quit"),
            Msg::RaftMessage(_) => write!(fmt, "Raft Message"),
            Msg::RaftCmd { .. } => write!(fmt, "Raft Command"),
            Msg::SplitCheckResult { .. } => write!(fmt, "Split Check Result"),
            Msg::ReportUnreachable { ref region_id, ref to_peer_id } => {
                write!(fmt,
                       "peer {} for region {} is unreachable",
                       to_peer_id,
                       region_id)
            }
            Msg::SnapshotStats => write!(fmt, "Snapshot stats"),
            Msg::ComputeHashResult { region_id, index, ref hash } => {
                write!(fmt,
                       "ComputeHashResult [region_id: {}, index: {}, hash: {}]",
                       region_id,
                       index,
                       escape(&hash))
            }
            Msg::RedirectRaftCmd { uuid, .. } => write!(fmt, "RedirectRaftCmd [uuid: {}]", uuid),
            Msg::RedirectRaftCmdResp { uuid, .. } => {
                write!(fmt, "RedirectRaftCmdResp [uuid: {}]", uuid)
            }
            Msg::NewPeerStatus { ref region, .. } => {
                write!(fmt, "NewPeerStatus [region_id: {}]", region.get_id())
            }
            Msg::RemovePeerStatus { region_id } => {
                write!(fmt, "RemovePeerStatus [region_id: {}]", region_id)
            }
            Msg::UpdatePeerStatus(PeerStatusChange { region_id, .. }) => {
                write!(fmt, "UpdatePeerStatus [region_id: {}]", region_id)
            }
            Msg::RegionRangeChange { region_id, .. } => {
                write!(fmt, "RegionRangeChange [region_id: {}]", region_id)
            }
        }
    }
}

impl Msg {
    pub fn new_raft_cmd(request: RaftCmdRequest, callback: Callback) -> Msg {
        Msg::RaftCmd {
            send_time: Instant::now(),
            request: request,
            callback: callback,
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
        wait_op!(|cb: Box<FnBox(RaftCmdResponse) + 'static + Send>| {
                     sendch.try_send(Msg::new_raft_cmd(request, cb)).unwrap()
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
                Msg::RaftCmd { callback, request, .. } => {
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
