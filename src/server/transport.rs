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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use raftstore::store::{Msg as StoreMsg, Transport, Callback};
use raftstore::Result as RaftStoreResult;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raft::SnapshotStatus;
use super::{Msg, ConnData};
use util::transport::SendCh;


pub trait RaftStoreRouter: Send + Clone {
    /// Send StoreMsg, retry if failed. Try times may vary from implementation.
    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    /// Send StoreMsg.
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftCmd {
            request: req,
            callback: cb,
        })
    }

    // Report sending snapshot status.
    fn report_snapshot(&self,
                       region_id: u64,
                       to_peer_id: u64,
                       status: SnapshotStatus)
                       -> RaftStoreResult<()> {
        self.send(StoreMsg::ReportSnapshot {
            region_id: region_id,
            to_peer_id: to_peer_id,
            status: status,
        })
    }

    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::ReportUnreachable {
            region_id: region_id,
            to_peer_id: to_peer_id,
        })
    }
}

#[derive(Clone)]
pub struct ServerRaftStoreRouter {
    pub ch: SendCh<StoreMsg>,
}

impl ServerRaftStoreRouter {
    pub fn new(ch: SendCh<StoreMsg>) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter { ch: ch }
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        try!(self.ch.try_send(msg));
        Ok(())
    }

    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        try!(self.ch.send(msg));
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerTransport {
    ch: SendCh<Msg>,
    msg_id: Arc<AtomicUsize>,
}

impl ServerTransport {
    pub fn new(ch: SendCh<Msg>) -> ServerTransport {
        ServerTransport {
            ch: ch,
            msg_id: Arc::new(AtomicUsize::new(1)),
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }
}

impl Transport for ServerTransport {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        try!(self.ch.try_send(Msg::SendStore {
            store_id: to_store_id,
            data: ConnData::new(self.alloc_msg_id(), req),
        }));
        Ok(())
    }
}


// MockRaftStoreRouter is used for passing compile.
#[derive(Clone)]
pub struct MockRaftStoreRouter;

impl RaftStoreRouter for MockRaftStoreRouter {
    fn send(&self, _: StoreMsg) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn try_send(&self, _: StoreMsg) -> RaftStoreResult<()> {
        unimplemented!();
    }
}
