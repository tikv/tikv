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
use std::path::Path;

use raftstore::store::{Msg as StoreMsg, Transport, Callback, SendCh};
use raftstore::Result as RaftStoreResult;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raft::SnapshotStatus;
use super::{SendCh as ServerSendCh, Msg, ConnData};


pub trait RaftStoreRouter: Send + Sync {
    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()>;

    // Report sending snapshot status.
    fn report_snapshot(&self,
                       region_id: u64,
                       to_peer_id: u64,
                       status: SnapshotStatus)
                       -> RaftStoreResult<()>;

    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()>;
}

pub struct ServerRaftStoreRouter {
    pub ch: SendCh,
}

impl ServerRaftStoreRouter {
    pub fn new(ch: SendCh) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter { ch: ch }
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        try!(self.ch.send(StoreMsg::RaftMessage(msg)));

        Ok(())
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        try!(self.ch.send(StoreMsg::RaftCmd {
            request: req,
            callback: cb,
        }));

        Ok(())
    }

    fn report_snapshot(&self,
                       region_id: u64,
                       to_peer_id: u64,
                       status: SnapshotStatus)
                       -> RaftStoreResult<()> {
        try!(self.ch.send(StoreMsg::ReportSnapshot {
            region_id: region_id,
            to_peer_id: to_peer_id,
            status: status,
        }));

        Ok(())
    }

    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        try!(self.ch.send(StoreMsg::ReportUnreachable {
            region_id: region_id,
            to_peer_id: to_peer_id,
        }));

        Ok(())
    }
}

pub struct ServerTransport {
    ch: ServerSendCh,
    msg_id: AtomicUsize,
}

impl ServerTransport {
    pub fn new(ch: ServerSendCh) -> ServerTransport {
        ServerTransport {
            ch: ch,
            msg_id: AtomicUsize::new(1),
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

        if let Err(e) = self.ch.send(Msg::SendStore {
            store_id: to_store_id,
            data: ConnData::new(self.alloc_msg_id(), req),
        }) {
            return Err(box_err!("send data to store {} err {:?}", to_store_id, e));
        }
        Ok(())
    }

    fn send_snapshot(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        if let Err(e) = self.ch.send(Msg::SendStore {
            store_id: to_store_id,
            data: ConnData::new(self.alloc_msg_id(), req),
        }) {
            return Err(box_err!("send data to store {} err {:?}", to_store_id, e));
        }
        Ok(())
    }
}


// MockRaftStoreRouter is used for passing compile.
pub struct MockRaftStoreRouter;

impl RaftStoreRouter for MockRaftStoreRouter {
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn report_snapshot(&self, _: u64, _: u64, _: SnapshotStatus) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn report_unreachable(&self, _: u64, _: u64) -> RaftStoreResult<()> {
        unimplemented!();
    }
}
