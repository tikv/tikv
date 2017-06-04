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
use raftstore::{Result as RaftStoreResult, Error as RaftStoreError};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use super::{Msg, ConnData};
use util::transport::SendCh;
use super::metrics::*;

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
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    fn report_unreachable(&self, region_id: u64, to_peer_id: u64, _: u64) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::ReportUnreachable {
            region_id: region_id,
            to_peer_id: to_peer_id,
        })
    }
}

#[derive(Clone)]
pub struct ServerRaftStoreRouter {
    pub ch: SendCh<StoreMsg>,
    store_id: u64,
}

impl ServerRaftStoreRouter {
    pub fn new(ch: SendCh<StoreMsg>, store_id: u64) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter {
            ch: ch,
            store_id: store_id,
        }
    }

    fn validate_store_id(&self, store_id: u64) -> RaftStoreResult<()> {
        if store_id != self.store_id {
            let store = store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER.with_label_values(&["store_not_match", &*store]).inc();
            Err(RaftStoreError::StoreNotMatch(store_id, self.store_id))
        } else {
            Ok(())
        }
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

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let store_id = msg.get_to_peer().get_store_id();
        try!(self.validate_store_id(store_id));
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        let store_id = req.get_header().get_peer().get_store_id();
        try!(self.validate_store_id(store_id));
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    fn report_unreachable(&self,
                          region_id: u64,
                          to_peer_id: u64,
                          to_store_id: u64)
                          -> RaftStoreResult<()> {
        let store = to_store_id.to_string();
        REPORT_FAILURE_MSG_COUNTER.with_label_values(&["unreachable", &*store]).inc();
        self.try_send(StoreMsg::ReportUnreachable {
            region_id: region_id,
            to_peer_id: to_peer_id,
        })
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

    fn batch_send(&self, msgs: Vec<RaftMessage>) -> RaftStoreResult<()> {
        let mut batch = Vec::with_capacity(msgs.len());
        for msg in msgs {
            let to_store_id = msg.get_to_peer().get_store_id();

            let mut req = Message::new();
            req.set_msg_type(MessageType::Raft);
            req.set_raft(msg);

            batch.push((to_store_id, ConnData::new(self.alloc_msg_id(), req)));
        }

        try!(self.ch.try_send(Msg::BatchSendStores { batch: batch }));
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

#[cfg(test)]
mod tests {
    extern crate mio;

    use super::*;
    use raftstore::store::Msg;
    use util::transport::SendCh;
    use kvproto::metapb::Peer;
    use kvproto::raft_serverpb::RaftMessage;
    use kvproto::raft_cmdpb::{RaftRequestHeader, RaftCmdRequest};
    use mio::{EventLoop, Handler};

    struct FooHandler;

    impl Handler for FooHandler {
        type Timeout = ();
        type Message = Msg;
    }

    fn new_raft_msg(store_id: u64) -> RaftMessage {
        let mut peer = Peer::new();
        peer.set_store_id(store_id);
        let mut msg = RaftMessage::new();
        msg.set_to_peer(peer);
        msg
    }

    fn new_raft_cmd(store_id: u64) -> RaftCmdRequest {
        let mut peer = Peer::new();
        peer.set_store_id(store_id);
        let mut header = RaftRequestHeader::new();
        header.set_peer(peer);
        let mut msg = RaftCmdRequest::new();
        msg.set_header(header);
        msg
    }

    #[test]
    fn test_store_not_match() {
        let store_id = 1;
        let invalid_store_id = store_id + 1;

        let evloop = EventLoop::<FooHandler>::new().unwrap();
        let sendch = SendCh::new(evloop.channel(), "test-store");
        let router = ServerRaftStoreRouter::new(sendch, store_id);

        let msg = new_raft_msg(store_id);
        let cmd = new_raft_cmd(store_id);
        assert!(router.send_raft_msg(msg).is_ok());
        let cb = |_| {};
        assert!(router.send_command(cmd, box cb).is_ok());

        let msg = new_raft_msg(invalid_store_id);
        let cmd = new_raft_cmd(invalid_store_id);
        assert!(router.send_raft_msg(msg).is_err());
        let cb = |_| {};
        assert!(router.send_command(cmd, box cb).is_err());
    }
}
