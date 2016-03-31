use std::option::Option;
use std::sync::{Arc, RwLock, Mutex};

use raftserver::store::{Msg as StoreMsg, Transport, Callback, StoreSendCh, SendCh};
use raftserver::{Result as RaftStoreResult, other as raft_other};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use pd::PdClient;
use util::HandyRwLock;
use super::{SendCh as ServerSendCh, Msg, ConnData};


pub struct ServerTransport<T: PdClient> {
    cluster_id: u64,

    store_handle: Option<StoreSendCh>,

    pd_client: Arc<RwLock<T>>,
    ch: ServerSendCh,
    msg_id: Mutex<u64>,
}

impl<T: PdClient> ServerTransport<T> {
    pub fn new(cluster_id: u64, ch: ServerSendCh, pd_client: Arc<RwLock<T>>) -> ServerTransport<T> {
        ServerTransport {
            cluster_id: cluster_id,
            store_handle: None,
            pd_client: pd_client.clone(),
            ch: ch,
            msg_id: Mutex::new(0),
        }
    }

    fn get_sendch(&self) -> RaftStoreResult<&SendCh> {
        match self.store_handle {
            None => Err(raft_other("current sender not set")),

            Some(ref h) => Ok(&h.ch),
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        let mut id = self.msg_id.lock().unwrap();
        *id += 1;
        *id
    }
}

impl<T: PdClient> Transport for ServerTransport<T> {
    fn set_sendch(&mut self, ch: StoreSendCh) {
        self.store_handle = Some(ch);
    }

    fn remove_sendch(&mut self) -> Option<StoreSendCh> {
        self.store_handle.take()
    }

    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        if self.store_handle.iter().any(|h| h.store_id == to_store_id) {
            // use store send channel directly.
            return self.store_handle.as_ref().unwrap().ch.send(StoreMsg::RaftMessage(msg));
        }

        let to_node_id = msg.get_to_peer().get_node_id();
        let node = try!(self.pd_client.rl().get_node(self.cluster_id, to_node_id));

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        self.ch
            .send(Msg::SendPeer {
                addr: node.get_address().to_owned(),
                data: ConnData::new(self.alloc_msg_id(), req),
            })
            .map_err(|e| raft_other(format!("send peer to {} err {:?}", node.get_address(), e)))
    }

    // Send RaftMessage to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let ch = try!(self.get_sendch());

        try!(ch.send(StoreMsg::RaftMessage(msg)));

        Ok(())
    }

    // Send RaftCmdRequest to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        let ch = try!(self.get_sendch());

        try!(ch.send(StoreMsg::RaftCmd {
            request: req,
            callback: cb,
        }));

        Ok(())
    }
}

// FakeTransport is used for Memory and RocksDB to pass compile.
pub struct MockTransport;

impl Transport for MockTransport {
    fn set_sendch(&mut self, _: StoreSendCh) {
        unimplemented!();
    }

    fn remove_sendch(&mut self) -> Option<StoreSendCh> {
        unimplemented!();
    }

    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn send(&self, _: RaftMessage) -> RaftStoreResult<()> {
        unimplemented!();
    }
}
