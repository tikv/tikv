use std::collections::HashMap;
use std::option::Option;
use std::sync::{Arc, RwLock, Mutex};

use raftserver::store::{Msg as StoreMsg, Transport, SendCh as StoreSendCh, Callback};
use raftserver::{Result as RaftResult, other as raft_other};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use pd::PdClient;
use util::HandyRwLock;
use super::{SendCh, Msg, ConnData};

pub struct ServerTransport<T: PdClient> {
    cluster_id: u64,
    stores: HashMap<u64, StoreSendCh>,

    pd_client: Arc<RwLock<T>>,
    ch: SendCh,
    msg_id: Mutex<u64>,
}

impl<T: PdClient> ServerTransport<T> {
    pub fn new(cluster_id: u64, ch: SendCh, pd_client: Arc<RwLock<T>>) -> ServerTransport<T> {
        ServerTransport {
            cluster_id: cluster_id,
            stores: HashMap::new(),
            pd_client: pd_client.clone(),
            ch: ch,
            msg_id: Mutex::new(0),
        }
    }

    fn get_sendch(&self, store_id: u64) -> RaftResult<&StoreSendCh> {
        match self.stores.get(&store_id) {
            None => {
                Err(raft_other(format!("send message to invalid store {}, missing send \
                                   channel",
                                       store_id)))
            }

            Some(ch) => Ok(ch),
        }
    }
}

impl<T: PdClient> Transport for ServerTransport<T> {
    fn add_sendch(&mut self, store_id: u64, ch: StoreSendCh) {
        self.stores.insert(store_id, ch);
    }

    fn remove_sendch(&mut self, store_id: u64) -> Option<StoreSendCh> {
        self.stores.remove(&store_id)
    }

    fn send(&self, msg: RaftMessage) -> RaftResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        if let Some(ch) = self.stores.get(&to_store_id) {
            // use store send channel directly.
            return ch.send(StoreMsg::RaftMessage(msg));
        }

        let to_node_id = msg.get_to_peer().get_node_id();
        let node = try!(self.pd_client.rl().get_node(self.cluster_id, to_node_id));

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        let mut id = self.msg_id.lock().unwrap();
        *id += 1;

        self.ch
            .send(Msg::SendPeer {
                addr: node.get_address().to_owned(),
                data: ConnData::new(*id, req),
            })
            .map_err(|e| raft_other(format!("send peer to {} err {:?}", node.get_address(), e)))
    }

    // Send RaftMessage to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        let ch = try!(self.get_sendch(to_store_id));

        try!(ch.send(StoreMsg::RaftMessage(msg)));

        Ok(())
    }

    // Send RaftCmdRequest to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftResult<()> {
        let to_store_id = req.get_header().get_peer().get_store_id();
        let ch = try!(self.get_sendch(to_store_id));

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
    fn add_sendch(&mut self, _: u64, _: StoreSendCh) {
        unimplemented!();
    }

    fn remove_sendch(&mut self, _: u64) -> Option<StoreSendCh> {
        unimplemented!();
    }

    fn send_raft_msg(&self, _: RaftMessage) -> RaftResult<()> {
        unimplemented!();
    }

    fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftResult<()> {
        unimplemented!();
    }

    fn send(&self, _: RaftMessage) -> RaftResult<()> {
        unimplemented!();
    }
}
