use std::collections::HashMap;
use std::option::Option;

use raftserver::store::{Transport, SendCh as StoreSendCh, Callback};
use raftserver::{Result, other};
use proto::raft_serverpb::RaftMessage;
use proto::raft_cmdpb::RaftCommandRequest;

pub struct ServerTransport {
    node_id: u64,
    stores: HashMap<u64, StoreSendCh>,
}

impl ServerTransport {
    pub fn new(node_id: u64) -> ServerTransport {
        ServerTransport {
            node_id: node_id,
            stores: HashMap::new(),
        }
    }
}

impl ServerTransport {
    pub fn add_sendch(&mut self, store_id: u64, ch: StoreSendCh) {
        self.stores.insert(store_id, ch);
    }

    pub fn remove_sendch(&mut self, store_id: u64) -> Option<StoreSendCh> {
        self.stores.remove(&store_id)
    }

    // Send RaftMessage to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    pub fn send_raft_msg(&self, msg: RaftMessage) -> Result<()> {
        let to_node_id = msg.get_to_peer().get_store_id();
        let to_store_id = msg.get_to_peer().get_store_id();
        let ch = try!(self.get_sendch(to_node_id, to_store_id));

        ch.send_raft_msg(msg)
    }

    // Send RaftCommandRequest to specified store, the store must exist in current node.
    // Unlike Transport trait Send, this function can only send message to local store.
    pub fn send_command(&self, msg: RaftCommandRequest, cb: Callback) -> Result<()> {
        let to_node_id = msg.get_header().get_peer().get_store_id();
        let to_store_id = msg.get_header().get_peer().get_store_id();
        let ch = try!(self.get_sendch(to_node_id, to_store_id));

        ch.send_command(msg, cb)
    }

    fn get_sendch(&self, node_id: u64, store_id: u64) -> Result<&StoreSendCh> {
        if node_id != self.node_id {
            return Err(other(format!("message should be sent to node {}, but we are {}",
                                     node_id,
                                     self.node_id)));
        }

        match self.stores.get(&store_id) {
            None => {
                Err(other(format!("send message to invalid store {} in node {}, missing send \
                                   channel",
                                  store_id,
                                  node_id)))
            }

            Some(ch) => Ok(ch),
        }
    }
}

impl Transport for ServerTransport {
    fn send(&self, msg: RaftMessage) -> Result<()> {
        let to_node_id = msg.get_to_peer().get_store_id();
        if to_node_id == self.node_id {
            // We will send message to ourself node,
            // use store send channel directly.
            return self.send_raft_msg(msg);
        }

        // TODO: use pd client to get the proper address for remote sending.

        Err(other("unsupported send now..."))
    }
}
