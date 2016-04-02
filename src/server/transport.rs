use std::sync::{Arc, RwLock, Mutex};

use raftstore::store::{Msg as StoreMsg, Transport, Callback, SendCh};
use raftstore::Result as RaftStoreResult;
use kvproto::raft_serverpb::RaftMessage;
use kvproto::msgpb::{Message, MessageType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use pd::PdClient;
use util::HandyRwLock;
use super::{SendCh as ServerSendCh, Msg, ConnData};


pub trait RaftStoreRouter {
    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()>;
}

pub struct ServerRaftStoreRouter {
    pub store_id: u64,
    pub ch: SendCh,
}

impl ServerRaftStoreRouter {
    pub fn new(store_id: u64, ch: SendCh) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter {
            store_id: store_id,
            ch: ch,
        }
    }

    fn check_store(&self, store_id: u64) -> RaftStoreResult<()> {
        if store_id != self.store_id {
            return Err(box_err!("invalid store {} != {}", store_id, self.store_id));
        }
        Ok(())
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        try!(self.check_store(msg.get_to_peer().get_store_id()));

        try!(self.ch.send(StoreMsg::RaftMessage(msg)));

        Ok(())
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        try!(self.check_store(req.get_header().get_peer().get_store_id()));

        try!(self.ch.send(StoreMsg::RaftCmd {
            request: req,
            callback: cb,
        }));

        Ok(())
    }
}

pub struct ServerTransport<T: PdClient> {
    cluster_id: u64,

    pd_client: Arc<RwLock<T>>,
    ch: ServerSendCh,
    msg_id: Mutex<u64>,
}

impl<T: PdClient> ServerTransport<T> {
    pub fn new(cluster_id: u64, ch: ServerSendCh, pd_client: Arc<RwLock<T>>) -> ServerTransport<T> {
        ServerTransport {
            cluster_id: cluster_id,
            pd_client: pd_client.clone(),
            ch: ch,
            msg_id: Mutex::new(0),
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        let mut id = self.msg_id.lock().unwrap();
        *id += 1;
        *id
    }
}

impl<T: PdClient> Transport for ServerTransport<T> {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();

        let store = try!(self.pd_client.rl().get_store(self.cluster_id, to_store_id));

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        if let Err(e) = self.ch.send(Msg::SendPeer {
            addr: store.get_address().to_owned(),
            data: ConnData::new(self.alloc_msg_id(), req),
        }) {
            return Err(box_err!("send peer to {} err {:?}", store.get_address(), e));
        }
        Ok(())
    }
}


// MockRaftStoreRouter is used for Memory and RocksDB to pass compile.
pub struct MockRaftStoreRouter;

impl RaftStoreRouter for MockRaftStoreRouter {
    fn send_raft_msg(&self, _: RaftMessage) -> RaftStoreResult<()> {
        unimplemented!();
    }

    fn send_command(&self, _: RaftCmdRequest, _: Callback) -> RaftStoreResult<()> {
        unimplemented!();
    }
}
