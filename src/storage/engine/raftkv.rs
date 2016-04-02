#![allow(dead_code)]

use server::Node;
use server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use raftstore::store::Transport;
use raftstore::errors::Error as RaftServerError;
use util::HandyRwLock;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response,
                          GetRequest, CmdType, SeekRequest, DeleteRequest, PutRequest};
use kvproto::errorpb;
use kvproto::metapb;
use kvproto::kvrpcpb::Context;

use pd::PdClient;
use uuid::Uuid;
use std::sync::{Arc, RwLock};
use std::sync::mpsc;
use std::fmt::{self, Formatter, Debug};
use std::io::Error as IoError;
use std::result;
use protobuf::RepeatedField;

use super::{Result as EngineResult, Error as EngineError, Engine, Modify};
use storage::{Key, Value, KvPair};

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RequestFailed(e: errorpb::Error) {
            from()
            description(e.get_message())
        }
        Io(e: IoError) {
            from()
            cause(e)
            description(e.description())
        }
        RocksDb(reason: String) {
            description(reason)
        }
        Server(e: RaftServerError) {
            from()
            cause(e)
            description(e.description())
        }
        InvalidResponse(reason: String) {
            description(reason)
        }
        InvalidRequest(reason: String) {
            description(reason)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for EngineError {
    fn from(e: Error) -> EngineError {
        match e {
            Error::RequestFailed(e) => EngineError::Request(e),
            e => EngineError::Other(box e),
        }
    }
}

/// RaftKv is a storage engine base on RaftKvServer.
pub struct RaftKv<T: PdClient + 'static, Trans: Transport + 'static> {
    node: Node<T, Trans>,
    router: Arc<RwLock<ServerRaftStoreRouter>>,
}

impl<T: PdClient, Trans: Transport> RaftKv<T, Trans> {
    /// Create a RaftKv using specified configuration.
    pub fn new(node: Node<T, Trans>) -> RaftKv<T, Trans> {
        let router = node.raft_store_router();
        RaftKv {
            node: node,
            router: router,
        }
    }

    fn exec_cmd_request(&self, req: RaftCmdRequest) -> Result<RaftCmdResponse> {
        let (tx, rx) = mpsc::channel();
        let uuid = req.get_header().get_uuid().to_vec();
        let l = req.get_requests().len();

        try!(self.router.rl().send_command(req,
                                           box move |r| {
                                               box_try!(tx.send(r));
                                               Ok(())
                                           }));


        // Only when tx is closed will recv return Err, which should never happen.
        let mut resp = rx.recv().unwrap();
        if resp.get_header().get_uuid() != &*uuid {
            return Err(Error::InvalidResponse("response is not correct!!!".to_owned()));
        }
        if resp.get_header().has_error() {
            return Err(Error::RequestFailed(resp.mut_header().take_error()));
        }
        if l != resp.get_responses().len() {
            return Err(Error::InvalidResponse("response count is not equal to requests, \
                                               something must go wrong."
                                                  .to_owned()));
        }
        Ok(resp)
    }

    fn new_request_header(&self, region_id: u64, peer: metapb::Peer) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(region_id);
        header.set_peer(peer);
        header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        header
    }

    fn exec_requests(&self,
                     region_id: u64,
                     peer: metapb::Peer,
                     reqs: Vec<Request>)
                     -> Result<Vec<Response>> {
        let header = self.new_request_header(region_id, peer);
        let mut command = RaftCmdRequest::new();
        command.set_header(header);
        command.set_requests(RepeatedField::from_vec(reqs));
        let mut resp = try!(self.exec_cmd_request(command));
        Ok(resp.take_responses().to_vec())
    }

    fn exec_request(&self, region_id: u64, peer: metapb::Peer, req: Request) -> Result<Response> {
        let resps = self.exec_requests(region_id, peer, vec![req]);
        resps.map(|mut v| v.remove(0))
    }
}

impl<T: PdClient, Trans: Transport> Debug for RaftKv<T, Trans> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<T: PdClient, Trans: Transport> Engine for RaftKv<T, Trans> {
    fn get(&self, ctx: &Context, key: &Key) -> EngineResult<Option<Value>> {
        let mut get = GetRequest::new();
        get.set_key(key.raw().clone());
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Get);
        req.set_get(get);
        let lead = ctx.get_peer().clone();
        let mut resp = try!(self.exec_request(ctx.get_region_id(), lead, req));
        if resp.get_cmd_type() != CmdType::Get {
            return Err(Error::InvalidResponse(format!("cmd type not match, want {:?}, got {:?}!",
                                                      CmdType::Get,
                                                      resp.get_cmd_type()))
                           .into());
        }
        let get_resp = resp.mut_get();
        if get_resp.has_value() {
            Ok(Some(get_resp.take_value()))
        } else {
            Ok(None)
        }
    }

    fn seek(&self, ctx: &Context, key: &Key) -> EngineResult<Option<KvPair>> {
        let mut seek = SeekRequest::new();
        seek.set_key(key.raw().clone());
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Seek);
        req.set_seek(seek);
        let lead = ctx.get_peer().clone();
        let mut resp = try!(self.exec_request(ctx.get_region_id(), lead, req));
        if resp.get_cmd_type() != CmdType::Seek {
            return Err(Error::InvalidResponse(format!("cmd type not match, want {:?}, got {:?}",
                                                      CmdType::Seek,
                                                      resp.get_cmd_type()))
                           .into());
        }
        let seek_resp = resp.mut_seek();
        if seek_resp.has_key() {
            Ok(Some((seek_resp.take_key(), seek_resp.take_value())))
        } else {
            Ok(None)
        }
    }

    fn write(&self, ctx: &Context, mut modifies: Vec<Modify>) -> EngineResult<()> {
        if modifies.len() == 0 {
            return Ok(());
        }
        let mut reqs = Vec::with_capacity(modifies.len());
        while !modifies.is_empty() {
            let m = modifies.pop().unwrap();
            let mut req = Request::new();
            match m {
                Modify::Delete(k) => {
                    let mut delete = DeleteRequest::new();
                    delete.set_key(k.raw().clone());
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put((k, v)) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.raw().clone());
                    put.set_value(v);
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
            }
            reqs.push(req);
        }
        try!(self.exec_requests(ctx.get_region_id(), ctx.get_peer().clone(), reqs));
        Ok(())
    }
}
