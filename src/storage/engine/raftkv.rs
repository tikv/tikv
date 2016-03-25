#![allow(dead_code)]

use raftserver::server::Server;
use raftserver::server::Config as ServerConfig;
use raftserver::server::{SendCh, Msg};
use raftserver::server::transport::ServerTransport;
use raftserver::errors::Error as RaftServerError;
use util::HandyRwLock;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response,
                          GetRequest, CmdType, SeekRequest, DeleteRequest, PutRequest};
use kvproto::errorpb;
use kvproto::metapb;

use rocksdb::{DB, Options as DbConfig};
use pd::PdClient;
use mio::{EventLoop, EventLoopConfig};
use uuid::Uuid;
use std::sync::{Arc, RwLock};
use std::sync::mpsc;
use std::fmt::{self, Formatter, Debug};
use std::io::Error as IoError;
use std::result;
use std::thread::{self, JoinHandle};
use protobuf::RepeatedField;

use super::{Result as EngineResult, Error as EngineError, Engine, Modify};
use storage::{Key, Value, KvPair, KvOpt};

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

#[derive(Default)]
pub struct Config {
    pub server_cfg: ServerConfig,
    pub store_pathes: Vec<String>,
    pub store_cfg: Option<DbConfig>,
    pub el_cfg: EventLoopConfig,
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // DbConfig is an opaque object, we don't know what's in there without ABI.
        let store_cfg_hint = if self.store_cfg.is_some() {
            "customized"
        } else {
            "default"
        };
        write!(f,
               "{{ server_cfg: {:?}, store_pathes: {:?}, store_config: {}, event loop config: \
                {:?} }}",
               self.server_cfg,
               self.store_pathes,
               store_cfg_hint,
               self.el_cfg)
    }
}

/// RaftKv is a storage engine base on RaftKvServer.
pub struct RaftKv<T: PdClient + 'static> {
    cluster_id: u64,
    ch: SendCh,
    trans: Arc<RwLock<ServerTransport<T>>>,
    handler: Option<JoinHandle<()>>,
}

impl<T: PdClient> RaftKv<T> {
    /// Create a RaftKv using specified configuration.
    pub fn new(cfg: &Config, t: Arc<RwLock<T>>) -> Result<RaftKv<T>> {
        assert!(cfg.store_pathes.len() > 0);
        let mut engines = Vec::with_capacity(cfg.store_pathes.len());
        for path in &cfg.store_pathes {
            let db = match cfg.store_cfg {
                None => DB::open_default(path),
                Some(ref opt) => DB::open(opt, path),
            };
            match db {
                Err(e) => return Err(Error::RocksDb(e)),
                Ok(db) => engines.push(Arc::new(db)),
            };
        }

        let mut event_loop = try!(EventLoop::configured(cfg.el_cfg.clone()));
        let mut server = try!(Server::new(&mut event_loop, cfg.server_cfg.clone(), engines, t));
        let ch = server.get_sendch();
        let trans = server.get_trans();
        let handler = try!(thread::Builder::new()
                               .name("raftkv server".to_owned())
                               .spawn(move || {
                                   if let Err(e) = server.run(&mut event_loop) {
                                       error!("failed to run server: {:?}", e);
                                   }
                               }));

        Ok(RaftKv {
            cluster_id: cfg.server_cfg.cluster_id,
            ch: ch,
            handler: Some(handler),
            trans: trans,
        })
    }

    fn close(&mut self) {
        if let Err(e) = self.ch.send(Msg::Quit) {
            error!("failed to send kill message to raft server: {}", e);
        }
        let handler = self.handler.take().unwrap();
        if handler.join().is_err() {
            error!("failed to wait raftkv server quit!");
        }
        info!("raftkv engine closed.");
    }

    fn exec_cmd_request(&self, req: RaftCmdRequest) -> Result<RaftCmdResponse> {
        let (tx, rx) = mpsc::channel();
        let uuid = req.get_header().get_uuid().to_vec();
        let l = req.get_requests().len();
        try!(self.trans.wl().send_command(req,
                                          Box::new(move |r| {
                                              tx.send(r).map_err(::raftserver::errors::other)
                                          })));

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

impl<T: PdClient> Drop for RaftKv<T> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T: PdClient> Debug for RaftKv<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<T: PdClient> Engine for RaftKv<T> {
    fn get(&self, key: &Key, opt: &KvOpt) -> EngineResult<Option<Value>> {
        let mut get = GetRequest::new();
        get.set_key(key.get_rawkey().to_vec());
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Get);
        req.set_get(get);
        let lead = opt.peer.clone();
        let mut resp = try!(self.exec_request(opt.region_id, lead, req));
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

    fn seek(&self, key: &Key, opt: &KvOpt) -> EngineResult<Option<KvPair>> {
        let mut seek = SeekRequest::new();
        seek.set_key(key.get_rawkey().to_vec());
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Seek);
        req.set_seek(seek);
        let lead = opt.peer.clone();
        let mut resp = try!(self.exec_request(opt.region_id, lead, req));
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

    fn write(&self, mut modifies: Vec<Modify>, opt: &KvOpt) -> EngineResult<()> {
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
                    delete.set_key(k.get_rawkey().to_vec());
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put((k, v)) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.get_rawkey().to_vec());
                    put.set_value(v);
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
            }
            reqs.push(req);
        }
        try!(self.exec_requests(opt.region_id, opt.peer.clone(), reqs));
        Ok(())
    }
}
