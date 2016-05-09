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

#![allow(dead_code)]

use server::Node;
use server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use raftstore::store::{Transport, Peekable};
use raftstore::errors::Error as RaftServerError;
use raftstore::coprocessor::RegionSnapshot;
use util::HandyRwLock;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response,
                          CmdType, DeleteRequest, PutRequest};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;

use pd::PdClient;
use uuid::Uuid;
use std::sync::{Arc, RwLock};
use std::fmt::{self, Formatter, Debug};
use std::io::Error as IoError;
use std::time::Duration;
use std::result;
use rocksdb::DB;
use protobuf::RepeatedField;

use storage::engine;
use super::{Engine, Modify, Snapshot};
use util::event::Event;
use storage::{Key, Value, KvPair};

const DEFAULT_TIMEOUT_SECS: u64 = 5;

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
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for engine::Error {
    fn from(e: Error) -> engine::Error {
        match e {
            Error::RequestFailed(e) => engine::Error::Request(e),
            e => box_err!(e),
        }
    }
}

/// RaftKv is a storage engine base on RaftKvServer.
pub struct RaftKv<T: PdClient + 'static, Trans: Transport + 'static> {
    node: Node<T, Trans>,
    db: Arc<DB>,
    router: Arc<RwLock<ServerRaftStoreRouter>>,
}

impl<T: PdClient, Trans: Transport> RaftKv<T, Trans> {
    /// Create a RaftKv using specified configuration.
    pub fn new(node: Node<T, Trans>, db: Arc<DB>) -> RaftKv<T, Trans> {
        let router = node.raft_store_router();
        RaftKv {
            node: node,
            db: db,
            router: router,
        }
    }

    pub fn call_command(&self, request: RaftCmdRequest) -> Result<Event<RaftCmdResponse>> {
        let finished = Event::new();
        let finished2 = finished.clone();
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

        try!(self.router.rl().send_command(request,
                                           box move |resp| {
                                               finished2.set(resp);
                                               // Wait for response to be consumed or `finished` is
                                               // dropped.
                                               finished2.wait_clear(None);
                                               Ok(())
                                           }));

        if finished.wait_timeout(Some(timeout)) {
            return Ok(finished);
        }

        Err(Error::Timeout(timeout))
    }

    fn exec_cmd_request(&self, req: RaftCmdRequest) -> Result<Event<RaftCmdResponse>> {
        let uuid = req.get_header().get_uuid().to_vec();
        let l = req.get_requests().len();

        let resp = try!(self.call_command(req));
        try!(resp.apply(|resp| {
                     if resp.get_header().get_uuid() != &*uuid {
                         return Err(Error::InvalidResponse("response is not correct!!!"
                                                               .to_owned()));
                     }
                     if resp.get_header().has_error() {
                         return Err(Error::RequestFailed(resp.take_header().take_error()));
                     }
                     if l != resp.get_responses().len() {
                         return Err(Error::InvalidResponse("response count is not equal to \
                                                            requests, something must go wrong."
                                                               .to_owned()));
                     }
                     Ok(())
                 })
                 .unwrap());
        Ok(resp)
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(ctx.get_region_id());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        header
    }

    fn exec_requests(&self, ctx: &Context, reqs: Vec<Request>) -> Result<Vec<Response>> {
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));
        let resp = try!(self.exec_cmd_request(cmd));
        Ok(resp.take().unwrap().take_responses().to_vec())
    }

    fn exec_request(&self, ctx: &Context, req: Request) -> Result<Response> {
        let resps = self.exec_requests(ctx, vec![req]);
        resps.map(|mut v| v.remove(0))
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> engine::Error {
    Error::InvalidResponse(format!("cmd type not match, want {:?}, got {:?}!", exp, act)).into()
}

impl<T: PdClient, Trans: Transport> Debug for RaftKv<T, Trans> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<T: PdClient, Trans: Transport> Engine for RaftKv<T, Trans> {
    fn get(&self, ctx: &Context, key: &Key) -> engine::Result<Option<Value>> {
        let snap = self.snapshot(ctx).unwrap();
        snap.get(key)
    }

    fn seek(&self, ctx: &Context, key: &Key) -> engine::Result<Option<KvPair>> {
        let snap = self.snapshot(ctx).unwrap();
        snap.seek(key)
    }

    fn write(&self, ctx: &Context, mut modifies: Vec<Modify>) -> engine::Result<()> {
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
        try!(self.exec_requests(ctx, reqs));
        Ok(())
    }

    fn snapshot<'a>(&'a self, ctx: &Context) -> engine::Result<Box<Snapshot + 'a>> {
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Snap);

        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.mut_requests().push(req);

        let resp = try!(self.exec_cmd_request(cmd));
        let region = try!(resp.apply(|resp| {
                                  let mut resp = resp.take_responses().remove(0);
                                  if resp.get_cmd_type() != CmdType::Snap {
                                      return Err(invalid_resp_type(CmdType::Snap,
                                                                   resp.get_cmd_type()));
                                  }
                                  Ok(resp.take_snap().take_region().clone())
                              })
                              .unwrap());
        let snap = RegionSnapshot::from_raw(self.db.as_ref(), region);
        Ok(box snap)
    }
}

impl<'a> Snapshot for RegionSnapshot<'a> {
    fn get(&self, key: &Key) -> engine::Result<Option<Value>> {
        let v = box_try!(self.get_value(key.raw()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn seek(&self, key: &Key) -> engine::Result<Option<KvPair>> {
        let pair = box_try!(self.seek(key.raw()));
        Ok(pair)
    }

    fn reverse_seek(&self, key: &Key) -> engine::Result<Option<KvPair>> {
        let pair = box_try!(self.reverse_seek(key.raw()));
        Ok(pair)
    }
}
