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
                          GetRequest, CmdType, SeekRequest, DeleteRequest, PutRequest};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;

use pd::PdClient;
use uuid::Uuid;
use std::sync::{Arc, RwLock, Mutex, Condvar};
use std::fmt::{self, Formatter, Debug};
use std::io::Error as IoError;
use std::time::Duration;
use std::result;
use rocksdb::DB;
use protobuf::RepeatedField;

use storage::engine;
use super::{Engine, Modify, Snapshot};
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

    // TODO: reuse msg code.
    pub fn call_command(&self, request: RaftCmdRequest) -> Result<RaftCmdResponse> {
        let resp: Option<RaftCmdResponse> = None;
        let pair = Arc::new((Mutex::new(resp), Condvar::new()));
        let pair2 = pair.clone();
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

        try!(self.router.rl().send_command(request,
                                           box move |resp| {
                                               let &(ref lock, ref cvar) = &*pair2;
                                               let mut v = lock.lock().unwrap();
                                               *v = Some(resp);
                                               cvar.notify_one();
                                               Ok(())
                                           }));

        let &(ref lock, ref cvar) = &*pair;
        let mut v = lock.lock().unwrap();
        while v.is_none() {
            let (resp, timeout_res) = cvar.wait_timeout(v, timeout).unwrap();
            if timeout_res.timed_out() {
                return Err(Error::Timeout(timeout));
            }

            v = resp
        }

        Ok(v.take().unwrap())
    }

    fn exec_cmd_request(&self, req: RaftCmdRequest) -> Result<RaftCmdResponse> {
        let uuid = req.get_header().get_uuid().to_vec();
        let l = req.get_requests().len();

        // Only when tx is closed will recv return Err, which should never happen.
        let mut resp = try!(self.call_command(req));
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

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        header
    }

    fn exec_requests(&self, ctx: &Context, reqs: Vec<Request>) -> Result<Vec<Response>> {
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));
        let mut resp = try!(self.exec_cmd_request(cmd));
        Ok(resp.take_responses().to_vec())
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
        let mut get = GetRequest::new();
        get.set_key(key.raw().clone());
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Get);
        req.set_get(get);
        let mut resp = try!(self.exec_request(ctx, req));
        if resp.get_cmd_type() != CmdType::Get {
            return Err(invalid_resp_type(CmdType::Get, resp.get_cmd_type()));
        }
        let get_resp = resp.mut_get();
        if get_resp.has_value() {
            Ok(Some(get_resp.take_value()))
        } else {
            Ok(None)
        }
    }

    fn seek(&self, ctx: &Context, key: &Key) -> engine::Result<Option<KvPair>> {
        let mut seek = SeekRequest::new();
        seek.set_key(key.raw().clone());
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Seek);
        req.set_seek(seek);
        let mut resp = try!(self.exec_request(ctx, req));
        if resp.get_cmd_type() != CmdType::Seek {
            return Err(invalid_resp_type(CmdType::Seek, resp.get_cmd_type()));
        }
        let seek_resp = resp.mut_seek();
        if seek_resp.has_key() {
            Ok(Some((seek_resp.take_key(), seek_resp.take_value())))
        } else {
            Ok(None)
        }
    }

    fn reverse_seek(&self, _: &Context, _: &Key) -> engine::Result<Option<KvPair>> {
        unimplemented!();
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
        let mut resp = try!(self.exec_request(ctx, req));
        if resp.get_cmd_type() != CmdType::Snap {
            return Err(invalid_resp_type(CmdType::Snap, resp.get_cmd_type()));
        }
        let region = resp.take_snap().take_region();
        // TODO figure out a way to create snapshot when apply
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
