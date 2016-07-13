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


use server::Node;
use server::transport::{ServerRaftStoreRouter, RaftStoreRouter};
use raftstore::errors::Error as RaftServerError;
use raftstore::coprocessor::{RegionSnapshot, RegionIterator};
use raftstore::store::engine::Peekable;
use util::HandyRwLock;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response,
                          CmdType, DeleteRequest, PutRequest};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;

use pd::PdClient;
use uuid::Uuid;
use std::sync::{Arc, RwLock, Mutex};
use std::fmt::{self, Formatter, Debug};
use std::io::Error as IoError;
use std::time::Duration;
use std::result;
use rocksdb::DB;
use protobuf::RepeatedField;

use storage::engine;
use super::{Engine, Modify, Cursor, Snapshot, DEFAULT_CFNAME};
use util::event::Event;
use storage::{Key, Value, CfName};

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
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

impl From<RaftServerError> for engine::Error {
    fn from(e: RaftServerError) -> engine::Error {
        engine::Error::Request(e.into())
    }
}

/// RaftKv is a storage engine base on RaftKvServer.
pub struct RaftKv<C: PdClient + 'static> {
    node: Mutex<Node<C>>,
    db: Arc<DB>,
    router: Arc<RwLock<ServerRaftStoreRouter>>,
}

enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot),
}

fn on_result(mut resp: RaftCmdResponse,
             resp_cnt: usize,
             uuid: &[u8],
             db: Arc<DB>)
             -> Result<CmdRes> {
    if resp.get_header().get_uuid() != uuid {
        return Err(Error::InvalidResponse("response is not correct!!!".to_owned()));
    }
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }
    if resp_cnt != resp.get_responses().len() {
        return Err(Error::InvalidResponse("response count is not equal to requests, \
                                            something must go wrong."
            .to_owned()));
    }
    let mut resps = resp.take_responses();
    if resps.len() != 1 || resps[0].get_cmd_type() != CmdType::Snap {
        return Ok(CmdRes::Resp(resps.into_vec()));
    }
    let snap = RegionSnapshot::from_raw(db, resps[0].take_snap().take_region());
    Ok(CmdRes::Snap(snap))
}

impl<C: PdClient> RaftKv<C> {
    /// Create a RaftKv using specified configuration.
    pub fn new(node: Node<C>, db: Arc<DB>) -> RaftKv<C> {
        let router = node.raft_store_router();
        RaftKv {
            node: Mutex::new(node),
            db: db,
            router: router,
        }
    }

    fn call_command(&self, req: RaftCmdRequest) -> Result<CmdRes> {
        let uuid = req.get_header().get_uuid().to_vec();
        let l = req.get_requests().len();
        let db = self.db.clone();
        let finished = Event::new();
        let finished2 = finished.clone();
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);

        try!(self.router.rl().send_command(req,
                                           box move |resp| {
                                               let res = on_result(resp, l, &uuid, db);
                                               finished2.set(res);
                                               Ok(())
                                           }));

        if finished.wait_timeout(Some(timeout)) {
            return finished.take().unwrap();
        }

        Err(Error::Timeout(timeout))
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        header
    }

    fn exec_requests(&self, ctx: &Context, reqs: Vec<Request>) -> Result<CmdRes> {
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));
        self.call_command(cmd)
    }

    fn raw_snapshot(&self, ctx: &Context) -> Result<RegionSnapshot> {
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Snap);
        match try!(self.exec_requests(ctx, vec![req])) {
            CmdRes::Resp(rs) => Err(invalid_resp_type(CmdType::Snap, rs[0].get_cmd_type()).into()),
            CmdRes::Snap(s) => Ok(s),
        }
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!("cmd type not match, want {:?}, got {:?}!", exp, act))
}

impl<C: PdClient> Debug for RaftKv<C> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<C: PdClient> Engine for RaftKv<C> {
    fn get(&self, ctx: &Context, key: &Key) -> engine::Result<Option<Value>> {
        let snap = try!(self.snapshot(ctx));
        snap.get(key)
    }

    fn get_cf(&self, ctx: &Context, cf: CfName, key: &Key) -> engine::Result<Option<Value>> {
        let snap = try!(self.snapshot(ctx));
        snap.get_cf(cf, key)
    }

    fn iter<'a>(&'a self, ctx: &Context) -> engine::Result<Box<Cursor + 'a>> {
        let snap = try!(self.raw_snapshot(ctx));
        Ok(box RegionIterator::new(self.db.iter(), snap.get_region().clone()))
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
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::new();
                    delete.set_key(k.encoded().to_owned());
                    if cf != DEFAULT_CFNAME {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.encoded().to_owned());
                    put.set_value(v);
                    if cf != DEFAULT_CFNAME {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
            }
            reqs.push(req);
        }
        match try!(self.exec_requests(ctx, reqs)) {
            CmdRes::Resp(_) => Ok(()),
            CmdRes::Snap(_) => Err(box_err!("unexpect snapshot, should mutate instead.")),
        }
    }

    fn snapshot(&self, ctx: &Context) -> engine::Result<Box<Snapshot>> {
        let snap = try!(self.raw_snapshot(ctx));
        Ok(box snap)
    }

    fn close(&self) {
        self.node.lock().unwrap().stop();
    }
}

impl Snapshot for RegionSnapshot {
    fn get(&self, key: &Key) -> engine::Result<Option<Value>> {
        let v = box_try!(self.get_value(key.encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> engine::Result<Option<Value>> {
        let v = box_try!(self.get_value_cf(cf, key.encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter<'b>(&'b self) -> engine::Result<Box<Cursor + 'b>> {
        Ok(box RegionSnapshot::iter(self))
    }
}

impl<'a> Cursor for RegionIterator<'a> {
    fn next(&mut self) -> bool {
        RegionIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        RegionIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> engine::Result<bool> {
        RegionIterator::seek(self, &key.encoded()).map_err(|e| {
            let pb = e.into();
            engine::Error::Request(pb)
        })
    }

    fn seek_to_first(&mut self) -> bool {
        RegionIterator::seek_to_first(self)
    }

    fn seek_to_last(&mut self) -> bool {
        RegionIterator::seek_to_last(self)
    }

    fn valid(&self) -> bool {
        RegionIterator::valid(self)
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        RegionIterator::value(self)
    }
}
