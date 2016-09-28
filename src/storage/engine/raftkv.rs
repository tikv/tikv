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


use server::transport::RaftStoreRouter;
use raftstore::errors::Error as RaftServerError;
use raftstore::coprocessor::{RegionSnapshot, RegionIterator};
use raftstore::store::engine::Peekable;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response,
                          CmdType, DeleteRequest, PutRequest};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;

use uuid::Uuid;
use std::sync::Arc;
use std::fmt::{self, Formatter, Debug};
use std::io::Error as IoError;
use std::time::Duration;
use std::result;
use rocksdb::DB;
use protobuf::RepeatedField;

use storage::engine;
use super::{Engine, Modify, Cursor, Snapshot, Callback, Iterator as EngineIterator};
use storage::{Key, Value, CfName, CF_DEFAULT};
use super::metrics::*;

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

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S: RaftStoreRouter + 'static> {
    db: Arc<DB>,
    router: S,
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

impl<S: RaftStoreRouter> RaftKv<S> {
    /// Create a RaftKv using specified configuration.
    pub fn new(db: Arc<DB>, router: S) -> RaftKv<S> {
        RaftKv {
            db: db,
            router: router,
        }
    }

    fn call_command(&self, req: RaftCmdRequest, cb: Callback<CmdRes>) -> Result<()> {
        let uuid = req.get_header().get_uuid().to_vec();
        let l = req.get_requests().len();
        let db = self.db.clone();
        try!(self.router.send_command(req,
                                      box move |resp| {
                                          cb(on_result(resp, l, &uuid, db).map_err(Error::into));
                                          Ok(())
                                      }));
        Ok(())
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
        header.set_read_quorum(ctx.get_read_quorum());
        header
    }

    fn exec_requests(&self, ctx: &Context, reqs: Vec<Request>, cb: Callback<CmdRes>) -> Result<()> {
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));
        self.call_command(cmd, cb)
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!("cmd type not match, want {:?}, got {:?}!", exp, act))
}

impl<S: RaftStoreRouter> Debug for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter> Engine for RaftKv<S> {
    fn async_write(&self,
                   ctx: &Context,
                   mut modifies: Vec<Modify>,
                   cb: Callback<()>)
                   -> engine::Result<()> {
        let mut reqs = Vec::with_capacity(modifies.len());
        while !modifies.is_empty() {
            let m = modifies.pop().unwrap();
            let mut req = Request::new();
            match m {
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::new();
                    delete.set_key(k.encoded().to_owned());
                    if cf != CF_DEFAULT {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.encoded().to_owned());
                    put.set_value(v);
                    if cf != CF_DEFAULT {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
            }
            reqs.push(req);
        }

        ASYNC_REQUESTS_COUNTER_VEC.with_label_values(&["write", "all"]).inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC.with_label_values(&["write"]).start_timer();

        try!(self.exec_requests(ctx,
                                reqs,
                                box move |res| {
            match res {
                Ok(CmdRes::Resp(_)) => {
                    req_timer.observe_duration();
                    ASYNC_REQUESTS_COUNTER_VEC.with_label_values(&["write", "success"]).inc();

                    cb(Ok(()))
                }
                Ok(CmdRes::Snap(_)) => {
                    cb(Err(box_err!("unexpect snapshot, should mutate instead.")))
                }
                Err(e) => {
                    ASYNC_REQUESTS_COUNTER_VEC.with_label_values(&["write", "failed"]).inc();
                    cb(Err(e))
                }
            }
        }));
        Ok(())
    }

    fn async_snapshot(&self, ctx: &Context, cb: Callback<Box<Snapshot>>) -> engine::Result<()> {
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Snap);

        ASYNC_REQUESTS_COUNTER_VEC.with_label_values(&["snapshot", "all"]).inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC.with_label_values(&["snapshot"]).start_timer();

        try!(self.exec_requests(ctx,
                                vec![req],
                                box move |res| {
            match res {
                Ok(CmdRes::Resp(r)) => {
                    cb(Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()))
                }
                Ok(CmdRes::Snap(s)) => {
                    req_timer.observe_duration();
                    ASYNC_REQUESTS_COUNTER_VEC.with_label_values(&["snapshot", "success"]).inc();
                    cb(Ok(box s))
                }
                Err(e) => {
                    ASYNC_REQUESTS_COUNTER_VEC.with_label_values(&["snapshot", "failed"]).inc();
                    cb(Err(e))
                }
            }
        }));
        Ok(())
    }

    fn clone(&self) -> Box<Engine> {
        box RaftKv::new(self.db.clone(), self.router.clone())
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

    #[allow(needless_lifetimes)]
    fn iter<'b>(&'b self, upper_bound: Option<&[u8]>) -> engine::Result<Cursor<'b>> {
        Ok(Cursor::new(RegionSnapshot::iter(self, upper_bound)))
    }

    #[allow(needless_lifetimes)]
    fn iter_cf<'b>(&'b self,
                   cf: CfName,
                   upper_bound: Option<&[u8]>)
                   -> engine::Result<Cursor<'b>> {
        Ok(Cursor::new(try!(RegionSnapshot::iter_cf(self, cf, upper_bound))))
    }
}

impl<'a> EngineIterator for RegionIterator<'a> {
    fn next(&mut self) -> bool {
        RegionIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        RegionIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> engine::Result<bool> {
        RegionIterator::seek(self, key.encoded()).map_err(|e| {
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
