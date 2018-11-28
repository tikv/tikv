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

use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;
use std::result;
use std::sync::mpsc;
use std::time::Duration;

use kvproto::errorpb;
use kvproto::kvrpcpb::Context;
use kvproto::raft_cmdpb::{
    CmdType, DeleteRangeRequest, DeleteRequest, PutRequest, RaftCmdRequest, RaftCmdResponse,
    RaftRequestHeader, Request, Response,
};
use protobuf::RepeatedField;

use super::metrics::*;
use super::{
    Callback, CbContext, Cursor, Engine, Iterator as EngineIterator, Modify, RegionInfoProvider,
    ScanMode, Snapshot,
};
use raftstore::errors::Error as RaftServerError;
use raftstore::store::engine::IterOption;
use raftstore::store::engine::Peekable;
use raftstore::store::{Callback as StoreCallback, ReadResponse, WriteResponse};
use raftstore::store::{
    Msg as StoreMsg, RegionIterator, RegionSnapshot, SeekRegionFilter, SeekRegionResult,
};
use rocksdb::TablePropertiesCollection;
use server::transport::RaftStoreRouter;
use storage::{self, engine, CfName, Key, Value, CF_DEFAULT};

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

fn get_status_kind_from_error(e: &Error) -> RequestStatusKind {
    match *e {
        Error::RequestFailed(ref header) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }
        Error::Io(_) => RequestStatusKind::err_io,
        Error::RocksDb(_) => RequestStatusKind::err_rocksdb,
        Error::Server(_) => RequestStatusKind::err_server,
        Error::InvalidResponse(_) => RequestStatusKind::err_invalid_resp,
        Error::InvalidRequest(_) => RequestStatusKind::err_invalid_req,
        Error::Timeout(_) => RequestStatusKind::err_timeout,
    }
}

fn get_status_kind_from_engine_error(e: &engine::Error) -> RequestStatusKind {
    match *e {
        engine::Error::Request(ref header) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }
        engine::Error::RocksDb(_) => RequestStatusKind::err_rocksdb,
        engine::Error::Timeout(_) => RequestStatusKind::err_timeout,
        engine::Error::EmptyRequest => RequestStatusKind::err_empty_request,
        engine::Error::Other(_) => RequestStatusKind::err_other,
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
    router: S,
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot),
}

fn new_ctx(resp: &RaftCmdResponse) -> CbContext {
    let mut cb_ctx = CbContext::new();
    cb_ctx.term = Some(resp.get_header().get_current_term());
    cb_ctx
}

fn check_raft_cmd_response(resp: &mut RaftCmdResponse, req_cnt: usize) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }
    if req_cnt != resp.get_responses().len() {
        return Err(Error::InvalidResponse(format!(
            "responses count {} is not equal to requests count {}",
            resp.get_responses().len(),
            req_cnt
        )));
    }

    Ok(())
}

fn on_write_result(mut write_resp: WriteResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&write_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut write_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = write_resp.response.take_responses();
    (cb_ctx, Ok(CmdRes::Resp(resps.into_vec())))
}

fn on_read_result(mut read_resp: ReadResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&read_resp.response);
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = read_resp.response.take_responses();
    if resps.len() >= 1 || resps[0].get_cmd_type() == CmdType::Snap {
        (cb_ctx, Ok(CmdRes::Snap(read_resp.snapshot.unwrap())))
    } else {
        (cb_ctx, Ok(CmdRes::Resp(resps.into_vec())))
    }
}

impl<S: RaftStoreRouter> RaftKv<S> {
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S) -> RaftKv<S> {
        RaftKv { router }
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::new();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        if ctx.get_term() != 0 {
            header.set_term(ctx.get_term());
        }
        header.set_sync_log(ctx.get_sync_log());
        header
    }

    fn exec_read_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));

        self.router
            .send_command(
                cmd,
                StoreCallback::Read(box move |resp| {
                    let (cb_ctx, res) = on_read_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                }),
            )
            .map_err(From::from)
    }

    fn exec_write_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        fail_point!("raftkv_early_error_report", |_| Err(
            RaftServerError::RegionNotFound(ctx.get_region_id()).into()
        ));
        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::new();
        cmd.set_header(header);
        cmd.set_requests(RepeatedField::from_vec(reqs));

        self.router
            .send_command(
                cmd,
                StoreCallback::Write(box move |resp| {
                    let (cb_ctx, res) = on_write_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                }),
            )
            .map_err(From::from)
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

impl<S: RaftStoreRouter> Display for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter> Debug for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter> Engine for RaftKv<S> {
    type Iter = RegionIterator;
    type Snap = RegionSnapshot;

    fn async_write(
        &self,
        ctx: &Context,
        modifies: Vec<Modify>,
        cb: Callback<()>,
    ) -> engine::Result<()> {
        fail_point!("raftkv_async_write");
        if modifies.is_empty() {
            return Err(engine::Error::EmptyRequest);
        }

        let mut reqs = Vec::with_capacity(modifies.len());
        for m in modifies {
            let mut req = Request::new();
            match m {
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::new();
                    delete.set_key(k.into_encoded());
                    if cf != CF_DEFAULT {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::new();
                    put.set_key(k.into_encoded());
                    put.set_value(v);
                    if cf != CF_DEFAULT {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
                Modify::DeleteRange(cf, start_key, end_key) => {
                    let mut delete_range = DeleteRangeRequest::new();
                    delete_range.set_cf(cf.to_string());
                    delete_range.set_start_key(start_key.into_encoded());
                    delete_range.set_end_key(end_key.into_encoded());
                    req.set_cmd_type(CmdType::DeleteRange);
                    req.set_delete_range(delete_range);
                }
            }
            reqs.push(req);
        }

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC.write.start_coarse_timer();

        self.exec_write_requests(ctx, reqs, box move |(cb_ctx, res)| match res {
            Ok(CmdRes::Resp(_)) => {
                req_timer.observe_duration();
                ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                fail_point!("raftkv_async_write_finish");
                cb((cb_ctx, Ok(())))
            }
            Ok(CmdRes::Snap(_)) => cb((
                cb_ctx,
                Err(box_err!("unexpect snapshot, should mutate instead.")),
            )),
            Err(e) => {
                let status_kind = get_status_kind_from_engine_error(&e);
                ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                cb((cb_ctx, Err(e)))
            }
        }).map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
            e.into()
        })
    }

    fn async_snapshot(&self, ctx: &Context, cb: Callback<Self::Snap>) -> engine::Result<()> {
        fail_point!("raftkv_async_snapshot");
        let mut req = Request::new();
        req.set_cmd_type(CmdType::Snap);

        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC.snapshot.start_coarse_timer();

        self.exec_read_requests(ctx, vec![req], box move |(cb_ctx, res)| match res {
            Ok(CmdRes::Resp(r)) => cb((
                cb_ctx,
                Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()),
            )),
            Ok(CmdRes::Snap(s)) => {
                req_timer.observe_duration();
                ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                cb((cb_ctx, Ok(s)))
            }
            Err(e) => {
                let status_kind = get_status_kind_from_engine_error(&e);
                ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                cb((cb_ctx, Err(e)))
            }
        }).map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
            e.into()
        })
    }
}

impl<S: RaftStoreRouter> RegionInfoProvider for RaftKv<S> {
    // This method may block until raftstore returns the result.
    fn seek_region(
        &self,
        from_key: &[u8],
        filter: SeekRegionFilter,
        limit: u32,
    ) -> engine::Result<SeekRegionResult> {
        let (tx, rx) = mpsc::channel();
        let callback = box move |result| {
            tx.send(result).unwrap_or_else(|e| {
                panic!(
                    "raftstore failed to send seek_local_region result back to raft router: {:?}",
                    e
                );
            });
        };

        self.router.try_send(StoreMsg::SeekRegion {
            from_key: from_key.to_vec(),
            filter,
            limit,
            callback,
        })?;
        rx.recv().map_err(|e| {
            box_err!(
                "failed to receive seek_local_region result from raftstore: {:?}",
                e
            )
        })
    }
}

impl Snapshot for RegionSnapshot {
    type Iter = RegionIterator;

    fn get(&self, key: &Key) -> engine::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get", |_| Err(box_err!(
            "injected error for get"
        )));
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> engine::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf(cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> engine::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter", |_| Err(box_err!(
            "injected error for iter"
        )));
        Ok(Cursor::new(RegionSnapshot::iter(self, iter_opt), mode))
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> engine::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        Ok(Cursor::new(
            RegionSnapshot::iter_cf(self, cf, iter_opt)?,
            mode,
        ))
    }

    fn get_properties_cf(&self, cf: CfName) -> engine::Result<TablePropertiesCollection> {
        RegionSnapshot::get_properties_cf(self, cf).map_err(|e| e.into())
    }
}

impl EngineIterator for RegionIterator {
    fn next(&mut self) -> bool {
        RegionIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        RegionIterator::prev(self)
    }

    fn seek(&mut self, key: &Key) -> engine::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        RegionIterator::seek(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> engine::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        RegionIterator::seek_for_prev(self, key.as_encoded()).map_err(From::from)
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

    fn validate_key(&self, key: &Key) -> engine::Result<()> {
        self.should_seekable(key.as_encoded()).map_err(From::from)
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        RegionIterator::value(self)
    }
}
