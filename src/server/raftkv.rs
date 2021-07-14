// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;
use std::result;
use std::time::Duration;

use engine::IterOption;
use engine_rocks::{RocksEngine, RocksTablePropertiesCollection};
use engine_traits::{CfName, Peekable, ReadOptions, CF_DEFAULT};
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;
use kvproto::raft_cmdpb::{
    CmdType, DeleteRangeRequest, DeleteRequest, PutRequest, RaftCmdRequest, RaftCmdResponse,
    RaftRequestHeader, Request, Response,
};
use txn_types::{Key, TxnExtra, Value};

use super::metrics::*;
use crate::storage::kv::{
    Callback, CbContext, Cursor, Engine, Error as KvError, ErrorInner as KvErrorInner, ExtCallback,
    Iterator as EngineIterator, Modify, ScanMode, Snapshot, WriteData,
};
use crate::storage::{self, kv};
use raftstore::errors::Error as RaftServerError;
use raftstore::router::RaftStoreRouter;
use raftstore::store::{Callback as StoreCallback, ReadResponse, WriteResponse};
use raftstore::store::{RegionIterator, RegionSnapshot};

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
        Error::Server(_) => RequestStatusKind::err_server,
        Error::InvalidResponse(_) => RequestStatusKind::err_invalid_resp,
        Error::InvalidRequest(_) => RequestStatusKind::err_invalid_req,
        Error::Timeout(_) => RequestStatusKind::err_timeout,
    }
}

fn get_status_kind_from_engine_error(e: &kv::Error) -> RequestStatusKind {
    match *e {
        KvError(box KvErrorInner::Request(ref header)) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }

        KvError(box KvErrorInner::Timeout(_)) => RequestStatusKind::err_timeout,
        KvError(box KvErrorInner::EmptyRequest) => RequestStatusKind::err_empty_request,
        KvError(box KvErrorInner::Other(_)) => RequestStatusKind::err_other,
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => KvError::from(KvErrorInner::Request(e)),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

impl From<RaftServerError> for KvError {
    fn from(e: RaftServerError) -> KvError {
        KvError(Box::new(KvErrorInner::Request(e.into())))
    }
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S: RaftStoreRouter + 'static> {
    router: S,
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot<RocksEngine>),
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
    (cb_ctx, Ok(CmdRes::Resp(resps.into())))
}

fn on_read_result(
    mut read_resp: ReadResponse<RocksEngine>,
    req_cnt: usize,
) -> (CbContext, Result<CmdRes>) {
    let mut cb_ctx = new_ctx(&read_resp.response);
    cb_ctx.txn_extra_op = read_resp.txn_extra_op;
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = read_resp.response.take_responses();
    if !resps.is_empty() || resps[0].get_cmd_type() == CmdType::Snap {
        (cb_ctx, Ok(CmdRes::Snap(read_resp.snapshot.unwrap())))
    } else {
        (cb_ctx, Ok(CmdRes::Resp(resps.into())))
    }
}

impl<S: RaftStoreRouter> RaftKv<S> {
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S) -> RaftKv<S> {
        RaftKv { router }
    }

    fn new_request_header(&self, ctx: &Context) -> RaftRequestHeader {
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        if ctx.get_term() != 0 {
            header.set_term(ctx.get_term());
        }
        header.set_sync_log(ctx.get_sync_log());
        header.set_replica_read(ctx.get_replica_read());
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
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.router
            .send_command(
                cmd,
                StoreCallback::Read(Box::new(move |resp| {
                    let (cb_ctx, res) = on_read_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                })),
            )
            .map_err(From::from)
    }

    fn exec_write_requests(
        &self,
        ctx: &Context,
        reqs: Vec<Request>,
        txn_extra: TxnExtra,
        write_cb: Callback<CmdRes>,
        proposed_cb: Option<ExtCallback>,
    ) -> Result<()> {
        #[cfg(feature = "failpoints")]
        {
            // If rid is some, only the specified region reports error.
            // If rid is None, all regions report error.
            let raftkv_early_error_report_fp = || -> Result<()> {
                fail_point!("raftkv_early_error_report", |rid| {
                    let region_id = ctx.get_region_id();
                    rid.and_then(|rid| {
                        let rid: u64 = rid.parse().unwrap();
                        if rid == region_id {
                            None
                        } else {
                            Some(())
                        }
                    })
                    .ok_or_else(|| RaftServerError::RegionNotFound(region_id).into())
                });
                Ok(())
            };
            raftkv_early_error_report_fp()?;
        }

        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.router
            .send_command_txn_extra(
                cmd,
                txn_extra,
                StoreCallback::write_ext(
                    Box::new(move |resp| {
                        let (cb_ctx, res) = on_write_result(resp, len);
                        write_cb((cb_ctx, res.map_err(Error::into)));
                    }),
                    proposed_cb,
                ),
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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter> Debug for RaftKv<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S: RaftStoreRouter> Engine for RaftKv<S> {
    type Snap = RegionSnapshot<RocksEngine>;

    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
    ) -> kv::Result<()> {
        self.async_write_ext(ctx, batch, write_cb, None)
    }

    fn async_write_ext(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
        proposed_cb: Option<ExtCallback>,
    ) -> kv::Result<()> {
        fail_point!("raftkv_async_write");
        if batch.modifies.is_empty() {
            return Err(KvError::from(KvErrorInner::EmptyRequest));
        }

        let mut reqs = Vec::with_capacity(batch.modifies.len());
        for m in batch.modifies {
            let mut req = Request::default();
            match m {
                Modify::Delete(cf, k) => {
                    let mut delete = DeleteRequest::default();
                    delete.set_key(k.into_encoded());
                    if cf != CF_DEFAULT {
                        delete.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Delete);
                    req.set_delete(delete);
                }
                Modify::Put(cf, k, v) => {
                    let mut put = PutRequest::default();
                    put.set_key(k.into_encoded());
                    put.set_value(v);
                    if cf != CF_DEFAULT {
                        put.set_cf(cf.to_string());
                    }
                    req.set_cmd_type(CmdType::Put);
                    req.set_put(put);
                }
                Modify::DeleteRange(cf, start_key, end_key, notify_only) => {
                    let mut delete_range = DeleteRangeRequest::default();
                    delete_range.set_cf(cf.to_string());
                    delete_range.set_start_key(start_key.into_encoded());
                    delete_range.set_end_key(end_key.into_encoded());
                    delete_range.set_notify_only(notify_only);
                    req.set_cmd_type(CmdType::DeleteRange);
                    req.set_delete_range(delete_range);
                }
            }
            reqs.push(req);
        }

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC.write.start_coarse_timer();

        self.exec_write_requests(
            ctx,
            reqs,
            batch.extra,
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(_)) => {
                    req_timer.observe_duration();
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
<<<<<<< HEAD
=======
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.saturating_elapsed_secs());
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
                    fail_point!("raftkv_async_write_finish");
                    write_cb((cb_ctx, Ok(())))
                }
                Ok(CmdRes::Snap(_)) => write_cb((
                    cb_ctx,
                    Err(box_err!("unexpect snapshot, should mutate instead.")),
                )),
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                    write_cb((cb_ctx, Err(e)))
                }
            }),
            proposed_cb,
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
            e.into()
        })
    }

    fn async_snapshot(&self, ctx: &Context, cb: Callback<Self::Snap>) -> kv::Result<()> {
        fail_point!("raftkv_async_snapshot");
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);

        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let req_timer = ASYNC_REQUESTS_DURATIONS_VEC.snapshot.start_coarse_timer();

        self.exec_read_requests(
            ctx,
            vec![req],
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(r)) => cb((
                    cb_ctx,
                    Err(invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()),
                )),
                Ok(CmdRes::Snap(s)) => {
<<<<<<< HEAD
                    req_timer.observe_duration();
=======
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    cb((cb_ctx, Ok(s)))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    cb((cb_ctx, Err(e)))
                }
            }),
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
            e.into()
        })
    }
}

impl Snapshot for RegionSnapshot<RocksEngine> {
    type Iter = RegionIterator<RocksEngine>;

    fn get(&self, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get", |_| Err(box_err!(
            "injected error for get"
        )));
        let v = box_try!(self.get_value(key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf(cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> kv::Result<Option<Value>> {
        fail_point!("raftkv_snapshot_get_cf", |_| Err(box_err!(
            "injected error for get_cf"
        )));
        let v = box_try!(self.get_value_cf_opt(&opts, cf, key.as_encoded()));
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter", |_| Err(box_err!(
            "injected error for iter"
        )));
        let prefix_seek = iter_opt.prefix_seek_used();
        Ok(Cursor::new(
            RegionSnapshot::iter(self, iter_opt),
            mode,
            prefix_seek,
        ))
    }

    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        let prefix_seek = iter_opt.prefix_seek_used();
        Ok(Cursor::new(
            RegionSnapshot::iter_cf(self, cf, iter_opt)?,
            mode,
            prefix_seek,
        ))
    }

    fn get_properties_cf(&self, cf: CfName) -> kv::Result<RocksTablePropertiesCollection> {
        RegionSnapshot::get_properties_cf(self, cf).map_err(|e| e.into())
    }

    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        Some(self.get_start_key())
    }

    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        Some(self.get_end_key())
    }

    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        self.get_apply_index().ok()
    }
}

impl EngineIterator for RegionIterator<RocksEngine> {
    fn next(&mut self) -> kv::Result<bool> {
        RegionIterator::next(self).map_err(KvError::from)
    }

<<<<<<< HEAD
    fn prev(&mut self) -> kv::Result<bool> {
        RegionIterator::prev(self).map_err(KvError::from)
=======
impl ReadIndexObserver for ReplicaReadLockChecker {
    fn on_step(&self, msg: &mut eraftpb::Message) {
        if msg.get_msg_type() != MessageType::MsgReadIndex {
            return;
        }
        assert_eq!(msg.get_entries().len(), 1);
        let mut rctx = ReadIndexContext::parse(msg.get_entries()[0].get_data()).unwrap();
        if let Some(mut request) = rctx.request.take() {
            let begin_instant = Instant::now();

            let start_ts = request.get_start_ts().into();
            self.concurrency_manager.update_max_ts(start_ts);
            for range in request.mut_key_ranges().iter_mut() {
                let key_bound = |key: Vec<u8>| {
                    if key.is_empty() {
                        None
                    } else {
                        Some(txn_types::Key::from_encoded(key))
                    }
                };
                let start_key = key_bound(range.take_start_key());
                let end_key = key_bound(range.take_end_key());
                let res = self.concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        txn_types::Lock::check_ts_conflict(
                            Cow::Borrowed(lock),
                            key,
                            start_ts,
                            &Default::default(),
                        )
                    },
                );
                if let Err(txn_types::Error(box txn_types::ErrorInner::KeyIsLocked(lock))) = res {
                    rctx.locked = Some(lock);
                    REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                        .locked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                } else {
                    REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                        .unlocked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                }
            }
            msg.mut_entries()[0].set_data(rctx.to_bytes().into());
        }
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
    }

    fn seek(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek", |_| Err(box_err!(
            "injected error for iter_seek"
        )));
        RegionIterator::seek(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> kv::Result<bool> {
        fail_point!("raftkv_iter_seek_for_prev", |_| Err(box_err!(
            "injected error for iter_seek_for_prev"
        )));
        RegionIterator::seek_for_prev(self, key.as_encoded()).map_err(From::from)
    }

    fn seek_to_first(&mut self) -> kv::Result<bool> {
        RegionIterator::seek_to_first(self).map_err(KvError::from)
    }

    fn seek_to_last(&mut self) -> kv::Result<bool> {
        RegionIterator::seek_to_last(self).map_err(KvError::from)
    }

    fn valid(&self) -> kv::Result<bool> {
        RegionIterator::valid(self).map_err(KvError::from)
    }

    fn validate_key(&self, key: &Key) -> kv::Result<()> {
        self.should_seekable(key.as_encoded()).map_err(From::from)
    }

    fn key(&self) -> &[u8] {
        RegionIterator::key(self)
    }

    fn value(&self) -> &[u8] {
        RegionIterator::value(self)
    }
}
