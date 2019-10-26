// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::io::Error as IoError;
use std::result;
use std::time::Duration;

use engine::rocks::TablePropertiesCollection;
use engine::CfName;
use engine::IterOption;
use engine::Peekable;
use engine::CF_DEFAULT;
use kvproto::errorpb;
use kvproto::kvrpcpb::Context;
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{
    CmdType, DeleteRangeRequest, DeleteRequest, PutRequest, RaftCmdRequest, RaftCmdResponse,
    RaftRequestHeader, Request, Response,
};

use super::metrics::*;
use crate::raftstore::coprocessor::{ApplyObserver, Coprocessor};
use crate::raftstore::errors::Error as RaftServerError;
use crate::raftstore::store::{Callback as StoreCallback, ReadResponse, WriteResponse};
use crate::raftstore::store::{RegionIterator, RegionSnapshot};
use crate::server::transport::RaftStoreRouter;
use crate::storage::kv::{
    Callback, CbContext, Cursor, Engine, Iterator as EngineIterator, Modify, ScanMode, Snapshot,
};
use crate::storage::{self, kv, Key, Value};

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
        kv::Error::Request(ref header) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }

        kv::Error::Timeout(_) => RequestStatusKind::err_timeout,
        kv::Error::EmptyRequest => RequestStatusKind::err_empty_request,
        kv::Error::Other(_) => RequestStatusKind::err_other,
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => kv::Error::Request(e),
            Error::Server(e) => e.into(),
            e => box_err!(e),
        }
    }
}

impl From<RaftServerError> for kv::Error {
    fn from(e: RaftServerError) -> kv::Error {
        kv::Error::Request(e.into())
    }
}

use engine::DB;
use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S: RaftStoreRouter + 'static> {
    router: S,
    apply_observer: Arc<RwLock<Option<KvApplyObserver>>>,
}

struct PendingFollowerRead {
    ctx: Context,
    applied_index: u64,
    cb: Callback<RegionSnapshot>,
}

impl Eq for PendingFollowerRead {}
impl PartialEq for PendingFollowerRead {
    fn eq(&self, other: &PendingFollowerRead) -> bool {
        self.applied_index == other.applied_index
    }
}

impl Ord for PendingFollowerRead {
    fn cmp(&self, other: &PendingFollowerRead) -> cmp::Ordering {
        other.applied_index.cmp(&self.applied_index)
    }
}

impl PartialOrd for PendingFollowerRead {
    fn partial_cmp(&self, other: &PendingFollowerRead) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct CurrentAndPending {
    applied_index: u64,
    region: Region,
    pendings: BinaryHeap<PendingFollowerRead>,
}

#[derive(Clone)]
struct ReadQueue(Arc<Mutex<CurrentAndPending>>);

#[derive(Clone)]
pub struct KvApplyObserver {
    db: Arc<DB>,
    pending_reads: Arc<RwLock<HashMap<u64, ReadQueue>>>,
}

impl KvApplyObserver {
    pub fn new(db: Arc<DB>) -> Self {
        KvApplyObserver {
            db,
            pending_reads: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    fn get_quque(&self, rid: u64) -> ReadQueue {
        let pending_reads = self.pending_reads.read().unwrap();
        if let Some(q) = pending_reads.get(&rid) {
            return q.clone();
        }
        drop(pending_reads);

        let mut pending_reads = self.pending_reads.write().unwrap();
        let q = ReadQueue(Arc::new(Mutex::new(CurrentAndPending::default())));
        let t = pending_reads.insert(rid, q.clone());
        assert!(t.is_none());
        q
    }
}

impl Coprocessor for KvApplyObserver {
    fn start(&self) {}
    fn stop(&self) {}
}

impl ApplyObserver for KvApplyObserver {
    fn on_applied_index_change(&self, r: &Region, applied: Option<u64>) {
        let rid = r.get_id();
        let applied = applied.unwrap();
        info!("{} applied index changed to {}", rid, applied);

        let rq = self.get_quque(rid);
        let mut rq_locked = rq.0.lock().unwrap();
        while let Some(req) = rq_locked.pendings.pop() {
            if req.applied_index <= applied {
                // TODO: term in ctx?
                let cb_ctx = CbContext::new();
                let req_epoch = req.ctx.get_region_epoch();
                if req_epoch.get_version() != r.get_region_epoch().get_version() {
                    let mut e = errorpb::Error::new();
                    e.mut_epoch_not_match()
                        .mut_current_regions()
                        .push(r.clone());
                    let e = crate::storage::kv::Error::Request(e);
                    (req.cb)((cb_ctx, Err(e)));
                } else {
                    let s = RegionSnapshot::from_raw(self.db.clone(), r.clone());
                    (req.cb)((cb_ctx, Ok(s)));
                }
            } else {
                rq_locked.pendings.push(req);
                break;
            }
        }
        rq_locked.applied_index = applied;
        if rq_locked.region.get_region_epoch() != r.get_region_epoch() {
            rq_locked.region = r.clone();
        }
    }
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
    (cb_ctx, Ok(CmdRes::Resp(resps.into())))
}

fn on_read_result(mut read_resp: ReadResponse, req_cnt: usize) -> (CbContext, Result<CmdRes>) {
    let cb_ctx = new_ctx(&read_resp.response);
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
        RaftKv {
            router,
            apply_observer: Arc::new(RwLock::new(None)),
        }
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
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        fail_point!("raftkv_early_error_report", |_| Err(
            RaftServerError::RegionNotFound(ctx.get_region_id()).into()
        ));
        let len = reqs.len();
        let header = self.new_request_header(ctx);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.router
            .send_command(
                cmd,
                StoreCallback::Write(Box::new(move |resp| {
                    let (cb_ctx, res) = on_write_result(resp, len);
                    cb((cb_ctx, res.map_err(Error::into)));
                })),
            )
            .map_err(From::from)
    }

    pub fn attach_apply_observer(&self, apply_observer: KvApplyObserver) {
        let mut ptr = self.apply_observer.write().unwrap();
        *ptr = Some(apply_observer);
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
    type Snap = RegionSnapshot;

    fn async_write(
        &self,
        ctx: &Context,
        modifies: Vec<Modify>,
        cb: Callback<()>,
    ) -> kv::Result<()> {
        fail_point!("raftkv_async_write");
        if modifies.is_empty() {
            return Err(kv::Error::EmptyRequest);
        }

        let mut reqs = Vec::with_capacity(modifies.len());
        for m in modifies {
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
            Box::new(move |(cb_ctx, res)| match res {
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
            }),
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
                    req_timer.observe_duration();
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

    fn async_snapshot_on_follower(
        &self,
        ctx: &Context,
        applied_index: u64,
        cb: Callback<Self::Snap>,
    ) -> kv::Result<()> {
        error!(
            "async_snapshot_on_follower, region: {}, epoch: {:?}",
            ctx.get_region_id(),
            ctx.get_region_epoch(),
        );
        let apply_observer = self.apply_observer.read().unwrap();
        let apply_observer = apply_observer.as_ref().unwrap();
        let pending_reads = apply_observer.pending_reads.read().unwrap();
        match pending_reads.get(&ctx.get_region_id()) {
            None => {
                let mut e = errorpb::Error::new();
                e.mut_region_not_found().set_region_id(ctx.get_region_id());
                let e = crate::storage::kv::Error::Request(e);
                cb((CbContext::new(), Err(e)));
            }
            Some(queue) => {
                let mut q = queue.0.lock().unwrap();
                if q.applied_index >= applied_index {
                    let db = apply_observer.db.clone();
                    let s = RegionSnapshot::from_raw(db, q.region.clone());
                    cb((CbContext::new(), Ok(s)));
                } else {
                    q.pendings.push(PendingFollowerRead {
                        ctx: ctx.clone(),
                        applied_index,
                        cb,
                    });
                }
            }
        }
        Ok(())
    }
}

impl Snapshot for RegionSnapshot {
    type Iter = RegionIterator;

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

    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> kv::Result<Cursor<Self::Iter>> {
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
    ) -> kv::Result<Cursor<Self::Iter>> {
        fail_point!("raftkv_snapshot_iter_cf", |_| Err(box_err!(
            "injected error for iter_cf"
        )));
        Ok(Cursor::new(
            RegionSnapshot::iter_cf(self, cf, iter_opt)?,
            mode,
        ))
    }

    fn get_properties_cf(&self, cf: CfName) -> kv::Result<TablePropertiesCollection> {
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
}

impl EngineIterator for RegionIterator {
    fn next(&mut self) -> bool {
        RegionIterator::next(self)
    }

    fn prev(&mut self) -> bool {
        RegionIterator::prev(self)
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

    fn seek_to_first(&mut self) -> bool {
        RegionIterator::seek_to_first(self)
    }

    fn seek_to_last(&mut self) -> bool {
        RegionIterator::seek_to_last(self)
    }

    fn valid(&self) -> bool {
        RegionIterator::valid(self)
    }

    fn status(&self) -> kv::Result<()> {
        RegionIterator::status(self).map_err(From::from)
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
