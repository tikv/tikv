// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;
use std::{borrow::Cow, io::Error as IoError};
use std::{
    fmt::{self, Debug, Display, Formatter},
    mem,
};
use std::{sync::Arc, time::Duration};

use bitflags::bitflags;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::{RocksEngine, RocksSnapshot, RocksTablePropertiesCollection};
use engine_traits::CF_DEFAULT;
use engine_traits::{CfName, KvEngine};
use engine_traits::{MvccProperties, MvccPropertiesExt, TablePropertiesExt};
use kvproto::kvrpcpb::Context;
use kvproto::raft_cmdpb::{
    CmdType, DeleteRangeRequest, DeleteRequest, PutRequest, RaftCmdRequest, RaftCmdResponse,
    RaftRequestHeader, Request, Response,
};
use kvproto::{errorpb, metapb};
use raft::eraftpb::{self, MessageType};
use txn_types::{Key, TimeStamp, TxnExtra, TxnExtraScheduler};

use super::metrics::*;
use crate::storage::kv::{
    write_modifies, Callback, CbContext, Engine, Error as KvError, ErrorInner as KvErrorInner,
    ExtCallback, Modify, SnapContext, WriteData,
};
use crate::storage::{self, kv};
use raftstore::{coprocessor::dispatcher::BoxReadIndexObserver, store::RegionSnapshot};
use raftstore::{
    coprocessor::Coprocessor,
    router::{LocalReadRouter, RaftStoreRouter},
};
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{Callback as StoreCallback, ReadIndexContext, ReadResponse, WriteResponse},
};
use raftstore::{coprocessor::ReadIndexObserver, errors::Error as RaftServerError};
use tikv_util::time::Instant;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        RequestFailed(e: errorpb::Error) {
            from()
            display("{}", e.get_message())
        }
        Io(e: IoError) {
            from()
            cause(e)
            display("{}", e)
        }

        Server(e: RaftServerError) {
            from()
            cause(e)
            display("{}", e)
        }
        InvalidResponse(reason: String) {
            display("{}", reason)
        }
        InvalidRequest(reason: String) {
            display("{}", reason)
        }
        Timeout(d: Duration) {
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
        KvError(box KvErrorInner::KeyIsLocked(_)) => {
            RequestStatusKind::err_leader_memory_lock_check
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

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<S>
where
    S: RaftStoreRouter<RocksEngine> + LocalReadRouter<RocksEngine> + 'static,
{
    router: S,
    engine: RocksEngine,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot<RocksSnapshot>),
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
    mut read_resp: ReadResponse<RocksSnapshot>,
    req_cnt: usize,
) -> (CbContext, Result<CmdRes>) {
    let mut cb_ctx = new_ctx(&read_resp.response);
    cb_ctx.txn_extra_op = read_resp.txn_extra_op;
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response, req_cnt) {
        return (cb_ctx, Err(e));
    }
    let resps = read_resp.response.take_responses();
    if let Some(snapshot) = read_resp.snapshot {
        (cb_ctx, Ok(CmdRes::Snap(snapshot)))
    } else {
        (cb_ctx, Ok(CmdRes::Resp(resps.into())))
    }
}

impl<S> RaftKv<S>
where
    S: RaftStoreRouter<RocksEngine> + LocalReadRouter<RocksEngine> + 'static,
{
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S, engine: RocksEngine) -> RaftKv<S> {
        RaftKv {
            router,
            engine,
            txn_extra_scheduler: None,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
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

    fn exec_snapshot(
        &self,
        ctx: SnapContext<'_>,
        req: Request,
        cb: Callback<CmdRes>,
    ) -> Result<()> {
        let header = self.new_request_header(&*ctx.pb_ctx);
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        self.router
            .read(
                ctx.read_id,
                cmd,
                StoreCallback::Read(Box::new(move |resp| {
                    let (cb_ctx, res) = on_read_result(resp, 1);
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
        committed_cb: Option<ExtCallback>,
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
        let mut header = self.new_request_header(ctx);
        if txn_extra.one_pc {
            header.set_flags(WriteBatchFlags::ONE_PC.bits());
        }

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }

        self.router
            .send_command(
                cmd,
                StoreCallback::write_ext(
                    Box::new(move |resp| {
                        let (cb_ctx, res) = on_write_result(resp, len);
                        write_cb((cb_ctx, res.map_err(Error::into)));
                    }),
                    proposed_cb,
                    committed_cb,
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

impl<S> Display for RaftKv<S>
where
    S: RaftStoreRouter<RocksEngine> + LocalReadRouter<RocksEngine> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S> Debug for RaftKv<S>
where
    S: RaftStoreRouter<RocksEngine> + LocalReadRouter<RocksEngine> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<S> Engine for RaftKv<S>
where
    S: RaftStoreRouter<RocksEngine> + LocalReadRouter<RocksEngine> + 'static,
{
    type Snap = RegionSnapshot<RocksSnapshot>;
    type Local = RocksEngine;

    fn kv_engine(&self) -> RocksEngine {
        self.engine.clone()
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> kv::Result<Self::Snap> {
        let mut region = metapb::Region::default();
        region.set_start_key(start_key.to_owned());
        region.set_end_key(end_key.to_owned());
        // Use a fake peer to avoid panic.
        region.mut_peers().push(Default::default());
        Ok(RegionSnapshot::<RocksSnapshot>::from_raw(
            self.engine.clone(),
            region,
        ))
    }

    fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
        for modify in &mut modifies {
            match modify {
                Modify::Delete(_, ref mut key) => {
                    let bytes = keys::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::Put(_, ref mut key, _) => {
                    let bytes = keys::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
                }
                Modify::DeleteRange(_, ref mut key1, ref mut key2, _) => {
                    let bytes = keys::data_key(key1.as_encoded());
                    *key1 = Key::from_encoded(bytes);
                    let bytes = keys::data_end_key(key2.as_encoded());
                    *key2 = Key::from_encoded(bytes);
                }
            }
        }
        write_modifies(&self.engine, modifies)
    }

    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
    ) -> kv::Result<()> {
        self.async_write_ext(ctx, batch, write_cb, None, None)
    }

    fn async_write_ext(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> kv::Result<()> {
        fail_point!("raftkv_async_write");
        if batch.modifies.is_empty() {
            return Err(KvError::from(KvErrorInner::EmptyRequest));
        }

        let reqs = modifies_to_requests(batch.modifies);
        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        self.exec_write_requests(
            ctx,
            reqs,
            batch.extra,
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(_)) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.elapsed_secs());
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
            committed_cb,
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
            e.into()
        })
    }

    fn async_snapshot(&self, mut ctx: SnapContext<'_>, cb: Callback<Self::Snap>) -> kv::Result<()> {
        fail_point!("raftkv_async_snapshot_err", |_| Err(box_err!(
            "injected error for async_snapshot"
        )));

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.start_ts.is_zero() {
            req.mut_read_index().set_start_ts(ctx.start_ts.into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        self.exec_snapshot(
            ctx,
            req,
            Box::new(move |(cb_ctx, res)| match res {
                Ok(CmdRes::Resp(mut r)) => {
                    let e = if r
                        .get(0)
                        .map(|resp| resp.get_read_index().has_locked())
                        .unwrap_or(false)
                    {
                        let locked = r[0].take_read_index().take_locked();
                        KvError::from(KvErrorInner::KeyIsLocked(locked))
                    } else {
                        invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()
                    };
                    cb((cb_ctx, Err(e)))
                }
                Ok(CmdRes::Snap(s)) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.elapsed_secs());
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

    fn release_snapshot(&self) {
        self.router.release_snapshot_cache();
    }

    fn get_properties_cf(
        &self,
        cf: CfName,
        start: &[u8],
        end: &[u8],
    ) -> kv::Result<RocksTablePropertiesCollection> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .get_range_properties_cf(cf, &start, &end)
            .map_err(|e| e.into())
    }

    fn get_mvcc_properties_cf(
        &self,
        cf: CfName,
        safe_point: TimeStamp,
        start: &[u8],
        end: &[u8],
    ) -> Option<MvccProperties> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .get_mvcc_properties_cf(cf, safe_point, &start, &end)
    }
}

#[derive(Clone)]
pub struct ReplicaReadLockChecker {
    concurrency_manager: ConcurrencyManager,
}

impl ReplicaReadLockChecker {
    pub fn new(concurrency_manager: ConcurrencyManager) -> Self {
        ReplicaReadLockChecker {
            concurrency_manager,
        }
    }

    pub fn register<E: KvEngine + 'static>(self, host: &mut CoprocessorHost<E>) {
        host.registry
            .register_read_index_observer(1, BoxReadIndexObserver::new(self));
    }
}

impl Coprocessor for ReplicaReadLockChecker {}

impl ReadIndexObserver for ReplicaReadLockChecker {
    fn on_step(&self, msg: &mut eraftpb::Message) {
        if msg.get_msg_type() != MessageType::MsgReadIndex {
            return;
        }
        assert_eq!(msg.get_entries().len(), 1);
        let mut rctx = ReadIndexContext::parse(msg.get_entries()[0].get_data()).unwrap();
        if let Some(mut request) = rctx.request.take() {
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
                }
            }
            msg.mut_entries()[0].set_data(rctx.to_bytes());
        }
    }
}

bitflags! {
    /// Additional flags for a write batch.
    /// They should be set in the `flags` field in `RaftRequestHeader`.
    pub struct WriteBatchFlags: u64 {
        /// Indicates this request is from a 1PC transaction.
        /// It helps CDC recognize 1PC transactions and handle them correctly.
        const ONE_PC = 0b00000001;
    }
}

pub fn modifies_to_requests(modifies: Vec<Modify>) -> Vec<Request> {
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
    reqs
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // This test ensures `ReplicaReadLockChecker` won't change UUID context of read index.
    #[test]
    fn test_replica_read_lock_checker_for_single_uuid() {
        let cm = ConcurrencyManager::new(1.into());
        let checker = ReplicaReadLockChecker::new(cm);
        let mut m = eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let uuid = Uuid::new_v4();
        let mut e = eraftpb::Entry::default();
        e.set_data(uuid.as_bytes().to_vec());
        m.mut_entries().push(e);

        checker.on_step(&mut m);
        assert_eq!(m.get_entries()[0].get_data(), uuid.as_bytes());
    }
}
