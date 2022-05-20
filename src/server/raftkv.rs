// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    mem,
    num::NonZeroU64,
    result,
    sync::Arc,
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, MvccProperties, Snapshot};
use kvproto::{
    errorpb,
    kvrpcpb::{Context, IsolationLevel},
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response},
};
use raft::{
    eraftpb::{self, MessageType},
    StateRole,
};
use raftstore::{
    coprocessor::{
        dispatcher::BoxReadIndexObserver, Coprocessor, CoprocessorHost, ReadIndexObserver,
    },
    errors::Error as RaftServerError,
    router::{LocalReadRouter, RaftStoreRouter},
    store::{
        Callback as StoreCallback, RaftCmdExtraOpts, ReadIndexContext, ReadResponse,
        RegionSnapshot, WriteResponse,
    },
};
use thiserror::Error;
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::{Key, TimeStamp, TxnExtra, TxnExtraScheduler, WriteBatchFlags};

use super::metrics::*;
use crate::storage::{
    self, kv,
    kv::{
        write_modifies, Callback, Engine, Error as KvError, ErrorInner as KvErrorInner,
        ExtCallback, Modify, SnapContext, WriteData,
    },
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{}", .0.get_message())]
    RequestFailed(errorpb::Error),

    #[error("{0}")]
    Io(#[from] IoError),

    #[error("{0}")]
    Server(#[from] RaftServerError),

    #[error("{0}")]
    InvalidResponse(String),

    #[error("{0}")]
    InvalidRequest(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
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

pub enum CmdRes<S>
where
    S: Snapshot,
{
    Resp(Vec<Response>),
    Snap(RegionSnapshot<S>),
}

fn check_raft_cmd_response(resp: &mut RaftCmdResponse) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }

    Ok(())
}

fn on_write_result<S>(mut write_resp: WriteResponse) -> Result<CmdRes<S>>
where
    S: Snapshot,
{
    if let Err(e) = check_raft_cmd_response(&mut write_resp.response) {
        return Err(e);
    }
    let resps = write_resp.response.take_responses();
    Ok(CmdRes::Resp(resps.into()))
}

fn on_read_result<S>(mut read_resp: ReadResponse<S>) -> Result<CmdRes<S>>
where
    S: Snapshot,
{
    if let Err(e) = check_raft_cmd_response(&mut read_resp.response) {
        return Err(e);
    }
    let resps = read_resp.response.take_responses();
    if let Some(mut snapshot) = read_resp.snapshot {
        snapshot.term = NonZeroU64::new(read_resp.response.get_header().get_current_term());
        snapshot.txn_extra_op = read_resp.txn_extra_op;
        Ok(CmdRes::Snap(snapshot))
    } else {
        Ok(CmdRes::Resp(resps.into()))
    }
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    router: S,
    engine: E,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
}

impl<E, S> RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S, engine: E) -> RaftKv<E, S> {
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
        cb: Callback<CmdRes<E::Snapshot>>,
    ) -> Result<()> {
        let mut header = self.new_request_header(&*ctx.pb_ctx);
        if ctx.pb_ctx.get_stale_read() && !ctx.start_ts.is_zero() {
            let mut data = [0u8; 8];
            (&mut data[..])
                .encode_u64(ctx.start_ts.into_inner())
                .unwrap();
            header.set_flags(WriteBatchFlags::STALE_READ.bits());
            header.set_flag_data(data.into());
        }
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        self.router
            .read(
                ctx.read_id,
                cmd,
                StoreCallback::Read(Box::new(move |resp| {
                    cb(on_read_result(resp).map_err(Error::into));
                })),
            )
            .map_err(From::from)
    }

    fn exec_write_requests(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<CmdRes<E::Snapshot>>,
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
                        if rid == region_id { None } else { Some(()) }
                    })
                    .ok_or_else(|| RaftServerError::RegionNotFound(region_id).into())
                });
                Ok(())
            };
            raftkv_early_error_report_fp()?;
        }

        let reqs: Vec<Request> = batch.modifies.into_iter().map(Into::into).collect();
        let txn_extra = batch.extra;
        let mut header = self.new_request_header(ctx);
        if txn_extra.one_pc {
            header.set_flags(WriteBatchFlags::ONE_PC.bits());
        }

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.schedule_txn_extra(txn_extra);

        let cb = StoreCallback::write_ext(
            Box::new(move |resp| {
                write_cb(on_write_result(resp).map_err(Error::into));
            }),
            proposed_cb,
            committed_cb,
        );
        let extra_opts = RaftCmdExtraOpts {
            deadline: batch.deadline,
            disk_full_opt: batch.disk_full_opt,
        };
        self.router.send_command(cmd, cb, extra_opts)?;

        Ok(())
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

impl<E, S> Display for RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<E, S> Debug for RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl<E, S> Engine for RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    type Snap = RegionSnapshot<E::Snapshot>;
    type Local = E;

    fn kv_engine(&self) -> E {
        self.engine.clone()
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> kv::Result<Self::Snap> {
        let mut region = metapb::Region::default();
        region.set_start_key(start_key.to_owned());
        region.set_end_key(end_key.to_owned());
        // Use a fake peer to avoid panic.
        region.mut_peers().push(Default::default());
        Ok(RegionSnapshot::<E::Snapshot>::from_raw(
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
                Modify::PessimisticLock(ref mut key, _) => {
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

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        self.exec_write_requests(
            ctx,
            batch,
            Box::new(move |res| match res {
                Ok(CmdRes::Resp(_)) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.saturating_elapsed_secs());
                    fail_point!("raftkv_async_write_finish");
                    write_cb(Ok(()))
                }
                Ok(CmdRes::Snap(_)) => {
                    write_cb(Err(box_err!("unexpect snapshot, should mutate instead.")))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                    write_cb(Err(e))
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
        if !ctx.key_ranges.is_empty() && !ctx.start_ts.is_zero() {
            req.mut_read_index().set_start_ts(ctx.start_ts.into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        self.exec_snapshot(
            ctx,
            req,
            Box::new(move |res| match res {
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
                    cb(Err(e))
                }
                Ok(CmdRes::Snap(s)) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    cb(Ok(s))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    cb(Err(e))
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

    fn schedule_txn_extra(&self, txn_extra: TxnExtra) {
        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }
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
    fn on_step(&self, msg: &mut eraftpb::Message, role: StateRole) {
        // Only check and return result if the current peer is a leader.
        // If it's not a leader, the read index request will be redirected to the leader later.
        if msg.get_msg_type() != MessageType::MsgReadIndex || role != StateRole::Leader {
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
                // The replica read is not compatible with `RcCheckTs` isolation level yet.
                // It's ensured in the tidb side when `RcCheckTs` is enabled for read requests,
                // the replica read would not be enabled at the same time.
                let res = self.concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        txn_types::Lock::check_ts_conflict(
                            Cow::Borrowed(lock),
                            key,
                            start_ts,
                            &Default::default(),
                            IsolationLevel::Si,
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
    }
}

#[cfg(test)]
mod tests {
    use kvproto::raft_cmdpb;
    use uuid::Uuid;

    use super::*;

    // This test ensures `ReplicaReadLockChecker` won't change UUID context of read index.
    #[test]
    fn test_replica_read_lock_checker_for_single_uuid() {
        let cm = ConcurrencyManager::new(1.into());
        let checker = ReplicaReadLockChecker::new(cm);
        let mut m = eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let uuid = Uuid::new_v4();
        let mut e = eraftpb::Entry::default();
        e.set_data(uuid.as_bytes().to_vec().into());
        m.mut_entries().push(e);

        checker.on_step(&mut m, StateRole::Leader);
        assert_eq!(m.get_entries()[0].get_data(), uuid.as_bytes());
    }

    #[test]
    fn test_replica_read_lock_check_when_not_leader() {
        let cm = ConcurrencyManager::new(1.into());
        let checker = ReplicaReadLockChecker::new(cm);
        let mut m = eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut request = raft_cmdpb::ReadIndexRequest::default();
        request.set_start_ts(100);
        let rctx = ReadIndexContext {
            id: Uuid::new_v4(),
            request: Some(request),
            locked: None,
        };
        let mut e = eraftpb::Entry::default();
        e.set_data(rctx.to_bytes().into());
        m.mut_entries().push(e);

        checker.on_step(&mut m, StateRole::Follower);
        assert_eq!(m.get_entries()[0].get_data(), rctx.to_bytes());
    }
}
