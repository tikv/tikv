// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

mod raft_extension;

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    cell::UnsafeCell,
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    mem,
    num::NonZeroU64,
    pin::Pin,
    result,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, RwLock,
    },
    task::Poll,
    time::Duration,
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, MvccProperties, Snapshot};
use futures::{future::BoxFuture, task::AtomicWaker, Future, Stream, StreamExt};
use kvproto::{
    errorpb,
    kvrpcpb::{Context, IsolationLevel},
    raft_cmdpb::{
        AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request,
        Response,
    },
};
use raft::{
    eraftpb::{self, MessageType},
    StateRole,
};
pub use raft_extension::RaftRouterWrap;
use raftstore::{
    coprocessor::{
        dispatcher::BoxReadIndexObserver, Coprocessor, CoprocessorHost, ReadIndexObserver,
    },
    errors::Error as RaftServerError,
    router::{LocalReadRouter, RaftStoreRouter},
    store::{
        self, util::encode_start_ts_into_flag_data, Callback as StoreCallback, RaftCmdExtraOpts,
        ReadCallback, ReadIndexContext, ReadResponse, RegionSnapshot, StoreMsg, WriteResponse,
    },
};
use thiserror::Error;
use tikv_kv::{write_modifies, OnAppliedCb, WriteEvent};
use tikv_util::{
    callback::must_call,
    future::{paired_future_callback, paired_must_called_future_callback},
    time::Instant,
};
use tracker::GLOBAL_TRACKERS;
use txn_types::{Key, TimeStamp, TxnExtra, TxnExtraScheduler, WriteBatchFlags};

use super::metrics::*;
use crate::storage::{
    self, kv,
    kv::{Engine, Error as KvError, ErrorInner as KvErrorInner, Modify, SnapContext, WriteData},
};

pub const ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG: &str = "async write on_applied callback is dropped";

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

    #[error("{0}")]
    Undetermined(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),
}

pub fn get_status_kind_from_engine_error(e: &kv::Error) -> RequestStatusKind {
    match *e {
        KvError(box KvErrorInner::Request(ref header)) => {
            RequestStatusKind::from(storage::get_error_kind_from_header(header))
        }
        KvError(box KvErrorInner::KeyIsLocked(_)) => {
            RequestStatusKind::err_leader_memory_lock_check
        }
        KvError(box KvErrorInner::Timeout(_)) => RequestStatusKind::err_timeout,
        KvError(box KvErrorInner::EmptyRequest) => RequestStatusKind::err_empty_request,
        KvError(box KvErrorInner::Undetermined(_)) => RequestStatusKind::err_undetermind,
        KvError(box KvErrorInner::Other(_)) => RequestStatusKind::err_other,
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<Error> for kv::Error {
    fn from(e: Error) -> kv::Error {
        match e {
            Error::RequestFailed(e) => KvError::from(KvErrorInner::Request(e)),
            Error::Undetermined(e) => KvError::from(KvErrorInner::Undetermined(e)),
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

pub fn check_raft_cmd_response(resp: &mut RaftCmdResponse) -> Result<()> {
    if resp.get_header().has_error() {
        let mut err = resp.take_header().take_error();
        if err.get_message() == ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG {
            return Err(Error::Undetermined(err.take_message()));
        }
        return Err(Error::RequestFailed(err));
    }

    Ok(())
}

fn on_write_result<S>(mut write_resp: WriteResponse) -> Result<CmdRes<S>>
where
    S: Snapshot,
{
    check_raft_cmd_response(&mut write_resp.response)?;
    let resps = write_resp.response.take_responses();
    Ok(CmdRes::Resp(resps.into()))
}

fn on_read_result<S>(mut read_resp: ReadResponse<S>) -> Result<CmdRes<S>>
where
    S: Snapshot,
{
    check_raft_cmd_response(&mut read_resp.response)?;
    let resps = read_resp.response.take_responses();
    if let Some(mut snapshot) = read_resp.snapshot {
        snapshot.term = NonZeroU64::new(read_resp.response.get_header().get_current_term());
        snapshot.txn_extra_op = read_resp.txn_extra_op;
        Ok(CmdRes::Snap(snapshot))
    } else {
        Ok(CmdRes::Resp(resps.into()))
    }
}

#[inline]
pub fn new_request_header(ctx: &Context) -> RaftRequestHeader {
    let mut header = RaftRequestHeader::default();
    header.set_region_id(ctx.get_region_id());
    header.set_peer(ctx.get_peer().clone());
    header.set_region_epoch(ctx.get_region_epoch().clone());
    if ctx.get_term() != 0 {
        header.set_term(ctx.get_term());
    }
    header.set_sync_log(ctx.get_sync_log());
    header.set_replica_read(ctx.get_replica_read());
    header.set_resource_group_name(
        ctx.get_resource_control_context()
            .get_resource_group_name()
            .to_owned(),
    );
    header
}

#[inline]
pub fn new_flashback_req(ctx: &Context, ty: AdminCmdType) -> RaftCmdRequest {
    let header = new_request_header(ctx);
    let mut req = RaftCmdRequest::default();
    req.set_header(header);
    req.mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    req.mut_admin_request().set_cmd_type(ty);
    req
}

fn exec_admin<E: KvEngine, S: RaftStoreRouter<E>>(
    router: &S,
    req: RaftCmdRequest,
) -> BoxFuture<'static, kv::Result<()>> {
    let (cb, f) = paired_future_callback();
    let res = router.send_command(
        req,
        raftstore::store::Callback::write(cb),
        RaftCmdExtraOpts::default(),
    );
    Box::pin(async move {
        res?;
        let mut resp = box_try!(f.await);
        check_raft_cmd_response(&mut resp.response)?;
        Ok(())
    })
}

pub fn drop_snapshot_callback<T>() -> kv::Result<T> {
    let bt = backtrace::Backtrace::new();
    warn!("async snapshot callback is dropped"; "backtrace" => ?bt);
    let mut err = errorpb::Error::default();
    err.set_message("async snapshot callback is dropped".to_string());
    Err(kv::Error::from(kv::ErrorInner::Request(err)))
}

pub fn async_write_callback_dropped_err() -> errorpb::Error {
    let mut err = errorpb::Error::default();
    err.set_message(ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG.to_string());
    err
}

pub fn drop_on_applied_callback() -> WriteResponse {
    let bt = backtrace::Backtrace::new();
    error!("async write on_applied callback is dropped"; "backtrace" => ?bt);
    let mut write_resp = WriteResponse {
        response: Default::default(),
    };
    write_resp
        .response
        .mut_header()
        .set_error(async_write_callback_dropped_err());
    write_resp
}

struct WriteResCore {
    ev: AtomicU8,
    result: UnsafeCell<Option<kv::Result<()>>>,
    wake: AtomicWaker,
}

struct WriteResSub {
    notified_ev: u8,
    core: Arc<WriteResCore>,
}

unsafe impl Send for WriteResSub {}

impl Stream for WriteResSub {
    type Item = WriteEvent;

    #[inline]
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut s = self.as_mut();
        let mut cur_ev = s.core.ev.load(Ordering::Acquire);
        if cur_ev == s.notified_ev {
            s.core.wake.register(cx.waker());
            cur_ev = s.core.ev.load(Ordering::Acquire);
            if cur_ev == s.notified_ev {
                return Poll::Pending;
            }
        }
        s.notified_ev = cur_ev;
        match cur_ev {
            WriteEvent::EVENT_PROPOSED => Poll::Ready(Some(WriteEvent::Proposed)),
            WriteEvent::EVENT_COMMITTED => Poll::Ready(Some(WriteEvent::Committed)),
            u8::MAX => {
                let result = unsafe { (*s.core.result.get()).take().unwrap() };
                Poll::Ready(Some(WriteEvent::Finished(result)))
            }
            e => panic!("unexpected event {}", e),
        }
    }
}

#[derive(Clone)]
struct WriteResFeed {
    core: Arc<WriteResCore>,
}

unsafe impl Send for WriteResFeed {}

impl WriteResFeed {
    fn pair() -> (Self, WriteResSub) {
        let core = Arc::new(WriteResCore {
            ev: AtomicU8::new(0),
            result: UnsafeCell::new(None),
            wake: AtomicWaker::new(),
        });
        (
            Self { core: core.clone() },
            WriteResSub {
                notified_ev: 0,
                core,
            },
        )
    }

    fn notify_proposed(&self) {
        self.core
            .ev
            .store(WriteEvent::EVENT_PROPOSED, Ordering::Release);
        self.core.wake.wake();
    }

    fn notify_committed(&self) {
        self.core
            .ev
            .store(WriteEvent::EVENT_COMMITTED, Ordering::Release);
        self.core.wake.wake();
    }

    fn notify(&self, result: kv::Result<()>) {
        unsafe {
            (*self.core.result.get()) = Some(result);
        }
        self.core.ev.store(u8::MAX, Ordering::Release);
        self.core.wake.wake();
    }
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    router: RaftRouterWrap<S, E>,
    engine: E,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
    region_leaders: Arc<RwLock<HashSet<u64>>>,
}

impl<E, S> RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    /// Create a RaftKv using specified configuration.
    pub fn new(router: S, engine: E, region_leaders: Arc<RwLock<HashSet<u64>>>) -> RaftKv<E, S> {
        RaftKv {
            router: RaftRouterWrap::new(router),
            engine,
            txn_extra_scheduler: None,
            region_leaders,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
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

    fn kv_engine(&self) -> Option<E> {
        Some(self.engine.clone())
    }

    type RaftExtension = RaftRouterWrap<S, E>;
    #[inline]
    fn raft_extension(&self) -> Self::RaftExtension {
        self.router.clone()
    }

    fn modify_on_kv_engine(
        &self,
        mut region_modifies: HashMap<u64, Vec<Modify>>,
    ) -> kv::Result<()> {
        for modifies in region_modifies.values_mut() {
            for modify in modifies.iter_mut() {
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
                    Modify::Ingest(_) => {
                        return Err(box_err!("ingest sst is not supported in local engine"));
                    }
                }
            }
        }

        write_modifies(
            &self.engine,
            region_modifies.into_values().flatten().collect(),
        )
    }

    fn precheck_write_with_ctx(&self, ctx: &Context) -> kv::Result<()> {
        let region_id = ctx.get_region_id();
        match self.region_leaders.read().unwrap().get(&region_id) {
            Some(_) => Ok(()),
            None => Err(RaftServerError::NotLeader(region_id, None).into()),
        }
    }

    type WriteRes = impl Stream<Item = WriteEvent> + Send + Unpin;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        subscribed: u8,
        on_applied: Option<OnAppliedCb>,
    ) -> Self::WriteRes {
        let mut res = (|| {
            fail_point!("raftkv_async_write");
            if batch.modifies.is_empty() {
                return Err(KvError::from(KvErrorInner::EmptyRequest));
            }
            Ok(())
        })();

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        if res.is_ok() {
            // If rid is some, only the specified region reports error.
            // If rid is None, all regions report error.
            res = (|| {
                fail_point!("raftkv_early_error_report", |rid| {
                    let region_id = ctx.get_region_id();
                    rid.and_then(|rid| {
                        let rid: u64 = rid.parse().unwrap();
                        if rid == region_id { None } else { Some(()) }
                    })
                    .ok_or_else(|| RaftServerError::RegionNotFound(region_id).into())
                });
                Ok(())
            })();
        }

        let reqs: Vec<Request> = batch.modifies.into_iter().map(Into::into).collect();
        let txn_extra = batch.extra;
        let mut header = new_request_header(ctx);
        if batch.avoid_batch {
            header.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
        }
        let mut flags = 0;
        if txn_extra.one_pc {
            flags |= WriteBatchFlags::ONE_PC.bits();
        }
        if txn_extra.allowed_in_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.schedule_txn_extra(txn_extra);

        let (tx, rx) = WriteResFeed::pair();
        if res.is_ok() {
            let proposed_cb = if !WriteEvent::subscribed_proposed(subscribed) {
                None
            } else {
                let tx = tx.clone();
                Some(Box::new(move || tx.notify_proposed()) as store::ExtCallback)
            };
            let committed_cb = if !WriteEvent::subscribed_committed(subscribed) {
                None
            } else {
                let tx = tx.clone();
                Some(Box::new(move || tx.notify_committed()) as store::ExtCallback)
            };
            let applied_tx = tx.clone();
            let applied_cb = must_call(
                Box::new(move |resp: WriteResponse| {
                    fail_point!("applied_cb_return_undetermined_err", |_| {
                        applied_tx.notify(Err(kv::Error::from(Error::Undetermined(
                            ASYNC_WRITE_CALLBACK_DROPPED_ERR_MSG.to_string(),
                        ))));
                    });
                    let mut res = match on_write_result::<E::Snapshot>(resp) {
                        Ok(CmdRes::Resp(_)) => {
                            fail_point!("raftkv_async_write_finish");
                            Ok(())
                        }
                        Ok(CmdRes::Snap(_)) => {
                            Err(box_err!("unexpect snapshot, should mutate instead."))
                        }
                        Err(e) => Err(kv::Error::from(e)),
                    };
                    if let Some(cb) = on_applied {
                        cb(&mut res);
                    }
                    applied_tx.notify(res);
                }),
                drop_on_applied_callback,
            );

            let cb = StoreCallback::write_ext(applied_cb, proposed_cb, committed_cb);
            let extra_opts = RaftCmdExtraOpts {
                deadline: batch.deadline,
                disk_full_opt: batch.disk_full_opt,
            };
            res = self
                .router
                .send_command(cmd, cb, extra_opts)
                .map_err(kv::Error::from);
        }
        if res.is_err() {
            // Note that `on_applied` is not called in this case. We send message to the
            // channel here to notify the caller that the writing ended, like
            // how the `applied_cb` does.
            tx.notify(res);
        }
        rx.inspect(move |ev| {
            let WriteEvent::Finished(res) = ev else { return };
            match res {
                Ok(()) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.saturating_elapsed_secs());
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                }
            }
        })
    }

    type SnapshotRes = impl Future<Output = kv::Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, mut ctx: SnapContext<'_>) -> Self::SnapshotRes {
        let mut res: kv::Result<()> = (|| {
            fail_point!("raftkv_async_snapshot_err", |_| {
                Err(box_err!("injected error for async_snapshot"))
            });
            Ok(())
        })();

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.key_ranges.is_empty() && ctx.start_ts.map_or(false, |ts| !ts.is_zero()) {
            req.mut_read_index()
                .set_start_ts(ctx.start_ts.as_ref().unwrap().into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now();
        let (cb, f) = paired_must_called_future_callback(drop_snapshot_callback);

        let mut header = new_request_header(ctx.pb_ctx);
        let mut flags = 0;
        let need_encoded_start_ts = ctx.start_ts.map_or(true, |ts| !ts.is_zero());
        if ctx.pb_ctx.get_stale_read() && need_encoded_start_ts {
            flags |= WriteBatchFlags::STALE_READ.bits();
        }
        if ctx.allowed_in_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);
        // Encode `start_ts` in `flag_data` for the check of stale read and flashback.
        if need_encoded_start_ts {
            encode_start_ts_into_flag_data(
                &mut header,
                ctx.start_ts.unwrap_or_default().into_inner(),
            );
        }

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        let store_cb = StoreCallback::read(Box::new(move |resp| {
            cb(on_read_result(resp).map_err(Error::into));
        }));
        let tracker = store_cb.read_tracker().unwrap();

        if res.is_ok() {
            res = self
                .router
                .read(ctx.read_id, cmd, store_cb)
                .map_err(kv::Error::from);
        }
        async move {
            let res = match res {
                Ok(()) => match f.await {
                    Ok(r) => r,
                    // Canceled may be returned during shutdown.
                    Err(e) => Err(kv::Error::from(kv::ErrorInner::Other(box_err!(e)))),
                },
                Err(e) => Err(e),
            };
            match res {
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
                    Err(e)
                }
                Ok(CmdRes::Snap(s)) => {
                    let elapse = begin_instant.saturating_elapsed_secs();
                    GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                        if tracker.metrics.read_index_propose_wait_nanos > 0 {
                            ASYNC_REQUESTS_DURATIONS_VEC
                                .snapshot_read_index_propose_wait
                                .observe(
                                    tracker.metrics.read_index_propose_wait_nanos as f64
                                        / 1_000_000_000.0,
                                );
                            // assert!(tracker.metrics.read_index_confirm_wait_nanos > 0);
                            ASYNC_REQUESTS_DURATIONS_VEC
                                .snapshot_read_index_confirm
                                .observe(
                                    tracker.metrics.read_index_confirm_wait_nanos as f64
                                        / 1_000_000_000.0,
                                );
                        } else if tracker.metrics.local_read {
                            ASYNC_REQUESTS_DURATIONS_VEC
                                .snapshot_local_read
                                .observe(elapse);
                        }
                    });
                    ASYNC_REQUESTS_DURATIONS_VEC.snapshot.observe(elapse);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    Ok(s)
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    Err(e)
                }
            }
        }
    }

    fn release_snapshot(&mut self) {
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

    fn start_flashback(&self, ctx: &Context, start_ts: u64) -> BoxFuture<'static, kv::Result<()>> {
        // Send an `AdminCmdType::PrepareFlashback` to prepare the raftstore for the
        // later flashback. Once invoked, we will update the persistent region meta and
        // the memory state of the flashback in Peer FSM to reject all read, write
        // and scheduling operations for this region when propose/apply before we
        // start the actual data flashback transaction command in the next phase.
        let mut req = new_flashback_req(ctx, AdminCmdType::PrepareFlashback);
        req.mut_admin_request()
            .mut_prepare_flashback()
            .set_start_ts(start_ts);
        exec_admin(&*self.router, req)
    }

    fn end_flashback(&self, ctx: &Context) -> BoxFuture<'static, kv::Result<()>> {
        // Send an `AdminCmdType::FinishFlashback` to unset the persistence state
        // in `RegionLocalState` and region's meta, and when that admin cmd is applied,
        // will update the memory state of the flashback
        let req = new_flashback_req(ctx, AdminCmdType::FinishFlashback);
        exec_admin(&*self.router, req)
    }

    fn hint_change_in_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) {
        self.router
            .send_store_msg(StoreMsg::ClearRegionSizeInRange { start_key, end_key })
            .unwrap_or_else(|e| {
                // Warn and ignore it.
                warn!("unsafe destroy range: failed sending ClearRegionSizeInRange"; "err" => ?e);
            });
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
        // If it's not a leader, the read index request will be redirected to the leader
        // later.
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
                        txn_types::Lock::check_ts_conflict_for_replica_read(
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

    // This test ensures `ReplicaReadLockChecker` won't change UUID context of read
    // index.
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
