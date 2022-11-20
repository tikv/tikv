// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    marker::PhantomData,
    mem,
    num::NonZeroU64,
    result,
    sync::{Arc, RwLock},
    time::Duration,
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, MvccProperties, Snapshot};
use futures::{future::BoxFuture, Future, StreamExt};
use kvproto::{
    errorpb,
    kvrpcpb::{Context, DiskFullOpt, IsolationLevel},
    metapb::{Region, RegionEpoch},
    raft_cmdpb::{
        AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request,
        Response,
    },
    raft_serverpb::RaftMessage,
};
use raft::{
    eraftpb::{self, MessageType},
    SnapshotStatus, StateRole,
};
use raftstore::{
    coprocessor::{
        dispatcher::BoxReadIndexObserver, Coprocessor, CoprocessorHost, ReadIndexObserver,
    },
    errors::Error as RaftServerError,
    router::{LocalReadRouter, RaftStoreRouter},
    store::{
        region_meta::{RaftStateRole, RegionMeta},
        Callback as StoreCallback, CasualMessage, PeerMsg, RaftCmdExtraOpts, ReadIndexContext,
        ReadResponse, RegionSnapshot, SignificantMsg, StoreMsg, WriteResponse,
    },
};
use thiserror::Error;
use tikv_kv::{
    raft_extension::RaftExtension, write_modifies, OnReturnCallback, WriteSubscriber,
    SUBSCRIBE_COMMITTED, SUBSCRIBE_PROPOSED,
};
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::{Key, TimeStamp, TxnExtra, TxnExtraScheduler, WriteBatchFlags};

use super::metrics::*;
use crate::storage::{
    self, kv,
    kv::{
        Engine, Error as KvError, ErrorInner as KvErrorInner, ExtCallback, Modify, SnapContext,
        WriteData,
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

pub(crate) fn get_status_kind_from_engine_error(e: &kv::Error) -> RequestStatusKind {
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

pub fn check_raft_cmd_response(resp: &mut RaftCmdResponse) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
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

enum WriteEvent {
    Proposed,
    Committed,
    Applied(kv::Result<()>),
}

pub struct RaftKvWriteSubscriber {
    receiver: futures::channel::mpsc::Receiver<WriteEvent>,
    last_event: Option<WriteEvent>,
}

impl WriteSubscriber for RaftKvWriteSubscriber {
    type ProposedWaiter<'a> = impl Future<Output = bool> + 'a
    where
        Self: 'a;

    fn wait_proposed(&mut self) -> Self::ProposedWaiter<'_> {
        async move {
            if self.last_event.is_some() {
                return false;
            }
            match self.receiver.next().await {
                Some(WriteEvent::Proposed) => return true,
                e @ (Some(WriteEvent::Committed) | Some(WriteEvent::Applied(_))) => {
                    self.last_event = e
                }
                None => (),
            }
            false
        }
    }

    type CommittedWaiter<'a> = impl Future<Output = bool> + 'a
    where
        Self: 'a;

    fn wait_committed(&mut self) -> Self::CommittedWaiter<'_> {
        async move {
            match &self.last_event {
                Some(WriteEvent::Committed) => return true,
                Some(WriteEvent::Applied(_)) => return false,
                _ => (),
            }
            let mut res = self.receiver.next().await;
            if let Some(WriteEvent::Proposed) = res {
                res = self.receiver.next().await;
            }
            match res {
                Some(WriteEvent::Committed) => return true,
                e @ Some(WriteEvent::Applied(_)) => self.last_event = e,
                None => (),
                Some(WriteEvent::Proposed) => unreachable!(),
            }
            false
        }
    }

    type ResultWaiter = impl Future<Output = Option<kv::Result<()>>>;

    fn result(mut self) -> Self::ResultWaiter {
        async move {
            if let Some(WriteEvent::Applied(r)) = self.last_event {
                return Some(r);
            }
            loop {
                let res = self.receiver.next().await;
                match res {
                    Some(WriteEvent::Applied(r)) => return Some(r),
                    Some(_) => continue,
                    None => return None,
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RouterExtension<E: KvEngine, S: RaftStoreRouter<E> + 'static> {
    router: S,
    _phantom: PhantomData<E>,
}

impl<E: KvEngine, S: RaftStoreRouter<E>> RouterExtension<E, S> {
    pub fn new(router: S) -> Self {
        Self {
            router,
            _phantom: PhantomData,
        }
    }
}

impl<E: KvEngine, S: RaftStoreRouter<E>> RaftExtension for RouterExtension<E, S> {
    #[inline]
    fn feed(&self, msg: RaftMessage, key_message: bool) {
        // Channel full and region not found are ignored.
        let region_id = msg.get_region_id();
        let msg_ty = msg.get_message().get_msg_type();
        if let Err(e) = self.router.send_raft_msg(msg) && key_message {
            error!("failed to send raft message"; "region_id" => region_id, "msg_ty" => ?msg_ty, "err" => ?e);
        }
    }

    #[inline]
    fn report_reject_message(&self, region_id: u64, from_peer_id: u64) {
        let m = CasualMessage::RejectRaftAppend {
            peer_id: from_peer_id,
        };
        let _ = self.router.send_casual_msg(region_id, m);
    }

    #[inline]
    fn report_peer_unreachable(&self, region_id: u64, to_peer_id: u64) {
        let msg = SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        };
        let _ = self.router.significant_send(region_id, msg);
    }

    #[inline]
    fn report_store_unreachable(&self, store_id: u64) {
        let _ = self
            .router
            .send_store_msg(StoreMsg::StoreUnreachable { store_id });
    }

    #[inline]
    fn report_snapshot_status(&self, region_id: u64, to_peer_id: u64, status: SnapshotStatus) {
        let msg = SignificantMsg::SnapshotStatus {
            region_id,
            to_peer_id,
            status,
        };
        if let Err(e) = self.router.significant_send(region_id, msg) {
            error!(?e;
                "report snapshot to peer failes";
                "to_peer_id" => to_peer_id,
                "status" => ?status,
                "region_id" => region_id,
            );
        }
    }

    #[inline]
    fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreResolved { store_id, group_id })
        })
    }

    #[inline]
    fn split(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: String,
    ) -> BoxFuture<'static, kv::Result<Vec<Region>>> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let req = CasualMessage::SplitRegion {
            region_epoch,
            split_keys,
            callback: raftstore::store::Callback::write(Box::new(move |res| {
                let _ = tx.send(res);
            })),
            source: source.into(),
        };
        let res = self.router.send_casual_msg(region_id, req);
        Box::pin(async move {
            res?;
            let mut admin_resp = box_try!(rx.await);
            check_raft_cmd_response(&mut admin_resp.response)?;
            let regions = admin_resp
                .response
                .mut_admin_response()
                .mut_splits()
                .take_regions();
            Ok(regions.into())
        })
    }

    type ReadIndexRes = impl Future<Output = kv::Result<u64>>;
    fn read_index(&self, _ctx: &Context) -> Self::ReadIndexRes {
        async move { unimplemented!() }
    }

    fn query_region(&self, region_id: u64) -> BoxFuture<'static, kv::Result<RegionMeta>> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let res = self.router.send_casual_msg(
            region_id,
            CasualMessage::AccessPeer(Box::new(move |meta| {
                if let Err(meta) = tx.send(meta) {
                    error!("receiver dropped, region meta: {:?}", meta)
                }
            })),
        );
        Box::pin(async move {
            res?;
            Ok(box_try!(rx.await))
        })
    }

    fn check_consistency(&self, region_id: u64) -> BoxFuture<'static, kv::Result<()>> {
        let region = self.query_region(region_id);
        let router = self.router.clone();
        Box::pin(async move {
            let meta: RegionMeta = region.await?;
            let leader_id = meta.raft_status.soft_state.leader_id;
            let mut leader = None;
            for peer in meta.region_state.peers {
                if peer.id == leader_id {
                    leader = Some(peer.into());
                }
            }
            if meta.raft_status.soft_state.raft_state != RaftStateRole::Leader {
                return Err(RaftServerError::NotLeader(region_id, leader).into());
            }
            let mut req = RaftCmdRequest::default();
            req.mut_header().set_region_id(region_id);
            req.mut_header().set_peer(leader.unwrap());
            req.mut_admin_request()
                .set_cmd_type(AdminCmdType::ComputeHash);
            let f = exec_admin(&router, req);
            f.await
        })
    }
}

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
    header
}

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    extension: RouterExtension<E, S>,
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
            extension: RouterExtension::new(router),
            engine,
            txn_extra_scheduler: None,
            region_leaders,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
    }

    fn exec_snapshot(
        &mut self,
        ctx: SnapContext<'_>,
        req: Request,
    ) -> impl Future<Output = Result<CmdRes<E::Snapshot>>> {
        let mut header = new_request_header(ctx.pb_ctx);
        let mut flags = 0;
        if ctx.pb_ctx.get_stale_read() && ctx.start_ts.map_or(true, |ts| !ts.is_zero()) {
            let mut data = [0u8; 8];
            (&mut data[..])
                .encode_u64(ctx.start_ts.unwrap_or_default().into_inner())
                .unwrap();
            flags |= WriteBatchFlags::STALE_READ.bits();
            header.set_flag_data(data.into());
        }
        if ctx.for_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let region_id = header.get_region_id();
        let peer_id = header.get_peer().get_id();

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        let (tx, rx) = futures::channel::oneshot::channel();
        let res = self.extension.router.read(
            ctx.read_id,
            cmd,
            StoreCallback::read(Box::new(move |resp| {
                let _ = tx.send(on_read_result(resp).map_err(Error::into));
            })),
        );
        async move {
            res?;
            if let Ok(r) = rx.await {
                return r;
            }
            if !tikv_util::thread_group::is_shutdown(false) {
                error!("channel is dropped without response, while not shutdown"; "region_id" => region_id, "peer_id" => peer_id);
            }
            Err(Error::Server(RaftServerError::DataIsNotReady {
                region_id,
                peer_id,
                safe_ts: 0,
            }))
        }
    }
}

pub fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
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
    let (tx, rx) = futures::channel::oneshot::channel();
    let res = router.send_command(
        req,
        raftstore::store::Callback::write(Box::new(move |resp| {
            let _ = tx.send(resp);
        })),
        RaftCmdExtraOpts {
            deadline: None,
            disk_full_opt: DiskFullOpt::AllowedOnAlmostFull,
        },
    );
    Box::pin(async move {
        res?;
        let mut resp = box_try!(rx.await);
        check_raft_cmd_response(&mut resp.response).map_err(kv::Error::from)
    })
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

    type RaftExtension = RouterExtension<E, S>;

    fn raft_extension(&self) -> &Self::RaftExtension {
        &self.extension
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

    type WriteSubscriber = RaftKvWriteSubscriber;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        subscribed_event: u8,
        on_return: Option<OnReturnCallback<()>>,
    ) -> Self::WriteSubscriber {
        fail_point!("raftkv_async_write");
        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let timer = Instant::now_coarse();

        let reqs: Vec<Request> = batch.modifies.into_iter().map(Into::into).collect();
        let txn_extra = batch.extra;
        let mut header = new_request_header(ctx);
        let mut flags = 0;
        if txn_extra.one_pc {
            flags |= WriteBatchFlags::ONE_PC.bits();
        }
        if txn_extra.for_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.schedule_txn_extra(txn_extra);

        let (mut tx, rx) = futures::channel::mpsc::channel(3);
        let mut res;
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
            res = raftkv_early_error_report_fp().map_err(KvError::from);
        }
        #[cfg(not(feature = "failpoints"))]
        {
            res = Ok(());
        }

        let proposed_cb: Option<ExtCallback> = if subscribed_event & SUBSCRIBE_PROPOSED != 0 {
            let mut tx0 = tx.clone();
            Some(Box::new(move || {
                let _ = tx0.try_send(WriteEvent::Proposed);
            }))
        } else {
            None
        };
        let committed_cb: Option<ExtCallback> = if subscribed_event & SUBSCRIBE_COMMITTED != 0 {
            let mut tx0 = tx.clone();
            Some(Box::new(move || {
                let _ = tx0.try_send(WriteEvent::Committed);
            }))
        } else {
            None
        };
        let cb = StoreCallback::write_ext(
            Box::new(move |resp| {
                let mut res = match on_write_result::<E::Snapshot>(resp).map_err(Error::into) {
                    Ok(CmdRes::Resp(_)) => Ok(()),
                    Ok(CmdRes::Snap(_)) => {
                        Err(box_err!("unexpect snapshot, should mutate instead."))
                    }
                    Err(e) => Err(e),
                };
                if let Some(cb) = on_return {
                    cb(&mut res);
                }
                match &res {
                    Ok(_) => {
                        ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                        ASYNC_REQUESTS_DURATIONS_VEC
                            .write
                            .observe(timer.saturating_elapsed_secs());
                        fail_point!("raftkv_async_write_finish");
                    }
                    Err(e) => {
                        let status_kind = get_status_kind_from_engine_error(e);
                        ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                    }
                }
                let _ = tx.try_send(WriteEvent::Applied(res));
            }),
            proposed_cb,
            committed_cb,
        );
        let extra_opts = RaftCmdExtraOpts {
            deadline: batch.deadline,
            disk_full_opt: batch.disk_full_opt,
        };
        if res.is_ok() && cmd.get_requests().is_empty() {
            res = Err(KvError::from(KvErrorInner::EmptyRequest))
        }
        if res.is_ok() {
            res = self
                .extension
                .router
                .send_command(cmd, cb, extra_opts)
                .map_err(KvError::from);
        }

        let last_event = match res {
            Ok(_) => None,
            Err(e) => Some(WriteEvent::Applied(Err(e))),
        };

        RaftKvWriteSubscriber {
            receiver: rx,
            last_event,
        }
    }

    type SnapshotRes = impl Future<Output = kv::Result<Self::Snap>>;
    fn async_snapshot(&mut self, mut ctx: SnapContext<'_>) -> Self::SnapshotRes {
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.key_ranges.is_empty() && ctx.start_ts.map_or(false, |ts| !ts.is_zero()) {
            req.mut_read_index()
                .set_start_ts(ctx.start_ts.as_ref().unwrap().into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        let f = self.exec_snapshot(ctx, req);
        async move {
            fail_point!("raftkv_async_snapshot_err", |_| Err(box_err!(
                "injected error for async_snapshot"
            )));

            match f.await {
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
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    Ok(s)
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    Err(e.into())
                }
            }
        }
    }

    fn release_snapshot(&mut self) {
        self.extension.router.release_snapshot_cache();
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

    fn hint_change_in_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) {
        self.extension
            .router
            .send_store_msg(StoreMsg::ClearRegionSizeInRange { start_key, end_key })
            .unwrap_or_else(|e| {
                // Warn and ignore it.
                warn!("unsafe destroy range: failed sending ClearRegionSizeInRange"; "err" => ?e);
            });
    }

    fn start_flashback(&self, ctx: &Context) -> BoxFuture<'static, kv::Result<()>> {
        let req = new_flashback_req(ctx, AdminCmdType::PrepareFlashback);
        exec_admin(&self.extension.router, req)
    }

    fn end_flashback(&self, ctx: &Context) -> BoxFuture<'static, kv::Result<()>> {
        let req = new_flashback_req(ctx, AdminCmdType::FinishFlashback);
        exec_admin(&self.extension.router, req)
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
