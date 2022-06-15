// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

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

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, MvccProperties, CF_LOCK, CF_WRITE};
use kvproto::{
    errorpb,
    kvrpcpb::{Context, IsolationLevel},
    metapb,
    raft_cmdpb::{
        CmdType, CustomRequest, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request,
        Response,
    },
};
use raft::{
    eraftpb::{self, MessageType},
    StateRole,
};
use raftstore::coprocessor::{
    dispatcher::BoxReadIndexObserver, Coprocessor, CoprocessorHost, ReadIndexObserver,
};
use rfstore::{
    store::{
        rlog, Callback as StoreCallback, CustomBuilder, ReadIndexContext, ReadResponse,
        RegionSnapshot, WriteResponse,
    },
    Error as RaftServerError, LocalReadRouter, RaftStoreRouter, ServerRaftStoreRouter,
};
use thiserror::Error;
use tikv::{
    server::metrics::*,
    storage::{
        self,
        kv::{
            self, write_modifies, Callback, Engine, Error as KvError, ErrorInner as KvErrorInner,
            ExtCallback, Modify, SnapContext, WriteData,
        },
    },
};
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::{
    Key, Lock, LockType, ReqType, TimeStamp, TxnExtraScheduler, WriteBatchFlags, WriteRef,
    WriteType,
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

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftKv {
    router: ServerRaftStoreRouter,
    engine: kvengine::Engine,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
}

pub enum CmdRes {
    Resp(Vec<Response>),
    Snap(RegionSnapshot),
}

fn check_raft_cmd_response(resp: &mut RaftCmdResponse) -> Result<()> {
    if resp.get_header().has_error() {
        return Err(Error::RequestFailed(resp.take_header().take_error()));
    }

    Ok(())
}

fn on_write_result(mut write_resp: WriteResponse) -> Result<CmdRes> {
    if let Err(e) = check_raft_cmd_response(&mut write_resp.response) {
        return Err(e);
    }
    let resps = write_resp.response.take_responses();
    Ok(CmdRes::Resp(resps.into()))
}

fn on_read_result(mut read_resp: ReadResponse) -> Result<CmdRes> {
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

impl RaftKv {
    /// Create a RaftKv using specified configuration.
    pub fn new(router: ServerRaftStoreRouter, engine: kvengine::Engine) -> RaftKv {
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
        mut batch: WriteData,
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
                        if rid == region_id { None } else { Some(()) }
                    })
                    .ok_or_else(|| RaftServerError::RegionNotFound(region_id, None).into())
                });
                Ok(())
            };
            raftkv_early_error_report_fp()?;
        }

        let req = modifies_to_requests(ctx, &mut batch);
        let txn_extra = batch.extra;
        let mut header = self.new_request_header(ctx);
        if txn_extra.one_pc {
            header.set_flags(WriteBatchFlags::ONE_PC.bits());
        }

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_custom_request(req);

        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }

        let cb = StoreCallback::write_ext(
            Box::new(move |resp| {
                write_cb(on_write_result(resp).map_err(Error::into));
            }),
            proposed_cb,
            committed_cb,
        );
        if let Some(deadline) = batch.deadline {
            self.router.send_command_with_deadline(cmd, cb, deadline);
        } else {
            self.router.send_command(cmd, cb);
        }
        Ok(())
    }
}

fn invalid_resp_type(exp: CmdType, act: CmdType) -> Error {
    Error::InvalidResponse(format!(
        "cmd type not match, want {:?}, got {:?}!",
        exp, act
    ))
}

impl Display for RaftKv {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

impl Debug for RaftKv {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftKv")
    }
}

#[allow(dead_code)]
impl Engine for RaftKv {
    type Snap = RegionSnapshot;
    type Local = kvengine::Engine;

    fn kv_engine(&self) -> kvengine::Engine {
        self.engine.clone()
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> kv::Result<Self::Snap> {
        let mut region = metapb::Region::default();
        region.set_start_key(start_key.to_owned());
        region.set_end_key(end_key.to_owned());
        // Use a fake peer to avoid panic.
        region.mut_peers().push(Default::default());
        Ok(RegionSnapshot::from_raw(&self.engine, &region))
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
                Modify::PessimisticLock(ref mut key, _) => {
                    let bytes = keys::data_key(key.as_encoded());
                    *key = Key::from_encoded(bytes);
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

    fn get_mvcc_properties_cf(
        &self,
        _cf: CfName,
        _safe_point: TimeStamp,
        _start: &[u8],
        _end: &[u8],
    ) -> Option<MvccProperties> {
        // TODO(x)
        None
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
    fn on_step(&self, msg: &mut eraftpb::Message, _role: StateRole) {
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

/**
Reconstruct transaction operations from separated modifications.

pessimistic_lock
    modify::put lock(pessimistic)

prewrite
    optional modify::put cf_default
    | modify::put cf_lock

1pc
    optional modify::put cf_default
    | modify::put cf_write:put
    | modify::put cf_write:del

commit
    modify:put cf_write + modify::del cf_lock

rollback
    modify:del cf_lock
    | modify::put cf_write::put overlapped + modify:del cf_lock
    | modify::put cf_write::rollback
    | modify::put cf_write::rollback + modify:del cf_lock
    | modify::put cf_lock(has rollback_ts) + modify:put cf_write:rollback
*/
pub fn modifies_to_requests(_ctx: &Context, data: &mut WriteData) -> CustomRequest {
    let builder = &mut rlog::CustomBuilder::new();
    let modifies = std::mem::take(&mut data.modifies);
    if data.extra.one_pc {
        build_one_pc(builder, modifies);
    } else {
        match data.extra.req_type {
            ReqType::PessimisticLock => build_pessimistic_lock(builder, modifies),
            ReqType::Prewrite => build_prewrite(builder, modifies),
            ReqType::Commit => build_commit(builder, modifies),
            ReqType::PessimisticRollback => build_pessimistic_rollback(builder, modifies),
            ReqType::CheckTxnStatus => build_check_txn_status(builder, modifies),
            ReqType::CheckSecondaryLocks | ReqType::Rollback => build_rollback(builder, modifies),
            ReqType::ResolveLock => build_resolve_lock(builder, modifies),
            ReqType::Heartbeat => build_heartbeat(builder, modifies),
            ReqType::Noop => unreachable!("modifies: {:?}", modifies),
        }
    }
    builder.build()
}

fn build_pessimistic_lock(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::TYPE_PESSIMISTIC_LOCK);
    for m in modifies {
        match m {
            Modify::PessimisticLock(key, lock) => {
                let raw_key = key.into_raw().unwrap();
                let val = lock.into_lock().to_bytes();
                builder.append_lock(&raw_key, &val);
            }
            Modify::Put(CF_LOCK, key, val) => {
                let raw_key = key.into_raw().unwrap();
                builder.append_lock(&raw_key, &val);
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_prewrite(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::TYPE_PREWRITE);
    for m in modifies {
        match m {
            Modify::Put(CF_LOCK, key, val) => {
                let raw_key = key.into_raw().unwrap();
                builder.append_lock(&raw_key, &val);
            }
            _ => unreachable!("unexpected modify {:?}", m),
        }
    }
}

fn build_commit(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::TYPE_COMMIT);
    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, _) => {
                let commit_ts = key.decode_ts().unwrap();
                let raw_key = key.into_raw().unwrap();
                builder.append_commit(&raw_key, commit_ts.into_inner());
                // value will be fetched during applying.
            }
            Modify::Delete(CF_LOCK, _) => {
                // lock will be deleted during applying.
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_one_pc(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::TYPE_ONE_PC);

    let deleted_lock = modifies
        .iter()
        .filter_map(|m| {
            if let Modify::Delete(CF_LOCK, k) = m {
                Some(k.clone())
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, val) => {
                let commit_ts = key.decode_ts().unwrap().into_inner();
                let key = key.truncate_ts().unwrap();
                let del_lock = deleted_lock.contains(&key);
                let raw_key = key.into_raw().unwrap();
                let write = WriteRef::parse(&val).unwrap();
                let mut value = vec![];
                if write.write_type == WriteType::Put {
                    value = write.short_value.unwrap().to_vec();
                }
                let is_extra = write.write_type == WriteType::Lock;
                let start_ts = write.start_ts.into_inner();
                builder.append_one_pc(&raw_key, &value, is_extra, del_lock, start_ts, commit_ts);
            }
            Modify::Delete(CF_LOCK, _) => {}
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_pessimistic_rollback(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::TYPE_PESSIMISTIC_ROLLBACK);
    for m in modifies {
        match m {
            Modify::Delete(CF_LOCK, key) => {
                let raw_key = key.into_raw().unwrap();
                builder.append_del_lock(&raw_key);
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_rollback(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    builder.set_type(rlog::TYPE_ROLLBACK);

    let deleted_lock = modifies
        .iter()
        .filter_map(|m| {
            if let Modify::Delete(CF_LOCK, k) = m {
                Some(k.clone())
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, _) => {
                let start_ts = key.decode_ts().unwrap().into_inner();
                let key = key.truncate_ts().unwrap();
                let del_lock = deleted_lock.contains(&key);
                let raw_key = key.into_raw().unwrap();
                builder.append_rollback(&raw_key, start_ts, del_lock);
            }
            Modify::Delete(CF_LOCK, _) => {
                // Lock deletion is carried on putting write.
            }
            Modify::Put(CF_LOCK, ..) => {
                // The EXTRA_CF ensures no overlapped rollback and commit record, so we ignore them.
            }
            Modify::Delete(CF_WRITE, _) => {
                // No need to collapse rollback write due to the EXTRA_CF.
            }
            _ => unreachable!("unexpected modify: {:?}", m),
        }
    }
}

fn build_check_txn_status(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    let tp = match &modifies[0] {
        Modify::Put(CF_LOCK, _, v) => match Lock::parse(v).unwrap().lock_type {
            LockType::Pessimistic => rlog::TYPE_PESSIMISTIC_LOCK,
            _ => rlog::TYPE_PREWRITE,
        },
        Modify::Delete(CF_LOCK, _) => {
            if modifies.len() == 1 {
                rlog::TYPE_PESSIMISTIC_ROLLBACK
            } else {
                rlog::TYPE_ROLLBACK
            }
        }
        _ => unreachable!("unexpected modifies: {:?}", modifies),
    };
    match tp {
        rlog::TYPE_PESSIMISTIC_LOCK => build_pessimistic_lock(builder, modifies),
        rlog::TYPE_PREWRITE => build_prewrite(builder, modifies),
        rlog::TYPE_PESSIMISTIC_ROLLBACK => build_pessimistic_rollback(builder, modifies),
        rlog::TYPE_ROLLBACK => build_rollback(builder, modifies),
        _ => unreachable!(),
    };
}

fn build_resolve_lock(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    // Group modifies by key.
    let mut grouped = HashMap::<Vec<_>, Vec<_>>::default();
    for m in modifies {
        let k = m.key().to_raw().unwrap();
        grouped.entry(k).or_default().push(m);
    }
    for (_, modifies) in grouped {
        if modifies.len() == 2 {
            if let Modify::Put(CF_WRITE, _, v) = &modifies[0] {
                let write = WriteRef::parse(v).unwrap();
                if write.write_type != WriteType::Rollback {
                    assert!(matches!(&modifies[1], Modify::Delete(CF_LOCK, _)));
                    builder.append_type(rlog::TYPE_COMMIT);
                    build_commit(builder, modifies);
                    continue;
                }
            }
            builder.append_type(rlog::TYPE_ROLLBACK);
            build_rollback(builder, modifies);
        }
    }
    builder.set_type(rlog::TYPE_RESOLVE_LOCK);
}

fn build_heartbeat(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    let tp = match &modifies[0] {
        Modify::Put(CF_LOCK, _, v) => match Lock::parse(v).unwrap().lock_type {
            LockType::Pessimistic => rlog::TYPE_PESSIMISTIC_LOCK,
            _ => rlog::TYPE_PREWRITE,
        },
        _ => unreachable!("unexpected modifies: {:?}", modifies),
    };
    match tp {
        rlog::TYPE_PESSIMISTIC_LOCK => build_pessimistic_lock(builder, modifies),
        rlog::TYPE_PREWRITE => build_prewrite(builder, modifies),
        _ => unreachable!(),
    };
}

#[cfg(test)]
mod tests {
    use std::{
        iter::FromIterator,
        sync::{mpsc, Mutex},
    };

    use tikv_kv::RocksEngine;
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

        checker.on_step(&mut m, raft::StateRole::Leader);
        assert_eq!(m.get_entries()[0].get_data(), uuid.as_bytes());
    }

    // It saves the write data of the last request for testing custom raft log.
    #[derive(Clone)]
    struct MockEngine {
        base: RocksEngine,
        last_write_data: Arc<Mutex<Option<WriteData>>>,
    }

    impl MockEngine {
        fn new(base: RocksEngine) -> Self {
            Self {
                base,
                last_write_data: Arc::default(),
            }
        }

        fn take_last_write_data(&self) -> Option<WriteData> {
            self.last_write_data.lock().unwrap().take()
        }

        fn set_write_data(&self, data: &WriteData) {
            let clone = WriteData::new(data.modifies.clone(), data.extra.clone());
            *self.last_write_data.lock().unwrap() = Some(clone);
        }
    }

    impl Engine for MockEngine {
        type Snap = <RocksEngine as Engine>::Snap;
        type Local = <RocksEngine as Engine>::Local;

        fn kv_engine(&self) -> Self::Local {
            self.base.kv_engine()
        }

        fn snapshot_on_kv_engine(
            &self,
            start_key: &[u8],
            end_key: &[u8],
        ) -> kv::Result<Self::Snap> {
            self.base.snapshot_on_kv_engine(start_key, end_key)
        }

        fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> kv::Result<()> {
            self.base.modify_on_kv_engine(modifies)
        }

        fn async_snapshot(&self, ctx: SnapContext<'_>, cb: Callback<Self::Snap>) -> kv::Result<()> {
            self.base.async_snapshot(ctx, cb)
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
            self.set_write_data(&batch);
            self.base
                .async_write_ext(ctx, batch, write_cb, proposed_cb, committed_cb)
        }
    }

    #[test]
    fn test_custom_raft_log() {
        use kvproto::kvrpcpb::AssertionLevel;
        use tikv::storage::{
            kv::TestEngineBuilder, lock_manager::DummyLockManager, test_util::expect_ok_callback,
            txn::commands, TestStorageBuilderApiV1,
        };
        use txn_types::Mutation;

        let engine = MockEngine::new(TestEngineBuilder::new().build().unwrap());
        let storage =
            TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine.clone(), DummyLockManager {})
                .build()
                .unwrap();
        let (tx, rx) = mpsc::channel();
        let get_modify_value = |modify: &_| match modify {
            Modify::Put(_, _, v) => v.clone(),
            Modify::PessimisticLock(_, lock) => lock.to_lock().to_bytes(),
            _ => unreachable!("unexpected modify: {:?}", modify),
        };

        // Prewrite
        storage
            .sched_txn_command(
                commands::Prewrite::with_context(
                    vec![
                        Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                        Mutation::make_put(Key::from_raw(b"k2"), b"v2".to_vec()),
                    ],
                    b"k1".to_vec(),
                    10.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        let modifies = data.modifies.clone();
        assert_eq!(data.extra.req_type, ReqType::Prewrite);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::TYPE_PREWRITE);
        let mut iter = modifies.into_iter();
        custom_log.iterate_lock(|k, v| {
            let modify = iter.next().unwrap();
            assert_eq!(k, modify.key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modify));
        });
        assert!(iter.next().is_none());

        // Commit
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(b"k1"), Key::from_raw(b"k2")],
                    10.into(),
                    20.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::Commit);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::TYPE_COMMIT);
        let mut keys = [b"k1", b"k2"].iter();
        custom_log.iterate_commit(|k, ts| {
            assert_eq!(k, keys.next().unwrap().as_slice());
            assert_eq!(ts, 20);
        });
        assert!(keys.next().is_none());

        // 1PC
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec())],
                    b"k1".to_vec(),
                    30.into(),
                    3000,
                    false,
                    0,
                    31.into(),
                    40.into(),
                    None,
                    true,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert!(data.extra.one_pc);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::TYPE_ONE_PC);
        let mut cnt = 0;
        custom_log.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
            assert_eq!(k, b"k1");
            assert_eq!(v, b"v1");
            assert!(!is_extra);
            assert!(!del_lock);
            assert_eq!(start_ts, 30);
            assert_eq!(commit_ts, 31);
            cnt += 1;
        });
        assert_eq!(cnt, 1);

        // PessimisticLock
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(Key::from_raw(b"k1"), false)],
                    b"k1".to_vec(),
                    40.into(),
                    3000,
                    true,
                    40.into(),
                    None,
                    false,
                    40.into(),
                    Default::default(),
                    false,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        let modifies = data.modifies.clone();
        assert_eq!(data.extra.req_type, ReqType::PessimisticLock);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::TYPE_PESSIMISTIC_LOCK);
        let mut cnt = 0;
        custom_log.iterate_lock(|k, v| {
            assert_eq!(k, &modifies[0].key().to_raw().unwrap());
            assert_eq!(v, &get_modify_value(&modifies[0]));
            cnt += 1;
        });
        assert_eq!(cnt, 1);

        // Pessimistic 1PC
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![
                        (
                            Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec()),
                            true,
                        ),
                        (Mutation::make_lock(Key::from_raw(b"k2")), false),
                    ],
                    b"k1".to_vec(),
                    40.into(),
                    3000,
                    40.into(),
                    0,
                    41.into(),
                    50.into(),
                    Some(vec![b"k2".to_vec()]),
                    true,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert!(data.extra.one_pc);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::TYPE_ONE_PC);
        let mut cnt = 0;
        custom_log.iterate_one_pc(|k, v, is_extra, del_lock, start_ts, commit_ts| {
            match k {
                b"k1" => {
                    assert_eq!(v, b"v1");
                    assert!(!is_extra);
                    assert!(del_lock);
                }
                b"k2" => {
                    assert!(v.is_empty());
                    assert!(is_extra);
                    assert!(!del_lock);
                }
                _ => unreachable!("unexpected key: {:?}", k),
            };
            assert_eq!(start_ts, 40);
            assert_eq!(commit_ts, 41);
            cnt += 1;
        });
        assert_eq!(cnt, 2);

        // ResolveLock
        storage
            .sched_txn_command(
                commands::Prewrite::with_context(
                    vec![Mutation::make_put(Key::from_raw(b"k1"), b"v1".to_vec())],
                    b"k1".to_vec(),
                    50.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::Prewrite::with_context(
                    vec![Mutation::make_put(Key::from_raw(b"k2"), b"v1".to_vec())],
                    b"k2".to_vec(),
                    60.into(),
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let txn_status = HashMap::from_iter(
            [(50.into(), 51.into()), (60.into(), 0.into())]
                .iter()
                .cloned(),
        );
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx, 1),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut data = engine.take_last_write_data().unwrap();
        assert_eq!(data.extra.req_type, ReqType::ResolveLock);
        let custom_req = modifies_to_requests(&Context::default(), &mut data);
        let custom_log = rlog::CustomRaftLog::new_from_data(custom_req.get_data());
        assert_eq!(custom_log.get_type(), rlog::TYPE_RESOLVE_LOCK);
        let mut cnt = 0;
        custom_log.iterate_resolve_lock(|tp, k, ts, del_lock| {
            match tp {
                rlog::TYPE_COMMIT => {
                    assert_eq!(k, b"k1");
                    assert_eq!(ts, 51);
                }
                rlog::TYPE_ROLLBACK => {
                    assert_eq!(k, b"k2");
                    assert_eq!(ts, 60);
                    assert!(del_lock);
                }
                _ => unreachable!("unexpected type: {:?}", tp),
            };
            cnt += 1
        });
        assert_eq!(cnt, 2);
    }
}
