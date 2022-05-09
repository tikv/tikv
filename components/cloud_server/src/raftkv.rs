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

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, MvccProperties, CF_DEFAULT, CF_LOCK, CF_WRITE};
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
        kv::{
            self, write_modifies, Callback, Engine, Error as KvError, ErrorInner as KvErrorInner,
            ExtCallback, Modify, SnapContext, WriteData,
        },
        {self},
    },
};
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::{
    Key, Lock, LockType, TimeStamp, TxnExtraScheduler, WriteBatchFlags, WriteRef, WriteType,
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
        batch: WriteData,
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

        let req = modifies_to_requests(ctx, batch.modifies);
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
        cf: CfName,
        safe_point: TimeStamp,
        start: &[u8],
        end: &[u8],
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
pub fn modifies_to_requests(ctx: &Context, modifies: Vec<Modify>) -> CustomRequest {
    let builder = &mut rlog::CustomBuilder::new();
    let custom_type = detect_custom_type(&modifies);
    builder.set_type(custom_type);
    match custom_type {
        rlog::TYPE_PREWRITE => build_prewrite(builder, modifies),
        rlog::TYPE_PESSIMISTIC_LOCK => build_pessimistic_lock(builder, modifies),
        rlog::TYPE_COMMIT => build_commit(builder, modifies),
        rlog::TYPE_ONE_PC => build_one_pc(builder, modifies),
        rlog::TYPE_ROLLBACK => build_rollback(builder, modifies),
        rlog::TYPE_PESSIMISTIC_ROLLBACK => build_pessimistic_rollback(builder, modifies),
        _ => unreachable!("modifies {:?}", modifies),
    }
    builder.build()
}

fn detect_custom_type(modifies: &Vec<Modify>) -> rlog::CustomRaftlogType {
    for (i, m) in modifies.iter().enumerate() {
        match m {
            Modify::Delete(cf, _) => {
                assert_eq!(*cf, CF_LOCK);
                return rlog::TYPE_PESSIMISTIC_ROLLBACK;
            }
            Modify::Put(cf, key, value) => {
                match *cf {
                    CF_DEFAULT => {
                        // undetermined
                    }
                    CF_WRITE => {
                        let write = WriteRef::parse(value).unwrap();
                        match write.write_type {
                            WriteType::Put | WriteType::Delete | WriteType::Lock => {
                                if write.has_overlapped_rollback {
                                    return rlog::TYPE_ROLLBACK;
                                }
                                if is_same_key_del_lock(modifies, i + 1, key) {
                                    return rlog::TYPE_COMMIT;
                                }
                                return rlog::TYPE_ONE_PC;
                            }
                            WriteType::Rollback => {
                                return rlog::TYPE_ROLLBACK;
                            }
                        }
                    }
                    CF_LOCK => {
                        let lock = Lock::parse(value).unwrap();
                        match lock.lock_type {
                            LockType::Put | LockType::Lock | LockType::Delete => {
                                return rlog::TYPE_PREWRITE;
                            }
                            LockType::Pessimistic => {
                                return rlog::TYPE_PESSIMISTIC_LOCK;
                            }
                        }
                    }
                    _ => unreachable!("unknown cf {:?}", modifies),
                }
            }
            Modify::PessimisticLock(..) => {
                return rlog::TYPE_PESSIMISTIC_LOCK;
            }
            Modify::DeleteRange(..) => unreachable!("delete range in modifies"),
        }
    }
    unreachable!("failed to detect custom type, modifies {:?}", modifies)
}

fn build_pessimistic_lock(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    for m in &modifies {
        match m {
            Modify::Put(cf, key, val) => {
                assert_eq!(*cf, CF_LOCK);
                let raw_key = key.to_raw().unwrap();
                builder.append_lock(&raw_key, val);
            }
            Modify::PessimisticLock(key, lock) => {
                let raw_key = key.to_raw().unwrap();
                let val = lock.to_lock().to_bytes();
                builder.append_lock(&raw_key, &val);
            }
            _ => unreachable!(),
        }
    }
}

fn build_prewrite(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    let mut values = HashMap::default();
    for m in modifies {
        match m {
            Modify::Put(cf, key, val) => match cf {
                CF_DEFAULT => {
                    let raw_key = key.to_raw().unwrap();
                    values.insert(raw_key, val);
                }
                CF_LOCK => {
                    let raw_key = key.to_raw().unwrap();
                    let mut lock = Lock::parse(&val).unwrap();
                    if lock.lock_type == LockType::Put && lock.short_value.is_none() {
                        lock.short_value = values.remove(&raw_key);
                    }
                    builder.append_lock(&raw_key, &lock.to_bytes());
                }
                _ => unreachable!("cf {:?}", cf),
            },
            _ => unreachable!("modify {:?}", m),
        }
    }
}

fn build_commit(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    for m in modifies {
        match m {
            Modify::Put(CF_WRITE, key, val) => {
                let commit_ts = key.decode_ts().unwrap();
                let raw_key = key.to_raw().unwrap();
                builder.append_commit(&raw_key, commit_ts.into_inner());
            }
            Modify::Delete(cf, key) => {
                assert_eq!(cf, CF_LOCK);
            }
            _ => unreachable!(),
        }
    }
}

fn build_one_pc(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    let mut values = HashMap::default();
    for (i, m) in modifies.iter().enumerate() {
        match m {
            Modify::Put(cf, key, val) => match *cf {
                CF_DEFAULT => {
                    let raw_key = key.to_raw().unwrap();
                    values.insert(raw_key, val.clone());
                }
                CF_WRITE => {
                    let commit_ts = key.decode_ts().unwrap().into_inner();
                    let raw_key = key.to_raw().unwrap();
                    let write = WriteRef::parse(val).unwrap();
                    let mut value = vec![];
                    if write.write_type == WriteType::Put {
                        if write.short_value.is_none() {
                            value = values.remove(&raw_key).unwrap();
                        } else {
                            value = write.short_value.unwrap().to_vec();
                        }
                    }
                    let is_extra = write.write_type == WriteType::Lock;
                    let start_ts = write.start_ts.into_inner();
                    let del_lock = is_same_key_del_lock(&modifies, i + 1, key);
                    builder
                        .append_one_pc(&raw_key, &value, is_extra, del_lock, start_ts, commit_ts);
                }
                _ => unreachable!("cf {:?}", cf),
            },
            Modify::Delete(cf, key) => {
                assert_eq!(*cf, CF_LOCK);
            }
            _ => unreachable!("{:?}", m),
        }
    }
}

fn build_pessimistic_rollback(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    for m in modifies {
        match m {
            Modify::Delete(cf, key) => {
                assert_eq!(cf, CF_LOCK);
                let raw_key = key.to_raw().unwrap();
                builder.append_del_lock(&raw_key);
            }
            _ => unreachable!(),
        }
    }
}

fn build_rollback(builder: &mut CustomBuilder, modifies: Vec<Modify>) {
    for (i, m) in modifies.iter().enumerate() {
        match m {
            Modify::Put(cf, key, val) => {
                assert_eq!(*cf, CF_WRITE);
                let start_ts = key.decode_ts().unwrap().into_inner();
                let raw_key = key.to_raw().unwrap();
                let delete_lock = is_same_key_del_lock(&modifies, i + 1, key);
                builder.append_rollback(&raw_key, start_ts, delete_lock);
            }
            Modify::Delete(cf, key) => {
                assert_eq!(*cf, CF_LOCK)
            }
            _ => unreachable!(),
        }
    }
}

fn is_same_key_del_lock(modifies: &[Modify], next_idx: usize, key: &Key) -> bool {
    if modifies.len() <= next_idx {
        return false;
    }
    let next_modify = &modifies[next_idx];
    match next_modify {
        Modify::Delete(cf, next_key) => {
            if *cf != CF_LOCK {
                return false;
            }
            let next_encoded = next_key.as_encoded();
            let key_encoded_with_ts = key.as_encoded();
            let key_encoded = &key_encoded_with_ts[..key_encoded_with_ts.len() - 8];
            next_encoded == key_encoded
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
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
}
