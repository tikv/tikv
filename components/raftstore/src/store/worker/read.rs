// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    fmt::{self, Display, Formatter},
    ops::Deref,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crossbeam::{atomic::AtomicCell, channel::TrySendError};
use engine_traits::{KvEngine, RaftEngine, Snapshot};
use fail::fail_point;
use kvproto::{
    errorpb,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, ReadIndexResponse, Request, Response},
};
use pd_client::BucketMeta;
use tikv_util::{
    codec::number::decode_u64,
    debug, error,
    lru::LruCache,
    time::{monotonic_raw_now, ThreadReadId},
};
use time::Timespec;

use super::metrics::*;
use crate::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp,
        fsm::store::StoreMeta,
        msg::ReadResponseTrait,
        util::{self, LeaseState, RegionReadProgress, RemoteLease},
        Callback, CasualMessage, CasualRouter, Peer, ProposalRouter, RaftCommand, ReadCallback,
        ReadResponse, RegionSnapshot, RequestInspector, RequestPolicy, TxnExt,
    },
    Error, Result,
};

/// #[RaftstoreCommon]
pub trait ReadExecutor<E: KvEngine> {
    type Response: ReadResponseTrait<E::Snapshot>;

    fn get_tablet(&mut self) -> &E;

    fn get_snapshot(
        &mut self,
        ts: Option<ThreadReadId>,
        read_context: &mut Option<LocalReadContext<'_, E>>,
    ) -> Arc<E::Snapshot>;

    fn get_value(&mut self, req: &Request, region: &metapb::Region) -> Result<Response> {
        let key = req.get_get().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, region)?;

        let engine = self.get_tablet();
        let mut resp = Response::default();
        let res = if !req.get_get().get_cf().is_empty() {
            let cf = req.get_get().get_cf();
            engine
                .get_value_cf(cf, &keys::data_key(key))
                .unwrap_or_else(|e| {
                    panic!(
                        "[region {}] failed to get {} with cf {}: {:?}",
                        region.get_id(),
                        log_wrappers::Value::key(key),
                        cf,
                        e
                    )
                })
        } else {
            engine.get_value(&keys::data_key(key)).unwrap_or_else(|e| {
                panic!(
                    "[region {}] failed to get {}: {:?}",
                    region.get_id(),
                    log_wrappers::Value::key(key),
                    e
                )
            })
        };
        if let Some(res) = res {
            resp.mut_get().set_value(res.to_vec());
        }

        Ok(resp)
    }

    fn execute(
        &mut self,
        msg: &RaftCmdRequest,
        region: &Arc<metapb::Region>,
        read_index: Option<u64>,
        mut ts: Option<ThreadReadId>,
        mut read_context: Option<LocalReadContext<'_, E>>,
    ) -> Self::Response {
        let requests = msg.get_requests();
        let mut response = Self::Response::default();
        let mut responses = Vec::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => match self.get_value(req, region.as_ref()) {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(?e;
                            "failed to execute get command";
                            "region_id" => region.get_id(),
                        );
                        response.set_error(cmd_resp::new_error(e));
                        return response;
                    }
                },
                CmdType::Snap => {
                    let snapshot = RegionSnapshot::from_snapshot(
                        self.get_snapshot(ts.take(), &mut read_context),
                        region.clone(),
                    );
                    response.set_snapshot(snapshot);
                    Response::default()
                }
                CmdType::ReadIndex => {
                    let mut resp = Response::default();
                    if let Some(read_index) = read_index {
                        let mut res = ReadIndexResponse::default();
                        res.set_read_index(read_index);
                        resp.set_read_index(res);
                    } else {
                        panic!("[region {}] can not get readindex", region.get_id());
                    }
                    resp
                }
                CmdType::Prewrite
                | CmdType::Put
                | CmdType::Delete
                | CmdType::DeleteRange
                | CmdType::IngestSst
                | CmdType::Invalid => unreachable!(),
            };
            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }
        response.set_responses(responses.into());
        response
    }
}

/// #[RaftstoreCommon]: A read only delegate of `Peer`.
#[derive(Clone, Debug)]
pub struct ReadDelegate {
    pub region: Arc<metapb::Region>,
    pub peer_id: u64,
    pub term: u64,
    pub applied_term: u64,
    pub leader_lease: Option<RemoteLease>,
    pub last_valid_ts: Timespec,

    pub tag: String,
    pub bucket_meta: Option<Arc<BucketMeta>>,
    pub txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    pub txn_ext: Arc<TxnExt>,
    pub read_progress: Arc<RegionReadProgress>,
    pub pending_remove: bool,

    // `track_ver` used to keep the local `ReadDelegate` in `LocalReader`
    // up-to-date with the global `ReadDelegate` stored at `StoreMeta`
    pub track_ver: TrackVer,
}

/// CachedReadDelegate is a wrapper the ReadDelegate and kv_engine. LocalReader
/// dispatch local read requests to ReadDeleage according to the region_id where
/// ReadDelegate needs kv_engine to read data or fetch snapshot.
pub struct CachedReadDelegate<E>
where
    E: KvEngine,
{
    delegate: Arc<ReadDelegate>,
    kv_engine: E,
}

impl<E> Deref for CachedReadDelegate<E>
where
    E: KvEngine,
{
    type Target = ReadDelegate;

    fn deref(&self) -> &Self::Target {
        self.delegate.as_ref()
    }
}

impl<E> Clone for CachedReadDelegate<E>
where
    E: KvEngine,
{
    fn clone(&self) -> Self {
        CachedReadDelegate {
            delegate: Arc::clone(&self.delegate),
            kv_engine: self.kv_engine.clone(),
        }
    }
}

/// #[RaftstoreCommon]: LocalReadContext combines some LocalReader's fields for temporary usage.
pub struct LocalReadContext<'a, E>
where
    E: KvEngine,
{
    read_id: &'a mut ThreadReadId,
    snap_cache: &'a mut Box<Option<Arc<E::Snapshot>>>,
}

impl Drop for ReadDelegate {
    fn drop(&mut self) {
        // call `inc` to notify the source `ReadDelegate` is dropped
        self.track_ver.inc();
    }
}

/// #[RaftstoreCommon]
pub trait ReadExecutorProvider<E>: Send + Clone + 'static
where
    E: KvEngine,
{
    type Executor: ReadExecutor<E>;

    fn store_id(&self) -> Option<u64>;

    /// get the ReadDelegate with region_id and the number of delegates in the
    /// StoreMeta
    fn get_executor_and_len(&self, region_id: u64) -> (usize, Option<Self::Executor>);
}

#[derive(Clone)]
pub struct StoreMetaDelegate<E>
where
    E: KvEngine,
{
    store_meta: Arc<Mutex<StoreMeta>>,
    kv_engine: E,
}

impl<E> StoreMetaDelegate<E>
where
    E: KvEngine,
{
    pub fn new(store_meta: Arc<Mutex<StoreMeta>>, kv_engine: E) -> Self {
        StoreMetaDelegate {
            store_meta,
            kv_engine,
        }
    }
}

impl<E> ReadExecutorProvider<E> for StoreMetaDelegate<E>
where
    E: KvEngine,
{
    type Executor = CachedReadDelegate<E>;

    fn store_id(&self) -> Option<u64> {
        self.store_meta.as_ref().lock().unwrap().store_id
    }

    /// get the ReadDelegate with region_id and the number of delegates in the
    /// StoreMeta
    fn get_executor_and_len(&self, region_id: u64) -> (usize, Option<Self::Executor>) {
        let meta = self.store_meta.as_ref().lock().unwrap();
        let reader = meta.readers.get(&region_id).cloned();
        if let Some(reader) = reader {
            return (
                meta.readers.len(),
                Some(CachedReadDelegate {
                    delegate: Arc::new(reader),
                    kv_engine: self.kv_engine.clone(),
                }),
            );
        }
        (meta.readers.len(), None)
    }
}

/// #[RaftstoreCommon]
#[derive(Debug)]
pub struct TrackVer {
    version: Arc<AtomicU64>,
    local_ver: u64,
    // source set to `true` means the `TrackVer` is created by `TrackVer::new` instead
    // of `TrackVer::clone`, more specific, only the `ReadDelegate` created by `ReadDelegate::new`
    // will have source `TrackVer` and be able to increase `TrackVer::version`, because these
    // `ReadDelegate` are store at `StoreMeta` and only them will invoke `ReadDelegate::update`
    source: bool,
}

impl TrackVer {
    pub fn new() -> TrackVer {
        TrackVer {
            version: Arc::new(AtomicU64::from(0)),
            local_ver: 0,
            source: true,
        }
    }

    // Take `&mut self` to prevent calling `inc` and `clone` at the same time
    pub fn inc(&mut self) {
        // Only the source `TrackVer` can increase version
        if self.source {
            self.version.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn any_new(&self) -> bool {
        self.version.load(Ordering::Relaxed) > self.local_ver
    }
}

impl Default for TrackVer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TrackVer {
    fn clone(&self) -> Self {
        TrackVer {
            version: Arc::clone(&self.version),
            local_ver: self.version.load(Ordering::Relaxed),
            source: false,
        }
    }
}

impl ReadDelegate {
    pub fn from_peer<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> ReadDelegate {
        let region = peer.region().clone();
        let region_id = region.get_id();
        let peer_id = peer.peer.get_id();
        ReadDelegate {
            region: Arc::new(region),
            peer_id,
            term: peer.term(),
            applied_term: peer.get_store().applied_term(),
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, peer_id),
            txn_extra_op: peer.txn_extra_op.clone(),
            txn_ext: peer.txn_ext.clone(),
            read_progress: peer.read_progress.clone(),
            pending_remove: false,
            bucket_meta: peer.region_buckets.as_ref().map(|b| b.meta.clone()),
            track_ver: TrackVer::new(),
        }
    }

    pub fn fresh_valid_ts(&mut self) {
        self.last_valid_ts = monotonic_raw_now();
    }

    pub fn mark_pending_remove(&mut self) {
        self.pending_remove = true;
        self.track_ver.inc();
    }

    pub fn update(&mut self, progress: Progress) {
        self.fresh_valid_ts();
        self.track_ver.inc();
        match progress {
            Progress::Region(region) => {
                self.region = Arc::new(region);
            }
            Progress::Term(term) => {
                self.term = term;
            }
            Progress::AppliedTerm(applied_term) => {
                self.applied_term = applied_term;
            }
            Progress::LeaderLease(leader_lease) => {
                self.leader_lease = Some(leader_lease);
            }
            Progress::RegionBuckets(bucket_meta) => {
                self.bucket_meta = Some(bucket_meta);
            }
        }
    }

    // If the remote lease will be expired in near future send message
    // to `raftstore` renew it
    pub fn maybe_renew_lease_advance<EK: KvEngine>(
        &self,
        router: &dyn CasualRouter<EK>,
        ts: Timespec,
    ) {
        if !self
            .leader_lease
            .as_ref()
            .map(|lease| lease.need_renew(ts))
            .unwrap_or(false)
        {
            return;
        }
        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().renew_lease_advance.inc());
        let region_id = self.region.get_id();
        if let Err(e) = router.send(region_id, CasualMessage::RenewLease) {
            debug!(
                "failed to send renew lease message";
                "region" => region_id,
                "error" => ?e
            )
        }
    }

    pub fn is_in_leader_lease(&self, ts: Timespec) -> bool {
        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                if lease.inspect(Some(ts)) == LeaseState::Valid {
                    return true;
                } else {
                    TLS_LOCAL_READ_METRICS
                        .with(|m| m.borrow_mut().reject_reason.lease_expire.inc());
                    debug!("rejected by lease expire"; "tag" => &self.tag);
                }
            } else {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.term_mismatch.inc());
                debug!("rejected by term mismatch"; "tag" => &self.tag);
            }
        }

        false
    }

    pub fn check_stale_read_safe<EK: KvEngine, R: ReadResponseTrait<EK::Snapshot>>(
        &self,
        read_ts: u64,
    ) -> std::result::Result<(), R> {
        let safe_ts = self.read_progress.safe_ts();
        if safe_ts >= read_ts {
            return Ok(());
        }
        debug!(
            "reject stale read by safe ts";
            "tag" => &self.tag,
            "safe ts" => safe_ts,
            "read ts" => read_ts
        );
        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.safe_ts.inc());
        let mut response = cmd_resp::new_error(Error::DataIsNotReady {
            region_id: self.region.get_id(),
            peer_id: self.peer_id,
            safe_ts,
        });
        cmd_resp::bind_term(&mut response, self.term);
        let mut read_response = R::default();
        read_response.set_error(response);
        Err(read_response)
    }

    /// Used in some external tests.
    pub fn mock(region_id: u64) -> Self {
        let mut region: metapb::Region = Default::default();
        region.set_id(region_id);
        let read_progress = Arc::new(RegionReadProgress::new(&region, 0, 0, "mock".to_owned()));
        ReadDelegate {
            region: Arc::new(region),
            peer_id: 1,
            term: 1,
            applied_term: 1,
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, 1),
            txn_extra_op: Default::default(),
            txn_ext: Default::default(),
            read_progress,
            pending_remove: false,
            track_ver: TrackVer::new(),
            bucket_meta: None,
        }
    }
}

impl Display for ReadDelegate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadDelegate for region {}, \
             leader {} at term {}, applied_term {}, has lease {}",
            self.region.get_id(),
            self.peer_id,
            self.term,
            self.applied_term,
            self.leader_lease.is_some(),
        )
    }
}

/// #[RaftstoreCommon]
#[derive(Debug)]
pub enum Progress {
    Region(metapb::Region),
    Term(u64),
    AppliedTerm(u64),
    LeaderLease(RemoteLease),
    RegionBuckets(Arc<BucketMeta>),
}

impl Progress {
    pub fn region(region: metapb::Region) -> Progress {
        Progress::Region(region)
    }

    pub fn term(term: u64) -> Progress {
        Progress::Term(term)
    }

    pub fn applied_term(applied_term: u64) -> Progress {
        Progress::AppliedTerm(applied_term)
    }

    pub fn leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(lease)
    }

    pub fn region_buckets(bucket_meta: Arc<BucketMeta>) -> Progress {
        Progress::RegionBuckets(bucket_meta)
    }
}

// , Response = <Self::CB as ReadCallback<E::Snapshot>>::Response
trait LocalReaderTrait<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E> + Clone,
    E: KvEngine,
    D: ReadExecutor<E> + Deref<Target = ReadDelegate> + Clone,
    S: ReadExecutorProvider<E, Executor = D>,
{
    type CB: ReadCallback<E::Snapshot>;

    fn router(&self) -> &C {
        unimplemented!()
    }

    fn local_read_context(&mut self) -> LocalReadContext<'_, E> {
        unimplemented!()
    }

    fn delegate_lru_and_store_meta(&mut self) -> (&mut LruCache<u64, D>, &S) {
        unimplemented!()
    }

    fn store_meta(&self) -> &S {
        unimplemented!()
    }

    fn store_id(&self) -> &Cell<Option<u64>> {
        unimplemented!()
    }

    // Ideally `get_delegate` should return `Option<&ReadDelegate>`, but if so the
    // lifetime of the returned `&ReadDelegate` will bind to `self`, and make it
    // impossible to use `&mut self` while the `&ReadDelegate` is alive, a better
    // choice is use `Rc` but `LocalReader: Send` will be violated, which is
    // required by `LocalReadRouter: Send`, use `Arc` will introduce extra cost but
    // make the logic clear
    fn get_delegate(&mut self, region_id: u64) -> Option<D> {
        let (delegates, store_meta) = self.delegate_lru_and_store_meta();
        let rd = match delegates.get(&region_id) {
            // The local `ReadDelegate` is up to date
            Some(d) if !d.track_ver.any_new() => Some(d.clone()),
            _ => {
                debug!("update local read delegate"; "region_id" => region_id);
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.cache_miss.inc());

                let (meta_len, meta_reader) = { store_meta.get_executor_and_len(region_id) };

                // Remove the stale delegate
                delegates.remove(&region_id);
                delegates.resize(meta_len);
                match meta_reader {
                    Some(reader) => {
                        delegates.insert(region_id, reader.clone());
                        Some(reader)
                    }
                    None => None,
                }
            }
        };
        // Return `None` if the read delegate is pending remove
        rd.filter(|r| !r.pending_remove)
    }

    fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> Result<Option<(D, RequestPolicy)>> {
        // Check store id.
        if self.store_id().get().is_none() {
            let store_id = self.store_meta().store_id();
            self.store_id().set(store_id);
        }
        let store_id = self.store_id().get().unwrap();

        if let Err(e) = util::check_store_id(req, store_id) {
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.store_id_mismatch.inc());
            debug!("rejected by store id not match"; "err" => %e);
            return Err(e);
        }

        // Check region id.
        let region_id = req.get_header().get_region_id();
        let delegate = match self.get_delegate(region_id) {
            Some(d) => d,
            None => {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_region.inc());
                debug!("rejected by no region"; "region_id" => region_id);
                return Ok(None);
            }
        };

        fail_point!("localreader_on_find_delegate");

        // Check peer id.
        if let Err(e) = util::check_peer_id(req, delegate.peer_id) {
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.peer_id_mismatch.inc());
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, delegate.term) {
            debug!(
                "check term";
                "delegate_term" => delegate.term,
                "header_term" => req.get_header().get_term(),
            );
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.term_mismatch.inc());
            return Err(e);
        }

        // Check region epoch.
        if util::check_region_epoch(req, &delegate.region, false).is_err() {
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.epoch.inc());
            // Stale epoch, redirect it to raftstore to get the latest region.
            debug!("rejected by epoch not match"; "tag" => &delegate.tag);
            return Ok(None);
        }

        let mut inspector = Inspector {
            delegate: &delegate,
        };
        match inspector.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(Some((delegate, RequestPolicy::ReadLocal))),
            Ok(RequestPolicy::StaleRead) => Ok(Some((delegate, RequestPolicy::StaleRead))),
            // It can not handle other policies.
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn read_local(
        &mut self,
        mut read_id: Option<ThreadReadId>,
        req: &RaftCmdRequest,
        delegate: &mut D,
    ) -> Option<<D as ReadExecutor<E>>::Response> {
        let snapshot_ts = match read_id.as_mut() {
            // If this peer became Leader not long ago and just after the cached
            // snapshot was created, this snapshot can not see all data of the peer.
            Some(id) => {
                if id.create_time <= delegate.last_valid_ts {
                    id.create_time = monotonic_raw_now();
                }
                id.create_time
            }
            None => monotonic_raw_now(),
        };
        if !delegate.is_in_leader_lease(snapshot_ts) {
            // Forward to raftstore.
            // self.redirect(RaftCommand::new(req, cb));
            return None;
        }

        let delegate_ext = self.local_read_context();

        let region = Arc::clone(&delegate.region);
        let response = delegate.execute(req, &region, None, read_id, Some(delegate_ext)); // Try renew lease in advance
        delegate.maybe_renew_lease_advance(self.router(), snapshot_ts);
        Some(response)
    }

    fn stale_read(
        &mut self,
        mut read_id: Option<ThreadReadId>,
        req: &RaftCmdRequest,
        delegate: &mut D,
    ) -> std::result::Result<
        <D as ReadExecutor<E>>::Response,
        <Self::CB as ReadCallback<E::Snapshot>>::Response,
    > {
        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
        assert!(read_ts > 0);
        delegate.check_stale_read_safe::<E, <Self::CB as ReadCallback<E::Snapshot>>::Response>(
            read_ts,
        )?;

        let delegate_ext = self.local_read_context();

        let region = Arc::clone(&delegate.region);
        // Getting the snapshot
        let response = delegate.execute(req, &region, None, read_id, Some(delegate_ext));

        // Double check in case `safe_ts` change after the first check and before
        // getting snapshot
        delegate.check_stale_read_safe::<E, <Self::CB as ReadCallback<E::Snapshot>>::Response>(
            read_ts,
        )?;

        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().local_executed_stale_read_requests.inc());
        Ok(response)
    }

    fn propose_raft_command(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Self::CB,
    ) {
        unimplemented!()
    }

    /// If read requests are received at the same RPC request, we can create one
    /// snapshot for all of them and check whether the time when the snapshot
    /// was created is in lease. We use ThreadReadId to figure out whether this
    /// RaftCommand comes from the same RPC request with the last RaftCommand
    /// which left a snapshot cached in LocalReader. ThreadReadId is composed by
    /// thread_id and a thread_local incremental sequence.
    #[inline]
    fn read(&mut self, read_id: Option<ThreadReadId>, req: RaftCmdRequest, cb: Self::CB) {
        unimplemented!()
    }

    fn release_snapshot_cache(&mut self) {
        unimplemented!()
    }
}

impl<C, E, D, S> LocalReaderTrait<C, E, D, S> for LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E> + Clone,
    E: KvEngine,
    D: ReadExecutor<E, Response = ReadResponse<E::Snapshot>> + Deref<Target = ReadDelegate> + Clone,
    S: ReadExecutorProvider<E, Executor = D>,
{
    type CB = Callback<E::Snapshot>;

    fn delegate_lru_and_store_meta(&mut self) -> (&mut LruCache<u64, D>, &S) {
        (&mut self.delegates, &self.store_meta)
    }

    fn propose_raft_command(
        &mut self,
        mut read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Self::CB,
    ) {
        match self.pre_propose_raft_command(&req) {
            Ok(Some((mut delegate, policy))) => {
                let delegate_ext: LocalReadContext<'_, E>;
                let mut response = match policy {
                    // Leader can read local if and only if it is in lease.
                    RequestPolicy::ReadLocal => {
                        let response = self.read_local(read_id, &req, &mut delegate);
                        // todo(SpadeA): deal with None
                        response.unwrap()
                    }
                    // Replica can serve stale read if and only if its `safe_ts` >= `read_ts`
                    RequestPolicy::StaleRead => match self.stale_read(read_id, &req, &mut delegate)
                    {
                        Ok(response) => response,
                        Err(response) => {
                            cb.set_result(response);
                            return;
                        }
                    },
                    _ => unreachable!(),
                };
                response.set_term(delegate.term);
                if let Some(snap) = response.mut_snapshot() {
                    snap.txn_ext = Some(delegate.txn_ext.clone());
                    snap.bucket_meta = delegate.bucket_meta.clone();
                }
                response.set_txn_extra_op(delegate.txn_extra_op.load());
                cb.set_result(response);
            }
            // Forward to raftstore.
            Ok(None) => self.redirect(RaftCommand::new(req, cb)),
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self.delegates.get(&req.get_header().get_region_id()) {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                cb.set_result(ReadResponse {
                    response,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
            }
        }
    }

    fn read(&mut self, read_id: Option<ThreadReadId>, req: RaftCmdRequest, cb: Self::CB) {
        self.propose_raft_command(read_id, req, cb);
        maybe_tls_local_read_metrics_flush();
    }

    fn release_snapshot_cache(&mut self) {
        self.snap_cache.as_mut().take();
    }
}

/// #[RaftstoreCommon]: LocalReader is an entry point where local read requests are dipatch to the
/// relevant regions by LocalReader so that these requests can be handled by the
/// relevant ReadDelegate respectively.
pub struct LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
    E: KvEngine,
    D: ReadExecutor<E> + Deref<Target = ReadDelegate>,
    S: ReadExecutorProvider<E, Executor = D>,
{
    pub store_id: Cell<Option<u64>>,
    store_meta: S,
    kv_engine: E,
    // region id -> ReadDelegate
    // The use of `Arc` here is a workaround, see the comment at `get_delegate`
    pub delegates: LruCache<u64, D>,
    snap_cache: Box<Option<Arc<E::Snapshot>>>,
    cache_read_id: ThreadReadId,
    // A channel to raftstore.
    router: C,
}

impl<C, E, D, S> LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
    E: KvEngine,
    D: ReadExecutor<E> + Deref<Target = ReadDelegate> + Clone,
    S: ReadExecutorProvider<E, Executor = D>,
{
    pub fn new(kv_engine: E, store_meta: S, router: C) -> Self {
        let cache_read_id = ThreadReadId::new();
        LocalReader {
            store_meta,
            kv_engine,
            router,
            snap_cache: Box::new(None),
            cache_read_id,
            store_id: Cell::new(None),
            delegates: LruCache::with_capacity_and_sample(0, 7),
        }
    }

    fn redirect(&mut self, mut cmd: RaftCommand<E::Snapshot>) {
        debug!("localreader redirects command"; "command" => ?cmd);
        let region_id = cmd.request.get_header().get_region_id();
        let mut err = errorpb::Error::default();
        match ProposalRouter::send(&self.router, cmd) {
            Ok(()) => return,
            Err(TrySendError::Full(c)) => {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.channel_full.inc());
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd = c;
            }
            Err(TrySendError::Disconnected(c)) => {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_region.inc());
                err.set_message(format!("region {} is missing", region_id));
                err.mut_region_not_found().set_region_id(region_id);
                cmd = c;
            }
        }

        let mut resp = RaftCmdResponse::default();
        resp.mut_header().set_error(err);
        let read_resp = ReadResponse {
            response: resp,
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };

        cmd.callback.invoke_read(read_resp);
    }

    pub fn read(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        unimplemented!()
    }

    pub fn release_snapshot_cache(&mut self) {
        self.snap_cache.as_mut().take();
    }
}

impl<E> ReadExecutor<E> for CachedReadDelegate<E>
where
    E: KvEngine,
{
    type Response = ReadResponse<E::Snapshot>;

    fn get_tablet(&mut self) -> &E {
        &self.kv_engine
    }

    fn get_snapshot(
        &mut self,
        create_time: Option<ThreadReadId>,
        read_context: &mut Option<LocalReadContext<'_, E>>,
    ) -> Arc<E::Snapshot> {
        let ctx = read_context.as_mut().unwrap();
        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().local_executed_requests.inc());
        if let Some(ts) = create_time {
            if ts == *ctx.read_id {
                if let Some(snap) = ctx.snap_cache.as_ref().as_ref() {
                    TLS_LOCAL_READ_METRICS
                        .with(|m| m.borrow_mut().local_executed_snapshot_cache_hit.inc());
                    return snap.clone();
                }
            }
            let snap = Arc::new(self.kv_engine.snapshot());
            *ctx.read_id = ts;
            *ctx.snap_cache = Box::new(Some(snap.clone()));
            return snap;
        }
        Arc::new(self.kv_engine.snapshot())
    }
}

impl<C, E, D, S> Clone for LocalReader<C, E, D, S>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E> + Clone,
    E: KvEngine,
    D: ReadExecutor<E> + Deref<Target = ReadDelegate>,
    S: ReadExecutorProvider<E, Executor = D>,
{
    fn clone(&self) -> Self {
        LocalReader {
            store_meta: self.store_meta.clone(),
            kv_engine: self.kv_engine.clone(),
            router: self.router.clone(),
            store_id: self.store_id.clone(),
            delegates: LruCache::with_capacity_and_sample(0, 7),
            snap_cache: self.snap_cache.clone(),
            cache_read_id: self.cache_read_id.clone(),
        }
    }
}

/// #[RaftstoreCommon]
struct Inspector<'r> {
    delegate: &'r ReadDelegate,
}

impl<'r> RequestInspector for Inspector<'r> {
    fn has_applied_to_current_term(&mut self) -> bool {
        if self.delegate.applied_term == self.delegate.term {
            true
        } else {
            debug!(
                "rejected by term check";
                "tag" => &self.delegate.tag,
                "applied_term" => self.delegate.applied_term,
                "delegate_term" => ?self.delegate.term,
            );

            // only for metric.
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.applied_term.inc());
            false
        }
    }

    fn inspect_lease(&mut self) -> LeaseState {
        // TODO: disable localreader if we did not enable raft's check_quorum.
        if self.delegate.leader_lease.is_some() {
            // We skip lease check, because it is postponed until `handle_read`.
            LeaseState::Valid
        } else {
            debug!("rejected by leader lease"; "tag" => &self.delegate.tag);
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_lease.inc());
            LeaseState::Expired
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc::*, thread};

    use crossbeam::channel::TrySendError;
    use engine_test::kv::{KvTestEngine, KvTestSnapshot};
    use engine_traits::{Peekable, SyncMutable, ALL_CFS};
    use kvproto::raft_cmdpb::*;
    use tempfile::{Builder, TempDir};
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};
    use time::Duration;
    use txn_types::WriteBatchFlags;

    use super::*;
    use crate::store::{util::Lease, Callback};

    struct MockRouter {
        p_router: SyncSender<RaftCommand<KvTestSnapshot>>,
        c_router: SyncSender<(u64, CasualMessage<KvTestEngine>)>,
    }

    impl MockRouter {
        #[allow(clippy::type_complexity)]
        fn new() -> (
            MockRouter,
            Receiver<RaftCommand<KvTestSnapshot>>,
            Receiver<(u64, CasualMessage<KvTestEngine>)>,
        ) {
            let (p_ch, p_rx) = sync_channel(1);
            let (c_ch, c_rx) = sync_channel(1);
            (
                MockRouter {
                    p_router: p_ch,
                    c_router: c_ch,
                },
                p_rx,
                c_rx,
            )
        }
    }

    impl ProposalRouter<KvTestSnapshot> for MockRouter {
        fn send(
            &self,
            cmd: RaftCommand<KvTestSnapshot>,
        ) -> std::result::Result<(), TrySendError<RaftCommand<KvTestSnapshot>>> {
            ProposalRouter::send(&self.p_router, cmd)
        }
    }

    impl CasualRouter<KvTestEngine> for MockRouter {
        fn send(&self, region_id: u64, msg: CasualMessage<KvTestEngine>) -> Result<()> {
            CasualRouter::send(&self.c_router, region_id, msg)
        }
    }

    #[allow(clippy::type_complexity)]
    fn new_reader(
        path: &str,
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta>>,
    ) -> (
        TempDir,
        LocalReader<
            MockRouter,
            KvTestEngine,
            CachedReadDelegate<KvTestEngine>,
            StoreMetaDelegate<KvTestEngine>,
        >,
        Receiver<RaftCommand<KvTestSnapshot>>,
    ) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let db = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
        let (ch, rx, _) = MockRouter::new();
        let mut reader = LocalReader::new(db.clone(), StoreMetaDelegate::new(store_meta, db), ch);
        reader.store_id = Cell::new(Some(store_id));
        (path, reader, rx)
    }

    fn new_peers(store_id: u64, pr_ids: Vec<u64>) -> Vec<metapb::Peer> {
        pr_ids
            .into_iter()
            .map(|id| {
                let mut pr = metapb::Peer::default();
                pr.set_store_id(store_id);
                pr.set_id(id);
                pr
            })
            .collect()
    }

    fn must_redirect(
        reader: &mut LocalReader<
            MockRouter,
            KvTestEngine,
            CachedReadDelegate<KvTestEngine>,
            StoreMetaDelegate<KvTestEngine>,
        >,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        cmd: RaftCmdRequest,
    ) {
        reader.propose_raft_command(
            None,
            cmd.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd
        );
    }

    fn must_not_redirect(
        reader: &mut LocalReader<
            MockRouter,
            KvTestEngine,
            CachedReadDelegate<KvTestEngine>,
            StoreMetaDelegate<KvTestEngine>,
        >,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        task: RaftCommand<KvTestSnapshot>,
    ) {
        reader.propose_raft_command(None, task.request, task.callback);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[test]
    fn test_read() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());

        // region: 1,
        // peers: 2, 3, 4,
        // leader:2,
        // from "" to "",
        // epoch 1, 1,
        // term 6.
        let mut region1 = metapb::Region::default();
        region1.set_id(1);
        let prs = new_peers(store_id, vec![2, 3, 4]);
        region1.set_peers(prs.clone().into());
        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let leader2 = prs[0].clone();
        region1.set_region_epoch(epoch13.clone());
        let term6 = 6;
        let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
        let read_progress = Arc::new(RegionReadProgress::new(&region1, 1, 1, "".to_owned()));

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader2.clone());
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // The region is not register yet.
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.no_region.get()),
            1
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            1
        );
        assert!(reader.delegates.get(&1).is_none());

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        // But the applied_term is stale.
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region1.clone()),
                peer_id: leader2.get_id(),
                term: term6,
                applied_term: term6 - 1,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                read_progress: read_progress.clone(),
                pending_remove: false,
                track_ver: TrackVer::new(),
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
        }

        // The applied_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            2
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.applied_term.get()),
            1
        );

        // Make the applied_term matches current term.
        let pg = Progress::applied_term(term6);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            3
        );

        // Let's read.
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region1);
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

        // Wait for expiration.
        thread::sleep(Duration::seconds(1).to_std().unwrap());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.lease_expire.get()),
            1
        );

        // Renew lease.
        lease.renew(monotonic_raw_now());

        // Store id mismatch.
        let mut cmd_store_id = cmd.clone();
        cmd_store_id
            .mut_header()
            .mut_peer()
            .set_store_id(store_id + 1);
        reader.propose_raft_command(
            None,
            cmd_store_id,
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_store_not_match());
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.store_id_mismatch.get()),
            1
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            3
        );

        // metapb::Peer id mismatch.
        let mut cmd_peer_id = cmd.clone();
        cmd_peer_id
            .mut_header()
            .mut_peer()
            .set_id(leader2.get_id() + 1);
        reader.propose_raft_command(
            None,
            cmd_peer_id,
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                assert!(
                    resp.response.get_header().has_error(),
                    "{:?}",
                    resp.response
                );
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.peer_id_mismatch.get()),
            1
        );

        // Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        must_redirect(&mut reader, &rx, cmd_read_quorum);

        // Term mismatch.
        let mut cmd_term = cmd.clone();
        cmd_term.mut_header().set_term(term6 - 2);
        reader.propose_raft_command(
            None,
            cmd_term,
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_stale_command(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.term_mismatch.get()),
            1
        );

        // Stale epoch.
        let mut epoch12 = epoch13;
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_region_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.epoch.get()),
            1
        );

        // Expire lease manually, and it can not be renewed.
        let previous_lease_rejection =
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.lease_expire.get());
        lease.expire();
        lease.renew(monotonic_raw_now());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.lease_expire.get()),
            previous_lease_rejection + 1
        );

        // Channel full.
        reader.propose_raft_command(None, cmd.clone(), Callback::None);
        reader.propose_raft_command(
            None,
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_server_is_busy(), "{:?}", resp);
                assert!(resp.snapshot.is_none());
            })),
        );
        rx.try_recv().unwrap();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.channel_full.get()),
            1
        );

        // Reject by term mismatch in lease.
        let previous_term_rejection =
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.term_mismatch.get());
        let mut cmd9 = cmd.clone();
        cmd9.mut_header().set_term(term6 + 3);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::term(term6 + 3));
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::applied_term(term6 + 3));
        }
        reader.propose_raft_command(
            None,
            cmd9.clone(),
            Callback::Read(Box::new(|resp| {
                panic!("unexpected invoke, {:?}", resp);
            })),
        );
        assert_eq!(
            rx.recv_timeout(Duration::seconds(5).to_std().unwrap())
                .unwrap()
                .request,
            cmd9
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.term_mismatch.get()),
            previous_term_rejection + 1
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            4
        );

        // Stale local ReadDelegate
        cmd.mut_header().set_term(term6 + 3);
        lease.expire_remote_lease();
        let remote_lease = lease.maybe_new_remote_lease(term6 + 3).unwrap();
        let pg = Progress::leader_lease(remote_lease);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            5
        );

        // Stale read
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            0
        );
        read_progress.update_safe_ts(1, 1);
        assert_eq!(read_progress.safe_ts(), 1);

        let data = {
            let mut d = [0u8; 8];
            (&mut d[..]).encode_u64(2).unwrap();
            d
        };
        cmd.mut_header()
            .set_flags(WriteBatchFlags::STALE_READ.bits());
        cmd.mut_header().set_flag_data(data.into());
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let err = resp.response.get_header().get_error();
                assert!(err.has_data_is_not_ready());
                assert!(resp.snapshot.is_none());
            })),
        );
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            1
        );

        read_progress.update_safe_ts(1, 2);
        assert_eq!(read_progress.safe_ts(), 2);
        let task = RaftCommand::<KvTestSnapshot>::new(cmd, Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            1
        );

        // Remove invalid delegate
        let reader_clone = store_meta.lock().unwrap().readers.get(&1).unwrap().clone();
        assert!(reader.get_delegate(1).is_some());

        // dropping the non-source `reader` will not make other readers invalid
        drop(reader_clone);
        assert!(reader.get_delegate(1).is_some());

        // drop the source `reader`
        store_meta.lock().unwrap().readers.remove(&1).unwrap();
        // the invalid delegate should be removed
        assert!(reader.get_delegate(1).is_none());
    }

    #[test]
    fn test_read_delegate_cache_update() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, _) = new_reader("test-local-reader", store_id, store_meta.clone());
        let mut region = metapb::Region::default();
        region.set_id(1);
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region.clone()),
                peer_id: 1,
                term: 1,
                applied_term: 1,
                leader_lease: None,
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                track_ver: TrackVer::new(),
                read_progress: Arc::new(RegionReadProgress::new(&region, 0, 0, "".to_owned())),
                pending_remove: false,
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
        }

        let d = reader.get_delegate(1).unwrap();
        assert_eq!(&*d.region, &region);
        assert_eq!(d.term, 1);
        assert_eq!(d.applied_term, 1);
        assert!(d.leader_lease.is_none());
        drop(d);

        {
            region.mut_region_epoch().set_version(10);
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::region(region.clone()));
        }
        assert_eq!(&*reader.get_delegate(1).unwrap().region, &region);

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(Progress::term(2));
        }
        assert_eq!(reader.get_delegate(1).unwrap().term, 2);

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::applied_term(2));
        }
        assert_eq!(reader.get_delegate(1).unwrap().applied_term, 2);

        {
            let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
            let remote = lease.maybe_new_remote_lease(3).unwrap();
            let pg = Progress::leader_lease(remote);
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let d = reader.get_delegate(1).unwrap();
        assert_eq!(d.leader_lease.clone().unwrap().term(), 3);
    }

    #[test]
    fn test_read_delegate() {
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let kv_engine =
            engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
        kv_engine.put(b"a1", b"val1").unwrap();
        let store_meta =
            StoreMetaDelegate::new(Arc::new(Mutex::new(StoreMeta::new(0))), kv_engine.clone());

        {
            let mut meta = store_meta.store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let read_delegate = ReadDelegate::mock(1);
            meta.readers.insert(1, read_delegate);

            // Create read_delegate with region id 1
            let read_delegate = ReadDelegate::mock(2);
            meta.readers.insert(2, read_delegate);
        }

        let mut read_id = ThreadReadId::new();
        let mut snap_cache = Box::new(None);

        let read_id_copy = Some(read_id.clone());

        let mut read_context = Some(LocalReadContext {
            read_id: &mut read_id,
            snap_cache: &mut snap_cache,
        });

        let (_, delegate) = store_meta.get_executor_and_len(1);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(kv_engine.as_inner().path(), tablet.as_inner().path());
        let snapshot = delegate.get_snapshot(read_id_copy.clone(), &mut read_context);
        assert_eq!(
            b"val1".to_vec(),
            *snapshot.get_value(b"a1").unwrap().unwrap()
        );

        let (_, delegate) = store_meta.get_executor_and_len(2);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(kv_engine.as_inner().path(), tablet.as_inner().path());
        let snapshot = delegate.get_snapshot(read_id_copy, &mut read_context);
        assert_eq!(
            b"val1".to_vec(),
            *snapshot.get_value(b"a1").unwrap().unwrap()
        );

        assert!(snap_cache.as_ref().is_some());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_requests.get()),
            2
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_snapshot_cache_hit.get()),
            1
        );
    }

    #[test]
    fn test_snap_cache_hit() {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, _) = new_reader("test-local-reader", 1, store_meta.clone());

        let mut region1 = metapb::Region::default();
        region1.set_id(1);

        // Register region 1
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region1.clone()),
                peer_id: 1,
                term: 1,
                applied_term: 1,
                leader_lease: None,
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                read_progress: Arc::new(RegionReadProgress::new(&region1, 1, 1, "".to_owned())),
                pending_remove: false,
                track_ver: TrackVer::new(),
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
        }

        let mut delegate = reader.get_delegate(region1.id).unwrap();
        let read_id = Some(ThreadReadId::new());

        {
            let mut read_context = Some(LocalReadContext {
                snap_cache: &mut reader.snap_cache,
                read_id: &mut reader.cache_read_id,
            });

            for _ in 0..10 {
                // Different region id should reuse the cache
                let _ = delegate.get_snapshot(read_id.clone(), &mut read_context);
            }
        }
        // We should hit cache 9 times
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_snapshot_cache_hit.get()),
            9
        );

        let read_id = Some(ThreadReadId::new());

        {
            let read_context = LocalReadContext {
                snap_cache: &mut reader.snap_cache,
                read_id: &mut reader.cache_read_id,
            };

            let _ = delegate.get_snapshot(read_id.clone(), &mut Some(read_context));
        }
        // This time, we will miss the cache
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_snapshot_cache_hit.get()),
            9
        );

        {
            let read_context = LocalReadContext {
                snap_cache: &mut reader.snap_cache,
                read_id: &mut reader.cache_read_id,
            };
            let _ = delegate.get_snapshot(read_id.clone(), &mut Some(read_context));
            // We can hit it again.
            assert_eq!(
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_snapshot_cache_hit.get()),
                10
            );
        }

        reader.release_snapshot_cache();
        {
            let read_context = LocalReadContext {
                snap_cache: &mut reader.snap_cache,
                read_id: &mut reader.cache_read_id,
            };
            let _ = delegate.get_snapshot(read_id.clone(), &mut Some(read_context));
        }
        // After release, we will mss the cache even with the prevsiou read_id.
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_snapshot_cache_hit.get()),
            10
        );

        {
            let read_context = LocalReadContext {
                snap_cache: &mut reader.snap_cache,
                read_id: &mut reader.cache_read_id,
            };
            let _ = delegate.get_snapshot(read_id, &mut Some(read_context));
        }
        // We can hit it again.
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().local_executed_snapshot_cache_hit.get()),
            11
        );
    }
}
