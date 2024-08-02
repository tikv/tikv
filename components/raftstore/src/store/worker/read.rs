// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    fmt::{self, Display, Formatter},
    ops::Deref,
    sync::{
        atomic::{self, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crossbeam::{atomic::AtomicCell, channel::TrySendError};
use engine_traits::{CacheRange, KvEngine, Peekable, RaftEngine, SnapshotContext};
use fail::fail_point;
use kvproto::{
    errorpb,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb::{self, Region},
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, ReadIndexResponse, Request, Response},
};
use pd_client::BucketMeta;
use tikv_util::{
    codec::number::decode_u64,
    debug, error,
    lru::LruCache,
    store::find_peer_by_id,
    time::{monotonic_raw_now, ThreadReadId},
};
use time::Timespec;
use tracker::GLOBAL_TRACKERS;
use txn_types::{TimeStamp, WriteBatchFlags};

use super::metrics::*;
use crate::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp,
        fsm::store::StoreMeta,
        util::{self, LeaseState, RegionReadProgress, RemoteLease},
        Callback, CasualMessage, CasualRouter, Peer, ProposalRouter, RaftCommand, ReadCallback,
        ReadResponse, RegionSnapshot, RequestInspector, RequestPolicy, TxnExt,
    },
    Error, Result,
};

/// #[RaftstoreCommon]
pub trait ReadExecutor {
    type Tablet: KvEngine;

    fn get_tablet(&mut self) -> &Self::Tablet;

    /// Get the snapshot fo the tablet.
    ///
    /// If the tablet is not ready, `None` is returned.
    /// Currently, only multi-rocksdb version may return `None`.
    fn get_snapshot(
        &mut self,
        snap_ctx: Option<SnapshotContext>,
        read_context: &Option<LocalReadContext<'_, Self::Tablet>>,
    ) -> Arc<<Self::Tablet as KvEngine>::Snapshot>;

    fn get_value(
        &mut self,
        req: &Request,
        region: &metapb::Region,
        snap_ctx: Option<SnapshotContext>,
        read_context: &Option<LocalReadContext<'_, Self::Tablet>>,
    ) -> Result<Response> {
        let key = req.get_get().get_key();
        // region key range has no data prefix, so we must use origin key to check.
        util::check_key_in_region(key, region)?;

        let mut resp = Response::default();
        let snapshot = self.get_snapshot(snap_ctx, read_context);
        let res = if !req.get_get().get_cf().is_empty() {
            let cf = req.get_get().get_cf();
            snapshot
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
            snapshot
                .get_value(&keys::data_key(key))
                .unwrap_or_else(|e| {
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
        snap_ctx: Option<SnapshotContext>,
        local_read_ctx: Option<LocalReadContext<'_, Self::Tablet>>,
    ) -> ReadResponse<<Self::Tablet as KvEngine>::Snapshot> {
        let requests = msg.get_requests();
        let mut response = ReadResponse {
            response: RaftCmdResponse::default(),
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        };
        let mut responses = Vec::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = match cmd_type {
                CmdType::Get => {
                    match self.get_value(req, region.as_ref(), snap_ctx.clone(), &local_read_ctx) {
                        Ok(resp) => resp,
                        Err(e) => {
                            error!(?e;
                                "failed to execute get command";
                                "region_id" => region.get_id(),
                            );
                            response.response = cmd_resp::new_error(e);
                            return response;
                        }
                    }
                }
                CmdType::Snap => {
                    let snapshot = RegionSnapshot::from_snapshot(
                        self.get_snapshot(snap_ctx.clone(), &local_read_ctx),
                        region.clone(),
                    );
                    response.snapshot = Some(snapshot);
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
        response.response.set_responses(responses.into());
        response
    }
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

pub struct LocalReadContext<'a, E>
where
    E: KvEngine,
{
    read_id: Option<ThreadReadId>,
    snap_cache: &'a mut SnapCache<E>,

    // Used when read_id is not set, duplicated definition to avoid cache invalidation in case
    // stale read and local read are mixed in one batch.
    snapshot: Option<Arc<E::Snapshot>>,
    snapshot_ts: Option<Timespec>,
}

impl<'a, E> LocalReadContext<'a, E>
where
    E: KvEngine,
{
    fn new(snap_cache: &'a mut SnapCache<E>, read_id: Option<ThreadReadId>) -> Self {
        Self {
            snap_cache,
            read_id,
            snapshot: None,
            snapshot_ts: None,
        }
    }

    // Update the snapshot in the `snap_cache` if the read_id is None or does
    // not match.
    // snap_ctx is used (if not None) to acquire the snapshot of the relevant region
    // from region cache engine
    fn maybe_update_snapshot(
        &mut self,
        engine: &E,
        snap_ctx: Option<SnapshotContext>,
        delegate_last_valid_ts: Timespec,
    ) -> bool {
        // When the read_id is None, it means the `snap_cache` has been cleared
        // before and the `cached_read_id` of it is None because only a consecutive
        // requests will have the same cache and the cache will be cleared after the
        // last request of the batch.
        if self.read_id.is_some() {
            if self.snap_cache.cached_read_id == self.read_id
                && self.read_id.as_ref().unwrap().create_time >= delegate_last_valid_ts
            {
                // Cache hit
                return false;
            }

            self.snap_cache.cached_read_id = self.read_id.clone();
            self.snap_cache.snapshot = Some(Arc::new(engine.snapshot(snap_ctx)));

            // Ensures the snapshot is acquired before getting the time
            atomic::fence(atomic::Ordering::Release);
            self.snap_cache.cached_snapshot_ts = monotonic_raw_now();
        } else {
            // read_id being None means the snapshot acquired will only be used in this
            // request
            self.snapshot = Some(Arc::new(engine.snapshot(snap_ctx)));

            // Ensures the snapshot is acquired before getting the time
            atomic::fence(atomic::Ordering::Release);
            self.snapshot_ts = Some(monotonic_raw_now());
        }

        true
    }

    fn snapshot_ts(&self) -> Option<Timespec> {
        if self.read_id.is_some() {
            Some(self.snap_cache.cached_snapshot_ts)
        } else {
            self.snapshot_ts
        }
    }

    // Note: must be called after `maybe_update_snapshot`
    fn snapshot(&self) -> Option<Arc<E::Snapshot>> {
        // read_id being some means we go through cache
        if self.read_id.is_some() {
            self.snap_cache.snapshot.clone()
        } else {
            self.snapshot.clone()
        }
    }
}

impl Drop for ReadDelegate {
    fn drop(&mut self) {
        // call `inc` to notify the source `ReadDelegate` is dropped
        self.track_ver.inc();
    }
}

/// #[RaftstoreCommon]
pub trait ReadExecutorProvider: Send + Clone + 'static {
    type Executor;
    type StoreMeta;

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

impl<E> ReadExecutorProvider for StoreMetaDelegate<E>
where
    E: KvEngine,
{
    type Executor = CachedReadDelegate<E>;
    type StoreMeta = Arc<Mutex<StoreMeta>>;

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
    /// Indicates whether the peer is waiting data. See more in `Peer`.
    pub wait_data: bool,

    // `track_ver` used to keep the local `ReadDelegate` in `LocalReader`
    // up-to-date with the global `ReadDelegate` stored at `StoreMeta`
    pub track_ver: TrackVer,
}

impl ReadDelegate {
    pub fn from_peer<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> Self {
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
            wait_data: false,
            bucket_meta: peer
                .region_buckets_info()
                .bucket_stat()
                .map(|b| b.meta.clone()),
            track_ver: TrackVer::new(),
        }
    }

    pub fn new(
        peer_id: u64,
        term: u64,
        region: Region,
        applied_term: u64,
        txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
        txn_ext: Arc<TxnExt>,
        read_progress: Arc<RegionReadProgress>,
        bucket_meta: Option<Arc<BucketMeta>>,
    ) -> Self {
        let region_id = region.id;
        ReadDelegate {
            region: Arc::new(region),
            peer_id,
            term,
            applied_term,
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, peer_id),
            txn_extra_op,
            txn_ext,
            read_progress,
            pending_remove: false,
            wait_data: false,
            bucket_meta,
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
                self.leader_lease = leader_lease;
            }
            Progress::RegionBuckets(bucket_meta) => {
                if let Some(meta) = &self.bucket_meta {
                    if meta.version >= bucket_meta.version {
                        return;
                    }
                }
                self.bucket_meta = Some(bucket_meta);
            }
            Progress::WaitData(wait_data) => {
                self.wait_data = wait_data;
            }
        }
    }

    pub fn need_renew_lease(&self, ts: Timespec) -> bool {
        self.leader_lease
            .as_ref()
            .map(|lease| lease.need_renew(ts))
            .unwrap_or(false)
    }

    // If the remote lease will be expired in near future send message
    // to `raftstore` to renew it
    pub fn maybe_renew_lease_advance<EK: KvEngine>(
        &self,
        router: &dyn CasualRouter<EK>,
        ts: Timespec,
    ) {
        if !self.need_renew_lease(ts) {
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
        fail_point!("perform_read_local", |_| true);

        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                if lease.inspect(Some(ts)) == LeaseState::Valid {
                    fail_point!("after_pass_lease_check");
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

    pub fn check_stale_read_safe(&self, read_ts: u64) -> std::result::Result<(), RaftCmdResponse> {
        let safe_ts = self.read_progress.safe_ts();
        fail_point!("skip_check_stale_read_safe", |_| Ok(()));
        if safe_ts >= read_ts {
            return Ok(());
        }
        // Advancing resolved ts may be expensive, only notify if read_ts - safe_ts >
        // 200ms.
        if TimeStamp::from(read_ts).physical() > TimeStamp::from(safe_ts).physical() + 200 {
            self.read_progress.notify_advance_resolved_ts();
        }
        debug!(
            "reject stale read by safe ts";
            "safe_ts" => safe_ts,
            "read_ts" => read_ts,
            "region_id" => self.region.get_id(),
            "peer_id" => self.peer_id,
        );
        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.safe_ts.inc());
        let mut response = cmd_resp::new_error(Error::DataIsNotReady {
            region_id: self.region.get_id(),
            peer_id: self.peer_id,
            safe_ts,
        });
        cmd_resp::bind_term(&mut response, self.term);
        Err(response)
    }

    /// Used in some external tests.
    pub fn mock(region_id: u64) -> Self {
        let mut region: metapb::Region = Default::default();
        region.set_id(region_id);
        let read_progress = Arc::new(RegionReadProgress::new(&region, 0, 0, 1));
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
            wait_data: false,
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
    LeaderLease(Option<RemoteLease>),
    RegionBuckets(Arc<BucketMeta>),
    WaitData(bool),
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

    pub fn set_leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(Some(lease))
    }

    pub fn unset_leader_lease() -> Progress {
        Progress::LeaderLease(None)
    }

    pub fn region_buckets(bucket_meta: Arc<BucketMeta>) -> Progress {
        Progress::RegionBuckets(bucket_meta)
    }

    pub fn wait_data(wait_data: bool) -> Progress {
        Progress::WaitData(wait_data)
    }
}

struct SnapCache<E>
where
    E: KvEngine,
{
    cached_read_id: Option<ThreadReadId>,
    snapshot: Option<Arc<E::Snapshot>>,
    cached_snapshot_ts: Timespec,
}

impl<E> SnapCache<E>
where
    E: KvEngine,
{
    fn new() -> Self {
        SnapCache {
            cached_read_id: None,
            snapshot: None,
            cached_snapshot_ts: Timespec::new(0, 0),
        }
    }

    fn clear(&mut self) {
        self.cached_read_id.take();
        self.snapshot.take();
    }
}

impl<E> Clone for SnapCache<E>
where
    E: KvEngine,
{
    fn clone(&self) -> Self {
        Self {
            cached_read_id: self.cached_read_id.clone(),
            snapshot: self.snapshot.clone(),
            cached_snapshot_ts: self.cached_snapshot_ts,
        }
    }
}

/// #[RaftstoreCommon]: LocalReader is an entry point where local read requests are dipatch to the
/// relevant regions by LocalReader so that these requests can be handled by the
/// relevant ReadDelegate respectively.
pub struct LocalReaderCore<D, S> {
    pub store_id: Cell<Option<u64>>,
    store_meta: S,
    pub delegates: LruCache<u64, D>,
}

impl<D, S> LocalReaderCore<D, S>
where
    D: Deref<Target = ReadDelegate> + Clone,
    S: ReadExecutorProvider<Executor = D>,
{
    pub fn new(store_meta: S) -> Self {
        LocalReaderCore {
            store_meta,
            store_id: Cell::new(None),
            delegates: LruCache::with_capacity_and_sample(0, 7),
        }
    }

    pub fn store_meta(&self) -> &S {
        &self.store_meta
    }

    // Ideally `get_delegate` should return `Option<&ReadDelegate>`, but if so the
    // lifetime of the returned `&ReadDelegate` will bind to `self`, and make it
    // impossible to use `&mut self` while the `&ReadDelegate` is alive, a better
    // choice is use `Rc` but `LocalReader: Send` will be violated, which is
    // required by `LocalReadRouter: Send`, use `Arc` will introduce extra cost but
    // make the logic clear
    pub fn get_delegate(&mut self, region_id: u64) -> Option<D> {
        let rd = match self.delegates.get(&region_id) {
            // The local `ReadDelegate` is up to date
            Some(d) if !d.track_ver.any_new() => Some(d.clone()),
            _ => {
                debug!("update local read delegate"; "region_id" => region_id);
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.cache_miss.inc());

                let (meta_len, meta_reader) = { self.store_meta.get_executor_and_len(region_id) };

                // Remove the stale delegate
                self.delegates.remove(&region_id);
                self.delegates.resize(meta_len);
                match meta_reader {
                    Some(reader) => {
                        self.delegates.insert(region_id, reader.clone());
                        Some(reader)
                    }
                    None => None,
                }
            }
        };
        // Return `None` if the read delegate is pending remove
        rd.filter(|r| !r.pending_remove)
    }

    pub fn validate_request(&mut self, req: &RaftCmdRequest) -> Result<Option<D>> {
        // Check store id.
        if self.store_id.get().is_none() {
            let store_id = self.store_meta.store_id();
            self.store_id.set(store_id);
        }
        let store_id = self.store_id.get().unwrap();

        if let Err(e) = util::check_store_id(req.get_header(), store_id) {
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
        if let Err(e) = util::check_peer_id(req.get_header(), delegate.peer_id) {
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.peer_id_mismatch.inc());
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req.get_header(), delegate.term) {
            debug!(
                "check term";
                "delegate_term" => delegate.term,
                "header_term" => req.get_header().get_term(),
                "tag" => &delegate.tag,
            );
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.term_mismatch.inc());
            return Err(e);
        }

        // Check region epoch.
        if util::check_req_region_epoch(req, &delegate.region, false).is_err() {
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.epoch.inc());
            // Stale epoch, redirect it to raftstore to get the latest region.
            debug!("rejected by epoch not match"; "tag" => &delegate.tag);
            return Ok(None);
        }

        match find_peer_by_id(&delegate.region, delegate.peer_id) {
            // Check witness
            Some(peer) => {
                if peer.is_witness {
                    TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.witness.inc());
                    return Err(Error::IsWitness(region_id));
                }
            }
            // This (rarely) happen in witness disabled clusters while the conf change applied but
            // region not removed. We shouldn't return `IsWitness` here because our client back off
            // for a long time while encountering that.
            None => {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_region.inc());
                return Err(Error::RegionNotFound(region_id));
            }
        }

        // Check non-witness hasn't finish applying snapshot yet.
        if delegate.wait_data {
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.wait_data.inc());
            return Err(Error::IsWitness(region_id));
        }

        // Check whether the region is in the flashback state and the local read could
        // be performed.
        let is_in_flashback = delegate.region.is_in_flashback;
        let flashback_start_ts = delegate.region.flashback_start_ts;
        let header = req.get_header();
        let admin_type = req.admin_request.as_ref().map(|req| req.get_cmd_type());
        if let Err(e) = util::check_flashback_state(
            is_in_flashback,
            flashback_start_ts,
            header,
            admin_type,
            region_id,
            true,
        ) {
            debug!("rejected by flashback state";
                "error" => ?e,
                "is_in_flashback" => is_in_flashback,
                "tag" => &delegate.tag);
            match e {
                Error::FlashbackNotPrepared(_) => {
                    TLS_LOCAL_READ_METRICS
                        .with(|m| m.borrow_mut().reject_reason.flashback_not_prepared.inc());
                }
                Error::FlashbackInProgress(..) => {
                    TLS_LOCAL_READ_METRICS
                        .with(|m| m.borrow_mut().reject_reason.flashback_in_progress.inc());
                }
                _ => unreachable!("{:?}", e),
            };
            return Err(e);
        }

        Ok(Some(delegate))
    }
}

impl<D, S> Clone for LocalReaderCore<D, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        LocalReaderCore {
            store_meta: self.store_meta.clone(),
            store_id: self.store_id.clone(),
            delegates: LruCache::with_capacity_and_sample(0, 7),
        }
    }
}

pub struct LocalReader<E, C>
where
    E: KvEngine,
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
{
    local_reader: LocalReaderCore<CachedReadDelegate<E>, StoreMetaDelegate<E>>,
    kv_engine: E,
    snap_cache: SnapCache<E>,
    // A channel to raftstore.
    router: C,
}

impl<E, C> LocalReader<E, C>
where
    E: KvEngine,
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
{
    pub fn new(kv_engine: E, store_meta: StoreMetaDelegate<E>, router: C) -> Self {
        Self {
            local_reader: LocalReaderCore::new(store_meta),
            kv_engine,
            snap_cache: SnapCache::new(),
            router,
        }
    }

    pub fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> Result<Option<(CachedReadDelegate<E>, RequestPolicy)>> {
        if let Some(delegate) = self.local_reader.validate_request(req)? {
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
        } else {
            Ok(None)
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

        cmd.callback.set_result(read_resp);
    }

    /// Try to handle the read request using local read, if the leader is valid
    /// the read response is returned, otherwise None is returned.
    fn try_local_leader_read(
        &mut self,
        req: &RaftCmdRequest,
        delegate: &mut CachedReadDelegate<E>,
        snap_ctx: Option<SnapshotContext>,
        read_id: Option<ThreadReadId>,
        snap_updated: &mut bool,
        last_valid_ts: Timespec,
    ) -> Option<ReadResponse<E::Snapshot>> {
        let mut local_read_ctx = LocalReadContext::new(&mut self.snap_cache, read_id);
        if snap_ctx.is_some() {
            // When `snap_ctx` is some, it means we want to acquire the range cache engine
            // snapshot which cannot be used across different regions. So we don't use
            // cache.
            local_read_ctx.read_id.take();
        }

        (*snap_updated) = local_read_ctx.maybe_update_snapshot(
            delegate.get_tablet(),
            snap_ctx.clone(),
            last_valid_ts,
        );

        let snapshot_ts = local_read_ctx.snapshot_ts().unwrap();
        if !delegate.is_in_leader_lease(snapshot_ts) {
            return None;
        }

        let region = Arc::clone(&delegate.region);
        let mut response = delegate.execute(req, &region, None, snap_ctx, Some(local_read_ctx));
        if let Some(snap) = response.snapshot.as_mut() {
            snap.bucket_meta = delegate.bucket_meta.clone();
        }
        // Try renew lease in advance
        delegate.maybe_renew_lease_advance(&self.router, snapshot_ts);
        Some(response)
    }

    /// Try to handle the stale read request, if the read_ts < safe_ts the read
    /// response is returned, otherwise the raft command response with
    /// `DataIsNotReady` error is returned.
    fn try_local_stale_read(
        &mut self,
        req: &RaftCmdRequest,
        delegate: &mut CachedReadDelegate<E>,
        snap_updated: &mut bool,
        last_valid_ts: Timespec,
    ) -> std::result::Result<ReadResponse<E::Snapshot>, RaftCmdResponse> {
        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
        delegate.check_stale_read_safe(read_ts)?;

        // Stale read does not use cache, so we pass None for read_id
        let mut local_read_ctx = LocalReadContext::new(&mut self.snap_cache, None);
        (*snap_updated) =
            local_read_ctx.maybe_update_snapshot(delegate.get_tablet(), None, last_valid_ts);

        let region = Arc::clone(&delegate.region);
        // Getting the snapshot
        let mut response = delegate.execute(req, &region, None, None, Some(local_read_ctx));
        if let Some(snap) = response.snapshot.as_mut() {
            snap.bucket_meta = delegate.bucket_meta.clone();
        }
        // Double check in case `safe_ts` change after the first check and before
        // getting snapshot
        delegate.check_stale_read_safe(read_ts)?;

        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().local_executed_stale_read_requests.inc());
        Ok(response)
    }

    pub fn propose_raft_command(
        &mut self,
        mut snap_ctx: Option<SnapshotContext>,
        read_id: Option<ThreadReadId>,
        mut req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        match self.pre_propose_raft_command(&req) {
            Ok(Some((mut delegate, policy))) => {
                if let Some(ref mut ctx) = snap_ctx {
                    ctx.region_id = delegate.region.id;
                    ctx.epoch_version = delegate.region.get_region_epoch().version;
                    ctx.set_range(CacheRange::from_region(&delegate.region))
                }

                let mut snap_updated = false;
                let last_valid_ts = delegate.last_valid_ts;
                let mut response = match policy {
                    // Leader can read local if and only if it is in lease.
                    RequestPolicy::ReadLocal => {
                        if let Some(read_resp) = self.try_local_leader_read(
                            &req,
                            &mut delegate,
                            snap_ctx,
                            read_id,
                            &mut snap_updated,
                            last_valid_ts,
                        ) {
                            read_resp
                        } else {
                            fail_point!("localreader_before_redirect", |_| {});
                            // Forward to raftstore.
                            self.redirect(RaftCommand::new(req, cb));
                            return;
                        }
                    }
                    // Replica can serve stale read if and only if its `safe_ts` >= `read_ts`
                    RequestPolicy::StaleRead => {
                        match self.try_local_stale_read(
                            &req,
                            &mut delegate,
                            &mut snap_updated,
                            last_valid_ts,
                        ) {
                            Ok(read_resp) => read_resp,
                            Err(err_resp) => {
                                // It's safe to change the header of the `RaftCmdRequest`, as it
                                // would not affect the `SnapCtx` used in upper layer like.
                                let unset_stale_flag = req.get_header().get_flags()
                                    & (!WriteBatchFlags::STALE_READ.bits());
                                req.mut_header().set_flags(unset_stale_flag);
                                let mut inspector = Inspector {
                                    delegate: &delegate,
                                };
                                // The read request could be handled using snapshot read if the
                                // local peer is a valid leader.
                                let allow_fallback_leader_read = inspector
                                    .inspect(&req)
                                    .map_or(false, |r| r == RequestPolicy::ReadLocal);
                                if !allow_fallback_leader_read {
                                    cb.set_result(ReadResponse {
                                        response: err_resp,
                                        snapshot: None,
                                        txn_extra_op: TxnExtraOp::Noop,
                                    });
                                    return;
                                }
                                if let Some(read_resp) = self.try_local_leader_read(
                                    &req,
                                    &mut delegate,
                                    None,
                                    None,
                                    &mut snap_updated,
                                    last_valid_ts,
                                ) {
                                    TLS_LOCAL_READ_METRICS.with(|m| {
                                        m.borrow_mut()
                                            .local_executed_stale_read_fallback_success_requests
                                            .inc()
                                    });
                                    read_resp
                                } else {
                                    TLS_LOCAL_READ_METRICS.with(|m| {
                                        m.borrow_mut()
                                            .local_executed_stale_read_fallback_failure_requests
                                            .inc()
                                    });
                                    cb.set_result(ReadResponse {
                                        response: err_resp,
                                        snapshot: None,
                                        txn_extra_op: TxnExtraOp::Noop,
                                    });
                                    return;
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                };

                cb.read_tracker().map(|tracker| {
                    GLOBAL_TRACKERS.with_tracker(tracker, |t| {
                        t.metrics.local_read = true;
                    })
                });

                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().local_executed_requests.inc());
                if !snap_updated {
                    TLS_LOCAL_READ_METRICS
                        .with(|m| m.borrow_mut().local_executed_snapshot_cache_hit.inc());
                }

                cmd_resp::bind_term(&mut response.response, delegate.term);
                if let Some(snap) = response.snapshot.as_mut() {
                    snap.txn_ext = Some(delegate.txn_ext.clone());
                    snap.bucket_meta = delegate.bucket_meta.clone();
                }
                response.txn_extra_op = delegate.txn_extra_op.load();
                cb.set_result(response);
            }
            // Forward to raftstore.
            Ok(None) => self.redirect(RaftCommand::new(req, cb)),
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self
                    .local_reader
                    .delegates
                    .get(&req.get_header().get_region_id())
                {
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

    /// If read requests are received at the same RPC request, we can create one
    /// snapshot for all of them and check whether the time when the snapshot
    /// was created is in lease. We use ThreadReadId to figure out whether this
    /// RaftCommand comes from the same RPC request with the last RaftCommand
    /// which left a snapshot cached in LocalReader. ThreadReadId is composed by
    /// thread_id and a thread_local incremental sequence.
    #[inline]
    pub fn read(
        &mut self,
        snap_ctx: Option<SnapshotContext>,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        self.propose_raft_command(snap_ctx, read_id, req, cb);
        maybe_tls_local_read_metrics_flush();
    }

    pub fn release_snapshot_cache(&mut self) {
        self.snap_cache.clear();
    }
}

impl<E, C> Clone for LocalReader<E, C>
where
    E: KvEngine,
    C: ProposalRouter<E::Snapshot> + CasualRouter<E> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            local_reader: self.local_reader.clone(),
            kv_engine: self.kv_engine.clone(),
            snap_cache: self.snap_cache.clone(),
            router: self.router.clone(),
        }
    }
}

impl<E> ReadExecutor for CachedReadDelegate<E>
where
    E: KvEngine,
{
    type Tablet = E;

    fn get_tablet(&mut self) -> &E {
        &self.kv_engine
    }

    fn get_snapshot(
        &mut self,
        _: Option<SnapshotContext>,
        read_context: &Option<LocalReadContext<'_, E>>,
    ) -> Arc<E::Snapshot> {
        read_context.as_ref().unwrap().snapshot().unwrap()
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
    use std::{ops::Add, sync::mpsc::*, thread};

    use crossbeam::channel::TrySendError;
    use engine_test::kv::{KvTestEngine, KvTestSnapshot};
    use engine_traits::{CacheRange, MiscExt, Peekable, SyncMutable, ALL_CFS};
    use hybrid_engine::{HybridEngine, HybridEngineSnapshot};
    use keys::DATA_PREFIX;
    use kvproto::{metapb::RegionEpoch, raft_cmdpb::*};
    use range_cache_memory_engine::{
        RangeCacheEngineConfig, RangeCacheEngineContext, RangeCacheMemoryEngine,
    };
    use tempfile::{Builder, TempDir};
    use tikv_util::{codec::number::NumberEncoder, config::VersionTrack, time::monotonic_raw_now};
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
        LocalReader<KvTestEngine, MockRouter>,
        Receiver<RaftCommand<KvTestSnapshot>>,
    ) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let db = engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
        let (ch, rx, _) = MockRouter::new();
        let mut reader = LocalReader::new(db.clone(), StoreMetaDelegate::new(store_meta, db), ch);
        reader.local_reader.store_id = Cell::new(Some(store_id));
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
        reader: &mut LocalReader<KvTestEngine, MockRouter>,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        cmd: RaftCmdRequest,
    ) {
        reader.propose_raft_command(
            None,
            None,
            cmd.clone(),
            Callback::read(Box::new(|resp| {
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
        reader: &mut LocalReader<KvTestEngine, MockRouter>,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        task: RaftCommand<KvTestSnapshot>,
    ) {
        must_not_redirect_with_read_id(reader, rx, task, None);
    }

    fn must_not_redirect_with_read_id(
        reader: &mut LocalReader<KvTestEngine, MockRouter>,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        task: RaftCommand<KvTestSnapshot>,
        read_id: Option<ThreadReadId>,
    ) {
        reader.propose_raft_command(None, read_id, task.request, task.callback);
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
        let read_progress = Arc::new(RegionReadProgress::new(&region1, 1, 1, 1));

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
        assert!(reader.local_reader.delegates.get(&1).is_none());

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
                wait_data: false,
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
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            3
        );

        // Let's read.
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
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
            None,
            cmd_store_id,
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
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
            None,
            cmd_peer_id,
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
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
            None,
            cmd_term,
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
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
        reader.propose_raft_command(None, None, cmd.clone(), Callback::None);
        reader.propose_raft_command(
            None,
            None,
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
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
            None,
            cmd9.clone(),
            Callback::read(Box::new(|resp| {
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
        let pg = Progress::set_leader_lease(remote_lease);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::read(Box::new(move |_| {})));
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

        // Expire lease manually to avoid local retry on leader peer.
        lease.expire();
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
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
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
        let task = RaftCommand::<KvTestSnapshot>::new(cmd, Callback::read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            1
        );

        // Remove invalid delegate
        let reader_clone = store_meta.lock().unwrap().readers.get(&1).unwrap().clone();
        assert!(reader.local_reader.get_delegate(1).is_some());

        // dropping the non-source `reader` will not make other readers invalid
        drop(reader_clone);
        assert!(reader.local_reader.get_delegate(1).is_some());

        // drop the source `reader`
        store_meta.lock().unwrap().readers.remove(&1).unwrap();
        // the invalid delegate should be removed
        assert!(reader.local_reader.get_delegate(1).is_none());
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
                read_progress: Arc::new(RegionReadProgress::new(&region, 0, 0, 1)),
                pending_remove: false,
                wait_data: false,
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
        }

        let d = reader.local_reader.get_delegate(1).unwrap();
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
        assert_eq!(
            &*reader.local_reader.get_delegate(1).unwrap().region,
            &region
        );

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(Progress::term(2));
        }
        assert_eq!(reader.local_reader.get_delegate(1).unwrap().term, 2);

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .update(Progress::applied_term(2));
        }
        assert_eq!(reader.local_reader.get_delegate(1).unwrap().applied_term, 2);

        {
            let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
            let remote = lease.maybe_new_remote_lease(3).unwrap();
            let pg = Progress::set_leader_lease(remote);
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().update(pg);
        }
        let d = reader.local_reader.get_delegate(1).unwrap();
        assert_eq!(d.leader_lease.clone().unwrap().term(), 3);
    }

    #[test]
    fn test_read_executor_provider() {
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let kv_engine =
            engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
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

        let (len, delegate) = store_meta.get_executor_and_len(1);
        assert_eq!(2, len);
        let mut delegate = delegate.unwrap();
        assert_eq!(1, delegate.region.id);
        let tablet = delegate.get_tablet();
        assert_eq!(kv_engine.path(), tablet.path());

        let (len, delegate) = store_meta.get_executor_and_len(2);
        assert_eq!(2, len);
        let mut delegate = delegate.unwrap();
        assert_eq!(2, delegate.region.id);
        let tablet = delegate.get_tablet();
        assert_eq!(kv_engine.path(), tablet.path());
    }

    fn prepare_read_delegate_with_lease(
        store_id: u64,
        region_id: u64,
        term: u64,
        pr_ids: Vec<u64>,
        region_epoch: RegionEpoch,
        store_meta: Arc<Mutex<StoreMeta>>,
        max_lease: Duration,
    ) {
        let mut region = metapb::Region::default();
        region.set_id(region_id);
        let prs = new_peers(store_id, pr_ids);
        region.set_peers(prs.clone().into());

        let leader = prs[0].clone();
        region.set_region_epoch(region_epoch);
        let mut lease = Lease::new(max_lease, Duration::milliseconds(250)); // 1s is long enough.
        let read_progress = Arc::new(RegionReadProgress::new(&region, 1, 1, 1));

        // Register region
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term).unwrap();
        // But the applied_term is stale.
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region.clone()),
                peer_id: leader.get_id(),
                term,
                applied_term: term,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                read_progress,
                pending_remove: false,
                wait_data: false,
                track_ver: TrackVer::new(),
                bucket_meta: None,
            };
            meta.readers.insert(region_id, read_delegate);
        }
    }

    fn prepare_read_delegate(
        store_id: u64,
        region_id: u64,
        term: u64,
        pr_ids: Vec<u64>,
        region_epoch: RegionEpoch,
        store_meta: Arc<Mutex<StoreMeta>>,
    ) {
        prepare_read_delegate_with_lease(
            store_id,
            region_id,
            term,
            pr_ids,
            region_epoch,
            store_meta,
            Duration::seconds(1),
        )
    }

    #[test]
    fn test_snap_across_regions() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());

        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let term6 = 6;

        // Register region1
        let pr_ids1 = vec![2, 3, 4];
        let prs1 = new_peers(store_id, pr_ids1.clone());
        prepare_read_delegate(
            store_id,
            1,
            term6,
            pr_ids1,
            epoch13.clone(),
            store_meta.clone(),
        );
        let leader1 = prs1[0].clone();

        // Register region2
        let pr_ids2 = vec![22, 33, 44];
        let prs2 = new_peers(store_id, pr_ids2.clone());
        prepare_read_delegate(store_id, 2, term6, pr_ids2, epoch13.clone(), store_meta);
        let leader2 = prs2[0].clone();

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader1);
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp.snapshot.unwrap()).unwrap();
            })),
        );

        // First request will not hit cache
        let read_id = Some(ThreadReadId::new());
        must_not_redirect_with_read_id(&mut reader, &rx, task, read_id.clone());
        let snap1 = snap_rx.recv().unwrap();

        let mut header = RaftRequestHeader::default();
        header.set_region_id(2);
        header.set_peer(leader2);
        header.set_region_epoch(epoch13);
        header.set_term(term6);
        cmd.set_header(header);
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp.snapshot.unwrap()).unwrap();
            })),
        );
        must_not_redirect_with_read_id(&mut reader, &rx, task, read_id);
        let snap2 = snap_rx.recv().unwrap();
        assert!(std::ptr::eq(snap1.get_snapshot(), snap2.get_snapshot()));

        // If we use a new read id, the cache will be miss and a new snapshot will be
        // generated
        let read_id = Some(ThreadReadId::new());
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp.snapshot.unwrap()).unwrap();
            })),
        );
        must_not_redirect_with_read_id(&mut reader, &rx, task, read_id);
        let snap2 = snap_rx.recv().unwrap();
        assert!(!std::ptr::eq(snap1.get_snapshot(), snap2.get_snapshot()));
    }

    fn create_engine(path: &str) -> KvTestEngine {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap()
    }

    #[test]
    fn test_snap_cache_context() {
        let db = create_engine("test_snap_cache_context");
        let mut snap_cache = SnapCache::new();
        let mut read_context = LocalReadContext::new(&mut snap_cache, None);

        assert!(read_context.snapshot().is_none());
        assert!(read_context.snapshot_ts().is_none());

        db.put(b"a1", b"val1").unwrap();

        let compare_ts = monotonic_raw_now();
        // Case 1: snap_cache_context.read_id is None
        assert!(read_context.maybe_update_snapshot(&db, None, Timespec::new(0, 0)));
        assert!(read_context.snapshot_ts().unwrap() > compare_ts);
        assert_eq!(
            read_context
                .snapshot()
                .unwrap()
                .get_value(b"a1")
                .unwrap()
                .unwrap(),
            b"val1"
        );

        // snap_cache_context is *not* created with read_id, so calling
        // `maybe_update_snapshot` again will update the snapshot
        let compare_ts = monotonic_raw_now();
        assert!(read_context.maybe_update_snapshot(&db, None, Timespec::new(0, 0)));
        assert!(read_context.snapshot_ts().unwrap() > compare_ts);

        let read_id = ThreadReadId::new();
        let read_id_clone = read_id.clone();
        let mut read_context = LocalReadContext::new(&mut snap_cache, Some(read_id));

        let compare_ts = monotonic_raw_now();
        // Case 2: snap_cache_context.read_id is not None but not equals to the
        // snap_cache.cached_read_id
        assert!(read_context.maybe_update_snapshot(&db, None, Timespec::new(0, 0)));
        assert!(read_context.snapshot_ts().unwrap() > compare_ts);
        let snap_ts = read_context.snapshot_ts().unwrap();
        assert_eq!(
            read_context
                .snapshot()
                .unwrap()
                .get_value(b"a1")
                .unwrap()
                .unwrap(),
            b"val1"
        );

        let db2 = create_engine("test_snap_cache_context2");
        // snap_cache_context is created with read_id, so calling
        // `maybe_update_snapshot` again will *not* update the snapshot
        // Case 3: snap_cache_context.read_id is not None and equals to the
        // snap_cache.cached_read_id
        assert!(!read_context.maybe_update_snapshot(&db2, None, Timespec::new(0, 0)));
        assert_eq!(read_context.snapshot_ts().unwrap(), snap_ts);
        assert_eq!(
            read_context
                .snapshot()
                .unwrap()
                .get_value(b"a1")
                .unwrap()
                .unwrap(),
            b"val1"
        );

        // Case 4: delegate.last_valid_ts is larger than create_time of read_id
        let mut last_valid_ts = read_id_clone.create_time;
        last_valid_ts = last_valid_ts.add(Duration::nanoseconds(1));
        assert!(read_context.maybe_update_snapshot(&db2, None, last_valid_ts));
        assert!(read_context.snapshot_ts().unwrap() > snap_ts);
        assert!(
            read_context
                .snapshot()
                .unwrap()
                .get_value(b"a1")
                .unwrap()
                .is_none(),
        );
    }

    #[test]
    fn test_snap_release_for_not_using_cache() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());
        reader.kv_engine.put(b"key", b"value").unwrap();

        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let term6 = 6;

        // Register region1
        let pr_ids1 = vec![2, 3, 4];
        let prs1 = new_peers(store_id, pr_ids1.clone());
        prepare_read_delegate(store_id, 1, term6, pr_ids1, epoch13.clone(), store_meta);
        let leader1 = prs1[0].clone();

        // Local read
        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader1);
        header.set_region_epoch(epoch13);
        header.set_term(term6);
        cmd.set_header(header.clone());
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // using cache and release
        let read_id = ThreadReadId::new();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |_: ReadResponse<KvTestSnapshot>| {})),
        );
        must_not_redirect_with_read_id(&mut reader, &rx, task, Some(read_id));
        assert!(
            reader
                .kv_engine
                .get_oldest_snapshot_sequence_number()
                .is_some()
        );
        reader.release_snapshot_cache();
        assert!(
            reader
                .kv_engine
                .get_oldest_snapshot_sequence_number()
                .is_none()
        );

        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |_: ReadResponse<KvTestSnapshot>| {})),
        );

        // not use cache
        must_not_redirect_with_read_id(&mut reader, &rx, task, None);
        assert!(
            reader
                .kv_engine
                .get_oldest_snapshot_sequence_number()
                .is_none()
        );

        // Stale read
        let mut data = [0u8; 8];
        (&mut data[..]).encode_u64(0).unwrap();
        header.set_flags(header.get_flags() | WriteBatchFlags::STALE_READ.bits());
        header.set_flag_data(data.into());

        cmd.set_header(header);
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd,
            Callback::read(Box::new(move |_: ReadResponse<KvTestSnapshot>| {})),
        );
        let read_id = ThreadReadId::new();
        must_not_redirect_with_read_id(&mut reader, &rx, task, Some(read_id));
        // Stale read will not use snap cache
        assert!(reader.snap_cache.snapshot.is_none());
        assert!(
            reader
                .kv_engine
                .get_oldest_snapshot_sequence_number()
                .is_none()
        );
    }

    #[test]
    fn test_stale_read_notify() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader("test-local-reader", store_id, store_meta.clone());
        reader.kv_engine.put(b"key", b"value").unwrap();

        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let term6 = 6;

        // Register region1
        let pr_ids1 = vec![2, 3, 4];
        let prs1 = new_peers(store_id, pr_ids1.clone());
        prepare_read_delegate(
            store_id,
            1,
            term6,
            pr_ids1,
            epoch13.clone(),
            store_meta.clone(),
        );
        let leader1 = prs1[0].clone();

        // Local read
        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader1);
        header.set_region_epoch(epoch13);
        header.set_term(term6);
        header.set_flags(header.get_flags() | WriteBatchFlags::STALE_READ.bits());
        cmd.set_header(header.clone());
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // A peer can serve read_ts < safe_ts.
        let safe_ts = TimeStamp::compose(2, 0);
        {
            let mut meta = store_meta.lock().unwrap();
            let delegate = meta.readers.get_mut(&1).unwrap();
            delegate
                .read_progress
                .update_safe_ts(1, safe_ts.into_inner());
            assert_eq!(delegate.read_progress.safe_ts(), safe_ts.into_inner());
        }
        let read_ts_1 = TimeStamp::compose(1, 0);
        let mut data = [0u8; 8];
        (&mut data[..]).encode_u64(read_ts_1.into_inner()).unwrap();
        header.set_flag_data(data.into());
        cmd.set_header(header.clone());
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp).unwrap();
            })),
        );
        must_not_redirect(&mut reader, &rx, task);
        snap_rx.recv().unwrap().snapshot.unwrap();

        // A peer has to notify advancing resolved ts if read_ts >= safe_ts.
        let notify = Arc::new(tokio::sync::Notify::new());
        {
            let mut meta = store_meta.lock().unwrap();
            let delegate = meta.readers.get_mut(&1).unwrap();
            delegate
                .read_progress
                .update_advance_resolved_ts_notify(notify.clone());
        }
        // 201ms larger than safe_ts.
        let read_ts_2 = TimeStamp::compose(safe_ts.physical() + 201, 0);
        let mut data = [0u8; 8];
        (&mut data[..]).encode_u64(read_ts_2.into_inner()).unwrap();
        header.set_flag_data(data.into());
        cmd.set_header(header.clone());
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |_: ReadResponse<KvTestSnapshot>| {})),
        );
        let (notify_tx, notify_rx) = channel();
        let (wait_spawn_tx, wait_spawn_rx) = channel();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _ = runtime.spawn(async move {
            wait_spawn_tx.send(()).unwrap();
            notify.notified().await;
            notify_tx.send(()).unwrap();
        });
        wait_spawn_rx.recv().unwrap();
        thread::sleep(std::time::Duration::from_millis(500)); // Prevent lost notify.
        must_not_redirect(&mut reader, &rx, task);
        notify_rx.recv().unwrap();
    }

    #[test]
    fn test_stale_read_local_leader_fallback() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx) = new_reader(
            "test-stale-local-leader-fallback",
            store_id,
            store_meta.clone(),
        );
        reader.kv_engine.put(b"key", b"value").unwrap();

        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let term6 = 6;

        // Register region1.
        let pr_ids1 = vec![2, 3, 4];
        let prs1 = new_peers(store_id, pr_ids1.clone());
        // Ensure the leader lease is long enough so the fallback would work.
        prepare_read_delegate_with_lease(
            store_id,
            1,
            term6,
            pr_ids1.clone(),
            epoch13.clone(),
            store_meta.clone(),
            Duration::seconds(10),
        );
        let leader1 = prs1[0].clone();

        // Local read.
        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader1);
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        header.set_flags(header.get_flags() | WriteBatchFlags::STALE_READ.bits());
        cmd.set_header(header.clone());
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        // A peer can serve read_ts < safe_ts.
        let safe_ts = TimeStamp::compose(2, 0);
        {
            let mut meta = store_meta.lock().unwrap();
            let delegate = meta.readers.get_mut(&1).unwrap();
            delegate
                .read_progress
                .update_safe_ts(1, safe_ts.into_inner());
            assert_eq!(delegate.read_progress.safe_ts(), safe_ts.into_inner());
        }
        let read_ts_1 = TimeStamp::compose(1, 0);
        let mut data = [0u8; 8];
        (&mut data[..]).encode_u64(read_ts_1.into_inner()).unwrap();
        header.set_flag_data(data.into());
        cmd.set_header(header.clone());
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp).unwrap();
            })),
        );
        must_not_redirect(&mut reader, &rx, task);
        snap_rx.recv().unwrap().snapshot.unwrap();

        // When read_ts > safe_ts, the leader peer could still serve if its lease is
        // valid.
        let read_ts_2 = TimeStamp::compose(safe_ts.physical() + 201, 0);
        let mut data = [0u8; 8];
        (&mut data[..]).encode_u64(read_ts_2.into_inner()).unwrap();
        header.set_flag_data(data.into());
        cmd.set_header(header.clone());
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp).unwrap();
            })),
        );
        must_not_redirect(&mut reader, &rx, task);
        snap_rx.recv().unwrap().snapshot.unwrap();

        // The fallback would not happen if the lease is not valid.
        prepare_read_delegate_with_lease(
            store_id,
            1,
            term6,
            pr_ids1,
            epoch13,
            store_meta,
            Duration::milliseconds(1),
        );
        thread::sleep(std::time::Duration::from_millis(50));
        let (snap_tx, snap_rx) = channel();
        let task2 = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                snap_tx.send(resp).unwrap();
            })),
        );
        must_not_redirect(&mut reader, &rx, task2);
        assert!(
            snap_rx
                .recv()
                .unwrap()
                .response
                .get_header()
                .get_error()
                .has_data_is_not_ready()
        );
    }

    type HybridTestEnigne = HybridEngine<KvTestEngine, RangeCacheMemoryEngine>;
    type HybridEngineTestSnapshot = HybridEngineSnapshot<KvTestEngine, RangeCacheMemoryEngine>;

    struct HybridEngineMockRouter {
        p_router: SyncSender<RaftCommand<HybridEngineTestSnapshot>>,
        c_router: SyncSender<(u64, CasualMessage<HybridTestEnigne>)>,
    }

    impl HybridEngineMockRouter {
        #[allow(clippy::type_complexity)]
        fn new() -> (
            HybridEngineMockRouter,
            Receiver<RaftCommand<HybridEngineTestSnapshot>>,
            Receiver<(u64, CasualMessage<HybridTestEnigne>)>,
        ) {
            let (p_ch, p_rx) = sync_channel(1);
            let (c_ch, c_rx) = sync_channel(1);
            (
                HybridEngineMockRouter {
                    p_router: p_ch,
                    c_router: c_ch,
                },
                p_rx,
                c_rx,
            )
        }
    }

    impl ProposalRouter<HybridEngineTestSnapshot> for HybridEngineMockRouter {
        fn send(
            &self,
            cmd: RaftCommand<HybridEngineTestSnapshot>,
        ) -> std::result::Result<(), TrySendError<RaftCommand<HybridEngineTestSnapshot>>> {
            ProposalRouter::send(&self.p_router, cmd)
        }
    }

    impl CasualRouter<HybridTestEnigne> for HybridEngineMockRouter {
        fn send(&self, region_id: u64, msg: CasualMessage<HybridTestEnigne>) -> Result<()> {
            CasualRouter::send(&self.c_router, region_id, msg)
        }
    }

    #[allow(clippy::type_complexity)]
    fn new_hybrid_engine_reader(
        path: &str,
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta>>,
        engine_config: RangeCacheEngineConfig,
    ) -> (
        TempDir,
        LocalReader<HybridTestEnigne, HybridEngineMockRouter>,
        Receiver<RaftCommand<HybridEngineTestSnapshot>>,
        RangeCacheMemoryEngine,
    ) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let disk_engine =
            engine_test::kv::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
        let (ch, rx, _) = HybridEngineMockRouter::new();
        let config = Arc::new(VersionTrack::new(engine_config));
        let memory_engine =
            RangeCacheMemoryEngine::new(RangeCacheEngineContext::new_for_tests(config));
        let engine = HybridEngine::new(disk_engine, memory_engine.clone());
        let mut reader = LocalReader::new(
            engine.clone(),
            StoreMetaDelegate::new(store_meta, engine),
            ch,
        );
        reader.local_reader.store_id = Cell::new(Some(store_id));
        (path, reader, rx, memory_engine)
    }

    fn get_snapshot(
        snap_ctx: Option<SnapshotContext>,
        reader: &mut LocalReader<HybridTestEnigne, HybridEngineMockRouter>,
        request: RaftCmdRequest,
        rx: &Receiver<RaftCommand<HybridEngineTestSnapshot>>,
    ) -> Arc<HybridEngineTestSnapshot> {
        let (sender, receiver) = channel();
        reader.propose_raft_command(
            snap_ctx,
            None,
            request,
            Callback::read(Box::new(move |snap| {
                sender.send(snap).unwrap();
            })),
        );
        // no direct is expected
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        receiver.recv().unwrap().snapshot.unwrap().snap()
    }

    #[test]
    fn test_hybrid_engine_read() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx, memory_engine) = new_hybrid_engine_reader(
            "test-local-hybrid-engine-reader",
            store_id,
            store_meta.clone(),
            RangeCacheEngineConfig::config_for_test(),
        );

        // set up region so we can acquire snapshot from local reader
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
        memory_engine.new_region(region1.clone());
        {
            let mut core = memory_engine.core().write();
            core.mut_range_manager().set_safe_point(reigon1.id, 1);
        }
        let kv = (&[DATA_PREFIX, b'a'], b"b");
        reader.kv_engine.put(kv.0, kv.1).unwrap();
        let term6 = 6;
        let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
        let read_progress = Arc::new(RegionReadProgress::new(&region1, 1, 1, 1));

        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region1.clone()),
                peer_id: leader2.get_id(),
                term: term6,
                applied_term: term6,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: Arc::new(TxnExt::default()),
                read_progress,
                pending_remove: false,
                wait_data: false,
                track_ver: TrackVer::new(),
                bucket_meta: None,
            };
            meta.readers.insert(1, read_delegate);
        }

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader2);
        header.set_region_epoch(epoch13);
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());

        let s = get_snapshot(None, &mut reader, cmd.clone(), &rx);
        assert!(!s.range_cache_snapshot_available());

        {
            let mut core = memory_engine.core().write();
            core.mut_range_manager().set_safe_point(region1.id, 10);
        }

        let snap_ctx = SnapshotContext {
            region_id: 0,
            epoch_version: 0,
            read_ts: 15,
            range: None,
        };

        let s = get_snapshot(Some(snap_ctx.clone()), &mut reader, cmd.clone(), &rx);
        assert!(s.range_cache_snapshot_available());
        assert_eq!(s.get_value(kv.0).unwrap().unwrap(), kv.1);
    }

    #[test]
    fn test_not_use_snap_cache_in_hybrid_engine() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(0)));
        let (_tmp, mut reader, rx, _) = new_hybrid_engine_reader(
            "test-not-use-snap-cache",
            store_id,
            store_meta.clone(),
            RangeCacheEngineConfig::config_for_test(),
        );

        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };
        let term6 = 6;

        // Register region1
        let pr_ids1 = vec![2, 3, 4];
        let prs1 = new_peers(store_id, pr_ids1.clone());
        prepare_read_delegate(
            store_id,
            1,
            term6,
            pr_ids1,
            epoch13.clone(),
            store_meta.clone(),
        );
        let leader1 = prs1[0].clone();

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(1);
        header.set_peer(leader1);
        header.set_region_epoch(epoch13.clone());
        header.set_term(term6);
        cmd.set_header(header);
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        cmd.set_requests(vec![req].into());
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<HybridEngineTestSnapshot>::new(
            cmd.clone(),
            Callback::read(Box::new(
                move |resp: ReadResponse<HybridEngineTestSnapshot>| {
                    snap_tx.send(resp.snapshot.unwrap()).unwrap();
                },
            )),
        );

        let read_id = Some(ThreadReadId::new());
        // If snap_ctx is None and read_id is Some, it will cache the snapshot.
        reader.propose_raft_command(None, read_id.clone(), task.request, task.callback);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        let _ = snap_rx.recv().unwrap();
        assert!(reader.snap_cache.snapshot.is_some());

        // Release the snapshot and try with snap_ctx
        let (snap_tx, snap_rx) = channel();
        let task = RaftCommand::<HybridEngineTestSnapshot>::new(
            cmd,
            Callback::read(Box::new(
                move |resp: ReadResponse<HybridEngineTestSnapshot>| {
                    snap_tx.send(resp.snapshot.unwrap()).unwrap();
                },
            )),
        );
        reader.release_snapshot_cache();
        let snap_ctx = SnapshotContext {
            read_ts: 15,
            range: None,
        };
        reader.propose_raft_command(Some(snap_ctx), read_id, task.request, task.callback);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        let _ = snap_rx.recv().unwrap();
        assert!(reader.snap_cache.snapshot.is_none());
    }
}
