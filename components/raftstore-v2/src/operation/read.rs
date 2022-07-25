// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use crossbeam::{atomic::AtomicCell, channel::TrySendError};
use engine_traits::{KvEngine, RaftEngine, Snapshot, TabletFactory};
use fail::fail_point;
use kvproto::{
    errorpb,
    kvrpcpb::ExtraOp as TxnExtraOp,
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, ReadIndexResponse, Request, Response},
};
use pd_client::BucketMeta;
use raftstore::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp,
        util::{self, LeaseState, RegionReadProgress, RemoteLease},
        Callback, CasualMessage, CasualRouter, Peer, ProposalRouter, RaftCommand, ReadMetrics,
        ReadResponse, RegionSnapshot, RequestInspector, RequestPolicy, TxnExt, TrackVer,
    },
    Error, Result,
};
use tikv_util::{
    codec::number::decode_u64,
    debug, error,
    lru::LruCache,
    time::{monotonic_raw_now, Instant, ThreadReadId},
};
use time::Timespec;

use crate::tablet::CachedTablet;

pub trait ReadExecutor<E: KvEngine> {
    fn get_tablet(&self) -> E;

    fn get_snapshot(&mut self) -> Arc<E::Snapshot>;

    fn get_value(&self, req: &Request, region: &metapb::Region) -> Result<Response> {
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
    ) -> ReadResponse<E::Snapshot> {
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
                CmdType::Get => match self.get_value(req, region.as_ref()) {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(?e;
                            "failed to execute get command";
                            "region_id" => region.get_id(),
                        );
                        response.response = cmd_resp::new_error(e);
                        return response;
                    }
                },
                CmdType::Snap => {
                    let snapshot =
                        RegionSnapshot::from_snapshot(self.get_snapshot(), region.clone());
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

/// A read only delegate of `Peer`.
#[derive(Clone, Debug)]
pub struct ReadDelegate<E>
where
    E: KvEngine,
{
    pub region: Arc<metapb::Region>,
    pub peer_id: u64,
    pub term: u64,
    pub applied_index_term: u64,
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

    cached_tablet: CachedTablet<E>,
}

impl<E> Drop for ReadDelegate<E>
where
    E: KvEngine,
{
    fn drop(&mut self) {
        // call `inc` to notify the source `ReadDelegate` is dropped
        self.track_ver.inc();
    }
}

impl<E> ReadDelegate<E>
where
    E: KvEngine,
{
    pub fn from_peer<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> ReadDelegate<E> {
        let region = peer.region().clone();
        let region_id = region.get_id();
        let peer_id = peer.peer.get_id();
        let cache_read_id = ThreadReadId::new();
        ReadDelegate {
            region: Arc::new(region),
            peer_id,
            term: peer.term(),
            applied_index_term: peer.get_store().applied_index_term(),
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, peer_id),
            txn_extra_op: peer.txn_extra_op.clone(),
            txn_ext: peer.txn_ext.clone(),
            read_progress: peer.read_progress.clone(),
            pending_remove: false,
            bucket_meta: peer.region_buckets.as_ref().map(|b| b.meta.clone()),
            track_ver: TrackVer::new(),
            cached_tablet: CachedTablet::new(None),
        }
    }

    fn fresh_valid_ts(&mut self) {
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
            Progress::AppliedIndexTerm(applied_index_term) => {
                self.applied_index_term = applied_index_term;
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
    fn maybe_renew_lease_advance<EK: KvEngine>(
        &self,
        router: &dyn CasualRouter<EK>,
        ts: Timespec,
        metrics: &mut ReadMetrics,
    ) {
        if !self
            .leader_lease
            .as_ref()
            .map(|lease| lease.need_renew(ts))
            .unwrap_or(false)
        {
            return;
        }
        metrics.renew_lease_advance += 1;
        let region_id = self.region.get_id();
        if let Err(e) = router.send(region_id, CasualMessage::RenewLease) {
            debug!(
                "failed to send renew lease message";
                "region" => region_id,
                "error" => ?e
            )
        }
    }

    fn is_in_leader_lease(&self, ts: Timespec, metrics: &mut ReadMetrics) -> bool {
        if let Some(ref lease) = self.leader_lease {
            let term = lease.term();
            if term == self.term {
                if lease.inspect(Some(ts)) == LeaseState::Valid {
                    return true;
                } else {
                    metrics.rejected_by_lease_expire += 1;
                    debug!("rejected by lease expire"; "tag" => &self.tag);
                }
            } else {
                metrics.rejected_by_term_mismatch += 1;
                debug!("rejected by term mismatch"; "tag" => &self.tag);
            }
        }

        false
    }

    fn check_stale_read_safe<S: Snapshot>(
        &self,
        read_ts: u64,
        metrics: &mut ReadMetrics,
    ) -> std::result::Result<(), ReadResponse<S>> {
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
        metrics.rejected_by_safe_timestamp += 1;
        let mut response = cmd_resp::new_error(Error::DataIsNotReady {
            region_id: self.region.get_id(),
            peer_id: self.peer_id,
            safe_ts,
        });
        cmd_resp::bind_term(&mut response, self.term);
        Err(ReadResponse {
            response,
            snapshot: None,
            txn_extra_op: TxnExtraOp::Noop,
        })
    }

    /// Used in some external tests.
    pub fn mock(region_id: u64) -> Self {
        let mut region: metapb::Region = Default::default();
        region.set_id(region_id);
        let read_progress = Arc::new(RegionReadProgress::new(&region, 0, 0, "mock".to_owned()));
        let cache_read_id = ThreadReadId::new();
        ReadDelegate {
            region: Arc::new(region),
            peer_id: 1,
            term: 1,
            applied_index_term: 1,
            leader_lease: None,
            last_valid_ts: Timespec::new(0, 0),
            tag: format!("[region {}] {}", region_id, 1),
            txn_extra_op: Default::default(),
            txn_ext: Default::default(),
            read_progress,
            pending_remove: false,
            track_ver: TrackVer::new(),
            bucket_meta: None,
            cached_tablet: CachedTablet::new(None),
        }
    }
}

impl<E> ReadExecutor<E> for ReadDelegate<E>
where
    E: KvEngine,
{
    fn get_tablet(&self) -> E {
        self.cached_tablet.cache().unwrap().clone()
    }

    fn get_snapshot(&mut self) -> Arc<E::Snapshot> {
        Arc::new(self.cached_tablet.latest().unwrap().clone().snapshot())
    }
}

impl<E> Display for ReadDelegate<E>
where
    E: KvEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadDelegate for region {}, \
             leader {} at term {}, applied_index_term {}, has lease {}",
            self.region.get_id(),
            self.peer_id,
            self.term,
            self.applied_index_term,
            self.leader_lease.is_some(),
        )
    }
}

#[derive(Debug)]
pub enum Progress {
    Region(metapb::Region),
    Term(u64),
    AppliedIndexTerm(u64),
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

    pub fn applied_index_term(applied_index_term: u64) -> Progress {
        Progress::AppliedIndexTerm(applied_index_term)
    }

    pub fn leader_lease(lease: RemoteLease) -> Progress {
        Progress::LeaderLease(lease)
    }

    pub fn region_buckets(bucket_meta: Arc<BucketMeta>) -> Progress {
        Progress::RegionBuckets(bucket_meta)
    }
}

pub struct LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
    E: KvEngine,
{
    store_id: Cell<Option<u64>>,
    store_meta: Arc<Mutex<StoreMeta<E>>>,
    metrics: ReadMetrics,
    // region id -> ReadDelegate
    // The use of `Arc` here is a workaround, see the comment at `get_delegate`
    delegates: LruCache<u64, Arc<ReadDelegate<E>>>,
    // A channel to raftstore.
    router: C,
}

struct SnapCache<E>
where
    E: KvEngine,
{
    snap_cache: HashMap<u64, Arc<E::Snapshot>>,
    cache_read_id: ThreadReadId,
}

impl<E> SnapCache<E>
where
    E: KvEngine,
{
    fn new() -> SnapCache<E> {
        let cache_read_id = ThreadReadId::new();
        SnapCache {
            snap_cache: HashMap::new(),
            cache_read_id,
        }
    }

    fn get_snap_from_cache(
        &mut self,
        region_id: u64,
        create_time: &Option<ThreadReadId>,
        read_metrics: &mut ReadMetrics,
    ) -> Option<&Arc<E::Snapshot>> {
        if let Some(ts) = create_time {
            if self.cache_read_id == *ts {
                let snap = self.snap_cache.get(&region_id);
                if let Some(snap) = snap {
                    read_metrics.local_executed_snapshot_cache_hit += 1;
                    return Some(snap);
                }
            } else {
                // We should clear the cache when read id is changed since since all
                // requests in a batch should have the same read id. Which means once the read id
                // is changed, items in the cache are not needed.
                self.snap_cache.clear();
            }
        }
        None
    }

    fn store_snap_in_cache(
        &mut self,
        region_id: u64,
        snap: Arc<E::Snapshot>,
        create_time: ThreadReadId,
    ) {
        self.snap_cache.insert(region_id, snap);
        self.cache_read_id = create_time;
    }
}

struct StoreMeta<E>
where
    E: KvEngine,
{
    pub store_id: Option<u64>,
    pub readers: HashMap<u64, ReadDelegate<E>>,
}

impl<C, E> LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E>,
    E: KvEngine,
{
    pub fn new(
        factory: Arc<dyn TabletFactory<E> + Send + Sync>,
        store_meta: Arc<Mutex<StoreMeta<E>>>,
        router: C,
    ) -> Self {
        let cache_read_id = ThreadReadId::new();
        LocalReader {
            store_meta,
            router,
            store_id: Cell::new(None),
            metrics: Default::default(),
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
                self.metrics.rejected_by_channel_full += 1;
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                cmd = c;
            }
            Err(TrySendError::Disconnected(c)) => {
                self.metrics.rejected_by_no_region += 1;
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

    // Ideally `get_delegate` should return `Option<&ReadDelegate>`, but if so the lifetime of
    // the returned `&ReadDelegate` will bind to `self`, and make it impossible to use `&mut self`
    // while the `&ReadDelegate` is alive, a better choice is use `Rc` but `LocalReader: Send` will be
    // violated, which is required by `LocalReadRouter: Send`, use `Arc` will introduce extra cost but
    // make the logic clear
    fn get_delegate(&mut self, region_id: u64) -> Option<Arc<ReadDelegate<E>>> {
        let rd = match self.delegates.get(&region_id) {
            // The local `ReadDelegate` is up to date
            Some(d) if !d.track_ver.any_new() => Some(Arc::clone(d)),
            _ => {
                debug!("update local read delegate"; "region_id" => region_id);
                self.metrics.rejected_by_cache_miss += 1;

                let (meta_len, meta_reader) = {
                    let meta = self.store_meta.lock().unwrap();
                    (
                        meta.readers.len(),
                        meta.readers.get(&region_id).cloned().map(Arc::new),
                    )
                };

                // Remove the stale delegate
                self.delegates.remove(&region_id);
                self.delegates.resize(meta_len);
                match meta_reader {
                    Some(reader) => {
                        self.delegates.insert(region_id, Arc::clone(&reader));
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
    ) -> Result<Option<(Arc<ReadDelegate<E>>, RequestPolicy)>> {
        // Check store id.
        if self.store_id.get().is_none() {
            let store_id = self.store_meta.lock().unwrap().store_id;
            self.store_id.set(store_id);
        }
        let store_id = self.store_id.get().unwrap();

        if let Err(e) = util::check_store_id(req, store_id) {
            self.metrics.rejected_by_store_id_mismatch += 1;
            debug!("rejected by store id not match"; "err" => %e);
            return Err(e);
        }

        // Check region id.
        let region_id = req.get_header().get_region_id();
        let delegate = match self.get_delegate(region_id) {
            Some(d) => d,
            None => {
                self.metrics.rejected_by_no_region += 1;
                debug!("rejected by no region"; "region_id" => region_id);
                return Ok(None);
            }
        };

        fail_point!("localreader_on_find_delegate");

        // Check peer id.
        if let Err(e) = util::check_peer_id(req, delegate.peer_id) {
            self.metrics.rejected_by_peer_id_mismatch += 1;
            return Err(e);
        }

        // Check term.
        if let Err(e) = util::check_term(req, delegate.term) {
            debug!(
                "check term";
                "delegate_term" => delegate.term,
                "header_term" => req.get_header().get_term(),
            );
            self.metrics.rejected_by_term_mismatch += 1;
            return Err(e);
        }

        // Check region epoch.
        if util::check_region_epoch(req, &delegate.region, false).is_err() {
            self.metrics.rejected_by_epoch += 1;
            // Stale epoch, redirect it to raftstore to get the latest region.
            debug!("rejected by epoch not match"; "tag" => &delegate.tag);
            return Ok(None);
        }

        let mut inspector = Inspector {
            delegate: &delegate,
            metrics: &mut self.metrics,
        };
        match inspector.inspect(req) {
            Ok(RequestPolicy::ReadLocal) => Ok(Some((delegate, RequestPolicy::ReadLocal))),
            Ok(RequestPolicy::StaleRead) => Ok(Some((delegate, RequestPolicy::StaleRead))),
            // It can not handle other policies.
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn propose_raft_command(
        &mut self,
        mut read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        match self.pre_propose_raft_command(&req) {
            Ok(Some((delegate, policy))) => {
                let mut response = match policy {
                    // Leader can read local if and only if it is in lease.
                    RequestPolicy::ReadLocal => {
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
                        if !delegate.is_in_leader_lease(snapshot_ts, &mut self.metrics) {
                            // Forward to raftstore.
                            self.redirect(RaftCommand::new(req, cb));
                            return;
                        }
                        let response = delegate.execute(&req, &delegate.region, None);
                        // Try renew lease in advance
                        delegate.maybe_renew_lease_advance(
                            &self.router,
                            snapshot_ts,
                            &mut self.metrics,
                        );
                        response
                    }
                    // Replica can serve stale read if and only if its `safe_ts` >= `read_ts`
                    RequestPolicy::StaleRead => {
                        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
                        assert!(read_ts > 0);
                        if let Err(resp) =
                            delegate.check_stale_read_safe(read_ts, &mut self.metrics)
                        {
                            cb.invoke_read(resp);
                            return;
                        }

                        // Getting the snapshot
                        let response = delegate.execute(&req, &delegate.region, None);

                        // Double check in case `safe_ts` change after the first check and before getting snapshot
                        if let Err(resp) =
                            delegate.check_stale_read_safe(read_ts, &mut self.metrics)
                        {
                            cb.invoke_read(resp);
                            return;
                        }
                        self.metrics.local_executed_stale_read_requests += 1;
                        response
                    }
                    _ => unreachable!(),
                };
                cmd_resp::bind_term(&mut response.response, delegate.term);
                if let Some(snap) = response.snapshot.as_mut() {
                    snap.txn_ext = Some(delegate.txn_ext.clone());
                    snap.bucket_meta = delegate.bucket_meta.clone();
                }
                response.txn_extra_op = delegate.txn_extra_op.load();
                cb.invoke_read(response);
            }
            // Forward to raftstore.
            Ok(None) => self.redirect(RaftCommand::new(req, cb)),
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self.delegates.get(&req.get_header().get_region_id()) {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                cb.invoke_read(ReadResponse {
                    response,
                    snapshot: None,
                    txn_extra_op: TxnExtraOp::Noop,
                });
            }
        }
    }

    /// If read requests are received at the same RPC request, we can create one snapshot for all
    /// of them and check whether the time when the snapshot was created is in lease. We use
    /// ThreadReadId to figure out whether this RaftCommand comes from the same RPC request with
    /// the last RaftCommand which left a snapshot cached in LocalReader. ThreadReadId is composed
    /// by thread_id and a thread_local incremental sequence.
    #[inline]
    pub fn read(
        &mut self,
        read_id: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<E::Snapshot>,
    ) {
        self.propose_raft_command(read_id, req, cb);
        self.metrics.maybe_flush();
    }

    /// Now, We don't have snapshot cache for multi-rocks version, so we do nothing here.
    pub fn release_snapshot_cache(&mut self) {}
}

impl<C, E> Clone for LocalReader<C, E>
where
    C: ProposalRouter<E::Snapshot> + CasualRouter<E> + Clone,
    E: KvEngine,
{
    fn clone(&self) -> Self {
        LocalReader {
            store_meta: self.store_meta.clone(),
            router: self.router.clone(),
            store_id: self.store_id.clone(),
            metrics: Default::default(),
            delegates: LruCache::with_capacity_and_sample(0, 7),
        }
    }
}

struct Inspector<'r, 'm, E>
where
    E: KvEngine,
{
    delegate: &'r ReadDelegate<E>,
    metrics: &'m mut ReadMetrics,
}

impl<'r, 'm, E> RequestInspector for Inspector<'r, 'm, E>
where
    E: KvEngine,
{
    fn has_applied_to_current_term(&mut self) -> bool {
        if self.delegate.applied_index_term == self.delegate.term {
            true
        } else {
            debug!(
                "rejected by term check";
                "tag" => &self.delegate.tag,
                "applied_index_term" => self.delegate.applied_index_term,
                "delegate_term" => ?self.delegate.term,
            );

            // only for metric.
            self.metrics.rejected_by_applied_term += 1;
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
            self.metrics.rejected_by_no_lease += 1;
            LeaseState::Expired
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc::*, thread};

    use crossbeam::channel::TrySendError;
    use engine_test::kv::{KvTestEngine, KvTestSnapshot};
    use engine_traits::ALL_CFS;
    use kvproto::raft_cmdpb::*;
    use tempfile::{Builder, TempDir};
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};

    use super::*;

    #[test]
    fn it_works() {}
}
