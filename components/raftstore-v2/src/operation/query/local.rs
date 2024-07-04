// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    num::NonZeroU64,
    ops::Deref,
    sync::{atomic, Arc, Mutex},
};

use batch_system::Router;
use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use futures::Future;
use kvproto::{
    errorpb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse},
};
use raftstore::{
    errors::RAFTSTORE_IS_BUSY,
    store::{
        cmd_resp,
        util::LeaseState,
        worker_metrics::{self, TLS_LOCAL_READ_METRICS},
        LocalReaderCore, ReadDelegate, ReadExecutorProvider, RegionSnapshot,
    },
    Result,
};
use slog::{debug, Logger};
use tikv_util::{box_err, codec::number::decode_u64, time::monotonic_raw_now, Either};
use time::Timespec;
use tracker::{get_tls_tracker_token, GLOBAL_TRACKERS};
use txn_types::WriteBatchFlags;

use crate::{
    fsm::StoreMeta,
    router::{PeerMsg, QueryResult},
    StoreRouter,
};

pub trait MsgRouter: Clone + Send + 'static {
    fn send(&self, addr: u64, msg: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>>;
}

impl<EK, ER> MsgRouter for StoreRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn send(&self, addr: u64, msg: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>> {
        Router::send(self, addr, msg)
    }
}

pub type ReadDelegatePair<EK> = (ReadDelegate, SharedReadTablet<EK>);

/// A share struct for local reader.
///
/// Though it looks like `CachedTablet`, but there are subtle differences.
/// 1. `CachedTablet` always hold the latest version of the tablet. But
/// `SharedReadTablet` should only hold the tablet that matches epoch. So it
/// will be updated only when the epoch is updated.
/// 2. `SharedReadTablet` should always hold a tablet and the same tablet. If
/// tablet is taken, then it should be considered as stale and should check
/// again epoch to load the new `SharedReadTablet`.
/// 3. `SharedReadTablet` may be cloned into thread local. So its cache should
/// be released as soon as possible, so there should be no strong reference
/// that prevents tablet from being dropped after it's marked as stale by other
/// threads.
pub struct SharedReadTablet<EK> {
    tablet: Arc<Mutex<Option<EK>>>,
    cache: Option<EK>,
    source: bool,
}

impl<EK> SharedReadTablet<EK> {
    pub fn new(tablet: EK) -> Self {
        Self {
            tablet: Arc::new(Mutex::new(Some(tablet))),
            cache: None,
            source: true,
        }
    }

    /// Should call `fill_cache` first.
    pub fn cache(&self) -> &EK {
        self.cache.as_ref().unwrap()
    }

    pub fn fill_cache(&mut self) -> bool
    where
        EK: Clone,
    {
        self.cache = self.tablet.lock().unwrap().clone();
        self.cache.is_some()
    }

    pub fn release(&mut self) {
        self.cache = None;
    }
}

impl<EK> Clone for SharedReadTablet<EK> {
    fn clone(&self) -> Self {
        Self {
            tablet: Arc::clone(&self.tablet),
            cache: None,
            source: false,
        }
    }
}

impl<EK> Drop for SharedReadTablet<EK> {
    fn drop(&mut self) {
        if self.source {
            self.tablet.lock().unwrap().take();
        }
    }
}

enum ReadResult<T, E = crate::Error> {
    Ok(T),
    Redirect,
    RetryForStaleDelegate,
    Err(E),
}

fn fail_resp(msg: String) -> RaftCmdResponse {
    let mut err = errorpb::Error::default();
    err.set_message(msg);
    let mut resp = RaftCmdResponse::default();
    resp.mut_header().set_error(err);
    resp
}

#[derive(Clone)]
pub struct LocalReader<E, C>
where
    E: KvEngine,
    C: MsgRouter,
{
    local_reader: LocalReaderCore<CachedReadDelegate<E>, StoreMetaDelegate<E>>,
    router: C,

    logger: Logger,
}

impl<E, C> LocalReader<E, C>
where
    E: KvEngine,
    C: MsgRouter,
{
    pub fn new(store_meta: Arc<Mutex<StoreMeta<E>>>, router: C, logger: Logger) -> Self {
        Self {
            local_reader: LocalReaderCore::new(StoreMetaDelegate::new(store_meta)),
            router,
            logger,
        }
    }

    pub fn store_meta(&self) -> &Arc<Mutex<StoreMeta<E>>> {
        &self.local_reader.store_meta().store_meta
    }

    fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> ReadResult<(CachedReadDelegate<E>, ReadRequestPolicy)> {
        let mut delegate = match self.local_reader.validate_request(req) {
            Ok(Some(delegate)) => delegate,
            Ok(None) => return ReadResult::Redirect,
            Err(e) => return ReadResult::Err(e),
        };

        if !delegate.cached_tablet.fill_cache() {
            return ReadResult::RetryForStaleDelegate;
        }
        let mut inspector = SnapRequestInspector {
            delegate: &delegate,
            logger: &self.logger,
        };
        match inspector.inspect(req) {
            Ok(ReadRequestPolicy::ReadLocal) => {
                ReadResult::Ok((delegate, ReadRequestPolicy::ReadLocal))
            }
            Ok(ReadRequestPolicy::StaleRead) => {
                ReadResult::Ok((delegate, ReadRequestPolicy::StaleRead))
            }
            // TODO: we should only abort when lease expires. For other cases we should retry
            // infinitely.
            Ok(ReadRequestPolicy::ReadIndex) => {
                if req.get_header().get_replica_read() {
                    ReadResult::Ok((delegate, ReadRequestPolicy::ReadIndex))
                } else {
                    ReadResult::Redirect
                }
            }
            Err(e) => ReadResult::Err(e),
        }
    }

    fn try_get_snapshot(
        &mut self,
        req: &RaftCmdRequest,
        has_read_index_success: bool,
    ) -> ReadResult<RegionSnapshot<E::Snapshot>, RaftCmdResponse> {
        match self.pre_propose_raft_command(req) {
            ReadResult::Ok((mut delegate, policy)) => {
                let mut snap = match policy {
                    ReadRequestPolicy::ReadLocal => {
                        let region = Arc::clone(&delegate.region);
                        let snap = RegionSnapshot::from_snapshot(
                            Arc::new(delegate.cached_tablet.cache().snapshot(None)),
                            region,
                        );

                        // Ensures the snapshot is acquired before getting the time
                        atomic::fence(atomic::Ordering::Release);
                        let snapshot_ts = monotonic_raw_now();

                        if !delegate.is_in_leader_lease(snapshot_ts) && !has_read_index_success {
                            // Redirect if it's not in lease and it has not finish read index.
                            return ReadResult::Redirect;
                        }

                        TLS_LOCAL_READ_METRICS
                            .with(|m| m.borrow_mut().local_executed_requests.inc());

                        if !has_read_index_success {
                            // Try renew lease in advance only if it has not read index before.
                            // Because a successful read index has already renewed lease.
                            self.maybe_renew_lease_in_advance(&delegate, req, snapshot_ts);
                        }
                        snap
                    }
                    ReadRequestPolicy::StaleRead => {
                        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
                        if let Err(e) = delegate.check_stale_read_safe(read_ts) {
                            return ReadResult::Err(e);
                        }

                        let region = Arc::clone(&delegate.region);
                        let snap = RegionSnapshot::from_snapshot(
                            Arc::new(delegate.cached_tablet.cache().snapshot(None)),
                            region,
                        );

                        TLS_LOCAL_READ_METRICS
                            .with(|m| m.borrow_mut().local_executed_requests.inc());

                        if let Err(e) = delegate.check_stale_read_safe(read_ts) {
                            return ReadResult::Err(e);
                        }

                        TLS_LOCAL_READ_METRICS
                            .with(|m| m.borrow_mut().local_executed_stale_read_requests.inc());
                        snap
                    }
                    ReadRequestPolicy::ReadIndex => {
                        // ReadIndex is returned only for replica read.
                        if !has_read_index_success {
                            // It needs to read index before getting snapshot.
                            return ReadResult::Redirect;
                        }

                        let region = Arc::clone(&delegate.region);
                        let snap = RegionSnapshot::from_snapshot(
                            Arc::new(delegate.cached_tablet.cache().snapshot(None)),
                            region,
                        );

                        TLS_LOCAL_READ_METRICS.with(|m| {
                            m.borrow_mut().local_executed_requests.inc();
                            m.borrow_mut().local_executed_replica_read_requests.inc()
                        });

                        snap
                    }
                };

                snap.set_from_v2();
                snap.txn_ext = Some(delegate.txn_ext.clone());
                snap.term = NonZeroU64::new(delegate.term);
                snap.txn_extra_op = delegate.txn_extra_op.load();
                snap.bucket_meta = delegate.bucket_meta.clone();

                delegate.cached_tablet.release();

                ReadResult::Ok(snap)
            }
            ReadResult::Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self
                    .local_reader
                    .delegates
                    .get(&req.get_header().get_region_id())
                {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                ReadResult::Err(response)
            }
            ReadResult::Redirect => ReadResult::Redirect,
            ReadResult::RetryForStaleDelegate => ReadResult::RetryForStaleDelegate,
        }
    }

    pub fn snapshot(
        &mut self,
        mut req: RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<RegionSnapshot<E::Snapshot>, RaftCmdResponse>>
    + Send
    + 'static {
        let region_id = req.header.get_ref().region_id;
        let mut tried_cnt = 0;
        let res = loop {
            let res = self.try_get_snapshot(&req, false /* after_read_index */);
            match res {
                ReadResult::Ok(snap) => break Either::Left(Ok(snap)),
                ReadResult::Err(e) => break Either::Left(Err(e)),
                ReadResult::Redirect => {
                    break Either::Right((self.try_to_renew_lease(region_id, &req), self.clone()));
                }
                ReadResult::RetryForStaleDelegate => {
                    tried_cnt += 1;
                    if tried_cnt < 10 {
                        continue;
                    }
                    break Either::Left(Err(fail_resp(format!(
                        "internal error: failed to get valid dalegate for {}",
                        region_id
                    ))));
                }
            }
        };

        worker_metrics::maybe_tls_local_read_metrics_flush();

        async move {
            let (mut fut, mut reader) = match res {
                Either::Left(Ok(snap)) => {
                    GLOBAL_TRACKERS.with_tracker(get_tls_tracker_token(), |t| {
                        t.metrics.local_read = true;
                    });
                    return Ok(snap);
                }
                Either::Left(Err(e)) => return Err(e),
                Either::Right((fut, reader)) => (fut, reader),
            };

            let mut tried_cnt = 0;
            loop {
                match fut.await? {
                    Some(query_res) => {
                        if query_res.read().is_none() {
                            let QueryResult::Response(res) = query_res else {
                                unreachable!()
                            };
                            // Get an error explicitly in header,
                            // or leader reports KeyIsLocked error via read index.
                            assert!(
                                res.get_header().has_error()
                                    || res
                                        .get_responses()
                                        .first()
                                        .map_or(false, |r| r.get_read_index().has_locked()),
                                "{:?}",
                                res
                            );
                            return Err(res);
                        }
                    }
                    None => {
                        return Err(fail_resp(format!(
                            "internal error: failed to extend lease: canceled: {}",
                            region_id
                        )));
                    }
                }

                // If query successful, try again.
                req.mut_header().set_read_quorum(false);
                loop {
                    let r = reader.try_get_snapshot(&req, true /* after_read_index */);
                    match r {
                        ReadResult::Ok(snap) => return Ok(snap),
                        ReadResult::Err(e) => return Err(e),
                        ReadResult::Redirect => {
                            tried_cnt += 1;
                            if tried_cnt < 10 {
                                fut = reader.try_to_renew_lease(region_id, &req);
                                break;
                            }
                            return Err(fail_resp(format!(
                                "internal error: can't handle msg in local reader for {}",
                                region_id
                            )));
                        }
                        ReadResult::RetryForStaleDelegate => {
                            tried_cnt += 1;
                            if tried_cnt < 10 {
                                continue;
                            }
                            return Err(fail_resp(format!(
                                "internal error: failed to get valid dalegate for {}",
                                region_id
                            )));
                        }
                    }
                }
            }
        }
    }

    // try to renew the lease by sending read query where the reading process may
    // renew the lease
    fn try_to_renew_lease(
        &self,
        region_id: u64,
        req: &RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<Option<QueryResult>, RaftCmdResponse>> + 'static
    {
        let mut req = req.clone();
        // Remote lease is updated step by step. It's possible local reader expires
        // while the raftstore doesn't. So we need to trigger an update
        // explicitly. TODO: find a way to reduce the triggered heartbeats.
        req.mut_header().set_read_quorum(true);
        let (msg, sub) = PeerMsg::raft_query(req);
        let res = match MsgRouter::send(&self.router, region_id, msg) {
            Ok(()) => Ok(sub),
            Err(TrySendError::Full(_)) => {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.channel_full.inc());
                let mut err = errorpb::Error::default();
                err.set_message(RAFTSTORE_IS_BUSY.to_owned());
                err.mut_server_is_busy()
                    .set_reason(RAFTSTORE_IS_BUSY.to_owned());
                Err(err)
            }
            Err(TrySendError::Disconnected(_)) => {
                TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_region.inc());
                let mut err = errorpb::Error::default();
                err.set_message(format!("region {} is missing", region_id));
                err.mut_region_not_found().set_region_id(region_id);
                Err(err)
            }
        };

        async move {
            match res {
                Ok(sub) => Ok(sub.result().await),
                Err(e) => {
                    let mut resp = RaftCmdResponse::default();
                    resp.mut_header().set_error(e);
                    Err(resp)
                }
            }
        }
    }

    // If the remote lease will be expired in near future send message
    // to `raftstore` to renew it
    fn maybe_renew_lease_in_advance(
        &self,
        delegate: &ReadDelegate,
        req: &RaftCmdRequest,
        ts: Timespec,
    ) {
        if !delegate.need_renew_lease(ts) {
            return;
        }

        let region_id = req.header.get_ref().region_id;
        TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().renew_lease_advance.inc());
        // Send a read query which may renew the lease
        let msg = PeerMsg::raft_query(req.clone()).0;
        if let Err(e) = MsgRouter::send(&self.router, region_id, msg) {
            debug!(
                self.logger,
                "failed to send query for trying to renew lease";
                "region" => region_id,
                "error" => ?e
            )
        }
    }
}

/// CachedReadDelegate is a wrapper the ReadDelegate and CachedTablet.
/// CachedTablet can fetch the latest tablet of this ReadDelegate's region. The
/// main purpose of this wrapping is to implement ReadExecutor where the latest
/// tablet is needed.
pub struct CachedReadDelegate<E>
where
    E: KvEngine,
{
    // The reason for this to be Arc, see the comment on get_delegate in
    // raftstore/src/store/worker/read.rs
    delegate: Arc<ReadDelegate>,
    cached_tablet: SharedReadTablet<E>,
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
            cached_tablet: self.cached_tablet.clone(),
        }
    }
}

#[derive(Clone)]
struct StoreMetaDelegate<E>
where
    E: KvEngine,
{
    store_meta: Arc<Mutex<StoreMeta<E>>>,
}

impl<E> StoreMetaDelegate<E>
where
    E: KvEngine,
{
    pub fn new(store_meta: Arc<Mutex<StoreMeta<E>>>) -> StoreMetaDelegate<E> {
        StoreMetaDelegate { store_meta }
    }
}

impl<E> ReadExecutorProvider for StoreMetaDelegate<E>
where
    E: KvEngine,
{
    type Executor = CachedReadDelegate<E>;
    type StoreMeta = Arc<Mutex<StoreMeta<E>>>;

    fn store_id(&self) -> Option<u64> {
        Some(self.store_meta.as_ref().lock().unwrap().store_id)
    }

    /// get the ReadDelegate with region_id and the number of delegates in the
    /// StoreMeta
    fn get_executor_and_len(&self, region_id: u64) -> (usize, Option<Self::Executor>) {
        let meta = self.store_meta.as_ref().lock().unwrap();
        let reader = meta.readers.get(&region_id).cloned();
        if let Some((reader, read_tablet)) = reader {
            // If reader is not None, cache must not be None.
            return (
                meta.readers.len(),
                Some(CachedReadDelegate {
                    delegate: Arc::new(reader),
                    cached_tablet: read_tablet,
                }),
            );
        }
        (meta.readers.len(), None)
    }
    fn locate_key(&self, _key: &[u8]) -> Option<u64> {
        return None;
    }
}

enum ReadRequestPolicy {
    StaleRead,
    ReadLocal,
    ReadIndex,
}

struct SnapRequestInspector<'r> {
    delegate: &'r ReadDelegate,
    logger: &'r Logger,
}

impl<'r> SnapRequestInspector<'r> {
    fn inspect(&mut self, req: &RaftCmdRequest) -> Result<ReadRequestPolicy> {
        assert!(!req.has_admin_request());
        if req.get_requests().len() != 1
            || req.get_requests().first().unwrap().get_cmd_type() != CmdType::Snap
        {
            return Err(box_err!(
                "LocalReader can only serve for exactly one Snap request"
            ));
        }

        fail::fail_point!("perform_read_index", |_| Ok(ReadRequestPolicy::ReadIndex));

        fail::fail_point!("perform_read_local", |_| Ok(ReadRequestPolicy::ReadLocal));

        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Ok(ReadRequestPolicy::StaleRead);
        }

        if req.get_header().get_read_quorum() {
            return Ok(ReadRequestPolicy::ReadIndex);
        }

        // If applied index's term differs from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return Ok(ReadRequestPolicy::ReadIndex);
        }

        // Local read should be performed, if and only if leader is in lease.
        match self.inspect_lease() {
            LeaseState::Valid => Ok(ReadRequestPolicy::ReadLocal),
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(ReadRequestPolicy::ReadIndex)
            }
        }
    }

    fn has_applied_to_current_term(&mut self) -> bool {
        if self.delegate.applied_term == self.delegate.term {
            true
        } else {
            debug!(
                self.logger,
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
            debug!(self.logger, "rejected by leader lease"; "tag" => &self.delegate.tag);
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow_mut().reject_reason.no_lease.inc());
            LeaseState::Expired
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        sync::mpsc::*,
        thread::{self, JoinHandle},
    };

    use collections::HashSet;
    use crossbeam::{atomic::AtomicCell, channel::TrySendError};
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, TestTabletFactory},
    };
    use engine_traits::{MiscExt, SyncMutable, TabletContext, TabletRegistry, DATA_CFS};
    use futures::executor::block_on;
    use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, metapb, raft_cmdpb::*};
    use pd_client::BucketMeta;
    use raftstore::store::{
        util::Lease, worker_metrics::TLS_LOCAL_READ_METRICS, ReadCallback, ReadProgress,
        RegionReadProgress, TrackVer, TxnExt,
    };
    use slog::o;
    use tempfile::Builder;
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};
    use time::Duration;
    use txn_types::WriteBatchFlags;

    use super::*;
    use crate::router::{QueryResult, ReadResponse};

    #[derive(Clone)]
    struct MockRouter {
        p_router: SyncSender<(u64, PeerMsg)>,
        addresses: Arc<Mutex<HashSet<u64>>>,
    }

    impl MockRouter {
        fn new(addresses: Arc<Mutex<HashSet<u64>>>) -> (MockRouter, Receiver<(u64, PeerMsg)>) {
            let (p_ch, p_rx) = sync_channel(1);
            (
                MockRouter {
                    p_router: p_ch,
                    addresses,
                },
                p_rx,
            )
        }
    }

    impl MsgRouter for MockRouter {
        fn send(&self, addr: u64, cmd: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>> {
            if !self.addresses.lock().unwrap().contains(&addr) {
                return Err(TrySendError::Disconnected(cmd));
            }
            self.p_router.send((addr, cmd)).unwrap();
            Ok(())
        }
    }

    #[allow(clippy::type_complexity)]
    fn new_reader(
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta<KvTestEngine>>>,
        addresses: Arc<Mutex<HashSet<u64>>>,
    ) -> (
        LocalReader<KvTestEngine, MockRouter>,
        Receiver<(u64, PeerMsg)>,
    ) {
        let (ch, rx) = MockRouter::new(addresses);
        let mut reader = LocalReader::new(
            store_meta,
            ch,
            Logger::root(slog::Discard, o!("key1" => "value1")),
        );
        reader.local_reader.store_id = Cell::new(Some(store_id));
        (reader, rx)
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

    // It mocks that local reader communications with raftstore.
    // mix_rx receives a closure, msg receiver, and sender of the msg receiver
    // - closure: do some update such as renew lease or something which we could do
    //   in real raftstore
    // - msg receiver: receives the msg from local reader
    // - sender of the msg receiver: send the msg receiver out of the thread so that
    //   we can use it again.
    fn mock_raftstore(
        mix_rx: Receiver<(
            Box<dyn FnOnce() + Send + 'static>,
            Receiver<(u64, PeerMsg)>,
            SyncSender<Receiver<(u64, PeerMsg)>>,
        )>,
    ) -> JoinHandle<()> {
        thread::spawn(move || {
            while let Ok((f, rx, ch_tx)) = mix_rx.recv() {
                // Receives msg from local reader
                let (_, msg) = rx.recv().unwrap();
                f();

                match msg {
                    // send the result back to local reader
                    PeerMsg::RaftQuery(query) => {
                        assert!(query.request.get_header().get_read_quorum());
                        ReadCallback::set_result(
                            query.ch,
                            QueryResult::Read(ReadResponse {
                                read_index: 0,
                                txn_extra_op: Default::default(),
                            }),
                        )
                    }
                    _ => unreachable!(),
                }
                ch_tx.send(rx).unwrap();
            }
        })
    }

    #[test]
    fn test_read() {
        let store_id = 1;

        // Building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = DATA_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let factory = Box::new(TestTabletFactory::new(ops, cf_opts));
        let reg = TabletRegistry::new(factory, path.path()).unwrap();

        let store_meta = Arc::new(Mutex::new(StoreMeta::new(store_id)));
        let addresses: Arc<Mutex<HashSet<u64>>> = Arc::default();
        let (mut reader, mut rx) = new_reader(store_id, store_meta.clone(), addresses.clone());
        let (mix_tx, mix_rx) = sync_channel(1);
        let handler = mock_raftstore(mix_rx);

        let mut region1 = metapb::Region::default();
        region1.set_id(1);
        let prs = new_peers(store_id, vec![1, 2, 3]);
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
        let mut lease = Lease::new(Duration::seconds(10), Duration::milliseconds(2500));
        let read_progress = Arc::new(RegionReadProgress::new(&region1, 1, 1, 1));

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

        // The region is not register yet.
        let res = block_on(reader.snapshot(cmd.clone())).unwrap_err();
        assert!(
            res.header
                .as_ref()
                .unwrap()
                .get_error()
                .has_region_not_found()
        );
        // No msg will ben sent
        rx.try_recv().unwrap_err();
        // It will be rejected first when processing local, and then rejected when
        // trying to forward to raftstore.
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.no_region.get()),
            2
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            1
        );
        assert!(reader.local_reader.delegates.get(&1).is_none());

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        let txn_ext = Arc::new(TxnExt::default());
        let bucket_meta = Arc::new(BucketMeta::default());
        {
            let mut meta = store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let read_delegate = ReadDelegate {
                tag: String::new(),
                region: Arc::new(region1.clone()),
                peer_id: 1,
                term: term6,
                applied_term: term6 - 1,
                leader_lease: Some(remote),
                last_valid_ts: Timespec::new(0, 0),
                txn_extra_op: Arc::new(AtomicCell::new(TxnExtraOp::default())),
                txn_ext: txn_ext.clone(),
                read_progress: read_progress.clone(),
                pending_remove: false,
                wait_data: false,
                track_ver: TrackVer::new(),
                bucket_meta: Some(bucket_meta.clone()),
            };
            // create tablet with region_id 1 and prepare some data
            let ctx = TabletContext::new(&region1, Some(10));
            let mut tablet = reg.load(ctx, true).unwrap();
            let shared = SharedReadTablet::new(tablet.latest().unwrap().clone());
            meta.readers.insert(1, (read_delegate, shared));
        }

        let (ch_tx, ch_rx) = sync_channel(1);

        // Case: Applied term not match
        let store_meta_clone = store_meta.clone();
        // Send what we want to do to mock raftstore
        mix_tx
            .send((
                Box::new(move || {
                    let mut meta = store_meta_clone.lock().unwrap();
                    meta.readers
                        .get_mut(&1)
                        .unwrap()
                        .0
                        .update(ReadProgress::applied_term(term6));
                }),
                rx,
                ch_tx.clone(),
            ))
            .unwrap();
        // The first try will be rejected due to unmatched applied term but after update
        // the applied term by the above thread, the snapshot will be acquired by
        // retrying.
        addresses.lock().unwrap().insert(1);
        let snap = block_on(reader.snapshot(cmd.clone())).unwrap();
        assert!(Arc::ptr_eq(snap.txn_ext.as_ref().unwrap(), &txn_ext));
        assert!(Arc::ptr_eq(
            snap.bucket_meta.as_ref().unwrap(),
            &bucket_meta
        ));
        assert_eq!(*snap.get_region(), region1);
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            3
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.applied_term.get()),
            1
        );
        rx = ch_rx.recv().unwrap();

        // Case: Expire lease to make the local reader lease check fail.
        lease.expire_remote_lease();
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        let meta = store_meta.clone();
        // Send what we want to do to mock raftstore
        mix_tx
            .send((
                Box::new(move || {
                    let mut meta = meta.lock().unwrap();
                    meta.readers
                        .get_mut(&1)
                        .unwrap()
                        .0
                        .update(ReadProgress::set_leader_lease(remote));
                }),
                rx,
                ch_tx.clone(),
            ))
            .unwrap();
        block_on(reader.snapshot(cmd.clone())).unwrap();
        // Updating lease makes cache miss. And because the cache is updated on cloned
        // copy, so the old cache will still need to be updated again.
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            5
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.lease_expire.get()),
            1
        );
        rx = ch_rx.recv().unwrap();

        // Case: Tablet miss should triger retry.
        {
            let ctx = TabletContext::new(&region1, Some(15));
            let mut tablet = reg.load(ctx, true).unwrap();
            let shared = SharedReadTablet::new(tablet.latest().unwrap().clone());
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().1 = shared;
        }
        block_on(reader.snapshot(cmd.clone())).unwrap();
        // Tablet miss should trigger reload tablet, so cache miss should increase.
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.cache_miss.get()),
            6
        );
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.lease_expire.get()),
            1
        );

        // Case: Read quorum.
        let mut cmd_read_quorum = cmd.clone();
        cmd_read_quorum.mut_header().set_read_quorum(true);
        mix_tx.send((Box::new(move || {}), rx, ch_tx)).unwrap();
        let _ = block_on(reader.snapshot(cmd_read_quorum.clone())).unwrap();
        ch_rx.recv().unwrap();

        // Case: Stale read
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
        let res = block_on(reader.snapshot(cmd.clone())).unwrap_err();
        assert!(res.get_header().get_error().has_data_is_not_ready());
        assert_eq!(
            TLS_LOCAL_READ_METRICS.with(|m| m.borrow().reject_reason.safe_ts.get()),
            1
        );
        read_progress.update_safe_ts(1, 2);
        assert_eq!(read_progress.safe_ts(), 2);
        let snap = block_on(reader.snapshot(cmd.clone())).unwrap();
        assert_eq!(*snap.get_region(), region1);
        assert_eq!(snap.term, NonZeroU64::new(term6));

        drop(mix_tx);
        handler.join().unwrap();
    }

    #[test]
    fn test_read_delegate() {
        // Building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = DATA_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let factory = Box::new(TestTabletFactory::new(ops, cf_opts));
        let reg = TabletRegistry::new(factory, path.path()).unwrap();

        let store_meta = StoreMetaDelegate::new(Arc::new(Mutex::new(StoreMeta::new(1))));

        let tablet1;
        let tablet2;
        {
            let mut meta = store_meta.store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let read_delegate = ReadDelegate::mock(1);

            // create tablet with region_id 1 and prepare some data
            let mut ctx = TabletContext::with_infinite_region(1, Some(10));
            reg.load(ctx, true).unwrap();
            tablet1 = reg.get(1).unwrap().latest().unwrap().clone();
            tablet1.put(b"a1", b"val1").unwrap();
            let shared1 = SharedReadTablet::new(tablet1.clone());
            meta.readers.insert(1, (read_delegate, shared1));

            // Create read_delegate with region id 2
            let read_delegate = ReadDelegate::mock(2);

            // create tablet with region_id 1 and prepare some data
            ctx = TabletContext::with_infinite_region(2, Some(10));
            reg.load(ctx, true).unwrap();
            tablet2 = reg.get(2).unwrap().latest().unwrap().clone();
            tablet2.put(b"a2", b"val2").unwrap();
            let shared2 = SharedReadTablet::new(tablet2.clone());
            meta.readers.insert(2, (read_delegate, shared2));
        }

        let (_, delegate) = store_meta.get_executor_and_len(1);
        let mut delegate = delegate.unwrap();
        assert!(delegate.cached_tablet.fill_cache());
        let tablet = delegate.cached_tablet.cache();
        assert_eq!(tablet1.path(), tablet.path());
        let path1 = tablet.path().to_owned();
        delegate.cached_tablet.release();

        let (_, delegate) = store_meta.get_executor_and_len(2);
        let mut delegate = delegate.unwrap();
        assert!(delegate.cached_tablet.fill_cache());
        let tablet = delegate.cached_tablet.cache();
        assert_eq!(tablet2.path(), tablet.path());

        assert!(KvTestEngine::locked(&path1).unwrap());
        drop(tablet1);
        drop(reg);
        assert!(KvTestEngine::locked(&path1).unwrap());
        store_meta.store_meta.lock().unwrap().readers.remove(&1);
        assert!(!KvTestEngine::locked(&path1).unwrap());
    }
}
