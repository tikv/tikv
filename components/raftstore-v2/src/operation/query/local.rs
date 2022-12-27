// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    ops::Deref,
    sync::{atomic, Arc, Mutex},
};

use batch_system::Router;
use crossbeam::channel::TrySendError;
use engine_traits::{CachedTablet, KvEngine, RaftEngine, TabletRegistry};
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
        LocalReadContext, LocalReaderCore, ReadDelegate, ReadExecutor, ReadExecutorProvider,
        RegionSnapshot, RequestPolicy,
    },
    Error, Result,
};
use slog::{debug, Logger};
use tikv_util::{box_err, codec::number::decode_u64, time::monotonic_raw_now, Either};
use time::Timespec;
use txn_types::WriteBatchFlags;

use crate::{
    fsm::StoreMeta,
    router::{PeerMsg, QueryResult},
    StoreRouter,
};

pub trait MsgRouter: Clone + Send {
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
    pub fn new(
        store_meta: Arc<Mutex<StoreMeta>>,
        reg: TabletRegistry<E>,
        router: C,
        logger: Logger,
    ) -> Self {
        Self {
            local_reader: LocalReaderCore::new(StoreMetaDelegate::new(store_meta, reg)),
            router,
            logger,
        }
    }

    pub fn store_meta(&self) -> &Arc<Mutex<StoreMeta>> {
        &self.local_reader.store_meta().store_meta
    }

    pub fn pre_propose_raft_command(
        &mut self,
        req: &RaftCmdRequest,
    ) -> Result<Option<(CachedReadDelegate<E>, RequestPolicy)>> {
        if let Some(delegate) = self.local_reader.validate_request(req)? {
            let mut inspector = SnapRequestInspector {
                delegate: &delegate,
                logger: &self.logger,
            };
            match inspector.inspect(req) {
                Ok(RequestPolicy::ReadLocal) => Ok(Some((delegate, RequestPolicy::ReadLocal))),
                Ok(RequestPolicy::StaleRead) => Ok(Some((delegate, RequestPolicy::StaleRead))),
                // It can not handle other policies.
                // TODO: we should only abort when lease expires. For other cases we should retry
                // infinitely.
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            }
        } else {
            Err(Error::RegionNotFound(req.get_header().get_region_id()))
        }
    }

    fn try_get_snapshot(
        &mut self,
        req: &RaftCmdRequest,
    ) -> std::result::Result<Option<RegionSnapshot<E::Snapshot>>, RaftCmdResponse> {
        match self.pre_propose_raft_command(req) {
            Ok(Some((mut delegate, policy))) => {
                let mut snap = match policy {
                    RequestPolicy::ReadLocal => {
                        let region = Arc::clone(&delegate.region);
                        let snap =
                            RegionSnapshot::from_snapshot(delegate.get_snapshot(&None), region);
                        // Ensures the snapshot is acquired before getting the time
                        atomic::fence(atomic::Ordering::Release);
                        let snapshot_ts = monotonic_raw_now();

                        if !delegate.is_in_leader_lease(snapshot_ts) {
                            return Ok(None);
                        }

                        TLS_LOCAL_READ_METRICS
                            .with(|m| m.borrow_mut().local_executed_requests.inc());

                        // Try renew lease in advance
                        self.maybe_renew_lease_in_advance(&delegate, req, snapshot_ts);
                        snap
                    }
                    RequestPolicy::StaleRead => {
                        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
                        delegate.check_stale_read_safe(read_ts)?;

                        let region = Arc::clone(&delegate.region);
                        let snap =
                            RegionSnapshot::from_snapshot(delegate.get_snapshot(&None), region);

                        TLS_LOCAL_READ_METRICS
                            .with(|m| m.borrow_mut().local_executed_requests.inc());

                        delegate.check_stale_read_safe(read_ts)?;

                        TLS_LOCAL_READ_METRICS
                            .with(|m| m.borrow_mut().local_executed_stale_read_requests.inc());
                        snap
                    }
                    _ => unreachable!(),
                };

                snap.txn_ext = Some(delegate.txn_ext.clone());
                snap.bucket_meta = delegate.bucket_meta.clone();

                Ok(Some(snap))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                let mut response = cmd_resp::new_error(e);
                if let Some(delegate) = self
                    .local_reader
                    .delegates
                    .get(&req.get_header().get_region_id())
                {
                    cmd_resp::bind_term(&mut response, delegate.term);
                }
                Err(response)
            }
        }
    }

    pub fn snapshot(
        &mut self,
        mut req: RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<RegionSnapshot<E::Snapshot>, RaftCmdResponse>> + Send
    {
        let region_id = req.header.get_ref().region_id;
        let res = match self.try_get_snapshot(&req) {
            res @ (Ok(Some(_)) | Err(_)) => Either::Left(res),
            Ok(None) => Either::Right((self.try_to_renew_lease(region_id, &req), self.clone())),
        };

        worker_metrics::maybe_tls_local_read_metrics_flush();

        async move {
            match res {
                Either::Left(Ok(Some(snap))) => Ok(snap),
                Either::Left(Err(e)) => Err(e),
                Either::Right((fut, mut reader)) => {
                    let err = match fut.await? {
                        Some(query_res) => {
                            if query_res.read().is_some() {
                                // If query successful, try again.
                                req.mut_header().set_read_quorum(false);
                                if let Some(snap) = reader.try_get_snapshot(&req)? {
                                    return Ok(snap);
                                } else {
                                    let mut err = errorpb::Error::default();
                                    err.set_message(format!("no delegate found for {}", region_id));
                                    err
                                }
                            } else {
                                let QueryResult::Response(res) = query_res else { unreachable!() };
                                assert!(res.get_header().has_error(), "{:?}", res);
                                return Err(res);
                            }
                        }
                        None => {
                            let mut err = errorpb::Error::default();
                            err.set_message(format!(
                                "failed to extend lease: canceled: {}",
                                region_id
                            ));
                            err
                        }
                    };
                    let mut resp = RaftCmdResponse::default();
                    resp.mut_header().set_error(err);
                    Err(resp)
                }
                Either::Left(Ok(None)) => unreachable!(),
            }
        }
    }

    // try to renew the lease by sending read query where the reading process may
    // renew the lease
    fn try_to_renew_lease(
        &self,
        region_id: u64,
        req: &RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<Option<QueryResult>, RaftCmdResponse>> {
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
    cached_tablet: CachedTablet<E>,
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

impl<E> ReadExecutor for CachedReadDelegate<E>
where
    E: KvEngine,
{
    type Tablet = E;

    fn get_tablet(&mut self) -> &E {
        self.cached_tablet.latest().unwrap()
    }

    fn get_snapshot(&mut self, _: &Option<LocalReadContext<'_, Self::Tablet>>) -> Arc<E::Snapshot> {
        Arc::new(self.cached_tablet.latest().unwrap().snapshot())
    }
}

#[derive(Clone)]
struct StoreMetaDelegate<E>
where
    E: KvEngine,
{
    store_meta: Arc<Mutex<StoreMeta>>,
    reg: TabletRegistry<E>,
}

impl<E> StoreMetaDelegate<E>
where
    E: KvEngine,
{
    pub fn new(store_meta: Arc<Mutex<StoreMeta>>, reg: TabletRegistry<E>) -> StoreMetaDelegate<E> {
        StoreMetaDelegate { store_meta, reg }
    }
}

impl<E> ReadExecutorProvider for StoreMetaDelegate<E>
where
    E: KvEngine,
{
    type Executor = CachedReadDelegate<E>;
    type StoreMeta = Arc<Mutex<StoreMeta>>;

    fn store_id(&self) -> Option<u64> {
        Some(self.store_meta.as_ref().lock().unwrap().store_id)
    }

    /// get the ReadDelegate with region_id and the number of delegates in the
    /// StoreMeta
    fn get_executor_and_len(&self, region_id: u64) -> (usize, Option<Self::Executor>) {
        let meta = self.store_meta.as_ref().lock().unwrap();
        let reader = meta.readers.get(&region_id).cloned();
        if let Some(reader) = reader {
            // If reader is not None, cache must not be None.
            let cached_tablet = self.reg.get(region_id).unwrap();
            return (
                meta.readers.len(),
                Some(CachedReadDelegate {
                    delegate: Arc::new(reader),
                    cached_tablet,
                }),
            );
        }
        (meta.readers.len(), None)
    }
}

struct SnapRequestInspector<'r> {
    delegate: &'r ReadDelegate,
    logger: &'r Logger,
}

impl<'r> SnapRequestInspector<'r> {
    fn inspect(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        assert!(!req.has_admin_request());
        if req.get_requests().len() != 1
            || req.get_requests().first().unwrap().get_cmd_type() != CmdType::Snap
        {
            return Err(box_err!(
                "LocalReader can only serve for exactly one Snap request"
            ));
        }

        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Ok(RequestPolicy::StaleRead);
        }

        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // If applied index's term differs from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if !self.has_applied_to_current_term() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // Local read should be performed, if and only if leader is in lease.
        // None for now.
        match self.inspect_lease() {
            LeaseState::Valid => Ok(RequestPolicy::ReadLocal),
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(RequestPolicy::ReadIndex)
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

    use crossbeam::{atomic::AtomicCell, channel::TrySendError};
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, TestTabletFactory},
    };
    use engine_traits::{MiscExt, Peekable, SyncMutable, TabletContext, DATA_CFS};
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
    }

    impl MockRouter {
        fn new() -> (MockRouter, Receiver<(u64, PeerMsg)>) {
            let (p_ch, p_rx) = sync_channel(1);
            (MockRouter { p_router: p_ch }, p_rx)
        }
    }

    impl MsgRouter for MockRouter {
        fn send(&self, addr: u64, cmd: PeerMsg) -> std::result::Result<(), TrySendError<PeerMsg>> {
            self.p_router.send((addr, cmd)).unwrap();
            Ok(())
        }
    }

    #[allow(clippy::type_complexity)]
    fn new_reader(
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta>>,
        reg: TabletRegistry<KvTestEngine>,
    ) -> (
        LocalReader<KvTestEngine, MockRouter>,
        Receiver<(u64, PeerMsg)>,
    ) {
        let (ch, rx) = MockRouter::new();
        let mut reader = LocalReader::new(
            store_meta,
            reg,
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
        let (mut reader, mut rx) = new_reader(store_id, store_meta.clone(), reg.clone());
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
                track_ver: TrackVer::new(),
                bucket_meta: Some(bucket_meta.clone()),
            };
            meta.readers.insert(1, read_delegate);
            // create tablet with region_id 1 and prepare some data
            let ctx = TabletContext::new(&region1, Some(10));
            reg.load(ctx, true).unwrap();
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
                        .update(ReadProgress::applied_term(term6));
                }),
                rx,
                ch_tx.clone(),
            ))
            .unwrap();
        // The first try will be rejected due to unmatched applied term but after update
        // the applied term by the above thread, the snapshot will be acquired by
        // retrying.
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
        // Send what we want to do to mock raftstore
        mix_tx
            .send((
                Box::new(move || {
                    let mut meta = store_meta.lock().unwrap();
                    meta.readers
                        .get_mut(&1)
                        .unwrap()
                        .update(ReadProgress::leader_lease(remote));
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

        let store_meta =
            StoreMetaDelegate::new(Arc::new(Mutex::new(StoreMeta::new(1))), reg.clone());

        let tablet1;
        let tablet2;
        {
            let mut meta = store_meta.store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let read_delegate = ReadDelegate::mock(1);
            meta.readers.insert(1, read_delegate);

            // create tablet with region_id 1 and prepare some data
            let mut ctx = TabletContext::with_infinite_region(1, Some(10));
            reg.load(ctx, true).unwrap();
            tablet1 = reg.get(1).unwrap().latest().unwrap().clone();
            tablet1.put(b"a1", b"val1").unwrap();

            // Create read_delegate with region id 2
            let read_delegate = ReadDelegate::mock(2);
            meta.readers.insert(2, read_delegate);

            // create tablet with region_id 1 and prepare some data
            ctx = TabletContext::with_infinite_region(2, Some(10));
            reg.load(ctx, true).unwrap();
            tablet2 = reg.get(2).unwrap().latest().unwrap().clone();
            tablet2.put(b"a2", b"val2").unwrap();
        }

        let (_, delegate) = store_meta.get_executor_and_len(1);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(tablet1.path(), tablet.path());
        let snapshot = delegate.get_snapshot(&None);
        assert_eq!(
            b"val1".to_vec(),
            *snapshot.get_value(b"a1").unwrap().unwrap()
        );

        let (_, delegate) = store_meta.get_executor_and_len(2);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(tablet2.path(), tablet.path());
        let snapshot = delegate.get_snapshot(&None);
        assert_eq!(
            b"val2".to_vec(),
            *snapshot.get_value(b"a2").unwrap().unwrap()
        );
    }
}
