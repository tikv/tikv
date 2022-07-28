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
        ReadDelegateCore, ReadDelegateExtTrait, ReadDelegateTrait, ReadExecutor, ReadMetrics,
        ReadProgress, ReadResponse, RegionSnapshot, RequestInspector, RequestPolicy, TrackVer,
        TxnExt,
    },
    Error, Result,
};
use slog::{debug, error, info, o, warn, Logger};
use tikv_util::{
    codec::number::decode_u64,
    lru::LruCache,
    time::{monotonic_raw_now, Instant, ThreadReadId},
};
use time::Timespec;

use crate::tablet::CachedTablet;

pub struct ReadDelegate<E>
where
    E: KvEngine,
{
    // The reason for this to be Arc, see the comment on get_delegate in raftstore/src/store/worker/read.rs
    delegate: Arc<ReadDelegateCore>,
    cached_tablet: CachedTablet<E>,
}

impl ReadDelegateTrait for ReadDelegate<E> {
    fn from_meta(
        meta: &std::sync::MutexGuard<raftstore::store::fsm::StoreMeta>,
        region_id: u64,
    ) -> Option<Self> {
        let reader = meta.readers.get(&region_id).cloned();
    }

    fn delegate(&self) -> &ReadDelegateCore {
        self.delegate.as_ref()
    }
}

impl Clone for ReadDelegate {
    fn clone(&self) -> Self {
        ReadDelegate {
            delegate: Arc::clone(&self.delegate),
            cached_tablet: self.cached_tablet.clone(),
        }
    }
}

/// ReadDelegateExt is a wrapper of ReadMetrics (now, only ReadMetrics, it can has other fields in the future) and
/// v2's ReadDelegate which is a wrapper of v1's ReadDelegate and CachedTablet which will be used as a temporay
/// local variable to complete each execution.
///
/// The reasons for the wrappings are:
/// 1. For v2's ReadDelegate (the wrapper of v1's ReadDelegate and CachedTablet): Unlike v1, which uses a single
/// global kv rocksdb, each region in v2 has it's own kv rocksdb, namely the tablet. Equipping ReadDelegate with a
/// CachedTablet makes tablet/snapshot acquisition very quickly. But CachedTablet requires &mut self to acquire tablet
/// as sometimes tablet may be updated which makes CachedTablet to mutate itself. ReadDelegate is read only, so wrapping them
/// together to workaround this.
///
/// 2. For ReadDelgateWithMetric (the wrapper of ReadMetrics and v2's ReadDelegate): Unlike v1 where the LocalReader implements
/// ReadExecutor, in v2, we use ReadDelegateExt to implement ReadDelegateExt so that each delegate can get its
/// tablet/snapshot very quickly. But we also need to update some metrics which is a field of LocalReader, so we use this wrapper
/// which will be used as a temporary local variable to make compiler happy.
pub struct ReadDelegateExt<'a, E>
where
    E: KvEngine,
{
    delegate: Option<ReadDelegate<E>>,
    metrics: &'a mut ReadMetrics,
}

impl<'a, E, D> ReadDelegateExtTrait<'a, E, D> for ReadDelegateExt<'a, E, D>
where
    E: KvEngine,
    D: ReadDelegateTrait,
{
    fn from_read_delegate(
        delegate: D,
        _kv_engine: &'a E,
        metrics: &'a mut ReadMetrics,
        _read_id: &'a mut ThreadReadId,
        _snap_cache: &'a mut Option<Arc<<E as KvEngine>::Snapshot>>,
    ) -> Self {
        ReadDelegateExt::<'a, _, _> {
            delegate: Some(delegate),
            metrics,
        }
    }

    fn delegate(&self) -> &ReadDelegateCore {
        // It should not be called after calling take_delegate
        assert!(self.delegate.is_some());
        self.delegate.as_ref().unwrap().delegate()
    }

    fn take_delegate(&mut self) -> D {
        // It should only be called once
        assert!(self.delegate.is_some());
        self.delegate.take().unwrap()
    }

    fn metrics(&mut self) -> &mut ReadMetrics {
        self.metrics
    }
}

impl<E> ReadExecutor<E> for ReadDelegateExt<'_, E>
where
    E: KvEngine,
{
    fn get_tablet(&mut self) -> &E {
        self.delegate
            .as_mut()
            .unwrap()
            .cached_tablet
            .latest()
            .unwrap()
    }

    fn get_snapshot(&mut self, create_time: Option<ThreadReadId>) -> Arc<E::Snapshot> {
        self.metrics.local_executed_requests += 1;
        Arc::new(
            self.delegate
                .cached_tablet
                .latest()
                .unwrap()
                .clone()
                .snapshot(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, sync::mpsc::*, thread};

    use crossbeam::channel::TrySendError;
    use engine_rocks::raw::Writable;
    use engine_test::kv::{KvTestEngine, KvTestSnapshot, TestTabletFactoryV2};
    use engine_traits::{Peekable, SyncMutable, ALL_CFS};
    use kvproto::raft_cmdpb::*;
    use raftstore::store::util::Lease;
    use tempfile::{Builder, TempDir};
    use tikv_kv::Snapshot;
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};
    use time::Duration;
    use txn_types::{Key, Lock, LockType, WriteBatchFlags};

    use super::*;

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
    fn new_reader_and_factory(
        path: &str,
        store_id: u64,
        store_meta: Arc<Mutex<StoreMeta<KvTestEngine>>>,
    ) -> (
        TempDir,
        LocalReader<MockRouter, KvTestEngine>,
        Receiver<RaftCommand<KvTestSnapshot>>,
        Arc<dyn TabletFactory<KvTestEngine> + Send + Sync>,
    ) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let db = engine_test::kv::new_engine(path_str, None, ALL_CFS, None).unwrap();
        let (ch, rx, _) = MockRouter::new();
        let mut reader =
            LocalReader::new(store_meta, ch, Logger::root(slog::Discard, o!("" => "")));
        reader.store_id = Cell::new(Some(store_id));

        let factory = Arc::new(TestTabletFactoryV2::new(path_str, None, ALL_CFS, None));

        (path, reader, rx, factory)
    }

    fn new_read_delegate(
        region: &metapb::Region,
        peer_id: u64,
        term: u64,
        applied_index_term: u64,
    ) -> ReadDelegateInner {
        let mut read_delegate_inner = ReadDelegateInner::mock(region);
        read_delegate_inner.read_delegate.peer_id = peer_id;
        read_delegate_inner.read_delegate.term = term;
        read_delegate_inner.read_delegate.applied_index_term = applied_index_term;
        read_delegate_inner
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
        reader: &mut LocalReader<MockRouter, KvTestEngine>,
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
        reader: &mut LocalReader<MockRouter, KvTestEngine>,
        rx: &Receiver<RaftCommand<KvTestSnapshot>>,
        task: RaftCommand<KvTestSnapshot>,
    ) {
        reader.propose_raft_command(None, task.request, task.callback);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    }

    #[test]
    fn test_read() {
        // This test is almost the same with test_read in v1 with some adaptive modification to v2.
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::<KvTestEngine>::new()));
        let (_tmp, mut reader, rx, factory) =
            new_reader_and_factory("test-local-reader", store_id, store_meta.clone());

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
        assert_eq!(reader.metrics.rejected_by_no_region, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 1);
        assert!(reader.cached_delegates.get(&1).is_none());

        // Register region 1
        lease.renew(monotonic_raw_now());
        let remote = lease.maybe_new_remote_lease(term6).unwrap();
        let tablet1 = factory.create_tablet(region1.id, 0).unwrap();
        let mut cached_tablet1 = CachedTablet::new(Some(tablet1));
        // But the applied_index_term is stale
        {
            let mut meta = store_meta.lock().unwrap();
            let mut read_delegate = new_read_delegate(&region1, leader2.get_id(), term6, term6 - 1);
            read_delegate.read_delegate.leader_lease = Some(remote);
            meta.readers.insert(1, read_delegate);
            meta.caches.insert(1, cached_tablet1);
        }

        // The applied_index_term is stale
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(reader.metrics.rejected_by_cache_miss, 2);
        assert_eq!(reader.metrics.rejected_by_applied_term, 1);

        // Make the applied_index_term matches current term.
        let pg = ReadProgress::applied_index_term(term6);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().read_delegate.update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

        // Let's read
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region1);
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

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
        assert_eq!(reader.metrics.rejected_by_store_id_mismatch, 1);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 3);

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
        assert_eq!(reader.metrics.rejected_by_peer_id_mismatch, 1);

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
        assert_eq!(reader.metrics.rejected_by_term_mismatch, 1);

        // Stale epoch.
        let mut epoch12 = epoch13;
        epoch12.set_version(2);
        let mut cmd_epoch = cmd.clone();
        cmd_epoch.mut_header().set_region_epoch(epoch12);
        must_redirect(&mut reader, &rx, cmd_epoch);
        assert_eq!(reader.metrics.rejected_by_epoch, 1);

        // Expire lease manually, and it can not be renewed.
        let previous_lease_rejection = reader.metrics.rejected_by_lease_expire;
        lease.expire();
        lease.renew(monotonic_raw_now());
        must_redirect(&mut reader, &rx, cmd.clone());
        assert_eq!(
            reader.metrics.rejected_by_lease_expire,
            previous_lease_rejection + 1
        );

        // Channel full. The channel bound is set to 1 in this case, so the second request makes
        // channel full.
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
        assert_eq!(reader.metrics.rejected_by_channel_full, 1);

        // Reject by term mismatch in lease.
        let previous_term_rejection = reader.metrics.rejected_by_term_mismatch;
        let mut cmd9 = cmd.clone();
        cmd9.mut_header().set_term(term6 + 3);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .read_delegate
                .update(ReadProgress::term(term6 + 3));
            meta.readers
                .get_mut(&1)
                .unwrap()
                .read_delegate
                .update(ReadProgress::applied_index_term(term6 + 3));
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
            reader.metrics.rejected_by_term_mismatch,
            previous_term_rejection + 1,
        );
        assert_eq!(reader.metrics.rejected_by_cache_miss, 4);

        // Stale local ReadDelegate
        // get_delegate will notice this and update the local ReadDelegate
        cmd.mut_header().set_term(term6 + 3);
        lease.expire_remote_lease();
        let remote_lease = lease.maybe_new_remote_lease(term6 + 3).unwrap();
        let pg = ReadProgress::leader_lease(remote_lease);
        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().read_delegate.update(pg);
        }
        let task =
            RaftCommand::<KvTestSnapshot>::new(cmd.clone(), Callback::Read(Box::new(move |_| {})));
        must_not_redirect(&mut reader, &rx, task);
        assert_eq!(reader.metrics.rejected_by_cache_miss, 5);

        // Stale read
        assert_eq!(reader.metrics.rejected_by_safe_timestamp, 0);
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
        assert_eq!(reader.metrics.rejected_by_safe_timestamp, 1);

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
    fn test_get_snapshot_from_different_regions() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::<KvTestEngine>::new()));
        let (_tmp, mut reader, rx, factory) =
            new_reader_and_factory("test-local-reader", store_id, store_meta.clone());
        let mut region1 = metapb::Region::default();
        region1.set_id(1);
        let mut region2 = metapb::Region::default();
        region2.set_id(2);
        let epoch13 = {
            let mut ep = metapb::RegionEpoch::default();
            ep.set_conf_ver(1);
            ep.set_version(3);
            ep
        };

        let prs1 = new_peers(store_id, vec![1]);
        let prs2 = new_peers(store_id, vec![2]);
        region1.set_peers(prs1.clone().into());
        region2.set_peers(prs2.clone().into());
        region1.set_region_epoch(epoch13.clone());
        region2.set_region_epoch(epoch13.clone());

        let mut cmd1 = RaftCmdRequest::default();
        let mut header1 = RaftRequestHeader::default();
        header1.set_region_id(1);
        header1.set_peer(prs1[0].clone());
        header1.set_region_epoch(epoch13.clone());
        header1.set_term(1);
        cmd1.set_header(header1);
        let mut req1 = Request::default();
        req1.set_cmd_type(CmdType::Snap);
        cmd1.set_requests(vec![req1].into());

        let mut cmd2 = RaftCmdRequest::default();
        let mut header2 = RaftRequestHeader::default();
        header2.set_region_id(2);
        header2.set_peer(prs2[0].clone());
        header2.set_region_epoch(epoch13);
        header2.set_term(1);
        cmd2.set_header(header2);
        let mut req2 = Request::default();
        req2.set_cmd_type(CmdType::Snap);
        cmd2.set_requests(vec![req2].into());

        // Create some tablets and prepare some data
        let tablet1 = factory.create_tablet(1, 0).unwrap();
        let db = tablet1.get_sync_db();
        db.put(b"za1", b"val_a1").unwrap();
        let tablet2 = factory.create_tablet(2, 0).unwrap();
        let db = tablet2.get_sync_db();
        db.put(b"za2", b"val_a2").unwrap();

        let mut cached_tablet1 = CachedTablet::new(Some(tablet1));
        let mut cached_tablet2 = CachedTablet::new(Some(tablet2));
        {
            let mut meta = store_meta.lock().unwrap();
            let mut read_delegate1 = new_read_delegate(&region1, 1, 1, 1);
            let mut read_delegate2 = new_read_delegate(&region2, 2, 1, 1);

            let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250));
            lease.renew(monotonic_raw_now());
            let remote = lease.maybe_new_remote_lease(1).unwrap();

            read_delegate1.read_delegate.leader_lease = Some(remote.clone());
            read_delegate2.read_delegate.leader_lease = Some(remote);

            meta.readers.insert(1, read_delegate1);
            meta.caches.insert(1, cached_tablet1);
            meta.readers.insert(2, read_delegate2);
            meta.caches.insert(2, cached_tablet2);
        }

        let region1_clone = region1.clone();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd1.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region1_clone);
                assert_eq!(
                    snap.get(&Key::from_encoded(b"a1".to_vec()))
                        .unwrap()
                        .unwrap(),
                    b"val_a1"
                );
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd2.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region2);
                assert_eq!(
                    snap.get(&Key::from_encoded(b"a2".to_vec()))
                        .unwrap()
                        .unwrap(),
                    b"val_a2"
                );
            })),
        );
        must_not_redirect(&mut reader, &rx, task);

        // Now open a tablet with a higher suffix, delete the old key and put a new key, and change the cached_tablet
        let tablet_path = factory.tablet_path(1, 0);
        let tablet1 = factory.load_tablet(&tablet_path, 1, 10).unwrap();
        let db = tablet1.get_sync_db();
        db.delete(b"za1");
        db.put(b"za3", b"val_a3").unwrap();
        let task = RaftCommand::<KvTestSnapshot>::new(
            cmd1.clone(),
            Callback::Read(Box::new(move |resp: ReadResponse<KvTestSnapshot>| {
                let snap = resp.snapshot.unwrap();
                assert_eq!(snap.get_region(), &region1);
                assert!(
                    snap.get(&Key::from_encoded(b"a1".to_vec()))
                        .unwrap()
                        .is_none()
                );
                assert_eq!(
                    snap.get(&Key::from_encoded(b"a3".to_vec()))
                        .unwrap()
                        .unwrap(),
                    b"val_a3"
                );
            })),
        );
    }

    #[test]
    fn test_read_delegate_cache_update() {
        let store_id = 2;
        let store_meta = Arc::new(Mutex::new(StoreMeta::<KvTestEngine>::new()));
        let (_tmp, mut reader, _, factory) =
            new_reader_and_factory("test-local-reader", store_id, store_meta.clone());
        let mut region = metapb::Region::default();
        region.set_id(1);
        let tablet = factory.create_tablet(1, 0).unwrap();
        {
            let mut meta = store_meta.lock().unwrap();
            let read_delegate = new_read_delegate(&region, 1, 1, 1);
            meta.readers.insert(1, read_delegate);
            meta.caches.insert(1, CachedTablet::new(Some(tablet)));
        }

        let d = reader.get_delegate(1).unwrap();
        assert_eq!(&*d.delegate_inner.read_delegate.region, &region);
        assert_eq!(d.delegate_inner.read_delegate.term, 1);
        assert_eq!(d.delegate_inner.read_delegate.applied_index_term, 1);
        assert!(d.delegate_inner.read_delegate.leader_lease.is_none());
        drop(d);

        {
            region.mut_region_epoch().set_version(10);
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .read_delegate
                .update(ReadProgress::region(region.clone()));
        }
        assert_eq!(
            &*reader
                .get_delegate(1)
                .unwrap()
                .delegate_inner
                .read_delegate
                .region,
            &region
        );

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .read_delegate
                .update(ReadProgress::term(2));
        }
        assert_eq!(
            reader
                .get_delegate(1)
                .unwrap()
                .delegate_inner
                .read_delegate
                .term,
            2
        );

        {
            let mut meta = store_meta.lock().unwrap();
            meta.readers
                .get_mut(&1)
                .unwrap()
                .read_delegate
                .update(ReadProgress::applied_index_term(2));
        }
        assert_eq!(
            reader
                .get_delegate(1)
                .unwrap()
                .delegate_inner
                .read_delegate
                .applied_index_term,
            2
        );

        {
            let mut lease = Lease::new(Duration::seconds(1), Duration::milliseconds(250)); // 1s is long enough.
            let remote = lease.maybe_new_remote_lease(3).unwrap();
            let pg = ReadProgress::leader_lease(remote);
            let mut meta = store_meta.lock().unwrap();
            meta.readers.get_mut(&1).unwrap().read_delegate.update(pg);
        }
        let d = reader.get_delegate(1).unwrap();
        assert_eq!(
            d.delegate_inner
                .read_delegate
                .leader_lease
                .clone()
                .unwrap()
                .term(),
            3
        );
    }
}
