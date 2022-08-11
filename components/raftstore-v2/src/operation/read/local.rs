// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    marker::PhantomData,
    ops::Deref,
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
        ReadDelegate, ReadExecutor, ReadExecutorProvider, ReadMetrics, ReadProgress, ReadResponse,
        RegionSnapshot, RequestInspector, RequestPolicy, TrackVer, TxnExt,
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

use crate::{fsm::StoreMeta, tablet::CachedTablet};

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

impl<E> ReadExecutor<E> for CachedReadDelegate<E>
where
    E: KvEngine,
{
    fn get_tablet(&mut self) -> &E {
        self.cached_tablet.latest().unwrap()
    }

    fn get_snapshot(
        &mut self,
        _: Option<ThreadReadId>,
        _: &mut Option<raftstore::store::LocalReadContext<'_, E>>,
    ) -> Arc<E::Snapshot> {
        Arc::new(self.cached_tablet.latest().unwrap().snapshot())
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
            // If reader is not None, cache must not be None.
            let cached_tablet = meta.tablet_caches.get(&region_id).cloned().unwrap();
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

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, sync::mpsc::*, thread};

    use crossbeam::channel::TrySendError;
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::{KvTestEngine, KvTestSnapshot, TestTabletFactoryV2},
    };
    use engine_traits::{OpenOptions, Peekable, SyncMutable, ALL_CFS, CF_DEFAULT};
    use kvproto::{metapb::Region, raft_cmdpb::*};
    use raftstore::store::{
        util::Lease, Callback, CasualMessage, CasualRouter, LocalReader, ProposalRouter,
        RaftCommand,
    };
    use tempfile::{Builder, TempDir};
    use tikv_util::{codec::number::NumberEncoder, time::monotonic_raw_now};
    use time::Duration;
    use txn_types::{Key, Lock, LockType, WriteBatchFlags};

    use super::*;

    fn new_read_delegate(
        region: &Region,
        peer_id: u64,
        term: u64,
        applied_index_term: u64,
    ) -> ReadDelegate {
        let mut read_delegate_core = ReadDelegate::mock(region.id);
        read_delegate_core.peer_id = peer_id;
        read_delegate_core.term = term;
        read_delegate_core.applied_term = applied_index_term;
        read_delegate_core.region = Arc::new(region.clone());
        read_delegate_core
    }

    #[test]
    fn test_read_delegate() {
        // Building a tablet factory
        let ops = DbOptions::default();
        let cf_opts = ALL_CFS.iter().map(|cf| (*cf, CfOptions::new())).collect();
        let path = Builder::new()
            .prefix("test-local-reader")
            .tempdir()
            .unwrap();
        let factory = Arc::new(TestTabletFactoryV2::new(
            path.path().to_str().unwrap(),
            ops,
            cf_opts,
        ));

        let store_meta =
            StoreMetaDelegate::new(Arc::new(Mutex::new(StoreMeta::<KvTestEngine>::new())));

        let tablet1;
        let tablet2;
        {
            let mut meta = store_meta.store_meta.as_ref().lock().unwrap();

            // Create read_delegate with region id 1
            let mut read_delegate = ReadDelegate::mock(1);
            meta.readers.insert(1, read_delegate);

            // create tablet with region_id 1 and prepare some data
            tablet1 = factory
                .open_tablet(1, Some(10), OpenOptions::default().set_create_new(true))
                .unwrap();
            tablet1.put(b"a1", b"val1").unwrap();
            let cache = CachedTablet::new(Some(tablet1.clone()));
            meta.tablet_caches.insert(1, cache);

            // Create read_delegate with region id 1
            let mut read_delegate = ReadDelegate::mock(2);
            let cache = CachedTablet::new(Some(read_delegate.clone()));
            meta.readers.insert(2, read_delegate);

            // create tablet with region_id 1 and prepare some data
            tablet2 = factory
                .open_tablet(2, Some(10), OpenOptions::default().set_create_new(true))
                .unwrap();
            tablet2.put(b"a2", b"val2").unwrap();
            let cache = CachedTablet::new(Some(tablet2.clone()));
            meta.tablet_caches.insert(2, cache);
        }

        let (_, delegate) = store_meta.get_executor_and_len(1);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(tablet1.as_inner().path(), tablet.as_inner().path());
        let snapshot = delegate.get_snapshot(None, &mut None);
        assert_eq!(
            b"val1".to_vec(),
            *snapshot.get_value(b"a1").unwrap().unwrap()
        );

        let (_, delegate) = store_meta.get_executor_and_len(2);
        let mut delegate = delegate.unwrap();
        let tablet = delegate.get_tablet();
        assert_eq!(tablet2.as_inner().path(), tablet.as_inner().path());
        let snapshot = delegate.get_snapshot(None, &mut None);
        assert_eq!(
            b"val2".to_vec(),
            *snapshot.get_value(b"a2").unwrap().unwrap()
        );
    }
}
