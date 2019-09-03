// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: remove it after all code been merged.
#![allow(unused_variables)]
#![allow(dead_code)]

use std::cmp;
use std::fmt;
use std::sync::atomic::*;
use std::sync::*;
use std::time::*;

use engine::DB;
use external_storage::*;
use futures::sync::mpsc::*;
use futures::{lazy, Future};
use kvproto::backup::*;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use kvproto::metapb::*;
use raft::StateRole;
use tikv::raftstore::coprocessor::RegionInfoAccessor;
use tikv::raftstore::store::util::find_peer;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::storage::kv::{
    Engine, Error as EngineError, RegionInfoProvider, ScanMode, StatisticsSummary,
};
use tikv::storage::txn::{
    EntryBatch, Error as TxnError, Msg, Scanner, SnapshotStore, Store, TxnEntryScanner,
    TxnEntryStore,
};
use tikv::storage::{Key, Statistics};
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};

use crate::*;

/// Backup task.
pub struct Task {
    pub(crate) resp: UnboundedSender<BackupResponse>,

    cancel: Arc<AtomicBool>,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupTask").finish()
    }
}

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        req: BackupRequest,
        resp: UnboundedSender<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let cancel = Arc::new(AtomicBool::new(false));
        Ok((
            Task {
                resp,
                cancel: cancel.clone(),
            },
            cancel,
        ))
    }

    /// Check whether the task is canceled.
    pub fn has_canceled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

/// The endpoint of backup.
///
/// It coordinates backup tasks and dispathes them to different workers.
pub struct Endpoint<E: Engine, R: RegionInfoProvider> {
    db: Arc<DB>,
    pub(crate) engine: E,
    pub(crate) region_info: R,
}

impl<E: Engine, R: RegionInfoProvider> Endpoint<E, R> {
    pub fn new(engine: E, region_info: R, db: Arc<DB>) -> Endpoint<E, R> {
        Endpoint {
            engine,
            region_info,
            db,
        }
    }

    pub fn handle_backup_task(&self, _task: Task) {
        // TODO: fill it.
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use external_storage::LocalStorage;
    use futures::{Future, Stream};
    use kvproto::metapb;
    use std::collections::BTreeMap;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use tempfile::TempDir;
    use tikv::raftstore::coprocessor::RegionCollector;
    use tikv::raftstore::coprocessor::{RegionInfo, SeekRegionCallback};
    use tikv::raftstore::store::util::new_peer;
    use tikv::storage::kv::Result as EngineResult;
    use tikv::storage::mvcc::tests::*;
    use tikv::storage::SHORT_VALUE_MAX_LEN;
    use tikv::storage::{
        Mutation, Options, RocksEngine, Storage, TestEngineBuilder, TestStorageBuilder,
    };

    #[derive(Clone)]
    pub struct MockRegionInfoProvider {
        // start_key -> (region_id, end_key)
        regions: Arc<Mutex<RegionCollector>>,
        cancel: Option<Arc<AtomicBool>>,
    }
    impl MockRegionInfoProvider {
        pub fn new() -> Self {
            MockRegionInfoProvider {
                regions: Arc::new(Mutex::new(RegionCollector::new())),
                cancel: None,
            }
        }
        pub fn set_regions(&self, regions: Vec<(Vec<u8>, Vec<u8>, u64)>) {
            let mut map = self.regions.lock().unwrap();
            for (mut start_key, mut end_key, id) in regions {
                if !start_key.is_empty() {
                    start_key = Key::from_raw(&start_key).into_encoded();
                }
                if !end_key.is_empty() {
                    end_key = Key::from_raw(&end_key).into_encoded();
                }
                let mut r = metapb::Region::default();
                r.set_id(id);
                r.set_start_key(start_key.clone());
                r.set_end_key(end_key);
                r.mut_peers().push(new_peer(1, 1));
                map.create_region(r, StateRole::Leader);
            }
        }
        fn canecl_on_seek(&mut self, cancel: Arc<AtomicBool>) {
            self.cancel = Some(cancel);
        }
    }
    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> EngineResult<()> {
            let from = from.to_vec();
            let regions = self.regions.lock().unwrap();
            if let Some(c) = self.cancel.as_ref() {
                c.store(true, Ordering::SeqCst);
            }
            regions.handle_seek_region(from, callback);
            Ok(())
        }
    }

    pub fn new_endpoint() -> (TempDir, Endpoint<RocksEngine, MockRegionInfoProvider>) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine::CF_DEFAULT, engine::CF_LOCK, engine::CF_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        (
            temp,
            Endpoint::new(rocks, MockRegionInfoProvider::new(), db),
        )
    }
}
