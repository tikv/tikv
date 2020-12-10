use std::marker::PhantomData;
use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::kvrpcpb::{ExtraOp as TxnExtraOp, IsolationLevel};
use kvproto::metapb::Region;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeCmd, ChangeObserve, ObserveID, ObserveRange};
use raftstore::store::msg::{Callback, SignificantMsg};
use raftstore::store::RegionSnapshot;
use tikv::storage::kv::{ScanMode as MvccScanMode, Snapshot};
use tikv::storage::mvcc::{DeltaScanner, MvccReader, ScannerBuilder};
use tikv::storage::txn::{TxnEntry, TxnEntryScanner};
use tokio::runtime::{Builder, Runtime};
use txn_types::{Key, Lock, TimeStamp};

use crate::endpoint::Task;
use crate::errors::{Error, Result};

const DEFAULT_SCAN_BATCH_SIZE: usize = 1024;

pub enum ScanMode {
    LockOnly,
    All,
    AllWithOldValue,
}

pub struct ScanTask {
    pub id: ObserveID,
    pub tag: String,
    pub mode: ScanMode,
    pub region: Region,
    pub checkpoint_ts: TimeStamp,
    pub cancelled: Box<dyn Fn() -> bool + Send>,
    pub send_entries: Box<dyn Fn(Vec<ScanEntry>) + Send>,
    pub before_start: Option<Box<dyn Fn() + Send>>,
    pub on_error: Option<Box<dyn Fn(ObserveID, Region, Error) + Send>>,
}

#[derive(Debug)]
pub enum ScanEntry {
    TxnEntry(Vec<TxnEntry>),
    Lock(Vec<(Key, Lock)>),
    None,
}

#[derive(Clone)]
pub struct ScannerPool<T, E> {
    workers: Arc<Runtime>,
    raft_router: T,
    _phantom: PhantomData<E>,
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> ScannerPool<T, E> {
    pub fn new(count: usize, raft_router: T) -> Self {
        let workers = Arc::new(
            Builder::new()
                .threaded_scheduler()
                .thread_name("inc-scan")
                .core_threads(count)
                .build()
                .unwrap(),
        );
        Self {
            workers,
            raft_router,
            _phantom: PhantomData::default(),
        }
    }

    pub fn spawn_task(&self, mut task: ScanTask) {
        let raft_router = self.raft_router.clone();
        let fut = async move {
            let snap = match Self::get_snapshot(&mut task, raft_router).await {
                Ok(snap) => snap,
                Err(e) => {
                    warn!("resolved_ts scan get snapshot failed"; "err" => ?e);
                    let ScanTask {
                        on_error,
                        region,
                        id,
                        ..
                    } = task;
                    if let Some(on_error) = on_error {
                        on_error(id, region, e);
                    }
                    return;
                }
            };
            let mut entries = vec![];
            match task.mode {
                ScanMode::All | ScanMode::AllWithOldValue => {
                    let txn_extra_op = if let ScanMode::AllWithOldValue = task.mode {
                        TxnExtraOp::ReadOldValue
                    } else {
                        TxnExtraOp::Noop
                    };
                    let mut scanner = ScannerBuilder::new(snap, TimeStamp::max(), false)
                        .range(None, None)
                        .build_delta_scanner(task.checkpoint_ts, txn_extra_op)
                        .unwrap();
                    let mut done = false;
                    while !done && !(task.cancelled)() {
                        let (es, has_remaining) = match Self::scan_delta(&mut scanner) {
                            Ok(rs) => rs,
                            Err(e) => {
                                warn!("resolved_ts scan delta failed"; "err" => ?e);
                                let ScanTask {
                                    on_error,
                                    region,
                                    id,
                                    ..
                                } = task;
                                if let Some(on_error) = on_error {
                                    on_error(id, region, e);
                                }
                                return;
                            }
                        };
                        done = !has_remaining;
                        entries.push(ScanEntry::TxnEntry(es));
                    }
                }
                ScanMode::LockOnly => {
                    let mut reader = MvccReader::new(
                        snap,
                        Some(MvccScanMode::Forward),
                        false,
                        IsolationLevel::Si,
                    );
                    let mut done = false;
                    let mut start = None;
                    while !done && !(task.cancelled)() {
                        let (locks, has_remaining) =
                            match Self::scan_locks(&mut reader, start.as_ref(), task.checkpoint_ts)
                            {
                                Ok(rs) => rs,
                                Err(e) => {
                                    warn!("resolved_ts scan delta failed"; "err" => ?e);
                                    let ScanTask {
                                        on_error,
                                        region,
                                        id,
                                        ..
                                    } = task;
                                    if let Some(on_error) = on_error {
                                        on_error(id, region, e);
                                    }
                                    return;
                                }
                            };
                        done = !has_remaining;
                        if has_remaining {
                            start = Some(locks.last().unwrap().0.clone())
                        }
                        entries.push(ScanEntry::Lock(locks));
                    }
                }
            }
            entries.push(ScanEntry::None);
            (task.send_entries)(entries);
        };
        self.workers.spawn(fut);
    }

    async fn get_snapshot(
        task: &mut ScanTask,
        raft_router: T,
    ) -> Result<RegionSnapshot<E::Snapshot>> {
        let (cb, fut) = tikv_util::future::paired_future_callback();
        let before_start = task.before_start.take();
        let change_cmd = ChangeCmd {
            region_id: task.region.id,
            change_observe: ChangeObserve {
                id: task.id,
                range: ObserveRange::All,
            },
        };
        raft_router.significant_send(
            task.region.id,
            SignificantMsg::CaptureChange {
                cmd: change_cmd,
                region_epoch: task.region.get_region_epoch().clone(),
                callback: Callback::Read(Box::new(move |resp| {
                    if let Some(f) = before_start {
                        f();
                    }
                    cb(resp)
                })),
            },
        )?;
        let mut resp = box_try!(fut.await);
        if resp.response.get_header().has_error() {
            return Err(Error::Request(resp.response.take_header().take_error()));
        }
        Ok(resp.snapshot.unwrap())
    }

    fn scan_locks<S: Snapshot>(
        reader: &mut MvccReader<S>,
        start: Option<&Key>,
        checkpoint_ts: TimeStamp,
    ) -> Result<(Vec<(Key, Lock)>, bool)> {
        let (locks, has_remaining) =
            reader.scan_locks(start, |l| l.ts <= checkpoint_ts, DEFAULT_SCAN_BATCH_SIZE)?;
        Ok((locks, has_remaining))
    }

    fn scan_delta<S: Snapshot>(scanner: &mut DeltaScanner<S>) -> Result<(Vec<TxnEntry>, bool)> {
        let mut entries = Vec::with_capacity(DEFAULT_SCAN_BATCH_SIZE);
        let mut has_remaining = true;
        while entries.len() < entries.capacity() {
            match scanner.next_entry()? {
                Some(entry) => {
                    entries.push(entry);
                }
                None => {
                    has_remaining = false;
                    break;
                }
            }
        }
        Ok((entries, has_remaining))
    }
}
