use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::kvrpcpb::{ExtraOp as TxnExtraOp, IsolationLevel};
use kvproto::metapb::RegionEpoch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeCmd, ChangeObserve, ObserveID, ObserveRange};
use raftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use raftstore::store::RegionSnapshot;
use tikv::storage::kv::{ScanMode as MvccScanMode, Snapshot};
use tikv::storage::mvcc::{DeltaScanner, MvccReader, ScannerBuilder};
use tikv::storage::txn::{TxnEntry, TxnEntryScanner};
use tikv::storage::Statistics;
use tokio::runtime::{Builder, Runtime};
use txn_types::{Key, Lock, TimeStamp};

use crate::errors::{Error, Result};

pub enum ScanMode {
    LockOnly,
    All,
    AllWithOldValue,
}

pub struct ScanTask {
    id: ObserveID,
    tag: String,
    mode: ScanMode,
    region_id: u64,
    region_epoch: RegionEpoch,
    checkpoint_ts: TimeStamp,
    batch_size: usize,
    cancelled: Box<dyn Fn() -> bool + Send>,
    before_start: Option<Box<dyn Fn() + Send>>,
}

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
    fn spawn_task(&self, mut task: ScanTask) {
        let raft_router = self.raft_router.clone();
        let fut = async move {
            let snap = match Self::get_snapshot(&mut task, raft_router).await {
                Ok(snap) => snap,
                Err(e) => {
                    warn!("resolved_ts scan get snapshot failed"; "err" => ?e);
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
                        let (es, has_remaining) =
                            match Self::scan_delta(&mut scanner, task.batch_size) {
                                Ok(rs) => rs,
                                Err(e) => {
                                    warn!("resolved_ts scan delta failed"; "err" => ?e);
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
                        let (locks, has_remaining) = match Self::scan_locks(
                            &mut reader,
                            start.as_ref(),
                            task.checkpoint_ts,
                            task.batch_size,
                        ) {
                            Ok(rs) => rs,
                            Err(e) => {
                                warn!("resolved_ts scan delta failed"; "err" => ?e);
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
            region_id: task.region_id,
            change_observe: ChangeObserve {
                id: task.id,
                range: ObserveRange::All,
            },
        };
        raft_router.significant_send(
            task.region_id,
            SignificantMsg::CaptureChange {
                cmd: change_cmd,
                region_epoch: task.region_epoch.clone(),
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
        batch_size: usize,
    ) -> Result<(Vec<(Key, Lock)>, bool)> {
        let (locks, has_remaining) =
            reader.scan_locks(start, |l| l.ts <= checkpoint_ts, batch_size)?;
        Ok((locks, has_remaining))
    }

    fn scan_delta<S: Snapshot>(
        scanner: &mut DeltaScanner<S>,
        batch_size: usize,
    ) -> Result<(Vec<TxnEntry>, bool)> {
        let mut entries = Vec::with_capacity(batch_size);
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
