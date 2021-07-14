// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
<<<<<<< HEAD
=======
use std::time::Duration;
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)

use engine_traits::KvEngine;
use kvproto::kvrpcpb::{ExtraOp as TxnExtraOp, IsolationLevel};
use kvproto::metapb::Region;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeObserver, ObserveID};
use raftstore::store::msg::{Callback, SignificantMsg};
use raftstore::store::RegionSnapshot;
use tikv::storage::kv::{ScanMode as MvccScanMode, Snapshot};
use tikv::storage::mvcc::{DeltaScanner, MvccReader, ScannerBuilder};
use tikv::storage::txn::{TxnEntry, TxnEntryScanner};
<<<<<<< HEAD
=======
use tikv_util::{time::Instant, timer::GLOBAL_TIMER_HANDLE};
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
use tokio::runtime::{Builder, Runtime};
use txn_types::{Key, Lock, TimeStamp};

use crate::errors::{Error, Result};

const DEFAULT_SCAN_BATCH_SIZE: usize = 1024;

pub type BeforeStartCallback = Box<dyn Fn() + Send>;
pub type OnErrorCallback = Box<dyn Fn(ObserveID, Region, Error) + Send>;
pub type OnEntriesCallback = Box<dyn Fn(Vec<ScanEntry>) + Send>;
pub type IsCancelledCallback = Box<dyn Fn() -> bool + Send>;

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
    pub is_cancelled: IsCancelledCallback,
    pub send_entries: OnEntriesCallback,
    pub before_start: Option<BeforeStartCallback>,
    pub on_error: Option<OnErrorCallback>,
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
                    while !done && !(task.is_cancelled)() {
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
                    while !done && !(task.is_cancelled)() {
                        let (locks, has_remaining) =
                            match Self::scan_locks(&mut reader, start.as_ref(), task.checkpoint_ts)
                            {
                                Ok(rs) => rs,
                                Err(e) => {
                                    warn!("resolved_ts scan lock failed"; "err" => ?e);
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
<<<<<<< HEAD
            (task.send_entries)(entries);
=======
            RTS_SCAN_DURATION_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
            (task.send_entries)(entries, apply_index);
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
        };
        self.workers.spawn(fut);
    }

    async fn get_snapshot(
        task: &mut ScanTask,
        raft_router: T,
    ) -> Result<RegionSnapshot<E::Snapshot>> {
<<<<<<< HEAD
        let (cb, fut) = tikv_util::future::paired_future_callback();
        let before_start = task.before_start.take();
        let change_cmd = ChangeObserver {
            observe_id: task.id,
            region_id: task.region.id,
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
=======
        let mut last_err = None;
        for retry_times in 0..=GET_SNAPSHOT_RETRY_TIME {
            if retry_times != 0 {
                if let Err(e) = GLOBAL_TIMER_HANDLE
                    .delay(
                        std::time::Instant::now() + retry_times * GET_SNAPSHOT_RETRY_BACKOFF_STEP,
                    )
                    .compat()
                    .await
                {
                    error!("failed to backoff"; "err" => ?e);
                }
                if (task.is_cancelled)() {
                    return Err(Error::Other("scan task cancelled".into()));
                }
            }
            let (cb, fut) = tikv_util::future::paired_future_callback();
            let change_cmd = ChangeObserver::from_rts(task.region.id, task.handle.clone());
            raft_router.significant_send(
                task.region.id,
                SignificantMsg::CaptureChange {
                    cmd: change_cmd,
                    region_epoch: task.region.get_region_epoch().clone(),
                    callback: Callback::Read(Box::new(cb)),
                },
            )?;
            let mut resp = box_try!(fut.await);
            if resp.response.get_header().has_error() {
                let err = resp.response.take_header().take_error();
                // These two errors can't handled by retrying since the epoch and observe id is unchanged
                if err.has_epoch_not_match() || err.get_message().contains("stale observe id") {
                    return Err(Error::request(err));
                }
                last_err = Some(err)
            } else {
                return Ok(resp.snapshot.unwrap());
            }
>>>>>>> a3860711c... Avoid duration calculation panic when clock jumps back (#10544)
        }
        Ok(resp.snapshot.unwrap())
    }

    fn scan_locks<S: Snapshot>(
        reader: &mut MvccReader<S>,
        start: Option<&Key>,
        _checkpoint_ts: TimeStamp,
    ) -> Result<(Vec<(Key, Lock)>, bool)> {
        let (locks, has_remaining) =
            reader.scan_locks(start, None, |_| true, DEFAULT_SCAN_BATCH_SIZE)?;
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
