// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc, time::Duration};

use engine_traits::KvEngine;
use futures::compat::Future01CompatExt;
use kvproto::{kvrpcpb::ExtraOp as TxnExtraOp, metapb::Region};
use raftstore::{
    coprocessor::{ObserveHandle, ObserveId},
    router::CdcHandle,
    store::{fsm::ChangeObserver, msg::Callback, RegionSnapshot},
};
use tikv::storage::{
    kv::{ScanMode as MvccScanMode, Snapshot},
    mvcc::{DeltaScanner, MvccReader, ScannerBuilder},
    txn::{TxnEntry, TxnEntryScanner},
};
use tikv_util::{sys::thread::ThreadBuildWrapper, time::Instant, timer::GLOBAL_TIMER_HANDLE};
use tokio::runtime::{Builder, Runtime};
use txn_types::{Key, Lock, LockType, TimeStamp};

use crate::{
    errors::{Error, Result},
    metrics::*,
};

const DEFAULT_SCAN_BATCH_SIZE: usize = 1024;
const GET_SNAPSHOT_RETRY_TIME: u32 = 3;
const GET_SNAPSHOT_RETRY_BACKOFF_STEP: Duration = Duration::from_millis(100);

pub type BeforeStartCallback = Box<dyn Fn() + Send>;
pub type OnErrorCallback = Box<dyn Fn(ObserveId, Region, Error) + Send>;
pub type OnEntriesCallback = Box<dyn Fn(Vec<ScanEntry>, u64) + Send>;
pub type IsCancelledCallback = Box<dyn Fn() -> bool + Send>;

pub enum ScanMode {
    LockOnly,
    All,
    AllWithOldValue,
}

pub struct ScanTask {
    pub handle: ObserveHandle,
    pub tag: String,
    pub mode: ScanMode,
    pub region: Region,
    pub checkpoint_ts: TimeStamp,
    pub backoff: Option<Duration>,
    pub is_cancelled: IsCancelledCallback,
    pub send_entries: OnEntriesCallback,
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
    cdc_handle: T,
    _phantom: PhantomData<E>,
}

impl<T: 'static + CdcHandle<E>, E: KvEngine> ScannerPool<T, E> {
    pub fn new(count: usize, cdc_handle: T) -> Self {
        let workers = Arc::new(
            Builder::new_multi_thread()
                .thread_name("inc-scan")
                .worker_threads(count)
                .with_sys_hooks()
                .build()
                .unwrap(),
        );
        Self {
            workers,
            cdc_handle,
            _phantom: PhantomData::default(),
        }
    }

    pub fn spawn_task(&self, mut task: ScanTask) {
        let cdc_handle = self.cdc_handle.clone();
        let fut = async move {
            if let Some(backoff) = task.backoff {
                RTS_INITIAL_SCAN_BACKOFF_DURATION_HISTOGRAM.observe(backoff.as_secs_f64());
                if let Err(e) = GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + backoff)
                    .compat()
                    .await
                {
                    error!("failed to backoff"; "err" => ?e);
                }
                if (task.is_cancelled)() {
                    return;
                }
            }
            let snap = match Self::get_snapshot(&mut task, cdc_handle).await {
                Ok(snap) => snap,
                Err(e) => {
                    warn!("resolved_ts scan get snapshot failed"; "err" => ?e);
                    let ScanTask {
                        on_error,
                        region,
                        handle,
                        ..
                    } = task;
                    if let Some(on_error) = on_error {
                        on_error(handle.id, region, e);
                    }
                    return;
                }
            };
            fail::fail_point!("resolved_ts_after_scanner_get_snapshot");
            let start = Instant::now();
            let apply_index = snap.get_apply_index().unwrap();
            let mut entries = vec![];
            match task.mode {
                ScanMode::All | ScanMode::AllWithOldValue => {
                    let txn_extra_op = if let ScanMode::AllWithOldValue = task.mode {
                        TxnExtraOp::ReadOldValue
                    } else {
                        TxnExtraOp::Noop
                    };
                    let mut scanner = ScannerBuilder::new(snap, TimeStamp::max())
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
                                    handle,
                                    ..
                                } = task;
                                if let Some(on_error) = on_error {
                                    on_error(handle.id, region, e);
                                }
                                return;
                            }
                        };
                        done = !has_remaining;
                        entries.push(ScanEntry::TxnEntry(es));
                    }
                }
                ScanMode::LockOnly => {
                    let mut reader = MvccReader::new(snap, Some(MvccScanMode::Forward), false);
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
                                        handle,
                                        ..
                                    } = task;
                                    if let Some(on_error) = on_error {
                                        on_error(handle.id, region, e);
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
            RTS_SCAN_DURATION_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
            (task.send_entries)(entries, apply_index);
        };
        self.workers.spawn(fut);
    }

    async fn get_snapshot(
        task: &mut ScanTask,
        cdc_handle: T,
    ) -> Result<RegionSnapshot<E::Snapshot>> {
        let mut last_err = None;
        for retry_times in 0..=GET_SNAPSHOT_RETRY_TIME {
            if retry_times != 0 {
                if let Err(e) = GLOBAL_TIMER_HANDLE
                    .delay(
                        std::time::Instant::now()
                            + GET_SNAPSHOT_RETRY_BACKOFF_STEP
                                .mul_f64(10_f64.powi(retry_times as i32 - 1)),
                    )
                    .compat()
                    .await
                {
                    error!("failed to backoff"; "err" => ?e);
                }
                if (task.is_cancelled)() {
                    return Err(box_err!("scan task cancelled"));
                }
            }
            let (cb, fut) = tikv_util::future::paired_future_callback();
            let change_cmd = ChangeObserver::from_rts(task.region.id, task.handle.clone());
            cdc_handle
                .capture_change(
                    task.region.id,
                    task.region.get_region_epoch().clone(),
                    change_cmd,
                    Callback::read(Box::new(cb)),
                )
                .map_err(|e| Error::Other(box_err!("{:?}", e)))?;
            let mut resp = box_try!(fut.await);
            if resp.response.get_header().has_error() {
                let err = resp.response.take_header().take_error();
                // These two errors can't handled by retrying since the epoch and observe id is
                // unchanged
                if err.has_epoch_not_match() || err.get_message().contains("stale observe id") {
                    return Err(box_err!("get snapshot failed: {:?}", err));
                }
                last_err = Some(err)
            } else {
                return Ok(resp.snapshot.unwrap());
            }
        }
        Err(box_err!(
            "backoff timeout after {} try, last error: {:?}",
            GET_SNAPSHOT_RETRY_TIME,
            last_err.unwrap()
        ))
    }

    fn scan_locks<S: Snapshot>(
        reader: &mut MvccReader<S>,
        start: Option<&Key>,
        _checkpoint_ts: TimeStamp,
    ) -> Result<(Vec<(Key, Lock)>, bool)> {
        let (locks, has_remaining) = reader
            .scan_locks(
                start,
                None,
                |lock| matches!(lock.lock_type, LockType::Put | LockType::Delete),
                DEFAULT_SCAN_BATCH_SIZE,
            )
            .map_err(|e| Error::Other(box_err!("{:?}", e)))?;
        Ok((locks, has_remaining))
    }

    fn scan_delta<S: Snapshot>(scanner: &mut DeltaScanner<S>) -> Result<(Vec<TxnEntry>, bool)> {
        let mut entries = Vec::with_capacity(DEFAULT_SCAN_BATCH_SIZE);
        let mut has_remaining = true;
        while entries.len() < entries.capacity() {
            match scanner
                .next_entry()
                .map_err(|e| Error::Other(box_err!("{:?}", e)))?
            {
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
