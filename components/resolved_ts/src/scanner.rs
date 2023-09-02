// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use engine_traits::KvEngine;
use futures::compat::Future01CompatExt;
use futures_timer::Delay;
use kvproto::metapb::Region;
use raftstore::{
    coprocessor::ObserveHandle,
    router::CdcHandle,
    store::{fsm::ChangeObserver, msg::Callback, RegionSnapshot},
};
use tikv::storage::{
    kv::{ScanMode as MvccScanMode, Snapshot},
    mvcc::MvccReader,
    txn::TxnEntry,
};
use tikv_util::{
    sys::thread::ThreadBuildWrapper, time::Instant, timer::GLOBAL_TIMER_HANDLE, worker::Scheduler,
};
use tokio::runtime::{Builder, Runtime};
use txn_types::{Key, Lock, LockType, TimeStamp};

use crate::{
    errors::{Error, Result},
    metrics::*,
    Task,
};

const DEFAULT_SCAN_BATCH_SIZE: usize = 1024;
const GET_SNAPSHOT_RETRY_TIME: u32 = 3;
const GET_SNAPSHOT_RETRY_BACKOFF_STEP: Duration = Duration::from_millis(100);

pub struct ScanTask {
    pub handle: ObserveHandle,
    pub region: Region,
    pub checkpoint_ts: TimeStamp,
    pub backoff: Option<Duration>,
    pub cancelled: Arc<AtomicBool>,
    pub scheduler: Scheduler<Task>,
}

impl ScanTask {
    async fn send_entries(&self, entries: Vec<ScanEntry>, apply_index: u64) {
        loop {
            if self.scheduler.pending_tasks() < 128 {
                break;
            }
            let f = Delay::new(Duration::from_millis(10));
            f.await;
        }
        let task = Task::ScanLocks {
            region_id: self.region.get_id(),
            observe_id: self.handle.id,
            entries,
            apply_index,
        };
        if let Err(e) = self.scheduler.schedule(task) {
            warn!("resolved_ts scheduler send entries failed"; "err" => ?e);
        }
        RTS_SCAN_TASKS.with_label_values(&["finish"]).inc();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn on_error(&self, err: Error) {
        if let Err(e) = self.scheduler.schedule(Task::ReRegisterRegion {
            region_id: self.region.get_id(),
            observe_id: self.handle.id,
            cause: err,
        }) {
            warn!("schedule re-register task failed";
                "region_id" => self.region.get_id(),
                "observe_id" => ?self.handle.id,
                "error" => ?e);
        }
        RTS_SCAN_TASKS.with_label_values(&["abort"]).inc();
    }
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
                if task.is_cancelled() {
                    return;
                }
            }
            let snap = match Self::get_snapshot(&mut task, cdc_handle).await {
                Ok(snap) => snap,
                Err(e) => {
                    warn!("resolved_ts scan get snapshot failed"; "err" => ?e);
                    task.on_error(e);
                    return;
                }
            };
            fail::fail_point!("resolved_ts_after_scanner_get_snapshot");
            let start = Instant::now();
            let apply_index = snap.get_apply_index().unwrap();
            let mut reader = MvccReader::new(snap, Some(MvccScanMode::Forward), false);
            let mut done = false;
            let mut start_key = None;
            while !done && !task.is_cancelled() {
                let (locks, has_remaining) =
                    match Self::scan_locks(&mut reader, start_key.as_ref(), task.checkpoint_ts) {
                        Ok(rs) => rs,
                        Err(e) => {
                            warn!("resolved_ts scan lock failed"; "err" => ?e);
                            task.on_error(e);
                            return;
                        }
                    };
                done = !has_remaining;
                if has_remaining {
                    start_key = Some(locks.last().unwrap().0.clone())
                }
                task.send_entries(vec![ScanEntry::Lock(locks)], apply_index)
                    .await;
            }
            RTS_SCAN_DURATION_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
            task.send_entries(vec![ScanEntry::None], apply_index).await;
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
                if task.is_cancelled() {
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
}
