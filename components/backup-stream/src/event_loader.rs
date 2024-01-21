// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, sync::Arc, time::Duration};

use engine_traits::{KvEngine, CF_DEFAULT, CF_WRITE};
use kvproto::{kvrpcpb::ExtraOp, metapb::Region, raft_cmdpb::CmdType};
use raftstore::{
    coprocessor::ObserveHandle,
    router::CdcHandle,
    store::{fsm::ChangeObserver, Callback},
};
use tikv::storage::{
    kv::StatisticsSummary,
    mvcc::{DeltaScanner, ScannerBuilder},
    txn::{TxnEntry, TxnEntryScanner},
    Snapshot, Statistics,
};
use tikv_util::{
    box_err,
    memory::{MemoryQuota, OwnedAllocated},
    time::{Instant, Limiter},
    worker::Scheduler,
};
use tokio::sync::Semaphore;
use txn_types::{Key, Lock, TimeStamp};

use crate::{
    annotate, debug,
    errors::{ContextualResultExt, Error, Result},
    metrics,
    router::{ApplyEvent, ApplyEvents, Router},
    subscription_track::{Ref, RefMut, SubscriptionTracer, TwoPhaseResolver},
    utils, Task,
};

const MAX_GET_SNAPSHOT_RETRY: usize = 5;
/// The threshold of slowing down initial scanning.
/// While the memory usage reaches this ratio, we will consume the result of
/// initial scanning more frequently.
const SLOW_DOWN_INITIAL_SCAN_RATIO: f64 = 0.7;

struct ScanResult {
    more: bool,
    out_of_memory: bool,
    statistics: Statistics,
}

/// EventLoader transforms data from the snapshot into ApplyEvent.
pub struct EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,
    // pooling the memory.
    region: Region,
    entry_batch: Vec<TxnEntry>,
}

const ENTRY_BATCH_SIZE: usize = 1024;

impl<S: Snapshot> EventLoader<S> {
    pub fn load_from(
        snapshot: S,
        from_ts: TimeStamp,
        to_ts: TimeStamp,
        region: &Region,
    ) -> Result<Self> {
        let region_id = region.get_id();
        let scanner = ScannerBuilder::new(snapshot, to_ts)
            .range(
                (!region.start_key.is_empty()).then(|| Key::from_encoded_slice(&region.start_key)),
                (!region.end_key.is_empty()).then(|| Key::from_encoded_slice(&region.end_key)),
            )
            .hint_min_ts(Some(from_ts))
            .fill_cache(false)
            .build_delta_scanner(from_ts, ExtraOp::Noop)
            .map_err(|err| Error::Txn(err.into()))
            .context(format_args!(
                "failed to create entry scanner from_ts = {}, to_ts = {}, region = {}",
                from_ts, to_ts, region_id
            ))?;

        Ok(Self {
            scanner,
            region: region.clone(),
            entry_batch: Vec::with_capacity(ENTRY_BATCH_SIZE),
        })
    }

    fn scan_result(&mut self, more: bool) -> ScanResult {
        ScanResult {
            more,
            out_of_memory: false,
            statistics: self.scanner.take_statistics(),
        }
    }

    fn out_of_memory(&mut self) -> ScanResult {
        ScanResult {
            more: true,
            out_of_memory: true,
            statistics: self.scanner.take_statistics(),
        }
    }

    /// Scan a batch of events from the snapshot, and save them into the
    /// internal buffer.
    fn fill_entries(&mut self, memory_quota: &mut OwnedAllocated) -> Result<ScanResult> {
        assert!(
            self.entry_batch.is_empty(),
            "EventLoader: the entry batch isn't empty when filling entries, which is error-prone, please call `emit_entries_to` first. (len = {})",
            self.entry_batch.len()
        );
        let batch = &mut self.entry_batch;
        while batch.len() < batch.capacity() {
            match self.scanner.next_entry()? {
                Some(entry) => {
                    let size = entry.size();
                    batch.push(entry);
                    if memory_quota.alloc(size).is_err()
                        || memory_quota.source().used_ratio() > SLOW_DOWN_INITIAL_SCAN_RATIO
                    {
                        return Ok(self.out_of_memory());
                    }
                }
                None => return Ok(self.scan_result(false)),
            }
        }
        Ok(self.scan_result(true))
    }

    /// Drain the internal buffer, converting them to the [`ApplyEvents`],
    /// and tracking the locks at the same time.
    fn emit_entries_to(
        &mut self,
        result: &mut ApplyEvents,
        resolver: &mut TwoPhaseResolver,
    ) -> Result<()> {
        for entry in self.entry_batch.drain(..) {
            match entry {
                TxnEntry::Prewrite {
                    default: (key, value),
                    lock: (lock_at, lock_value),
                    ..
                } => {
                    if !key.is_empty() {
                        result.push(ApplyEvent {
                            key,
                            value,
                            cf: CF_DEFAULT,
                            cmd_type: CmdType::Put,
                        });
                    }
                    let lock = Lock::parse(&lock_value).map_err(|err| {
                        annotate!(
                            err,
                            "BUG?: failed to parse ts from lock; key = {}",
                            utils::redact(&lock_at)
                        )
                    })?;
                    debug!("meet lock during initial scanning."; "key" => %utils::redact(&lock_at), "ts" => %lock.ts);
                    if utils::should_track_lock(&lock) {
                        resolver
                            .track_phase_one_lock(lock.ts, lock_at)
                            .map_err(|_| Error::OutOfQuota {
                                region_id: self.region.id,
                            })?;
                    }
                }
                TxnEntry::Commit { default, write, .. } => {
                    result.push(ApplyEvent {
                        key: write.0,
                        value: write.1,
                        cf: CF_WRITE,
                        cmd_type: CmdType::Put,
                    });
                    if !default.0.is_empty() {
                        result.push(ApplyEvent {
                            key: default.0,
                            value: default.1,
                            cf: CF_DEFAULT,
                            cmd_type: CmdType::Put,
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

/// The context for loading incremental data between range.
/// Like [`cdc::Initializer`].
/// Note: maybe we can merge those two structures?
#[derive(Clone)]
pub struct InitialDataLoader<E: KvEngine, H> {
    // Note: maybe we can make it an abstract thing like `EventSink` with
    //       method `async (KvEvent) -> Result<()>`?
    pub(crate) sink: Router,
    pub(crate) tracing: SubscriptionTracer,
    pub(crate) scheduler: Scheduler<Task>,

    pub(crate) quota: Arc<MemoryQuota>,
    pub(crate) limit: Limiter,
    // If there are too many concurrent initial scanning, the limit of disk speed or pending memory
    // quota will probably be triggered. Then the whole scanning will be pretty slow. And when
    // we are holding a iterator for a long time, the memtable may not be able to be flushed.
    // Using this to restrict the possibility of that.
    concurrency_limit: Arc<Semaphore>,

    cdc_handle: H,

    _engine: PhantomData<E>,
}

impl<E, H> InitialDataLoader<E, H>
where
    E: KvEngine,
    H: CdcHandle<E> + Sync,
{
    pub fn new(
        sink: Router,
        tracing: SubscriptionTracer,
        sched: Scheduler<Task>,
        quota: Arc<MemoryQuota>,
        limiter: Limiter,
        cdc_handle: H,
        concurrency_limit: Arc<Semaphore>,
    ) -> Self {
        Self {
            sink,
            tracing,
            scheduler: sched,
            _engine: PhantomData,
            quota,
            cdc_handle,
            concurrency_limit,
            limit: limiter,
        }
    }

    pub async fn capture_change(
        &self,
        region: &Region,
        cmd: ChangeObserver,
    ) -> Result<impl Snapshot> {
        let (callback, fut) =
            tikv_util::future::paired_future_callback::<std::result::Result<_, Error>>();

        self.cdc_handle
            .capture_change(
                region.get_id(),
                region.get_region_epoch().clone(),
                cmd,
                Callback::read(Box::new(|snapshot| {
                    if snapshot.response.get_header().has_error() {
                        callback(Err(Error::RaftRequest(
                            snapshot.response.get_header().get_error().clone(),
                        )));
                        return;
                    }
                    if let Some(snap) = snapshot.snapshot {
                        callback(Ok(snap));
                        return;
                    }
                    callback(Err(Error::Other(box_err!(
                        "PROBABLY BUG: the response contains neither error nor snapshot"
                    ))))
                })),
            )
            .context(format_args!(
                "failed to register the observer to region {}",
                region.get_id()
            ))?;

        let snap = fut
            .await
            .map_err(|err| {
                annotate!(
                    err,
                    "message 'CaptureChange' dropped for region {}",
                    region.id
                )
            })
            .flatten()
            .context(format_args!(
                "failed to get initial snapshot: failed to get the snapshot (region_id = {})",
                region.get_id(),
            ))?;
        // Note: maybe warp the snapshot via `RegionSnapshot`?
        Ok(snap)
    }

    pub async fn observe_over_with_retry(
        &self,
        region: &Region,
        mut cmd: impl FnMut() -> ChangeObserver,
    ) -> Result<impl Snapshot> {
        let mut last_err = None;
        for _ in 0..MAX_GET_SNAPSHOT_RETRY {
            let c = cmd();
            let r = self.capture_change(region, c).await;
            match r {
                Ok(s) => {
                    return Ok(s);
                }
                Err(e) => {
                    let can_retry = match e.without_context() {
                        Error::RaftRequest(pbe) => {
                            !(pbe.has_epoch_not_match()
                                || pbe.has_not_leader()
                                || pbe.get_message().contains("stale observe id")
                                || pbe.has_region_not_found())
                        }
                        Error::RaftStore(raftstore::Error::RegionNotFound(_))
                        | Error::RaftStore(raftstore::Error::NotLeader(..)) => false,
                        _ => true,
                    };
                    e.report(format_args!(
                        "during getting initial snapshot for region {:?}; can retry = {}",
                        region, can_retry
                    ));
                    last_err = match last_err {
                        None => Some(e),
                        Some(err) => Some(Error::Contextual {
                            context: format!("and error {}", err),
                            inner_error: Box::new(e),
                        }),
                    };

                    if !can_retry {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
        Err(last_err.expect("BUG: max retry time exceed but no error"))
    }

    fn with_resolver<T: 'static>(
        &self,
        region: &Region,
        handle: &ObserveHandle,
        f: impl FnOnce(&mut TwoPhaseResolver) -> Result<T>,
    ) -> Result<T> {
        Self::with_resolver_by(&self.tracing, region, handle, f)
    }

    fn with_resolver_by<T: 'static>(
        tracing: &SubscriptionTracer,
        region: &Region,
        handle: &ObserveHandle,
        f: impl FnOnce(&mut TwoPhaseResolver) -> Result<T>,
    ) -> Result<T> {
        let region_id = region.get_id();
        let mut v = tracing
            .get_subscription_of(region_id)
            .ok_or_else(|| Error::Other(box_err!("observer for region {} canceled", region_id)))
            .and_then(|v| {
                // NOTE: once we have compared the observer handle, perhaps we can remove this 
                // check because epoch version changed implies observer handle changed.
                raftstore::store::util::compare_region_epoch(
                    region.get_region_epoch(),
                    &v.value().meta,
                    // No need for checking conf version because conf change won't cancel the
                    // observation.
                    false,
                    true,
                    false,
                )?;
                if v.value().handle().id != handle.id {
                    return Err(box_err!("stale observe handle {:?}, should be {:?}, perhaps new initial scanning starts", 
                        handle.id, v.value().handle().id));
                }
                Ok(v)
            })
            .map_err(|err| Error::Contextual {
                // Both when we cannot find the region in the track and the epoch has changed means
                // that we should cancel the current turn of initial scanning.
                inner_error: Box::new(Error::ObserveCanceled(
                    region_id,
                    region.get_region_epoch().clone(),
                )),
                context: format!("{}", err),
            })?;
        f(v.value_mut().resolver())
    }

    async fn scan_and_async_send(
        &self,
        region: &Region,
        handle: &ObserveHandle,
        mut event_loader: EventLoader<impl Snapshot>,
        join_handles: &mut Vec<tokio::task::JoinHandle<()>>,
    ) -> Result<Statistics> {
        let mut stats = StatisticsSummary::default();
        let start = Instant::now();
        loop {
            fail::fail_point!("scan_and_async_send", |msg| Err(Error::Other(box_err!(
                "{:?}", msg
            ))));
            let mut events = ApplyEvents::with_capacity(1024, region.id);
            // Note: the call of `fill_entries` is the only step which would read the disk.
            //       we only need to record the disk throughput of this.
            let mut allocated = OwnedAllocated::new(Arc::clone(&self.quota));
            let (res, disk_read) =
                utils::with_record_read_throughput(|| event_loader.fill_entries(&mut allocated));
            let res = res?;
            self.with_resolver(region, handle, |r| {
                event_loader.emit_entries_to(&mut events, r)
            })?;
            stats.add_statistics(&res.statistics);
            let region_id = region.get_id();
            let sink = self.sink.clone();
            let event_size = events.size();
            let sched = self.scheduler.clone();
            self.limit.consume(disk_read as _).await;
            debug!("sending events to router"; "size" => %event_size, "region" => %region_id);
            metrics::INCREMENTAL_SCAN_SIZE.observe(event_size as f64);
            metrics::INCREMENTAL_SCAN_DISK_READ.inc_by(disk_read as f64);
            metrics::HEAP_MEMORY.add(event_size as _);
            fail::fail_point!("scan_and_async_send::about_to_consume");
            join_handles.push(tokio::spawn(async move {
                utils::handle_on_event_result(&sched, sink.on_events(events).await);
                metrics::HEAP_MEMORY.sub(event_size as _);
                drop(allocated);
                debug!("apply event done"; "size" => %event_size, "region" => %region_id);
            }));
            if !res.more {
                metrics::INITIAL_SCAN_DURATION.observe(start.saturating_elapsed_secs());
                return Ok(stats.stat);
            }
            if res.out_of_memory {
                futures::future::try_join_all(join_handles.drain(..))
                    .await
                    .map_err(|err| {
                        annotate!(
                            err,
                            "failed to join tokio runtime during out-of-memory-quota"
                        )
                    })?;
            }
        }
    }

    pub async fn do_initial_scan(
        &self,
        region: &Region,
        // We are using this handle for checking whether the initial scan is stale.
        handle: ObserveHandle,
        start_ts: TimeStamp,
        snap: impl Snapshot,
    ) -> Result<Statistics> {
<<<<<<< HEAD
        let tr = self.tracing.clone();
        let region_id = region.get_id();

=======
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
        let mut join_handles = Vec::with_capacity(8);

        let permit = self
            .concurrency_limit
            .acquire()
            .await
            .expect("BUG: semaphore closed");
        // It is ok to sink more data than needed. So scan to +inf TS for convenance.
        let event_loader = EventLoader::load_from(snap, start_ts, TimeStamp::max(), region)?;
        let stats = self
            .scan_and_async_send(region, &handle, event_loader, &mut join_handles)
            .await?;
        drop(permit);

        futures::future::try_join_all(join_handles)
            .await
            .map_err(|err| annotate!(err, "tokio runtime failed to join consuming threads"))?;

<<<<<<< HEAD
        Self::with_resolver_by(&tr, region, &handle, |r| {
            r.phase_one_done();
            Ok(())
        })
        .context(format_args!(
            "failed to finish phase 1 for region {:?}",
            region_id
        ))?;

=======
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::executor::block_on;
    use kvproto::metapb::*;
    use tikv::storage::{txn::tests::*, TestEngineBuilder};
    use tikv_kv::SnapContext;
    use tikv_util::memory::{MemoryQuota, OwnedAllocated};
    use txn_types::TimeStamp;

    use super::EventLoader;
    use crate::{
        router::ApplyEvents, subscription_track::TwoPhaseResolver,
        utils::with_record_read_throughput,
    };

    #[test]
    fn test_disk_read() {
        let mut engine = TestEngineBuilder::new().build_without_cache().unwrap();
        for i in 0..100 {
            let owned_key = format!("{:06}", i);
            let key = owned_key.as_bytes();
            let owned_value = [i as u8; 512];
            let value = owned_value.as_slice();
            must_prewrite_put(&mut engine, key, value, key, i * 2);
            must_commit(&mut engine, key, i * 2, i * 2 + 1);
        }
        // let compact the memtable to disk so we can see the disk read.
        engine.get_rocksdb().as_inner().compact_range(None, None);

        let mut r = Region::new();
        r.set_id(42);
        r.set_start_key(b"".to_vec());
        r.set_end_key(b"".to_vec());

        let snap = block_on(async { tikv_kv::snapshot(&mut engine, SnapContext::default()).await })
            .unwrap();
        let quota_inf = Arc::new(MemoryQuota::new(usize::MAX));
        let mut loader =
            EventLoader::load_from(snap, TimeStamp::zero(), TimeStamp::max(), &r).unwrap();

        let (r, data_load) = with_record_read_throughput(|| {
            loader.fill_entries(&mut OwnedAllocated::new(quota_inf))
        });
        r.unwrap();
        let mut events = ApplyEvents::with_capacity(1024, 42);
        let mut res = TwoPhaseResolver::new(42, None);
        loader.emit_entries_to(&mut events, &mut res).unwrap();
        assert_ne!(events.len(), 0);
        assert_ne!(data_load, 0);
    }
}
