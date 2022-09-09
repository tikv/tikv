// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    marker::PhantomData,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use engine_traits::{KvEngine, CF_DEFAULT, CF_WRITE};
use futures::executor::block_on;
use kvproto::{kvrpcpb::ExtraOp, metapb::Region, raft_cmdpb::CmdType};
use raftstore::{
    coprocessor::RegionInfoProvider,
    router::RaftStoreRouter,
    store::{fsm::ChangeObserver, Callback, SignificantMsg},
};
use tikv::storage::{
    kv::StatisticsSummary,
    mvcc::{DeltaScanner, ScannerBuilder},
    txn::{EntryBatch, TxnEntry, TxnEntryScanner},
    Snapshot, Statistics,
};
use tikv_util::{
    box_err,
    time::{Instant, Limiter},
    worker::Scheduler,
};
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use txn_types::{Key, Lock, TimeStamp};

use crate::{
    annotate, debug,
    endpoint::ObserveOp,
    errors::{ContextualResultExt, Error, Result},
    metrics,
    router::{ApplyEvent, ApplyEvents, Router},
    subscription_track::{SubscriptionTracer, TwoPhaseResolver},
    try_send,
    utils::{self, RegionPager},
    Task,
};

const MAX_GET_SNAPSHOT_RETRY: usize = 3;

#[derive(Clone)]
pub struct PendingMemoryQuota(Arc<Semaphore>);

impl std::fmt::Debug for PendingMemoryQuota {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingMemoryQuota")
            .field("remain", &self.0.available_permits())
            .field("total", &self.0)
            .finish()
    }
}

pub struct PendingMemory(OwnedSemaphorePermit);

impl PendingMemoryQuota {
    pub fn new(quota: usize) -> Self {
        Self(Arc::new(Semaphore::new(quota)))
    }

    pub fn pending(&self, size: usize) -> PendingMemory {
        PendingMemory(
            Handle::current()
                .block_on(self.0.clone().acquire_many_owned(size as _))
                .expect("BUG: the semaphore is closed unexpectedly."),
        )
    }
}

/// EventLoader transforms data from the snapshot into ApplyEvent.
pub struct EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,
    // pooling the memory.
    entry_batch: EntryBatch,
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
            entry_batch: EntryBatch::with_capacity(ENTRY_BATCH_SIZE),
        })
    }

    /// Scan a batch of events from the snapshot, and save them into the
    /// internal buffer.
    fn fill_entries(&mut self) -> Result<Statistics> {
        assert!(
            self.entry_batch.is_empty(),
            "EventLoader: the entry batch isn't empty when filling entries, which is error-prone, please call `omit_entries` first. (len = {})",
            self.entry_batch.len()
        );
        self.scanner.scan_entries(&mut self.entry_batch)?;
        Ok(self.scanner.take_statistics())
    }

    /// Drain the internal buffer, converting them to the [`ApplyEvents`],
    /// and tracking the locks at the same time.
    fn emit_entries_to(
        &mut self,
        result: &mut ApplyEvents,
        resolver: &mut TwoPhaseResolver,
    ) -> Result<()> {
        for entry in self.entry_batch.drain() {
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
                        resolver.track_phase_one_lock(lock.ts, lock_at);
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
/// Like [`cdc::Initializer`], but supports initialize over range.
/// Note: maybe we can merge those two structures?
/// Note': maybe extract more fields to trait so it would be easier to test.
#[derive(Clone)]
pub struct InitialDataLoader<E, R, RT> {
    // Note: maybe we can make it an abstract thing like `EventSink` with
    //       method `async (KvEvent) -> Result<()>`?
    pub(crate) sink: Router,
    pub(crate) tracing: SubscriptionTracer,
    pub(crate) scheduler: Scheduler<Task>,
    // Note: this is only for `init_range`, maybe make it an argument?
    pub(crate) regions: R,
    // Note: Maybe move those fields about initial scanning into some trait?
    pub(crate) router: RT,
    pub(crate) quota: PendingMemoryQuota,
    pub(crate) limit: Limiter,

    pub(crate) handle: Handle,
    _engine: PhantomData<E>,
}

impl<E, R, RT> InitialDataLoader<E, R, RT>
where
    E: KvEngine,
    R: RegionInfoProvider + Clone + 'static,
    RT: RaftStoreRouter<E>,
{
    pub fn new(
        router: RT,
        regions: R,
        sink: Router,
        tracing: SubscriptionTracer,
        sched: Scheduler<Task>,
        quota: PendingMemoryQuota,
        handle: Handle,
        limiter: Limiter,
    ) -> Self {
        Self {
            router,
            regions,
            sink,
            tracing,
            scheduler: sched,
            _engine: PhantomData,
            quota,
            handle,
            limit: limiter,
        }
    }

    pub fn observe_over_with_retry(
        &self,
        region: &Region,
        mut cmd: impl FnMut() -> ChangeObserver,
    ) -> Result<impl Snapshot> {
        let mut last_err = None;
        for _ in 0..MAX_GET_SNAPSHOT_RETRY {
            let r = self.observe_over(region, cmd());
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
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
            }
        }
        Err(last_err.expect("BUG: max retry time exceed but no error"))
    }

    /// Start observe over some region.
    /// This will register the region to the raftstore as observing,
    /// and return the current snapshot of that region.
    fn observe_over(&self, region: &Region, cmd: ChangeObserver) -> Result<impl Snapshot> {
        // There are 2 ways for getting the initial snapshot of a region:
        // - the BR method: use the interface in the RaftKv interface, read the
        //   key-values directly.
        // - the CDC method: use the raftstore message `SignificantMsg::CaptureChange`
        //   to register the region to CDC observer and get a snapshot at the same time.
        // Registering the observer to the raftstore is necessary because we should only
        // listen events from leader. In CDC, the change observer is
        // per-delegate(i.e. per-region), we can create the command per-region here too.

        let (callback, fut) =
            tikv_util::future::paired_future_callback::<std::result::Result<_, Error>>();
        self.router
            .significant_send(
                region.id,
                SignificantMsg::CaptureChange {
                    cmd,
                    region_epoch: region.get_region_epoch().clone(),
                    callback: Callback::read(Box::new(|snapshot| {
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
                },
            )
            .context(format_args!(
                "failed to register the observer to region {}",
                region.get_id()
            ))?;
        let snap = block_on(fut)
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

    pub fn with_resolver<T: 'static>(
        &self,
        region: &Region,
        f: impl FnOnce(&mut TwoPhaseResolver) -> Result<T>,
    ) -> Result<T> {
        Self::with_resolver_by(&self.tracing, region, f)
    }

    pub fn with_resolver_by<T: 'static>(
        tracing: &SubscriptionTracer,
        region: &Region,
        f: impl FnOnce(&mut TwoPhaseResolver) -> Result<T>,
    ) -> Result<T> {
        let region_id = region.get_id();
        let mut v = tracing
            .get_subscription_of(region_id)
            .ok_or_else(|| Error::Other(box_err!("observer for region {} canceled", region_id)))
            .and_then(|v| {
                raftstore::store::util::compare_region_epoch(
                    region.get_region_epoch(),
                    &v.value().meta,
                    // No need for checking conf version because conf change won't cancel the
                    // observation.
                    false,
                    true,
                    false,
                )?;
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

    fn scan_and_async_send(
        &self,
        region: &Region,
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
            let (stat, disk_read) =
                utils::with_record_read_throughput(|| event_loader.fill_entries());
            // We must use the size of entry batch here to check whether we have progress.
            // Or we may exit too early if there are only records:
            // - can be inlined to `write` CF (hence it won't be written to default CF)
            // - are prewritten. (hence it will only contains `Prewrite` records).
            // In this condition, ALL records generate no ApplyEvent(only lock change),
            // and we would exit after the first run of loop :(
            let no_progress = event_loader.entry_batch.is_empty();
            let stat = stat?;
            self.with_resolver(region, |r| event_loader.emit_entries_to(&mut events, r))?;
            if no_progress {
                metrics::INITIAL_SCAN_DURATION.observe(start.saturating_elapsed_secs());
                return Ok(stats.stat);
            }
            stats.add_statistics(&stat);
            let region_id = region.get_id();
            let sink = self.sink.clone();
            let event_size = events.size();
            let sched = self.scheduler.clone();
            let permit = self.quota.pending(event_size);
            self.limit.blocking_consume(disk_read as _);
            debug!("sending events to router"; "size" => %event_size, "region" => %region_id);
            metrics::INCREMENTAL_SCAN_SIZE.observe(event_size as f64);
            metrics::INCREMENTAL_SCAN_DISK_READ.inc_by(disk_read as f64);
            metrics::HEAP_MEMORY.add(event_size as _);
            join_handles.push(tokio::spawn(async move {
                utils::handle_on_event_result(&sched, sink.on_events(events).await);
                metrics::HEAP_MEMORY.sub(event_size as _);
                debug!("apply event done"; "size" => %event_size, "region" => %region_id);
                drop(permit);
            }));
        }
    }

    pub fn do_initial_scan(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        snap: impl Snapshot,
    ) -> Result<Statistics> {
        let _guard = self.handle.enter();
        let tr = self.tracing.clone();
        let region_id = region.get_id();

        let mut join_handles = Vec::with_capacity(8);

        // It is ok to sink more data than needed. So scan to +inf TS for convenance.
        let event_loader = EventLoader::load_from(snap, start_ts, TimeStamp::max(), region)?;
        let stats = self.scan_and_async_send(region, event_loader, &mut join_handles)?;

        Handle::current()
            .block_on(futures::future::try_join_all(join_handles))
            .map_err(|err| annotate!(err, "tokio runtime failed to join consuming threads"))?;

        Self::with_resolver_by(&tr, region, |r| {
            r.phase_one_done();
            Ok(())
        })
        .context(format_args!(
            "failed to finish phase 1 for region {:?}",
            region_id
        ))?;

        Ok(stats)
    }

    /// initialize a range: it simply scan the regions with leader role and send
    /// them to [`initialize_region`].
    pub fn initialize_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        let mut pager = RegionPager::scan_from(self.regions.clone(), start_key, end_key);
        loop {
            let regions = pager.next_page(8)?;
            debug!("scanning for entries in region."; "regions" => ?regions);
            if regions.is_empty() {
                break;
            }
            for r in regions {
                // Note: Even we did the initial scanning, and blocking resolved ts from
                // advancing, if the next_backup_ts was updated in some extreme condition, there
                // is still little chance to lost data: For example, if a region cannot elect
                // the leader for long time. (say, net work partition) At that time, we have
                // nowhere to record the lock status of this region.
                let success = try_send!(
                    self.scheduler,
                    Task::ModifyObserve(ObserveOp::Start { region: r.region })
                );
                if success {
                    crate::observer::IN_FLIGHT_START_OBSERVE_MESSAGE.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use kvproto::metapb::*;
    use tikv::storage::{txn::tests::*, TestEngineBuilder};
    use tikv_kv::SnapContext;
    use txn_types::TimeStamp;

    use super::EventLoader;
    use crate::{
        router::ApplyEvents, subscription_track::TwoPhaseResolver,
        utils::with_record_read_throughput,
    };

    #[test]
    fn test_disk_read() {
        let engine = TestEngineBuilder::new().build_without_cache().unwrap();
        for i in 0..100 {
            let owned_key = format!("{:06}", i);
            let key = owned_key.as_bytes();
            let owned_value = [i as u8; 512];
            let value = owned_value.as_slice();
            must_prewrite_put(&engine, key, value, key, i * 2);
            must_commit(&engine, key, i * 2, i * 2 + 1);
        }
        // let compact the memtable to disk so we can see the disk read.
        engine.get_rocksdb().as_inner().compact_range(None, None);

        let mut r = Region::new();
        r.set_id(42);
        r.set_start_key(b"".to_vec());
        r.set_end_key(b"".to_vec());

        let snap =
            block_on(async { tikv_kv::snapshot(&engine, SnapContext::default()).await }).unwrap();
        let mut loader =
            EventLoader::load_from(snap, TimeStamp::zero(), TimeStamp::max(), &r).unwrap();

        let (r, data_load) = with_record_read_throughput(|| loader.fill_entries());
        r.unwrap();
        let mut events = ApplyEvents::with_capacity(1024, 42);
        let mut res = TwoPhaseResolver::new(42, None);
        loader.emit_entries_to(&mut events, &mut res).unwrap();
        assert_ne!(events.len(), 0);
        assert_ne!(data_load, 0);
    }
}
