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
use tikv_util::{box_err, time::Instant, warn, worker::Scheduler};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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
            tokio::runtime::Handle::current()
                .block_on(self.0.clone().acquire_many_owned(size as _))
                .expect("BUG: the semaphore is closed unexpectedly."),
        )
    }
}

/// EventLoader transforms data from the snapshot into ApplyEvent.
pub struct EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,
}

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
                Some(Key::from_encoded_slice(&region.start_key)),
                Some(Key::from_encoded_slice(&region.end_key)),
            )
            .hint_min_ts(Some(from_ts))
            .fill_cache(false)
            .build_delta_scanner(from_ts, ExtraOp::Noop)
            .map_err(|err| Error::Txn(err.into()))
            .context(format_args!(
                "failed to create entry scanner from_ts = {}, to_ts = {}, region = {}",
                from_ts, to_ts, region_id
            ))?;

        Ok(Self { scanner })
    }

    /// scan a batch of events from the snapshot. Tracking the locks at the same time.
    /// note: maybe make something like [`EntryBatch`] for reducing allocation.
    fn scan_batch(
        &mut self,
        batch_size: usize,
        result: &mut ApplyEvents,
        resolver: &mut TwoPhaseResolver,
    ) -> Result<Statistics> {
        let mut b = EntryBatch::with_capacity(batch_size);
        self.scanner.scan_entries(&mut b)?;
        for entry in b.drain() {
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
                    resolver.track_phase_one_lock(lock.ts, lock_at)
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
        Ok(self.scanner.take_statistics())
    }
}

/// The context for loading incremental data between range.
/// Like [`cdc::Initializer`], but supports initialize over range.
/// Note: maybe we can merge those two structures?
#[derive(Clone)]
pub struct InitialDataLoader<E, R, RT> {
    router: RT,
    regions: R,
    // Note: maybe we can make it an abstract thing like `EventSink` with
    //       method `async (KvEvent) -> Result<()>`?
    sink: Router,
    tracing: SubscriptionTracer,
    scheduler: Scheduler<Task>,
    quota: PendingMemoryQuota,
    handle: tokio::runtime::Handle,

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
        handle: tokio::runtime::Handle,
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
                                || pbe.get_message().contains("stale observe id"))
                        }
                        Error::RaftStore(raftstore::Error::RegionNotFound(_))
                        | Error::RaftStore(raftstore::Error::NotLeader(..)) => false,
                        _ => true,
                    };
                    last_err = match last_err {
                        None => Some(e),
                        Some(err) => Some(Error::Contextual {
                            context: format!("and error {}", e),
                            inner_error: Box::new(err),
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
        //   1. the BR method: use the interface in the RaftKv interface, read the key-values directly.
        //   2. the CDC method: use the raftstore message `SignificantMsg::CaptureChange` to
        //      register the region to CDC observer and get a snapshot at the same time.
        // Registering the observer to the raftstore is necessary because we should only listen events from leader.
        // In CDC, the change observer is per-delegate(i.e. per-region), we can create the command per-region here too.

        let (callback, fut) =
            tikv_util::future::paired_future_callback::<std::result::Result<_, Error>>();
        self.router
            .significant_send(
                region.id,
                SignificantMsg::CaptureChange {
                    cmd,
                    region_epoch: region.get_region_epoch().clone(),
                    callback: Callback::Read(Box::new(|snapshot| {
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
                    // No need for checking conf version because conf change won't cancel the observation.
                    false,
                    true,
                    false,
                )?;
                Ok(v)
            })
            .map_err(|err| Error::Contextual {
                // Both when we cannot find the region in the track and
                // the epoch has changed means that we should cancel the current turn of initial scanning.
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
            let mut events = ApplyEvents::with_capacity(1024, region.id);
            let stat =
                self.with_resolver(region, |r| event_loader.scan_batch(1024, &mut events, r))?;
            if events.is_empty() {
                metrics::INITIAL_SCAN_DURATION.observe(start.saturating_elapsed_secs());
                return Ok(stats.stat);
            }
            stats.add_statistics(&stat);
            let region_id = region.get_id();
            let sink = self.sink.clone();
            let event_size = events.size();
            let sched = self.scheduler.clone();
            let permit = self.quota.pending(event_size);
            debug!("sending events to router"; "size" => %event_size, "region" => %region_id);
            metrics::INCREMENTAL_SCAN_SIZE.observe(event_size as f64);
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
        // It is ok to sink more data than needed. So scan to +inf TS for convenance.
        let event_loader = EventLoader::load_from(snap, start_ts, TimeStamp::max(), region)?;
        let tr = self.tracing.clone();
        let region_id = region.get_id();

        let mut join_handles = Vec::with_capacity(8);
        let stats = self.scan_and_async_send(region, event_loader, &mut join_handles);

        // we should mark phase one as finished whether scan successed.
        // TODO: use an `WaitGroup` with asynchronous support.
        let r = region.clone();
        tokio::spawn(async move {
            for h in join_handles {
                if let Err(err) = h.await {
                    warn!("failed to join task."; "err" => %err);
                }
            }
            let result = Self::with_resolver_by(&tr, &r, |r| {
                r.phase_one_done();
                Ok(())
            });
            if let Err(err) = result {
                err.report(format_args!(
                    "failed to finish phase 1 for region {:?}",
                    region_id
                ));
            }
        });
        stats
    }

    /// initialize a range: it simply scan the regions with leader role and send them to [`initialize_region`].
    pub fn initialize_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<()> {
        let mut pager = RegionPager::scan_from(self.regions.clone(), start_key, end_key);
        loop {
            let regions = pager.next_page(8)?;
            debug!("scanning for entries in region."; "regions" => ?regions);
            if regions.is_empty() {
                break;
            }
            for r in regions {
                // Note: Even we did the initial scanning, and blocking resolved ts from advancing,
                //       if the next_backup_ts was updated in some extreme condition, there is still little chance to lost data:
                //       For example, if a region cannot elect the leader for long time. (say, net work partition)
                //       At that time, we have nowhere to record the lock status of this region.
                let success = try_send!(
                    self.scheduler,
                    Task::ModifyObserve(ObserveOp::Start {
                        region: r.region,
                        needs_initial_scanning: true
                    })
                );
                if success {
                    crate::observer::IN_FLIGHT_START_OBSERVE_MESSAGE.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }
}
