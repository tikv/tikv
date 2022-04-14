// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use engine_traits::{KvEngine, CF_DEFAULT, CF_WRITE};

use futures::executor::block_on;
use raftstore::{
    coprocessor::{ObserveHandle, RegionInfoProvider},
    router::RaftStoreRouter,
    store::{fsm::ChangeObserver, Callback, SignificantMsg},
};
use tikv::storage::{
    kv::StatisticsSummary,
    mvcc::{DeltaScanner, ScannerBuilder},
    txn::{EntryBatch, TxnEntry, TxnEntryScanner},
    Snapshot, Statistics,
};
use tikv_util::{box_err, info, warn};
use txn_types::{Key, TimeStamp};

use crate::{
    errors::{ContextualResultExt, Error, Result},
    router::ApplyEvent,
    utils::RegionPager,
};
use crate::{
    metrics,
    router::{ApplyEvents, Router},
};

use kvproto::{kvrpcpb::ExtraOp, metapb::Region, raft_cmdpb::CmdType};

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

    /// scan a batch of events from the snapshot.
    /// note: maybe make something like [`EntryBatch`] for reducing allocation.
    fn scan_batch(&mut self, batch_size: usize, result: &mut ApplyEvents) -> Result<Statistics> {
        let mut b = EntryBatch::with_capacity(batch_size);
        self.scanner.scan_entries(&mut b)?;
        for entry in b.drain() {
            match entry {
                TxnEntry::Prewrite {
                    default: (key, value),
                    ..
                } => {
                    // FIXME: we also need to update the information for the `resolver` in the endpoint,
                    //        otherwise we may advance the resolved ts too far in some conditions?
                    if !key.is_empty() {
                        result.push(ApplyEvent {
                            key,
                            value,
                            cf: CF_DEFAULT,
                            cmd_type: CmdType::Put,
                        });
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
        Ok(self.scanner.take_statistics())
    }
}

/// The context for loading incremental data between range.
/// Like [`cdc::Initializer`], but supports initialize over range.
/// Note: maybe we can merge those two structures?
#[derive(Clone)]
#[allow(dead_code)]
pub struct InitialDataLoader<E, R, RT> {
    router: RT,
    regions: R,
    // Note: maybe we can make it an abstract thing like `EventSink` with
    //       method `async (KvEvent) -> Result<()>`?
    sink: Router,

    _engine: PhantomData<E>,
}

impl<E, R, RT> InitialDataLoader<E, R, RT>
where
    E: KvEngine,
    R: RegionInfoProvider + Clone + 'static,
    RT: RaftStoreRouter<E>,
{
    pub fn new(router: RT, regions: R, sink: Router) -> Self {
        Self {
            router,
            regions,
            sink,
            _engine: PhantomData,
        }
    }

    /// Start observe over some region.
    /// This will register the region to the raftstore as observing,
    /// and return the current snapshot of that region.
    pub fn observe_over(&self, region: &Region, cmd: ChangeObserver) -> Result<impl Snapshot> {
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
                            callback(Err(Error::RaftStore(
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
            .map_err(|err| Error::Other(Box::new(err)))
            .context(format_args!(
                "failed to register the observer to region {}",
                region.get_id()
            ))?;
        let snap = block_on(fut)
            .expect("BUG: channel of paired_future_callback canceled.")
            .context(format_args!(
                "failed to get initial snapshot: failed to get the snapshot (region_id = {})",
                region.get_id(),
            ))?;
        // Note: maybe warp the snapshot via `RegionSnapshot`?
        Ok(snap)
    }

    pub fn do_initial_scan(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        snap: impl Snapshot,
    ) -> Result<Statistics> {
        // It is ok to sink more data than needed. So scan to +inf TS for convenance.
        let mut event_loader = EventLoader::load_from(snap, start_ts, TimeStamp::max(), region)?;
        let mut stats = StatisticsSummary::default();
        loop {
            let mut events = ApplyEvents::with_capacity(1024, region.id);
            let stat = event_loader.scan_batch(1024, &mut events)?;
            if events.len() == 0 {
                break;
            }
            stats.add_statistics(&stat);
            let sink = self.sink.clone();
            // Note: maybe we'd better don't spawn it to another thread for preventing OOM?
            tokio::spawn(async move {
                metrics::INCREMENTAL_SCAN_SIZE.observe(events.size() as f64);
                if let Err(err) = sink.on_events(events).await {
                    warn!("failed to send event to sink"; "err" => %err);
                }
            });
        }
        Ok(stats.stat)
    }

    /// Initialize the region: register it to the raftstore and the observer.
    /// At the same time, perform the initial scanning, (an incremental scanning from `start_ts`)
    /// and generate the corresponding ApplyEvent to the sink directly.
    pub fn initialize_region(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        cmd: ChangeObserver,
    ) -> Result<Statistics> {
        let snap = self.observe_over(region, cmd)?;
        self.do_initial_scan(region, start_ts, snap)
    }

    /// initialize a range: it simply scan the regions with leader role and send them to [`initialize_region`].
    pub fn initialize_range(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        start_ts: TimeStamp,
        mut on_register_range: impl FnMut(u64, ObserveHandle),
    ) -> Result<Statistics> {
        let mut pager = RegionPager::scan_from(self.regions.clone(), start_key, end_key);
        let mut total_stat = StatisticsSummary::default();
        loop {
            let regions = pager.next_page(8)?;
            info!("scanning for entries in region."; "regions" => ?regions);
            if regions.is_empty() {
                break;
            }
            for r in regions {
                let handle = ObserveHandle::new();
                let ob = ChangeObserver::from_cdc(r.region.get_id(), handle.clone());
                let stat = self.initialize_region(&r.region, start_ts, ob)?;
                on_register_range(r.region.get_id(), handle);
                total_stat.add_statistics(&stat);
            }
        }
        Ok(total_stat.stat)
    }
}
