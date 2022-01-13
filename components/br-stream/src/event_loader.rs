// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_DEFAULT, CF_WRITE};

use raftstore::coprocessor::RegionInfoProvider;
use tikv::storage::{
    kv::{SnapContext, StatisticsSummary},
    mvcc::{DeltaScanner, ScannerBuilder},
    txn::{EntryBatch, TxnEntry, TxnEntryScanner},
    Engine, Snapshot, SnapshotStore, Statistics,
};
use tikv_util::{box_err, info, warn};
use txn_types::{Key, TimeStamp};

use crate::{
    errors::{Error, Result},
    metadata::store::MetaStore,
    utils::RegionPager,
};
use crate::{
    metrics,
    router::{ApplyEvent, Router},
};
use kvproto::kvrpcpb::IsolationLevel;
use kvproto::{
    kvrpcpb::{Context, ExtraOp},
    metapb::{Peer, Region},
};

pub struct EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,
    region_id: u64,
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
            .map_err(|err| {
                Error::Other(box_err!(
                    "failed to create entry scanner from_ts = {}, to_ts = {}, region = {}: {}",
                    from_ts,
                    to_ts,
                    region_id,
                    err
                ))
            })?;

        Ok(Self { scanner, region_id })
    }

    /// scan a batch of events from the snapshot.
    /// note: maybe make something like [`EntryBatch`] for reducing allocation.
    fn scan_batch(
        &mut self,
        batch_size: usize,
        result: &mut Vec<ApplyEvent>,
    ) -> Result<Statistics> {
        let mut b = EntryBatch::with_capacity(batch_size);
        self.scanner.scan_entries(&mut b)?;
        for entry in b.drain() {
            match entry {
                TxnEntry::Prewrite {
                    default: (key, value),
                    ..
                } => {
                    if !key.is_empty() {
                        result.push(ApplyEvent::from_prewrite(key, value, self.region_id));
                    }
                }
                TxnEntry::Commit { default, write, .. } => {
                    let write =
                        ApplyEvent::from_committed(CF_WRITE, write.0, write.1, self.region_id)?;
                    result.push(write);
                    if !default.0.is_empty() {
                        let default = ApplyEvent::from_committed(
                            CF_DEFAULT,
                            default.0,
                            default.1,
                            self.region_id,
                        )?;
                        result.push(default);
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
pub struct InitialDataLoader<E, R> {
    engine: E,
    regions: R,
    start_ts: TimeStamp,
    // Note: maybe we can make it an abstract thing like `EventSink` with
    //       method `async (KvEvent) -> Result<()>`?
    sink: Router,
    store_id: u64,
}

impl<E, R> InitialDataLoader<E, R>
where
    E: Engine,
    R: RegionInfoProvider + Clone + 'static,
{
    pub fn new(engine: E, regions: R, start_ts: TimeStamp, sink: Router, store_id: u64) -> Self {
        Self {
            engine,
            regions,
            start_ts,
            sink,
            store_id,
        }
    }

    fn find_peer<'a>(&self, region: &'a Region) -> Option<&'a Peer> {
        region
            .get_peers()
            .iter()
            .find(|peer| peer.get_store_id() == self.store_id)
    }

    pub fn initialize_region(&self, region: &Region) -> Result<Statistics> {
        // There are 2 ways for getting the initial snapshot of a region:
        //   1. the BR method: use the interface in the RaftKv interface, read the key-values directly.
        //   2. the CDC method: use the raftstore message `SignificantMsg::CaptureChange` to
        //      register the region to CDC observer and get a snapshot at the same time.
        // We use the BR method here for fast dev.
        // We need register the region as observed then.
        let mut region_ctx = Context::new();
        region_ctx.set_region_id(region.get_id());
        region_ctx.set_region_epoch(region.get_region_epoch().clone());
        region_ctx.set_peer(
            self.find_peer(&region)
                .ok_or_else(|| {
                    Error::Other(box_err!("failed to find peer from region {:?}", region))
                })?
                .clone(),
        );
        let ctx = SnapContext {
            pb_ctx: &region_ctx,
            ..Default::default()
        };
        let snap = self.engine.snapshot(ctx).map_err(|err| {
            Error::Other(box_err!(
                "failed to get snapshot for incremental scan (region id = {}): {}",
                region.get_id(),
                err
            ))
        })?;
        let mut event_loader =
            EventLoader::load_from(snap, self.start_ts, TimeStamp::max(), region)?;
        let mut events = Vec::with_capacity(2048);
        let stat = event_loader.scan_batch(1024, &mut events)?;
        info!("the only roll of scanning done"; "size" => %events.len());
        let sink = self.sink.clone();
        tokio::spawn(async move {
            for event in events {
                metrics::INCREMENTAL_SCAN_SIZE.observe(event.size() as f64);
                if let Err(err) = sink.on_event(event).await {
                    warn!("failed to send event to sink"; "err" => %err);
                }
            }
        });
        Ok(stat)
    }

    pub fn initialize_range(&self, start_key: Vec<u8>, end_key: Vec<u8>) -> Result<Statistics> {
        let mut pager = RegionPager::scan_from(self.regions.clone(), start_key, end_key);
        let mut total_stat = StatisticsSummary::default();
        loop {
            let regions = pager.next_page(8)?;
            info!("scanning for entries in region."; "regions" => ?regions);
            if regions.len() == 0 {
                break;
            }
            for r in regions {
                let stat = self.initialize_region(&r.region)?;
                total_stat.add_statistics(&stat);
            }
        }
        Ok(total_stat.stat)
    }
}
