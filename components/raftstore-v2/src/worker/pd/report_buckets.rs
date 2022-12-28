// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{KvEngine, RaftEngine};
use pd_client::{merge_bucket_stats, BucketStat, PdClient};
use raftstore::store::ReportBucket;
use slog::debug;
use tikv_util::time::UnixSecs;

use super::Runner;

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn handle_report_region_buckets(&mut self, region_buckets: BucketStat) {
        let region_id = region_buckets.meta.region_id;
        self.merge_buckets(region_buckets);
        let report_buckets = self.region_buckets.get_mut(&region_id).unwrap();
        let last_report_ts = if report_buckets.last_report_ts().is_zero() {
            self.start_ts
        } else {
            report_buckets.last_report_ts()
        };
        let now = UnixSecs::now();
        let interval_second = now.into_inner() - last_report_ts.into_inner();
        let delta = report_buckets.new_report(now);
        let resp = self
            .pd_client
            .report_region_buckets(&delta, Duration::from_secs(interval_second));
        let logger = self.logger.clone();
        let f = async move {
            if let Err(e) = resp.await {
                debug!(
                    logger,
                    "failed to report buckets";
                    "version" => delta.meta.version,
                    "region_epoch" => ?delta.meta.region_epoch,
                    "err" => ?e
                );
            }
        };
        self.remote.spawn(f);
    }

    fn merge_buckets(&mut self, mut buckets: BucketStat) {
        let region_id = buckets.meta.region_id;
        self.region_buckets
            .entry(region_id)
            .and_modify(|report_bucket| {
                let current = report_bucket.current_stat_mut();
                if current.meta < buckets.meta {
                    std::mem::swap(current, &mut buckets);
                }

                merge_bucket_stats(
                    &current.meta.keys,
                    &mut current.stats,
                    &buckets.meta.keys,
                    &buckets.stats,
                );
            })
            .or_insert_with(|| ReportBucket::new(buckets));
    }
}
