// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the interactions with bucket.

use std::sync::Arc;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::metapb::{self, RegionEpoch};
use pd_client::{BucketMeta, BucketStat};
use raftstore::{
    coprocessor::RegionChangeEvent,
    store::{util, Bucket, BucketRange, ReadProgress, SplitCheckTask, Transport},
};
use slog::{error, warn};

use crate::{
    batch::StoreContext,
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{ApplyTask, PeerTick},
    worker::pd,
};

#[derive(Debug, Clone, Default)]
pub struct BucketStatsInfo {
    bucket_stat: Option<BucketStat>,
    // the last buckets records the stats that the recently refreshed.
    last_bucket_stat: Option<BucketStat>,
    // the report bucket stat records the increment stats after last report pd.
    // it will be reset after report pd.
    report_bucket_stat: Option<BucketStat>,
}

impl BucketStatsInfo {
    /// returns all bucket ranges those's write_bytes exceed the given
    /// diff_size_threshold.
    pub fn gen_bucket_range_for_update(
        &self,
        diff_size_threshold: u64,
    ) -> Option<Vec<BucketRange>> {
        let region_buckets = self.bucket_stat.as_ref()?;
        let stats = &region_buckets.stats;
        let keys = &region_buckets.meta.keys;

        let empty_last_keys = vec![];
        let empty_last_stats = metapb::BucketStats::default();
        let (last_keys, last_stats, stats_reset) = self
            .last_bucket_stat
            .as_ref()
            .map(|b| {
                (
                    &b.meta.keys,
                    &b.stats,
                    region_buckets.create_time != b.create_time,
                )
            })
            .unwrap_or((&empty_last_keys, &empty_last_stats, false));

        let mut bucket_ranges = vec![];
        let mut j = 0;
        assert_eq!(keys.len(), stats.write_bytes.len() + 1);
        for i in 0..stats.write_bytes.len() {
            let mut diff_in_bytes = stats.write_bytes[i];
            while j < last_keys.len() && keys[i] > last_keys[j] {
                j += 1;
            }
            if j < last_keys.len() && keys[i] == last_keys[j] {
                if !stats_reset {
                    diff_in_bytes -= last_stats.write_bytes[j];
                }
                j += 1;
            }
            if diff_in_bytes >= diff_size_threshold {
                bucket_ranges.push(BucketRange(keys[i].clone(), keys[i + 1].clone()));
            }
        }
        Some(bucket_ranges)
    }

    #[inline]
    pub fn version(&self) -> u64 {
        self.bucket_stat
            .as_ref()
            .or(self.last_bucket_stat.as_ref())
            .map(|b| b.meta.version)
            .unwrap_or_default()
    }
    #[inline]
    pub fn add_bucket_flow(&mut self, delta: &Option<BucketStat>) {
        if let (Some(buckets), Some(report_buckets), Some(delta)) = (
            self.bucket_stat.as_mut(),
            self.report_bucket_stat.as_mut(),
            delta,
        ) {
            buckets.merge(delta);
            report_buckets.merge(delta);
        }
    }

    #[inline]
    pub fn set_bucket_stat(&mut self, buckets: Option<BucketStat>) {
        if let Some(b) = self.bucket_stat.take() {
            self.last_bucket_stat = Some(b);
        }
        self.report_bucket_stat = buckets.clone();
        self.bucket_stat = buckets;
    }

    #[inline]
    pub fn clear_bucket_stat(&mut self) {
        if let Some(bucket) = self.report_bucket_stat.as_mut() {
            bucket.clear_stats();
        }
    }

    #[inline]
    pub fn report_bucket_stat(&mut self) -> BucketStat {
        let current = self.report_bucket_stat.as_mut().unwrap();
        let delta = current.clone();
        current.clear_stats();
        delta
    }

    #[inline]
    pub fn bucket_stat(&self) -> &Option<BucketStat> {
        &self.bucket_stat
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_refresh_region_buckets<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        region_epoch: RegionEpoch,
        mut buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        // bucket version layout
        //   term       logical counter
        // |-----------|-----------|
        //  high bits     low bits
        // term: given 10s election timeout, the 32 bit means 1362 year running time
        let gen_bucket_version = |term, current_version| {
            let current_version_term = current_version >> 32;
            let bucket_version: u64 = if current_version_term == term {
                current_version + 1
            } else {
                if term > u32::MAX.into() {
                    error!(
                        self.logger,
                        "unexpected term {} more than u32::MAX. Bucket
                    version will be backward.",
                        term
                    );
                }
                term << 32
            };
            bucket_version
        };

        let region = self.region();
        let current_version = self.region_buckets_info().version();
        let mut region_buckets: BucketStat;
        // The region buckets reset after this region happened split or merge.
        // The message should be dropped if it's epoch is lower than the regions.
        // The bucket ranges is none when the region buckets is also none.
        // So this condition indicates that the region buckets needs to refresh not
        // renew.
        if let (Some(bucket_ranges), Some(peer_region_buckets)) =
            (bucket_ranges, self.region_buckets_info().bucket_stat())
        {
            assert_eq!(buckets.len(), bucket_ranges.len());
            let mut meta_idx = 0;
            region_buckets = peer_region_buckets.clone();
            let mut meta = (*region_buckets.meta).clone();
            if !buckets.is_empty() {
                meta.version = gen_bucket_version(self.term(), current_version);
            }
            meta.region_epoch = region_epoch;
            for (bucket, bucket_range) in buckets.into_iter().zip(bucket_ranges) {
                // the bucket ranges maybe need to split or merge not all the meta keys, so it
                // needs to find the first keys.
                while meta_idx < meta.keys.len() && meta.keys[meta_idx] != bucket_range.0 {
                    meta_idx += 1;
                }
                // meta_idx can't be not the last entry (which is end key)
                if meta_idx >= meta.keys.len() - 1 {
                    warn!(
                        self.logger,
                        "can't find the bucket key";
                        "bucket_range_key" => log_wrappers::Value::key(&bucket_range.0));
                    break;
                }
                // the bucket size is small and does not have split keys,
                // then it should be merged with its left neighbor
                let region_bucket_merge_size = store_ctx
                    .coprocessor_host
                    .cfg
                    .region_bucket_merge_size_ratio
                    * (store_ctx.coprocessor_host.cfg.region_bucket_size.0 as f64);
                if bucket.keys.is_empty() && bucket.size <= (region_bucket_merge_size as u64) {
                    meta.sizes[meta_idx] = bucket.size;
                    // the region has more than one bucket
                    // and the left neighbor + current bucket size is not very big
                    if meta.keys.len() > 2
                        && meta_idx != 0
                        && meta.sizes[meta_idx - 1] + bucket.size
                            < store_ctx.coprocessor_host.cfg.region_bucket_size.0 * 2
                    {
                        // bucket is too small
                        region_buckets.left_merge(meta_idx);
                        meta.left_merge(meta_idx);
                        continue;
                    }
                } else {
                    // update size
                    meta.sizes[meta_idx] = bucket.size / (bucket.keys.len() + 1) as u64;
                    // insert new bucket keys (split the original bucket)
                    for bucket_key in bucket.keys {
                        meta_idx += 1;
                        region_buckets.split(meta_idx);
                        meta.split(meta_idx, bucket_key);
                    }
                }
                meta_idx += 1;
            }
            region_buckets.meta = Arc::new(meta);
        } else {
            // when the region buckets is none, the exclusive buckets includes all the
            // bucket keys.
            assert_eq!(buckets.len(), 1);
            let bucket_keys = buckets.pop().unwrap().keys;
            let bucket_count = bucket_keys.len() + 1;
            let mut meta = BucketMeta {
                region_id: self.region_id(),
                region_epoch,
                version: gen_bucket_version(self.term(), current_version),
                keys: bucket_keys,
                sizes: vec![store_ctx.coprocessor_host.cfg.region_bucket_size.0; bucket_count],
            };
            // padding the boundary keys and initialize the flow.
            meta.keys.insert(0, region.get_start_key().to_vec());
            meta.keys.push(region.get_end_key().to_vec());
            region_buckets = BucketStat::from_meta(Arc::new(meta));
        }

        let buckets_count = region_buckets.meta.keys.len() - 1;
        store_ctx.coprocessor_host.on_region_changed(
            region,
            RegionChangeEvent::UpdateBuckets(buckets_count),
            self.state_role(),
        );
        let meta = region_buckets.meta.clone();
        self.region_buckets_info_mut()
            .set_bucket_stat(Some(region_buckets.clone()));

        let mut store_meta = store_ctx.store_meta.lock().unwrap();
        if let Some(reader) = store_meta.readers.get_mut(&self.region_id()) {
            reader.0.update(ReadProgress::region_buckets(meta));
        }
        self.apply_scheduler()
            .unwrap()
            .send(ApplyTask::RefreshBucketStat(region_buckets.meta.clone()));
    }

    #[inline]
    pub fn report_region_buckets_pd<T>(&mut self, ctx: &StoreContext<EK, ER, T>) {
        let delta = self.region_buckets_info_mut().report_bucket_stat();
        let task = pd::Task::ReportBuckets(delta);
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(
                self.logger,
                "failed to report buckets to pd";
                "err" => ?e,
            );
        }
    }

    pub fn maybe_gen_approximate_buckets<T>(&self, ctx: &StoreContext<EK, ER, T>) {
        if ctx.coprocessor_host.cfg.enable_region_bucket() && self.storage().is_initialized() {
            if let Err(e) = ctx
                .schedulers
                .split_check
                .schedule(SplitCheckTask::ApproximateBuckets(self.region().clone()))
            {
                error!(
                    self.logger,
                    "failed to schedule check approximate buckets";
                    "err" => %e,
                );
            }
        }
    }

    // generate bucket range list to run split-check (to further split buckets)
    // It will return the suspected bucket ranges whose write bytes exceed the
    // threshold.
    pub fn gen_bucket_range_for_update<T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
    ) -> Option<Vec<BucketRange>> {
        if !ctx.coprocessor_host.cfg.enable_region_bucket() {
            return None;
        }
        let bucket_update_diff_size_threshold = ctx.coprocessor_host.cfg.region_bucket_size.0 / 2;
        self.region_buckets_info()
            .gen_bucket_range_for_update(bucket_update_diff_size_threshold)
    }
}

impl<'a, EK, ER, T: Transport> PeerFsmDelegate<'a, EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    pub fn on_report_region_buckets_tick(&mut self) {
        if !self.fsm.peer().is_leader()
            || self
                .fsm
                .peer()
                .region_buckets_info()
                .bucket_stat()
                .is_none()
        {
            return;
        }
        self.fsm.peer_mut().report_region_buckets_pd(self.store_ctx);
        self.schedule_tick(PeerTick::ReportBuckets);
    }

    pub fn on_refresh_region_buckets(
        &mut self,
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        if util::is_epoch_stale(&region_epoch, self.fsm.peer().region().get_region_epoch()) {
            error!(
                self.fsm.peer().logger,
                "receive a stale refresh region bucket message";
                "epoch" => ?region_epoch,
                "current_epoch" => ?self.fsm.peer().region().get_region_epoch(),
            );
            return;
        }
        self.fsm.peer_mut().on_refresh_region_buckets(
            self.store_ctx,
            region_epoch,
            buckets,
            bucket_ranges,
        );
        self.schedule_tick(PeerTick::ReportBuckets);
    }
}
