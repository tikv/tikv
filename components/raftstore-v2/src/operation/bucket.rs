// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the interactions with bucket.

use std::sync::Arc;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb::{self, RegionEpoch},
    raft_serverpb::{ExtraMessageType, RaftMessage, RefreshBuckets},
};
use pd_client::{BucketMeta, BucketStat};
use raftstore::{
    coprocessor::{Config, RegionChangeEvent},
    store::{util, Bucket, BucketRange, ReadProgress, SplitCheckTask, Transport},
};
use slog::{error, info};

use crate::{
    batch::StoreContext,
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{ApplyTask, PeerTick},
    worker::pd,
};

#[derive(Debug, Clone, Default)]
pub struct BucketStatsInfo {
    // the stats is increment flow.
    bucket_stat: Option<BucketStat>,
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
        let sizes = &region_buckets.meta.sizes;

        let mut bucket_ranges = vec![];
        assert_eq!(keys.len(), stats.write_bytes.len() + 1);
        for i in 0..stats.write_bytes.len() {
            let diff_in_bytes = stats.write_bytes[i] + sizes[i];
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
        self.bucket_stat = buckets.clone();
        if let Some(new_buckets) = buckets {
            let mut new_report_buckets = BucketStat::from_meta(new_buckets.meta);
            if let Some(old) = &mut self.report_bucket_stat {
                new_report_buckets.merge(&old);
                *old = new_report_buckets;
            } else {
                self.report_bucket_stat = Some(new_report_buckets);
            }
        } else {
            self.report_bucket_stat = None;
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

    pub fn on_refresh_region_buckets(
        &mut self,
        cfg: &Config,
        next_bucket_version: u64,
        buckets: Vec<Bucket>,
        region_epoch: RegionEpoch,
        region: metapb::Region,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) -> bool {
        let change_bucket_version: bool;
        // The region buckets reset after this region happened split or merge.
        // The message should be dropped if it's epoch is lower than the regions.
        // The bucket ranges is none when the region buckets is also none.
        // So this condition indicates that the region buckets needs to refresh not
        // renew.
        if let Some(bucket_ranges) = bucket_ranges&&self.bucket_stat.is_some(){
            assert_eq!(buckets.len(), bucket_ranges.len());
            change_bucket_version=self.update_buckets(cfg, next_bucket_version, buckets, region_epoch,  &bucket_ranges);
        }else{
            change_bucket_version = true;
            // when the region buckets is none, the exclusive buckets includes all the
            // bucket keys.
           self.init_buckets(&cfg, next_bucket_version, buckets, region_epoch, region);
        }
        change_bucket_version
    }

    fn update_buckets(
        &mut self,
        cfg: &Config,
        next_bucket_version: u64,
        buckets: Vec<Bucket>,
        region_epoch: RegionEpoch,
        bucket_ranges: &Vec<BucketRange>,
    ) -> bool {
        let origin_region_buckets = self.bucket_stat.as_ref().unwrap();
        let mut change_bucket_version = false;
        let mut meta_idx = 0;
        let mut region_buckets = origin_region_buckets.clone();
        let mut meta = (*region_buckets.meta).clone();
        meta.region_epoch = region_epoch;

        // bucket stats will clean if the bucket size is updated.
        for (bucket, bucket_range) in buckets.into_iter().zip(bucket_ranges) {
            // the bucket ranges maybe need to split or merge not all the meta keys, so it
            // needs to find the first keys.
            while meta_idx < meta.keys.len() && meta.keys[meta_idx] != bucket_range.0 {
                meta_idx += 1;
            }
            // meta_idx can't be not the last entry (which is end key)
            if meta_idx >= meta.keys.len() - 1 {
                break;
            }
            // the bucket size is small and does not have split keys,
            // then it should be merged with its left neighbor
            let region_bucket_merge_size =
                cfg.region_bucket_merge_size_ratio * (cfg.region_bucket_size.0 as f64);
            if bucket.keys.is_empty() && bucket.size <= (region_bucket_merge_size as u64) {
                meta.sizes[meta_idx] = bucket.size;
                region_buckets.clean_stats(meta_idx);
                // the region has more than one bucket
                // and the left neighbor + current bucket size is not very big
                if meta.keys.len() > 2
                    && meta_idx != 0
                    && meta.sizes[meta_idx - 1] + bucket.size < cfg.region_bucket_size.0 * 2
                {
                    // bucket is too small
                    region_buckets.left_merge(meta_idx);
                    meta.left_merge(meta_idx);
                    change_bucket_version = true;
                    continue;
                }
            } else {
                // update size
                meta.sizes[meta_idx] = bucket.size / (bucket.keys.len() + 1) as u64;
                region_buckets.clean_stats(meta_idx);
                // insert new bucket keys (split the original bucket)
                for bucket_key in bucket.keys {
                    meta_idx += 1;
                    region_buckets.split(meta_idx);
                    meta.split(meta_idx, bucket_key);
                    change_bucket_version = true;
                }
            }
            meta_idx += 1;
        }
        if change_bucket_version {
            meta.version = next_bucket_version;
        }
        region_buckets.meta = Arc::new(meta);
        self.set_bucket_stat(Some(region_buckets));
        change_bucket_version
    }

    fn init_buckets(
        &mut self,
        cfg: &Config,
        next_bucket_version: u64,
        mut buckets: Vec<Bucket>,
        region_epoch: RegionEpoch,
        region: metapb::Region,
    ) {
        // when the region buckets is none, the exclusive buckets includes all the
        // bucket keys.
        assert_eq!(buckets.len(), 1);
        let bucket_keys = buckets.pop().unwrap().keys;
        let bucket_count = bucket_keys.len() + 1;
        let mut meta = BucketMeta {
            region_id: region.get_id(),
            region_epoch,
            version: next_bucket_version,
            keys: bucket_keys,
            sizes: vec![cfg.region_bucket_size.0; bucket_count],
        };
        // padding the boundary keys and initialize the flow.
        meta.keys.insert(0, region.get_start_key().to_vec());
        meta.keys.push(region.get_end_key().to_vec());
        let bucket_stats = BucketStat::from_meta(Arc::new(meta));
        self.set_bucket_stat(Some(bucket_stats));
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn on_refresh_region_buckets<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        region_epoch: RegionEpoch,
        buckets: Vec<Bucket>,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        if self.term() > u32::MAX.into() {
            error!(
                self.logger,
                "unexpected term {} more than u32::MAX. Bucket version will be backward.",
                self.term()
            );
        }

        let current_version = self.region_buckets_info().version();
        let next_bucket_version = util::gen_bucket_version(self.term(), current_version);
        // let mut is_first_refresh = true;
        let region = self.region().clone();
        let change_bucket_version = self.region_buckets_info_mut().on_refresh_region_buckets(
            &store_ctx.coprocessor_host.cfg,
            next_bucket_version,
            buckets,
            region_epoch,
            region,
            bucket_ranges,
        );
        let region_buckets = self
            .region_buckets_info()
            .bucket_stat()
            .as_ref()
            .unwrap()
            .clone();
        let buckets_count = region_buckets.meta.keys.len() - 1;
        if change_bucket_version {
            // TODO: we may need to make it debug once the coprocessor timeout is resolved.
            info!(
                self.logger,
                "refreshed region bucket info";
                "bucket_version" => next_bucket_version,
                "buckets_count" => buckets_count,
                "estimated_region_size" => region_buckets.meta.total_size(),
            );
        } else {
            // it means the buckets key range not any change, so don't need to refresh.
            return;
        }

        store_ctx.coprocessor_host.on_region_changed(
            self.region(),
            RegionChangeEvent::UpdateBuckets(buckets_count),
            self.state_role(),
        );
        let meta = region_buckets.meta.clone();
        {
            let mut store_meta = store_ctx.store_meta.lock().unwrap();
            if let Some(reader) = store_meta.readers.get_mut(&self.region_id()) {
                reader.0.update(ReadProgress::region_buckets(meta));
            }
        }
        // it's possible that apply_scheduler is not initialized yet
        if let Some(apply_scheduler) = self.apply_scheduler() {
            apply_scheduler.send(ApplyTask::RefreshBucketStat(region_buckets.meta.clone()));
        }
        let version = region_buckets.meta.version;
        let keys = region_buckets.meta.keys.clone();
        if !self.is_leader() {
            return;
        }
        // Notify followers to flush their relevant memtables
        let peers = self.region().get_peers().to_vec();
        for p in peers {
            if p == *self.peer() || p.is_witness {
                continue;
            }
            let mut msg = RaftMessage::default();
            msg.set_region_id(self.region_id());
            msg.set_from_peer(self.peer().clone());
            msg.set_to_peer(p.clone());
            msg.set_region_epoch(self.region().get_region_epoch().clone());
            let extra_msg = msg.mut_extra_msg();
            extra_msg.set_type(ExtraMessageType::MsgRefreshBuckets);
            let mut refresh_buckets = RefreshBuckets::new();
            refresh_buckets.set_version(version);
            refresh_buckets.set_keys(keys.clone().into());
            extra_msg.set_refresh_buckets(refresh_buckets);
            self.send_raft_message(store_ctx, msg);
        }
    }

    pub fn on_msg_refresh_buckets<T: Transport>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        msg: &RaftMessage,
    ) {
        // leader should not receive this message
        if self.is_leader() {
            return;
        }
        let extra_msg = msg.get_extra_msg();
        let version = extra_msg.get_refresh_buckets().get_version();
        let keys = extra_msg.get_refresh_buckets().get_keys().to_vec();
        let region_epoch = msg.get_region_epoch();

        let meta = BucketMeta {
            region_id: self.region_id(),
            version,
            region_epoch: region_epoch.clone(),
            keys,
            sizes: vec![],
        };

        let mut store_meta = store_ctx.store_meta.lock().unwrap();
        if let Some(reader) = store_meta.readers.get_mut(&self.region_id()) {
            reader
                .0
                .update(ReadProgress::region_buckets(Arc::new(meta)));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    // create BucketStatsInfo include three keys: ["","100","200",""].
    fn mock_bucket_stats_info() -> BucketStatsInfo {
        let mut bucket_stats_info = BucketStatsInfo::default();
        let cfg = Config::default();
        let next_bucket_version = 1;
        let bucket_ranges = None;
        let mut region_epoch = RegionEpoch::default();
        region_epoch.set_conf_ver(1);
        region_epoch.set_version(1);
        let mut region = metapb::Region::default();
        region.set_id(1);

        let mut buckets = vec![];
        let mut bucket = Bucket::default();
        bucket.keys.push(vec![100]);
        bucket.keys.push(vec![200]);
        buckets.insert(0, bucket);

        let _ = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets.clone(),
            region_epoch.clone(),
            region.clone(),
            bucket_ranges.clone(),
        );
        bucket_stats_info
    }

    #[test]
    pub fn test_insert_new_buckets() {
        let bucket_stats_info = mock_bucket_stats_info();

        let cfg = Config::default();
        let bucket_stat = bucket_stats_info.bucket_stat.unwrap();
        assert_eq!(
            vec![vec![], vec![100], vec![200], vec![]],
            bucket_stat.meta.keys
        );
        for i in 0..bucket_stat.stats.write_bytes.len() {
            assert_eq!(cfg.region_bucket_size.0, bucket_stat.meta.sizes[i]);
            assert_eq!(0, bucket_stat.stats.write_bytes[i]);
        }
    }

    #[test]
    pub fn test_report_buckets(){
        let mut bucket_stats_info = mock_bucket_stats_info();
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        let mut delta_bucket_stats = bucket_stats.clone();
        delta_bucket_stats.write_key(&[1], 1);
        delta_bucket_stats.write_key(&[201], 1);
        bucket_stats_info.add_bucket_flow(&Some(delta_bucket_stats.clone()));
        let bucket_stats = bucket_stats_info.report_bucket_stat();
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);

        let report_bucket_stats = bucket_stats_info.report_bucket_stat();
        assert_eq!(vec![0, 0, 0], report_bucket_stats.stats.write_bytes);
        bucket_stats_info.add_bucket_flow(&Some(delta_bucket_stats));
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);
    }

    #[test]
    pub fn test_spilt_and_merge_buckets() {
        let mut bucket_stats_info = mock_bucket_stats_info();
        let next_bucket_version = 2;
        let mut region = metapb::Region::default();
        region.set_id(1);
        let cfg = Config::default();
        let bucket_size = cfg.region_bucket_size.0;
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        let region_epoch = bucket_stats.meta.region_epoch.clone();

        // step1: update buckets flow
        let mut delta_bucket_stats = bucket_stats.clone();
        delta_bucket_stats.write_key(&[1], 1);
        delta_bucket_stats.write_key(&[201], 1);
        bucket_stats_info.add_bucket_flow(&Some(delta_bucket_stats));
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);

        // step2: tick not affect anything
        let bucket_ranges = Some(vec![]);
        let buckets = vec![];
        let mut change_bucket_version = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets.clone(),
            region_epoch.clone(),
            region.clone(),
            bucket_ranges.clone(),
        );
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert!(!change_bucket_version);
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);

        // step3: split key 50
        let mut bucket_ranges = Some(vec![BucketRange(vec![], vec![100])]);
        let mut bucket = Bucket::default();
        bucket.keys = vec![vec![50]];
        bucket.size = bucket_size;
        let mut buckets = vec![bucket];
        change_bucket_version = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets.clone(),
            region_epoch.clone(),
            region.clone(),
            bucket_ranges.clone(),
        );
        assert!(change_bucket_version);
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert_eq!(
            vec![vec![], vec![50], vec![100], vec![200], vec![]],
            bucket_stats.meta.keys
        );
        assert_eq!(
            vec![bucket_size / 2, bucket_size / 2, bucket_size, bucket_size],
            bucket_stats.meta.sizes
        );
        assert_eq!(vec![0, 0, 0, 2], bucket_stats.stats.write_bytes);

        // step4: merge [50-100] to [0-50],
        bucket_ranges = Some(vec![BucketRange(vec![50], vec![100])]);
        let mut bucket = Bucket::default();
        bucket.keys = vec![];
        bucket.size = 0;
        buckets = vec![bucket];
        change_bucket_version = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets,
            region_epoch,
            region,
            bucket_ranges,
        );
        assert!(change_bucket_version);

        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert_eq!(
            vec![vec![], vec![100], vec![200], vec![]],
            bucket_stats.meta.keys
        );
        assert_eq!(
            vec![bucket_size / 2, bucket_size, bucket_size],
            bucket_stats.meta.sizes
        );
        assert_eq!(vec![0, 0, 2], bucket_stats.stats.write_bytes);

        // report buckets doesn't be affected by the split and merge.
        let report_bucket_stats = bucket_stats_info.report_bucket_stat();
        assert_eq!(vec![4, 0, 2], report_bucket_stats.stats.write_bytes);
    }
}
