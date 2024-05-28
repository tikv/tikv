// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the interactions with bucket.

use std::sync::Arc;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    metapb::RegionEpoch,
    raft_serverpb::{ExtraMessageType, RaftMessage, RefreshBuckets},
};
use pd_client::BucketMeta;
use raftstore::{
    coprocessor::RegionChangeEvent,
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
        let region = self.region().clone();
        let change_bucket_version = self.region_buckets_info_mut().on_refresh_region_buckets(
            &store_ctx.coprocessor_host.cfg,
            next_bucket_version,
            buckets,
            region_epoch,
            &region,
            bucket_ranges,
        );
        let region_buckets = self.region_buckets_info().bucket_stat().unwrap().clone();
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
        if !self.is_leader() {
            return;
        }
        let version = region_buckets.meta.version;
        let keys = region_buckets.meta.keys.clone();
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
            let mut refresh_buckets = RefreshBuckets::default();
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
        let region_bucket_max_size = ctx.coprocessor_host.cfg.region_bucket_size.0 * 2;
        self.region_buckets_info()
            .gen_bucket_range_for_update(region_bucket_max_size)
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
