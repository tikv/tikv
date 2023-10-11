// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the interactions with pd.

use std::sync::atomic::Ordering;

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::{metapb, pdpb};
use raftstore::store::{metrics::STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC, Transport};
use slog::{debug, error};
use tikv_util::{slog_panic, time::Instant};

use crate::{
    batch::StoreContext,
    fsm::{PeerFsmDelegate, Store, StoreFsmDelegate},
    raft::Peer,
    router::{CmdResChannel, PeerTick, StoreTick},
    worker::pd,
};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    #[inline]
    pub fn on_pd_store_heartbeat(&mut self) {
        self.fsm.store.store_heartbeat_pd(self.store_ctx, None);
        self.schedule_tick(
            StoreTick::PdStoreHeartbeat,
            self.store_ctx.cfg.pd_store_heartbeat_tick_interval.0,
        );
    }
}

impl Store {
    pub fn store_heartbeat_pd<EK, ER, T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        report: Option<pdpb::StoreReport>,
    ) where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let mut stats = pdpb::StoreStats::default();

        stats.set_store_id(self.store_id());
        {
            let meta = ctx.store_meta.lock().unwrap();
            stats.set_region_count(meta.readers.len() as u32);
        }

        let snap_stats = ctx.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        stats.set_snapshot_stats(snap_stats.stats.into());

        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(stats.get_sending_snap_count() as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(stats.get_receiving_snap_count() as i64);

        stats.set_start_time(self.start_time().unwrap() as u32);

        stats.set_bytes_written(
            ctx.global_stat
                .stat
                .engine_total_bytes_written
                .swap(0, Ordering::Relaxed),
        );
        stats.set_keys_written(
            ctx.global_stat
                .stat
                .engine_total_keys_written
                .swap(0, Ordering::Relaxed),
        );
        stats.set_is_busy(false);
        // TODO: add query stats
        let task = pd::Task::StoreHeartbeat { stats, report };
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(self.logger(), "notify pd failed";
                "store_id" => self.store_id(),
                "err" => ?e
            );
        }
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    #[inline]
    pub fn on_pd_heartbeat(&mut self) {
        self.fsm.peer_mut().update_peer_statistics();
        if self.fsm.peer().is_leader() {
            self.fsm.peer_mut().region_heartbeat_pd(self.store_ctx);
        }
        // TODO: hibernate region
        self.schedule_tick(PeerTick::PdHeartbeat);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    #[inline]
    pub fn region_heartbeat_pd<T>(&mut self, ctx: &StoreContext<EK, ER, T>) {
        let task = pd::Task::RegionHeartbeat(pd::RegionHeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            down_peers: self.collect_down_peers(ctx),
            peer: self.peer().clone(),
            pending_peers: self.collect_pending_peers(ctx),
            written_bytes: self.self_stat().written_bytes,
            written_keys: self.self_stat().written_keys,
            approximate_size: self.split_flow_control_mut().approximate_size(),
            approximate_keys: self.split_flow_control_mut().approximate_keys(),
            wait_data_peers: Vec::new(),
        });
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd";
                "err" => ?e,
            );
            return;
        }
        fail_point!("schedule_check_split");
    }

    /// Collects all pending peers and update `peers_start_pending_time`.
    fn collect_pending_peers<T>(&mut self, ctx: &StoreContext<EK, ER, T>) -> Vec<metapb::Peer> {
        let mut pending_peers = Vec::with_capacity(self.region().get_peers().len());
        let status = self.raft_group().status();
        let truncated_idx = self
            .storage()
            .apply_state()
            .get_truncated_state()
            .get_index();

        if status.progress.is_none() {
            return pending_peers;
        }

        self.abnormal_peer_context().flush_metrics();

        let progresses = status.progress.unwrap().iter();
        let mut peers_start_pending_time = Vec::with_capacity(self.region().get_peers().len());
        for (&id, progress) in progresses {
            if id == self.peer_id() {
                continue;
            }
            // The `matched` is 0 only in these two cases:
            // 1. Current leader hasn't communicated with this peer.
            // 2. This peer does not exist yet(maybe it is created but not initialized)
            //
            // The correctness of region merge depends on the fact that all target peers
            // must exist during merging. (PD rely on `pending_peers` to check whether all
            // target peers exist)
            //
            // So if the `matched` is 0, it must be a pending peer.
            // It can be ensured because `truncated_index` must be greater than
            // `RAFT_INIT_LOG_INDEX`(5).
            if progress.matched < truncated_idx {
                if let Some(p) = self.peer_from_cache(id) {
                    pending_peers.push(p);
                    if !self
                        .abnormal_peer_context()
                        .pending_peers()
                        .iter()
                        .any(|p| p.0 == id)
                    {
                        let now = Instant::now();
                        peers_start_pending_time.push((id, now));
                        debug!(
                            self.logger,
                            "peer start pending";
                            "get_peer_id" => id,
                            "time" => ?now,
                        );
                    }
                } else {
                    if ctx.cfg.dev_assert {
                        slog_panic!(
                            self.logger,
                            "failed to get peer from cache";
                            "get_peer_id" => id
                        );
                    }
                    error!(
                        self.logger,
                        "failed to get peer from cache";
                        "get_peer_id" => id,
                    );
                }
            }
        }
        self.abnormal_peer_context_mut()
            .pending_peers_mut()
            .append(&mut peers_start_pending_time);
        pending_peers
    }

    #[inline]
    pub fn destroy_peer_pd<T>(&self, ctx: &StoreContext<EK, ER, T>) {
        let task = pd::Task::DestroyPeer {
            region_id: self.region_id(),
        };
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd with DestroyPeer";
                "err" => %e,
            );
        }
    }

    #[inline]
    pub fn ask_batch_split_pd<T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        split_keys: Vec<Vec<u8>>,
        share_source_region_size: bool,
        ch: CmdResChannel,
    ) {
        let task = pd::Task::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer().clone(),
            right_derive: ctx.cfg.right_derive_when_split,
            share_source_region_size,
            ch,
        };
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd with AskBatchSplit";
                "err" => %e,
            );
        }
    }

    #[inline]
    pub fn report_batch_split_pd<T>(
        &self,
        ctx: &StoreContext<EK, ER, T>,
        regions: Vec<metapb::Region>,
    ) {
        let task = pd::Task::ReportBatchSplit { regions };
        if let Err(e) = ctx.schedulers.pd.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd with ReportBatchSplit";
                "err" => %e,
            );
        }
    }
}
