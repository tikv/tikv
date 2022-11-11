// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements the interactions with pd.

use std::cmp;

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::pdpb;
use raftstore::store::Transport;
use slog::error;

use crate::{
    batch::StoreContext,
    fsm::{PeerFsmDelegate, Store, StoreFsmDelegate},
    raft::Peer,
    router::{PeerTick, StoreTick},
    worker::{PdHeartbeatTask, PdTask},
};

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    pub fn on_pd_store_heartbeat(&mut self) {
        self.fsm.store.store_heartbeat_pd(self.store_ctx);
        self.schedule_tick(
            StoreTick::PdStoreHeartbeat,
            self.store_ctx.cfg.pd_store_heartbeat_tick_interval.0,
        );
    }
}

impl Store {
    pub fn store_heartbeat_pd<EK, ER, T>(&mut self, ctx: &mut StoreContext<EK, ER, T>)
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        let mut stats = pdpb::StoreStats::default();

        stats.set_store_id(self.store_id());
        {
            let meta = ctx.store_meta.lock().unwrap();
            stats.set_region_count(meta.tablet_caches.len() as u32);
        }

        stats.set_sending_snap_count(0);
        stats.set_receiving_snap_count(0);

        stats.set_start_time(self.start_time().unwrap() as u32);

        stats.set_bytes_written(0);
        stats.set_keys_written(0);
        stats.set_is_busy(false);

        // stats.set_query_stats(query_stats);

        let task = PdTask::StoreHeartbeat { stats };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(self.logger(), "notify pd failed";
                "store_id" => self.store_id(),
                "err" => ?e
            );
        }
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_pd_heartbeat(&mut self) {
        self.fsm.peer_mut().heartbeat_pd(self.store_ctx);
        self.schedule_tick(PeerTick::PdHeartbeat);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn heartbeat_pd<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        let task = PdTask::Heartbeat(PdHeartbeatTask {
            term: self.term(),
            region: self.region().clone(),
            down_peers: Vec::new(),
            peer: self.peer().clone(),
            pending_peers: Vec::new(),
            written_bytes: 0,
            written_keys: 0,
            approximate_size: None,
            approximate_keys: None,
            wait_data_peers: Vec::new(),
        });
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => ?e,
            );
            return;
        }
        fail_point!("schedule_check_split");
    }

    pub fn destroy_peer_pd<T>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        let task = PdTask::DestroyPeer {
            region_id: self.region_id(),
        };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => %e,
            );
        }
    }

    pub fn ask_batch_split_pd<T>(
        &mut self,
        ctx: &mut StoreContext<EK, ER, T>,
        split_keys: Vec<Vec<u8>>,
    ) {
        let task = PdTask::AskBatchSplit {
            region: self.region().clone(),
            split_keys,
            peer: self.peer().clone(),
            right_derive: ctx.cfg.right_derive_when_split,
        };
        if let Err(e) = ctx.pd_scheduler.schedule(task) {
            error!(
                self.logger,
                "failed to notify pd to split";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "err" => %e,
            );
        }
    }
}
