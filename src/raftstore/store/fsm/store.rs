// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::u64;
use time;

use futures::Future;
use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::StoreStats;
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};
use rocksdb::{CompactionJobInfo, DB};

use pd::{PdClient, PdTask};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::util::is_initial_msg;
use raftstore::Result;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::mpsc::Receiver;
use util::rocksdb;
use util::rocksdb::{CompactedEvent, CompactionListener};
use util::time::duration_to_sec;
use util::time::SlowTimer;
use util::worker::Scheduler;

use raftstore::store::config::Config;
use raftstore::store::engine::Peekable;
use raftstore::store::fsm::transport::PollContext;
use raftstore::store::fsm::ApplyRouter;
use raftstore::store::fsm::{transport, ConfigProvider, Router, StoreMeta};
use raftstore::store::keys::{self, data_end_key, data_key, enc_start_key};
use raftstore::store::metrics::*;
use raftstore::store::peer::Peer;
use raftstore::store::transport::Transport;
use raftstore::store::worker::{CleanupSSTTask, CompactTask, ReadTask, RegionTask};
use raftstore::store::{
    util, Engines, PeerMsg, SeekRegionCallback, SeekRegionFilter, StoreMsg, StoreTick,
};
use time::Timespec;

type Key = Vec<u8>;

pub struct StoreCore {
    cfg: Option<Arc<Config>>,
    store: Option<metapb::Store>,
    last_compact_checked_key: Key,
    pub stopped: bool,

    tag: String,

    start_time: Timespec,
}

pub struct Store<'a, T: 'static, C: 'static> {
    core: &'a mut StoreCore,
    ctx: &'a mut PollContext<T, C>,
    scheduler: &'a transport::Scheduler,
}

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}

impl<'a, T: Transport, C> ConfigProvider for Store<'a, T, C> {
    #[inline]
    fn store_id(&self) -> u64 {
        self.core.store.as_ref().unwrap().get_id()
    }

    #[inline]
    fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.ctx.region_scheduler.clone()
    }

    #[inline]
    fn apply_scheduler(&self) -> ApplyRouter {
        self.ctx.apply_router.clone()
    }

    #[inline]
    fn read_scheduler(&self) -> Scheduler<ReadTask> {
        self.ctx.local_reader.clone()
    }

    #[inline]
    fn engines(&self) -> &Engines {
        &self.ctx.engines
    }

    #[inline]
    fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        self.ctx.coprocessor_host.clone()
    }

    #[inline]
    fn config(&self) -> Arc<Config> {
        self.core.cfg.as_ref().unwrap().clone()
    }
}

impl StoreCore {
    pub fn new() -> StoreCore {
        StoreCore {
            cfg: None,
            store: None,
            stopped: false,
            last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
            tag: "".to_owned(),
            start_time: time::get_time(),
        }
    }
}

impl<'a, T, C> Store<'a, T, C> {
    pub fn new(
        core: &'a mut StoreCore,
        ctx: &'a mut PollContext<T, C>,
        scheduler: &'a transport::Scheduler,
    ) -> Store<'a, T, C> {
        Store {
            core,
            ctx,
            scheduler,
        }
    }

    pub fn kv_engine(&self) -> Arc<DB> {
        Arc::clone(&self.ctx.engines.kv)
    }

    pub fn raft_engine(&self) -> Arc<DB> {
        Arc::clone(&self.ctx.engines.raft)
    }
}

impl<'a, T: Transport, C: PdClient> Store<'a, T, C> {
    pub fn start(&mut self, meta: metapb::Store, cfg: Arc<Config>) {
        self.core.cfg = Some(cfg);
        self.core.tag = format!("[store {}]", meta.get_id());
        self.core.store = Some(meta);
        self.schedule_compact_check_tick();
        self.schedule_pd_store_heartbeat_tick();
        self.schedule_snap_mgr_gc_tick();
        self.schedule_compact_lock_cf_tick();
        self.schedule_consistency_check_tick();
        self.schedule_cleanup_import_sst_tick();
    }

    fn stop(&mut self) {
        self.core.stopped = true;
    }

    #[inline]
    fn schedule_tick(&self, dur: Duration, tick: StoreTick) {
        if dur != Duration::new(0, 0) {
            let tx = self.scheduler.router().store_notifier();
            let f = self.ctx.timer.delay(Instant::now() + dur).map(move |_| {
                let _ = tx.force_send(StoreMsg::Tick(tick));
            });
            self.ctx.poller.spawn(f).forget()
        }
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    pub fn maybe_create_peer(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let target = msg.get_to_peer();
        // we may encounter a message with larger peer id, which means
        // current peer is stale, then we should remove current peer
        let mut meta_guard = self.ctx.store_meta.lock().unwrap();
        let meta: &mut StoreMeta = &mut *meta_guard;
        if meta.regions.contains_key(&region_id) {
            return Ok(true);
        }

        if !is_initial_msg(msg.get_message()) {
            let msg_type = msg.get_message().get_msg_type();
            debug!(
                "target peer {:?} doesn't exist, stale message {:?}.",
                target, msg_type
            );
            self.ctx.raft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        let start_key = data_key(msg.get_start_key());
        if let Some((_, &exist_region_id)) = meta
            .region_ranges
            .range((Excluded(start_key), Unbounded::<Key>))
            .next()
        {
            let exist_region = &meta.regions[&exist_region_id];
            if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                debug!("msg {:?} is overlapped with region {:?}", msg, exist_region);
                if util::is_first_vote_msg(msg.get_message()) {
                    meta.pending_votes.push(msg.to_owned());
                }
                self.ctx.raft_metrics.message_dropped.region_overlap += 1;
                meta.pending_cross_snap
                    .insert(region_id, msg.get_region_epoch().to_owned());
                return Ok(false);
            }
        }

        // New created peers should know it's learner or not.
        let peer = Peer::replicate(self, region_id, target.clone())?;
        // following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        meta.regions.insert(region_id, peer.region().to_owned());
        self.scheduler.schedule(peer, None);
        Ok(true)
    }

    fn on_compaction_finished(&mut self, event: CompactedEvent) {
        // If size declining is trivial, skip.
        let total_bytes_declined = if event.total_input_bytes > event.total_output_bytes {
            event.total_input_bytes - event.total_output_bytes
        } else {
            0
        };
        if self
            .core
            .cfg
            .as_ref()
            .map_or(true, |c| c.region_split_check_diff.0 > total_bytes_declined)
            || total_bytes_declined * 10 < event.total_input_bytes
        {
            return;
        }

        let output_level_str = event.output_level.to_string();
        COMPACTION_DECLINED_BYTES
            .with_label_values(&[&output_level_str])
            .observe(total_bytes_declined as f64);

        // self.cfg.region_split_check_diff.0 / 16 is an experienced value.
        let mut region_declined_bytes = {
            let meta = self.ctx.store_meta.lock().unwrap();
            calc_region_declined_bytes(
                event,
                &meta.region_ranges,
                self.core.cfg.as_ref().unwrap().region_split_check_diff.0 / 16,
            )
        };

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(region_declined_bytes.len() as f64);

        for (region_id, declined_bytes) in region_declined_bytes.drain(..) {
            let _ = self
                .scheduler
                .router()
                .send_peer_message(region_id, PeerMsg::CompactionDeclinedBytes(declined_bytes));
        }
    }

    #[inline]
    fn schedule_compact_check_tick(&self) {
        self.schedule_tick(
            self.core
                .cfg
                .as_ref()
                .unwrap()
                .region_compact_check_interval
                .0,
            StoreTick::CompactCheck,
        )
    }

    fn on_compact_check_tick(&mut self) {
        if self.ctx.compact_scheduler.is_busy() {
            info!(
                "{} compact worker is busy, check space redundancy next time",
                self.core.tag
            );
        } else if rocksdb::auto_compactions_is_disabled(&self.ctx.engines.kv) {
            debug!(
                "{} skip compact check when disabled auto compactions.",
                self.core.tag
            );
        } else {
            // Start from last checked key.
            let mut ranges_need_check = Vec::with_capacity(
                self.core.cfg.as_ref().unwrap().region_compact_check_step as usize + 1,
            );
            ranges_need_check.push(self.core.last_compact_checked_key.clone());

            let largest_key = {
                let meta = self.ctx.store_meta.lock().unwrap();
                // Collect continuous ranges.
                let left_ranges = meta.region_ranges.range((
                    Excluded(self.core.last_compact_checked_key.clone()),
                    Unbounded::<Key>,
                ));
                ranges_need_check.extend(
                    left_ranges
                        .take(self.core.cfg.as_ref().unwrap().region_compact_check_step as usize)
                        .map(|(k, _)| k.to_owned()),
                );

                // Update last_compact_checked_key.
                meta.region_ranges.keys().last().unwrap().to_vec()
            };
            let last_key = ranges_need_check.last().unwrap().clone();
            if last_key == largest_key {
                // Range [largest key, DATA_MAX_KEY) also need to check.
                if last_key != keys::DATA_MAX_KEY.to_vec() {
                    ranges_need_check.push(keys::DATA_MAX_KEY.to_vec());
                }
                // Next task will start from the very beginning.
                self.core.last_compact_checked_key = keys::DATA_MIN_KEY.to_vec();
            } else {
                self.core.last_compact_checked_key = last_key;
            }

            // Schedule the task.
            let cf_names = vec![CF_DEFAULT.to_owned(), CF_WRITE.to_owned()];
            if let Err(e) = self
                .ctx
                .compact_scheduler
                .schedule(CompactTask::CheckAndCompact {
                    cf_names,
                    ranges: ranges_need_check,
                    tombstones_num_threshold: self
                        .core
                        .cfg
                        .as_ref()
                        .unwrap()
                        .region_compact_min_tombstones,
                    tombstones_percent_threshold: self
                        .core
                        .cfg
                        .as_ref()
                        .unwrap()
                        .region_compact_tombstones_percent,
                }) {
                error!(
                    "{} failed to schedule space check task: {}",
                    self.core.tag, e
                );
            }
        }

        self.schedule_compact_check_tick();
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::new();

        let used_size = self.ctx.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
        stats.set_store_id(self.store_id());

        let snap_stats = self.ctx.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        {
            let meta = self.ctx.store_meta.lock().unwrap();
            stats.set_region_count(meta.regions.len() as u32);

            // TODO: count applying snapshot.
        }
        STORE_PD_HEARTBEAT_GAUGE_VEC
            .with_label_values(&["region"])
            .set(i64::from(stats.get_region_count()));

        stats.set_start_time(self.core.start_time.sec as u32);

        // report store write flow to pd
        stats.set_bytes_written(
            self.ctx
                .global_stat
                .stat
                .engine_total_bytes_written
                .swap(0, Ordering::Relaxed),
        );
        stats.set_keys_written(
            self.ctx
                .global_stat
                .stat
                .engine_total_keys_written
                .swap(0, Ordering::Relaxed),
        );
        stats.set_is_busy(
            self.ctx
                .global_stat
                .stat
                .is_busy
                .swap(false, Ordering::Relaxed),
        );

        let store_info = StoreInfo {
            engine: Arc::clone(&self.ctx.engines.kv),
            capacity: self.core.cfg.as_ref().unwrap().capacity.0,
        };

        let task = PdTask::StoreHeartbeat { stats, store_info };
        if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd: {}", self.core.tag, e);
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.schedule_pd_store_heartbeat_tick();
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        let snap_keys = self.ctx.snap_mgr.list_idle_snap()?;
        if snap_keys.is_empty() {
            return Ok(());
        }
        let mut last_region_id = 0;
        let mut keys = vec![];
        for (key, is_sending) in snap_keys {
            if key.region_id != last_region_id {
                if !keys.is_empty() {
                    debug!(
                        "{} schedule snap gc for region {}",
                        self.core.tag, last_region_id
                    );
                    let _ = self
                        .scheduler
                        .router()
                        .send_peer_message(last_region_id, PeerMsg::GcSnap(keys));
                    keys = vec![];
                }
                last_region_id = key.region_id;
            }
            keys.push((key, is_sending));
        }
        if !keys.is_empty() {
            debug!(
                "{} schedule snap gc for region {}",
                self.core.tag, last_region_id
            );
            let _ = self
                .scheduler
                .router()
                .send_peer_message(last_region_id, PeerMsg::GcSnap(keys));
        }
        Ok(())
    }

    fn on_snap_mgr_gc(&mut self) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!("{} failed to gc snap manager: {:?}", self.core.tag, e);
        }
        self.schedule_snap_mgr_gc_tick();
    }

    fn on_compact_lock_cf(&mut self) {
        // Create a compact lock cf task(compact whole range) and schedule directly.
        let lock_cf_bytes_written = self
            .ctx
            .global_stat
            .stat
            .lock_cf_bytes_written
            .load(Ordering::Relaxed);
        if lock_cf_bytes_written
            > self
                .core
                .cfg
                .as_ref()
                .unwrap()
                .lock_cf_compact_bytes_threshold
                .0
        {
            self.ctx
                .global_stat
                .stat
                .lock_cf_bytes_written
                .fetch_sub(lock_cf_bytes_written, Ordering::Relaxed);
            let task = CompactTask::Compact {
                cf_name: String::from(CF_LOCK),
                start_key: None,
                end_key: None,
            };
            if let Err(e) = self.ctx.compact_scheduler.schedule(task) {
                error!(
                    "{} failed to schedule compact lock cf task: {:?}",
                    self.core.tag, e
                );
            }
        }

        self.schedule_compact_lock_cf_tick();
    }

    #[inline]
    fn schedule_pd_store_heartbeat_tick(&self) {
        self.schedule_tick(
            self.core
                .cfg
                .as_ref()
                .unwrap()
                .pd_store_heartbeat_tick_interval
                .0,
            StoreTick::PdStoreHeartbeat,
        );
    }

    #[inline]
    fn schedule_snap_mgr_gc_tick(&self) {
        self.schedule_tick(
            self.core.cfg.as_ref().unwrap().snap_mgr_gc_tick_interval.0,
            StoreTick::SnapGc,
        )
    }

    #[inline]
    fn schedule_compact_lock_cf_tick(&self) {
        self.schedule_tick(
            self.core.cfg.as_ref().unwrap().lock_cf_compact_interval.0,
            StoreTick::CompactLockCf,
        )
    }

    #[inline]
    fn schedule_consistency_check_tick(&self) {
        self.schedule_tick(
            self.core.cfg.as_ref().unwrap().consistency_check_interval.0,
            StoreTick::ConsistencyCheck,
        );
    }

    fn on_consistency_check_tick(&mut self) {
        if self.ctx.consistency_check_scheduler.is_busy() {
            // To avoid frequent scan, schedule new check only when all the
            // scheduled check is done.
            self.schedule_consistency_check_tick();
            return;
        }

        // TODO: find a policy to schedule consistency check.

        self.schedule_consistency_check_tick();
    }
}

impl<'a, T: Transport, C: PdClient> Store<'a, T, C> {
    fn on_validate_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        if ssts.is_empty() {
            return;
        }
        // A stale peer can still ingest a stale SST before it is
        // destroyed. We need to make sure that no stale peer exists.
        let mut delete_ssts = Vec::new();
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            for sst in ssts {
                if !meta.regions.contains_key(&sst.get_region_id()) {
                    delete_ssts.push(sst);
                }
            }
        }
        if delete_ssts.is_empty() {
            return;
        }

        let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
        if let Err(e) = self.ctx.cleanup_sst_scheduler.schedule(task) {
            error!("schedule to delete ssts: {:?}", e);
        }
    }

    fn on_cleanup_import_sst(&mut self) -> Result<()> {
        let mut delete_ssts = Vec::new();
        let mut validate_ssts = Vec::new();

        let ssts = box_try!(self.ctx.importer.list_ssts());
        if ssts.is_empty() {
            return Ok(());
        }
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            for sst in ssts {
                if let Some(r) = meta.regions.get(&sst.get_region_id()) {
                    let region_epoch = r.get_region_epoch();
                    if util::is_epoch_stale(sst.get_region_epoch(), region_epoch) {
                        // If the SST epoch is stale, it will not be ingested anymore.
                        delete_ssts.push(sst);
                    }
                } else {
                    // If the peer doesn't exist, we need to validate the SST through PD.
                    validate_ssts.push(sst);
                }
            }
        }

        if !delete_ssts.is_empty() {
            let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
            if let Err(e) = self.ctx.cleanup_sst_scheduler.schedule(task) {
                error!("schedule to delete ssts: {:?}", e);
            }
        }

        if !validate_ssts.is_empty() {
            let task = CleanupSSTTask::ValidateSST {
                ssts: validate_ssts,
            };
            if let Err(e) = self.ctx.cleanup_sst_scheduler.schedule(task) {
                error!("schedule to validate ssts: {:?}", e);
            }
        }

        Ok(())
    }

    fn on_cleanup_import_sst_tick(&mut self) {
        if let Err(e) = self.on_cleanup_import_sst() {
            error!("{} failed to cleanup import sst: {:?}", self.core.tag, e);
        }
        self.schedule_cleanup_import_sst_tick();
    }

    #[inline]
    fn schedule_cleanup_import_sst_tick(&self) {
        self.schedule_tick(
            self.core
                .cfg
                .as_ref()
                .unwrap()
                .cleanup_import_sst_interval
                .0,
            StoreTick::CleanupImportSST,
        )
    }

    /// Find the first region `r` whose range contains or greater than `from_key` and the peer on
    /// this TiKV satisfies `filter(peer)` returns true.
    fn seek_region(&self, _: &[u8], _: SeekRegionFilter, _: u32, _: SeekRegionCallback) {
        unimplemented!()
    }

    fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        cur_epoch: RegionEpoch,
        need_gc: bool,
        target_region: Option<Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "{} [region {}] raft message {:?} is stale, current {:?}, ignore it",
                self.core.tag, region_id, msg_type, cur_epoch
            );
            self.ctx.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "{} [region {}] raft message {:?} is stale, current {:?}, tell to gc",
            self.core.tag, region_id, msg_type, cur_epoch
        );

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        if let Some(r) = target_region {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = self.ctx.trans.send(gc_msg) {
            error!(
                "{} [region {}] send gc message failed {:?}",
                self.core.tag, region_id, e
            );
        }
        self.ctx.need_flush_trans = true;
    }

    fn clear_region_size_in_range(&mut self, start_key: &[u8], end_key: &[u8]) {
        let start_key = data_key(start_key);
        let end_key = data_end_key(end_key);

        let meta = self.ctx.store_meta.lock().unwrap();
        for (_, region_id) in meta
            .region_ranges
            .range((Excluded(start_key), Included(end_key)))
        {
            let _ = self
                .scheduler
                .router()
                .send_peer_message(*region_id, PeerMsg::ClearStat);
        }
    }
}

impl<'a, T: Transport, C: PdClient> Store<'a, T, C> {
    fn check_msg(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let is_vote_msg = util::is_vote_msg(msg.get_message());
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
        // tell 2 is stale, so 2 can remove itself.

        // no exist, check with tombstone key.
        let state_key = keys::region_state_key(region_id);
        if let Some(local_state) = self
            .ctx
            .engines
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            if local_state.get_state() != PeerState::Tombstone {
                // Maybe split, but not registered yet.
                self.ctx.raft_metrics.message_dropped.region_nonexistent += 1;
                if util::is_first_vote_msg(msg.get_message()) {
                    info!(
                        "[region {}] doesn't exist yet, wait for it to be split",
                        region_id
                    );
                    let mut meta = self.ctx.store_meta.lock().unwrap();
                    return if meta.regions.contains_key(&region_id) {
                        // Retry.
                        Ok(false)
                    } else {
                        meta.pending_votes.push(msg.to_owned());
                        Ok(true)
                    };
                }
                return Err(box_err!(
                    "[region {}] region not exist but not tombstone: {:?}",
                    region_id,
                    local_state
                ));
            }
            debug!("[region {}] tombstone state: {:?}", region_id, local_state);
            let region = local_state.get_region();
            let region_epoch = region.get_region_epoch();
            if local_state.has_merge_state() {
                info!(
                    "[region {}] merged peer [epoch: {:?}] receive a stale message {:?}",
                    region_id, region_epoch, msg_type
                );

                let merge_target = if let Some(peer) = util::find_peer(region, from_store_id) {
                    // Maybe the target is promoted from learner to voter, but the follower
                    // doesn't know it. So we only compare peer id.
                    assert_eq!(peer.get_id(), msg.get_from_peer().get_id());
                    // Let stale peer decides whether it should wait for merging or just remove
                    // itself.
                    Some(local_state.get_merge_state().get_target().to_owned())
                } else {
                    // If a peer is isolated before prepare_merge and conf remove, it should just
                    // remove itself.
                    None
                };
                self.handle_stale_msg(msg, region_epoch.clone(), true, merge_target);
                return Ok(true);
            }
            // The region in this peer is already destroyed
            if util::is_epoch_stale(from_epoch, region_epoch) {
                info!(
                    "[region {}] tombstone peer [epoch: {:?}] \
                     receive a stale message {:?}",
                    region_id, region_epoch, msg_type,
                );

                let not_exist = util::find_peer(region, from_store_id).is_none();
                self.handle_stale_msg(msg, region_epoch.clone(), is_vote_msg && not_exist, None);

                return Ok(true);
            }

            if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                self.ctx.raft_metrics.message_dropped.region_tombstone_peer += 1;
                return Err(box_err!(
                    "tombstone peer [epoch: {:?}] receive an invalid \
                     message {:?}, ignore it",
                    region_epoch,
                    msg_type
                ));
            }
        }

        Ok(false)
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        // Don't use send_raft_message, which will fallback to raftstore.
        match self
            .scheduler
            .router()
            .send_peer_message(msg.get_region_id(), PeerMsg::RaftMessage(msg))
        {
            // TODO: full report.
            Ok(()) | Err(TrySendError::Full(_)) => return Ok(()),
            Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m))) => msg = m,
            _ => unreachable!(),
        }

        debug!(
            "{} [region {}] handle raft message {:?}, from {} to {}",
            self.core.tag,
            msg.get_region_id(),
            msg.get_message().get_msg_type(),
            msg.get_from_peer().get_id(),
            msg.get_to_peer().get_id()
        );

        if msg.get_to_peer().get_store_id() != self.store_id() {
            warn!(
                "[region {}] store not match, to store id {}, mine {}, ignore it",
                msg.get_region_id(),
                msg.get_to_peer().get_store_id(),
                self.store_id()
            );
            self.ctx.raft_metrics.message_dropped.mismatch_store_id += 1;
            return Ok(());
        }

        if !msg.has_region_epoch() {
            error!(
                "[region {}] missing epoch in raft message, ignore it",
                msg.get_region_id()
            );
            self.ctx.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return Ok(());
        }

        if msg.get_is_tombstone() || msg.has_merge_target() {
            return Ok(());
        }

        if self.check_msg(&msg)? {
            return Ok(());
        }

        if !self.maybe_create_peer(&msg)? {
            return Ok(());
        }

        let _ = self
            .scheduler
            .router()
            .send_peer_message(msg.get_region_id(), PeerMsg::RaftMessage(msg));
        Ok(())
    }

    fn on_store_tick(&mut self, tick: StoreTick) {
        let t = SlowTimer::new();
        match tick {
            StoreTick::CleanupImportSST => self.on_cleanup_import_sst_tick(),
            StoreTick::CompactCheck => self.on_compact_check_tick(),
            StoreTick::CompactLockCf => self.on_compact_lock_cf(),
            StoreTick::ConsistencyCheck => self.on_consistency_check_tick(),
            StoreTick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(),
            StoreTick::SnapGc => self.on_snap_mgr_gc(),
        }
        RAFT_EVENT_DURATION
            .with_label_values(&[tick.tag()])
            .observe(duration_to_sec(t.elapsed()) as f64);
        slow_log!(t, "{} handle timeout {:?}", self.core.tag, tick);
    }

    fn on_store_msg(&mut self, msg: StoreMsg) {
        match msg {
            StoreMsg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.core.tag, e);
            },
            StoreMsg::Tick(tick) => self.on_store_tick(tick),
            StoreMsg::SnapshotStats => self.store_heartbeat_pd(),
            StoreMsg::CompactedEvent(event) => self.on_compaction_finished(event),
            StoreMsg::ValidateSSTResult { invalid_ssts } => {
                self.on_validate_sst_result(invalid_ssts)
            }
            StoreMsg::SeekRegion {
                from_key,
                filter,
                limit,
                callback,
            } => self.seek_region(&from_key, filter, limit, callback),
            StoreMsg::Start(meta, cfg) => self.start(meta, cfg),
            StoreMsg::ClearRegionSizeInRange { start_key, end_key } => {
                self.clear_region_size_in_range(&start_key, &end_key)
            }
        }
    }

    pub fn poll(
        &mut self,
        receiver: &Receiver<StoreMsg>,
        buf: &mut Vec<StoreMsg>,
    ) -> Option<usize> {
        let mut mark_poll_pos = None;
        while buf.len() < self.core.cfg.as_ref().map_or(1024, |c| c.messages_per_tick) {
            match receiver.try_recv() {
                Ok(msg) => buf.push(msg),
                Err(TryRecvError::Empty) => {
                    mark_poll_pos = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    self.stop();
                    mark_poll_pos = Some(0);
                    break;
                }
            }
        }
        for msg in buf.drain(..) {
            self.on_store_msg(msg);
        }
        mark_poll_pos
    }
}

fn size_change_filter(info: &CompactionJobInfo) -> bool {
    // When calculating region size, we only consider write and default
    // column families.
    let cf = info.cf_name();
    if cf != CF_WRITE && cf != CF_DEFAULT {
        return false;
    }
    // Compactions in level 0 and level 1 are very frequently.
    if info.output_level() < 2 {
        return false;
    }

    true
}

pub fn new_compaction_listener(ch: Router) -> CompactionListener {
    let router = Mutex::new(ch);
    let compacted_handler = box move |compacted_event: CompactedEvent| {
        if let Err(e) = router
            .lock()
            .unwrap()
            .send_store_message(StoreMsg::CompactedEvent(compacted_event))
        {
            error!(
                "Send compaction finished event to raftstore failed: {:?}",
                e
            );
        }
    };
    CompactionListener::new(compacted_handler, Some(size_change_filter))
}

fn calc_region_declined_bytes(
    event: CompactedEvent,
    region_ranges: &BTreeMap<Key, u64>,
    bytes_threshold: u64,
) -> Vec<(u64, u64)> {
    // Calculate influenced regions.
    let mut influenced_regions = vec![];
    for (end_key, region_id) in
        region_ranges.range((Excluded(event.start_key), Included(event.end_key.clone())))
    {
        influenced_regions.push((region_id, end_key.clone()));
    }
    if let Some((end_key, region_id)) = region_ranges
        .range((Included(event.end_key), Unbounded))
        .next()
    {
        influenced_regions.push((region_id, end_key.clone()));
    }

    // Calculate declined bytes for each region.
    // `end_key` in influenced_regions are in incremental order.
    let mut region_declined_bytes = vec![];
    let mut last_end_key: Vec<u8> = vec![];
    for (region_id, end_key) in influenced_regions {
        let mut old_size = 0;
        for prop in &event.input_props {
            old_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
        }
        let mut new_size = 0;
        for prop in &event.output_props {
            new_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
        }
        last_end_key = end_key;

        // Filter some trivial declines for better performance.
        if old_size > new_size && old_size - new_size > bytes_threshold {
            region_declined_bytes.push((*region_id, old_size - new_size));
        }
    }

    region_declined_bytes
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use util::rocksdb::properties::{IndexHandle, IndexHandles, SizeProperties};
    use util::rocksdb::CompactedEvent;

    use super::*;

    #[test]
    fn test_calc_region_declined_bytes() {
        let index_handle1 = IndexHandle {
            size: 4 * 1024,
            offset: 4 * 1024,
        };
        let index_handle2 = IndexHandle {
            size: 4 * 1024,
            offset: 8 * 1024,
        };
        let index_handle3 = IndexHandle {
            size: 4 * 1024,
            offset: 12 * 1024,
        };
        let mut index_handles = IndexHandles::new();
        index_handles.add(b"a".to_vec(), index_handle1);
        index_handles.add(b"b".to_vec(), index_handle2);
        index_handles.add(b"c".to_vec(), index_handle3);
        let size_prop = SizeProperties {
            total_size: 12 * 1024,
            index_handles,
        };
        let event = CompactedEvent {
            cf: "default".to_owned(),
            output_level: 3,
            total_input_bytes: 12 * 1024,
            total_output_bytes: 0,
            start_key: size_prop.smallest_key().unwrap(),
            end_key: size_prop.largest_key().unwrap(),
            input_props: vec![size_prop.into()],
            output_props: vec![],
        };

        let mut region_ranges = BTreeMap::new();
        region_ranges.insert(b"a".to_vec(), 1);
        region_ranges.insert(b"b".to_vec(), 2);
        region_ranges.insert(b"c".to_vec(), 3);

        let declined_bytes = calc_region_declined_bytes(event, &region_ranges, 1024);
        let expected_declined_bytes = vec![(2, 8192), (3, 4096)];
        assert_eq!(declined_bytes, expected_declined_bytes);
    }
}
