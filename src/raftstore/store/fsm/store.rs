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

use protobuf;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver as StdReceiver};
use std::sync::Arc;
use std::time::Instant;
use std::{thread, u64};
use time;

use mio::{self, EventLoop, EventLoopConfig, Sender};
use rocksdb::{CompactionJobInfo, WriteBatch, DB};

use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb;
use kvproto::pdpb::StoreStats;
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};

use raft::StateRole;

use pd::{PdClient, PdRunner, PdTask};
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::coprocessor::{CoprocessorHost, RegionChangeEvent};
use raftstore::store::util::{is_initial_msg, KeysInfoFormatter};
use raftstore::Result;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::collections::{HashMap, HashSet};
use util::rocksdb::{CompactedEvent, CompactionListener};
use util::time::{duration_to_sec, SlowTimer};
use util::transport::SendCh;
use util::worker::{FutureWorker, Scheduler, Worker};
use util::{rocksdb, sys as util_sys, RingQueue};

use import::SSTImporter;
use raftstore::store::config::Config;
use raftstore::store::engine::{Iterable, Mutable, Peekable};
use raftstore::store::keys::{
    self, data_end_key, data_key, enc_end_key, enc_start_key, origin_key, DATA_MAX_KEY,
};
use raftstore::store::local_metrics::RaftMetrics;
use raftstore::store::metrics::*;
use raftstore::store::peer::Peer;
use raftstore::store::peer_storage::{self, CacheQueryStats};
use raftstore::store::transport::Transport;
use raftstore::store::worker::{
    ApplyRunner, ApplyTask, CleanupSSTRunner, CleanupSSTTask, CompactRunner, CompactTask,
    ConsistencyCheckRunner, LocalReader, RaftlogGcRunner, ReadTask, RegionRunner, RegionTask,
    SplitCheckRunner,
};
use raftstore::store::{
    util, Engines, Msg, SeekRegionCallback, SeekRegionFilter, SeekRegionResult, SignificantMsg,
    SnapManager, SnapshotDeleter, Store, Tick,
};

type Key = Vec<u8>;

const MIO_TICK_RATIO: u64 = 10;
const PENDING_VOTES_CAP: usize = 20;

// A helper structure to bundle all channels for messages to `Store`.
pub struct StoreChannel {
    pub sender: Sender<Msg>,
    pub significant_msg_receiver: StdReceiver<SignificantMsg>,
}

pub struct StoreStat {
    pub lock_cf_bytes_written: u64,

    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,

    pub engine_last_total_bytes_written: u64,
    pub engine_last_total_keys_written: u64,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,

            engine_last_total_bytes_written: 0,
            engine_last_total_keys_written: 0,
        }
    }
}

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}

pub fn create_event_loop<T, C>(cfg: &Config) -> Result<EventLoop<Store<T, C>>>
where
    T: Transport,
    C: PdClient,
{
    let mut config = EventLoopConfig::new();
    // To make raft base tick more accurate, timer tick should be small enough.
    config.timer_tick_ms(cfg.raft_base_tick_interval.as_millis() / MIO_TICK_RATIO);
    config.notify_capacity(cfg.notify_capacity);
    config.messages_per_tick(cfg.messages_per_tick);
    let event_loop = EventLoop::configured(config)?;
    Ok(event_loop)
}

impl<T: Transport, C: PdClient> Store<T, C> {
    #[cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
    pub fn new(
        ch: StoreChannel,
        meta: metapb::Store,
        mut cfg: Config,
        engines: Engines,
        trans: T,
        pd_client: Arc<C>,
        mgr: SnapManager,
        pd_worker: FutureWorker<PdTask>,
        local_reader: Worker<ReadTask>,
        mut coprocessor_host: CoprocessorHost,
        importer: Arc<SSTImporter>,
    ) -> Result<Store<T, C>> {
        // TODO: we can get cluster meta regularly too later.
        cfg.validate()?;

        let sendch = SendCh::new(ch.sender, "raftstore");
        let tag = format!("[store {}]", meta.get_id());

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, box SplitObserver);

        let mut s = Store {
            cfg: Rc::new(cfg),
            store: meta,
            engines,
            sendch,
            significant_msg_receiver: ch.significant_msg_receiver,
            region_peers: HashMap::default(),
            merging_regions: Some(vec![]),
            pending_raft_groups: HashSet::default(),
            split_check_worker: Worker::new("split-check"),
            region_worker: Worker::new("snapshot-worker"),
            raftlog_gc_worker: Worker::new("raft-gc-worker"),
            compact_worker: Worker::new("compact-worker"),
            pd_worker,
            consistency_check_worker: Worker::new("consistency-check"),
            cleanup_sst_worker: Worker::new("cleanup-sst"),
            apply_worker: Worker::new("apply-worker"),
            apply_res_receiver: None,
            local_reader,
            last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
            region_ranges: BTreeMap::new(),
            pending_snapshot_regions: vec![],
            pending_cross_snap: HashMap::default(),
            trans,
            pd_client,
            coprocessor_host: Arc::new(coprocessor_host),
            importer,
            snap_mgr: mgr,
            raft_metrics: RaftMetrics::default(),
            entry_cache_metries: Rc::new(RefCell::new(CacheQueryStats::default())),
            pending_votes: RingQueue::with_capacity(PENDING_VOTES_CAP),
            tag,
            start_time: time::get_time(),
            is_busy: false,
            store_stat: StoreStat::default(),
        };
        s.init()?;
        Ok(s)
    }

    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let kv_engine = Arc::clone(&self.engines.kv);
        let mut total_count = 0;
        let mut tomebstone_count = 0;
        let mut applying_count = 0;

        let t = Instant::now();
        let mut kv_wb = WriteBatch::new();
        let mut raft_wb = WriteBatch::new();
        let mut applying_regions = vec![];
        let mut prepare_merge = vec![];
        kv_engine.scan_cf(CF_RAFT, start_key, end_key, false, |key, value| {
            let (region_id, suffix) = keys::decode_region_meta_key(key)?;
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_count += 1;

            let local_state = protobuf::parse_from_bytes::<RegionLocalState>(value)?;
            let region = local_state.get_region();
            if local_state.get_state() == PeerState::Tombstone {
                tomebstone_count += 1;
                debug!(
                    "region {:?} is tombstone in store {}",
                    region,
                    self.store_id()
                );
                self.clear_stale_meta(&mut kv_wb, &mut raft_wb, &local_state);
                return Ok(true);
            }
            if local_state.get_state() == PeerState::Applying {
                // in case of restart happen when we just write region state to Applying,
                // but not write raft_local_state to raft rocksdb in time.
                peer_storage::recover_from_applying_state(&self.engines, &raft_wb, region_id)?;
                applying_count += 1;
                applying_regions.push(region.clone());
                return Ok(true);
            }
            if local_state.get_state() == PeerState::Merging {
                prepare_merge.push((
                    local_state.get_region().to_owned(),
                    local_state.get_merge_state().to_owned(),
                ));
            }

            let peer = Peer::create(self, region)?;
            self.region_ranges.insert(enc_end_key(region), region_id);
            // No need to check duplicated here, because we use region id as the key
            // in DB.
            self.region_peers.insert(region_id, peer);
            self.coprocessor_host.on_region_changed(
                region,
                RegionChangeEvent::Create,
                StateRole::Follower,
            );
            Ok(true)
        })?;

        if !kv_wb.is_empty() {
            self.engines.kv.write(kv_wb).unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.engines.raft.write(raft_wb).unwrap();
            self.engines.raft.sync_wal().unwrap();
        }

        // schedule applying snapshot after raft writebatch were written.
        for region in applying_regions {
            info!(
                "region {:?} is applying in store {}",
                region,
                self.store_id()
            );
            let mut peer = Peer::create(self, &region)?;
            peer.mut_store().schedule_applying_snapshot();
            self.region_ranges
                .insert(enc_end_key(&region), region.get_id());
            self.region_peers.insert(region.get_id(), peer);
        }

        // recover prepare_merge
        let merging_count = prepare_merge.len();
        for (region, state) in prepare_merge {
            info!(
                "region {:?} is merging in store {}",
                region,
                self.store_id()
            );
            self.on_ready_prepare_merge(region, state, false);
        }

        info!(
            "{} starts with {} regions, including {} tombstones, {} applying \
             regions and {} merging regions, takes {:?}",
            self.tag,
            total_count,
            tomebstone_count,
            applying_count,
            merging_count,
            t.elapsed()
        );

        self.clear_stale_data()?;

        Ok(())
    }
}

impl<T, C> Store<T, C> {
    fn clear_stale_meta(
        &mut self,
        kv_wb: &mut WriteBatch,
        raft_wb: &mut WriteBatch,
        origin_state: &RegionLocalState,
    ) {
        let region = origin_state.get_region();
        let raft_key = keys::raft_state_key(region.get_id());
        let raft_state = match self.engines.raft.get_msg(&raft_key).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };

        peer_storage::clear_meta(&self.engines, kv_wb, raft_wb, region.get_id(), &raft_state)
            .unwrap();
        let key = keys::region_state_key(region.get_id());
        let handle = rocksdb::get_cf_handle(&self.engines.kv, CF_RAFT).unwrap();
        kv_wb.put_msg_cf(handle, &key, origin_state).unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&mut self) -> Result<()> {
        let t = Instant::now();

        let mut ranges = Vec::new();
        let mut last_start_key = keys::data_key(b"");
        for region_id in self.region_ranges.values() {
            let region = self.region_peers[region_id].region();
            let start_key = keys::enc_start_key(region);
            ranges.push((last_start_key, start_key));
            last_start_key = keys::enc_end_key(region);
        }
        ranges.push((last_start_key, keys::DATA_MAX_KEY.to_vec()));

        rocksdb::roughly_cleanup_ranges(&self.engines.kv, &ranges)?;

        info!(
            "{} cleans up {} ranges garbage data, takes {:?}",
            self.tag,
            ranges.len(),
            t.elapsed()
        );

        Ok(())
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }

    #[inline]
    pub fn get_snap_mgr(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    pub fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    pub fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    pub fn read_scheduler(&self) -> Scheduler<ReadTask> {
        self.local_reader.scheduler()
    }

    pub fn engines(&self) -> Engines {
        self.engines.clone()
    }

    pub fn kv_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.kv)
    }

    pub fn raft_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.raft)
    }

    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn get_peers(&self) -> &HashMap<u64, Peer> {
        &self.region_peers
    }

    pub fn config(&self) -> Rc<Config> {
        Rc::clone(&self.cfg)
    }
}

impl<T: Transport, C: PdClient> Store<T, C> {
    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        self.snap_mgr.init()?;

        self.register_raft_base_tick(event_loop);
        self.register_raft_gc_log_tick(event_loop);
        self.register_split_region_check_tick(event_loop);
        self.register_compact_check_tick(event_loop);
        self.register_pd_store_heartbeat_tick(event_loop);
        self.register_pd_heartbeat_tick(event_loop);
        self.register_snap_mgr_gc_tick(event_loop);
        self.register_compact_lock_cf_tick(event_loop);
        self.register_consistency_check_tick(event_loop);
        self.register_merge_check_tick(event_loop);
        self.register_check_peer_stale_state_tick(event_loop);
        self.register_cleanup_import_sst_tick(event_loop);

        let split_check_runner = SplitCheckRunner::new(
            Arc::clone(&self.engines.kv),
            self.sendch.clone(),
            Arc::clone(&self.coprocessor_host),
        );

        box_try!(self.split_check_worker.start(split_check_runner));

        let region_runner = RegionRunner::new(
            self.engines.clone(),
            self.snap_mgr.clone(),
            self.cfg.snap_apply_batch_size.0 as usize,
            self.cfg.use_delete_range,
            self.cfg.clean_stale_peer_delay.0,
        );
        let timer = RegionRunner::new_timer();
        box_try!(self.region_worker.start_with_timer(region_runner, timer));

        let raftlog_gc_runner = RaftlogGcRunner::new(None);
        box_try!(self.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(Arc::clone(&self.engines.kv));
        box_try!(self.compact_worker.start(compact_runner));

        let pd_runner = PdRunner::new(
            self.store_id(),
            Arc::clone(&self.pd_client),
            self.sendch.clone(),
            Arc::clone(&self.engines.kv),
            self.pd_worker.scheduler(),
        );
        box_try!(self.pd_worker.start(pd_runner));

        let consistency_check_runner = ConsistencyCheckRunner::new(self.sendch.clone());
        box_try!(
            self.consistency_check_worker
                .start(consistency_check_runner)
        );

        let cleanup_sst_runner = CleanupSSTRunner::new(
            self.store_id(),
            self.sendch.clone(),
            Arc::clone(&self.importer),
            Arc::clone(&self.pd_client),
        );
        box_try!(self.cleanup_sst_worker.start(cleanup_sst_runner));

        let (tx, rx) = mpsc::channel();
        let apply_runner = ApplyRunner::new(self, tx, self.cfg.sync_log, self.cfg.use_delete_range);
        self.apply_res_receiver = Some(rx);
        box_try!(self.apply_worker.start(apply_runner));

        let reader = LocalReader::new(self);
        let timer = LocalReader::new_timer();
        box_try!(self.local_reader.start_with_timer(reader, timer));

        if let Err(e) = util_sys::thread::set_priority(util_sys::HIGH_PRI) {
            warn!("set thread priority for raftstore failed, error: {:?}", e);
        }

        event_loop.run(self)?;
        Ok(())
    }

    fn stop(&mut self) {
        info!("start to stop raftstore.");

        // Applying snapshot may take an unexpected long time.
        for peer in self.region_peers.values_mut() {
            peer.stop();
        }

        // Wait all workers finish.
        let mut handles: Vec<Option<thread::JoinHandle<()>>> = vec![];
        handles.push(self.split_check_worker.stop());
        handles.push(self.region_worker.stop());
        handles.push(self.raftlog_gc_worker.stop());
        handles.push(self.compact_worker.stop());
        handles.push(self.pd_worker.stop());
        handles.push(self.consistency_check_worker.stop());
        handles.push(self.cleanup_sst_worker.stop());
        handles.push(self.apply_worker.stop());
        handles.push(self.local_reader.stop());

        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }

        self.coprocessor_host.shutdown();

        info!("stop raftstore finished.");
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    pub fn maybe_create_peer(&mut self, region_id: u64, msg: &RaftMessage) -> Result<bool> {
        let target = msg.get_to_peer();
        // we may encounter a message with larger peer id, which means
        // current peer is stale, then we should remove current peer
        let mut has_peer = false;
        let mut job = None;
        if let Some(p) = self.region_peers.get_mut(&region_id) {
            has_peer = true;
            let target_peer_id = target.get_id();
            if p.peer_id() < target_peer_id {
                job = p.maybe_destroy();
                if job.is_none() {
                    self.raft_metrics.message_dropped.applying_snap += 1;
                    return Ok(false);
                }
            } else if p.peer_id() > target_peer_id {
                info!(
                    "[region {}] target peer id {} is less than {}, msg maybe stale.",
                    region_id,
                    target_peer_id,
                    p.peer_id()
                );
                self.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(false);
            }
        }

        if let Some(job) = job {
            info!(
                "[region {}] try to destroy stale peer {:?}",
                region_id, job.peer
            );
            if !self.handle_destroy_peer(job) {
                return Ok(false);
            }
            has_peer = false;
        }

        if has_peer {
            return Ok(true);
        }

        if !is_initial_msg(msg.get_message()) {
            let msg_type = msg.get_message().get_msg_type();
            debug!(
                "target peer {:?} doesn't exist, stale message {:?}.",
                target, msg_type
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        let start_key = data_key(msg.get_start_key());
        if let Some((_, exist_region_id)) = self
            .region_ranges
            .range((Excluded(start_key), Unbounded::<Key>))
            .next()
        {
            let exist_region = self.region_peers[exist_region_id].region();
            if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                debug!("msg {:?} is overlapped with region {:?}", msg, exist_region);
                if util::is_first_vote_msg(msg.get_message()) {
                    self.pending_votes.push(msg.to_owned());
                }
                self.raft_metrics.message_dropped.region_overlap += 1;

                // Make sure the range of region from msg is covered by existing regions.
                // If so, means that the region may be generated by some kinds of split
                // and merge by catching logs. So there is no need to accept a snapshot.
                if !is_range_covered(
                    &self.region_ranges,
                    |id: u64| self.region_peers[&id].region(),
                    data_key(msg.get_start_key()),
                    data_end_key(msg.get_end_key()),
                ) {
                    self.pending_cross_snap
                        .insert(region_id, msg.get_region_epoch().to_owned());
                }

                return Ok(false);
            }
        }

        // New created peers should know it's learner or not.
        let peer = Peer::replicate(self, region_id, target.clone())?;
        // following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        self.region_peers.insert(region_id, peer);
        Ok(true)
    }

    fn on_compaction_finished(&mut self, event: CompactedEvent) {
        // If size declining is trivial, skip.
        let total_bytes_declined = if event.total_input_bytes > event.total_output_bytes {
            event.total_input_bytes - event.total_output_bytes
        } else {
            0
        };
        if total_bytes_declined < self.cfg.region_split_check_diff.0
            || total_bytes_declined * 10 < event.total_input_bytes
        {
            return;
        }

        let output_level_str = event.output_level.to_string();
        COMPACTION_DECLINED_BYTES
            .with_label_values(&[&output_level_str])
            .observe(total_bytes_declined as f64);

        // self.cfg.region_split_check_diff.0 / 16 is an experienced value.
        let mut region_declined_bytes = calc_region_declined_bytes(
            event,
            &self.region_ranges,
            self.cfg.region_split_check_diff.0 / 16,
        );

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(region_declined_bytes.len() as f64);

        for (region_id, declined_bytes) in region_declined_bytes.drain(..) {
            if let Some(peer) = self.region_peers.get_mut(&region_id) {
                peer.compaction_declined_bytes += declined_bytes;
                if peer.compaction_declined_bytes >= self.cfg.region_split_check_diff.0 {
                    UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
                }
            }
        }
    }

    fn register_compact_check_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::CompactCheck,
            self.cfg.region_compact_check_interval.as_millis(),
        ) {
            error!("{} register compact check tick err: {:?}", self.tag, e);
        }
    }

    fn on_compact_check_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if self.compact_worker.is_busy() {
            debug!("compact worker is busy, check space redundancy next time");
        } else if self.region_ranges.is_empty() {
            debug!("there is no range need to check");
        } else if rocksdb::auto_compactions_is_disabled(&self.engines.kv) {
            debug!("skip compact check when disabled auto compactions.");
        } else {
            // Start from last checked key.
            let mut ranges_need_check =
                Vec::with_capacity(self.cfg.region_compact_check_step as usize + 1);
            ranges_need_check.push(self.last_compact_checked_key.clone());

            // Collect continuous ranges.
            let left_ranges = self.region_ranges.range((
                Excluded(self.last_compact_checked_key.clone()),
                Unbounded::<Key>,
            ));
            ranges_need_check.extend(
                left_ranges
                    .take(self.cfg.region_compact_check_step as usize)
                    .map(|(k, _)| k.to_owned()),
            );

            // Update last_compact_checked_key.
            let largest_key = self.region_ranges.keys().last().unwrap().to_vec();
            let last_key = ranges_need_check.last().unwrap().clone();
            if last_key == largest_key {
                // Range [largest key, DATA_MAX_KEY) also need to check.
                if last_key != keys::DATA_MAX_KEY.to_vec() {
                    ranges_need_check.push(keys::DATA_MAX_KEY.to_vec());
                }
                // Next task will start from the very beginning.
                self.last_compact_checked_key = keys::DATA_MIN_KEY.to_vec();
            } else {
                self.last_compact_checked_key = last_key;
            }

            // Schedule the task.
            let cf_names = vec![CF_DEFAULT.to_owned(), CF_WRITE.to_owned()];
            if let Err(e) = self.compact_worker.schedule(CompactTask::CheckAndCompact {
                cf_names,
                ranges: ranges_need_check,
                tombstones_num_threshold: self.cfg.region_compact_min_tombstones,
                tombstones_percent_threshold: self.cfg.region_compact_tombstones_percent,
            }) {
                error!("{} failed to schedule space check task: {}", self.tag, e);
            }
        }

        self.register_compact_check_tick(event_loop);
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::new();

        let used_size = self.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
        stats.set_store_id(self.store_id());
        stats.set_region_count(self.region_peers.len() as u32);

        let snap_stats = self.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        let mut apply_snapshot_count = 0;
        for peer in self.region_peers.values_mut() {
            if peer.mut_store().check_applying_snap() {
                apply_snapshot_count += 1;
            }
        }

        stats.set_applying_snap_count(apply_snapshot_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["applying"])
            .set(apply_snapshot_count);

        stats.set_start_time(self.start_time.sec as u32);

        // report store write flow to pd
        stats.set_bytes_written(
            self.store_stat.engine_total_bytes_written
                - self.store_stat.engine_last_total_bytes_written,
        );
        stats.set_keys_written(
            self.store_stat.engine_total_keys_written
                - self.store_stat.engine_last_total_keys_written,
        );
        self.store_stat.engine_last_total_bytes_written =
            self.store_stat.engine_total_bytes_written;
        self.store_stat.engine_last_total_keys_written = self.store_stat.engine_total_keys_written;

        stats.set_is_busy(self.is_busy);
        self.is_busy = false;

        let store_info = StoreInfo {
            engine: Arc::clone(&self.engines.kv),
            capacity: self.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat { stats, store_info };
        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick(event_loop);
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        let snap_keys = self.snap_mgr.list_idle_snap()?;
        if snap_keys.is_empty() {
            return Ok(());
        }
        let (mut last_region_id, mut compacted_idx, mut compacted_term) = (0, u64::MAX, u64::MAX);
        let mut is_applying_snap = false;
        for (key, is_sending) in snap_keys {
            if last_region_id != key.region_id {
                last_region_id = key.region_id;
                match self.region_peers.get(&key.region_id) {
                    None => {
                        // region is deleted
                        compacted_idx = u64::MAX;
                        compacted_term = u64::MAX;
                        is_applying_snap = false;
                    }
                    Some(peer) => {
                        let s = peer.get_store();
                        compacted_idx = s.truncated_index();
                        compacted_term = s.truncated_term();
                        is_applying_snap = s.is_applying_snapshot();
                    }
                };
            }

            if is_sending {
                let s = self.snap_mgr.get_snapshot_for_sending(&key)?;
                if key.term < compacted_term || key.idx < compacted_idx {
                    info!(
                        "[region {}] snap file {} has been compacted, delete.",
                        key.region_id, key
                    );
                    self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                } else if let Ok(meta) = s.meta() {
                    let modified = box_try!(meta.modified());
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed > self.cfg.snap_gc_timeout.0 {
                            info!(
                                "[region {}] snap file {} has been expired, delete.",
                                key.region_id, key
                            );
                            self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
                        }
                    }
                }
            } else if key.term <= compacted_term
                && (key.idx < compacted_idx || key.idx == compacted_idx && !is_applying_snap)
            {
                info!(
                    "[region {}] snap file {} has been applied, delete.",
                    key.region_id, key
                );
                let a = self.snap_mgr.get_snapshot_for_applying(&key)?;
                self.snap_mgr.delete_snapshot(&key, a.as_ref(), false);
            }
        }
        Ok(())
    }

    fn on_snap_mgr_gc(&mut self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!("{} failed to gc snap manager: {:?}", self.tag, e);
        }
        self.register_snap_mgr_gc_tick(event_loop);
    }

    fn on_compact_lock_cf(&mut self, event_loop: &mut EventLoop<Self>) {
        // Create a compact lock cf task(compact whole range) and schedule directly.
        if self.store_stat.lock_cf_bytes_written > self.cfg.lock_cf_compact_bytes_threshold.0 {
            self.store_stat.lock_cf_bytes_written = 0;
            let task = CompactTask::Compact {
                cf_name: String::from(CF_LOCK),
                start_key: None,
                end_key: None,
            };
            if let Err(e) = self.compact_worker.schedule(task) {
                error!(
                    "{} failed to schedule compact lock cf task: {:?}",
                    self.tag, e
                );
            }
        }

        self.register_compact_lock_cf_tick(event_loop);
    }

    fn register_pd_store_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::PdStoreHeartbeat,
            self.cfg.pd_store_heartbeat_tick_interval.as_millis(),
        ) {
            error!("{} register pd store heartbeat tick err: {:?}", self.tag, e);
        };
    }

    fn register_snap_mgr_gc_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::SnapGc,
            self.cfg.snap_mgr_gc_tick_interval.as_millis(),
        ) {
            error!("{} register snap mgr gc tick err: {:?}", self.tag, e);
        }
    }

    fn register_compact_lock_cf_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::CompactLockCf,
            self.cfg.lock_cf_compact_interval.as_millis(),
        ) {
            error!("{} register compact cf-lock tick err: {:?}", self.tag, e);
        }
    }
}

impl<T: Transport, C: PdClient> Store<T, C> {
    fn on_validate_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        // A stale peer can still ingest a stale SST before it is
        // destroyed. We need to make sure that no stale peer exists.
        let mut delete_ssts = Vec::new();
        for sst in ssts {
            if !self.region_peers.contains_key(&sst.get_region_id()) {
                delete_ssts.push(sst);
            }
        }
        if delete_ssts.is_empty() {
            return;
        }

        let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
        if let Err(e) = self.cleanup_sst_worker.schedule(task) {
            error!("schedule to delete ssts: {:?}", e);
        }
    }

    fn on_cleanup_import_sst(&mut self) -> Result<()> {
        let mut delete_ssts = Vec::new();
        let mut validate_ssts = Vec::new();

        let ssts = box_try!(self.importer.list_ssts());
        for sst in ssts {
            if let Some(peer) = self.region_peers.get(&sst.get_region_id()) {
                let region_epoch = peer.region().get_region_epoch();
                if util::is_epoch_stale(sst.get_region_epoch(), region_epoch) {
                    // If the SST epoch is stale, it will not be ingested anymore.
                    delete_ssts.push(sst);
                }
            } else {
                // If the peer doesn't exist, we need to validate the SST through PD.
                validate_ssts.push(sst);
            }
        }

        if !delete_ssts.is_empty() {
            let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
            if let Err(e) = self.cleanup_sst_worker.schedule(task) {
                error!("schedule to delete ssts: {:?}", e);
            }
        }

        if !validate_ssts.is_empty() {
            let task = CleanupSSTTask::ValidateSST {
                ssts: validate_ssts,
            };
            if let Err(e) = self.cleanup_sst_worker.schedule(task) {
                error!("schedule to validate ssts: {:?}", e);
            }
        }

        Ok(())
    }

    fn on_cleanup_import_sst_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = self.on_cleanup_import_sst() {
            error!("{} failed to cleanup import sst: {:?}", self.tag, e);
        }
        self.register_cleanup_import_sst_tick(event_loop);
    }

    fn register_cleanup_import_sst_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(
            event_loop,
            Tick::CleanupImportSST,
            self.cfg.cleanup_import_sst_interval.as_millis(),
        ) {
            error!("{} register cleanup import sst tick err: {:?}", self.tag, e);
        }
    }

    /// Find the first region `r` whose range contains or greater than `from_key` and the peer on
    /// this TiKV satisfies `filter(peer)` returns true.
    fn seek_region(
        &self,
        from_key: &[u8],
        filter: SeekRegionFilter,
        mut limit: u32,
        callback: SeekRegionCallback,
    ) {
        assert!(limit > 0);

        let from_key = data_key(from_key);
        for (end_key, region_id) in self.region_ranges.range((Excluded(from_key), Unbounded)) {
            let peer = &self.region_peers[region_id];
            if filter(peer.region(), peer.raft_group.raft.state) {
                callback(SeekRegionResult::Found(peer.region().clone()));
                return;
            }

            limit -= 1;
            if limit == 0 {
                // `origin_key` does not handle `DATA_MAX_KEY`, but we can return `Ended` rather
                // than `LimitExceeded`.
                if end_key.as_slice() >= DATA_MAX_KEY {
                    break;
                }

                callback(SeekRegionResult::LimitExceeded {
                    next_key: origin_key(end_key).to_vec(),
                });
                return;
            }
        }
        callback(SeekRegionResult::Ended);
    }

    fn clear_region_size_in_range(&mut self, start_key: &[u8], end_key: &[u8]) {
        let start_key = data_key(start_key);
        let end_key = data_end_key(end_key);

        for (_, region_id) in self
            .region_ranges
            .range((Excluded(start_key), Included(end_key)))
        {
            let peer = self.region_peers.get_mut(region_id).unwrap();

            peer.approximate_size = None;
            peer.approximate_keys = None;
        }
    }
}

pub fn register_timer<T: Transport, C: PdClient>(
    event_loop: &mut EventLoop<Store<T, C>>,
    tick: Tick,
    delay: u64,
) -> Result<()> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    if delay == 0 {
        // 0 delay means turn off the timer.
        return Ok(());
    }
    if let Err(e) = event_loop.timeout_ms(tick, delay) {
        return Err(box_err!(
            "failed to register timeout [{:?}, delay: {:?}ms]: {:?}",
            tick,
            delay,
            e
        ));
    }
    Ok(())
}

impl<T: Transport, C: PdClient> mio::Handler for Store<T, C> {
    type Timeout = Tick;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
        match msg {
            Msg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.tag, e);
            },
            Msg::RaftCmd {
                send_time,
                request,
                callback,
            } => {
                self.raft_metrics
                    .propose
                    .request_wait_time
                    .observe(duration_to_sec(send_time.elapsed()) as f64);
                self.propose_raft_command(request, callback)
            }
            Msg::Quit => {
                info!("{} receive quit message", self.tag);
                event_loop.shutdown();
            }
            Msg::SnapshotStats => self.store_heartbeat_pd(),
            Msg::ComputeHashResult {
                region_id,
                index,
                hash,
            } => {
                self.on_hash_computed(region_id, index, hash);
            }
            Msg::SplitRegion {
                region_id,
                region_epoch,
                split_keys,
                callback,
            } => {
                info!(
                    "on split region {} with {}",
                    region_id,
                    KeysInfoFormatter(&split_keys)
                );
                self.on_prepare_split_region(region_id, region_epoch, split_keys, callback);
            }
            Msg::RegionApproximateSize { region_id, size } => {
                self.on_approximate_region_size(region_id, size)
            }
            Msg::RegionApproximateKeys { region_id, keys } => {
                self.on_approximate_region_keys(region_id, keys)
            }
            Msg::CompactedEvent(event) => self.on_compaction_finished(event),
            Msg::HalfSplitRegion {
                region_id,
                region_epoch,
                policy,
            } => self.on_schedule_half_split_region(region_id, &region_epoch, policy),
            Msg::MergeFail { region_id } => self.on_merge_fail(region_id),
            Msg::ValidateSSTResult { invalid_ssts } => self.on_validate_sst_result(invalid_ssts),
            Msg::SeekRegion {
                from_key,
                filter,
                limit,
                callback,
            } => self.seek_region(&from_key, filter, limit, callback),
            Msg::ClearRegionSizeInRange { start_key, end_key } => {
                self.clear_region_size_in_range(&start_key, &end_key)
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
        let t = SlowTimer::new();
        match timeout {
            Tick::Raft => self.on_raft_base_tick(event_loop),
            Tick::RaftLogGc => self.on_raft_gc_log_tick(event_loop),
            Tick::SplitRegionCheck => self.on_split_region_check_tick(event_loop),
            Tick::CompactCheck => self.on_compact_check_tick(event_loop),
            Tick::PdHeartbeat => self.on_pd_heartbeat_tick(event_loop),
            Tick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(event_loop),
            Tick::SnapGc => self.on_snap_mgr_gc(event_loop),
            Tick::CompactLockCf => self.on_compact_lock_cf(event_loop),
            Tick::ConsistencyCheck => self.on_consistency_check_tick(event_loop),
            Tick::CheckMerge => self.on_check_merge(event_loop),
            Tick::CheckPeerStaleState => self.on_check_peer_stale_state_tick(event_loop),
            Tick::CleanupImportSST => self.on_cleanup_import_sst_tick(event_loop),
        }
        RAFT_EVENT_DURATION
            .with_label_values(&[timeout.tag()])
            .observe(duration_to_sec(t.elapsed()) as f64);
        slow_log!(t, "{} handle timeout {:?}", self.tag, timeout);
    }

    // This method is invoked very frequently, should avoid time consuming operation.
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        if !event_loop.is_running() {
            self.stop();
            return;
        }

        // We handle raft ready in event loop.
        if !self.pending_raft_groups.is_empty() {
            self.on_raft_ready();
        }

        self.poll_significant_msg();

        self.poll_apply();

        self.pending_snapshot_regions.clear();
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

pub fn new_compaction_listener(ch: SendCh<Msg>) -> CompactionListener {
    let compacted_handler = box move |compacted_event: CompactedEvent| {
        if let Err(e) = ch.try_send(Msg::CompactedEvent(compacted_event)) {
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

// check whether the range is covered by existing regions.
fn is_range_covered<'a, F: Fn(u64) -> &'a metapb::Region>(
    region_ranges: &BTreeMap<Key, u64>,
    get_region: F,
    mut start: Vec<u8>,
    end: Vec<u8>,
) -> bool {
    for (end_key, &id) in region_ranges.range((Excluded(start.clone()), Unbounded::<Key>)) {
        let mut region = get_region(id);
        // find a missing range
        if start < enc_start_key(region) {
            return false;
        }
        if *end_key >= end {
            return true;
        }
        start = end_key.clone();
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;

    use protobuf::RepeatedField;
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

    #[test]
    fn test_is_range_covered() {
        let meta = vec![(b"b", b"d"), (b"d", b"e"), (b"e", b"f"), (b"f", b"h")];
        let mut region_ranges = BTreeMap::new();
        let mut region_peers = HashMap::new();

        {
            for (i, (start, end)) in meta.into_iter().enumerate() {
                let mut region = metapb::Region::new();
                let peer = metapb::Peer::new();
                region.set_peers(RepeatedField::from_vec(vec![peer]));
                region.set_start_key(start.to_vec());
                region.set_end_key(end.to_vec());

                region_ranges.insert(enc_end_key(&region), i as u64);
                region_peers.insert(i as u64, region);
            }

            let check_range = |start: &[u8], end: &[u8]| {
                is_range_covered(
                    &region_ranges,
                    |id: u64| &region_peers[&id],
                    data_key(start),
                    data_end_key(end),
                )
            };

            assert!(!check_range(b"a", b"c"));
            assert!(check_range(b"b", b"d"));
            assert!(check_range(b"b", b"e"));
            assert!(check_range(b"e", b"f"));
            assert!(check_range(b"b", b"g"));
            assert!(check_range(b"e", b"h"));
            assert!(!check_range(b"e", b"n"));
            assert!(!check_range(b"g", b"n"));
            assert!(!check_range(b"o", b"z"));
            assert!(!check_range(b"", b""));
        }

        let meta = vec![(b"b", b"d"), (b"e", b"f"), (b"f", b"h")];
        region_ranges.clear();
        region_peers.clear();
        {
            for (i, (start, end)) in meta.into_iter().enumerate() {
                let mut region = metapb::Region::new();
                let peer = metapb::Peer::new();
                region.set_peers(RepeatedField::from_vec(vec![peer]));
                region.set_start_key(start.to_vec());
                region.set_end_key(end.to_vec());

                region_ranges.insert(enc_end_key(&region), i as u64);
                region_peers.insert(i as u64, region);
            }

            let check_range = |start: &[u8], end: &[u8]| {
                is_range_covered(
                    &region_ranges,
                    |id: u64| &region_peers[&id],
                    data_key(start),
                    data_end_key(end),
                )
            };

            assert!(!check_range(b"a", b"c"));
            assert!(check_range(b"b", b"d"));
            assert!(!check_range(b"b", b"e"));
            assert!(check_range(b"e", b"f"));
            assert!(!check_range(b"b", b"g"));
            assert!(check_range(b"e", b"g"));
            assert!(check_range(b"e", b"h"));
            assert!(!check_range(b"e", b"n"));
            assert!(!check_range(b"g", b"n"));
            assert!(!check_range(b"o", b"z"));
            assert!(!check_range(b"", b""));
        }
    }
}
