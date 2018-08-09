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
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{thread, u64};
use time::{self, Timespec};

use futures::{Async, Future, Poll, Stream};
use rocksdb::{CompactionJobInfo, WriteBatch, DB};

use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb;
use kvproto::pdpb::StoreStats;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest};
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState};
use raft::eraftpb::MessageType;
use raft::INVALID_INDEX;

use pd::{PdClient, PdRunner, PdTask};
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::Result;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::collections::HashMap;
use util::escape;
use util::mpsc::{loose_bounded, LooseBoundedSender, Receiver};
use util::rocksdb::event_listener::CompactionListener;
use util::rocksdb::{self, CompactedEvent};
use util::sys as util_sys;
use util::timer::Timer;
use util::transport::RetryableSendCh;
use util::worker::{FutureWorker, Scheduler, Worker};
use util::RingQueue;

use super::config::Config;
use super::engine::{Iterable, Mutable, Peekable};
use super::keys::{self, data_end_key, data_key, enc_end_key, enc_start_key};
use super::local_metrics::RaftMetrics;
use super::metrics::*;
use super::peer::{ConsistencyState, Peer, PeerStat};
use super::peer_storage;
use super::pineapple::PeerHolder;
use super::router::{InternalMailboxes, InternalTransport, RangeState};
use super::transport::Transport;
use super::worker::{
    ApplyRunner, ApplyTask, CleanupSSTRunner, CleanupSSTTask, CompactRunner, CompactTask,
    ConsistencyCheckRunner, ConsistencyCheckTask, RaftlogGcRunner, RaftlogGcTask, RegionRunner,
    RegionTask, SplitCheckRunner, SplitCheckTask, STALE_PEER_CHECK_INTERVAL,
};
use super::{util, AllMsg, Engines, Msg, SnapManager, Tick};
use futures_cpupool::CpuPool;
use import::SSTImporter;
use std::sync::Mutex;
use tokio_timer::timer::Handle;
use util::timer::GLOBAL_TIMER_HANDLE;

type Key = Vec<u8>;

const PENDING_VOTES_CAP: usize = 20;

pub fn create_transport(cfg: &Config) -> (InternalMailboxes, Receiver<AllMsg>) {
    let (tx, rx) = loose_bounded(cfg.notify_capacity);
    let boxes = InternalMailboxes::default();
    boxes.lock().unwrap().insert(0, tx);
    (boxes, rx)
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

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub async_remove: bool,
    pub region_id: u64,
    pub peer: metapb::Peer,
}

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}

pub struct Store<T: 'static, C: 'static> {
    cfg: Arc<Config>,
    engines: Engines,
    store: metapb::Store,
    internal_mailboxes: InternalMailboxes,
    scheduler: LooseBoundedSender<AllMsg>,

    range_states: Arc<Mutex<RangeState>>,
    pub split_check_worker: Worker<SplitCheckTask>,
    pub raftlog_gc_worker: Worker<RaftlogGcTask>,
    region_worker: Worker<RegionTask>,
    compact_worker: Worker<CompactTask>,
    pub pd_worker: FutureWorker<PdTask>,
    pub consistency_check_worker: Worker<ConsistencyCheckTask>,
    pub cleanup_sst_worker: Worker<CleanupSSTTask>,
    pub apply_worker: Worker<ApplyTask>,

    last_compact_checked_key: Key,

    trans: T,
    pd_client: Arc<C>,

    pub coprocessor_host: Arc<CoprocessorHost>,

    importer: Arc<SSTImporter>,

    pub snap_mgr: SnapManager,

    raft_metrics: RaftMetrics,

    tag: String,

    start_time: Timespec,
    is_busy: bool,

    pending_votes: RingQueue<RaftMessage>,

    store_stat: StoreStat,
    poller: CpuPool,
    receiver: Receiver<AllMsg>,
    handle: Handle,
    stopped: bool,
}

impl<T: Transport, C: PdClient> Store<T, C> {
    #[cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
    pub fn new(
        internal_mailboxes: InternalMailboxes,
        receiver: Receiver<AllMsg>,
        meta: metapb::Store,
        mut cfg: Config,
        engines: Engines,
        trans: T,
        pd_client: Arc<C>,
        mgr: SnapManager,
        pd_worker: FutureWorker<PdTask>,
        mut coprocessor_host: CoprocessorHost,
        importer: Arc<SSTImporter>,
        poller: CpuPool,
    ) -> Result<Store<T, C>> {
        // TODO: we can get cluster meta regularly too later.
        cfg.validate()?;

        let tag = format!("[store {}]", meta.get_id());

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, box SplitObserver);
        let scheduler = internal_mailboxes.lock().unwrap()[&0].to_owned();

        let mut s = Store {
            cfg: Arc::new(cfg),
            store: meta,
            engines,
            internal_mailboxes,
            scheduler,
            range_states: Arc::new(Mutex::new(RangeState::default())),
            split_check_worker: Worker::new("split check worker"),
            region_worker: Worker::new("snapshot worker"),
            raftlog_gc_worker: Worker::new("raft gc worker"),
            compact_worker: Worker::new("compact worker"),
            pd_worker,
            consistency_check_worker: Worker::new("consistency check worker"),
            cleanup_sst_worker: Worker::new("cleanup sst worker"),
            apply_worker: Worker::new("apply worker"),
            last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
            trans,
            pd_client,
            coprocessor_host: Arc::new(coprocessor_host),
            importer,
            snap_mgr: mgr,
            raft_metrics: RaftMetrics::default(),
            pending_votes: RingQueue::with_capacity(PENDING_VOTES_CAP),
            tag,
            start_time: time::get_time(),
            is_busy: false,
            store_stat: StoreStat::default(),
            poller,
            receiver,
            handle: GLOBAL_TIMER_HANDLE.clone(),
            stopped: false,
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
        let mut merging_count = 0;
        let mut peers = HashMap::default();
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

            let mut peer = Peer::create(self, region)?;
            if local_state.get_state() == PeerState::Merging {
                merging_count += 1;
                peer.pending_merge_state = Some(local_state.get_merge_state().to_owned());
            }
            // No need to check duplicated here, because we use region id as the key
            // in DB.
            peers.insert(region_id, peer);
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
            peers.insert(region.get_id(), peer);
        }

        let mut range_states = self.range_states.lock().unwrap();
        let mut mailboxes = self.internal_mailboxes.lock().unwrap();
        for (region_id, peer) in peers {
            assert_ne!(region_id, 0);
            let region = peer.region().to_owned();
            range_states
                .region_ranges
                .insert(enc_end_key(&region), region_id);
            let _is_merging = peer.pending_merge_state.is_some();
            let trans = InternalTransport::new(self.internal_mailboxes.clone());
            let (sender, mut holder) = PeerHolder::new(
                peer,
                &self,
                self.poller.clone(),
                self.range_states.clone(),
                trans,
                self.trans.clone(),
            );
            holder.start();
            self.poller.spawn(holder).forget();
            range_states.region_peers.insert(region_id, region);
            mailboxes.insert(region_id, sender);
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

        self.clear_stale_data(&range_states)?;

        Ok(())
    }
}

impl<T, C> Store<T, C> {
    pub fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    pub fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    pub fn engines(&self) -> Engines {
        self.engines.clone()
    }

    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn config(&self) -> Arc<Config> {
        Arc::clone(&self.cfg)
    }

    pub fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        Arc::clone(&self.coprocessor_host)
    }

    pub fn importer(&self) -> Arc<SSTImporter> {
        Arc::clone(&self.importer)
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
    fn clear_stale_data(&self, range_state: &RangeState) -> Result<()> {
        let t = Instant::now();

        let mut ranges = Vec::new();
        let mut last_start_key = keys::data_key(b"");
        for region_id in range_state.region_ranges.values() {
            let region = &range_state.region_peers[region_id];
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

    pub fn get_sendch(&self) -> RetryableSendCh<Msg, InternalTransport> {
        RetryableSendCh::new(
            InternalTransport::new(self.internal_mailboxes.clone()),
            "raftstore",
        )
    }

    #[inline]
    pub fn get_snap_mgr(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    pub fn kv_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.kv)
    }

    pub fn raft_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.raft)
    }
}

impl<T: Transport, C: PdClient> Store<T, C> {
    pub fn run(mut self) -> Result<()> {
        self.snap_mgr.init()?;

        self.register_compact_check_tick();
        self.register_pd_store_heartbeat_tick();
        self.register_compact_lock_cf_tick();
        self.register_cleanup_import_sst_tick();

        let split_check_runner = SplitCheckRunner::new(
            Arc::clone(&self.engines.kv),
            self.get_sendch(),
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
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(STALE_PEER_CHECK_INTERVAL), ());
        box_try!(self.region_worker.start_with_timer(region_runner, timer));

        let raftlog_gc_runner = RaftlogGcRunner::new(None);
        box_try!(self.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(Arc::clone(&self.engines.kv));
        box_try!(self.compact_worker.start(compact_runner));

        let pd_runner = PdRunner::new(
            self.store_id(),
            Arc::clone(&self.pd_client),
            self.get_sendch(),
            Arc::clone(&self.engines.kv),
        );
        box_try!(self.pd_worker.start(pd_runner));

        let consistency_check_runner = ConsistencyCheckRunner::new(self.get_sendch());
        box_try!(
            self.consistency_check_worker
                .start(consistency_check_runner)
        );

        let cleanup_sst_runner = CleanupSSTRunner::new(
            self.store_id(),
            self.get_sendch(),
            Arc::clone(&self.importer),
            Arc::clone(&self.pd_client),
        );
        box_try!(self.cleanup_sst_worker.start(cleanup_sst_runner));

        let internal_tans = InternalTransport::new(self.internal_mailboxes.clone());
        let apply_runner = ApplyRunner::new(
            &self,
            internal_tans,
            self.cfg.sync_log,
            self.cfg.use_delete_range,
        );
        box_try!(self.apply_worker.start(apply_runner));

        if let Err(e) = util_sys::pri::set_priority(util_sys::HIGH_PRI) {
            warn!("set priority for raftstore failed, error: {:?}", e);
        }

        let poller = self.poller.clone();
        poller.spawn(self).forget();

        Ok(())
    }

    fn stop(&mut self) {
        info!("{} start to stop raftstore.", self.tag);

        // Applying snapshot may take an unexpected long time.
        {
            let mut mailboxes = self.internal_mailboxes.lock().unwrap();
            for sendch in mailboxes.values_mut() {
                let _ = sendch.force_send(AllMsg::Msg(Msg::Quit));
            }
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

        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }

        self.coprocessor_host.shutdown();
        self.stopped = true;

        info!("{} stop raftstore finished.", self.tag);
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn on_raft_message(&mut self, msg: RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        if region_id == 0 {
            return Err(box_err!("invalid target region 0: {:?}", msg));
        }
        {
            let mut mailboxes = self.internal_mailboxes.lock().unwrap();
            if let Some(sender) = mailboxes.get_mut(&region_id) {
                let _ = sender.force_send(AllMsg::Msg(Msg::RaftMessage(msg)));
                return Ok(false);
            }
        }

        let msg_type = msg.get_message().get_msg_type();
        let is_vote_msg = msg_type == MessageType::MsgRequestVote;

        // no exist, check with tombstone key.
        let state_key = keys::region_state_key(region_id);
        if let Some(local_state) = self
            .engines
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            if local_state.get_state() != PeerState::Tombstone {
                // Maybe split, but not registered yet.
                self.raft_metrics.message_dropped.region_nonexistent += 1;
                if util::is_first_vote_msg(&msg) {
                    self.pending_votes.push(msg);
                    info!(
                        "[region {}] doesn't exist yet, wait for it to be split",
                        region_id
                    );
                    return Ok(true);
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
            let from_store_id = msg.get_from_peer().get_store_id();
            let from_epoch = msg.get_region_epoch();
            if local_state.has_merge_state() {
                info!(
                    "[region {}] merged peer [epoch: {:?}] receive a stale message {:?}",
                    region_id, region_epoch, msg_type
                );

                let merge_target = if let Some(peer) = util::find_peer(region, from_store_id) {
                    assert_eq!(peer, msg.get_from_peer());
                    // Let stale peer decides whether it should wait for merging or just remove
                    // itself.
                    Some(local_state.get_merge_state().get_target().to_owned())
                } else {
                    // If a peer is isolated before prepare_merge and conf remove, it should just
                    // remove itself.
                    None
                };
                Self::handle_stale_msg(
                    &self.trans,
                    &msg,
                    region_epoch,
                    true,
                    merge_target,
                    &mut self.raft_metrics,
                );
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
                Self::handle_stale_msg(
                    &self.trans,
                    &msg,
                    region_epoch,
                    is_vote_msg && not_exist,
                    None,
                    &mut self.raft_metrics,
                );

                return Ok(true);
            }

            if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                self.raft_metrics.message_dropped.region_tombstone_peer += 1;
                return Err(box_err!(
                    "tombstone peer [epoch: {:?}] receive an invalid \
                     message {:?}, ignore it",
                    region_epoch,
                    msg_type
                ));
            }
        }

        if msg_type != MessageType::MsgRequestVote
            && (msg_type != MessageType::MsgHeartbeat
                || msg.get_message().get_commit() != INVALID_INDEX)
        {
            debug!(
                "target peer {:?} doesn't exist, stale message {:?}.",
                msg.get_to_peer(),
                msg_type
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        let start_key = data_key(msg.get_start_key());
        let mut range_states_guard = self.range_states.lock().unwrap();
        let range_states: &mut RangeState = &mut *range_states_guard;
        if let Some((_, &exist_region_id)) = range_states
            .region_ranges
            .range((Excluded(start_key), Unbounded::<Key>))
            .next()
        {
            let exist_region = &range_states.region_peers[&exist_region_id];
            if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                debug!("msg {:?} is overlapped with region {:?}", msg, exist_region);
                if util::is_first_vote_msg(&msg) {
                    self.pending_votes.push(msg.to_owned());
                }
                self.raft_metrics.message_dropped.region_overlap += 1;
                range_states
                    .pending_cross_snap
                    .insert(region_id, msg.get_region_epoch().to_owned());
                return Ok(false);
            }
        }

        // New created peers should know it's learner or not.
        let peer = Peer::replicate(self, region_id, msg.get_to_peer().clone())?;
        let trans = InternalTransport::new(self.internal_mailboxes.clone());
        let (sender, mut holder) = PeerHolder::new(
            peer,
            &self,
            self.poller.clone(),
            self.range_states.clone(),
            trans,
            self.trans.clone(),
        );
        holder.start();
        self.poller.spawn(holder).forget();
        // following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        self.internal_mailboxes
            .lock()
            .unwrap()
            .insert(region_id, sender);
        Ok(true)
    }

    fn handle_stale_msg(
        trans: &T,
        msg: &RaftMessage,
        cur_epoch: &metapb::RegionEpoch,
        need_gc: bool,
        target_region: Option<metapb::Region>,
        raft_metrics: &mut RaftMetrics,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "[region {}] raft message {:?} is stale, current {:?}, ignore it",
                region_id, msg_type, cur_epoch
            );
            raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "[region {}] raft message {:?} is stale, current {:?}, tell to gc",
            region_id, msg_type, cur_epoch
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
        if let Err(e) = trans.send(gc_msg) {
            error!("[region {}] send gc message failed {:?}", region_id, e);
        }
    }

    fn register_tick(&self, tick: Tick, delay: Duration) {
        let mut sender = self.scheduler.clone();
        self.poller
            .spawn(self.handle.delay(Instant::now() + delay).map(move |_| {
                let _ = sender.force_send(AllMsg::Tick(tick));
            }))
            .forget()
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
        let mut region_declined_bytes = {
            let range_state = self.range_states.lock().unwrap();
            calc_region_declined_bytes(
                event,
                &range_state.region_ranges,
                self.cfg.region_split_check_diff.0 / 16,
            )
        };

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(region_declined_bytes.len() as f64);

        {
            let mut mailboxes = self.internal_mailboxes.lock().unwrap();
            for (region_id, declined_bytes) in region_declined_bytes.drain(..) {
                if let Some(mailbox) = mailboxes.get_mut(&region_id) {
                    let _ =
                        mailbox.try_send(AllMsg::Msg(Msg::CompactedDeclinedBytes(declined_bytes)));
                }
            }
        }
    }

    fn register_compact_check_tick(&self) {
        self.register_tick(Tick::CompactCheck, self.cfg.region_compact_check_interval.0)
    }

    fn on_compact_check_tick(&mut self) {
        self.register_compact_check_tick();
        if self.compact_worker.is_busy() {
            debug!("compact worker is busy, check space redundancy next time");
            return;
        }
        let region_ranges = self.range_states.lock().unwrap().region_ranges.clone();
        if region_ranges.is_empty() {
            debug!("there is no range need to check");
            return;
        }
        if rocksdb::auto_compactions_is_disabled(&self.engines.kv) {
            debug!("skip compact check when disabled auto compactions.");
            return;
        }

        // Start from last checked key.
        let mut ranges_need_check =
            Vec::with_capacity(self.cfg.region_compact_check_step as usize + 1);
        ranges_need_check.push(self.last_compact_checked_key.clone());

        // Collect continuous ranges.
        let left_ranges = region_ranges.range((
            Excluded(self.last_compact_checked_key.clone()),
            Unbounded::<Key>,
        ));
        ranges_need_check.extend(
            left_ranges
                .take(self.cfg.region_compact_check_step as usize)
                .map(|(k, _)| k.to_owned()),
        );

        // Update last_compact_checked_key.
        let largest_key = region_ranges.keys().last().unwrap().to_vec();
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

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::new();

        let used_size = self.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
        stats.set_store_id(self.store_id());
        // TODO: support count peers and applying.
        stats.set_region_count(self.range_states.lock().unwrap().region_peers.len() as u32);

        let snap_stats = self.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

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

    fn on_pd_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick();
    }

    fn on_compact_lock_cf(&mut self) {
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

        self.register_compact_lock_cf_tick();
    }

    fn register_pd_store_heartbeat_tick(&self) {
        self.register_tick(
            Tick::PdStoreHeartbeat,
            self.cfg.pd_store_heartbeat_tick_interval.0,
        )
    }

    fn register_compact_lock_cf_tick(&self) {
        self.register_tick(Tick::CompactLockCf, self.cfg.lock_cf_compact_interval.0)
    }

    fn on_validate_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        // A stale peer can still ingest a stale SST before it is
        // destroyed. We need to make sure that no stale peer exists.
        let mut delete_ssts = Vec::new();
        {
            let mailboxes = self.internal_mailboxes.lock().unwrap();
            for sst in ssts {
                if !mailboxes.contains_key(&sst.get_region_id()) {
                    delete_ssts.push(sst);
                }
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
        let ssts = box_try!(self.importer.list_ssts());
        if ssts.is_empty() {
            return Ok(());
        }

        let mut delete_ssts = Vec::new();
        let mut validate_ssts = Vec::new();

        let region_peers = self.range_states.lock().unwrap().region_peers.clone();
        for sst in ssts {
            if let Some(region) = region_peers.get(&sst.get_region_id()) {
                let region_epoch = region.get_region_epoch();
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

    fn on_cleanup_import_sst_tick(&mut self) {
        if let Err(e) = self.on_cleanup_import_sst() {
            error!("{} failed to cleanup import sst: {:?}", self.tag, e);
        }
        self.register_cleanup_import_sst_tick();
    }

    fn register_cleanup_import_sst_tick(&self) {
        self.register_tick(
            Tick::CleanupImportSST,
            self.cfg.cleanup_import_sst_interval.0,
        )
    }

    // TODO: remove this function.
    fn redirect_msg(&mut self, region_id: u64, msg: Msg) {
        debug!("{} redirect {:?}", self.tag, msg);
        if region_id == 0 {
            return;
        }
        let res = InternalTransport::new(self.internal_mailboxes.clone()).send(region_id, msg);
        debug!("{} redirect: {:?}", self.tag, res);
    }

    fn on_init_split(
        &mut self,
        region: metapb::Region,
        peer_stat: PeerStat,
        right_derive: bool,
        is_leader: bool,
    ) {
        info!("[region {}] initializing split", region.get_id());
        let mut range_states = self.range_states.lock().unwrap();
        let region_id = region.get_id();
        if range_states.region_peers.contains_key(&region_id) {
            panic!(
                "{} [region {}] duplicated region {:?} for split region",
                self.tag, region_id, region
            );
        }
        let mut peer = match Peer::create(self, &region) {
            Ok(p) => p,
            Err(e) => panic!("[region {}] failed to create peer: {:?}", region_id, e),
        };
        peer.peer_stat = peer_stat;
        let res = range_states
            .region_ranges
            .insert(enc_end_key(&region), region.get_id());
        if let Some(id) = res {
            panic!(
                "{} [region {}] region {:?} should not exist, but got {}",
                self.tag, region_id, region, id
            );
        }
        if !right_derive {
            peer.size_diff_hint = peer.cfg.region_split_check_diff.0;
        }
        self.apply_worker
            .schedule(ApplyTask::register(&peer))
            .unwrap();

        let trans = InternalTransport::new(self.internal_mailboxes.clone());
        let (sender, mut holder) = PeerHolder::new(
            peer,
            &self,
            self.poller.clone(),
            self.range_states.clone(),
            trans,
            self.trans.clone(),
        );
        holder.has_ready |= holder.peer.maybe_campaign(is_leader);
        if is_leader {
            holder.peer.heartbeat_pd(&holder.pd_scheduler);
        }
        holder.start();
        self.poller.spawn(holder).forget();
        range_states.region_peers.insert(region_id, region);
        self.internal_mailboxes
            .lock()
            .unwrap()
            .insert(region_id, sender);
        // TODO: handle pending votes.
    }

    fn handle_msg(&mut self, msg: Msg) {
        match msg {
            Msg::RaftMessage(message) => if let Err(e) = self.on_raft_message(message) {
                error!("{} handle raft message err: {:?}", self.tag, e);
            },
            Msg::Quit => self.stop(),
            Msg::SnapshotStats => self.store_heartbeat_pd(),
            Msg::ValidateSSTResult { invalid_ssts } => self.on_validate_sst_result(invalid_ssts),
            Msg::CompactedEvent(event) => self.on_compaction_finished(event),
            Msg::InitSplit {
                region,
                peer_stat,
                right_derive,
                is_leader,
            } => self.on_init_split(region, peer_stat, right_derive, is_leader),
            // May be called by split checker.
            Msg::SplitRegion {
                region_id,
                region_epoch,
                split_key,
                callback,
            } => self.redirect_msg(
                region_id,
                Msg::SplitRegion {
                    region_id,
                    region_epoch,
                    split_key,
                    callback,
                },
            ),
            // May be called by PD.
            Msg::RaftCmd {
                send_time,
                request,
                callback,
            } => {
                let region_id = request.get_header().get_region_id();
                self.redirect_msg(
                    region_id,
                    Msg::RaftCmd {
                        send_time,
                        request,
                        callback,
                    },
                )
            }
            Msg::MergeResult {
                successful,
                region_id,
                peer,
            } => self.redirect_msg(
                region_id,
                Msg::MergeResult {
                    successful,
                    region_id,
                    peer,
                },
            ),
            Msg::ComputeHashResult {
                region_id,
                index,
                hash,
            } => self.redirect_msg(
                region_id,
                Msg::ComputeHashResult {
                    region_id,
                    index,
                    hash,
                },
            ),
            Msg::BatchRaftSnapCmds {
                send_time,
                batch,
                on_finished,
            } => {
                let region_id = batch[0].get_header().get_region_id();
                self.redirect_msg(
                    region_id,
                    Msg::BatchRaftSnapCmds {
                        send_time,
                        batch,
                        on_finished,
                    },
                );
            }
            Msg::RegionApproximateSize { region_id, size } => {
                self.redirect_msg(region_id, Msg::RegionApproximateSize { region_id, size })
            }
            Msg::HalfSplitRegion {
                region_id,
                region_epoch,
                policy,
            } => self.redirect_msg(
                region_id,
                Msg::HalfSplitRegion {
                    region_id,
                    region_epoch,
                    policy,
                },
            ),
            m => panic!("{} unexpected msg: {:?}", self.tag, m),
        }
    }

    fn handle_tick(&mut self, tick: Tick) {
        match tick {
            Tick::CleanupImportSST => self.on_cleanup_import_sst_tick(),
            Tick::CompactCheck => self.on_compact_check_tick(),
            Tick::CompactLockCf => self.on_compact_lock_cf(),
            Tick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(),
            _ => unreachable!(),
        }
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<AllMsg>) {
        for msg in msgs.drain(..) {
            match msg {
                AllMsg::Msg(msg) => self.handle_msg(msg),
                AllMsg::Tick(tick) => self.handle_tick(tick),
                _ => unreachable!(),
            }
        }
    }
}

impl<T: Transport, C: PdClient> Future for Store<T, C> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        let mut msgs;
        match self.receiver.poll() {
            Ok(Async::Ready(Some(msg))) => {
                msgs = Vec::with_capacity(1024);
                msgs.push(msg);
            }
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            _ => unreachable!(),
        }
        loop {
            for _ in msgs.len()..msgs.capacity() {
                match self.receiver.poll() {
                    Ok(Async::Ready(Some(msg))) => msgs.push(msg),
                    Ok(Async::NotReady) => break,
                    _ => unreachable!(),
                }
            }
            if !msgs.is_empty() {
                self.handle_msgs(&mut msgs);
            } else {
                return Ok(Async::NotReady);
            }
            if self.stopped {
                return Ok(Async::Ready(()));
            }
        }
    }
}

pub fn size_change_filter(info: &CompactionJobInfo) -> bool {
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

pub fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

pub fn new_verify_hash_request(
    region_id: u64,
    peer: metapb::Peer,
    state: &ConsistencyState,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::VerifyHash);
    admin.mut_verify_hash().set_index(state.index);
    admin.mut_verify_hash().set_hash(state.hash.clone());
    request.set_admin_request(admin);
    request
}

pub fn new_compute_hash_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::ComputeHash);
    request.set_admin_request(admin);
    request
}

pub fn new_compact_log_request(
    region_id: u64,
    peer: metapb::Peer,
    compact_index: u64,
    compact_term: u64,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
    request.set_admin_request(admin);
    request
}

pub fn new_compaction_listener(ch: InternalTransport) -> CompactionListener {
    let compacted_handler = box move |compacted_event: CompactedEvent| {
        if let Err(e) = ch.send(0, Msg::CompactedEvent(compacted_event)) {
            error!(
                "Send compaction finished event to raftstore failed: {:?}",
                e
            );
        }
    };
    CompactionListener::new(compacted_handler, Some(size_change_filter))
}

pub fn calc_region_declined_bytes(
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

// Consistency Check implementation.

/// Verify and store the hash to state. return true means the hash has been stored successfully.
pub fn verify_and_store_hash(
    region_id: u64,
    state: &mut ConsistencyState,
    expected_index: u64,
    expected_hash: Vec<u8>,
) -> bool {
    if expected_index < state.index {
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "miss"])
            .inc();
        warn!(
            "[region {}] has scheduled a new hash: {} > {}, skip.",
            region_id, state.index, expected_index
        );
        return false;
    }

    if state.index == expected_index {
        if state.hash.is_empty() {
            warn!(
                "[region {}] duplicated consistency check detected, skip.",
                region_id
            );
            return false;
        }
        if state.hash != expected_hash {
            panic!(
                "[region {}] hash at {} not correct, want \"{}\", got \"{}\"!!!",
                region_id,
                state.index,
                escape(&expected_hash),
                escape(&state.hash)
            );
        }
        info!(
            "[region {}] consistency check at {} pass.",
            region_id, state.index
        );
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "matched"])
            .inc();
        state.hash = vec![];
        return false;
    }

    if state.index != INVALID_INDEX && !state.hash.is_empty() {
        // Maybe computing is too slow or computed result is dropped due to channel full.
        // If computing is too slow, miss count will be increased twice.
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "miss"])
            .inc();
        warn!(
            "[region {}] hash belongs to index {}, but we want {}, skip.",
            region_id, state.index, expected_index
        );
    }

    info!(
        "[region {}] save hash of {} for consistency check later.",
        region_id, expected_index
    );
    state.index = expected_index;
    state.hash = expected_hash;
    true
}
