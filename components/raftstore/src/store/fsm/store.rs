// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::{BasicMailbox, BatchRouter, BatchSystem, Fsm, HandlerBuilder, PollHandler};
use crossbeam::channel::{TryRecvError, TrySendError};
use engine::rocks;
use engine::DB;
use engine_rocks::{
    Compat, RocksCompactionJobInfo, RocksEngine, RocksWriteBatch, RocksWriteBatchVec,
};
use engine_rocks::{PerfContext, PerfLevel};
use engine_traits::{
    CompactionJobInfo, Iterable, KvEngine, Mutable, Peekable, WriteBatch, WriteBatchExt,
    WriteBatchVecExt, WriteOptions,
};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use futures::Future;
use kvproto::import_sstpb::SstMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::StoreStats;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest};
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use protobuf::Message;
use raft::StateRole;
use std::cmp::{Ord, Ordering as CmpOrdering};
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{mem, thread, u64};
use time::{self, Timespec};

use crate::coprocessor::split_observer::SplitObserver;
use crate::coprocessor::{BoxAdminObserver, CoprocessorHost, RegionChangeEvent};
use crate::observe_perf_context_type;
use crate::report_perf_context;
use crate::store::config::Config;
use crate::store::fsm::metrics::*;
use crate::store::fsm::peer::{
    maybe_destroy_source, new_admin_request, PeerFsm, PeerFsmDelegate, SenderFsmPair,
};
#[cfg(feature = "failpoints")]
use crate::store::fsm::ApplyTaskRes;
use crate::store::fsm::{
    create_apply_batch_system, ApplyBatchSystem, ApplyPollerBuilder, ApplyRouter, ApplyTask,
    CollectedReady,
};
use crate::store::fsm::{ApplyNotifier, RegionProposal};
use crate::store::local_metrics::RaftMetrics;
use crate::store::metrics::*;
use crate::store::peer_storage::{self, HandleRaftReadyContext};
use crate::store::transport::Transport;
use crate::store::util::{is_initial_msg, PerfContextStatistics};
use crate::store::worker::{
    AutoSplitController, CleanupRunner, CleanupSSTRunner, CleanupSSTTask, CleanupTask,
    CompactRunner, CompactTask, ConsistencyCheckRunner, ConsistencyCheckTask, PdRunner,
    RaftlogGcRunner, RaftlogGcTask, ReadDelegate, RegionRunner, RegionTask, SplitCheckTask,
};
use crate::store::PdTask;
use crate::store::PeerTicks;
use crate::store::{
    util, Callback, CasualMessage, MergeResultKind, PeerMsg, RaftCommand, SignificantMsg,
    SnapManager, StoreMsg, StoreTick,
};

use crate::Result;
use engine::Engines;
use engine_rocks::{CompactedEvent, CompactionListener};
use keys::{self, data_end_key, data_key, enc_end_key, enc_start_key};
use pd_client::PdClient;
use sst_importer::SSTImporter;
use tikv_util::collections::HashMap;
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::future::poll_future_notify;
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};
use tikv_util::time::{duration_to_sec, Instant as TiInstant};
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{FutureScheduler, FutureWorker, Scheduler, Worker};
use tikv_util::{is_zero_duration, sys as sys_util, Either, RingQueue};

type Key = Vec<u8>;

const KV_WB_SHRINK_SIZE: usize = 256 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 1024 * 1024;
pub const PENDING_MSG_CAP: usize = 100;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}

pub struct StoreMeta {
    /// store id
    pub store_id: Option<u64>,
    /// region_end_key -> region_id
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    /// region_id -> region
    pub regions: HashMap<u64, Region>,
    /// region_id -> reader
    pub readers: HashMap<u64, ReadDelegate>,
    /// `MsgRequestPreVote`, `MsgRequestVote` or `MsgAppend` messages from newly split Regions shouldn't be
    /// dropped if there is no such Region in this store now. So the messages are recorded temporarily and
    /// will be handled later.
    pub pending_msgs: RingQueue<RaftMessage>,
    /// The regions with pending snapshots.
    pub pending_snapshot_regions: Vec<Region>,
    /// A marker used to indicate the peer of a Region has received a merge target message and waits to be destroyed.
    /// target_region_id -> (source_region_id -> merge_target_region)
    pub pending_merge_targets: HashMap<u64, HashMap<u64, metapb::Region>>,
    /// An inverse mapping of `pending_merge_targets` used to let source peer help target peer to clean up related entry.
    /// source_region_id -> target_region_id
    pub targets_map: HashMap<u64, u64>,
    /// `atomic_snap_regions` and `destroyed_region_for_snap` are used for making destroy overlapped regions
    /// and apply snapshot atomically.
    /// region_id -> wait_destroy_regions_map(source_region_id -> is_ready)
    /// A target peer must wait for all source peer to ready before applying snapshot.
    pub atomic_snap_regions: HashMap<u64, HashMap<u64, bool>>,
    /// source_region_id -> need_atomic
    /// Used for reminding the source peer to switch to ready in `atomic_snap_regions`.
    pub destroyed_region_for_snap: HashMap<u64, bool>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize) -> StoreMeta {
        StoreMeta {
            store_id: None,
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
            readers: HashMap::default(),
            pending_msgs: RingQueue::with_capacity(vote_capacity),
            pending_snapshot_regions: Vec::default(),
            pending_merge_targets: HashMap::default(),
            targets_map: HashMap::default(),
            atomic_snap_regions: HashMap::default(),
            destroyed_region_for_snap: HashMap::default(),
        }
    }

    #[inline]
    pub fn set_region(
        &mut self,
        host: &CoprocessorHost,
        region: Region,
        peer: &mut crate::store::Peer,
    ) {
        let prev = self.regions.insert(region.get_id(), region.clone());
        if prev.map_or(true, |r| r.get_id() != region.get_id()) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.tag);
        }
        let reader = self.readers.get_mut(&region.get_id()).unwrap();
        peer.set_region(host, reader, region);
    }
}

#[derive(Clone)]
pub struct RaftRouter<E: KvEngine> {
    pub router: BatchRouter<PeerFsm<E>, StoreFsm>,
}

impl<E: KvEngine> Deref for RaftRouter<E> {
    type Target = BatchRouter<PeerFsm<E>, StoreFsm>;

    fn deref(&self) -> &BatchRouter<PeerFsm<E>, StoreFsm> {
        &self.router
    }
}

impl<E: KvEngine> RaftRouter<E> {
    pub fn send_raft_message(
        &self,
        mut msg: RaftMessage,
    ) -> std::result::Result<(), TrySendError<RaftMessage>> {
        let id = msg.get_region_id();
        match self.try_send(id, PeerMsg::RaftMessage(msg)) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Full(m));
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Disconnected(m));
            }
            Either::Right(PeerMsg::RaftMessage(m)) => msg = m,
            _ => unreachable!(),
        }
        match self.send_control(StoreMsg::RaftMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(StoreMsg::RaftMessage(m))) => Err(TrySendError::Full(m)),
            Err(TrySendError::Disconnected(StoreMsg::RaftMessage(m))) => {
                Err(TrySendError::Disconnected(m))
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn send_raft_command(
        &self,
        cmd: RaftCommand<E>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<E>>> {
        let region_id = cmd.request.get_header().get_region_id();
        match self.send(region_id, PeerMsg::RaftCommand(cmd)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(PeerMsg::RaftCommand(cmd))) => Err(TrySendError::Full(cmd)),
            Err(TrySendError::Disconnected(PeerMsg::RaftCommand(cmd))) => {
                Err(TrySendError::Disconnected(cmd))
            }
            _ => unreachable!(),
        }
    }

    fn report_unreachable(&self, store_id: u64) {
        self.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreUnreachable { store_id })
        });
    }
}

#[derive(Default)]
pub struct PeerTickBatch {
    pub ticks: Vec<Box<dyn FnOnce() + Send>>,
    pub wait_duration: Duration,
}

impl Clone for PeerTickBatch {
    fn clone(&self) -> PeerTickBatch {
        PeerTickBatch {
            ticks: vec![],
            wait_duration: self.wait_duration,
        }
    }
}

pub struct PollContext<T, C: 'static> {
    /// The count of processed normal Fsm.
    pub processed_fsm_count: usize,
    pub cfg: Config,
    pub store: metapb::Store,
    pub pd_scheduler: FutureScheduler<PdTask>,
    pub consistency_check_scheduler: Scheduler<ConsistencyCheckTask<RocksEngine>>,
    pub split_check_scheduler: Scheduler<SplitCheckTask>,
    // handle Compact, CleanupSST task
    pub cleanup_scheduler: Scheduler<CleanupTask>,
    pub raftlog_gc_scheduler: Scheduler<RaftlogGcTask<RocksEngine>>,
    pub region_scheduler: Scheduler<RegionTask>,
    pub apply_router: ApplyRouter,
    pub router: RaftRouter<RocksEngine>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    /// region_id -> (peer_id, is_splitting)
    /// Used for handling race between splitting and creating new peer.
    /// An uninitialized peer can be replaced to the one from splitting iff they are exactly the same peer.
    ///
    /// WARNING:
    /// To avoid deadlock, if you want to use `store_meta` and `pending_create_peers` together,
    /// the lock sequence MUST BE:
    /// 1. lock the store_meta.
    /// 2. lock the pending_create_peers.
    pub pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    pub raft_metrics: RaftMetrics,
    pub snap_mgr: SnapManager,
    pub applying_snap_count: Arc<AtomicUsize>,
    pub coprocessor_host: CoprocessorHost,
    pub timer: SteadyTimer,
    pub trans: T,
    pub pd_client: Arc<C>,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub engines: Engines,
    pub kv_wb: RocksWriteBatch,
    pub raft_wb: RocksWriteBatch,
    pub pending_count: usize,
    pub sync_log: bool,
    pub has_ready: bool,
    pub ready_res: Vec<CollectedReady>,
    pub need_flush_trans: bool,
    pub current_time: Option<Timespec>,
    pub perf_context_statistics: PerfContextStatistics,
    pub tick_batch: Vec<PeerTickBatch>,
    pub node_start_time: Option<TiInstant>,
}

impl<T, C> HandleRaftReadyContext for PollContext<T, C> {
    fn wb_mut(&mut self) -> (&mut RocksWriteBatch, &mut RocksWriteBatch) {
        (&mut self.kv_wb, &mut self.raft_wb)
    }

    #[inline]
    fn kv_wb_mut(&mut self) -> &mut RocksWriteBatch {
        &mut self.kv_wb
    }

    #[inline]
    fn raft_wb_mut(&mut self) -> &mut RocksWriteBatch {
        &mut self.raft_wb
    }

    #[inline]
    fn sync_log(&self) -> bool {
        self.sync_log
    }

    #[inline]
    fn set_sync_log(&mut self, sync: bool) {
        self.sync_log = sync;
    }
}

impl<T, C> PollContext<T, C> {
    #[inline]
    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    /// Timeout is calculated from TiKV start, the node should not become
    /// hibernated if it still within the hibernate timeout, see
    /// https://github.com/tikv/tikv/issues/7747
    pub fn is_hibernate_timeout(&mut self) -> bool {
        let timeout = match self.node_start_time {
            Some(t) => t.elapsed() >= self.cfg.hibernate_timeout.0,
            None => return true,
        };
        if timeout {
            self.node_start_time = None;
        }
        timeout
    }

    pub fn update_ticks_timeout(&mut self) {
        self.tick_batch[PeerTicks::RAFT.bits() as usize].wait_duration =
            self.cfg.raft_base_tick_interval.0;
        self.tick_batch[PeerTicks::RAFT_LOG_GC.bits() as usize].wait_duration =
            self.cfg.raft_log_gc_tick_interval.0;
        self.tick_batch[PeerTicks::PD_HEARTBEAT.bits() as usize].wait_duration =
            self.cfg.pd_heartbeat_tick_interval.0;
        self.tick_batch[PeerTicks::SPLIT_REGION_CHECK.bits() as usize].wait_duration =
            self.cfg.split_region_check_tick_interval.0;
        self.tick_batch[PeerTicks::CHECK_PEER_STALE_STATE.bits() as usize].wait_duration =
            self.cfg.peer_stale_state_check_interval.0;
        self.tick_batch[PeerTicks::CHECK_MERGE.bits() as usize].wait_duration =
            self.cfg.merge_check_tick_interval.0;
    }
}

impl<T: Transport, C> PollContext<T, C> {
    #[inline]
    fn schedule_store_tick(&self, tick: StoreTick, timeout: Duration) {
        if !is_zero_duration(&timeout) {
            let mb = self.router.control_mailbox();
            let delay = self
                .timer
                .delay(timeout)
                .map(move |_| {
                    if let Err(e) = mb.force_send(StoreMsg::Tick(tick)) {
                        info!(
                            "failed to schedule store tick, are we shutting down?";
                            "tick" => ?tick,
                            "err" => ?e
                        );
                    }
                })
                .map_err(move |e| {
                    panic!("tick {:?} is lost due to timeout error: {:?}", tick, e);
                });
            poll_future_notify(delay);
        }
    }

    pub fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        cur_epoch: RegionEpoch,
        target_region: Option<metapb::Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        info!(
            "raft message is stale, tell to gc";
            "region_id" => region_id,
            "current_region_epoch" => ?cur_epoch,
            "msg_type" => ?msg_type,
        );

        self.raft_metrics.message_dropped.stale_msg += 1;

        let mut gc_msg = RaftMessage::default();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch);
        if let Some(r) = target_region {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = self.trans.send(gc_msg) {
            error!(?e;
                "send gc message failed";
                "region_id" => region_id,
            );
        }
        self.need_flush_trans = true;
    }
}

struct Store {
    // store id, before start the id is 0.
    id: u64,
    last_compact_checked_key: Key,
    stopped: bool,
    start_time: Option<Timespec>,
    consistency_check_time: HashMap<u64, Instant>,
    last_unreachable_report: HashMap<u64, Instant>,
}

pub struct StoreFsm {
    store: Store,
    receiver: Receiver<StoreMsg>,
}

impl StoreFsm {
    pub fn new(cfg: &Config) -> (LooseBoundedSender<StoreMsg>, Box<StoreFsm>) {
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(StoreFsm {
            store: Store {
                id: 0,
                last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
                stopped: false,
                start_time: None,
                consistency_check_time: HashMap::default(),
                last_unreachable_report: HashMap::default(),
            },
            receiver: rx,
        });
        (tx, fsm)
    }
}

impl Fsm for StoreFsm {
    type Message = StoreMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.store.stopped
    }
}

struct StoreFsmDelegate<'a, T: 'static, C: 'static> {
    fsm: &'a mut StoreFsm,
    ctx: &'a mut PollContext<T, C>,
}

impl<'a, T: Transport, C: PdClient> StoreFsmDelegate<'a, T, C> {
    fn on_tick(&mut self, tick: StoreTick) {
        let t = TiInstant::now_coarse();
        match tick {
            StoreTick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(),
            StoreTick::SnapGc => self.on_snap_mgr_gc(),
            StoreTick::CompactLockCf => self.on_compact_lock_cf(),
            StoreTick::CompactCheck => self.on_compact_check_tick(),
            StoreTick::ConsistencyCheck => self.on_consistency_check_tick(),
            StoreTick::CleanupImportSST => self.on_cleanup_import_sst_tick(),
        }
        let elapsed = t.elapsed();
        RAFT_EVENT_DURATION
            .with_label_values(&[tick.tag()])
            .observe(duration_to_sec(elapsed) as f64);
        slow_log!(
            elapsed,
            "[store {}] handle timeout {:?}",
            self.fsm.store.id,
            tick
        );
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<StoreMsg>) {
        for m in msgs.drain(..) {
            match m {
                StoreMsg::Tick(tick) => self.on_tick(tick),
                StoreMsg::RaftMessage(msg) => {
                    if let Err(e) = self.on_raft_message(msg) {
                        error!(?e;
                            "handle raft message failed";
                            "store_id" => self.fsm.store.id,
                        );
                    }
                }
                StoreMsg::CompactedEvent(event) => self.on_compaction_finished(event),
                StoreMsg::ValidateSSTResult { invalid_ssts } => {
                    self.on_validate_sst_result(invalid_ssts)
                }
                StoreMsg::ClearRegionSizeInRange { start_key, end_key } => {
                    self.clear_region_size_in_range(&start_key, &end_key)
                }
                StoreMsg::StoreUnreachable { store_id } => {
                    self.on_store_unreachable(store_id);
                }
                StoreMsg::Start { store } => self.start(store),
                #[cfg(any(test, feature = "testexport"))]
                StoreMsg::Validate(f) => f(&self.ctx.cfg),
            }
        }
    }

    fn start(&mut self, store: metapb::Store) {
        if self.fsm.store.start_time.is_some() {
            panic!(
                "[store {}] unable to start again with meta {:?}",
                self.fsm.store.id, store
            );
        }
        self.fsm.store.id = store.get_id();
        self.fsm.store.start_time = Some(time::get_time());
        self.register_cleanup_import_sst_tick();
        self.register_compact_check_tick();
        self.register_pd_store_heartbeat_tick();
        self.register_compact_lock_cf_tick();
        self.register_snap_mgr_gc_tick();
        self.register_consistency_check_tick();
    }
}

pub struct RaftPoller<T: 'static, C: 'static> {
    tag: String,
    store_msg_buf: Vec<StoreMsg>,
    peer_msg_buf: Vec<PeerMsg<RocksEngine>>,
    previous_metrics: RaftMetrics,
    timer: TiInstant,
    poll_ctx: PollContext<T, C>,
    pending_proposals: Vec<RegionProposal>,
    messages_per_tick: usize,
    cfg_tracker: Tracker<Config>,
}

impl<T: Transport, C: PdClient> RaftPoller<T, C> {
    fn handle_raft_ready(&mut self, peers: &mut [Box<PeerFsm<RocksEngine>>]) {
        // Only enable the fail point when the store id is equal to 3, which is
        // the id of slow store in tests.
        fail_point!("on_raft_ready", self.poll_ctx.store_id() == 3, |_| {});
        if !self.pending_proposals.is_empty() {
            for prop in self.pending_proposals.drain(..) {
                self.poll_ctx
                    .apply_router
                    .schedule_task(prop.region_id, ApplyTask::Proposal(prop));
            }
        }
        if self.poll_ctx.need_flush_trans
            && (!self.poll_ctx.kv_wb.is_empty() || !self.poll_ctx.raft_wb.is_empty())
        {
            self.poll_ctx.trans.flush();
            self.poll_ctx.need_flush_trans = false;
        }
        let ready_cnt = self.poll_ctx.ready_res.len();
        if ready_cnt != 0 && self.poll_ctx.cfg.early_apply {
            let mut ready_res = mem::take(&mut self.poll_ctx.ready_res);
            for ready in &mut ready_res {
                PeerFsmDelegate::new(&mut peers[ready.batch_offset], &mut self.poll_ctx)
                    .handle_raft_ready_apply(ready);
            }
            self.poll_ctx.ready_res = ready_res;
        }
        self.poll_ctx.raft_metrics.ready.has_ready_region += ready_cnt as u64;
        fail_point!("raft_before_save");
        if !self.poll_ctx.kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.poll_ctx
                .engines
                .kv
                .c()
                .write_opt(&self.poll_ctx.kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
            let data_size = self.poll_ctx.kv_wb.data_size();
            if data_size > KV_WB_SHRINK_SIZE {
                self.poll_ctx.kv_wb = self.poll_ctx.engines.kv.c().write_batch_with_cap(4 * 1024);
            } else {
                self.poll_ctx.kv_wb.clear();
            }
        }
        fail_point!("raft_between_save");
        if !self.poll_ctx.raft_wb.is_empty() {
            fail_point!(
                "raft_before_save_on_store_1",
                self.poll_ctx.store_id() == 1,
                |_| {}
            );
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.poll_ctx.cfg.sync_log || self.poll_ctx.sync_log);
            self.poll_ctx
                .engines
                .raft
                .c()
                .write_opt(&self.poll_ctx.raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });
            let data_size = self.poll_ctx.raft_wb.data_size();
            if data_size > RAFT_WB_SHRINK_SIZE {
                self.poll_ctx.raft_wb = self
                    .poll_ctx
                    .engines
                    .raft
                    .c()
                    .write_batch_with_cap(4 * 1024);
            } else {
                self.poll_ctx.raft_wb.clear();
            }
        }

        report_perf_context!(
            self.poll_ctx.perf_context_statistics,
            STORE_PERF_CONTEXT_TIME_HISTOGRAM
        );
        fail_point!("raft_after_save");
        if ready_cnt != 0 {
            let mut ready_res = mem::take(&mut self.poll_ctx.ready_res);
            for ready in ready_res.drain(..) {
                PeerFsmDelegate::new(&mut peers[ready.batch_offset], &mut self.poll_ctx)
                    .post_raft_ready_append(ready);
            }
        }
        let dur = self.timer.elapsed();
        if !self.poll_ctx.store_stat.is_busy {
            let election_timeout = Duration::from_millis(
                self.poll_ctx.cfg.raft_base_tick_interval.as_millis()
                    * self.poll_ctx.cfg.raft_election_timeout_ticks as u64,
            );
            if dur >= election_timeout {
                self.poll_ctx.store_stat.is_busy = true;
            }
        }

        self.poll_ctx
            .raft_metrics
            .append_log
            .observe(duration_to_sec(dur) as f64);

        slow_log!(
            dur,
            "{} handle {} pending peers include {} ready, {} entries, {} messages and {} \
             snapshots",
            self.tag,
            self.poll_ctx.pending_count,
            ready_cnt,
            self.poll_ctx.raft_metrics.ready.append - self.previous_metrics.ready.append,
            self.poll_ctx.raft_metrics.ready.message - self.previous_metrics.ready.message,
            self.poll_ctx.raft_metrics.ready.snapshot - self.previous_metrics.ready.snapshot
        );
    }

    fn flush_ticks(&mut self) {
        for t in PeerTicks::get_all_ticks() {
            let idx = t.bits() as usize;
            if self.poll_ctx.tick_batch[idx].ticks.is_empty() {
                continue;
            }
            let peer_ticks = std::mem::replace(&mut self.poll_ctx.tick_batch[idx].ticks, vec![]);
            let f = self
                .poll_ctx
                .timer
                .delay(self.poll_ctx.tick_batch[idx].wait_duration)
                .map(move |_| {
                    for tick in peer_ticks {
                        tick();
                    }
                })
                .map_err(move |e| {
                    panic!("tick is lost due to timeout error: {:?}", e);
                });
            poll_future_notify(f);
        }
    }
}

impl<T: Transport, C: PdClient> PollHandler<PeerFsm<RocksEngine>, StoreFsm> for RaftPoller<T, C> {
    fn begin(&mut self, batch_size: usize) {
        self.previous_metrics = self.poll_ctx.raft_metrics.clone();
        self.poll_ctx.processed_fsm_count = 0;
        self.poll_ctx.pending_count = 0;
        self.poll_ctx.sync_log = false;
        self.poll_ctx.has_ready = false;
        if self.pending_proposals.capacity() == 0 {
            self.pending_proposals = Vec::with_capacity(batch_size);
        }
        self.timer = TiInstant::now_coarse();
        // update config
        self.poll_ctx.perf_context_statistics.start();
        if let Some(incoming) = self.cfg_tracker.any_new() {
            match Ord::cmp(
                &incoming.messages_per_tick,
                &self.poll_ctx.cfg.messages_per_tick,
            ) {
                CmpOrdering::Greater => {
                    self.store_msg_buf.reserve(incoming.messages_per_tick);
                    self.peer_msg_buf.reserve(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                CmpOrdering::Less => {
                    self.store_msg_buf.shrink_to(incoming.messages_per_tick);
                    self.peer_msg_buf.shrink_to(incoming.messages_per_tick);
                    self.messages_per_tick = incoming.messages_per_tick;
                }
                _ => {}
            }
            self.poll_ctx.cfg = incoming.clone();
            self.poll_ctx.update_ticks_timeout();
        }
    }

    fn handle_control(&mut self, store: &mut StoreFsm) -> Option<usize> {
        let mut expected_msg_count = None;
        while self.store_msg_buf.len() < self.messages_per_tick {
            match store.receiver.try_recv() {
                Ok(msg) => self.store_msg_buf.push(msg),
                Err(TryRecvError::Empty) => {
                    expected_msg_count = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    store.store.stopped = true;
                    expected_msg_count = Some(0);
                    break;
                }
            }
        }
        let mut delegate = StoreFsmDelegate {
            fsm: store,
            ctx: &mut self.poll_ctx,
        };
        delegate.handle_msgs(&mut self.store_msg_buf);
        expected_msg_count
    }

    fn handle_normal(&mut self, peer: &mut PeerFsm<RocksEngine>) -> Option<usize> {
        let mut expected_msg_count = None;

        fail_point!(
            "pause_on_peer_collect_message",
            peer.peer_id() == 1,
            |_| unreachable!()
        );

        fail_point!(
            "on_peer_collect_message_2",
            peer.peer_id() == 2,
            |_| unreachable!()
        );

        while self.peer_msg_buf.len() < self.messages_per_tick {
            match peer.receiver.try_recv() {
                // TODO: we may need a way to optimize the message copy.
                Ok(msg) => {
                    fail_point!(
                        "pause_on_peer_destroy_res",
                        peer.peer_id() == 1
                            && match msg {
                                PeerMsg::ApplyRes {
                                    res: ApplyTaskRes::Destroy { .. },
                                } => true,
                                _ => false,
                            },
                        |_| unreachable!()
                    );
                    self.peer_msg_buf.push(msg)
                }
                Err(TryRecvError::Empty) => {
                    expected_msg_count = Some(0);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    peer.stop();
                    expected_msg_count = Some(0);
                    break;
                }
            }
        }
        let mut delegate = PeerFsmDelegate::new(peer, &mut self.poll_ctx);
        delegate.handle_msgs(&mut self.peer_msg_buf);
        delegate.collect_ready(&mut self.pending_proposals);
        self.poll_ctx.processed_fsm_count += 1;
        expected_msg_count
    }

    fn end(&mut self, peers: &mut [Box<PeerFsm<RocksEngine>>]) {
        self.flush_ticks();
        if self.poll_ctx.has_ready {
            self.handle_raft_ready(peers);
        }
        self.poll_ctx.current_time = None;
        self.poll_ctx
            .raft_metrics
            .process_ready
            .observe(duration_to_sec(self.timer.elapsed()) as f64);
        self.poll_ctx.raft_metrics.flush();
        self.poll_ctx.store_stat.flush();
    }

    fn pause(&mut self) {
        if self.poll_ctx.need_flush_trans {
            self.poll_ctx.trans.flush();
            self.poll_ctx.need_flush_trans = false;
        }
    }
}

pub struct RaftPollerBuilder<T, C> {
    pub cfg: Arc<VersionTrack<Config>>,
    pub store: metapb::Store,
    pd_scheduler: FutureScheduler<PdTask>,
    consistency_check_scheduler: Scheduler<ConsistencyCheckTask<RocksEngine>>,
    split_check_scheduler: Scheduler<SplitCheckTask>,
    cleanup_scheduler: Scheduler<CleanupTask>,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask<RocksEngine>>,
    pub region_scheduler: Scheduler<RegionTask>,
    apply_router: ApplyRouter,
    pub router: RaftRouter<RocksEngine>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    pub pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    snap_mgr: SnapManager,
    pub coprocessor_host: CoprocessorHost,
    trans: T,
    pd_client: Arc<C>,
    global_stat: GlobalStoreStat,
    pub engines: Engines,
    applying_snap_count: Arc<AtomicUsize>,
}

impl<T, C> RaftPollerBuilder<T, C> {
    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<Vec<SenderFsmPair<RocksEngine>>> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let kv_engine = Arc::clone(&self.engines.kv);
        let store_id = self.store.get_id();
        let mut total_count = 0;
        let mut tombstone_count = 0;
        let mut applying_count = 0;
        let mut region_peers = vec![];

        let t = Instant::now();
        let mut kv_wb = self.engines.kv.c().write_batch();
        let mut raft_wb = self.engines.raft.c().write_batch();
        let mut applying_regions = vec![];
        let mut merging_count = 0;
        let mut meta = self.store_meta.lock().unwrap();
        kv_engine
            .c()
            .scan_cf(CF_RAFT, start_key, end_key, false, |key, value| {
                let (region_id, suffix) = box_try!(keys::decode_region_meta_key(key));
                if suffix != keys::REGION_STATE_SUFFIX {
                    return Ok(true);
                }

                total_count += 1;

                let mut local_state = RegionLocalState::default();
                local_state.merge_from_bytes(value)?;

                let region = local_state.get_region();
                if local_state.get_state() == PeerState::Tombstone {
                    tombstone_count += 1;
                    debug!("region is tombstone"; "region" => ?region, "store_id" => store_id);
                    self.clear_stale_meta(&mut kv_wb, &mut raft_wb, &local_state);
                    return Ok(true);
                }
                if local_state.get_state() == PeerState::Applying {
                    // in case of restart happen when we just write region state to Applying,
                    // but not write raft_local_state to raft rocksdb in time.
                    box_try!(peer_storage::recover_from_applying_state(
                        &self.engines,
                        &mut raft_wb,
                        region_id
                    ));
                    applying_count += 1;
                    applying_regions.push(region.clone());
                    return Ok(true);
                }

                let (tx, mut peer) = box_try!(PeerFsm::create(
                    store_id,
                    &self.cfg.value(),
                    self.region_scheduler.clone(),
                    self.engines.clone(),
                    region,
                ));
                if local_state.get_state() == PeerState::Merging {
                    info!("region is merging"; "region" => ?region, "store_id" => store_id);
                    merging_count += 1;
                    peer.set_pending_merge_state(local_state.get_merge_state().to_owned());
                }
                meta.region_ranges.insert(enc_end_key(region), region_id);
                meta.regions.insert(region_id, region.clone());
                // No need to check duplicated here, because we use region id as the key
                // in DB.
                region_peers.push((tx, peer));
                self.coprocessor_host.on_region_changed(
                    region,
                    RegionChangeEvent::Create,
                    StateRole::Follower,
                );
                Ok(true)
            })?;

        if !kv_wb.is_empty() {
            self.engines.kv.c().write(&kv_wb).unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.engines.raft.c().write(&raft_wb).unwrap();
            self.engines.raft.sync_wal().unwrap();
        }

        // schedule applying snapshot after raft writebatch were written.
        for region in applying_regions {
            info!("region is applying snapshot"; "region" => ?region, "store_id" => store_id);
            let (tx, mut peer) = PeerFsm::create(
                store_id,
                &self.cfg.value(),
                self.region_scheduler.clone(),
                self.engines.clone(),
                &region,
            )?;
            peer.schedule_applying_snapshot();
            meta.region_ranges
                .insert(enc_end_key(&region), region.get_id());
            meta.regions.insert(region.get_id(), region);
            region_peers.push((tx, peer));
        }

        info!(
            "start store";
            "store_id" => store_id,
            "region_count" => total_count,
            "tombstone_count" => tombstone_count,
            "applying_count" =>  applying_count,
            "merge_count" => merging_count,
            "takes" => ?t.elapsed(),
        );

        self.clear_stale_data(&meta)?;

        Ok(region_peers)
    }

    fn clear_stale_meta(
        &self,
        kv_wb: &mut RocksWriteBatch,
        raft_wb: &mut RocksWriteBatch,
        origin_state: &RegionLocalState,
    ) {
        let region = origin_state.get_region();
        let raft_key = keys::raft_state_key(region.get_id());
        let raft_state = match self.engines.raft.c().get_msg(&raft_key).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };

        peer_storage::clear_meta(&self.engines, kv_wb, raft_wb, region.get_id(), &raft_state)
            .unwrap();
        let key = keys::region_state_key(region.get_id());
        kv_wb.put_msg_cf(CF_RAFT, &key, origin_state).unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&self, meta: &StoreMeta) -> Result<()> {
        let t = Instant::now();

        let mut ranges = Vec::new();
        let mut last_start_key = keys::data_key(b"");
        for region_id in meta.region_ranges.values() {
            let region = &meta.regions[region_id];
            let start_key = keys::enc_start_key(region);
            ranges.push((last_start_key, start_key));
            last_start_key = keys::enc_end_key(region);
        }
        ranges.push((last_start_key, keys::DATA_MAX_KEY.to_vec()));

        rocks::util::roughly_cleanup_ranges(&self.engines.kv, &ranges)?;

        info!(
            "cleans up garbage data";
            "store_id" => self.store.get_id(),
            "garbage_range_count" => ranges.len(),
            "takes" => ?t.elapsed()
        );

        Ok(())
    }
}

impl<T, C> HandlerBuilder<PeerFsm<RocksEngine>, StoreFsm> for RaftPollerBuilder<T, C>
where
    T: Transport + 'static,
    C: PdClient + 'static,
{
    type Handler = RaftPoller<T, C>;

    fn build(&mut self) -> RaftPoller<T, C> {
        let mut ctx = PollContext {
            processed_fsm_count: 0,
            cfg: self.cfg.value().clone(),
            store: self.store.clone(),
            pd_scheduler: self.pd_scheduler.clone(),
            consistency_check_scheduler: self.consistency_check_scheduler.clone(),
            split_check_scheduler: self.split_check_scheduler.clone(),
            region_scheduler: self.region_scheduler.clone(),
            apply_router: self.apply_router.clone(),
            router: self.router.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            raftlog_gc_scheduler: self.raftlog_gc_scheduler.clone(),
            importer: self.importer.clone(),
            store_meta: self.store_meta.clone(),
            pending_create_peers: self.pending_create_peers.clone(),
            raft_metrics: RaftMetrics::default(),
            snap_mgr: self.snap_mgr.clone(),
            applying_snap_count: self.applying_snap_count.clone(),
            coprocessor_host: self.coprocessor_host.clone(),
            timer: SteadyTimer::default(),
            trans: self.trans.clone(),
            pd_client: self.pd_client.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            engines: self.engines.clone(),
            kv_wb: self.engines.kv.c().write_batch(),
            raft_wb: self.engines.raft.c().write_batch_with_cap(4 * 1024),
            pending_count: 0,
            sync_log: false,
            has_ready: false,
            ready_res: Vec::new(),
            need_flush_trans: false,
            current_time: None,
            perf_context_statistics: PerfContextStatistics::new(self.cfg.value().perf_level),
            tick_batch: vec![PeerTickBatch::default(); 256],
            node_start_time: Some(TiInstant::now_coarse()),
        };
        ctx.update_ticks_timeout();
        let tag = format!("[store {}]", ctx.store.get_id());
        RaftPoller {
            tag: tag.clone(),
            store_msg_buf: Vec::with_capacity(ctx.cfg.messages_per_tick),
            peer_msg_buf: Vec::with_capacity(ctx.cfg.messages_per_tick),
            previous_metrics: ctx.raft_metrics.clone(),
            timer: TiInstant::now_coarse(),
            messages_per_tick: ctx.cfg.messages_per_tick,
            poll_ctx: ctx,
            pending_proposals: Vec::new(),
            cfg_tracker: self.cfg.clone().tracker(tag),
        }
    }
}

struct Workers {
    pd_worker: FutureWorker<PdTask>,
    consistency_check_worker: Worker<ConsistencyCheckTask<RocksEngine>>,
    split_check_worker: Worker<SplitCheckTask>,
    // handle Compact, CleanupSST task
    cleanup_worker: Worker<CleanupTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask<RocksEngine>>,
    region_worker: Worker<RegionTask>,
    coprocessor_host: CoprocessorHost,
}

pub struct RaftBatchSystem {
    system: BatchSystem<PeerFsm<RocksEngine>, StoreFsm>,
    apply_router: ApplyRouter,
    apply_system: ApplyBatchSystem,
    router: RaftRouter<RocksEngine>,
    workers: Option<Workers>,
}

impl RaftBatchSystem {
    pub fn router(&self) -> RaftRouter<RocksEngine> {
        self.router.clone()
    }

    pub fn apply_router(&self) -> ApplyRouter {
        self.apply_router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn<T: Transport + 'static, C: PdClient + 'static>(
        &mut self,
        meta: metapb::Store,
        cfg: Arc<VersionTrack<Config>>,
        engines: Engines,
        trans: T,
        pd_client: Arc<C>,
        mgr: SnapManager,
        pd_worker: FutureWorker<PdTask>,
        store_meta: Arc<Mutex<StoreMeta>>,
        mut coprocessor_host: CoprocessorHost,
        importer: Arc<SSTImporter>,
        split_check_worker: Worker<SplitCheckTask>,
        auto_split_controller: AutoSplitController,
    ) -> Result<()> {
        assert!(self.workers.is_none());
        // TODO: we can get cluster meta regularly too later.

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, BoxAdminObserver::new(SplitObserver));

        let workers = Workers {
            split_check_worker,
            region_worker: Worker::new("snapshot-worker"),
            pd_worker,
            consistency_check_worker: Worker::new("consistency-check"),
            cleanup_worker: Worker::new("cleanup-worker"),
            raftlog_gc_worker: Worker::new("raft-gc-worker"),
            coprocessor_host,
        };
        let mut builder = RaftPollerBuilder {
            cfg,
            store: meta,
            engines,
            router: self.router.clone(),
            split_check_scheduler: workers.split_check_worker.scheduler(),
            region_scheduler: workers.region_worker.scheduler(),
            pd_scheduler: workers.pd_worker.scheduler(),
            consistency_check_scheduler: workers.consistency_check_worker.scheduler(),
            cleanup_scheduler: workers.cleanup_worker.scheduler(),
            raftlog_gc_scheduler: workers.raftlog_gc_worker.scheduler(),
            apply_router: self.apply_router.clone(),
            trans,
            pd_client,
            coprocessor_host: workers.coprocessor_host.clone(),
            importer,
            snap_mgr: mgr,
            global_stat: GlobalStoreStat::default(),
            store_meta,
            pending_create_peers: Arc::new(Mutex::new(HashMap::default())),
            applying_snap_count: Arc::new(AtomicUsize::new(0)),
        };
        let region_peers = builder.init()?;
        let engine = RocksEngine::from_db(builder.engines.kv.clone());
        if engine.support_write_batch_vec() {
            self.start_system::<T, C, RocksWriteBatchVec>(
                workers,
                region_peers,
                builder,
                auto_split_controller,
            )?;
        } else {
            self.start_system::<T, C, RocksWriteBatch>(
                workers,
                region_peers,
                builder,
                auto_split_controller,
            )?;
        }
        Ok(())
    }

    fn start_system<
        T: Transport + 'static,
        C: PdClient + 'static,
        W: WriteBatch + WriteBatchVecExt<RocksEngine> + 'static,
    >(
        &mut self,
        mut workers: Workers,
        region_peers: Vec<SenderFsmPair<RocksEngine>>,
        builder: RaftPollerBuilder<T, C>,
        auto_split_controller: AutoSplitController,
    ) -> Result<()> {
        builder.snap_mgr.init()?;

        let engines = builder.engines.clone();
        let snap_mgr = builder.snap_mgr.clone();
        let cfg = builder.cfg.value().clone();
        let store = builder.store.clone();
        let pd_client = builder.pd_client.clone();
        let importer = builder.importer.clone();

        let apply_poller_builder = ApplyPollerBuilder::<W>::new(
            &builder,
            ApplyNotifier::Router(self.router.clone()),
            self.apply_router.clone(),
        );
        self.apply_system
            .schedule_all(region_peers.iter().map(|pair| pair.1.get_peer()));

        {
            let mut meta = builder.store_meta.lock().unwrap();
            for (_, peer_fsm) in &region_peers {
                let peer = peer_fsm.get_peer();
                meta.readers
                    .insert(peer_fsm.region_id(), ReadDelegate::from_peer(peer));
            }
        }

        let router = Mutex::new(self.router.clone());
        pd_client.handle_reconnect(move || {
            router
                .lock()
                .unwrap()
                .broadcast_normal(|| PeerMsg::HeartbeatPd);
        });

        let tag = format!("raftstore-{}", store.get_id());
        self.system.spawn(tag, builder);
        let mut mailboxes = Vec::with_capacity(region_peers.len());
        let mut address = Vec::with_capacity(region_peers.len());
        for (tx, fsm) in region_peers {
            address.push(fsm.region_id());
            mailboxes.push((fsm.region_id(), BasicMailbox::new(tx, fsm)));
        }
        self.router.register_all(mailboxes);

        // Make sure Msg::Start is the first message each FSM received.
        for addr in address {
            self.router.force_send(addr, PeerMsg::Start).unwrap();
        }
        self.router
            .send_control(StoreMsg::Start {
                store: store.clone(),
            })
            .unwrap();

        self.apply_system
            .spawn("apply".to_owned(), apply_poller_builder);

        let region_runner = RegionRunner::new(
            engines.kv.c().clone(),
            snap_mgr.clone(),
            cfg.snap_apply_batch_size.0 as usize,
            cfg.use_delete_range,
            workers.coprocessor_host.clone(),
            self.router(),
        );
        let timer = region_runner.new_timer();
        box_try!(workers.region_worker.start_with_timer(region_runner, timer));

        let raftlog_gc_runner = RaftlogGcRunner::new(None);
        box_try!(workers.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(Arc::clone(&engines.kv));
        let cleanup_sst_runner = CleanupSSTRunner::new(
            store.get_id(),
            self.router.clone(),
            Arc::clone(&importer),
            Arc::clone(&pd_client),
        );
        let cleanup_runner = CleanupRunner::new(compact_runner, cleanup_sst_runner);
        box_try!(workers.cleanup_worker.start(cleanup_runner));

        let pd_runner = PdRunner::new(
            store.get_id(),
            Arc::clone(&pd_client),
            self.router.clone(),
            workers.pd_worker.scheduler(),
            cfg.pd_store_heartbeat_tick_interval.as_secs(),
            auto_split_controller,
            snap_mgr,
        );
        box_try!(workers.pd_worker.start(pd_runner));

        let consistency_check_runner = ConsistencyCheckRunner::new(self.router.clone());
        box_try!(workers
            .consistency_check_worker
            .start(consistency_check_runner));

        if let Err(e) = sys_util::thread::set_priority(sys_util::HIGH_PRI) {
            warn!("set thread priority for raftstore failed"; "error" => ?e);
        }
        self.workers = Some(workers);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        let mut workers = self.workers.take().unwrap();
        // Wait all workers finish.
        let mut handles: Vec<Option<thread::JoinHandle<()>>> = vec![];
        handles.push(workers.split_check_worker.stop());
        handles.push(workers.region_worker.stop());
        handles.push(workers.pd_worker.stop());
        handles.push(workers.consistency_check_worker.stop());
        handles.push(workers.cleanup_worker.stop());
        handles.push(workers.raftlog_gc_worker.stop());
        self.apply_system.shutdown();
        self.system.shutdown();
        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }
        workers.coprocessor_host.shutdown();
    }
}

pub fn create_raft_batch_system(cfg: &Config) -> (RaftRouter<RocksEngine>, RaftBatchSystem) {
    let (store_tx, store_fsm) = StoreFsm::new(cfg);
    let (apply_router, apply_system) = create_apply_batch_system(&cfg);
    let (router, system) =
        batch_system::create_system(&cfg.store_batch_system, store_tx, store_fsm);
    let raft_router = RaftRouter { router };
    let system = RaftBatchSystem {
        system,
        workers: None,
        apply_router,
        apply_system,
        router: raft_router.clone(),
    };
    (raft_router, system)
}

#[derive(Debug, PartialEq)]
enum CheckMsgStatus {
    // The message is the first message to an existing peer.
    FirstRequest,
    // The message can be dropped silently
    DropMsg,
    // Try to create the peer
    NewPeer,
    // Try to create the peer which is the first one of this region on local store.
    NewPeerFirst,
}

impl<'a, T: Transport, C: PdClient> StoreFsmDelegate<'a, T, C> {
    /// Checks if the message is targeting a stale peer.
    fn check_msg(&mut self, msg: &RaftMessage) -> Result<CheckMsgStatus> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let from_store_id = msg.get_from_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();

        // Check if the target peer is tombstone.
        let state_key = keys::region_state_key(region_id);
        let local_state: RegionLocalState =
            match self.ctx.engines.kv.c().get_msg_cf(CF_RAFT, &state_key)? {
                Some(state) => state,
                None => return Ok(CheckMsgStatus::NewPeerFirst),
            };

        if local_state.get_state() != PeerState::Tombstone {
            // Maybe split, but not registered yet.
            if !util::is_first_message(msg.get_message()) {
                self.ctx.raft_metrics.message_dropped.region_nonexistent += 1;
                return Err(box_err!(
                    "[region {}] region not exist but not tombstone: {:?}",
                    region_id,
                    local_state
                ));
            }
            info!(
                "region doesn't exist yet, wait for it to be split";
                "region_id" => region_id
            );
            return Ok(CheckMsgStatus::FirstRequest);
        }
        debug!(
            "region is in tombstone state";
            "region_id" => region_id,
            "region_local_state" => ?local_state,
        );
        let region = local_state.get_region();
        let region_epoch = region.get_region_epoch();
        if local_state.has_merge_state() {
            info!(
                "merged peer receives a stale message";
                "region_id" => region_id,
                "current_region_epoch" => ?region_epoch,
                "msg_type" => ?msg_type,
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
            self.ctx
                .handle_stale_msg(msg, region_epoch.clone(), merge_target);
            return Ok(CheckMsgStatus::DropMsg);
        }
        // The region in this peer is already destroyed
        if util::is_epoch_stale(from_epoch, region_epoch) {
            self.ctx.raft_metrics.message_dropped.region_tombstone_peer += 1;
            info!(
                "tombstone peer receives a stale message";
                "region_id" => region_id,
                "from_region_epoch" => ?from_epoch,
                "current_region_epoch" => ?region_epoch,
                "msg_type" => ?msg_type,
            );
            if util::find_peer(region, from_store_id).is_none() {
                self.ctx.handle_stale_msg(msg, region_epoch.clone(), None);
            } else {
                let mut need_gc_msg = util::is_vote_msg(msg.get_message());
                if msg.has_extra_msg() {
                    // A learner can't vote so it sends the check-stale-peer msg to others to find out whether
                    // it is removed due to conf change or merge.
                    need_gc_msg |=
                        msg.get_extra_msg().get_type() == ExtraMessageType::MsgCheckStalePeer;
                    // For backward compatibility
                    need_gc_msg |=
                        msg.get_extra_msg().get_type() == ExtraMessageType::MsgRegionWakeUp;
                }
                if need_gc_msg {
                    let mut send_msg = RaftMessage::default();
                    send_msg.set_region_id(region_id);
                    send_msg.set_from_peer(msg.get_to_peer().clone());
                    send_msg.set_to_peer(msg.get_from_peer().clone());
                    send_msg.set_region_epoch(region_epoch.clone());
                    let extra_msg = send_msg.mut_extra_msg();
                    extra_msg.set_type(ExtraMessageType::MsgCheckStalePeerResponse);
                    extra_msg.set_check_peers(region.get_peers().into());
                    if let Err(e) = self.ctx.trans.send(send_msg) {
                        error!(?e;
                            "send check stale peer response message failed";
                            "region_id" => region_id,
                        );
                    }
                }
                self.ctx.need_flush_trans = true;
            }

            return Ok(CheckMsgStatus::DropMsg);
        }
        // A tombstone peer may not apply the conf change log which removes itself.
        // In this case, the local epoch is stale and the local peer can be found from region.
        // We can compare the local peer id with to_peer_id to verify whether it is correct to create a new peer.
        if let Some(local_peer_id) =
            util::find_peer(region, self.ctx.store_id()).map(|r| r.get_id())
        {
            if to_peer_id <= local_peer_id {
                self.ctx.raft_metrics.message_dropped.region_tombstone_peer += 1;
                info!(
                    "tombstone peer receives a stale message, local_peer_id >= to_peer_id in msg";
                    "region_id" => region_id,
                    "local_peer_id" => local_peer_id,
                    "to_peer_id" => to_peer_id,
                    "msg_type" => ?msg_type
                );
                return Ok(CheckMsgStatus::DropMsg);
            }
        }
        Ok(CheckMsgStatus::NewPeer)
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        match self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg)) {
            Ok(()) | Err(TrySendError::Full(_)) => return Ok(()),
            Err(TrySendError::Disconnected(_)) if self.ctx.router.is_shutdown() => return Ok(()),
            Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m))) => msg = m,
            e => panic!(
                "[store {}] [region {}] unexpected redirect error: {:?}",
                self.fsm.store.id, region_id, e
            ),
        }

        debug!(
            "handle raft message";
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
            "store_id" => self.fsm.store.id,
            "region_id" => region_id,
            "msg_type" => ?msg.get_message().get_msg_type(),
        );

        if msg.get_to_peer().get_store_id() != self.ctx.store_id() {
            warn!(
                "store not match, ignore it";
                "store_id" => self.ctx.store_id(),
                "to_store_id" => msg.get_to_peer().get_store_id(),
                "region_id" => region_id,
            );
            self.ctx.raft_metrics.message_dropped.mismatch_store_id += 1;
            return Ok(());
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "region_id" => region_id,
            );
            self.ctx.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return Ok(());
        }
        if msg.get_is_tombstone() || msg.has_merge_target() {
            // Target tombstone peer doesn't exist, so ignore it.
            return Ok(());
        }
        let check_msg_status = self.check_msg(&msg)?;
        let is_first_request = match check_msg_status {
            CheckMsgStatus::DropMsg => return Ok(()),
            CheckMsgStatus::FirstRequest => true,
            CheckMsgStatus::NewPeer | CheckMsgStatus::NewPeerFirst => {
                if self.maybe_create_peer(
                    region_id,
                    &msg,
                    check_msg_status == CheckMsgStatus::NewPeerFirst,
                )? {
                    // Peer created, send the message again
                    let _ = self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg));
                    return Ok(());
                }
                // Can't create peer, see if we should keep this message
                util::is_first_message(msg.get_message())
            }
        };
        if is_first_request {
            // To void losing messages, either put it to pending_msg or force send.
            let mut store_meta = self.ctx.store_meta.lock().unwrap();
            if !store_meta.regions.contains_key(&region_id) {
                // Save one pending message for a peer is enough, remove
                // the previous pending message of this peer
                store_meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == msg.get_to_peer());

                store_meta.pending_msgs.push(msg);
            } else {
                drop(store_meta);
                if let Err(e) = self
                    .ctx
                    .router
                    .force_send(region_id, PeerMsg::RaftMessage(msg))
                {
                    warn!("handle first request failed"; "region_id" => region_id, "error" => ?e);
                }
            }
        }
        Ok(())
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(
        &mut self,
        region_id: u64,
        msg: &RaftMessage,
        is_local_first: bool,
    ) -> Result<bool> {
        if !is_initial_msg(msg.get_message()) {
            let msg_type = msg.get_message().get_msg_type();
            debug!(
                "target peer doesn't exist, stale message";
                "target_peer" => ?msg.get_to_peer(),
                "region_id" => region_id,
                "msg_type" => ?msg_type,
            );
            self.ctx.raft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        if is_local_first {
            let mut pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
            if pending_create_peers.contains_key(&region_id) {
                return Ok(false);
            }
            pending_create_peers.insert(region_id, (msg.get_to_peer().get_id(), false));
        }

        let res = self.maybe_create_peer_internal(region_id, &msg, is_local_first);
        // If failed, i.e. Err or Ok(false), remove this peer data from `pending_create_peers`.
        if res.as_ref().map_or(true, |b| !*b) && is_local_first {
            let mut pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
            if let Some(status) = pending_create_peers.get(&region_id) {
                if *status == (msg.get_to_peer().get_id(), false) {
                    pending_create_peers.remove(&region_id);
                }
            }
        }
        res
    }

    fn maybe_create_peer_internal(
        &mut self,
        region_id: u64,
        msg: &RaftMessage,
        is_local_first: bool,
    ) -> Result<bool> {
        if is_local_first
            && self
                .ctx
                .engines
                .kv
                .c()
                .get_value_cf(CF_RAFT, &keys::region_state_key(region_id))?
                .is_some()
        {
            return Ok(false);
        }

        let target = msg.get_to_peer();

        let mut meta = self.ctx.store_meta.lock().unwrap();
        if meta.regions.contains_key(&region_id) {
            return Ok(true);
        }

        if is_local_first {
            let pending_create_peers = self.ctx.pending_create_peers.lock().unwrap();
            match pending_create_peers.get(&region_id) {
                Some(status) if *status == (msg.get_to_peer().get_id(), false) => (),
                // If changed, it means this peer has been/will be replaced from the new one from splitting.
                _ => return Ok(false),
            }
            // Note that `StoreMeta` lock is held and status is (peer_id, false) in `pending_create_peers` now.
            // If this peer is created from splitting latter and then status in `pending_create_peers` is changed,
            // that peer creation in `on_ready_split_region` must be executed **after** current peer creation
            // because of the `StoreMeta` lock.
        }

        let mut is_overlapped = false;
        let mut regions_to_destroy = vec![];
        for (_, id) in meta.region_ranges.range((
            Excluded(data_key(msg.get_start_key())),
            Unbounded::<Vec<u8>>,
        )) {
            let exist_region = &meta.regions[&id];
            if enc_start_key(exist_region) >= data_end_key(msg.get_end_key()) {
                break;
            }

            debug!(
                "msg is overlapped with exist region";
                "region_id" => region_id,
                "msg" => ?msg,
                "exist_region" => ?exist_region,
            );
            let (can_destroy, merge_to_this_peer) = maybe_destroy_source(
                &meta,
                region_id,
                target.get_id(),
                exist_region.get_id(),
                msg.get_region_epoch().to_owned(),
            );
            if can_destroy {
                if !merge_to_this_peer {
                    regions_to_destroy.push(exist_region.get_id());
                } else {
                    error!(
                        "A new peer has a merge source peer";
                        "region_id" => region_id,
                        "peer_id" => target.get_id(),
                        "source_region" => ?exist_region,
                    );
                    if self.ctx.cfg.dev_assert {
                        panic!("something is wrong, maybe PD do not ensure all target peers exist before merging");
                    }
                }
                continue;
            }
            is_overlapped = true;
            if msg.get_region_epoch().get_version() > exist_region.get_region_epoch().get_version()
            {
                // If new region's epoch version is greater than exist region's, the exist region
                // may has been merged/splitted already.
                let _ = self.ctx.router.force_send(
                    exist_region.get_id(),
                    PeerMsg::CasualMessage(CasualMessage::RegionOverlapped),
                );
            }
        }

        if is_overlapped {
            self.ctx.raft_metrics.message_dropped.region_overlap += 1;
            return Ok(false);
        }

        for id in regions_to_destroy {
            self.ctx
                .router
                .force_send(
                    id,
                    PeerMsg::SignificantMsg(SignificantMsg::MergeResult {
                        target_region_id: region_id,
                        target: target.clone(),
                        result: MergeResultKind::Stale,
                    }),
                )
                .unwrap();
        }

        // New created peers should know it's learner or not.
        let (tx, mut peer) = PeerFsm::replicate(
            self.ctx.store_id(),
            &self.ctx.cfg,
            self.ctx.region_scheduler.clone(),
            self.ctx.engines.clone(),
            region_id,
            target.clone(),
        )?;

        // WARNING: The checking code must be above this line.
        // Now all checking passed

        peer.peer.local_first_replicate = is_local_first;

        // Following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        meta.regions
            .insert(region_id, peer.get_peer().region().to_owned());

        let mailbox = BasicMailbox::new(tx, peer);
        self.ctx.router.register(region_id, mailbox);
        self.ctx
            .router
            .force_send(region_id, PeerMsg::Start)
            .unwrap();
        Ok(true)
    }

    fn on_compaction_finished(&mut self, event: CompactedEvent) {
        // If size declining is trivial, skip.
        let total_bytes_declined = if event.total_input_bytes > event.total_output_bytes {
            event.total_input_bytes - event.total_output_bytes
        } else {
            0
        };
        if total_bytes_declined < self.ctx.cfg.region_split_check_diff.0
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
                self.ctx.cfg.region_split_check_diff.0 / 16,
            )
        };

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(region_declined_bytes.len() as f64);

        for (region_id, declined_bytes) in region_declined_bytes.drain(..) {
            let _ = self.ctx.router.send(
                region_id,
                PeerMsg::CasualMessage(CasualMessage::CompactionDeclinedBytes {
                    bytes: declined_bytes,
                }),
            );
        }
    }

    fn register_compact_check_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::CompactCheck,
            self.ctx.cfg.region_compact_check_interval.0,
        )
    }

    fn on_compact_check_tick(&mut self) {
        self.register_compact_check_tick();
        if self.ctx.cleanup_scheduler.is_busy() {
            debug!(
                "compact worker is busy, check space redundancy next time";
                "store_id" => self.fsm.store.id,
            );
            return;
        }

        if engine_rocks::util::auto_compactions_is_disabled(&self.ctx.engines.kv) {
            debug!(
                "skip compact check when disabled auto compactions";
                "store_id" => self.fsm.store.id,
            );
            return;
        }

        // Start from last checked key.
        let mut ranges_need_check =
            Vec::with_capacity(self.ctx.cfg.region_compact_check_step as usize + 1);
        ranges_need_check.push(self.fsm.store.last_compact_checked_key.clone());

        let largest_key = {
            let meta = self.ctx.store_meta.lock().unwrap();
            if meta.region_ranges.is_empty() {
                debug!(
                    "there is no range need to check";
                    "store_id" => self.fsm.store.id
                );
                return;
            }

            // Collect continuous ranges.
            let left_ranges = meta.region_ranges.range((
                Excluded(self.fsm.store.last_compact_checked_key.clone()),
                Unbounded::<Key>,
            ));
            ranges_need_check.extend(
                left_ranges
                    .take(self.ctx.cfg.region_compact_check_step as usize)
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
            self.fsm.store.last_compact_checked_key = keys::DATA_MIN_KEY.to_vec();
        } else {
            self.fsm.store.last_compact_checked_key = last_key;
        }

        // Schedule the task.
        let cf_names = vec![CF_DEFAULT.to_owned(), CF_WRITE.to_owned()];
        if let Err(e) = self.ctx.cleanup_scheduler.schedule(CleanupTask::Compact(
            CompactTask::CheckAndCompact {
                cf_names,
                ranges: ranges_need_check,
                tombstones_num_threshold: self.ctx.cfg.region_compact_min_tombstones,
                tombstones_percent_threshold: self.ctx.cfg.region_compact_tombstones_percent,
            },
        )) {
            error!(
                "schedule space check task failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e,
            );
        }
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::default();

        stats.set_store_id(self.ctx.store_id());
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            stats.set_region_count(meta.regions.len() as u32);
        }

        let snap_stats = self.ctx.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        let apply_snapshot_count = self.ctx.applying_snap_count.load(Ordering::SeqCst);
        stats.set_applying_snap_count(apply_snapshot_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["applying"])
            .set(apply_snapshot_count as i64);

        stats.set_start_time(self.fsm.store.start_time.unwrap().sec as u32);

        // report store write flow to pd
        stats.set_bytes_written(
            self.ctx
                .global_stat
                .stat
                .engine_total_bytes_written
                .swap(0, Ordering::SeqCst),
        );
        stats.set_keys_written(
            self.ctx
                .global_stat
                .stat
                .engine_total_keys_written
                .swap(0, Ordering::SeqCst),
        );

        stats.set_is_busy(
            self.ctx
                .global_stat
                .stat
                .is_busy
                .swap(false, Ordering::SeqCst),
        );

        let store_info = StoreInfo {
            engine: Arc::clone(&self.ctx.engines.kv),
            capacity: self.ctx.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat { stats, store_info };
        if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
            error!("notify pd failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e
            );
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick();
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        fail_point!("peer_2_handle_snap_mgr_gc", self.fsm.store.id == 2, |_| Ok(
            ()
        ));
        let snap_keys = self.ctx.snap_mgr.list_idle_snap()?;
        if snap_keys.is_empty() {
            return Ok(());
        }
        let (mut last_region_id, mut keys) = (0, vec![]);
        let schedule_gc_snap = |region_id: u64, snaps| -> Result<()> {
            debug!(
                "schedule snap gc";
                "store_id" => self.fsm.store.id,
                "region_id" => region_id,
            );
            let gc_snap = PeerMsg::CasualMessage(CasualMessage::GcSnap { snaps });
            match self.ctx.router.send(region_id, gc_snap) {
                Ok(()) => Ok(()),
                Err(TrySendError::Disconnected(_)) if self.ctx.router.is_shutdown() => Ok(()),
                Err(TrySendError::Disconnected(PeerMsg::CasualMessage(
                    CasualMessage::GcSnap { snaps },
                ))) => {
                    // The snapshot exists because MsgAppend has been rejected. So the
                    // peer must have been exist. But now it's disconnected, so the peer
                    // has to be destroyed instead of being created.
                    info!(
                        "region is disconnected, remove snaps";
                        "region_id" => region_id,
                        "snaps" => ?snaps,
                    );
                    for (key, is_sending) in snaps {
                        let snap = self.ctx.snap_mgr.get_snapshot_for_gc(&key, is_sending)?;
                        self.ctx
                            .snap_mgr
                            .delete_snapshot(&key, snap.as_ref(), false);
                    }
                    Ok(())
                }
                Err(TrySendError::Full(_)) => Ok(()),
                Err(TrySendError::Disconnected(_)) => unreachable!(),
            }
        };
        for (key, is_sending) in snap_keys {
            if last_region_id == key.region_id {
                keys.push((key, is_sending));
                continue;
            }

            if !keys.is_empty() {
                schedule_gc_snap(last_region_id, keys)?;
                keys = vec![];
            }

            last_region_id = key.region_id;
            keys.push((key, is_sending));
        }
        if !keys.is_empty() {
            schedule_gc_snap(last_region_id, keys)?;
        }
        Ok(())
    }

    fn on_snap_mgr_gc(&mut self) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!(?e;
                "handle gc snap failed";
                "store_id" => self.fsm.store.id,
            );
        }
        self.register_snap_mgr_gc_tick();
    }

    fn on_compact_lock_cf(&mut self) {
        // Create a compact lock cf task(compact whole range) and schedule directly.
        let lock_cf_bytes_written = self
            .ctx
            .global_stat
            .stat
            .lock_cf_bytes_written
            .load(Ordering::SeqCst);
        if lock_cf_bytes_written > self.ctx.cfg.lock_cf_compact_bytes_threshold.0 {
            self.ctx
                .global_stat
                .stat
                .lock_cf_bytes_written
                .fetch_sub(lock_cf_bytes_written, Ordering::SeqCst);

            let task = CompactTask::Compact {
                cf_name: String::from(CF_LOCK),
                start_key: None,
                end_key: None,
            };
            if let Err(e) = self
                .ctx
                .cleanup_scheduler
                .schedule(CleanupTask::Compact(task))
            {
                error!(
                    "schedule compact lock cf task failed";
                    "store_id" => self.fsm.store.id,
                    "err" => ?e,
                );
            }
        }

        self.register_compact_lock_cf_tick();
    }

    fn register_pd_store_heartbeat_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::PdStoreHeartbeat,
            self.ctx.cfg.pd_store_heartbeat_tick_interval.0,
        );
    }

    fn register_snap_mgr_gc_tick(&self) {
        self.ctx
            .schedule_store_tick(StoreTick::SnapGc, self.ctx.cfg.snap_mgr_gc_tick_interval.0)
    }

    fn register_compact_lock_cf_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::CompactLockCf,
            self.ctx.cfg.lock_cf_compact_interval.0,
        )
    }
}

impl<'a, T: Transport, C: PdClient> StoreFsmDelegate<'a, T, C> {
    fn on_validate_sst_result(&mut self, ssts: Vec<SstMeta>) {
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
        if let Err(e) = self
            .ctx
            .cleanup_scheduler
            .schedule(CleanupTask::CleanupSST(task))
        {
            error!(
                "schedule to delete ssts failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e,
            );
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
            if let Err(e) = self
                .ctx
                .cleanup_scheduler
                .schedule(CleanupTask::CleanupSST(task))
            {
                error!(
                    "schedule to delete ssts failed";
                    "store_id" => self.fsm.store.id,
                    "err" => ?e
                );
            }
        }

        if !validate_ssts.is_empty() {
            let task = CleanupSSTTask::ValidateSST {
                ssts: validate_ssts,
            };
            if let Err(e) = self
                .ctx
                .cleanup_scheduler
                .schedule(CleanupTask::CleanupSST(task))
            {
                error!(
                   "schedule to validate ssts failed";
                   "store_id" => self.fsm.store.id,
                   "err" => ?e,
                );
            }
        }

        Ok(())
    }

    fn register_consistency_check_tick(&mut self) {
        self.ctx.schedule_store_tick(
            StoreTick::ConsistencyCheck,
            self.ctx.cfg.consistency_check_interval.0,
        )
    }

    fn on_consistency_check_tick(&mut self) {
        self.register_consistency_check_tick();
        if self.ctx.consistency_check_scheduler.is_busy() {
            return;
        }
        let (mut target_region_id, mut oldest) = (0, Instant::now());
        let target_peer = {
            let meta = self.ctx.store_meta.lock().unwrap();
            for region_id in meta.regions.keys() {
                match self.fsm.store.consistency_check_time.get(region_id) {
                    Some(time) => {
                        if *time < oldest {
                            oldest = *time;
                            target_region_id = *region_id;
                        }
                    }
                    None => {
                        target_region_id = *region_id;
                        break;
                    }
                }
            }
            if target_region_id == 0 {
                return;
            }
            match util::find_peer(&meta.regions[&target_region_id], self.ctx.store_id()) {
                None => return,
                Some(p) => p.clone(),
            }
        };
        info!(
            "scheduling consistency check for region";
            "store_id" => self.fsm.store.id,
            "region_id" => target_region_id,
        );
        self.fsm
            .store
            .consistency_check_time
            .insert(target_region_id, Instant::now());
        let mut request = new_admin_request(target_region_id, target_peer);
        let mut admin = AdminRequest::default();
        admin.set_cmd_type(AdminCmdType::ComputeHash);
        request.set_admin_request(admin);

        let _ = self.ctx.router.send(
            target_region_id,
            PeerMsg::RaftCommand(RaftCommand::new(request, Callback::None)),
        );
    }

    fn on_cleanup_import_sst_tick(&mut self) {
        if let Err(e) = self.on_cleanup_import_sst() {
            error!(?e;
                "cleanup import sst failed";
                "store_id" => self.fsm.store.id,
            );
        }
        self.register_cleanup_import_sst_tick();
    }

    fn register_cleanup_import_sst_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::CleanupImportSST,
            self.ctx.cfg.cleanup_import_sst_interval.0,
        )
    }

    fn clear_region_size_in_range(&mut self, start_key: &[u8], end_key: &[u8]) {
        let start_key = data_key(start_key);
        let end_key = data_end_key(end_key);

        let mut regions = vec![];
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            for (_, region_id) in meta
                .region_ranges
                .range((Excluded(start_key), Included(end_key)))
            {
                regions.push(*region_id);
            }
        }
        for region_id in regions {
            let _ = self.ctx.router.send(
                region_id,
                PeerMsg::CasualMessage(CasualMessage::ClearRegionSize),
            );
        }
    }

    fn on_store_unreachable(&mut self, store_id: u64) {
        let now = Instant::now();
        if self
            .fsm
            .store
            .last_unreachable_report
            .get(&store_id)
            .map_or(UNREACHABLE_BACKOFF, |t| now.duration_since(*t))
            < UNREACHABLE_BACKOFF
        {
            return;
        }
        info!(
            "broadcasting unreachable";
            "store_id" => self.fsm.store.id,
            "unreachable_store_id" => store_id,
        );
        self.fsm.store.last_unreachable_report.insert(store_id, now);
        // It's possible to acquire the lock and only send notification to
        // involved regions. However loop over all the regions can take a
        // lot of time, which may block other operations.
        self.ctx.router.report_unreachable(store_id);
    }
}

fn size_change_filter(info: &RocksCompactionJobInfo) -> bool {
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

pub fn new_compaction_listener(ch: RaftRouter<RocksEngine>) -> CompactionListener {
    let ch = Mutex::new(ch);
    let compacted_handler = Box::new(move |compacted_event: CompactedEvent| {
        let ch = ch.lock().unwrap();
        if let Err(e) = ch.send_control(StoreMsg::CompactedEvent(compacted_event)) {
            error!(
                "send compaction finished event to raftstore failed"; "err" => ?e,
            );
        }
    });
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
    use engine_rocks::RangeOffsets;
    use engine_rocks::RangeProperties;

    use super::*;

    #[test]
    fn test_calc_region_declined_bytes() {
        let prop = RangeProperties {
            offsets: vec![
                (
                    b"a".to_vec(),
                    RangeOffsets {
                        size: 4 * 1024,
                        keys: 1,
                    },
                ),
                (
                    b"b".to_vec(),
                    RangeOffsets {
                        size: 8 * 1024,
                        keys: 2,
                    },
                ),
                (
                    b"c".to_vec(),
                    RangeOffsets {
                        size: 12 * 1024,
                        keys: 3,
                    },
                ),
            ],
        };
        let event = CompactedEvent {
            cf: "default".to_owned(),
            output_level: 3,
            total_input_bytes: 12 * 1024,
            total_output_bytes: 0,
            start_key: prop.smallest_key().unwrap(),
            end_key: prop.largest_key().unwrap(),
            input_props: vec![prop],
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
