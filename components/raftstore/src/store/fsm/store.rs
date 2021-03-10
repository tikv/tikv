// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::{Ord, Ordering as CmpOrdering};
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{mem, u64};

use batch_system::{BasicMailbox, BatchRouter, BatchSystem, Fsm, HandlerBuilder, PollHandler};
use crossbeam::channel::{TryRecvError, TrySendError};
use engine_traits::PerfContext;
use engine_traits::PerfContextKind;
use engine_traits::{Engines, KvEngine, Mutable, WriteBatch, WriteBatchExt, WriteOptions};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use futures::compat::Future01CompatExt;
use futures::FutureExt;
use kvproto::import_sstpb::SstMeta;
use kvproto::kvrpcpb::LeaderInfo;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::StoreStats;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest};
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use kvproto::replication_modepb::{ReplicationMode, ReplicationStatus};
use protobuf::Message;
use raft::StateRole;
use time::{self, Timespec};

use collections::HashMap;
use engine_traits::CompactedEvent;
use engine_traits::{RaftEngine, RaftLogBatch};
use keys::{self, data_end_key, data_key, enc_end_key, enc_start_key};
use pd_client::{FeatureGate, PdClient};
use sst_importer::SSTImporter;
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};
use tikv_util::time::{duration_to_sec, Instant as TiInstant};
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{FutureScheduler, FutureWorker, Scheduler, Worker};
use tikv_util::{is_zero_duration, sys as sys_util, Either, RingQueue};

use crate::coprocessor::split_observer::SplitObserver;
use crate::coprocessor::{BoxAdminObserver, CoprocessorHost, RegionChangeEvent};
use crate::store::config::Config;
use crate::store::fsm::metrics::*;
use crate::store::fsm::peer::{
    maybe_destroy_source, new_admin_request, PeerFsm, PeerFsmDelegate, SenderFsmPair,
};
use crate::store::fsm::ApplyNotifier;
use crate::store::fsm::ApplyTaskRes;
use crate::store::fsm::{
    create_apply_batch_system, ApplyBatchSystem, ApplyPollerBuilder, ApplyRes, ApplyRouter,
    CollectedReady,
};
use crate::store::local_metrics::RaftMetrics;
use crate::store::metrics::*;
use crate::store::peer_storage::{self, HandleRaftReadyContext};
use crate::store::transport::Transport;
use crate::store::util::is_initial_msg;
use crate::store::worker::{
    AutoSplitController, CleanupRunner, CleanupSSTRunner, CleanupSSTTask, CleanupTask,
    CompactRunner, CompactTask, ConsistencyCheckRunner, ConsistencyCheckTask, PdRunner,
    RaftlogGcRunner, RaftlogGcTask, ReadDelegate, RegionRunner, RegionTask, SplitCheckTask,
};
use crate::store::PdTask;
use crate::store::PeerTicks;
use crate::store::{
    util, Callback, CasualMessage, GlobalReplicationState, MergeResultKind, PeerMsg, RaftCommand,
    SignificantMsg, SnapManager, StoreMsg, StoreTick,
};
use crate::Result;
use concurrency_manager::ConcurrencyManager;
use tikv_util::future::poll_future_notify;

type Key = Vec<u8>;

const KV_WB_SHRINK_SIZE: usize = 256 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 1024 * 1024;
pub const PENDING_MSG_CAP: usize = 100;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);

pub struct StoreInfo<E> {
    pub engine: E,
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
    /// region_id -> (term, leader_peer_id)
    pub leaders: HashMap<u64, (u64, u64)>,
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
            leaders: HashMap::default(),
            pending_msgs: RingQueue::with_capacity(vote_capacity),
            pending_snapshot_regions: Vec::default(),
            pending_merge_targets: HashMap::default(),
            targets_map: HashMap::default(),
            atomic_snap_regions: HashMap::default(),
            destroyed_region_for_snap: HashMap::default(),
        }
    }

    #[inline]
    pub fn set_region<EK: KvEngine, ER: RaftEngine>(
        &mut self,
        host: &CoprocessorHost<EK>,
        region: Region,
        peer: &mut crate::store::Peer<EK, ER>,
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

pub struct RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub router: BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>>,
}

impl<EK, ER> Clone for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn clone(&self) -> Self {
        RaftRouter {
            router: self.router.clone(),
        }
    }
}

impl<EK, ER> Deref for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Target = BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>>;

    fn deref(&self) -> &BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>> {
        &self.router
    }
}

impl<EK, ER> ApplyNotifier<EK> for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn notify(&self, apply_res: Vec<ApplyRes<EK::Snapshot>>) {
        for r in apply_res {
            self.router.try_send(
                r.region_id,
                PeerMsg::ApplyRes {
                    res: ApplyTaskRes::Apply(r),
                },
            );
        }
    }
    fn notify_one(&self, region_id: u64, msg: PeerMsg<EK>) {
        self.router.try_send(region_id, msg);
    }

    fn clone_box(&self) -> Box<dyn ApplyNotifier<EK>> {
        Box::new(self.clone())
    }
}

impl<EK, ER> RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
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
        cmd: RaftCommand<EK::Snapshot>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<EK::Snapshot>>> {
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

    fn report_status_update(&self) {
        self.broadcast_normal(|| PeerMsg::UpdateReplicationMode)
    }

    /// Broadcasts resolved result to all regions.
    pub fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::StoreResolved { store_id, group_id })
        })
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

pub struct PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    /// The count of processed normal Fsm.
    pub processed_fsm_count: usize,
    pub cfg: Config,
    pub store: metapb::Store,
    pub pd_scheduler: FutureScheduler<PdTask<EK>>,
    pub consistency_check_scheduler: Scheduler<ConsistencyCheckTask<EK::Snapshot>>,
    pub split_check_scheduler: Scheduler<SplitCheckTask>,
    // handle Compact, CleanupSST task
    pub cleanup_scheduler: Scheduler<CleanupTask>,
    pub raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    pub region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    pub apply_router: ApplyRouter<EK>,
    pub router: RaftRouter<EK, ER>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    pub feature_gate: FeatureGate,
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
    pub coprocessor_host: CoprocessorHost<EK>,
    pub timer: SteadyTimer,
    pub trans: T,
    pub global_replication_state: Arc<Mutex<GlobalReplicationState>>,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub engines: Engines<EK, ER>,
    pub kv_wb: EK::WriteBatch,
    pub raft_wb: ER::LogBatch,
    pub pending_count: usize,
    pub sync_log: bool,
    pub has_ready: bool,
    pub ready_res: Vec<CollectedReady>,
    pub current_time: Option<Timespec>,
    pub perf_context: EK::PerfContext,
    pub tick_batch: Vec<PeerTickBatch>,
    pub node_start_time: Option<TiInstant>,
}

impl<EK, ER, T> HandleRaftReadyContext<EK::WriteBatch, ER::LogBatch> for PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn wb_mut(&mut self) -> (&mut EK::WriteBatch, &mut ER::LogBatch) {
        (&mut self.kv_wb, &mut self.raft_wb)
    }

    #[inline]
    fn kv_wb_mut(&mut self) -> &mut EK::WriteBatch {
        &mut self.kv_wb
    }

    #[inline]
    fn raft_wb_mut(&mut self) -> &mut ER::LogBatch {
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

impl<EK, ER, T> PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    pub fn store_id(&self) -> u64 {
        self.store.get_id()
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

impl<EK, ER, T: Transport> PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    #[inline]
    fn schedule_store_tick(&self, tick: StoreTick, timeout: Duration) {
        if !is_zero_duration(&timeout) {
            let mb = self.router.control_mailbox();
            let delay = self.timer.delay(timeout).compat().map(move |_| {
                if let Err(e) = mb.force_send(StoreMsg::Tick(tick)) {
                    info!(
                        "failed to schedule store tick, are we shutting down?";
                        "tick" => ?tick,
                        "err" => ?e
                    );
                }
            });
            poll_future_notify(delay);
        }
    }

    pub fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        cur_epoch: RegionEpoch,
        need_gc: bool,
        target_region: Option<metapb::Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "raft message is stale, ignore it";
                "region_id" => region_id,
                "current_region_epoch" => ?cur_epoch,
                "msg_type" => ?msg_type,
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "raft message is stale, tell to gc";
            "region_id" => region_id,
            "current_region_epoch" => ?cur_epoch,
            "msg_type" => ?msg_type,
        );

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

pub struct StoreFsm<EK>
where
    EK: KvEngine,
{
    store: Store,
    receiver: Receiver<StoreMsg<EK>>,
}

impl<EK> StoreFsm<EK>
where
    EK: KvEngine,
{
    pub fn new(cfg: &Config) -> (LooseBoundedSender<StoreMsg<EK>>, Box<StoreFsm<EK>>) {
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

impl<EK> Fsm for StoreFsm<EK>
where
    EK: KvEngine,
{
    type Message = StoreMsg<EK>;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.store.stopped
    }
}

struct StoreFsmDelegate<'a, EK: KvEngine + 'static, ER: RaftEngine + 'static, T: 'static> {
    fsm: &'a mut StoreFsm<EK>,
    ctx: &'a mut PollContext<EK, ER, T>,
}

impl<'a, EK: KvEngine + 'static, ER: RaftEngine + 'static, T: Transport>
    StoreFsmDelegate<'a, EK, ER, T>
{
    fn on_tick(&mut self, tick: StoreTick) {
        let t = TiInstant::now_coarse();
        match tick {
            StoreTick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(),
            StoreTick::SnapGc => self.on_snap_mgr_gc(),
            StoreTick::CompactLockCf => self.on_compact_lock_cf(),
            StoreTick::CompactCheck => self.on_compact_check_tick(),
            StoreTick::ConsistencyCheck => self.on_consistency_check_tick(),
            StoreTick::CleanupImportSST => self.on_cleanup_import_sst_tick(),
            StoreTick::RaftEnginePurge => self.on_raft_engine_purge_tick(),
        }
        let elapsed = t.elapsed();
        RAFT_EVENT_DURATION
            .get(tick.tag())
            .observe(duration_to_sec(elapsed) as f64);
        slow_log!(
            elapsed,
            "[store {}] handle timeout {:?}",
            self.fsm.store.id,
            tick
        );
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<StoreMsg<EK>>) {
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
                StoreMsg::CheckLeader { leaders, cb } => self.on_check_leader(leaders, cb),
                #[cfg(any(test, feature = "testexport"))]
                StoreMsg::Validate(f) => f(&self.ctx.cfg),
                StoreMsg::UpdateReplicationMode(status) => self.on_update_replication_mode(status),
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
        self.register_raft_engine_purge_tick();
    }
}

pub struct RaftPoller<EK: KvEngine + 'static, ER: RaftEngine + 'static, T: 'static> {
    tag: String,
    store_msg_buf: Vec<StoreMsg<EK>>,
    peer_msg_buf: Vec<PeerMsg<EK>>,
    previous_metrics: RaftMetrics,
    timer: TiInstant,
    poll_ctx: PollContext<EK, ER, T>,
    messages_per_tick: usize,
    cfg_tracker: Tracker<Config>,
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> RaftPoller<EK, ER, T> {
    fn handle_raft_ready(&mut self, peers: &mut [Box<PeerFsm<EK, ER>>]) {
        // Only enable the fail point when the store id is equal to 3, which is
        // the id of slow store in tests.
        fail_point!("on_raft_ready", self.poll_ctx.store_id() == 3, |_| {});
        if self.poll_ctx.trans.need_flush()
            && (!self.poll_ctx.kv_wb.is_empty() || !self.poll_ctx.raft_wb.is_empty())
        {
            self.poll_ctx.trans.flush();
        }
        let ready_cnt = self.poll_ctx.ready_res.len();
        self.poll_ctx.raft_metrics.ready.has_ready_region += ready_cnt as u64;
        fail_point!("raft_before_save");
        if !self.poll_ctx.kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.poll_ctx
                .kv_wb
                .write_opt(&write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
            let data_size = self.poll_ctx.kv_wb.data_size();
            if data_size > KV_WB_SHRINK_SIZE {
                self.poll_ctx.kv_wb = self.poll_ctx.engines.kv.write_batch_with_cap(4 * 1024);
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
            self.poll_ctx
                .engines
                .raft
                .consume_and_shrink(
                    &mut self.poll_ctx.raft_wb,
                    true,
                    RAFT_WB_SHRINK_SIZE,
                    4 * 1024,
                )
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });
        }

        self.poll_ctx.perf_context.report_metrics();
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
                .compat()
                .map(move |_| {
                    for tick in peer_ticks {
                        tick();
                    }
                });
            poll_future_notify(f);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> PollHandler<PeerFsm<EK, ER>, StoreFsm<EK>>
    for RaftPoller<EK, ER, T>
{
    fn begin(&mut self, _batch_size: usize) {
        self.previous_metrics = self.poll_ctx.raft_metrics.clone();
        self.poll_ctx.processed_fsm_count = 0;
        self.poll_ctx.pending_count = 0;
        self.poll_ctx.sync_log = false;
        self.poll_ctx.has_ready = false;
        self.timer = TiInstant::now_coarse();
        // update config
        self.poll_ctx.perf_context.start_observe();
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

    fn handle_control(&mut self, store: &mut StoreFsm<EK>) -> Option<usize> {
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

    fn handle_normal(&mut self, peer: &mut PeerFsm<EK, ER>) -> Option<usize> {
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
                            && matches!(
                                msg,
                                PeerMsg::ApplyRes {
                                    res: ApplyTaskRes::Destroy { .. },
                                }
                            ),
                        |_| unreachable!()
                    );
                    self.peer_msg_buf.push(msg);
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
        delegate.collect_ready();
        self.poll_ctx.processed_fsm_count += 1;
        expected_msg_count
    }

    fn end(&mut self, peers: &mut [Box<PeerFsm<EK, ER>>]) {
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
        if self.poll_ctx.trans.need_flush() {
            self.poll_ctx.trans.flush();
        }
    }
}

pub struct RaftPollerBuilder<EK: KvEngine, ER: RaftEngine, T> {
    pub cfg: Arc<VersionTrack<Config>>,
    pub store: metapb::Store,
    pd_scheduler: FutureScheduler<PdTask<EK>>,
    consistency_check_scheduler: Scheduler<ConsistencyCheckTask<EK::Snapshot>>,
    split_check_scheduler: Scheduler<SplitCheckTask>,
    cleanup_scheduler: Scheduler<CleanupTask>,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    pub region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    apply_router: ApplyRouter<EK>,
    pub router: RaftRouter<EK, ER>,
    pub importer: Arc<SSTImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    pub pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    snap_mgr: SnapManager,
    pub coprocessor_host: CoprocessorHost<EK>,
    trans: T,
    global_stat: GlobalStoreStat,
    pub engines: Engines<EK, ER>,
    applying_snap_count: Arc<AtomicUsize>,
    global_replication_state: Arc<Mutex<GlobalReplicationState>>,
    feature_gate: FeatureGate,
}

impl<EK: KvEngine, ER: RaftEngine, T> RaftPollerBuilder<EK, ER, T> {
    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<Vec<SenderFsmPair<EK, ER>>> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let kv_engine = self.engines.kv.clone();
        let store_id = self.store.get_id();
        let mut total_count = 0;
        let mut tombstone_count = 0;
        let mut applying_count = 0;
        let mut region_peers = vec![];

        let t = Instant::now();
        let mut kv_wb = self.engines.kv.write_batch();
        let mut raft_wb = self.engines.raft.log_batch(4 * 1024);
        let mut applying_regions = vec![];
        let mut merging_count = 0;
        let mut meta = self.store_meta.lock().unwrap();
        let mut replication_state = self.global_replication_state.lock().unwrap();
        kv_engine.scan_cf(CF_RAFT, start_key, end_key, false, |key, value| {
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
            peer.peer.init_replication_mode(&mut *replication_state);
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
            kv_wb.write().unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.engines.raft.consume(&mut raft_wb, true).unwrap();
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
            peer.peer.init_replication_mode(&mut *replication_state);
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
        kv_wb: &mut EK::WriteBatch,
        raft_wb: &mut ER::LogBatch,
        origin_state: &RegionLocalState,
    ) {
        let rid = origin_state.get_region().get_id();
        let raft_state = match self.engines.raft.get_raft_state(rid).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };
        peer_storage::clear_meta(&self.engines, kv_wb, raft_wb, rid, &raft_state).unwrap();
        let key = keys::region_state_key(rid);
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

        self.engines.kv.roughly_cleanup_ranges(&ranges)?;

        info!(
            "cleans up garbage data";
            "store_id" => self.store.get_id(),
            "garbage_range_count" => ranges.len(),
            "takes" => ?t.elapsed()
        );

        Ok(())
    }
}

impl<EK, ER, T> HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>> for RaftPollerBuilder<EK, ER, T>
where
    EK: KvEngine + 'static,
    ER: RaftEngine + 'static,
    T: Transport + 'static,
{
    type Handler = RaftPoller<EK, ER, T>;

    fn build(&mut self) -> RaftPoller<EK, ER, T> {
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
            global_replication_state: self.global_replication_state.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            engines: self.engines.clone(),
            kv_wb: self.engines.kv.write_batch(),
            raft_wb: self.engines.raft.log_batch(4 * 1024),
            pending_count: 0,
            sync_log: false,
            has_ready: false,
            ready_res: Vec::new(),
            current_time: None,
            perf_context: self
                .engines
                .kv
                .get_perf_context(self.cfg.value().perf_level, PerfContextKind::RaftstoreStore),
            tick_batch: vec![PeerTickBatch::default(); 256],
            node_start_time: Some(TiInstant::now_coarse()),
            feature_gate: self.feature_gate.clone(),
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
            cfg_tracker: self.cfg.clone().tracker(tag),
        }
    }
}

struct Workers<EK: KvEngine> {
    pd_worker: FutureWorker<PdTask<EK>>,
    background_worker: Worker,

    // Both of cleanup tasks and region tasks get their own workers, instead of reusing
    // background_workers. This is because the underlying compact_range call is a
    // blocking operation, which can take an extensive amount of time.
    cleanup_worker: Worker,
    region_worker: Worker,

    coprocessor_host: CoprocessorHost<EK>,
}

pub struct RaftBatchSystem<EK: KvEngine, ER: RaftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm<EK>>,
    apply_router: ApplyRouter<EK>,
    apply_system: ApplyBatchSystem<EK>,
    router: RaftRouter<EK, ER>,
    workers: Option<Workers<EK>>,
}

impl<EK: KvEngine, ER: RaftEngine> RaftBatchSystem<EK, ER> {
    pub fn router(&self) -> RaftRouter<EK, ER> {
        self.router.clone()
    }

    pub fn apply_router(&self) -> ApplyRouter<EK> {
        self.apply_router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn<T: Transport + 'static, C: PdClient + 'static>(
        &mut self,
        meta: metapb::Store,
        cfg: Arc<VersionTrack<Config>>,
        engines: Engines<EK, ER>,
        trans: T,
        pd_client: Arc<C>,
        mgr: SnapManager,
        pd_worker: FutureWorker<PdTask<EK>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        mut coprocessor_host: CoprocessorHost<EK>,
        importer: Arc<SSTImporter>,
        split_check_scheduler: Scheduler<SplitCheckTask>,
        background_worker: Worker,
        auto_split_controller: AutoSplitController,
        global_replication_state: Arc<Mutex<GlobalReplicationState>>,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()> {
        assert!(self.workers.is_none());
        // TODO: we can get cluster meta regularly too later.

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, BoxAdminObserver::new(SplitObserver));

        let workers = Workers {
            pd_worker,
            background_worker,
            cleanup_worker: Worker::new("cleanup-worker"),
            region_worker: Worker::new("region-worker"),
            coprocessor_host: coprocessor_host.clone(),
        };
        mgr.init()?;
        let region_runner = RegionRunner::new(
            engines.kv.clone(),
            mgr.clone(),
            cfg.value().snap_apply_batch_size.0 as usize,
            cfg.value().use_delete_range,
            workers.coprocessor_host.clone(),
            self.router(),
        );
        let region_scheduler = workers
            .region_worker
            .start_with_timer("snapshot-worker", region_runner);

        let raftlog_gc_runner = RaftlogGcRunner::new(self.router(), engines.clone());
        let raftlog_gc_scheduler = workers
            .background_worker
            .start_with_timer("raft-gc-worker", raftlog_gc_runner);
        let compact_runner = CompactRunner::new(engines.kv.clone());
        let cleanup_sst_runner = CleanupSSTRunner::new(
            meta.get_id(),
            self.router.clone(),
            Arc::clone(&importer),
            Arc::clone(&pd_client),
        );
        let cleanup_runner = CleanupRunner::new(compact_runner, cleanup_sst_runner);
        let cleanup_scheduler = workers
            .cleanup_worker
            .start("cleanup-worker", cleanup_runner);
        let consistency_check_runner =
            ConsistencyCheckRunner::<EK, _>::new(self.router.clone(), coprocessor_host.clone());
        let consistency_check_scheduler = workers
            .background_worker
            .start("consistency-check", consistency_check_runner);

        let mut builder = RaftPollerBuilder {
            cfg,
            store: meta,
            engines,
            router: self.router.clone(),
            split_check_scheduler,
            region_scheduler,
            pd_scheduler: workers.pd_worker.scheduler(),
            consistency_check_scheduler,
            cleanup_scheduler,
            raftlog_gc_scheduler,
            apply_router: self.apply_router.clone(),
            trans,
            coprocessor_host,
            importer,
            snap_mgr: mgr,
            global_replication_state,
            global_stat: GlobalStoreStat::default(),
            store_meta,
            pending_create_peers: Arc::new(Mutex::new(HashMap::default())),
            applying_snap_count: Arc::new(AtomicUsize::new(0)),
            feature_gate: pd_client.feature_gate().clone(),
        };
        let region_peers = builder.init()?;
        let engine = builder.engines.kv.clone();
        if engine.support_write_batch_vec() {
            self.start_system::<T, C, <EK as WriteBatchExt>::WriteBatchVec>(
                workers,
                region_peers,
                builder,
                auto_split_controller,
                concurrency_manager,
                pd_client,
            )?;
        } else {
            self.start_system::<T, C, <EK as WriteBatchExt>::WriteBatch>(
                workers,
                region_peers,
                builder,
                auto_split_controller,
                concurrency_manager,
                pd_client,
            )?;
        }
        Ok(())
    }

    fn start_system<T: Transport + 'static, C: PdClient + 'static, W: WriteBatch<EK> + 'static>(
        &mut self,
        mut workers: Workers<EK>,
        region_peers: Vec<SenderFsmPair<EK, ER>>,
        builder: RaftPollerBuilder<EK, ER, T>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
        pd_client: Arc<C>,
    ) -> Result<()> {
        let cfg = builder.cfg.value().clone();
        let store = builder.store.clone();

        let apply_poller_builder = ApplyPollerBuilder::<EK, W>::new(
            &builder,
            Box::new(self.router.clone()),
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

        let pd_runner = PdRunner::new(
            store.get_id(),
            Arc::clone(&pd_client),
            self.router.clone(),
            workers.pd_worker.scheduler(),
            cfg.pd_store_heartbeat_tick_interval.0,
            auto_split_controller,
            concurrency_manager,
        );
        box_try!(workers.pd_worker.start(pd_runner));

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
        let handle = workers.pd_worker.stop();
        self.apply_system.shutdown();
        self.system.shutdown();
        if let Some(h) = handle {
            h.join().unwrap();
        }
        workers.coprocessor_host.shutdown();
        workers.cleanup_worker.stop();
        workers.region_worker.stop();
        workers.background_worker.stop();
    }
}

pub fn create_raft_batch_system<EK: KvEngine, ER: RaftEngine>(
    cfg: &Config,
) -> (RaftRouter<EK, ER>, RaftBatchSystem<EK, ER>) {
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

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> StoreFsmDelegate<'a, EK, ER, T> {
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
            match self.ctx.engines.kv.get_msg_cf(CF_RAFT, &state_key)? {
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
                .handle_stale_msg(msg, region_epoch.clone(), true, merge_target);
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
            let mut need_gc_msg = util::is_vote_msg(msg.get_message());
            if msg.has_extra_msg() {
                // A learner can't vote so it sends the check-stale-peer msg to others to find out whether
                // it is removed due to conf change or merge.
                need_gc_msg |=
                    msg.get_extra_msg().get_type() == ExtraMessageType::MsgCheckStalePeer;
                // For backward compatibility
                need_gc_msg |= msg.get_extra_msg().get_type() == ExtraMessageType::MsgRegionWakeUp;
            }
            let not_exist = util::find_peer(region, from_store_id).is_none();
            self.ctx
                .handle_stale_msg(msg, region_epoch.clone(), need_gc_msg && not_exist, None);

            if need_gc_msg && !not_exist {
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
            "msg_type" => %util::MsgType(&msg),
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

        let mut replication_state = self.ctx.global_replication_state.lock().unwrap();
        peer.peer.init_replication_mode(&mut *replication_state);
        drop(replication_state);

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

    fn on_compaction_finished(&mut self, event: EK::CompactedEvent) {
        if event.is_size_declining_trivial(self.ctx.cfg.region_split_check_diff.0) {
            return;
        }

        let output_level_str = event.output_level_label();
        COMPACTION_DECLINED_BYTES
            .with_label_values(&[&output_level_str])
            .observe(event.total_bytes_declined() as f64);

        // self.cfg.region_split_check_diff.0 / 16 is an experienced value.
        let mut region_declined_bytes = {
            let meta = self.ctx.store_meta.lock().unwrap();
            event.calc_ranges_declined_bytes(
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

        if self
            .ctx
            .engines
            .kv
            .auto_compactions_is_disabled()
            .expect("cf")
        {
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

        let used_size = self.ctx.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
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
            engine: self.ctx.engines.kv.clone(),
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

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> StoreFsmDelegate<'a, EK, ER, T> {
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
        self.ctx
            .coprocessor_host
            .on_prepropose_compute_hash(admin.mut_compute_hash());
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

    fn on_update_replication_mode(&mut self, status: ReplicationStatus) {
        let mut state = self.ctx.global_replication_state.lock().unwrap();
        if state.status().mode == status.mode {
            if status.get_mode() == ReplicationMode::Majority {
                return;
            }
            let exist_dr = state.status().get_dr_auto_sync();
            let dr = status.get_dr_auto_sync();
            if exist_dr.state_id == dr.state_id && exist_dr.state == dr.state {
                return;
            }
        }
        info!("updating replication mode"; "status" => ?status);
        state.set_status(status);
        drop(state);
        self.ctx.router.report_status_update()
    }

    fn register_raft_engine_purge_tick(&self) {
        self.ctx.schedule_store_tick(
            StoreTick::RaftEnginePurge,
            self.ctx.cfg.raft_engine_purge_interval.0,
        )
    }

    fn on_raft_engine_purge_tick(&self) {
        let scheduler = &self.ctx.raftlog_gc_scheduler;
        let _ = scheduler.schedule(RaftlogGcTask::Purge);
        self.register_raft_engine_purge_tick();
    }

    fn on_check_leader(&self, leaders: Vec<LeaderInfo>, cb: Box<dyn FnOnce(Vec<u64>) + Send>) {
        let meta = self.ctx.store_meta.lock().unwrap();
        let regions = leaders
            .into_iter()
            .map(|leader_info| {
                if let Some((term, leader_id)) = meta.leaders.get(&leader_info.region_id) {
                    if let Some(region) = meta.regions.get(&leader_info.region_id) {
                        if *term == leader_info.term
                            && *leader_id == leader_info.peer_id
                            && util::compare_region_epoch(
                                leader_info.get_region_epoch(),
                                region,
                                true,
                                true,
                                false,
                            )
                            .is_ok()
                        {
                            return Some(leader_info.region_id);
                        }
                        debug!("check leader failed";
                            "leader_info" => ?leader_info,
                            "current_leader" => leader_id,
                            "current_term" => term,
                            "current_region" => ?region,
                            "store_id" => self.fsm.store.id,
                        );
                        return None;
                    }
                }
                debug!("check leader failed, meta not found";
                    "leader_info" => ?leader_info,
                    "store_id" => self.fsm.store.id,
                );
                None
            })
            .filter_map(|r| r)
            .collect();
        cb(regions);
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::RangeOffsets;
    use engine_rocks::RangeProperties;
    use engine_rocks::RocksCompactedEvent;

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
        let event = RocksCompactedEvent {
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

        let declined_bytes = event.calc_ranges_declined_bytes(&region_ranges, 1024);
        let expected_declined_bytes = vec![(2, 8192), (3, 4096)];
        assert_eq!(declined_bytes, expected_declined_bytes);
    }
}
