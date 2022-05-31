// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    cell::Cell,
    cmp::{Ord, Ordering as CmpOrdering},
    collections::{
        BTreeMap,
        Bound::{Excluded, Included, Unbounded},
    },
    mem,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
    u64,
};

use batch_system::{
    BasicMailbox, BatchRouter, BatchSystem, Config as BatchSystemConfig, Fsm, HandleResult,
    HandlerBuilder, PollHandler, Priority,
};
use collections::{HashMap, HashMapEntry, HashSet};
use concurrency_manager::ConcurrencyManager;
use crossbeam::channel::{unbounded, Sender, TryRecvError, TrySendError};
use engine_traits::{
    CompactedEvent, DeleteStrategy, Engines, KvEngine, Mutable, PerfContextKind, RaftEngine,
    RaftLogBatch, Range, WriteBatch, WriteOptions, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
};
use fail::fail_point;
use futures::{compat::Future01CompatExt, FutureExt};
use grpcio_health::HealthService;
use keys::{self, data_end_key, data_key, enc_end_key, enc_start_key};
use kvproto::{
    import_sstpb::{SstMeta, SwitchMode},
    metapb::{self, Region, RegionEpoch},
    pdpb::{self, QueryStats, StoreStats},
    raft_cmdpb::{AdminCmdType, AdminRequest},
    raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState},
    replication_modepb::{ReplicationMode, ReplicationStatus},
};
use pd_client::{Feature, FeatureGate, PdClient};
use protobuf::Message;
use raft::StateRole;
use resource_metering::CollectorRegHandle;
use sst_importer::SstImporter;
use tikv_alloc::trace::TraceEvent;
use tikv_util::{
    box_err, box_try,
    config::{Tracker, VersionTrack},
    debug, defer, error,
    future::poll_future_notify,
    info, is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
    slow_log, sys as sys_util,
    sys::disk::{get_disk_status, DiskUsage},
    time::{duration_to_sec, Instant as TiInstant},
    timer::SteadyTimer,
    warn,
    worker::{LazyWorker, Scheduler, Worker},
    Either, RingQueue,
};
use time::{self, Timespec};

use crate::{
    bytes_capacity,
    coprocessor::{
        split_observer::SplitObserver, BoxAdminObserver, CoprocessorHost, RegionChangeEvent,
        RegionChangeReason,
    },
    store::{
        async_io::write::{StoreWriters, Worker as WriteWorker, WriteMsg},
        config::Config,
        fsm::{
            create_apply_batch_system,
            metrics::*,
            peer::{
                maybe_destroy_source, new_admin_request, PeerFsm, PeerFsmDelegate, SenderFsmPair,
            },
            ApplyBatchSystem, ApplyNotifier, ApplyPollerBuilder, ApplyRes, ApplyRouter,
            ApplyTaskRes,
        },
        local_metrics::{RaftMetrics, RaftReadyMetrics},
        memory::*,
        metrics::*,
        peer_storage,
        transport::Transport,
        util,
        util::{is_initial_msg, RegionReadProgressRegistry},
        worker::{
            AutoSplitController, CleanupRunner, CleanupSstRunner, CleanupSstTask, CleanupTask,
            CompactRunner, CompactTask, ConsistencyCheckRunner, ConsistencyCheckTask,
            GcSnapshotRunner, GcSnapshotTask, PdRunner, RaftlogFetchRunner, RaftlogFetchTask,
            RaftlogGcRunner, RaftlogGcTask, ReadDelegate, RefreshConfigRunner, RefreshConfigTask,
            RegionRunner, RegionTask, SplitCheckTask,
        },
        Callback, CasualMessage, GlobalReplicationState, InspectedRaftMessage, MergeResultKind,
        PdTask, PeerMsg, PeerTick, RaftCommand, SignificantMsg, SnapManager, StoreMsg, StoreTick,
    },
    Result,
};

type Key = Vec<u8>;

pub const PENDING_MSG_CAP: usize = 100;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);
const ENTRY_CACHE_EVICT_TICK_DURATION: Duration = Duration::from_secs(1);
pub const MULTI_FILES_SNAPSHOT_FEATURE: Feature = Feature::require(6, 1, 0); // it only makes sense for large region

pub struct StoreInfo<EK, ER> {
    pub kv_engine: EK,
    pub raft_engine: ER,
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
    /// region_id -> `RegionReadProgress`
    pub region_read_progress: RegionReadProgressRegistry,
    /// record sst_file_name -> (sst_smallest_key, sst_largest_key)
    pub damaged_ranges: HashMap<String, (Vec<u8>, Vec<u8>)>,
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
            region_read_progress: RegionReadProgressRegistry::new(),
            damaged_ranges: HashMap::default(),
        }
    }

    #[inline]
    pub fn set_region<EK: KvEngine, ER: RaftEngine>(
        &mut self,
        host: &CoprocessorHost<EK>,
        region: Region,
        peer: &mut crate::store::Peer<EK, ER>,
        reason: RegionChangeReason,
    ) {
        let prev = self.regions.insert(region.get_id(), region.clone());
        if prev.map_or(true, |r| r.get_id() != region.get_id()) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.tag);
        }
        let reader = self.readers.get_mut(&region.get_id()).unwrap();
        peer.set_region(host, reader, region, reason);
    }

    /// Update damaged ranges and return true if overlap exists.
    ///
    /// Condition:
    /// end_key > file.smallestkey
    /// start_key <= file.largestkey
    pub fn update_overlap_damaged_ranges(&mut self, fname: &str, start: &[u8], end: &[u8]) -> bool {
        // `region_ranges` is promised to have no overlap so just check the first region.
        if let Some((_, id)) = self
            .region_ranges
            .range((Excluded(start.to_owned()), Unbounded::<Vec<u8>>))
            .next()
        {
            let region = &self.regions[id];
            if keys::enc_start_key(region).as_slice() <= end {
                if let HashMapEntry::Vacant(v) = self.damaged_ranges.entry(fname.to_owned()) {
                    v.insert((start.to_owned(), end.to_owned()));
                }
                return true;
            }
        }

        // It's OK to remove the range here before deleting real file.
        let _ = self.damaged_ranges.remove(fname);
        false
    }

    /// Get all region ids overlapping damaged ranges.
    pub fn get_all_damaged_region_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::default();
        for (_fname, (start, end)) in self.damaged_ranges.iter() {
            for (_, id) in self
                .region_ranges
                .range((Excluded(start.clone()), Unbounded::<Vec<u8>>))
            {
                let region = &self.regions[id];
                if &keys::enc_start_key(region) <= end {
                    ids.insert(*id);
                } else {
                    // `region_ranges` is promised to have no overlap.
                    break;
                }
            }
        }
        if !ids.is_empty() {
            warn!(
                "detected damaged regions overlapping damaged file ranges";
                "id" => ?&ids,
            );
        }
        ids
    }

    fn overlap_damaged_range(&self, start_key: &Vec<u8>, end_key: &Vec<u8>) -> bool {
        for (_fname, (file_start, file_end)) in self.damaged_ranges.iter() {
            if file_end >= start_key && file_start < end_key {
                return true;
            }
        }
        false
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
        msg: RaftMessage,
    ) -> std::result::Result<(), TrySendError<RaftMessage>> {
        fail_point!("send_raft_message_full", |_| Err(TrySendError::Full(
            RaftMessage::default()
        )));

        let id = msg.get_region_id();

        let mut heap_size = 0;
        for e in msg.get_message().get_entries() {
            heap_size += bytes_capacity(&e.data) + bytes_capacity(&e.context);
        }
        let peer_msg = PeerMsg::RaftMessage(InspectedRaftMessage { heap_size, msg });
        let event = TraceEvent::Add(heap_size);
        let send_failed = Cell::new(true);

        MEMTRACE_RAFT_MESSAGES.trace(event);
        defer!(if send_failed.get() {
            MEMTRACE_RAFT_MESSAGES.trace(TraceEvent::Sub(heap_size));
        });

        let store_msg = match self.try_send(id, peer_msg) {
            Either::Left(Ok(())) => {
                fail_point!("memtrace_raft_messages_overflow_check_send");
                send_failed.set(false);
                return Ok(());
            }
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(im)))) => {
                return Err(TrySendError::Full(im.msg));
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(im)))) => {
                return Err(TrySendError::Disconnected(im.msg));
            }
            Either::Right(PeerMsg::RaftMessage(im)) => StoreMsg::RaftMessage(im),
            _ => unreachable!(),
        };
        match self.send_control(store_msg) {
            Ok(()) => {
                send_failed.set(false);
                Ok(())
            }
            Err(TrySendError::Full(StoreMsg::RaftMessage(im))) => Err(TrySendError::Full(im.msg)),
            Err(TrySendError::Disconnected(StoreMsg::RaftMessage(im))) => {
                Err(TrySendError::Disconnected(im.msg))
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

    pub fn register(&self, region_id: u64, mailbox: BasicMailbox<PeerFsm<EK, ER>>) {
        self.router.register(region_id, mailbox);
        self.update_trace();
    }

    pub fn register_all(&self, mailboxes: Vec<(u64, BasicMailbox<PeerFsm<EK, ER>>)>) {
        self.router.register_all(mailboxes);
        self.update_trace();
    }

    pub fn close(&self, region_id: u64) {
        self.router.close(region_id);
        self.update_trace();
    }

    pub fn clear_cache(&self) {
        self.router.clear_cache();
    }

    fn update_trace(&self) {
        let router_trace = self.router.trace();
        MEMTRACE_RAFT_ROUTER_ALIVE.trace(TraceEvent::Reset(router_trace.alive));
        MEMTRACE_RAFT_ROUTER_LEAK.trace(TraceEvent::Reset(router_trace.leak));
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
    pub cfg: Config,
    pub store: metapb::Store,
    pub pd_scheduler: Scheduler<PdTask<EK, ER>>,
    pub consistency_check_scheduler: Scheduler<ConsistencyCheckTask<EK::Snapshot>>,
    pub split_check_scheduler: Scheduler<SplitCheckTask>,
    // handle Compact, CleanupSst task
    pub cleanup_scheduler: Scheduler<CleanupTask>,
    pub raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    pub raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
    pub region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    pub apply_router: ApplyRouter<EK>,
    pub router: RaftRouter<EK, ER>,
    pub importer: Arc<SstImporter>,
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
    pub coprocessor_host: CoprocessorHost<EK>,
    pub timer: SteadyTimer,
    pub trans: T,
    /// WARNING:
    /// To avoid deadlock, if you want to use `store_meta` and `global_replication_state` together,
    /// the lock sequence MUST BE:
    /// 1. lock the store_meta.
    /// 2. lock the global_replication_state.
    pub global_replication_state: Arc<Mutex<GlobalReplicationState>>,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub engines: Engines<EK, ER>,
    pub pending_count: usize,
    pub ready_count: usize,
    pub has_ready: bool,
    pub current_time: Option<Timespec>,
    pub perf_context: EK::PerfContext,
    pub tick_batch: Vec<PeerTickBatch>,
    pub node_start_time: Option<TiInstant>,
    /// Disk usage for the store itself.
    pub self_disk_usage: DiskUsage,

    // TODO: how to remove offlined stores?
    /// Disk usage for other stores. The store itself is not included.
    /// Only contains items which is not `DiskUsage::Normal`.
    pub store_disk_usages: HashMap<u64, DiskUsage>,
    pub write_senders: Vec<Sender<WriteMsg<EK, ER>>>,
    pub sync_write_worker: Option<WriteWorker<EK, ER, RaftRouter<EK, ER>, T>>,
    pub io_reschedule_concurrent_count: Arc<AtomicUsize>,
    pub pending_latency_inspect: Vec<util::LatencyInspector>,
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
        self.tick_batch[PeerTick::Raft as usize].wait_duration = self.cfg.raft_base_tick_interval.0;
        self.tick_batch[PeerTick::RaftLogGc as usize].wait_duration =
            self.cfg.raft_log_gc_tick_interval.0;
        self.tick_batch[PeerTick::EntryCacheEvict as usize].wait_duration =
            ENTRY_CACHE_EVICT_TICK_DURATION;
        self.tick_batch[PeerTick::PdHeartbeat as usize].wait_duration =
            self.cfg.pd_heartbeat_tick_interval.0;
        self.tick_batch[PeerTick::SplitRegionCheck as usize].wait_duration =
            self.cfg.split_region_check_tick_interval.0;
        self.tick_batch[PeerTick::CheckPeerStaleState as usize].wait_duration =
            self.cfg.peer_stale_state_check_interval.0;
        self.tick_batch[PeerTick::CheckMerge as usize].wait_duration =
            self.cfg.merge_check_tick_interval.0;
        self.tick_batch[PeerTick::CheckLeaderLease as usize].wait_duration =
            self.cfg.check_leader_lease_interval.0;
        self.tick_batch[PeerTick::ReactivateMemoryLock as usize].wait_duration =
            self.cfg.reactive_memory_lock_tick_interval.0;
        self.tick_batch[PeerTick::ReportBuckets as usize].wait_duration =
            self.cfg.report_region_buckets_tick_interval.0;
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
            StoreTick::CleanupImportSst => self.on_cleanup_import_sst_tick(),
        }
        let elapsed = t.saturating_elapsed();
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
                StoreMsg::ValidateSstResult { invalid_ssts } => {
                    self.on_validate_sst_result(invalid_ssts)
                }
                StoreMsg::ClearRegionSizeInRange { start_key, end_key } => {
                    self.clear_region_size_in_range(&start_key, &end_key)
                }
                StoreMsg::StoreUnreachable { store_id } => {
                    self.on_store_unreachable(store_id);
                }
                StoreMsg::Start { store } => self.start(store),
                StoreMsg::UpdateReplicationMode(status) => self.on_update_replication_mode(status),
                #[cfg(any(test, feature = "testexport"))]
                StoreMsg::Validate(f) => f(&self.ctx.cfg),
                StoreMsg::LatencyInspect {
                    send_time,
                    mut inspector,
                } => {
                    inspector.record_store_wait(send_time.saturating_elapsed());
                    self.ctx.pending_latency_inspect.push(inspector);
                }
                StoreMsg::UnsafeRecoveryReport(report) => self.store_heartbeat_pd(Some(report)),
                StoreMsg::UnsafeRecoveryCreatePeer { syncer, create } => {
                    self.on_unsafe_recovery_create_peer(create);
                    drop(syncer);
                }
                StoreMsg::GcSnapshotFinish => self.register_snap_mgr_gc_tick(),
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

pub struct RaftPoller<EK: KvEngine + 'static, ER: RaftEngine + 'static, T: 'static> {
    tag: String,
    store_msg_buf: Vec<StoreMsg<EK>>,
    peer_msg_buf: Vec<PeerMsg<EK>>,
    previous_metrics: RaftReadyMetrics,
    timer: TiInstant,
    poll_ctx: PollContext<EK, ER, T>,
    messages_per_tick: usize,
    cfg_tracker: Tracker<Config>,
    trace_event: TraceEvent,
    last_flush_time: TiInstant,
    need_flush_events: bool,
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> RaftPoller<EK, ER, T> {
    fn flush_events(&mut self) {
        self.flush_ticks();
        self.poll_ctx.raft_metrics.flush();
        self.poll_ctx.store_stat.flush();

        MEMTRACE_PEERS.trace(mem::take(&mut self.trace_event));
    }

    fn flush_ticks(&mut self) {
        for t in PeerTick::get_all_ticks() {
            let idx = *t as usize;
            if self.poll_ctx.tick_batch[idx].ticks.is_empty() {
                continue;
            }
            let peer_ticks = mem::take(&mut self.poll_ctx.tick_batch[idx].ticks);
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
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a BatchSystemConfig),
    {
        self.previous_metrics = self.poll_ctx.raft_metrics.ready.clone();
        self.poll_ctx.pending_count = 0;
        self.poll_ctx.ready_count = 0;
        self.poll_ctx.has_ready = false;
        self.poll_ctx.self_disk_usage = get_disk_status(self.poll_ctx.store.get_id());
        self.timer = TiInstant::now();
        // update config
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
            self.poll_ctx
                .snap_mgr
                .set_max_per_file_size(incoming.max_snapshot_file_raw_size.0);
            self.poll_ctx.cfg = incoming.clone();
            self.poll_ctx.raft_metrics.waterfall_metrics = self.poll_ctx.cfg.waterfall_metrics;
            self.poll_ctx.update_ticks_timeout();
            update_cfg(&incoming.store_batch_system);
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

    fn handle_normal(
        &mut self,
        peer: &mut impl DerefMut<Target = PeerFsm<EK, ER>>,
    ) -> HandleResult {
        let mut handle_result = HandleResult::KeepProcessing;

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
                    handle_result = HandleResult::stop_at(0, false);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    peer.stop();
                    handle_result = HandleResult::stop_at(0, false);
                    break;
                }
            }
        }

        let mut delegate = PeerFsmDelegate::new(peer, &mut self.poll_ctx);
        delegate.handle_msgs(&mut self.peer_msg_buf);
        // No readiness is generated and using sync write, skipping calling ready and release early.
        if !delegate.collect_ready() && self.poll_ctx.sync_write_worker.is_some() {
            if let HandleResult::StopAt { skip_end, .. } = &mut handle_result {
                *skip_end = true;
            }
        }

        handle_result
    }

    fn light_end(&mut self, peers: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {
        for peer in peers.iter_mut().flatten() {
            peer.update_memory_trace(&mut self.trace_event);
        }

        if let Some(write_worker) = &mut self.poll_ctx.sync_write_worker {
            if self.poll_ctx.trans.need_flush() && !write_worker.is_empty() {
                self.poll_ctx.trans.flush();
            }

            self.flush_events();
        } else {
            let now = TiInstant::now();

            if self.poll_ctx.trans.need_flush() {
                self.poll_ctx.trans.flush();
            }

            if now.saturating_duration_since(self.last_flush_time) >= Duration::from_millis(1) {
                self.last_flush_time = now;
                self.need_flush_events = false;
                self.flush_events();
            } else {
                self.need_flush_events = true;
            }
        }
    }

    fn end(&mut self, peers: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {
        if self.poll_ctx.has_ready {
            // Only enable the fail point when the store id is equal to 3, which is
            // the id of slow store in tests.
            fail_point!("on_raft_ready", self.poll_ctx.store_id() == 3, |_| {});
        }
        let mut latency_inspect = std::mem::take(&mut self.poll_ctx.pending_latency_inspect);
        let mut dur = self.timer.saturating_elapsed();

        for inspector in &mut latency_inspect {
            inspector.record_store_process(dur);
        }
        let write_begin = TiInstant::now();
        if let Some(write_worker) = &mut self.poll_ctx.sync_write_worker {
            if self.poll_ctx.has_ready {
                write_worker.write_to_db(false);

                for mut inspector in latency_inspect {
                    inspector.record_store_write(write_begin.saturating_elapsed());
                    inspector.finish();
                }

                for peer in peers.iter_mut().flatten() {
                    PeerFsmDelegate::new(peer, &mut self.poll_ctx).post_raft_ready_append();
                }
            } else {
                for inspector in latency_inspect {
                    inspector.finish();
                }
            }
        } else {
            let writer_id = rand::random::<usize>() % self.poll_ctx.cfg.store_io_pool_size;
            if let Err(err) =
                self.poll_ctx.write_senders[writer_id].try_send(WriteMsg::LatencyInspect {
                    send_time: write_begin,
                    inspector: latency_inspect,
                })
            {
                warn!("send latency inspecting to write workers failed"; "err" => ?err);
            }
        }
        dur = self.timer.saturating_elapsed();
        if self.poll_ctx.has_ready {
            if !self.poll_ctx.store_stat.is_busy {
                let election_timeout = Duration::from_millis(
                    self.poll_ctx.cfg.raft_base_tick_interval.as_millis()
                        * self.poll_ctx.cfg.raft_election_timeout_ticks as u64,
                );
                if dur >= election_timeout {
                    self.poll_ctx.store_stat.is_busy = true;
                }
            }

            slow_log!(
                dur,
                "{} handle {} pending peers include {} ready, {} entries, {} messages and {} \
                 snapshots",
                self.tag,
                self.poll_ctx.pending_count,
                self.poll_ctx.ready_count,
                self.poll_ctx
                    .raft_metrics
                    .ready
                    .append
                    .saturating_sub(self.previous_metrics.append),
                self.poll_ctx
                    .raft_metrics
                    .ready
                    .message
                    .saturating_sub(self.previous_metrics.message),
                self.poll_ctx
                    .raft_metrics
                    .ready
                    .snapshot
                    .saturating_sub(self.previous_metrics.snapshot),
            );
        }

        self.poll_ctx.current_time = None;
        self.poll_ctx
            .raft_metrics
            .process_ready
            .observe(duration_to_sec(dur));
    }

    fn pause(&mut self) {
        if self.poll_ctx.sync_write_worker.is_some() {
            if self.poll_ctx.trans.need_flush() {
                self.poll_ctx.trans.flush();
            }
        } else {
            if self.poll_ctx.trans.need_flush() {
                self.poll_ctx.trans.flush();
            }
            if self.need_flush_events {
                self.last_flush_time = TiInstant::now();
                self.need_flush_events = false;
                self.flush_events();
            }
        }
    }
}

pub struct RaftPollerBuilder<EK: KvEngine, ER: RaftEngine, T> {
    pub cfg: Arc<VersionTrack<Config>>,
    pub store: metapb::Store,
    pd_scheduler: Scheduler<PdTask<EK, ER>>,
    consistency_check_scheduler: Scheduler<ConsistencyCheckTask<EK::Snapshot>>,
    split_check_scheduler: Scheduler<SplitCheckTask>,
    cleanup_scheduler: Scheduler<CleanupTask>,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    raftlog_fetch_scheduler: Scheduler<RaftlogFetchTask>,
    pub region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    apply_router: ApplyRouter<EK>,
    pub router: RaftRouter<EK, ER>,
    pub importer: Arc<SstImporter>,
    pub store_meta: Arc<Mutex<StoreMeta>>,
    pub pending_create_peers: Arc<Mutex<HashMap<u64, (u64, bool)>>>,
    snap_mgr: SnapManager,
    pub coprocessor_host: CoprocessorHost<EK>,
    trans: T,
    global_stat: GlobalStoreStat,
    pub engines: Engines<EK, ER>,
    global_replication_state: Arc<Mutex<GlobalReplicationState>>,
    feature_gate: FeatureGate,
    write_senders: Vec<Sender<WriteMsg<EK, ER>>>,
    io_reschedule_concurrent_count: Arc<AtomicUsize>,
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

        let t = TiInstant::now();
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
                self.raftlog_fetch_scheduler.clone(),
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
            meta.region_read_progress
                .insert(region_id, peer.peer.read_progress.clone());
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
                self.raftlog_fetch_scheduler.clone(),
                self.engines.clone(),
                &region,
            )?;
            peer.peer.init_replication_mode(&mut *replication_state);
            peer.schedule_applying_snapshot();
            meta.region_ranges
                .insert(enc_end_key(&region), region.get_id());
            meta.region_read_progress
                .insert(region.get_id(), peer.peer.read_progress.clone());
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
            "takes" => ?t.saturating_elapsed(),
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
        peer_storage::clear_meta(&self.engines, kv_wb, raft_wb, rid, 0, &raft_state).unwrap();
        let key = keys::region_state_key(rid);
        kv_wb.put_msg_cf(CF_RAFT, &key, origin_state).unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&self, meta: &StoreMeta) -> Result<()> {
        let t = TiInstant::now();

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
            "takes" => ?t.saturating_elapsed()
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

    fn build(&mut self, _: Priority) -> RaftPoller<EK, ER, T> {
        let sync_write_worker = if self.write_senders.is_empty() {
            let (_, rx) = unbounded();
            Some(WriteWorker::new(
                self.store.get_id(),
                "sync-writer".to_string(),
                self.engines.clone(),
                rx,
                self.router.clone(),
                self.trans.clone(),
                &self.cfg,
            ))
        } else {
            None
        };
        let mut ctx = PollContext {
            cfg: self.cfg.value().clone(),
            store: self.store.clone(),
            pd_scheduler: self.pd_scheduler.clone(),
            consistency_check_scheduler: self.consistency_check_scheduler.clone(),
            split_check_scheduler: self.split_check_scheduler.clone(),
            region_scheduler: self.region_scheduler.clone(),
            apply_router: self.apply_router.clone(),
            router: self.router.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            raftlog_fetch_scheduler: self.raftlog_fetch_scheduler.clone(),
            raftlog_gc_scheduler: self.raftlog_gc_scheduler.clone(),
            importer: self.importer.clone(),
            store_meta: self.store_meta.clone(),
            pending_create_peers: self.pending_create_peers.clone(),
            raft_metrics: RaftMetrics::new(self.cfg.value().waterfall_metrics),
            snap_mgr: self.snap_mgr.clone(),
            coprocessor_host: self.coprocessor_host.clone(),
            timer: SteadyTimer::default(),
            trans: self.trans.clone(),
            global_replication_state: self.global_replication_state.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            engines: self.engines.clone(),
            pending_count: 0,
            ready_count: 0,
            has_ready: false,
            current_time: None,
            perf_context: self
                .engines
                .kv
                .get_perf_context(self.cfg.value().perf_level, PerfContextKind::RaftstoreStore),
            tick_batch: vec![PeerTickBatch::default(); PeerTick::VARIANT_COUNT],
            node_start_time: Some(TiInstant::now_coarse()),
            feature_gate: self.feature_gate.clone(),
            self_disk_usage: DiskUsage::Normal,
            store_disk_usages: Default::default(),
            write_senders: self.write_senders.clone(),
            sync_write_worker,
            io_reschedule_concurrent_count: self.io_reschedule_concurrent_count.clone(),
            pending_latency_inspect: vec![],
        };
        ctx.update_ticks_timeout();
        let tag = format!("[store {}]", ctx.store.get_id());
        RaftPoller {
            tag: tag.clone(),
            store_msg_buf: Vec::with_capacity(ctx.cfg.messages_per_tick),
            peer_msg_buf: Vec::with_capacity(ctx.cfg.messages_per_tick),
            previous_metrics: ctx.raft_metrics.ready.clone(),
            timer: TiInstant::now(),
            messages_per_tick: ctx.cfg.messages_per_tick,
            poll_ctx: ctx,
            cfg_tracker: self.cfg.clone().tracker(tag),
            trace_event: TraceEvent::default(),
            last_flush_time: TiInstant::now(),
            need_flush_events: false,
        }
    }
}

impl<EK, ER, T> Clone for RaftPollerBuilder<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Clone,
{
    fn clone(&self) -> Self {
        RaftPollerBuilder {
            cfg: self.cfg.clone(),
            store: self.store.clone(),
            pd_scheduler: self.pd_scheduler.clone(),
            consistency_check_scheduler: self.consistency_check_scheduler.clone(),
            split_check_scheduler: self.split_check_scheduler.clone(),
            cleanup_scheduler: self.cleanup_scheduler.clone(),
            raftlog_gc_scheduler: self.raftlog_gc_scheduler.clone(),
            raftlog_fetch_scheduler: self.raftlog_fetch_scheduler.clone(),
            region_scheduler: self.region_scheduler.clone(),
            apply_router: self.apply_router.clone(),
            router: self.router.clone(),
            importer: self.importer.clone(),
            store_meta: self.store_meta.clone(),
            pending_create_peers: self.pending_create_peers.clone(),
            snap_mgr: self.snap_mgr.clone(),
            coprocessor_host: self.coprocessor_host.clone(),
            trans: self.trans.clone(),
            global_stat: self.global_stat.clone(),
            engines: self.engines.clone(),
            global_replication_state: self.global_replication_state.clone(),
            feature_gate: self.feature_gate.clone(),
            write_senders: self.write_senders.clone(),
            io_reschedule_concurrent_count: self.io_reschedule_concurrent_count.clone(),
        }
    }
}

struct Workers<EK: KvEngine, ER: RaftEngine> {
    pd_worker: LazyWorker<PdTask<EK, ER>>,
    background_worker: Worker,

    // Both of cleanup tasks and region tasks get their own workers, instead of reusing
    // background_workers. This is because the underlying compact_range call is a
    // blocking operation, which can take an extensive amount of time.
    cleanup_worker: Worker,
    region_worker: Worker,
    // Used for calling `purge_expired_files`, which can be time-consuming for certain
    // engine implementations.
    purge_worker: Worker,

    raftlog_fetch_worker: Worker,

    coprocessor_host: CoprocessorHost<EK>,

    refresh_config_worker: LazyWorker<RefreshConfigTask>,
}

pub struct RaftBatchSystem<EK: KvEngine, ER: RaftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm<EK>>,
    apply_router: ApplyRouter<EK>,
    apply_system: ApplyBatchSystem<EK>,
    router: RaftRouter<EK, ER>,
    workers: Option<Workers<EK, ER>>,
    store_writers: StoreWriters<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> RaftBatchSystem<EK, ER> {
    pub fn router(&self) -> RaftRouter<EK, ER> {
        self.router.clone()
    }

    pub fn apply_router(&self) -> ApplyRouter<EK> {
        self.apply_router.clone()
    }

    pub fn refresh_config_scheduler(&mut self) -> Scheduler<RefreshConfigTask> {
        assert!(self.workers.is_some());
        self.workers
            .as_ref()
            .unwrap()
            .refresh_config_worker
            .scheduler()
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
        pd_worker: LazyWorker<PdTask<EK, ER>>,
        store_meta: Arc<Mutex<StoreMeta>>,
        mut coprocessor_host: CoprocessorHost<EK>,
        importer: Arc<SstImporter>,
        split_check_scheduler: Scheduler<SplitCheckTask>,
        background_worker: Worker,
        auto_split_controller: AutoSplitController,
        global_replication_state: Arc<Mutex<GlobalReplicationState>>,
        concurrency_manager: ConcurrencyManager,
        collector_reg_handle: CollectorRegHandle,
        health_service: Option<HealthService>,
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
            purge_worker: Worker::new("purge-worker"),
            raftlog_fetch_worker: Worker::new("raftlog-fetch-worker"),
            coprocessor_host: coprocessor_host.clone(),
            refresh_config_worker: LazyWorker::new("refreash-config-worker"),
        };
        mgr.init()?;
        let region_runner = RegionRunner::new(
            engines.kv.clone(),
            mgr.clone(),
            cfg.value().snap_apply_batch_size.0 as usize,
            cfg.value().use_delete_range,
            cfg.value().snap_generator_pool_size,
            workers.coprocessor_host.clone(),
            self.router(),
            Some(Arc::clone(&pd_client)),
        );
        let region_scheduler = workers
            .region_worker
            .start_with_timer("snapshot-worker", region_runner);

        let raftlog_gc_runner = RaftlogGcRunner::new(
            engines.clone(),
            cfg.value().raft_log_compact_sync_interval.0,
        );
        let raftlog_gc_scheduler = workers
            .background_worker
            .start_with_timer("raft-gc-worker", raftlog_gc_runner);
        let router_clone = self.router();
        let engines_clone = engines.clone();
        workers.purge_worker.spawn_interval_task(
            cfg.value().raft_engine_purge_interval.0,
            move || {
                match engines_clone.raft.purge_expired_files() {
                    Ok(regions) => {
                        for region_id in regions {
                            let _ = router_clone.send(
                                region_id,
                                PeerMsg::CasualMessage(CasualMessage::ForceCompactRaftLogs),
                            );
                        }
                    }
                    Err(e) => {
                        warn!("purge expired files"; "err" => %e);
                    }
                };
            },
        );

        let raftlog_fetch_scheduler = workers.raftlog_fetch_worker.start(
            "raftlog-fetch-worker",
            RaftlogFetchRunner::new(self.router.clone(), engines.raft.clone()),
        );

        let compact_runner = CompactRunner::new(engines.kv.clone());
        let cleanup_sst_runner = CleanupSstRunner::new(
            meta.get_id(),
            self.router.clone(),
            Arc::clone(&importer),
            Arc::clone(&pd_client),
        );
        let gc_snapshot_runner = GcSnapshotRunner::new(
            meta.get_id(),
            self.router.clone(), // RaftRouter
            mgr.clone(),
        );
        let cleanup_runner =
            CleanupRunner::new(compact_runner, cleanup_sst_runner, gc_snapshot_runner);
        let cleanup_scheduler = workers
            .cleanup_worker
            .start("cleanup-worker", cleanup_runner);
        let consistency_check_runner =
            ConsistencyCheckRunner::<EK, _>::new(self.router.clone(), coprocessor_host.clone());
        let consistency_check_scheduler = workers
            .background_worker
            .start("consistency-check", consistency_check_runner);

        self.store_writers
            .spawn(meta.get_id(), &engines, &self.router, &trans, &cfg)?;

        let region_read_progress = store_meta.lock().unwrap().region_read_progress.clone();
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
            raftlog_fetch_scheduler,
            apply_router: self.apply_router.clone(),
            trans,
            coprocessor_host,
            importer,
            snap_mgr: mgr.clone(),
            global_replication_state,
            global_stat: GlobalStoreStat::default(),
            store_meta,
            pending_create_peers: Arc::new(Mutex::new(HashMap::default())),
            feature_gate: pd_client.feature_gate().clone(),
            write_senders: self.store_writers.senders().clone(),
            io_reschedule_concurrent_count: Arc::new(AtomicUsize::new(0)),
        };
        let region_peers = builder.init()?;
        self.start_system::<T, C>(
            workers,
            region_peers,
            builder,
            auto_split_controller,
            concurrency_manager,
            mgr,
            pd_client,
            collector_reg_handle,
            region_read_progress,
            health_service,
        )?;
        Ok(())
    }

    fn start_system<T: Transport + 'static, C: PdClient + 'static>(
        &mut self,
        mut workers: Workers<EK, ER>,
        region_peers: Vec<SenderFsmPair<EK, ER>>,
        builder: RaftPollerBuilder<EK, ER, T>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
        snap_mgr: SnapManager,
        pd_client: Arc<C>,
        collector_reg_handle: CollectorRegHandle,
        region_read_progress: RegionReadProgressRegistry,
        health_service: Option<HealthService>,
    ) -> Result<()> {
        let cfg = builder.cfg.value().clone();
        let store = builder.store.clone();

        let apply_poller_builder = ApplyPollerBuilder::<EK>::new(
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

        let (raft_builder, apply_builder) = (builder.clone(), apply_poller_builder.clone());

        let tag = format!("raftstore-{}", store.get_id());
        self.system.spawn(tag, builder);
        let mut mailboxes = Vec::with_capacity(region_peers.len());
        let mut address = Vec::with_capacity(region_peers.len());
        for (tx, fsm) in region_peers {
            address.push(fsm.region_id());
            mailboxes.push((
                fsm.region_id(),
                BasicMailbox::new(tx, fsm, self.router.state_cnt().clone()),
            ));
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

        let refresh_config_runner = RefreshConfigRunner::new(
            self.apply_router.router.clone(),
            self.router.router.clone(),
            self.apply_system.build_pool_state(apply_builder),
            self.system.build_pool_state(raft_builder),
        );
        assert!(workers.refresh_config_worker.start(refresh_config_runner));

        let pd_runner = PdRunner::new(
            &cfg,
            store.get_id(),
            Arc::clone(&pd_client),
            self.router.clone(),
            workers.pd_worker.scheduler(),
            cfg.pd_store_heartbeat_tick_interval.0,
            auto_split_controller,
            concurrency_manager,
            snap_mgr,
            workers.pd_worker.remote(),
            collector_reg_handle,
            region_read_progress,
            health_service,
        );
        assert!(workers.pd_worker.start_with_timer(pd_runner));

        if let Err(e) = sys_util::thread::set_priority(sys_util::HIGH_PRI) {
            warn!("set thread priority for raftstore failed"; "error" => ?e);
        }
        self.workers = Some(workers);
        // This router will not be accessed again, free all caches.
        self.router.clear_cache();
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        let mut workers = self.workers.take().unwrap();
        // Wait all workers finish.
        workers.pd_worker.stop();

        self.apply_system.shutdown();
        MEMTRACE_APPLY_ROUTER_ALIVE.trace(TraceEvent::Reset(0));
        MEMTRACE_APPLY_ROUTER_LEAK.trace(TraceEvent::Reset(0));

        fail_point!("after_shutdown_apply");

        self.system.shutdown();
        self.store_writers.shutdown();
        MEMTRACE_RAFT_ROUTER_ALIVE.trace(TraceEvent::Reset(0));
        MEMTRACE_RAFT_ROUTER_LEAK.trace(TraceEvent::Reset(0));

        workers.coprocessor_host.shutdown();
        workers.cleanup_worker.stop();
        workers.region_worker.stop();
        workers.background_worker.stop();
        workers.purge_worker.stop();
        workers.refresh_config_worker.stop();
        workers.raftlog_fetch_worker.stop();
    }
}

pub fn create_raft_batch_system<EK: KvEngine, ER: RaftEngine>(
    cfg: &Config,
) -> (RaftRouter<EK, ER>, RaftBatchSystem<EK, ER>) {
    let (store_tx, store_fsm) = StoreFsm::new(cfg);
    let (apply_router, apply_system) = create_apply_batch_system(cfg);
    let (router, system) =
        batch_system::create_system(&cfg.store_batch_system, store_tx, store_fsm);
    let raft_router = RaftRouter { router };
    let system = RaftBatchSystem {
        system,
        workers: None,
        apply_router,
        apply_system,
        router: raft_router.clone(),
        store_writers: StoreWriters::new(),
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
                if peer.get_id() < msg.get_from_peer().get_id() {
                    panic!(
                        "peer id increased after region is merged, message peer id {}, local peer id {}, region {:?}",
                        msg.get_from_peer().get_id(),
                        peer.get_id(),
                        region
                    );
                }
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

    fn on_raft_message(&mut self, msg: InspectedRaftMessage) -> Result<()> {
        let (heap_size, forwarded) = (msg.heap_size, Cell::new(false));
        defer!(if !forwarded.get() {
            MEMTRACE_RAFT_MESSAGES.trace(TraceEvent::Sub(heap_size));
        });

        let region_id = msg.msg.get_region_id();
        let msg = match self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg)) {
            Ok(()) => {
                forwarded.set(true);
                return Ok(());
            }
            Err(TrySendError::Full(_)) => return Ok(()),
            Err(TrySendError::Disconnected(_)) if self.ctx.router.is_shutdown() => return Ok(()),
            Err(TrySendError::Disconnected(PeerMsg::RaftMessage(im))) => im.msg,
            Err(_) => unreachable!(),
        };

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
                    // Peer created, send the message again.
                    let peer_msg = PeerMsg::RaftMessage(InspectedRaftMessage { heap_size, msg });
                    if self.ctx.router.send(region_id, peer_msg).is_ok() {
                        forwarded.set(true);
                    }
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
                let peer_msg = PeerMsg::RaftMessage(InspectedRaftMessage { heap_size, msg });
                if let Err(e) = self.ctx.router.force_send(region_id, peer_msg) {
                    warn!("handle first request failed"; "region_id" => region_id, "error" => ?e);
                } else {
                    forwarded.set(true);
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

        let res = self.maybe_create_peer_internal(region_id, msg, is_local_first);
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
        fail_point!("after_acquire_store_meta_on_maybe_create_peer_internal");

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

        if meta.overlap_damaged_range(
            &data_key(msg.get_start_key()),
            &data_end_key(msg.get_end_key()),
        ) {
            warn!(
                "Damaged region overlapped and reject to create peer";
                "peer_id" => ?target,
                "region_id" => &region_id,
            );
            return Ok(false);
        }

        let mut is_overlapped = false;
        let mut regions_to_destroy = vec![];
        for (key, id) in meta.region_ranges.range((
            Excluded(data_key(msg.get_start_key())),
            Unbounded::<Vec<u8>>,
        )) {
            let exist_region = match meta.regions.get(id) {
                Some(r) => r,
                None => panic!(
                    "meta corrupted: no region for {} {} when creating {} {:?}",
                    id,
                    log_wrappers::Value::key(key),
                    region_id,
                    msg,
                ),
            };
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
                        panic!(
                            "something is wrong, maybe PD do not ensure all target peers exist before merging"
                        );
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
            self.ctx.raftlog_fetch_scheduler.clone(),
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
        meta.region_read_progress
            .insert(region_id, peer.peer.read_progress.clone());

        let mailbox = BasicMailbox::new(tx, peer, self.ctx.router.state_cnt().clone());
        self.ctx.router.register(region_id, mailbox);
        self.ctx
            .router
            .force_send(region_id, PeerMsg::Start)
            .unwrap();
        Ok(true)
    }

    fn on_compaction_finished(&mut self, event: EK::CompactedEvent) {
        if event.is_size_declining_trivial(self.ctx.cfg.region_split_check_diff().0) {
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
                self.ctx.cfg.region_split_check_diff().0 / 16,
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

    fn store_heartbeat_pd(&mut self, report: Option<pdpb::StoreReport>) {
        let mut stats = StoreStats::default();

        stats.set_store_id(self.ctx.store_id());
        {
            let meta = self.ctx.store_meta.lock().unwrap();
            stats.set_region_count(meta.regions.len() as u32);

            if !meta.damaged_ranges.is_empty() {
                let damaged_regions_id = meta.get_all_damaged_region_ids().into_iter().collect();
                stats.set_damaged_regions_id(damaged_regions_id);
            }
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

        let mut query_stats = QueryStats::default();
        query_stats.set_put(
            self.ctx
                .global_stat
                .stat
                .engine_total_query_put
                .swap(0, Ordering::SeqCst),
        );
        query_stats.set_delete(
            self.ctx
                .global_stat
                .stat
                .engine_total_query_delete
                .swap(0, Ordering::SeqCst),
        );
        query_stats.set_delete_range(
            self.ctx
                .global_stat
                .stat
                .engine_total_query_delete_range
                .swap(0, Ordering::SeqCst),
        );
        stats.set_query_stats(query_stats);

        let store_info = StoreInfo {
            kv_engine: self.ctx.engines.kv.clone(),
            raft_engine: self.ctx.engines.raft.clone(),
            capacity: self.ctx.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat {
            stats,
            store_info,
            report,
            dr_autosync_status: self
                .ctx
                .global_replication_state
                .lock()
                .unwrap()
                .store_dr_autosync_status(),
        };
        if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
            error!("notify pd failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e
            );
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd(None);
        self.register_pd_store_heartbeat_tick();
    }

    fn on_snap_mgr_gc(&mut self) {
        // refresh multi_snapshot_files enable flag
        self.ctx.snap_mgr.set_enable_multi_snapshot_files(
            self.ctx
                .feature_gate
                .can_enable(MULTI_FILES_SNAPSHOT_FEATURE),
        );

        if let Err(e) = self
            .ctx
            .cleanup_scheduler
            .schedule(CleanupTask::GcSnapshot(GcSnapshotTask::GcSnapshot))
        {
            error!(
                "schedule to delete ssts failed";
                "store_id" => self.fsm.store.id,
                "err" => ?e,
            );
        }
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
        if ssts.is_empty() || self.ctx.importer.get_mode() == SwitchMode::Import {
            return;
        }
        // A stale peer can still ingest a stale Sst before it is
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

        let task = CleanupSstTask::DeleteSst { ssts: delete_ssts };
        if let Err(e) = self
            .ctx
            .cleanup_scheduler
            .schedule(CleanupTask::CleanupSst(task))
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
            let task = CleanupSstTask::DeleteSst { ssts: delete_ssts };
            if let Err(e) = self
                .ctx
                .cleanup_scheduler
                .schedule(CleanupTask::CleanupSst(task))
            {
                error!(
                    "schedule to delete ssts failed";
                    "store_id" => self.fsm.store.id,
                    "err" => ?e
                );
            }
        }

        // When there is an import job running, the region which this sst belongs may has not been
        //  split from the origin region because the apply thread is so busy that it can not apply
        //  SplitRequest as soon as possible. So we can not delete this sst file.
        if !validate_ssts.is_empty() && self.ctx.importer.get_mode() != SwitchMode::Import {
            let task = CleanupSstTask::ValidateSst {
                ssts: validate_ssts,
            };
            if let Err(e) = self
                .ctx
                .cleanup_scheduler
                .schedule(CleanupTask::CleanupSst(task))
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
            StoreTick::CleanupImportSst,
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
            .map_or(UNREACHABLE_BACKOFF, |t| now.saturating_duration_since(*t))
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
        let store_allowlist = match status.get_mode() {
            ReplicationMode::DrAutoSync => {
                status.get_dr_auto_sync().get_available_stores().to_vec()
            }
            _ => vec![],
        };

        info!("updating replication mode"; "status" => ?status);
        state.set_status(status);
        drop(state);
        self.ctx.trans.set_store_allowlist(store_allowlist);
        self.ctx.router.report_status_update()
    }

    fn on_unsafe_recovery_create_peer(&self, region: Region) {
        info!("Unsafe recovery, creating a peer"; "peer" => ?region);
        let mut meta = self.ctx.store_meta.lock().unwrap();
        if let Some((_, id)) = meta
            .region_ranges
            .range((
                Excluded(data_key(region.get_start_key())),
                Unbounded::<Vec<u8>>,
            ))
            .next()
        {
            let exist_region = &meta.regions[id];
            if enc_start_key(exist_region) < data_end_key(region.get_end_key()) {
                if exist_region.get_id() == region.get_id() {
                    warn!(
                        "Unsafe recovery, region has already been created.";
                        "region" => ?region,
                        "exist_region" => ?exist_region,
                    );
                    return;
                } else {
                    error!(
                        "Unsafe recovery, region to be created overlaps with an existing region";
                        "region" => ?region,
                        "exist_region" => ?exist_region,
                    );
                    return;
                }
            }
        }
        let (sender, mut peer) = match PeerFsm::create(
            self.ctx.store.get_id(),
            &self.ctx.cfg,
            self.ctx.region_scheduler.clone(),
            self.ctx.raftlog_fetch_scheduler.clone(),
            self.ctx.engines.clone(),
            &region,
        ) {
            Ok((sender, peer)) => (sender, peer),
            Err(e) => {
                error!(
                    "Unsafe recovery, fail to create peer fsm";
                    "region" => ?region,
                    "err" => ?e,
                );
                return;
            }
        };
        let mut replication_state = self.ctx.global_replication_state.lock().unwrap();
        peer.peer.init_replication_mode(&mut *replication_state);
        drop(replication_state);
        peer.peer.activate(self.ctx);

        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        if meta
            .regions
            .insert(region.get_id(), region.clone())
            .is_some()
            || meta
                .region_ranges
                .insert(end_key.clone(), region.get_id())
                .is_some()
            || meta
                .readers
                .insert(region.get_id(), ReadDelegate::from_peer(peer.get_peer()))
                .is_some()
            || meta
                .region_read_progress
                .insert(region.get_id(), peer.peer.read_progress.clone())
                .is_some()
        {
            panic!(
                "Unsafe recovery, key conflicts while inserting region {:?} into store meta",
                region,
            );
        }
        drop(meta);

        if let Err(e) = self.ctx.engines.kv.delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &[Range::new(&start_key, &end_key)],
        ) {
            panic!(
                "Unsafe recovery, fail to clean up stale data while creating the new region {:?}, the error is {:?}",
                region, e,
            );
        }
        let mut kv_wb = self.ctx.engines.kv.write_batch();
        if let Err(e) = peer_storage::write_peer_state(&mut kv_wb, &region, PeerState::Normal, None)
        {
            panic!(
                "Unsafe recovery, fail to add peer state for {:?} into write batch, the error is {:?}",
                region, e,
            );
        }
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        if let Err(e) = kv_wb.write_opt(&write_opts) {
            panic!(
                "Unsafe recovery, fail to write to disk while creating peer {:?}, the error is {:?}",
                region, e,
            );
        }

        let mailbox = BasicMailbox::new(sender, peer, self.ctx.router.state_cnt().clone());
        self.ctx.router.register(region.get_id(), mailbox);
        self.ctx
            .router
            .force_send(region.get_id(), PeerMsg::Start)
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::{RangeOffsets, RangeProperties, RocksCompactedEvent};

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
