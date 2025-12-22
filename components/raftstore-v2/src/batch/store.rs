// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use batch_system::{
    BasicMailbox, BatchRouter, BatchSystem, HandleResult, HandlerBuilder, PollHandler,
};
use causal_ts::CausalTsProviderImpl;
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use crossbeam::channel::TrySendError;
use encryption_export::DataKeyManager;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry};
use file_system::{IoType, WithIoType, set_io_type};
use futures::compat::Future01CompatExt;
use health_controller::types::LatencyInspector;
use kvproto::{disk_usage::DiskUsage, raft_serverpb::RaftMessage};
use pd_client::PdClient;
use raft::{INVALID_ID, StateRole};
use raftstore::{
    coprocessor::{CoprocessorHost, RegionChangeEvent},
    store::{
        AutoSplitController, Config, ReadRunner, ReadTask, RefreshConfigTask, SplitCheckRunner,
        SplitCheckTask, StoreWriters, StoreWritersContext, TabletSnapManager, Transport,
        WriteRouterContext, WriteSenders, WriterContoller,
        fsm::{
            GlobalStoreStat, LocalStoreStat,
            store::{ENTRY_CACHE_EVICT_TICK_DURATION, PeerTickBatch},
        },
        local_metrics::RaftMetrics,
    },
};
use resource_control::ResourceController;
use resource_metering::CollectorRegHandle;
use service::service_manager::GrpcServiceManager;
use slog::{Logger, warn};
use sst_importer::SstImporter;
use tikv_util::{
    Either, box_err,
    config::{Tracker, VersionTrack},
    log::SlogFormat,
    sys::{SysQuota, disk::get_disk_status},
    time::{Instant as TiInstant, Limiter, duration_to_sec, monotonic_raw_now},
    timer::{GLOBAL_TIMER_HANDLE, SteadyTimer},
    worker::{Builder, LazyWorker, Scheduler, Worker},
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
};
use time::Timespec;

use crate::{
    Error, Result,
    fsm::{PeerFsm, PeerFsmDelegate, SenderFsmPair, StoreFsm, StoreFsmDelegate, StoreMeta},
    operation::{
        MERGE_IN_PROGRESS_PREFIX, MERGE_SOURCE_PREFIX, ReplayWatch, SPLIT_PREFIX, SharedReadTablet,
    },
    raft::Storage,
    router::{PeerMsg, PeerTick, StoreMsg},
    worker::{pd, refresh_config, tablet},
};

const MIN_MANUAL_FLUSH_RATE: f64 = 0.2;
const MAX_MANUAL_FLUSH_PERIOD: Duration = Duration::from_secs(120);

/// A per-thread context shared by the [`StoreFsm`] and multiple [`PeerFsm`]s.
pub struct StoreContext<EK: KvEngine, ER: RaftEngine, T> {
    /// A logger without any KV. It's clean for creating new PeerFSM.
    pub logger: Logger,
    pub store_id: u64,
    pub coprocessor_host: CoprocessorHost<EK>,
    /// The transport for sending messages to peers on other stores.
    pub trans: T,
    pub has_ready: bool,
    pub raft_metrics: RaftMetrics,
    /// The latest configuration.
    pub cfg: Config,
    pub router: StoreRouter<EK, ER>,
    /// The tick batch for delay ticking. It will be flushed at the end of every
    /// round.
    pub tick_batch: Vec<PeerTickBatch>,
    /// The precise timer for scheduling tick.
    pub timer: SteadyTimer,
    pub schedulers: Schedulers<EK, ER>,
    pub store_meta: Arc<Mutex<StoreMeta<EK>>>,
    pub shutdown: Arc<AtomicBool>,
    pub engine: ER,
    pub tablet_registry: TabletRegistry<EK>,
    pub apply_pool: FuturePool,
    /// A background pool used for high-priority works.
    pub high_priority_pool: FuturePool,

    /// current_time from monotonic_raw_now.
    pub current_time: Option<Timespec>,
    /// unsafe_vote_deadline from monotonic_raw_now.
    pub unsafe_vote_deadline: Option<Timespec>,

    /// Disk usage for the store itself.
    pub self_disk_usage: DiskUsage,
    // TODO: how to remove offlined stores?
    /// Disk usage for other stores. The store itself is not included.
    /// Only contains items which is not `DiskUsage::Normal`.
    pub store_disk_usages: HashMap<u64, DiskUsage>,

    pub snap_mgr: TabletSnapManager,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub sst_importer: Arc<SstImporter<EK>>,
    pub key_manager: Option<Arc<DataKeyManager>>,

    /// Inspector for latency inspecting
    pub pending_latency_inspect: Vec<LatencyInspector>,
}

impl<EK: KvEngine, ER: RaftEngine, T> StoreContext<EK, ER, T> {
    pub fn update_ticks_timeout(&mut self) {
        self.tick_batch[PeerTick::Raft as usize].wait_duration = self.cfg.raft_base_tick_interval.0;
        self.tick_batch[PeerTick::CompactLog as usize].wait_duration =
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
        self.tick_batch[PeerTick::CheckLongUncommitted as usize].wait_duration =
            self.cfg.check_long_uncommitted_interval.0;
        self.tick_batch[PeerTick::GcPeer as usize].wait_duration =
            self.cfg.gc_peer_check_interval.0;
    }

    // Return None means it has passed unsafe vote period.
    pub fn maybe_in_unsafe_vote_period(&mut self) -> Option<Duration> {
        if self.cfg.allow_unsafe_vote_after_start {
            return None;
        }
        let deadline = TiInstant::Monotonic(self.unsafe_vote_deadline?);
        let current_time =
            TiInstant::Monotonic(*self.current_time.get_or_insert_with(monotonic_raw_now));
        let remain_duration = deadline.saturating_duration_since(current_time);
        if remain_duration > Duration::ZERO {
            Some(remain_duration)
        } else {
            self.unsafe_vote_deadline.take();
            None
        }
    }
}

/// A [`PollHandler`] that handles updates of [`StoreFsm`]s and [`PeerFsm`]s.
///
/// It is responsible for:
///
/// - Keeping the local [`StoreContext`] up-to-date.
/// - Receiving and sending messages in and out of these FSMs.
struct StorePoller<EK: KvEngine, ER: RaftEngine, T> {
    poll_ctx: StoreContext<EK, ER, T>,
    cfg_tracker: Tracker<Config>,
    /// Buffers to hold in-coming messages.
    store_msg_buf: Vec<StoreMsg>,
    peer_msg_buf: Vec<PeerMsg>,
    timer: tikv_util::time::Instant,
    /// These fields controls the timing of flushing messages generated by
    /// FSMs.
    last_flush_time: TiInstant,
    need_flush_events: bool,
}

impl<EK: KvEngine, ER: RaftEngine, T> StorePoller<EK, ER, T> {
    pub fn new(poll_ctx: StoreContext<EK, ER, T>, cfg_tracker: Tracker<Config>) -> Self {
        Self {
            poll_ctx,
            cfg_tracker,
            store_msg_buf: Vec::new(),
            peer_msg_buf: Vec::new(),
            timer: tikv_util::time::Instant::now(),
            last_flush_time: TiInstant::now(),
            need_flush_events: false,
        }
    }

    /// Updates the internal buffer to match the latest configuration.
    fn apply_buf_capacity(&mut self) {
        let new_cap = self.messages_per_tick();
        tikv_util::set_vec_capacity(&mut self.store_msg_buf, new_cap);
        tikv_util::set_vec_capacity(&mut self.peer_msg_buf, new_cap);
    }

    #[inline]
    fn messages_per_tick(&self) -> usize {
        self.poll_ctx.cfg.messages_per_tick
    }

    fn flush_events(&mut self) {
        self.schedule_ticks();
        self.poll_ctx.raft_metrics.maybe_flush();
        self.poll_ctx.store_stat.flush();
    }

    fn schedule_ticks(&mut self) {
        assert_eq!(PeerTick::all_ticks().len(), self.poll_ctx.tick_batch.len());
        for batch in &mut self.poll_ctx.tick_batch {
            batch.schedule(&self.poll_ctx.timer);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport + 'static> PollHandler<PeerFsm<EK, ER>, StoreFsm>
    for StorePoller<EK, ER, T>
{
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a batch_system::Config),
    {
        if self.store_msg_buf.capacity() == 0 || self.peer_msg_buf.capacity() == 0 {
            self.apply_buf_capacity();
        }
        self.poll_ctx.self_disk_usage = get_disk_status(self.poll_ctx.store_id);
        // Apply configuration changes.
        if let Some(cfg) = self.cfg_tracker.any_new().map(|c| c.clone()) {
            let last_messages_per_tick = self.messages_per_tick();
            self.poll_ctx.cfg = cfg;
            if self.poll_ctx.cfg.messages_per_tick != last_messages_per_tick {
                self.apply_buf_capacity();
            }
            update_cfg(&self.poll_ctx.cfg.store_batch_system);
            self.poll_ctx.update_ticks_timeout();
        }
        self.poll_ctx.has_ready = false;
        self.poll_ctx.current_time = None;
        self.timer = tikv_util::time::Instant::now();
        // update store writers if necessary
        self.poll_ctx.schedulers.write.refresh();
    }

    fn handle_control(&mut self, fsm: &mut StoreFsm) -> Option<usize> {
        debug_assert!(self.store_msg_buf.is_empty());
        let batch_size = self.messages_per_tick();
        let received_cnt = fsm.recv(&mut self.store_msg_buf, batch_size);
        let expected_msg_count = if received_cnt == batch_size {
            None
        } else {
            Some(0)
        };
        let mut delegate = StoreFsmDelegate::new(fsm, &mut self.poll_ctx);
        delegate.handle_msgs(&mut self.store_msg_buf);
        expected_msg_count
    }

    fn handle_normal(&mut self, fsm: &mut impl DerefMut<Target = PeerFsm<EK, ER>>) -> HandleResult {
        fail::fail_point!(
            "pause_on_peer_collect_message",
            fsm.deref_mut().peer().peer_id() == 1,
            |_| unreachable!()
        );
        fail::fail_point!(
            "on_peer_collect_message_2",
            fsm.deref_mut().peer().peer_id() == 2,
            |_| unreachable!()
        );
        debug_assert!(self.peer_msg_buf.is_empty());
        let batch_size = self.messages_per_tick();
        let received_cnt = fsm.recv(&mut self.peer_msg_buf, batch_size);
        let handle_result = if received_cnt == batch_size {
            HandleResult::KeepProcessing
        } else {
            HandleResult::stop_at(0, false)
        };
        let mut delegate = PeerFsmDelegate::new(fsm, &mut self.poll_ctx);
        delegate.on_msgs(&mut self.peer_msg_buf);
        delegate
            .fsm
            .peer_mut()
            .handle_raft_ready(delegate.store_ctx);
        handle_result
    }

    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {
        if self.poll_ctx.trans.need_flush() {
            self.poll_ctx.trans.flush();
        }

        let now = TiInstant::now();
        if now.saturating_duration_since(self.last_flush_time) >= Duration::from_millis(1) {
            self.last_flush_time = now;
            self.need_flush_events = false;
            self.flush_events();
        } else {
            self.need_flush_events = true;
        }
    }

    fn end(&mut self, _batch: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {
        let dur = self.timer.saturating_elapsed();

        let mut latency_inspect = std::mem::take(&mut self.poll_ctx.pending_latency_inspect);
        for inspector in &mut latency_inspect {
            inspector.record_store_process(dur);
        }
        // Use the valid size of async-ios for generating `writer_id` when the local
        // senders haven't been updated by `poller.begin().
        let writer_id = rand::random::<usize>()
            % std::cmp::min(
                self.poll_ctx.cfg.store_io_pool_size,
                self.poll_ctx.write_senders().size(),
            );
        if let Err(err) = self.poll_ctx.write_senders()[writer_id].try_send(
            raftstore::store::WriteMsg::LatencyInspect {
                send_time: TiInstant::now(),
                inspector: latency_inspect,
            },
            None,
        ) {
            warn!(self.poll_ctx.logger, "send latency inspecting to write workers failed"; "err" => ?err);
        }
        self.poll_ctx
            .raft_metrics
            .process_ready
            .observe(duration_to_sec(dur));
    }

    fn pause(&mut self) {
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

#[derive(Clone)]
struct StorePollerBuilder<EK: KvEngine, ER: RaftEngine, T> {
    cfg: Arc<VersionTrack<Config>>,
    coprocessor_host: CoprocessorHost<EK>,
    store_id: u64,
    engine: ER,
    tablet_registry: TabletRegistry<EK>,
    trans: T,
    router: StoreRouter<EK, ER>,
    schedulers: Schedulers<EK, ER>,
    apply_pool: FuturePool,
    high_priority_pool: FuturePool,
    logger: Logger,
    store_meta: Arc<Mutex<StoreMeta<EK>>>,
    shutdown: Arc<AtomicBool>,
    snap_mgr: TabletSnapManager,
    global_stat: GlobalStoreStat,
    sst_importer: Arc<SstImporter<EK>>,
    key_manager: Option<Arc<DataKeyManager>>,
    node_start_time: Timespec, // monotonic_raw_now
}

impl<EK: KvEngine, ER: RaftEngine, T> StorePollerBuilder<EK, ER, T> {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        store_id: u64,
        engine: ER,
        tablet_registry: TabletRegistry<EK>,
        trans: T,
        router: StoreRouter<EK, ER>,
        schedulers: Schedulers<EK, ER>,
        high_priority_pool: FuturePool,
        logger: Logger,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        shutdown: Arc<AtomicBool>,
        snap_mgr: TabletSnapManager,
        coprocessor_host: CoprocessorHost<EK>,
        sst_importer: Arc<SstImporter<EK>>,
        key_manager: Option<Arc<DataKeyManager>>,
        node_start_time: Timespec, // monotonic_raw_now
    ) -> Self {
        let pool_size = cfg.value().apply_batch_system.pool_size;
        let max_pool_size = std::cmp::max(
            pool_size,
            std::cmp::max(4, SysQuota::cpu_cores_quota() as usize),
        );
        let apply_pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, pool_size, max_pool_size)
            .after_start(move || set_io_type(IoType::ForegroundWrite))
            .name_prefix("apply")
            .build_future_pool();
        let global_stat = GlobalStoreStat::default();
        StorePollerBuilder {
            cfg,
            store_id,
            engine,
            tablet_registry,
            trans,
            router,
            apply_pool,
            high_priority_pool,
            logger,
            schedulers,
            store_meta,
            snap_mgr,
            shutdown,
            coprocessor_host,
            global_stat,
            sst_importer,
            key_manager,
            node_start_time,
        }
    }

    /// Initializes all the existing raft machines and cleans up stale tablets.
    fn init(&self) -> Result<HashMap<u64, SenderFsmPair<EK, ER>>> {
        let mut regions = HashMap::default();
        let cfg = self.cfg.value();
        let mut meta = self.store_meta.lock().unwrap();
        self.engine
            .for_each_raft_group::<Error, _>(&mut |region_id| {
                assert_ne!(region_id, INVALID_ID);
                let storage = match Storage::new(
                    region_id,
                    self.store_id,
                    self.engine.clone(),
                    self.schedulers.read.clone(),
                    &self.logger,
                )? {
                    Some(p) => p,
                    None => return Ok(()),
                };

                if storage.is_initialized() {
                    self.coprocessor_host.on_region_changed(
                        storage.region(),
                        RegionChangeEvent::Create,
                        StateRole::Follower,
                    );
                }
                meta.set_region(storage.region(), storage.is_initialized(), &self.logger);

                let (sender, peer_fsm) = PeerFsm::new(
                    &cfg,
                    &self.tablet_registry,
                    self.key_manager.as_deref(),
                    &self.snap_mgr,
                    storage,
                )?;
                meta.region_read_progress
                    .insert(region_id, peer_fsm.as_ref().peer().read_progress().clone());

                let prev = regions.insert(region_id, (sender, peer_fsm));
                if let Some((_, p)) = prev {
                    return Err(box_err!(
                        "duplicate region {} vs {}",
                        SlogFormat(p.logger()),
                        SlogFormat(regions[&region_id].1.logger())
                    ));
                }
                Ok(())
            })?;
        self.clean_up_tablets(&regions)?;
        Ok(regions)
    }

    #[inline]
    fn remove_dir(&self, p: &Path) -> Result<()> {
        if let Some(m) = &self.key_manager {
            m.remove_dir(p, None)?;
        }
        file_system::remove_dir_all(p)?;
        Ok(())
    }

    fn clean_up_tablets(&self, peers: &HashMap<u64, SenderFsmPair<EK, ER>>) -> Result<()> {
        for entry in file_system::read_dir(self.tablet_registry.tablet_root())? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|s| s == "tmp") {
                // The directory may be generated by an aborted checkpoint.
                self.remove_dir(&path)?;
                continue;
            }
            let Some((prefix, region_id, tablet_index)) =
                self.tablet_registry.parse_tablet_name(&path)
            else {
                continue;
            };
            if prefix == MERGE_SOURCE_PREFIX {
                continue;
            }
            let fsm = match peers.get(&region_id) {
                Some((_, fsm)) => fsm,
                None => {
                    // The peer is either destroyed or not created yet. It will be
                    // recovered by leader heartbeats.
                    self.remove_dir(&path)?;
                    continue;
                }
            };
            // Valid split tablet should be installed during recovery.
            if prefix == SPLIT_PREFIX {
                self.remove_dir(&path)?;
                continue;
            } else if prefix == MERGE_IN_PROGRESS_PREFIX {
                continue;
            } else if prefix.is_empty() {
                // Stale split data can be deleted.
                if fsm.peer().storage().tablet_index() > tablet_index {
                    self.remove_dir(&path)?;
                }
            } else {
                debug_assert!(false, "unexpected tablet prefix: {}", path.display());
                warn!(self.logger, "unexpected tablet prefix"; "path" => %path.display());
            }
        }
        // TODO: list all available tablets and destroy those which are not in the
        // peers.
        Ok(())
    }
}

impl<EK, ER, T> HandlerBuilder<PeerFsm<EK, ER>, StoreFsm> for StorePollerBuilder<EK, ER, T>
where
    ER: RaftEngine,
    EK: KvEngine,
    T: Transport + 'static,
{
    type Handler = StorePoller<EK, ER, T>;

    fn build(&mut self, _priority: batch_system::Priority) -> Self::Handler {
        let cfg = self.cfg.value().clone();
        let election_timeout = cfg.raft_base_tick_interval.0
            * if cfg.raft_min_election_timeout_ticks != 0 {
                cfg.raft_min_election_timeout_ticks as u32
            } else {
                cfg.raft_election_timeout_ticks as u32
            };
        let unsafe_vote_deadline =
            Some(self.node_start_time + time::Duration::from_std(election_timeout).unwrap());
        let mut poll_ctx = StoreContext {
            logger: self.logger.clone(),
            store_id: self.store_id,
            trans: self.trans.clone(),
            current_time: None,
            unsafe_vote_deadline,
            has_ready: false,
            raft_metrics: RaftMetrics::new(cfg.waterfall_metrics),
            cfg,
            router: self.router.clone(),
            tick_batch: vec![PeerTickBatch::default(); PeerTick::VARIANT_COUNT],
            timer: SteadyTimer::default(),
            schedulers: self.schedulers.clone(),
            store_meta: self.store_meta.clone(),
            shutdown: self.shutdown.clone(),
            engine: self.engine.clone(),
            tablet_registry: self.tablet_registry.clone(),
            apply_pool: self.apply_pool.clone(),
            high_priority_pool: self.high_priority_pool.clone(),
            self_disk_usage: DiskUsage::Normal,
            store_disk_usages: Default::default(),
            snap_mgr: self.snap_mgr.clone(),
            coprocessor_host: self.coprocessor_host.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            sst_importer: self.sst_importer.clone(),
            key_manager: self.key_manager.clone(),
            pending_latency_inspect: vec![],
        };
        poll_ctx.update_ticks_timeout();
        let cfg_tracker = self.cfg.clone().tracker("raftstore".to_string());
        StorePoller::new(poll_ctx, cfg_tracker)
    }
}

#[derive(Clone)]
pub struct Schedulers<EK: KvEngine, ER: RaftEngine> {
    pub read: Scheduler<ReadTask<EK>>,
    pub pd: Scheduler<pd::Task>,
    pub tablet: Scheduler<tablet::Task<EK>>,
    pub write: WriteSenders<EK, ER>,
    pub refresh_config: Scheduler<RefreshConfigTask>,

    // Following is not maintained by raftstore itself.
    pub split_check: Scheduler<SplitCheckTask<EK>>,
}

impl<EK: KvEngine, ER: RaftEngine> Schedulers<EK, ER> {
    fn stop(&self) {
        self.read.stop();
        self.pd.stop();
        self.tablet.stop();
        self.split_check.stop();
    }
}

/// A set of background threads that will processing offloaded work from
/// raftstore.
struct Workers<EK: KvEngine, ER: RaftEngine> {
    /// Worker for fetching raft logs asynchronously
    async_read: Worker,
    pd: LazyWorker<pd::Task>,
    tablet: Worker,
    checkpoint: Worker,
    async_write: StoreWriters<EK, ER>,
    purge: Option<Worker>,

    refresh_config_worker: LazyWorker<RefreshConfigTask>,

    // Following is not maintained by raftstore itself.
    background: Worker,

    // A background pool used for high-priority works. We need to hold a reference to shut it down
    // manually.
    high_priority_pool: FuturePool,
}

impl<EK: KvEngine, ER: RaftEngine> Workers<EK, ER> {
    fn new(
        background: Worker,
        pd: LazyWorker<pd::Task>,
        purge: Option<Worker>,
        resource_control: Option<Arc<ResourceController>>,
    ) -> Self {
        let checkpoint = Builder::new("checkpoint-worker").thread_count(2).create();
        Self {
            async_read: Worker::new("async-read-worker"),
            pd,
            tablet: Worker::new("tablet-worker"),
            checkpoint,
            async_write: StoreWriters::new(resource_control),
            purge,
            refresh_config_worker: LazyWorker::new("refreash-config-worker"),
            background,
            high_priority_pool: YatpPoolBuilder::new(DefaultTicker::default())
                .thread_count(1, 1, 1)
                .after_start(move || set_io_type(IoType::ForegroundWrite))
                .name_prefix("store-bg")
                .build_future_pool(),
        }
    }

    fn stop(mut self) {
        self.async_write.shutdown();
        self.async_read.stop();
        self.pd.stop();
        self.tablet.stop();
        self.checkpoint.stop();
        self.refresh_config_worker.stop();
        if let Some(w) = self.purge {
            w.stop();
        }
        self.high_priority_pool.shutdown();
    }
}

/// The system used for polling Raft activities.
pub struct StoreSystem<EK: KvEngine, ER: RaftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm>,
    workers: Option<Workers<EK, ER>>,
    schedulers: Option<Schedulers<EK, ER>>,
    logger: Logger,
    shutdown: Arc<AtomicBool>,
    node_start_time: Timespec, // monotonic_raw_now
}

impl<EK: KvEngine, ER: RaftEngine> StoreSystem<EK, ER> {
    pub fn start<T, C>(
        &mut self,
        store_id: u64,
        cfg: Arc<VersionTrack<Config>>,
        raft_engine: ER,
        tablet_registry: TabletRegistry<EK>,
        trans: T,
        pd_client: Arc<C>,
        router: &StoreRouter<EK, ER>,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        snap_mgr: TabletSnapManager,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        coprocessor_host: CoprocessorHost<EK>,
        auto_split_controller: AutoSplitController,
        collector_reg_handle: CollectorRegHandle,
        background: Worker,
        pd_worker: LazyWorker<pd::Task>,
        sst_importer: Arc<SstImporter<EK>>,
        key_manager: Option<Arc<DataKeyManager>>,
        grpc_service_mgr: GrpcServiceManager,
        resource_ctl: Option<Arc<ResourceController>>,
    ) -> Result<()>
    where
        T: Transport + 'static,
        C: PdClient + 'static,
    {
        let sync_router = Mutex::new(router.clone());
        pd_client.handle_reconnect(move || {
            sync_router
                .lock()
                .unwrap()
                .broadcast_normal(|| PeerMsg::Tick(PeerTick::PdHeartbeat));
        });

        let purge_worker = if raft_engine.need_manual_purge()
            && !cfg.value().raft_engine_purge_interval.0.is_zero()
        {
            let worker = Worker::new("purge-worker");
            let raft_clone = raft_engine.clone();
            let logger = self.logger.clone();
            let router = router.clone();
            let registry = tablet_registry.clone();
            let base_max_rate = cfg
                .value()
                .max_manual_flush_rate
                .clamp(MIN_MANUAL_FLUSH_RATE, f64::INFINITY);
            let mut last_flush = (
                EK::get_accumulated_flush_count().unwrap(),
                TiInstant::now_coarse(),
            );
            worker.spawn_interval_async_task(cfg.value().raft_engine_purge_interval.0, move || {
                let regions = {
                    let _guard = WithIoType::new(IoType::RewriteLog);
                    match raft_clone.manual_purge() {
                        Err(e) => {
                            warn!(logger, "purge expired files"; "err" => %e);
                            Vec::new()
                        }
                        Ok(regions) => regions,
                    }
                };
                // Lift up max rate if the background flush rate is high.
                let flush_count = EK::get_accumulated_flush_count().unwrap();
                let now = TiInstant::now_coarse();
                let duration = now.saturating_duration_since(last_flush.1).as_secs_f64();
                let max_rate = if duration > 10.0 {
                    let total_flush_rate = (flush_count - last_flush.0) as f64 / duration;
                    last_flush = (flush_count, now);
                    base_max_rate.clamp(total_flush_rate, f64::INFINITY)
                } else {
                    base_max_rate
                };
                // Try to finish flush just in time.
                let rate = regions.len() as f64 / MAX_MANUAL_FLUSH_PERIOD.as_secs_f64();
                let rate = rate.clamp(MIN_MANUAL_FLUSH_RATE, max_rate);
                // Return early if there're too many regions. Otherwise even if we manage to
                // compact regions, the space can't be reclaimed in time.
                let mut to_flush = (rate * MAX_MANUAL_FLUSH_PERIOD.as_secs_f64()) as usize;
                // Skip tablets that are flushed elsewhere.
                let threshold = std::time::SystemTime::now() - MAX_MANUAL_FLUSH_PERIOD;
                for r in &regions {
                    let _ = router.send(*r, PeerMsg::ForceCompactLog);
                }
                let registry = registry.clone();
                let logger = logger.clone();
                let limiter = Limiter::new(rate);
                async move {
                    for r in regions {
                        if to_flush == 0 {
                            break;
                        }
                        if let Some(mut t) = registry.get(r)
                            && let Some(t) = t.latest()
                        {
                            match t.flush_oldest_cf(true, Some(threshold)) {
                                Err(e) => warn!(logger, "failed to flush oldest cf"; "err" => %e),
                                Ok(true) => {
                                    to_flush -= 1;
                                    let time =
                                        std::time::Instant::now() + limiter.consume_duration(1);
                                    let _ = GLOBAL_TIMER_HANDLE.delay(time).compat().await;
                                }
                                _ => (),
                            }
                        }
                    }
                }
            });
            Some(worker)
        } else {
            None
        };

        let mut workers = Workers::new(background, pd_worker, purge_worker, resource_ctl);
        workers
            .async_write
            .spawn(store_id, raft_engine.clone(), None, router, &trans, &cfg)?;

        let mut read_runner = ReadRunner::new(router.clone(), raft_engine.clone());
        read_runner.set_snap_mgr(snap_mgr.clone());
        let read_scheduler = workers.async_read.start("async-read-worker", read_runner);

        workers.pd.start(pd::Runner::new(
            store_id,
            pd_client,
            raft_engine.clone(),
            tablet_registry.clone(),
            snap_mgr.clone(),
            router.clone(),
            workers.pd.remote(),
            concurrency_manager,
            causal_ts_provider,
            workers.pd.scheduler(),
            auto_split_controller,
            collector_reg_handle,
            grpc_service_mgr,
            self.logger.clone(),
            self.shutdown.clone(),
            cfg.clone(),
            Arc::new(AtomicBool::new(false)),
        )?);

        let split_check_scheduler = workers.background.start(
            "split-check",
            SplitCheckRunner::with_registry(
                tablet_registry.clone(),
                router.clone(),
                coprocessor_host.clone(),
                None,
            ),
        );

        let tablet_scheduler = workers.tablet.start_with_timer(
            "tablet-worker",
            tablet::Runner::new(
                tablet_registry.clone(),
                sst_importer.clone(),
                snap_mgr.clone(),
                self.logger.clone(),
            ),
        );

        let refresh_config_scheduler = workers.refresh_config_worker.scheduler();

        let schedulers = Schedulers {
            read: read_scheduler,
            pd: workers.pd.scheduler(),
            tablet: tablet_scheduler,
            write: workers.async_write.senders(),
            split_check: split_check_scheduler,
            refresh_config: refresh_config_scheduler,
        };

        let builder = StorePollerBuilder::new(
            cfg.clone(),
            store_id,
            raft_engine.clone(),
            tablet_registry,
            trans.clone(),
            router.clone(),
            schedulers.clone(),
            workers.high_priority_pool.clone(),
            self.logger.clone(),
            store_meta.clone(),
            self.shutdown.clone(),
            snap_mgr,
            coprocessor_host,
            sst_importer,
            key_manager,
            self.node_start_time,
        );

        self.schedulers = Some(schedulers);
        let peers = builder.init()?;
        // Choose a different name so we know what version is actually used. rs stands
        // for raft store.
        let tag = format!("rs-{}", store_id);
        self.system.spawn(tag, builder.clone());

        let writer_control = WriterContoller::new(
            StoreWritersContext {
                store_id,
                raft_engine,
                kv_engine: None,
                transfer: trans,
                notifier: router.clone(),
                cfg: cfg.clone(),
            },
            workers.async_write.clone(),
        );
        let apply_pool = builder.apply_pool.clone();
        let refresh_config_runner = refresh_config::Runner::new(
            self.logger.clone(),
            router.router().clone(),
            self.system.build_pool_state(builder),
            writer_control,
            apply_pool,
        );
        assert!(workers.refresh_config_worker.start(refresh_config_runner));
        self.workers = Some(workers);

        let mut mailboxes = Vec::with_capacity(peers.len());
        let mut address = Vec::with_capacity(peers.len());
        {
            let mut meta = store_meta.as_ref().lock().unwrap();
            for (region_id, (tx, mut fsm)) in peers {
                if let Some(tablet) = fsm.peer_mut().tablet() {
                    let read_tablet = SharedReadTablet::new(tablet.clone());
                    meta.readers.insert(
                        region_id,
                        (fsm.peer().generate_read_delegate(), read_tablet),
                    );
                }

                address.push(region_id);
                mailboxes.push((
                    region_id,
                    BasicMailbox::new(tx, fsm, router.state_cnt().clone()),
                ));
            }
        }
        router.register_all(mailboxes);

        // Make sure Msg::Start is the first message each FSM received.
        let watch = Arc::new(ReplayWatch::new(self.logger.clone()));
        for addr in address {
            router
                .force_send(addr, PeerMsg::Start(Some(watch.clone())))
                .unwrap();
        }
        router.send_control(StoreMsg::Start).unwrap();
        Ok(())
    }

    pub fn refresh_config_scheduler(&mut self) -> Scheduler<RefreshConfigTask> {
        assert!(self.workers.is_some());
        self.workers
            .as_ref()
            .unwrap()
            .refresh_config_worker
            .scheduler()
    }

    pub fn pd_scheduler(&self) -> Scheduler<pd::Task> {
        assert!(self.workers.is_some());
        self.workers.as_ref().unwrap().pd.scheduler()
    }

    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if self.workers.is_none() {
            return;
        }
        let workers = self.workers.take().unwrap();

        // TODO: gracefully shutdown future apply pool

        // Stop schedulers first, so all background future worker pool will be stopped
        // gracefully.
        self.schedulers.take().unwrap().stop();
        self.system.shutdown();

        workers.stop();
    }
}

#[derive(Clone)]
pub struct StoreRouter<EK: KvEngine, ER: RaftEngine> {
    router: BatchRouter<PeerFsm<EK, ER>, StoreFsm>,
    logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine> StoreRouter<EK, ER> {
    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    #[inline]
    pub fn check_send(&self, addr: u64, msg: PeerMsg) -> crate::Result<()> {
        match self.router.send(addr, msg) {
            Ok(()) => Ok(()),
            Err(e) => Err(raftstore::router::handle_send_error(addr, e)),
        }
    }

    pub fn send_raft_message(
        &self,
        msg: Box<RaftMessage>,
    ) -> std::result::Result<(), TrySendError<Box<RaftMessage>>> {
        let id = msg.get_region_id();
        let peer_msg = PeerMsg::RaftMessage(msg, Some(TiInstant::now()));
        let store_msg = match self.router.try_send(id, peer_msg) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(m, _)))) => {
                return Err(TrySendError::Full(m));
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m, _)))) => {
                return Err(TrySendError::Disconnected(m));
            }
            Either::Right(PeerMsg::RaftMessage(m, _)) => StoreMsg::RaftMessage(m),
            _ => unreachable!(),
        };
        match self.router.send_control(store_msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(StoreMsg::RaftMessage(m))) => Err(TrySendError::Full(m)),
            Err(TrySendError::Disconnected(StoreMsg::RaftMessage(m))) => {
                Err(TrySendError::Disconnected(m))
            }
            _ => unreachable!(),
        }
    }

    pub fn router(&self) -> &BatchRouter<PeerFsm<EK, ER>, StoreFsm> {
        &self.router
    }
}

impl<EK: KvEngine, ER: RaftEngine> Deref for StoreRouter<EK, ER> {
    type Target = BatchRouter<PeerFsm<EK, ER>, StoreFsm>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.router
    }
}

impl<EK: KvEngine, ER: RaftEngine> DerefMut for StoreRouter<EK, ER> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.router
    }
}

/// Creates the batch system for polling raft activities.
pub fn create_store_batch_system<EK, ER>(
    cfg: &Config,
    store_id: u64,
    logger: Logger,
    resource_ctl: Option<Arc<ResourceController>>,
) -> (StoreRouter<EK, ER>, StoreSystem<EK, ER>)
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let (store_tx, store_fsm) = StoreFsm::new(cfg, store_id, logger.clone());
    let (router, system) =
        batch_system::create_system(&cfg.store_batch_system, store_tx, store_fsm, resource_ctl);
    let system = StoreSystem {
        system,
        workers: None,
        schedulers: None,
        logger: logger.clone(),
        shutdown: Arc::new(AtomicBool::new(false)),
        node_start_time: monotonic_raw_now(),
    };
    (StoreRouter { router, logger }, system)
}
