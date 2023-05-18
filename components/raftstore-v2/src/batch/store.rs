// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
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
use file_system::{set_io_type, IoType, WithIoType};
use kvproto::{disk_usage::DiskUsage, raft_serverpb::RaftMessage};
use pd_client::PdClient;
use raft::{StateRole, INVALID_ID};
use raftstore::{
    coprocessor::{CoprocessorHost, RegionChangeEvent},
    store::{
        fsm::{
            store::{PeerTickBatch, ENTRY_CACHE_EVICT_TICK_DURATION},
            GlobalStoreStat, LocalStoreStat,
        },
        local_metrics::RaftMetrics,
        AutoSplitController, Config, ReadRunner, ReadTask, SplitCheckRunner, SplitCheckTask,
        StoreWriters, TabletSnapManager, Transport, WriteSenders,
    },
};
use resource_metering::CollectorRegHandle;
use slog::{warn, Logger};
use sst_importer::SstImporter;
use tikv_util::{
    box_err,
    config::{Tracker, VersionTrack},
    log::SlogFormat,
    sys::SysQuota,
    time::{duration_to_sec, Instant as TiInstant},
    timer::SteadyTimer,
    worker::{Builder, LazyWorker, Scheduler, Worker},
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
    Either,
};
use time::Timespec;

use crate::{
    fsm::{PeerFsm, PeerFsmDelegate, SenderFsmPair, StoreFsm, StoreFsmDelegate, StoreMeta},
    operation::{SharedReadTablet, MERGE_IN_PROGRESS_PREFIX, MERGE_SOURCE_PREFIX, SPLIT_PREFIX},
    raft::Storage,
    router::{PeerMsg, PeerTick, StoreMsg},
    worker::{checkpoint, cleanup, pd, tablet},
    Error, Result,
};

/// A per-thread context shared by the [`StoreFsm`] and multiple [`PeerFsm`]s.
pub struct StoreContext<EK: KvEngine, ER: RaftEngine, T> {
    /// A logger without any KV. It's clean for creating new PeerFSM.
    pub logger: Logger,
    pub store_id: u64,
    pub coprocessor_host: CoprocessorHost<EK>,
    /// The transport for sending messages to peers on other stores.
    pub trans: T,
    pub current_time: Option<Timespec>,
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
    /// store meta
    pub store_meta: Arc<Mutex<StoreMeta<EK>>>,
    pub shutdown: Arc<AtomicBool>,
    pub engine: ER,
    pub tablet_registry: TabletRegistry<EK>,
    pub apply_pool: FuturePool,

    /// Disk usage for the store itself.
    pub self_disk_usage: DiskUsage,

    pub snap_mgr: TabletSnapManager,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub sst_importer: Arc<SstImporter>,
    pub key_manager: Option<Arc<DataKeyManager>>,
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
            60 * cmp::min(Duration::from_secs(1), self.cfg.raft_base_tick_interval.0);
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
    logger: Logger,
    store_meta: Arc<Mutex<StoreMeta<EK>>>,
    shutdown: Arc<AtomicBool>,
    snap_mgr: TabletSnapManager,
    global_stat: GlobalStoreStat,
    sst_importer: Arc<SstImporter>,
    key_manager: Option<Arc<DataKeyManager>>,
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
        logger: Logger,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        shutdown: Arc<AtomicBool>,
        snap_mgr: TabletSnapManager,
        coprocessor_host: CoprocessorHost<EK>,
        sst_importer: Arc<SstImporter>,
        key_manager: Option<Arc<DataKeyManager>>,
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
            logger,
            schedulers,
            store_meta,
            snap_mgr,
            shutdown,
            coprocessor_host,
            global_stat,
            sst_importer,
            key_manager,
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
            if path.extension().map_or(false, |s| s == "tmp") {
                // The directory may be generated by an aborted checkpoint.
                self.remove_dir(&path)?;
                continue;
            }
            let Some((prefix, region_id, tablet_index)) = self.tablet_registry.parse_tablet_name(&path) else { continue };
            // Keep the checkpoint even if source is destroyed.
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
        let mut poll_ctx = StoreContext {
            logger: self.logger.clone(),
            store_id: self.store_id,
            trans: self.trans.clone(),
            current_time: None,
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
            self_disk_usage: DiskUsage::Normal,
            snap_mgr: self.snap_mgr.clone(),
            coprocessor_host: self.coprocessor_host.clone(),
            global_stat: self.global_stat.clone(),
            store_stat: self.global_stat.local(),
            sst_importer: self.sst_importer.clone(),
            key_manager: self.key_manager.clone(),
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
    pub checkpoint: Scheduler<checkpoint::Task<EK>>,
    pub write: WriteSenders<EK, ER>,
    pub cleanup: Scheduler<cleanup::Task>,

    // Following is not maintained by raftstore itself.
    pub split_check: Scheduler<SplitCheckTask>,
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
    cleanup_worker: Worker,

    // Following is not maintained by raftstore itself.
    background: Worker,
}

impl<EK: KvEngine, ER: RaftEngine> Workers<EK, ER> {
    fn new(background: Worker, pd: LazyWorker<pd::Task>, purge: Option<Worker>) -> Self {
        let checkpoint = Builder::new("checkpoint-worker").thread_count(2).create();
        Self {
            async_read: Worker::new("async-read-worker"),
            pd,
            tablet: Worker::new("tablet-worker"),
            checkpoint,
            async_write: StoreWriters::new(None),
            purge,
            cleanup_worker: Worker::new("cleanup-worker"),
            background,
        }
    }

    fn stop(mut self) {
        self.async_write.shutdown();
        self.async_read.stop();
        self.pd.stop();
        self.tablet.stop();
        self.checkpoint.stop();
        if let Some(w) = self.purge {
            w.stop();
        }
    }
}

/// The system used for polling Raft activities.
pub struct StoreSystem<EK: KvEngine, ER: RaftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm>,
    workers: Option<Workers<EK, ER>>,
    schedulers: Option<Schedulers<EK, ER>>,
    logger: Logger,
    shutdown: Arc<AtomicBool>,
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
        sst_importer: Arc<SstImporter>,
        key_manager: Option<Arc<DataKeyManager>>,
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
            worker.spawn_interval_task(cfg.value().raft_engine_purge_interval.0, move || {
                let _guard = WithIoType::new(IoType::RewriteLog);
                match raft_clone.manual_purge() {
                    Ok(regions) => {
                        for r in regions {
                            let _ = router.send(r, PeerMsg::ForceCompactLog);
                        }
                    }
                    Err(e) => {
                        warn!(logger, "purge expired files"; "err" => %e);
                    }
                };
            });
            Some(worker)
        } else {
            None
        };

        let mut workers = Workers::new(background, pd_worker, purge_worker);
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
            store_meta.lock().unwrap().region_read_progress.clone(),
            collector_reg_handle,
            self.logger.clone(),
            self.shutdown.clone(),
            cfg.clone(),
        )?);

        let split_check_scheduler = workers.background.start(
            "split-check",
            SplitCheckRunner::with_registry(
                tablet_registry.clone(),
                router.clone(),
                coprocessor_host.clone(),
            ),
        );

        let tablet_scheduler = workers.tablet.start_with_timer(
            "tablet-worker",
            tablet::Runner::new(
                tablet_registry.clone(),
                sst_importer.clone(),
                self.logger.clone(),
            ),
        );

        let compact_runner =
            cleanup::CompactRunner::new(tablet_registry.clone(), self.logger.clone());
        let cleanup_worker_scheduler = workers
            .cleanup_worker
            .start("cleanup-worker", cleanup::Runner::new(compact_runner));

        let checkpoint_scheduler = workers.checkpoint.start(
            "checkpoint-worker",
            checkpoint::Runner::new(self.logger.clone(), tablet_registry.clone()),
        );

        let schedulers = Schedulers {
            read: read_scheduler,
            pd: workers.pd.scheduler(),
            tablet: tablet_scheduler,
            checkpoint: checkpoint_scheduler,
            write: workers.async_write.senders(),
            split_check: split_check_scheduler,
            cleanup: cleanup_worker_scheduler,
        };

        let builder = StorePollerBuilder::new(
            cfg.clone(),
            store_id,
            raft_engine,
            tablet_registry,
            trans,
            router.clone(),
            schedulers.clone(),
            self.logger.clone(),
            store_meta.clone(),
            self.shutdown.clone(),
            snap_mgr,
            coprocessor_host,
            sst_importer,
            key_manager,
        );
        self.workers = Some(workers);
        self.schedulers = Some(schedulers);
        let peers = builder.init()?;
        // Choose a different name so we know what version is actually used. rs stands
        // for raft store.
        let tag = format!("rs-{}", store_id);
        self.system.spawn(tag, builder);

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
        for addr in address {
            router.force_send(addr, PeerMsg::Start).unwrap();
        }
        router.send_control(StoreMsg::Start).unwrap();
        Ok(())
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
        let peer_msg = PeerMsg::RaftMessage(msg);
        let store_msg = match self.router.try_send(id, peer_msg) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Full(m));
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Disconnected(m));
            }
            Either::Right(PeerMsg::RaftMessage(m)) => StoreMsg::RaftMessage(m),
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
) -> (StoreRouter<EK, ER>, StoreSystem<EK, ER>)
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let (store_tx, store_fsm) = StoreFsm::new(cfg, store_id, logger.clone());
    let (router, system) =
        batch_system::create_system(&cfg.store_batch_system, store_tx, store_fsm, None);
    let system = StoreSystem {
        system,
        workers: None,
        schedulers: None,
        logger: logger.clone(),
        shutdown: Arc::new(AtomicBool::new(false)),
    };
    (StoreRouter { router, logger }, system)
}
