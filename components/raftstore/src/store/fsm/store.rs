// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{thread, u64};

use crossbeam::channel::TrySendError;
use engine_traits::{Engines, KvEngine, Mutable};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use futures::compat::Future01CompatExt;
use futures::FutureExt;
use kvproto::errorpb;
use kvproto::import_sstpb::SstMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::StoreStats;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdResponse};
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use kvproto::replication_modepb::{ReplicationMode, ReplicationStatus};
use protobuf::Message;
use raft::StateRole;
use time::{self, Timespec};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use yatp::pool::ThreadPool;
use yatp::task::future::TaskCell;
use yatp::Remote;

use engine_traits::CompactedEvent;
use engine_traits::{RaftEngine, RaftLogBatch};
use keys::{self, data_end_key, data_key, enc_end_key, enc_start_key};
use pd_client::PdClient;
use sst_importer::SSTImporter;
use tikv_util::collections::HashMap;
use tikv_util::config::{Tracker, VersionTrack};
use tikv_util::time::{duration_to_sec, Instant as TiInstant};
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{FutureScheduler, FutureWorker, Scheduler, Worker};
use tikv_util::yatp_pool::{PoolTicker, YatpPoolBuilder};
use tikv_util::{is_zero_duration, sys as sys_util, RingQueue};

use crate::coprocessor::split_observer::SplitObserver;
use crate::coprocessor::{BoxAdminObserver, CoprocessorHost, RegionChangeEvent};
use crate::store::config::Config;
use crate::store::fsm::metrics::*;
use crate::store::fsm::peer::{
    maybe_destroy_source, new_admin_request, PeerFsm, PeerFsmDelegate, SenderFsmPair,
};

use crate::store::fsm::{
    create_apply_batch_system, ApplyBatchSystem, ApplyNotifier, AsyncRouter, Registration,
};
use crate::store::fsm::{ApplyRes, ApplyTaskRes, AsyncRouterError};
use crate::store::local_metrics::RaftMetrics;
use crate::store::metrics::*;
use crate::store::peer_storage;
use crate::store::transport::Transport;
use crate::store::util::is_initial_msg;
use crate::store::worker::{
    AutoSplitController, CleanupRunner, CleanupSSTRunner, CleanupSSTTask, CleanupTask,
    CompactRunner, CompactTask, ConsistencyCheckRunner, ConsistencyCheckTask, PdRunner,
    RaftlogGcRunner, RaftlogGcTask, ReadDelegate, RegionRunner, RegionTask, SplitCheckTask,
};
use crate::store::PdTask;
use crate::store::{
    util, Callback, CasualMessage, GlobalReplicationState, MergeResultKind, PeerMsg, RaftCommand,
    SignificantMsg, SnapManager, StoreMsg, StoreTick,
};
use crate::Result;
use concurrency_manager::ConcurrencyManager;
use tikv_util::future::poll_future_notify;

type Key = Vec<u8>;

pub const PENDING_VOTES_CAP: usize = 20;
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
    /// `MsgRequestPreVote` or `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
    /// such Region in this store now. So the messages are recorded temporarily and will be handled later.
    pub pending_votes: RingQueue<RaftMessage>,
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
            pending_votes: RingQueue::with_capacity(vote_capacity),
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

pub struct RaftRouter<EK: KvEngine> {
    pub router: AsyncRouter<EK>,
}

impl<EK: KvEngine> Clone for RaftRouter<EK> {
    fn clone(&self) -> Self {
        RaftRouter {
            router: self.router.clone(),
        }
    }
}

impl<EK> Deref for RaftRouter<EK>
where
    EK: KvEngine,
{
    type Target = AsyncRouter<EK>;

    fn deref(&self) -> &AsyncRouter<EK> {
        &self.router
    }
}

impl<EK: KvEngine> ApplyNotifier<EK> for RaftRouter<EK> {
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
        let _ = self.router.send(region_id, msg);
    }

    fn clone_box(&self) -> Box<dyn ApplyNotifier<EK>> {
        Box::new(self.clone())
    }
}

impl<EK: KvEngine> RaftRouter<EK> {
    pub fn send_raft_message(
        &self,
        msg: RaftMessage,
    ) -> std::result::Result<(), TrySendError<RaftMessage>> {
        let id = msg.get_region_id();
        match self.router.send(id, PeerMsg::RaftMessage(msg)) {
            Ok(()) => Ok(()),
            Err(AsyncRouterError::NotExist(PeerMsg::RaftMessage(msg))) => self
                .router
                .send_control(StoreMsg::RaftMessage(msg))
                .map_err(|e| match e {
                    TrySendError::Disconnected(StoreMsg::RaftMessage(msg)) => {
                        TrySendError::Disconnected(msg)
                    }
                    _ => unreachable!(),
                }),
            Err(AsyncRouterError::Closed(PeerMsg::RaftMessage(m))) => {
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
            Err(AsyncRouterError::Closed(PeerMsg::RaftCommand(cmd))) => {
                Err(TrySendError::Disconnected(cmd))
            }
            Err(AsyncRouterError::NotExist(PeerMsg::RaftCommand(cmd))) => {
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

pub struct PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
{
    pub cfg: Config,
    pub store: metapb::Store,
    pub pd_scheduler: FutureScheduler<PdTask<EK>>,
    pub consistency_check_scheduler: Scheduler<ConsistencyCheckTask<EK::Snapshot>>,
    pub split_check_scheduler: Scheduler<SplitCheckTask>,
    // handle Compact, CleanupSST task
    pub cleanup_scheduler: Scheduler<CleanupTask>,
    pub raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    pub region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    pub router: RaftRouter<EK>,
    pub apply_batch_system: Arc<ApplyBatchSystem<EK>>,
    pub remote: Remote<TaskCell>,
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
    pub coprocessor_host: CoprocessorHost<EK>,
    pub timer: SteadyTimer,
    pub trans: Option<T>,
    pub global_replication_state: Arc<Mutex<GlobalReplicationState>>,
    pub global_stat: GlobalStoreStat,
    pub store_stat: LocalStoreStat,
    pub engines: Engines<EK, ER>,
    pub current_time: Option<Timespec>,
    pub node_start_time: Option<TiInstant>,
    pub cfg_tracker: Tracker<Config>,
}

impl<EK, ER, T> Clone for PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
{
    fn clone(&self) -> Self {
        let global_stat = self.global_stat.clone();
        PollContext {
            cfg: self.cfg.clone(),
            store: self.store.clone(),
            pd_scheduler: self.pd_scheduler.clone(),
            consistency_check_scheduler: self.consistency_check_scheduler.clone(),
            split_check_scheduler: self.split_check_scheduler.clone(),
            region_scheduler: self.region_scheduler.clone(),
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
            trans: None,
            global_replication_state: self.global_replication_state.clone(),
            store_stat: global_stat.local(),
            global_stat,
            engines: self.engines.clone(),
            current_time: None,
            node_start_time: Some(TiInstant::now_coarse()),
            apply_batch_system: self.apply_batch_system.clone(),
            remote: self.remote.clone(),
            cfg_tracker: self.cfg_tracker.clone(),
        }
    }
}
impl<EK, ER, T> PollContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport + 'static,
{
    #[inline]
    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn register(&self, new_region_id: u64, fsm: PeerFsm<EK, ER>) {
        let (tx, rx) = unbounded_channel();
        let ctx = self.clone();
        self.remote.spawn(async move {
            handle_normal(rx, ctx, fsm).await;
        });

        self.router.register(new_region_id, tx);
        self.router
            .force_send(new_region_id, PeerMsg::Start)
            .unwrap();
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

    pub fn flush(&mut self) {
        self.raft_metrics.flush();
        self.store_stat.flush();
        self.trans.as_mut().unwrap().flush();
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
                if let Err(e) = mb.send(StoreMsg::Tick(tick)) {
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
        if let Err(e) = self.trans.as_mut().unwrap().send(gc_msg) {
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
    start_time: Option<Timespec>,
    consistency_check_time: HashMap<u64, Instant>,
    last_unreachable_report: HashMap<u64, Instant>,
}

impl Store {
    pub fn new() -> Store {
        Store {
            id: 0,
            last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
            start_time: None,
            consistency_check_time: HashMap::default(),
            last_unreachable_report: HashMap::default(),
        }
    }
}

struct StoreFsmDelegate<EK: KvEngine + 'static, ER: RaftEngine + 'static, T: Transport + 'static> {
    store: Store,
    ctx: PollContext<EK, ER, T>,
}

impl<EK: KvEngine + 'static, ER: RaftEngine + 'static, T: Transport> StoreFsmDelegate<EK, ER, T> {
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
            self.store.id,
            tick
        );
    }

    fn handle_msgs(&mut self, m: StoreMsg<EK>) {
        match m {
            StoreMsg::Tick(tick) => self.on_tick(tick),
            StoreMsg::RaftMessage(msg) => {
                if let Err(e) = self.on_raft_message(msg) {
                    error!(?e;
                        "handle raft message failed";
                        "store_id" => self.store.id,
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
            StoreMsg::UpdateReplicationMode(status) => self.on_update_replication_mode(status),
        }
    }

    fn start(&mut self, store: metapb::Store) {
        if self.store.start_time.is_some() {
            panic!(
                "[store {}] unable to start again with meta {:?}",
                self.store.id, store
            );
        }
        self.store.id = store.get_id();
        self.store.start_time = Some(time::get_time());
        self.register_cleanup_import_sst_tick();
        self.register_compact_check_tick();
        self.register_pd_store_heartbeat_tick();
        self.register_compact_lock_cf_tick();
        self.register_snap_mgr_gc_tick();
        self.register_consistency_check_tick();
        self.register_raft_engine_purge_tick();
    }
}

fn begin<EK, ER, T>(poll_ctx: &mut PollContext<EK, ER, T>)
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport + 'static,
{
    // update config
    if let Some(incoming) = poll_ctx.cfg_tracker.any_new() {
        poll_ctx.cfg = incoming.clone();
    }
}

async fn handle_control<EK, ER, T>(
    mut receiver: UnboundedReceiver<StoreMsg<EK>>,
    poll_ctx: PollContext<EK, ER, T>,
    store: Store,
) where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport + 'static,
{
    let mut delegate = StoreFsmDelegate {
        store,
        ctx: poll_ctx,
    };
    while let Some(msg) = receiver.recv().await {
        begin(&mut delegate.ctx);
        delegate.handle_msgs(msg);
    }
}

struct ReceiverWrapper<EK: KvEngine> {
    receiver: UnboundedReceiver<PeerMsg<EK>>,
    region_id: u64,
}

impl<EK: KvEngine> Drop for ReceiverWrapper<EK> {
    fn drop(&mut self) {
        while let Ok(m) = self.receiver.try_recv() {
            let callback = match m {
                PeerMsg::RaftCommand(cmd) => cmd.callback,
                PeerMsg::CasualMessage(CasualMessage::SplitRegion { callback, .. }) => callback,
                _ => continue,
            };

            let mut err = errorpb::Error::default();
            err.set_message("region is not found".to_owned());
            err.mut_region_not_found().set_region_id(self.region_id);
            let mut resp = RaftCmdResponse::default();
            resp.mut_header().set_error(err);
            callback.invoke_with_response(resp);
        }
    }
}

async fn handle_normal<EK, ER, T>(
    receiver: UnboundedReceiver<PeerMsg<EK>>,
    poll_ctx: PollContext<EK, ER, T>,
    fsm: PeerFsm<EK, ER>,
) where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport + 'static,
{
    fail_point!(
        "pause_on_peer_collect_message",
        fsm.peer_id() == 1,
        |_| unreachable!()
    );

    let mut receiver = ReceiverWrapper {
        receiver,
        region_id: fsm.region_id(),
    };

    let mut delegate = PeerFsmDelegate::new(fsm, poll_ctx);
    while let Some(msg) = receiver.receiver.recv().await {
        // TODO: we may need a way to optimize the message copy.
        begin(&mut delegate.ctx);
        delegate.ctx.trans = Some(take_tls_trans());
        delegate.handle_msg(msg);
        while let Ok(msg) = receiver.receiver.try_recv() {
            fail_point!(
                "pause_on_peer_destroy_res",
                delegate.fsm.peer_id() == 1
                    && match msg {
                        PeerMsg::ApplyRes {
                            res: ApplyTaskRes::Destroy { .. },
                        } => true,
                        _ => false,
                    },
                |_| unreachable!()
            );
            delegate.handle_msg(msg);
            if delegate.fsm.is_stop() {
                break;
            }
        }
        if let Some((mut ready, invoke_ctx)) = delegate.collect_ready() {
            delegate.handle_raft_ready_apply(&mut ready);
            set_tls_trans(delegate.ctx.trans.take().unwrap());
            delegate.flush().await;
            delegate.ctx.trans = Some(take_tls_trans());
            delegate.post_raft_ready_append(ready, invoke_ctx);
        } else if delegate.fsm.is_stop() {
            receiver.receiver.close();
        }
        set_tls_trans(delegate.ctx.trans.take().unwrap());
    }
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> PollContext<EK, ER, T> {
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

            let (rec, mut peer) = box_try!(PeerFsm::create(
                store_id,
                &self.cfg,
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
            region_peers.push((rec, peer));
            self.coprocessor_host.on_region_changed(
                region,
                RegionChangeEvent::Create,
                StateRole::Follower,
            );
            Ok(true)
        })?;

        if !kv_wb.is_empty() {
            self.engines.kv.write(&kv_wb).unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.engines.raft.consume(&mut raft_wb, true).unwrap();
        }

        // schedule applying snapshot after raft writebatch were written.
        for region in applying_regions {
            info!("region is applying snapshot"; "region" => ?region, "store_id" => store_id);
            let (receiver, mut peer) = PeerFsm::create(
                store_id,
                &self.cfg,
                self.region_scheduler.clone(),
                self.engines.clone(),
                &region,
            )?;
            peer.peer.init_replication_mode(&mut *replication_state);
            peer.schedule_applying_snapshot();
            meta.region_ranges
                .insert(enc_end_key(&region), region.get_id());
            meta.regions.insert(region.get_id(), region);
            region_peers.push((receiver, peer));
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

struct Workers<EK: KvEngine> {
    pd_worker: FutureWorker<PdTask<EK>>,
    consistency_check_worker: Worker<ConsistencyCheckTask<EK::Snapshot>>,
    split_check_worker: Worker<SplitCheckTask>,
    // handle Compact, CleanupSST task
    cleanup_worker: Worker<CleanupTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    region_worker: Worker<RegionTask<EK::Snapshot>>,
    coprocessor_host: CoprocessorHost<EK>,
}

pub struct RaftBatchSystem<EK: KvEngine> {
    pool: Option<ThreadPool<TaskCell>>,
    store_rx: Option<UnboundedReceiver<StoreMsg<EK>>>,
    router: RaftRouter<EK>,
    workers: Option<Workers<EK>>,
    apply_system: Option<ApplyBatchSystem<EK>>,
}
use std::cell::UnsafeCell;
use std::ptr;

thread_local! {
    // A pointer to thread local Transport. Use raw pointer and `UnsafeCell` to reduce runtime check.
    static TLS_TRANSPORT: UnsafeCell<*mut ()> = UnsafeCell::new(ptr::null_mut());
}

/// Set the thread local ApplyContext.
///
/// Postcondition: `TLS_TRANSPORT` is non-null.
fn set_tls_trans<T: Transport>(trans: T) {
    // Safety: we check that `TLS_ENGINE_ANY` is null to ensure we don't leak an existing
    // engine; we ensure there are no other references to `engine`.
    TLS_TRANSPORT.with(move |e| unsafe {
        if (*e.get()).is_null() {
            let trans = Box::into_raw(Box::new(Some(trans))) as *mut ();
            *e.get() = trans;
            return;
        }
        let ctx = &mut *(*e.get() as *mut Option<T>);
        ctx.replace(trans);
    });
}

fn take_tls_trans<T: Transport>() -> T {
    TLS_TRANSPORT.with(|e| unsafe {
        let ctx = &mut *(*e.get() as *mut Option<T>);
        ctx.take().unwrap()
    })
}

pub fn flush_tls_trans<T: Transport>() {
    TLS_TRANSPORT.with(|e| unsafe {
        if (*e.get()).is_null() {
            return;
        }
        let trans = &mut *(*e.get() as *mut Option<T>);
        if let Some(t) = trans.as_mut() {
            t.flush();
        }
    })
}

#[derive(Clone)]
struct TransTicker<T: Transport + 'static> {
    _data: PhantomData<T>,
}

impl<T: Transport + 'static> Default for TransTicker<T> {
    fn default() -> Self {
        Self { _data: PhantomData }
    }
}

impl<T: Transport + 'static> PoolTicker for TransTicker<T> {
    fn on_tick(&mut self) {
        flush_tls_trans::<T>();
    }
}

impl<EK: KvEngine> RaftBatchSystem<EK> {
    pub fn router(&self) -> RaftRouter<EK> {
        self.router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn<ER: RaftEngine, T: Transport + 'static, C: PdClient + 'static>(
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
        split_check_worker: Worker<SplitCheckTask>,
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
            split_check_worker,
            region_worker: Worker::new("snapshot-worker"),
            pd_worker,
            consistency_check_worker: Worker::new("consistency-check"),
            cleanup_worker: Worker::new("cleanup-worker"),
            raftlog_gc_worker: Worker::new("raft-gc-worker"),
            coprocessor_host: coprocessor_host.clone(),
        };

        let pending_create_peers = Arc::new(Mutex::new(HashMap::default()));
        let region_scheduler = workers.region_worker.scheduler();
        let apply_system = create_apply_batch_system(
            meta.get_id(),
            format!("[store {}]", meta.get_id()),
            workers.coprocessor_host.clone(),
            importer.clone(),
            region_scheduler.clone(),
            engines.kv.clone(),
            Box::new(self.router.clone()),
            pending_create_peers.clone(),
            &cfg.value(),
        );
        let shared_trans = Arc::new(Mutex::new(trans));

        let pool = YatpPoolBuilder::new(TransTicker::<T>::default())
            .name_prefix("store")
            .thread_count(
                cfg.value().store_batch_system.pool_size,
                cfg.value().store_batch_system.pool_size,
            )
            .after_start(move || {
                let trans = shared_trans.lock().unwrap().clone();
                set_tls_trans(trans);
            })
            .before_pause(flush_tls_trans::<T>)
            .before_stop(|| {
                let mut trans: T = take_tls_trans();
                trans.flush();
            })
            .build_single_level_pool();

        let global_stat = GlobalStoreStat::default();
        let tag = format!("[store {}]", meta.get_id());
        let mut ctx = PollContext::<EK, ER, T> {
            cfg: cfg.value().clone(),
            store: meta,
            engines,
            current_time: None,
            region_scheduler,
            router: self.router.clone(),
            split_check_scheduler: workers.split_check_worker.scheduler(),
            pd_scheduler: workers.pd_worker.scheduler(),
            consistency_check_scheduler: workers.consistency_check_worker.scheduler(),
            cleanup_scheduler: workers.cleanup_worker.scheduler(),
            raftlog_gc_scheduler: workers.raftlog_gc_worker.scheduler(),
            coprocessor_host,
            timer: Default::default(),
            importer,
            snap_mgr: mgr,
            global_replication_state,
            store_meta,
            pending_create_peers,
            applying_snap_count: Arc::new(AtomicUsize::new(0)),
            apply_batch_system: Arc::new(apply_system.clone()),
            remote: pool.remote().clone(),
            raft_metrics: Default::default(),
            trans: None,
            store_stat: global_stat.local(),
            global_stat,
            node_start_time: Some(TiInstant::now_coarse()),
            cfg_tracker: cfg.clone().tracker(tag),
        };
        self.apply_system = Some(apply_system);
        self.pool = Some(pool);
        let region_peers = ctx.init()?;
        self.start_system(
            workers,
            region_peers,
            ctx,
            pd_client,
            auto_split_controller,
            concurrency_manager,
        )?;
        Ok(())
    }

    fn start_system<ER: RaftEngine, T: Transport + 'static, C: PdClient + 'static>(
        &mut self,
        mut workers: Workers<EK>,
        region_peers: Vec<SenderFsmPair<EK, ER>>,
        ctx: PollContext<EK, ER, T>,
        pd_client: Arc<C>,
        auto_split_controller: AutoSplitController,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()> {
        ctx.snap_mgr.init()?;
        let engines = ctx.engines.clone();
        let snap_mgr = ctx.snap_mgr.clone();
        let cfg = ctx.cfg.clone();
        let store = ctx.store.clone();
        let importer = ctx.importer.clone();
        {
            let mut meta = ctx.store_meta.lock().unwrap();
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

        let mut address = Vec::with_capacity(region_peers.len());
        for (receiver, fsm) in region_peers {
            address.push(fsm.region_id());
            ctx.apply_batch_system
                .register(Registration::new(fsm.get_peer()), receiver);
            ctx.register(fsm.region_id(), fsm);
        }
        let store_fsm = Store::new();
        let store_rx = self.store_rx.take().unwrap();
        let store_ctx = ctx.clone();
        self.pool.as_mut().unwrap().spawn(async move {
            handle_control(store_rx, store_ctx, store_fsm).await;
        });

        // Make sure Msg::Start is the first message each FSM received.
        for addr in address {
            self.router.send(addr, PeerMsg::Start).unwrap();
        }
        self.router
            .send_control(StoreMsg::Start {
                store: store.clone(),
            })
            .unwrap();

        let region_runner = RegionRunner::new(
            engines.clone(),
            snap_mgr,
            cfg.snap_apply_batch_size.0 as usize,
            cfg.use_delete_range,
            workers.coprocessor_host.clone(),
            self.router(),
        );
        let timer = region_runner.new_timer();
        box_try!(workers.region_worker.start_with_timer(region_runner, timer));

        let raftlog_gc_runner = RaftlogGcRunner::new(self.router(), engines.clone());
        let timer = raftlog_gc_runner.new_timer();
        box_try!(workers
            .raftlog_gc_worker
            .start_with_timer(raftlog_gc_runner, timer));

        let compact_runner = CompactRunner::new(engines.kv.clone());
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
            engines.kv,
            workers.pd_worker.scheduler(),
            cfg.pd_store_heartbeat_tick_interval.0,
            auto_split_controller,
            concurrency_manager,
        );
        box_try!(workers.pd_worker.start(pd_runner));

        let consistency_check_runner =
            ConsistencyCheckRunner::<EK, _>::new(self.router.clone(), ctx.coprocessor_host);
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
        self.router.shutdown();
        if let Some(apply_system) = self.apply_system.take() {
            apply_system.shutdown();
        }
        if let Some(pool) = self.pool.take() {
            pool.shutdown();
        }
        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }
        workers.coprocessor_host.shutdown();
    }
}

pub fn create_raft_batch_system<EK: KvEngine>(_: &Config) -> (RaftRouter<EK>, RaftBatchSystem<EK>) {
    let (store_tx, store_rx) = unbounded_channel();
    let raft_router = RaftRouter {
        router: AsyncRouter::new(store_tx),
    };

    let system = RaftBatchSystem {
        pool: None,
        store_rx: Some(store_rx),
        workers: None,
        router: raft_router.clone(),
        apply_system: None,
    };
    (raft_router, system)
}

#[derive(Debug, PartialEq)]
enum CheckMsgStatus {
    // The message is the first request vote message to an existing peer.
    FirstRequestVote,
    // The message can be dropped silently
    DropMsg,
    // Try to create the peer
    NewPeer,
    // Try to create the peer which is the first one of this region on local store.
    NewPeerFirst,
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport> StoreFsmDelegate<EK, ER, T> {
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
            if !util::is_first_vote_msg(msg.get_message()) {
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
            return Ok(CheckMsgStatus::FirstRequestVote);
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
                if let Err(e) = self.ctx.trans.as_mut().unwrap().send(send_msg) {
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
            Ok(()) => return Ok(()),
            Err(AsyncRouterError::Closed(_)) if self.ctx.router.is_shutdown() => return Ok(()),
            Err(AsyncRouterError::Closed(PeerMsg::RaftMessage(m))) => msg = m,
            Err(AsyncRouterError::NotExist(PeerMsg::RaftMessage(m))) => msg = m,
            e => panic!(
                "[store {}] [region {}] unexpected redirect error: {:?}",
                self.store.id, region_id, e
            ),
        }

        debug!(
            "handle raft message";
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
            "store_id" => self.store.id,
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
        let is_first_request_vote = match check_msg_status {
            CheckMsgStatus::DropMsg => return Ok(()),
            CheckMsgStatus::FirstRequestVote => true,
            CheckMsgStatus::NewPeer | CheckMsgStatus::NewPeerFirst => {
                if !self.maybe_create_peer(
                    region_id,
                    &msg,
                    check_msg_status == CheckMsgStatus::NewPeerFirst,
                )? {
                    if !util::is_first_vote_msg(msg.get_message()) {
                        // Can not create peer from the message and it's not the
                        // first request vote message.
                        return Ok(());
                    }
                    true
                } else {
                    false
                }
            }
        };
        if is_first_request_vote {
            // To void losing request vote messages, either put it to
            // pending_votes or force send.
            let mut store_meta = self.ctx.store_meta.lock().unwrap();
            if !store_meta.regions.contains_key(&region_id) {
                store_meta.pending_votes.push(msg);
                return Ok(());
            }
            if let Err(e) = self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg)) {
                warn!("handle first request vote failed"; "region_id" => region_id, "error" => ?e);
            }
            return Ok(());
        }

        let _ = self.ctx.router.send(region_id, PeerMsg::RaftMessage(msg));
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
        if is_local_first {
            if self
                .ctx
                .engines
                .kv
                .get_value_cf(CF_RAFT, &keys::region_state_key(region_id))?
                .is_some()
            {
                return Ok(false);
            }
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
                let _ = self.ctx.router.send(
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
                .send(
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
        let (receiver, mut peer) = PeerFsm::replicate(
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

        self.ctx
            .apply_batch_system
            .register(Registration::new(peer.get_peer()), receiver);
        self.ctx.register(region_id, peer);
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
                "store_id" => self.store.id,
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
                "store_id" => self.store.id,
            );
            return;
        }

        // Start from last checked key.
        let mut ranges_need_check =
            Vec::with_capacity(self.ctx.cfg.region_compact_check_step as usize + 1);
        ranges_need_check.push(self.store.last_compact_checked_key.clone());

        let largest_key = {
            let meta = self.ctx.store_meta.lock().unwrap();
            if meta.region_ranges.is_empty() {
                debug!(
                    "there is no range need to check";
                    "store_id" => self.store.id
                );
                return;
            }

            // Collect continuous ranges.
            let left_ranges = meta.region_ranges.range((
                Excluded(self.store.last_compact_checked_key.clone()),
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
            self.store.last_compact_checked_key = keys::DATA_MIN_KEY.to_vec();
        } else {
            self.store.last_compact_checked_key = last_key;
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
                "store_id" => self.store.id,
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

        stats.set_start_time(self.store.start_time.unwrap().sec as u32);

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
                "store_id" => self.store.id,
                "err" => ?e
            );
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick();
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        fail_point!("peer_2_handle_snap_mgr_gc", self.store.id == 2, |_| Ok(()));
        let snap_keys = self.ctx.snap_mgr.list_idle_snap()?;
        if snap_keys.is_empty() {
            return Ok(());
        }
        let (mut last_region_id, mut keys) = (0, vec![]);
        let schedule_gc_snap = |region_id: u64, snaps| -> Result<()> {
            debug!(
                "schedule snap gc";
                "store_id" => self.store.id,
                "region_id" => region_id,
            );
            let gc_snap = PeerMsg::CasualMessage(CasualMessage::GcSnap { snaps });
            match self.ctx.router.send(region_id, gc_snap) {
                Ok(()) => Ok(()),
                Err(AsyncRouterError::NotExist(_)) if self.ctx.router.is_shutdown() => Ok(()),
                Err(AsyncRouterError::Closed(_)) if self.ctx.router.is_shutdown() => Ok(()),
                Err(AsyncRouterError::Closed(PeerMsg::CasualMessage(CasualMessage::GcSnap {
                    snaps,
                }))) => {
                    // The snapshot exists because MsgAppend has been rejected. So the
                    // peer must have been exist. But now it's disconnected, so the peer
                    // has to be destroyed instead of being created.
                    info!(
                        "region is disconnected, remove snaps";
                        "region_id" => region_id,
                        "snaps" => ?snaps,
                    );
                    for (key, is_sending) in snaps {
                        let snap = if is_sending {
                            self.ctx.snap_mgr.get_snapshot_for_sending(&key)?
                        } else {
                            self.ctx.snap_mgr.get_snapshot_for_applying(&key)?
                        };
                        self.ctx
                            .snap_mgr
                            .delete_snapshot(&key, snap.as_ref(), false);
                    }
                    Ok(())
                }
                Err(_) => unreachable!(),
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
                "store_id" => self.store.id,
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
                    "store_id" => self.store.id,
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

impl<EK: KvEngine, ER: RaftEngine, T: Transport> StoreFsmDelegate<EK, ER, T> {
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
                "store_id" => self.store.id,
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
                    "store_id" => self.store.id,
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
                   "store_id" => self.store.id,
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
                match self.store.consistency_check_time.get(region_id) {
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
            "store_id" => self.store.id,
            "region_id" => target_region_id,
        );
        self.store
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
                "store_id" => self.store.id,
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
            "store_id" => self.store.id,
            "unreachable_store_id" => store_id,
        );
        self.store.last_unreachable_report.insert(store_id, now);
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
        let _ = self.ctx.raftlog_gc_scheduler.schedule(RaftlogGcTask::Purge);
        self.register_raft_engine_purge_tick();
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
