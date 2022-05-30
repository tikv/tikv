// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use concurrency_manager::ConcurrencyManager;
use fail::fail_point;
use kvproto::{
    metapb::{self, Region, RegionEpoch},
    pdpb,
    raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState},
};
use pd_client::PdClient;
use protobuf::Message;
use raft::{eraftpb::ConfChangeType, StateRole};
use raftstore::{
    coprocessor::{
        split_observer::SplitObserver, BoxAdminObserver, CoprocessorHost, RegionChangeEvent,
    },
    store::{
        local_metrics::RaftMetrics,
        util,
        util::{is_initial_msg, is_region_initialized},
    },
};
use tikv_util::{
    box_err,
    config::VersionTrack,
    debug, error, info,
    mpsc::Receiver,
    warn,
    worker::{LazyWorker, Scheduler},
    RingQueue,
};
use time::Timespec;

use super::{Config, *};
use crate::{store::peer_worker::ApplyWorker, RaftRouter, RaftStoreRouter, Result};

pub const PENDING_MSG_CAP: usize = 100;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);

struct Workers {
    pd_worker: LazyWorker<PdTask>,
    coprocessor_host: CoprocessorHost<kvengine::Engine>,
}

pub struct RaftBatchSystem {
    router: RaftRouter,

    // Change to some after spawn.
    workers: Option<Workers>,

    // Change to none after spawn.
    peer_receiver: Option<Receiver<(u64, PeerMsg)>>,
    store_fsm: Option<StoreFSM>,
}

impl RaftBatchSystem {
    pub fn new(engines: &Engines, conf: &Config) -> Self {
        let (store_sender, store_receiver) =
            engines.meta_change_channel.lock().unwrap().take().unwrap();
        let (peer_sender, peer_receiver) = tikv_util::mpsc::unbounded();
        let router = RaftRouter::new(peer_sender, store_sender);
        Self {
            router,
            workers: None,
            peer_receiver: Some(peer_receiver),
            store_fsm: Some(StoreFSM::new(store_receiver, conf)),
        }
    }

    pub fn router(&self) -> RaftRouter {
        self.router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn(
        &mut self,
        meta: metapb::Store,
        cfg: Arc<VersionTrack<Config>>,
        engines: Engines,
        trans: Box<dyn Transport>,
        pd_client: Arc<dyn PdClient>,
        pd_worker: LazyWorker<PdTask>,
        mut store_meta: StoreMeta,
        mut coprocessor_host: CoprocessorHost<kvengine::Engine>,
        concurrency_manager: ConcurrencyManager,
    ) -> Result<()> {
        assert!(self.workers.is_none());
        // TODO: we can get cluster meta regularly too later.

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, BoxAdminObserver::new(SplitObserver));

        let mut workers = Workers {
            pd_worker,
            coprocessor_host: coprocessor_host.clone(),
        };
        let pd_scheduler = workers.pd_worker.scheduler();
        let ctx = GlobalContext {
            cfg,
            engines,
            store: meta,
            readers: store_meta.readers.clone(),
            router: self.router.clone(),
            trans,
            pd_scheduler,
            coprocessor_host,
        };
        let mut region_peers = self.load_peers(&ctx, &mut store_meta)?;
        for peer_fsm in &region_peers {
            let peer = peer_fsm.get_peer();
            store_meta
                .readers
                .insert(peer_fsm.region_id(), ReadDelegate::from_peer(peer));
        }
        let mut region_ids = Vec::with_capacity(region_peers.len());
        let mut store_ctx = StoreContext::new(RaftContext::new(ctx.clone()), store_meta);
        let mut store_fsm = self.store_fsm.take().unwrap();
        for peer in region_peers.drain(..) {
            region_ids.push(peer.peer.region_id);
            let mut store_handler = StoreMsgHandler::new(&mut store_fsm, &mut store_ctx);
            store_handler.register(peer);
        }
        let store_id = ctx.store.get_id();
        let pd_runner = PdRunner::new(
            store_id,
            pd_client,
            self.router.clone(),
            workers.pd_worker.scheduler(),
            ctx.cfg.value().pd_store_heartbeat_tick_interval.into(),
            concurrency_manager,
            workers.pd_worker.remote(),
            ctx.engines.kv.clone(),
        );
        assert!(workers.pd_worker.start(pd_runner));
        self.workers = Some(workers);

        let (mut io_worker, io_sender) = IOWorker::new(
            ctx.engines.raft.clone(),
            self.router.clone(),
            ctx.trans.clone(),
        );
        let mut sync_io_worker = None;
        if ctx.cfg.value().async_io {
            let props = tikv_util::thread_group::current_properties();
            std::thread::Builder::new()
                .name("raft_io".to_string())
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    io_worker.run();
                })
                .unwrap();
        } else {
            sync_io_worker = Some(io_worker);
        }
        let peer_receiver = self.peer_receiver.take().unwrap();
        let (mut rw, mut apply_receivers) = RaftWorker::new(
            store_ctx,
            peer_receiver,
            ctx.router.clone(),
            io_sender,
            sync_io_worker,
            store_fsm,
        );
        let props = tikv_util::thread_group::current_properties();
        std::thread::Builder::new()
            .name("raftstore_0".to_string())
            .spawn(move || {
                tikv_util::thread_group::set_properties(props);
                rw.run();
            })
            .unwrap();

        for (i, apply_receiver) in apply_receivers.drain(..).enumerate() {
            let props = tikv_util::thread_group::current_properties();
            let mut aw =
                ApplyWorker::new(ctx.engines.kv.clone(), ctx.router.clone(), apply_receiver);
            let _peer_handle = std::thread::Builder::new()
                .name(format!("apply_{}", i))
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    aw.run();
                })
                .unwrap();
        }
        self.router.send_store(StoreMsg::Start {
            store: ctx.store.clone(),
        });
        for region_id in region_ids {
            self.router.send(region_id, PeerMsg::Start);
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        // TODO(x): stop the raft and apply worker.
        let mut workers = self.workers.take().unwrap();
        // Wait all workers finish.
        workers.pd_worker.stop();
        fail_point!("after_shutdown_apply");
        workers.coprocessor_host.shutdown();
    }

    /// load_peers loads peers in this store. It scans the kv engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn load_peers(&self, ctx: &GlobalContext, store_meta: &mut StoreMeta) -> Result<Vec<PeerFsm>> {
        // Scan region meta to get saved regions.
        let mut regions = vec![];
        let mut last_region_id: u64 = 0;
        ctx.engines
            .raft
            .iterate_all_states(true, |region_id, key, val| -> bool {
                if region_id == last_region_id {
                    return true;
                }
                if key[0] != REGION_META_KEY_BYTE {
                    return true;
                }
                last_region_id = region_id;
                let mut local_state = RegionLocalState::default();
                local_state.merge_from_bytes(val).unwrap();
                regions.push(local_state.get_region().clone());
                true
            });
        let mut peers = vec![];
        let store_id = ctx.store.id;
        for region in &regions {
            let mut peer =
                PeerFsm::create(store_id, &ctx.cfg.value(), ctx.engines.clone(), region)?;
            let shard = ctx.engines.kv.get_shard(region.get_id()).unwrap();
            let peer_store = peer.peer.mut_store();
            peer_store.initial_flushed = shard.get_initial_flushed();
            store_meta
                .region_ranges
                .insert(raw_end_key(region), region.get_id());
            store_meta.regions.insert(region.get_id(), region.clone());
            ctx.coprocessor_host.on_region_changed(
                region,
                RegionChangeEvent::Create,
                StateRole::Follower,
            );
            peers.push(peer);
        }
        Ok(peers)
    }
}

pub struct StoreInfo {
    pub kv_engine: kvengine::Engine,
    pub capacity: u64,
}

pub struct StoreMeta {
    /// store id
    pub store_id: Option<u64>,
    /// region_end_key -> region_id
    pub region_ranges: BTreeMap<Bytes, u64>,
    /// region_id -> region
    pub regions: HashMap<u64, Region>,

    pub cop_host: CoprocessorHost<kvengine::Engine>,
    /// region_id -> reader
    pub readers: Arc<dashmap::DashMap<u64, ReadDelegate>>,
    /// `MsgRequestPreVote`, `MsgRequestVote` or `MsgAppend` messages from newly split Regions shouldn't be
    /// dropped if there is no such Region in this store now. So the messages are recorded temporarily and
    /// will be handled later.
    pub pending_msgs: RingQueue<RaftMessage>,

    /// pending_new_regions is used to avoid the race between create peer by split and create peer by
    /// raft message.
    ///
    /// Events:
    ///
    /// A1: The split log is committed.
    ///     If the new region is found and initialized, avoid update the new region.
    ///     Otherwise add pending create in store meta.
    ///
    /// A2: Handle apply result of split.
    ///     If the new region is found, it must be initialized, then avoid create the peer.
    ///     Otherwise, create the peer and remove pending create.
    ///
    /// B1: store handle raft message maybe create peer.
    ///     If region or pending create is found, avoid create the peer.
    ///     Otherwise create the peer with the uninitialized region.
    ///
    /// B2: new peer recevied snapshot.
    ///     If prev region is uninitialized and the region in store is initialized, avoid update the store meta.
    ///     Otherwise update the store meta.
    ///
    /// There are 5 possible sequences to analyze:
    ///
    /// - A1, A2
    ///     After A2, the peer can be found in router.
    ///
    /// - A1, B1, A2
    ///     A1 set pending create in store meta.
    ///     B1 found the pending create in store meta, then avoid create peer.
    ///
    /// - B1, A1, A2, B2,
    ///     B1 create the peer in the store meta.
    ///     A1 found the peer is already exists in store meta, but region is not initialized, add pending create.
    ///     A2 replace the uninitialized region by split, removed the pending create.
    ///     B2 found no pending create, and prev region is not initialized, meta region is initialized, avoid update the meta.
    ///
    /// - B1, A1, B2, A2
    ///     B1 create the peer in the store meta.
    ///     A1 found the peer is already exists in store meta, but region is not initialized, add pending create.
    ///     B2 found pending create, avoid update the meta.
    ///     A2 replace the uninitialized region by split, removed the pending create.
    ///
    /// - B1. B2, A1, A2
    ///     B2 updated the meta with initialized region.
    ///     A1 found region is initialized, do not add pending create.
    ///     A2 do not create the initialized region on ready split.
    pub(crate) pending_new_regions: HashMap<u64, bool>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize, cop_host: CoprocessorHost<kvengine::Engine>) -> StoreMeta {
        StoreMeta {
            store_id: None,
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
            cop_host,
            readers: Arc::new(dashmap::DashMap::new()),
            pending_msgs: RingQueue::with_capacity(vote_capacity),
            pending_new_regions: HashMap::default(),
        }
    }

    #[inline]
    pub(crate) fn set_region(&mut self, region: Region, peer: &mut crate::store::Peer) {
        let region_id = region.get_id();
        let prev = self.regions.insert(region_id, region.clone());
        if prev.map_or(true, |r| r.get_id() != region_id) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.region_id);
        }
        peer.set_region(&self.cop_host, region);
        self.readers
            .insert(region_id, ReadDelegate::from_peer(peer));
    }
}

#[derive(Clone)]
pub(crate) struct GlobalContext {
    pub(crate) cfg: Arc<VersionTrack<Config>>,
    pub(crate) engines: Engines,
    pub(crate) store: metapb::Store,
    pub(crate) readers: Arc<dashmap::DashMap<u64, ReadDelegate>>,
    pub(crate) router: RaftRouter,
    pub(crate) trans: Box<dyn Transport>,
    pub(crate) pd_scheduler: Scheduler<PdTask>,
    pub(crate) coprocessor_host: CoprocessorHost<kvengine::Engine>,
}

pub(crate) struct RaftContext {
    pub(crate) global: GlobalContext,
    pub(crate) apply_msgs: ApplyMsgs,
    pub(crate) persist_readies: Vec<PersistReady>,
    pub(crate) raft_wb: rfengine::WriteBatch,
    pub(crate) current_time: Option<Timespec>,
    pub(crate) raft_metrics: RaftMetrics,
    pub(crate) cfg: Config,
}

// There is only one StoreContext owned by the main raft worker.
pub(crate) struct StoreContext {
    pub(crate) raft_ctx: RaftContext,
    pub(crate) peers: HashMap<u64, PeerStates>,
    pub(crate) store_meta: StoreMeta,
}

impl StoreContext {
    pub(crate) fn new(raft_ctx: RaftContext, store_meta: StoreMeta) -> StoreContext {
        StoreContext {
            raft_ctx,
            peers: HashMap::new(),
            store_meta,
        }
    }
}

impl Deref for StoreContext {
    type Target = RaftContext;

    fn deref(&self) -> &Self::Target {
        &self.raft_ctx
    }
}

impl DerefMut for StoreContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raft_ctx
    }
}

impl RaftContext {
    pub(crate) fn new(global: GlobalContext) -> Self {
        let cfg = global.cfg.value().clone();
        Self {
            global,
            apply_msgs: ApplyMsgs { msgs: vec![] },
            persist_readies: vec![],
            raft_wb: rfengine::WriteBatch::new(),
            current_time: None,
            raft_metrics: RaftMetrics::new(false),
            cfg,
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
        if let Err(e) = self.global.trans.send(gc_msg) {
            error!(?e;
                "send gc message failed";
                "region_id" => region_id,
            );
        }
    }

    pub(crate) fn store_id(&self) -> u64 {
        self.global.store.get_id()
    }
}

pub(crate) struct StoreFSM {
    pub(crate) id: u64,
    pub(crate) start_time: Option<Timespec>,
    pub(crate) receiver: Receiver<StoreMsg>,
    pub(crate) ticker: Ticker,
    pub(crate) last_tick: Instant,
    pub(crate) tick_millis: u64,
    last_unreachable_report: HashMap<u64, Instant>,
}

impl StoreFSM {
    pub fn new(receiver: Receiver<StoreMsg>, cfg: &Config) -> Self {
        Self {
            id: 0,
            start_time: None,
            receiver,
            ticker: Ticker::new_store(cfg),
            last_tick: Instant::now(),
            tick_millis: cfg.pd_store_heartbeat_tick_interval.as_millis(),
            last_unreachable_report: HashMap::new(),
        }
    }
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

pub(crate) struct StoreMsgHandler<'a> {
    store: &'a mut StoreFSM,
    pub(crate) ctx: &'a mut StoreContext,
}

impl<'a> StoreMsgHandler<'a> {
    pub(crate) fn new(store: &'a mut StoreFSM, ctx: &'a mut StoreContext) -> StoreMsgHandler<'a> {
        Self { store, ctx }
    }

    pub(crate) fn get_receiver(&self) -> &Receiver<StoreMsg> {
        &self.store.receiver
    }

    pub(crate) fn handle_msg(&mut self, msg: StoreMsg) -> Option<u64> {
        let mut apply_region = None;
        match msg {
            StoreMsg::Tick => self.on_tick(),
            StoreMsg::Start { store } => self.start(store),
            StoreMsg::StoreUnreachable { store_id } => self.on_store_unreachable(store_id),
            StoreMsg::GenerateEngineChangeSet(cs) => self.on_generate_engine_meta_change(cs),
            StoreMsg::SplitRegion(regions) => {
                apply_region = self.on_split_region(regions);
            }
            StoreMsg::ChangePeer(cp) => {
                apply_region = self.on_change_peer(cp);
            }
            StoreMsg::DestroyPeer(region_id) => self.on_destroy_peer(region_id, false),
            StoreMsg::RaftMessage(msg) => self.on_raft_message(msg),
            StoreMsg::SnapshotReady(region_id) => {
                apply_region = self.on_snapshot_ready(region_id);
            }
            StoreMsg::PendingNewRegions(new_regions) => self.on_pending_new_regions(new_regions),
        }
        apply_region
    }

    fn on_tick(&mut self) {
        self.store.ticker.tick_clock();
        if self.store.ticker.is_on_store_tick(STORE_TICK_PD_HEARTBEAT) {
            self.on_pd_heartbeat_tick();
        }
        if self
            .store
            .ticker
            .is_on_store_tick(STORE_TICK_UPDATE_SAFE_TS)
        {
            self.on_update_safe_ts();
        }
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = pdpb::StoreStats::default();

        stats.set_store_id(self.store.id);
        stats.set_region_count(self.ctx.store_meta.regions.len() as u32);

        stats.set_start_time(self.store.start_time.unwrap().sec as u32);

        // TODO(x): report store write flow to pd.

        let store_info = StoreInfo {
            kv_engine: self.ctx.global.engines.kv.clone(),
            capacity: self.ctx.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat {
            stats,
            store_info,
            send_detailed_report: false,
        };
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
            error!("notify pd failed";
                "store_id" => self.store.id,
                "err" => ?e
            );
        }
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.store.ticker.schedule_store(STORE_TICK_PD_HEARTBEAT);
    }

    fn on_update_safe_ts(&mut self) {
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(PdTask::UpdateSafeTS) {
            error!("update safe ts failed";
                "store_id" => self.store.id,
                "err" => ?e
            );
        }
        self.store.ticker.schedule_store(STORE_TICK_UPDATE_SAFE_TS);
    }

    fn start(&mut self, store: metapb::Store) {
        if self.store.start_time.is_some() {
            panic!("store unable to start again");
        }
        self.store.id = store.id;
        self.store.start_time = Some(time::get_time());
        self.store_heartbeat_pd();
        self.store.ticker.schedule_store(STORE_TICK_PD_HEARTBEAT);
        self.store.ticker.schedule_store(STORE_TICK_UPDATE_SAFE_TS);
    }

    fn on_store_unreachable(&mut self, store_id: u64) {
        let now = Instant::now();
        if self
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
            "store_id" => self.store.id,
            "unreachable_store_id" => store_id,
        );
        self.store.last_unreachable_report.insert(store_id, now);
        // It's possible to acquire the lock and only send notification to
        // involved regions. However loop over all the regions can take a
        // lot of time, which may block other operations.
        for id in self.ctx.peers.keys() {
            self.ctx.global.router.report_unreachable(*id, store_id);
        }
    }

    /// Checks if the message is targeting a stale peer.
    fn check_msg(&mut self, msg: &RaftMessage) -> Result<CheckMsgStatus> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let from_store_id = msg.get_from_peer().get_store_id();
        let to_peer_id = msg.get_to_peer().get_id();

        // Check if the target peer is tombstone.
        let rf_engine = &self.ctx.global.engines.raft;
        let local_state =
            match rf_engine.get_last_state_with_prefix(region_id, REGION_META_KEY_PREFIX) {
                Some(state_bin) => {
                    let mut state = RegionLocalState::default();
                    state.merge_from_bytes(&state_bin)?;
                    state
                }
                None => return Ok(CheckMsgStatus::NewPeerFirst),
            };
        if local_state.get_state() != PeerState::Tombstone {
            // Maybe split, but not registered yet.
            if !util::is_first_message(msg.get_message()) {
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
        // The region in this peer is already destroyed
        if util::is_epoch_stale(from_epoch, region_epoch) {
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
                    if let Err(e) = self.ctx.global.trans.send(send_msg) {
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

    fn on_raft_message(&mut self, msg: RaftMessage) {
        let region_id = msg.get_region_id();
        let region_ver = msg.get_region_epoch().version;
        debug!(
            "handle raft message";
            "from_peer_id" => msg.get_from_peer().get_id(),
            "to_peer_id" => msg.get_to_peer().get_id(),
            "store_id" => self.store.id,
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
            return;
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "region_id" => region_id,
            );
            return;
        }
        if msg.get_is_tombstone() || msg.has_merge_target() {
            // Target tombstone peer doesn't exist, so ignore it.
            return;
        }
        let check_msg_result = self.check_msg(&msg);
        if let Err(err) = check_msg_result {
            error!("failed to check message err:{:?}", err);
            return;
        }
        let check_msg_status = check_msg_result.unwrap();
        let is_first_request = match check_msg_status {
            CheckMsgStatus::DropMsg => return,
            CheckMsgStatus::FirstRequest => true,
            CheckMsgStatus::NewPeer | CheckMsgStatus::NewPeerFirst => {
                if self.maybe_create_peer(
                    region_id,
                    region_ver,
                    &msg,
                    check_msg_status == CheckMsgStatus::NewPeerFirst,
                ) {
                    // Peer created, send the message again.
                    let peer_msg = PeerMsg::RaftMessage(msg);
                    self.ctx.global.router.send(region_id, peer_msg);
                    return;
                }
                // Can't create peer, see if we should keep this message
                util::is_first_message(msg.get_message())
            }
        };
        if is_first_request {
            // To void losing messages, either put it to pending_msg or force send.
            if !self.ctx.store_meta.regions.contains_key(&region_id) {
                // Save one pending message for a peer is enough, remove
                // the previous pending message of this peer
                self.ctx
                    .store_meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == msg.get_to_peer());

                self.ctx.store_meta.pending_msgs.push(msg);
            } else {
                let peer_msg = PeerMsg::RaftMessage(msg);
                self.ctx.global.router.send(region_id, peer_msg);
            }
        }
    }

    fn on_generate_engine_meta_change(&self, change_set: kvenginepb::ChangeSet) {
        debug!("store on generate engine meta change {:?}", &change_set);
        // GenerateEngineMetaChange message is first sent to store handler,
        // Then send it to the router to create a raft log then propose this log, replicate to
        // followers.
        let id = change_set.get_shard_id();
        let peer_msg = PeerMsg::GenerateEngineChangeSet(change_set);
        // If the region is not found, there is no need to handle the engine meta change, so
        // we can ignore not found error.
        self.ctx.global.router.send(id, peer_msg);
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(
        &mut self,
        region_id: u64,
        region_ver: u64,
        msg: &RaftMessage,
        is_local_first: bool,
    ) -> bool {
        if !is_initial_msg(msg.get_message()) {
            let msg_type = msg.get_message().get_msg_type();
            debug!(
                "target peer doesn't exist, stale message";
                "target_peer" => ?msg.get_to_peer(),
                "region_id" => region_id,
                "msg_type" => ?msg_type,
            );
            self.ctx.raft_metrics.message_dropped.stale_msg += 1;
            return false;
        }
        match self.maybe_create_peer_internal(region_id, region_ver, msg, is_local_first) {
            Ok(created) => created,
            Err(err) => {
                error!(
                    "{}:{} failed to create peer {:?}",
                    region_id, region_ver, err
                );
                false
            }
        }
    }

    fn maybe_create_peer_internal(
        &mut self,
        region_id: u64,
        region_ver: u64,
        msg: &RaftMessage,
        is_local_first: bool,
    ) -> Result<bool> {
        let target = msg.get_to_peer();
        // New created peers should know it's learner or not.
        let peer = PeerFsm::replicate(
            self.ctx.store_id(),
            &self.ctx.cfg,
            self.ctx.global.engines.clone(),
            region_id,
            target.clone(),
        )?;
        if self.ctx.store_meta.regions.contains_key(&region_id) {
            return Ok(true);
        }
        fail_point!("after_acquire_store_meta_on_maybe_create_peer_internal");
        if self
            .ctx
            .store_meta
            .pending_new_regions
            .contains_key(&region_id)
        {
            return Ok(false);
        }

        // TODO(x) handle overlap exisitng region.

        // WARNING: The checking code must be above this line.
        // Now all checking passed

        // Following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        self.ctx
            .store_meta
            .regions
            .insert(region_id, peer.get_peer().region().to_owned());
        self.register(peer);
        self.ctx.global.router.send(region_id, PeerMsg::Start);
        Ok(true)
    }

    pub(crate) fn register(&mut self, peer: PeerFsm) {
        let id = peer.peer.region().id;
        let ver = peer.peer.region().get_region_epoch().get_version();
        info!(
            "register region {}:{}, peer {}",
            id,
            ver,
            peer.peer.peer_id()
        );
        let applier = Applier::new_from_peer(&peer);
        let new_peer = PeerStates::new(applier, peer);
        self.ctx.peers.insert(id, new_peer);
    }

    pub(crate) fn close(&mut self, id: u64) {
        if let Some(peer) = self.ctx.peers.get(&id) {
            let mut guard = peer.peer_fsm.lock().unwrap();
            guard.peer.pending_remove = true;
            drop(guard);
        }
        self.ctx.peers.remove(&id);
    }

    fn get_peer(&mut self, region_id: u64) -> PeerStates {
        self.ctx.peers.get(&region_id).unwrap().clone()
    }

    fn on_split_region(&mut self, regions: Vec<metapb::Region>) -> Option<u64> {
        fail_point!("on_split", self.ctx.store_id() == 3, |_| { None });
        let derived = regions.last().unwrap().clone();
        let derived_peer = self.get_peer(derived.get_id());
        let mut peer_fsm = derived_peer.peer_fsm.lock().unwrap();
        let region_id = derived.get_id();
        self.ctx.store_meta.set_region(derived, &mut peer_fsm.peer);

        let is_leader = peer_fsm.peer.is_leader();
        if is_leader {
            peer_fsm.peer.heartbeat_pd(self.ctx);
            // Notify pd immediately to let it update the region meta.
            info!(
                "notify pd with split";
                "region_id" => peer_fsm.region_id(),
                "peer_id" => peer_fsm.peer_id(),
                "split_count" => regions.len(),
            );
            // Now pd only uses ReportBatchSplit for history operation show,
            // so we send it independently here.
            let task = PdTask::ReportBatchSplit {
                regions: regions.to_vec(),
            };
            if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
                error!(
                    "failed to notify pd";
                    "region_id" => peer_fsm.region_id(),
                    "peer_id" => peer_fsm.peer_id(),
                    "err" => %e,
                );
            }
        }

        let last_key = raw_end_key(regions.last().unwrap());
        if self
            .ctx
            .store_meta
            .region_ranges
            .remove(&last_key)
            .is_none()
        {
            panic!("{} original region should exist", peer_fsm.peer.tag());
        }
        let mut new_peers = vec![];
        for new_region in regions {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = self
                    .ctx
                    .store_meta
                    .region_ranges
                    .insert(raw_end_key(&new_region), new_region_id)
                    .is_none();
                assert!(not_exist, "[region {}] should not exist", new_region_id);
                continue;
            }
            if let Some(r) = self.ctx.store_meta.regions.get(&new_region_id) {
                if is_region_initialized(r) {
                    // The region is created by raft message.
                    info!("initialized region already exists, must be created by raft message.");
                    continue;
                }
            }
            // Now all checking passed.
            // Insert new regions and validation
            info!(
                "insert new region";
                "region_id" => new_region_id,
                "region" => ?new_region,
            );
            self.ctx
                .store_meta
                .pending_new_regions
                .remove(&new_region_id);

            let mut new_peer = match PeerFsm::create(
                self.ctx.store_id(),
                &self.ctx.cfg,
                self.ctx.global.engines.clone(),
                &new_region,
            ) {
                Ok(new_peer) => new_peer,
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };

            let meta_peer = new_peer.peer.peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache.
                new_peer.peer.insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = peer_fsm.peer.peer_stat.clone();
            new_peer.peer.need_campaign = is_leader;

            if is_leader {
                // The new peer is likely to become leader, send a heartbeat immediately to reduce
                // client query miss.
                new_peer.peer.heartbeat_pd(self.ctx);
            }
            self.ctx.global.coprocessor_host.on_region_changed(
                &new_region,
                RegionChangeEvent::Create,
                new_peer.peer.get_role(),
            );
            self.ctx
                .store_meta
                .regions
                .insert(new_region_id, new_region.clone());
            let not_exist = self
                .ctx
                .store_meta
                .region_ranges
                .insert(raw_end_key(&new_region), new_region_id)
                .is_none();
            assert!(not_exist, "[region {}] should not exist", new_region_id);
            let read_delegate = ReadDelegate::from_peer(new_peer.get_peer());
            self.ctx
                .store_meta
                .readers
                .insert(new_region_id, read_delegate);

            new_peers.push(new_peer);
            self.ctx.global.router.send(new_region_id, PeerMsg::Start);

            if !is_leader {
                if let Some(msg) = self
                    .ctx
                    .store_meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    self.ctx
                        .global
                        .router
                        .send(new_region_id, PeerMsg::RaftMessage(msg));
                }
            }
        }
        for new_peer in new_peers {
            self.register(new_peer);
        }
        if is_leader {
            if let Some(shard) = self.ctx.global.engines.kv.get_shard(region_id) {
                self.ctx.global.engines.kv.trigger_flush(&shard);
            }
        }
        fail_point!("after_split", self.ctx.store_id() == 3, |_| { None });
        self.maybe_apply(region_id)
    }

    fn on_change_peer(&mut self, cp: ChangePeer) -> Option<u64> {
        let region_id = cp.region.id;
        let peer = self.get_peer(region_id);
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();

        if cp.index >= peer_fsm.peer.raft_group.raft.raft_log.first_index() {
            match peer_fsm.peer.raft_group.apply_conf_change(&cp.conf_change) {
                Ok(_) => {}
                // PD could dispatch redundant conf changes.
                Err(raft::Error::NotExists { .. }) | Err(raft::Error::Exists { .. }) => {}
                _ => unreachable!(),
            }
        } else {
            // Please take a look at test case test_redundant_conf_change_by_snapshot.
        }
        self.update_region(&mut peer_fsm, cp.region);

        fail_point!("change_peer_after_update_region");

        let now = Instant::now();
        let (mut remove_self, mut need_ping) = (false, false);
        for mut change in cp.changes {
            let (change_type, peer) = (change.get_change_type(), change.take_peer());
            let (store_id, peer_id) = (peer.get_store_id(), peer.get_id());
            match change_type {
                ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                    // Add this peer to peer_heartbeats.
                    peer_fsm.peer.peer_heartbeats.insert(peer_id, now);
                    if peer_fsm.peer.is_leader() {
                        need_ping = true;
                        peer_fsm.peer.peers_start_pending_time.push((peer_id, now));
                    }
                }
                ConfChangeType::RemoveNode => {
                    // Remove this peer from cache.
                    peer_fsm.peer.peer_heartbeats.remove(&peer_id);
                    if peer_fsm.peer.is_leader() {
                        peer_fsm
                            .peer
                            .peers_start_pending_time
                            .retain(|&(p, _)| p != peer_id);
                    }
                    peer_fsm.peer.remove_peer_from_cache(peer_id);
                    // We only care remove itself now.
                    if self.store.id == store_id {
                        if peer_fsm.peer.peer_id() == peer_id {
                            remove_self = true;
                        } else {
                            panic!(
                                "{} trying to remove unknown peer {:?}",
                                peer_fsm.peer.tag(),
                                peer
                            );
                        }
                    }
                }
            }
        }

        // In pattern matching above, if the peer is the leader,
        // it will push the change peer into `peers_start_pending_time`
        // without checking if it is duplicated. We move `heartbeat_pd` here
        // to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
        // adding the redundant peer.
        if peer_fsm.peer.is_leader() {
            // Notify pd immediately.
            info!(
                "notify pd with change peer region";
                "region_id" => peer_fsm.region_id(),
                "peer_id" => peer_fsm.peer_id(),
                "region" => ?peer_fsm.peer.region(),
            );
            peer_fsm.peer.heartbeat_pd(self.ctx);

            // Remove or demote leader will cause this raft group unavailable
            // until new leader elected, but we can't revert this operation
            // because its result is already persisted in apply worker
            // TODO: should we transfer leader here?
            let demote_self = util::is_learner(&peer_fsm.peer.peer);
            if remove_self || demote_self {
                warn!(
                    "Removing or demoting leader";
                    "region_id" => peer_fsm.region_id(),
                    "peer_id" => peer_fsm.peer_id(),
                    "remove" => remove_self,
                    "demote" => demote_self,
                );
                if demote_self {
                    let term = peer_fsm.peer.term();
                    peer_fsm
                        .peer
                        .raft_group
                        .raft
                        .become_follower(term, raft::INVALID_ID);
                }
                // Don't ping to speed up leader election
                need_ping = false;
            }
        }
        if need_ping {
            // Speed up snapshot instead of waiting another heartbeat.
            peer_fsm.peer.ping();
        }
        peer_fsm
            .peer
            .handle_raft_ready(&mut self.ctx.raft_ctx, None);
        if remove_self {
            self.on_destroy_peer(region_id, false);
        }
        self.maybe_apply(region_id)
    }

    fn maybe_apply(&mut self, region_id: u64) -> Option<u64> {
        if !self.ctx.apply_msgs.msgs.is_empty() {
            Some(region_id)
        } else {
            None
        }
    }

    fn update_region(&mut self, peer_fsm: &mut PeerFsm, mut region: metapb::Region) {
        self.ctx
            .store_meta
            .set_region(region.clone(), &mut peer_fsm.peer);
        write_peer_state(&mut self.ctx.raft_wb, &region);
        for peer in region.take_peers().into_iter() {
            if peer_fsm.peer.peer_id() == peer.get_id() {
                peer_fsm.peer.peer = peer.clone();
            }
            peer_fsm.peer.insert_peer_cache(peer);
        }
    }

    fn on_destroy_peer(&mut self, region_id: u64, keep_data: bool) {
        let peer = self.get_peer(region_id);
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        fail_point!("destroy_peer");
        info!(
            "starts destroy";
            "region_id" => peer_fsm.region_id(),
            "peer_id" => peer_fsm.peer_id(),
        );
        let region_id = peer_fsm.region_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!peer_fsm.peer.is_applying_snapshot());

        // Mark itself as pending_remove
        peer_fsm.peer.pending_remove = true;
        if let Some(parent_id) = peer_fsm.peer.mut_store().parent_id() {
            self.ctx
                .global
                .engines
                .raft
                .remove_dependent(parent_id, region_id);
        }
        // Destroy read delegates.
        self.ctx.store_meta.readers.remove(&region_id);

        // Trigger region change observer
        self.ctx.global.coprocessor_host.on_region_changed(
            peer_fsm.peer.region(),
            RegionChangeEvent::Destroy,
            peer_fsm.peer.get_role(),
        );
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.ctx.global.pd_scheduler.schedule(task) {
            error!(
                "failed to notify pd";
                "region_id" => peer_fsm.region_id(),
                "peer_id" => peer_fsm.peer_id(),
                "err" => %e,
            );
        }
        let is_initialized = peer_fsm.peer.is_initialized();
        if is_initialized
            && !keep_data
            && self
                .ctx
                .store_meta
                .region_ranges
                .remove(&raw_end_key(peer_fsm.peer.region()))
                .is_none()
        {
            panic!("{} meta corruption detected", peer_fsm.peer.tag());
        }
        if self.ctx.store_meta.regions.remove(&region_id).is_none() && !keep_data {
            panic!("{} meta corruption detected", peer_fsm.peer.tag())
        }
        if let Err(e) = peer_fsm.peer.destroy(&mut self.ctx.raft_wb) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!("{} destroy err {:?}", peer_fsm.peer.tag(), e);
        }
        // Some places use `force_send().unwrap()` if the StoreMeta lock is held.
        // So in here, it's necessary to held the StoreMeta lock when closing the router.

        peer_fsm.stop();
        self.close(region_id);
        self.ctx.global.engines.kv.remove_shard(region_id);
    }

    fn on_pending_new_regions(&mut self, new_regions: Vec<u64>) {
        for new_region in new_regions {
            if let Some(region) = self.ctx.store_meta.regions.get(&new_region) {
                if region.is_initialized() {
                    continue;
                }
            }
            self.ctx
                .store_meta
                .pending_new_regions
                .insert(new_region, true);
        }
    }

    fn on_snapshot_ready(&mut self, region_id: u64) -> Option<u64> {
        let peer = self.get_peer(region_id);
        let mut peer_fsm = peer.peer_fsm.lock().unwrap();
        let raft_ctx = &mut self.ctx.raft_ctx;
        let store_meta = &mut self.ctx.store_meta;
        peer_fsm.peer.handle_raft_ready(raft_ctx, Some(store_meta));
        self.maybe_apply(region_id)
    }
}
