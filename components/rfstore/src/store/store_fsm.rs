// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::Config;
use crate::{RaftRouter, RaftStoreRouter};
use bytes::Bytes;
use concurrency_manager::ConcurrencyManager;
use fail::fail_point;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb;
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use pd_client::PdClient;
use protobuf::Message;
use raft::StateRole;
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::coprocessor::{BoxAdminObserver, CoprocessorHost, RegionChangeEvent};
use raftstore::store::local_metrics::RaftMetrics;
use raftstore::store::util;
use raftstore::store::util::is_initial_msg;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tikv_util::config::VersionTrack;
use tikv_util::mpsc::Receiver;
use tikv_util::worker::{LazyWorker, Scheduler, Worker};
use tikv_util::{box_err, debug, error, info, warn, RingQueue};
use time::Timespec;

use super::*;
use crate::store::peer_worker::{ApplyWorker, StoreWorker};
use crate::{Error, Result};

pub const PENDING_MSG_CAP: usize = 100;
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(10);

struct Workers {
    pd_worker: LazyWorker<PdTask>,
    region_worker: Worker,
    region_apply_worker: Worker,
    coprocessor_host: CoprocessorHost<kvengine::Engine>,
}

pub struct RaftBatchSystem {
    router: RaftRouter,

    // Change to some after spawn.
    ctx: Option<GlobalContext>,
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
            ctx: None,
            workers: None,
            peer_receiver: Some(peer_receiver),
            store_fsm: Some(StoreFSM::new(store_receiver, conf)),
        }
    }

    pub fn router(&self) -> RaftRouter {
        self.router.clone()
    }

    // TODO: reduce arguments
    pub fn spawn<C: PdClient + 'static>(
        &mut self,
        meta: metapb::Store,
        cfg: Arc<VersionTrack<Config>>,
        engines: Engines,
        trans: Box<dyn Transport>,
        pd_client: Arc<C>,
        pd_worker: LazyWorker<PdTask>,
        store_meta: Arc<Mutex<StoreMeta>>,
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
            region_worker: Worker::new("region-worker"),
            region_apply_worker: Worker::new("region-apply-worker"),
            coprocessor_host: coprocessor_host.clone(),
        };

        let region_apply_runner = RegionApplyRunner::new(engines.kv.clone(), self.router());
        let region_apply_scheduler = workers
            .region_apply_worker
            .start("region-apply-worker", region_apply_runner);

        let region_runner =
            RegionRunner::new(engines.kv.clone(), self.router(), region_apply_scheduler);
        let region_scheduler = workers
            .region_worker
            .start("snapshot-worker", region_runner);
        let pd_scheduler = workers.pd_worker.scheduler();
        let ctx = GlobalContext {
            cfg,
            engines,
            store: meta.clone(),
            store_meta,
            router: self.router.clone(),
            trans,
            pd_scheduler,
            region_scheduler,
            coprocessor_host,
        };

        let mut region_peers = self.load_peers(&ctx)?;
        {
            let mut meta = ctx.store_meta.lock().unwrap();
            for peer_fsm in &region_peers {
                let peer = peer_fsm.get_peer();
                meta.readers
                    .insert(peer_fsm.region_id(), ReadDelegate::from_peer(peer));
            }
        }
        let mut region_ids = Vec::with_capacity(region_peers.len());
        for peer in region_peers.drain(..) {
            region_ids.push(peer.peer.region_id);
            self.router.register(peer)
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
        self.ctx = Some(ctx);
        let peer_receiver = self.peer_receiver.take().unwrap();

        let (mut io_worker, io_sender) = IOWorker::new(
            self.ctx.as_ref().unwrap().engines.raft.clone(),
            self.router.clone(),
            self.ctx.as_ref().unwrap().trans.clone(),
        );
        let mut sync_io_worker = None;
        if self.ctx.as_ref().unwrap().cfg.value().async_io {
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

        let (mut rw, apply_receivers) = RaftWorker::new(
            self.ctx.clone().unwrap(),
            peer_receiver,
            self.router.clone(),
            io_sender,
            sync_io_worker,
        );
        let props = tikv_util::thread_group::current_properties();
        std::thread::Builder::new()
            .name("raft".to_string())
            .spawn(move || {
                tikv_util::thread_group::set_properties(props);
                rw.run();
            })
            .unwrap();

        for apply_receiver in apply_receivers {
            let props = tikv_util::thread_group::current_properties();
            let mut aw = ApplyWorker::new(
                self.ctx.as_ref().unwrap().engines.kv.clone(),
                self.ctx.as_ref().unwrap().region_scheduler.clone(),
                self.router.clone(),
                apply_receiver,
            );
            let _peer_handle = std::thread::Builder::new()
                .name("apply".to_string())
                .spawn(move || {
                    tikv_util::thread_group::set_properties(props);
                    aw.run();
                })
                .unwrap();
        }

        let store_fsm = self.store_fsm.take().unwrap();
        let mut sw = StoreWorker::new(store_fsm, self.ctx.clone().unwrap());
        let _store_handle = std::thread::Builder::new()
            .name("store".to_string())
            .spawn(move || {
                sw.run();
            })
            .unwrap();

        self.router.send_store(StoreMsg::Start {
            store: self.ctx.as_ref().unwrap().store.clone(),
        });
        for region_id in region_ids {
            self.router.send(region_id, PeerMsg::Start).unwrap();
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
        workers.region_worker.stop();
        workers.region_apply_worker.stop();
    }

    /// load_peers loads peers in this store. It scans the kv engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn load_peers(&self, ctx: &GlobalContext) -> Result<Vec<PeerFsm>> {
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
        let mut store_meta = ctx.store_meta.lock().unwrap();
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
    /// region_id -> reader
    pub readers: HashMap<u64, ReadDelegate>,
    /// region_id -> (term, leader_peer_id)
    pub leaders: HashMap<u64, (u64, u64)>,
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
    /// 	If the new region is found and initialized, avoid update the new region.
    /// 	Otherwise add pending create in store meta.
    ///
    /// A2: Handle apply result of split.
    /// 	If the new region is found, it must be initialized, then avoid create the peer.
    /// 	Otherwise, create the peer and remove pending create.
    ///
    /// B1: store handle raft message maybe create peer.
    /// 	If region or pending create is found, avoid create the peer.
    /// 	Otherwise create the peer with the uninitialized region.
    ///
    /// B2: new peer recevied snapshot.
    /// 	If prev region is uninitialized and the region in store is initialized, avoid update the store meta.
    /// 	Otherwise update the store meta.
    ///
    /// There are 5 possible sequences to analyze:
    ///
    /// - A1, A2
    /// 	After A2, the peer can be found in router.
    ///
    /// - A1, B1, A2
    /// 	A1 set pending create in store meta.
    /// 	B1 found the pending create in store meta, then avoid create peer.
    ///
    /// - B1, A1, A2, B2,
    /// 	B1 create the peer in the store meta.
    /// 	A1 found the peer is already exists in store meta, but region is not initialized, add pending create.
    /// 	A2 replace the uninitialized region by split, removed the pending create.
    /// 	B2 found no pending create, and prev region is not initialized, meta region is initialized, avoid update the meta.
    ///
    /// - B1, A1, B2, A2
    /// 	B1 create the peer in the store meta.
    /// 	A1 found the peer is already exists in store meta, but region is not initialized, add pending create.
    /// 	B2 found pending create, avoid update the meta.
    /// 	A2 replace the uninitialized region by split, removed the pending create.
    ///
    /// - B1. B2, A1, A2
    /// 	B2 updated the meta with initialized region.
    /// 	A1 found region is initialized, do not add pending create.
    /// 	A2 do not create the initialized region on ready split.
    pub(crate) pending_new_regions: HashMap<u64, bool>,
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
            pending_new_regions: HashMap::default(),
        }
    }

    #[inline]
    pub(crate) fn set_region(
        &mut self,
        host: &CoprocessorHost<kvengine::Engine>,
        region: Region,
        peer: &mut crate::store::Peer,
    ) {
        let region_id = region.get_id();
        let prev = self.regions.insert(region_id, region.clone());
        if prev.map_or(true, |r| r.get_id() != region_id) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.region_id);
        }
        peer.set_region(host, region);
        self.readers
            .insert(region_id, ReadDelegate::from_peer(peer));
    }
}

#[derive(Clone)]
pub(crate) struct GlobalContext {
    pub(crate) cfg: Arc<VersionTrack<Config>>,
    pub(crate) engines: Engines,
    pub(crate) store: metapb::Store,
    pub(crate) store_meta: Arc<Mutex<StoreMeta>>,
    pub(crate) router: RaftRouter,
    pub(crate) trans: Box<dyn Transport>,
    pub(crate) pd_scheduler: Scheduler<PdTask>,
    pub(crate) region_scheduler: Scheduler<RegionTask>,
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
    last_unreachable_report: HashMap<u64, Instant>,
}

impl StoreFSM {
    pub fn new(receiver: Receiver<StoreMsg>, cfg: &Config) -> Self {
        Self {
            id: 0,
            start_time: None,
            receiver,
            ticker: Ticker::new_store(cfg),
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

pub(crate) struct StoreMsgHandler {
    store: StoreFSM,
    pub(crate) ctx: RaftContext,
}

impl StoreMsgHandler {
    pub(crate) fn new(store: StoreFSM, ctx: GlobalContext) -> StoreMsgHandler {
        let ctx = RaftContext::new(ctx);
        Self { store, ctx }
    }

    pub(crate) fn get_receiver(&self) -> &Receiver<StoreMsg> {
        &self.store.receiver
    }

    pub(crate) fn handle_msg(&mut self, msg: StoreMsg) {
        match msg {
            StoreMsg::Tick => self.on_tick(),
            StoreMsg::Start { store } => self.start(store),
            StoreMsg::StoreUnreachable { store_id } => self.on_store_unreachable(store_id),
            StoreMsg::RaftMessage(msg) => {
                if let Err(e) = self.on_raft_message(msg) {
                    error!(?e;
                        "handle raft message failed";
                        "store_id" => self.store.id,
                    );
                }
            }
            StoreMsg::GenerateEngineChangeSet(cs) => self.on_generate_engine_meta_change(cs),
        }
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
        {
            let meta = self.ctx.global.store_meta.lock().unwrap();
            stats.set_region_count(meta.regions.len() as u32);
        }

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
        self.ctx.global.router.report_unreachable(store_id).unwrap();
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

    fn on_raft_message(&mut self, msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        let region_ver = msg.get_region_epoch().version;
        let msg = match self
            .ctx
            .global
            .router
            .send(region_id, PeerMsg::RaftMessage(msg))
        {
            Ok(()) => {
                return Ok(());
            }
            Err(Error::RegionNotFound(_, Some(PeerMsg::RaftMessage(msg)))) => msg,
            Err(_) => unreachable!(),
        };

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
            return Ok(());
        }

        if !msg.has_region_epoch() {
            error!(
                "missing epoch in raft message, ignore it";
                "region_id" => region_id,
            );
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
                    region_ver,
                    &msg,
                    check_msg_status == CheckMsgStatus::NewPeerFirst,
                )? {
                    // Peer created, send the message again.
                    let peer_msg = PeerMsg::RaftMessage(msg);
                    self.ctx.global.router.send(region_id, peer_msg).unwrap();
                    return Ok(());
                }
                // Can't create peer, see if we should keep this message
                util::is_first_message(msg.get_message())
            }
        };
        if is_first_request {
            // To void losing messages, either put it to pending_msg or force send.
            let mut store_meta = self.ctx.global.store_meta.lock().unwrap();
            if !store_meta.regions.contains_key(&region_id) {
                // Save one pending message for a peer is enough, remove
                // the previous pending message of this peer
                store_meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == msg.get_to_peer());

                store_meta.pending_msgs.push(msg);
            } else {
                drop(store_meta);
                let peer_msg = PeerMsg::RaftMessage(msg);
                if let Err(e) = self.ctx.global.router.send(region_id, peer_msg) {
                    warn!("handle first request failed"; "region_id" => region_id, "error" => ?e);
                }
            }
        }
        Ok(())
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
        if let Err(e) = self.ctx.global.router.send(id, peer_msg) {
            warn!(
                "failed to send engine meta change for region {} {:?}",
                id, e
            );
        }
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

        self.maybe_create_peer_internal(region_id, region_ver, msg, is_local_first)
    }

    fn maybe_create_peer_internal(
        &mut self,
        region_id: u64,
        region_ver: u64,
        msg: &RaftMessage,
        is_local_first: bool,
    ) -> Result<bool> {
        let target = msg.get_to_peer();

        let mut meta = self.ctx.global.store_meta.lock().unwrap();
        if meta.regions.contains_key(&region_id) {
            return Ok(true);
        }
        fail_point!("after_acquire_store_meta_on_maybe_create_peer_internal");
        if meta.pending_new_regions.contains_key(&region_id) {
            return Ok(false);
        }

        // TODO(x) handle overlap exisitng region.

        // New created peers should know it's learner or not.
        let peer = PeerFsm::replicate(
            self.ctx.store_id(),
            &self.ctx.cfg,
            self.ctx.global.engines.clone(),
            region_id,
            target.clone(),
        )?;

        // WARNING: The checking code must be above this line.
        // Now all checking passed

        // Following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        meta.regions
            .insert(region_id, peer.get_peer().region().to_owned());
        let router = &self.ctx.global.router;
        router.register(peer);
        router.send(region_id, PeerMsg::Start).unwrap();
        Ok(true)
    }
}
