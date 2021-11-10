// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::Config;
use crate::{RaftRouter, RaftStoreRouter};
use concurrency_manager::ConcurrencyManager;
use fail::fail_point;
use kvproto::import_sstpb::SstMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb;
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest};
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use pd_client::PdClient;
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::coprocessor::{BoxAdminObserver, CoprocessorHost};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tikv_util::config::VersionTrack;
use tikv_util::mpsc::{Receiver, Sender};
use tikv_util::worker::{FutureScheduler, FutureWorker, Scheduler, Worker};
use tikv_util::RingQueue;
use time::Timespec;

use super::*;
use crate::{Error, Result};

pub const PENDING_MSG_CAP: usize = 100;

struct Workers {
    pd_worker: FutureWorker<PdTask>,
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
    peer_receiver: Option<Receiver<PeerMsg>>,
    store_fsm: Option<StoreFSM>,
}

impl RaftBatchSystem {
    pub fn new(engines: &Engines) -> Self {
        let (store_sender, store_receiver) =
            engines.meta_change_channel.lock().unwrap().take().unwrap();
        let (peer_sender, peer_receiver) = tikv_util::mpsc::unbounded();
        let router = RaftRouter::new(peer_sender, store_sender);
        Self {
            router,
            ctx: None,
            workers: None,
            peer_receiver: Some(peer_receiver),
            store_fsm: Some(StoreFSM::new(store_receiver)),
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
        mut engines: Engines,
        trans: Box<dyn Transport>,
        pd_client: Arc<C>,
        pd_worker: FutureWorker<PdTask>,
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
            .start_with_timer("region-apply-worker", region_apply_runner);

        let region_runner =
            RegionRunner::new(engines.kv.clone(), self.router(), region_apply_scheduler);
        let region_scheduler = workers
            .region_worker
            .start_with_timer("snapshot-worker", region_runner);

        let ctx = GlobalContext {
            cfg,
            engines,
            store: meta.clone(),
            store_meta,
            router: self.router.clone(),
            trans,
            pd_scheduler: workers.pd_worker.scheduler(),
            region_scheduler,
            coprocessor_host,
        };

        let mut region_peers = self.load_peers()?;
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
        );
        workers.pd_worker.start(pd_runner)?;
        self.workers = Some(workers);
        self.ctx = Some(ctx);
        let peer_receiver = self.peer_receiver.take().unwrap();
        let (mut rw, apply_recevier) = RaftWorker::new(
            self.ctx.clone().unwrap(),
            peer_receiver,
            self.router.clone(),
        );
        std::thread::spawn(move || {
            rw.run();
        });
        let mut aw = ApplyWorker::new(
            self.ctx.clone().unwrap(),
            apply_recevier,
            self.router.clone(),
        );
        let peer_handle = std::thread::spawn(move || {
            aw.run();
        });

        let store_fsm = self.store_fsm.take().unwrap();
        let mut sw = StoreWorker::new(store_fsm, self.ctx.clone().unwrap());
        let store_handle = std::thread::spawn(move || {
            sw.run();
        });

        self.router.send_store(StoreMsg::Start {
            store: self.ctx.as_ref().unwrap().store.clone(),
        });
        for region_id in region_ids {
            self.router
                .send(region_id, PeerMsg::new(region_id, PeerMsgPayload::Start));
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        todo!(); // stop the raft and apply worker.
        let mut workers = self.workers.take().unwrap();
        // Wait all workers finish.
        let handle = workers.pd_worker.stop();
        fail_point!("after_shutdown_apply");

        if let Some(h) = handle {
            h.join().unwrap();
        }
        workers.coprocessor_host.shutdown();
        workers.region_worker.stop();
        workers.region_apply_worker.stop();
    }

    /// load_peers loads peers in this store. It scans the kv engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn load_peers(&self) -> Result<Vec<PeerFSM>> {
        todo!()
    }
}

pub struct StoreInfo {
    pub engine: kvengine::Engine,
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
    /// region_id -> `RegionReadProgress`
    pub region_read_progress: RegionReadProgressRegistry,
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
            region_read_progress: RegionReadProgressRegistry::new(),
        }
    }

    #[inline]
    pub(crate) fn set_region(
        &mut self,
        host: &CoprocessorHost<kvengine::Engine>,
        region: Region,
        peer: &mut crate::store::Peer,
    ) {
        let prev = self.regions.insert(region.get_id(), region.clone());
        if prev.map_or(true, |r| r.get_id() != region.get_id()) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.region_id);
        }
        let reader = self.readers.get_mut(&region.get_id()).unwrap();
        peer.set_region(host, reader, region);
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
    pub(crate) pd_scheduler: FutureScheduler<PdTask>,
    pub(crate) region_scheduler: Scheduler<RegionTask>,
    pub(crate) coprocessor_host: CoprocessorHost<kvengine::Engine>,
}

pub(crate) struct RaftContext {
    pub(crate) global: GlobalContext,
    pub(crate) apply_msgs: ApplyMsgs,
    pub(crate) ready_res: Vec<ReadyICPair>,
    pub(crate) raft_wb: rfengine::WriteBatch,
    pub(crate) pending_count: usize,
    pub(crate) local_stats: StoreStats,
}

#[derive(Default)]
pub(crate) struct StoreStats {
    engine_total_bytes_written: u64,
    engine_total_keys_written: u64,
    is_busy: bool,
}

impl RaftContext {
    pub(crate) fn new(global: GlobalContext) -> Self {
        Self {
            global,
            apply_msgs: ApplyMsgs { msgs: vec![] },
            ready_res: vec![],
            raft_wb: rfengine::WriteBatch::new(),
            pending_count: 0,
            local_stats: Default::default(),
        }
    }

    pub(crate) fn flush_local_stats(&mut self) {
        todo!()
    }
}

pub(crate) struct StoreFSM {
    pub(crate) id: u64,
    pub(crate) start_time: Option<Timespec>,
    pub(crate) receiver: Receiver<StoreMsg>,
}

impl StoreFSM {
    pub fn new(receiver: Receiver<StoreMsg>) -> Self {
        Self {
            id: 0,
            start_time: None,
            receiver,
        }
    }
}

pub(crate) struct StoreMsgHandler {
    store: StoreFSM,
    ctx: GlobalContext,
}

impl StoreMsgHandler {
    pub(crate) fn new(store: StoreFSM, ctx: GlobalContext) -> StoreMsgHandler {
        Self { store, ctx }
    }

    pub(crate) fn handle_msg(&self, msg: StoreMsg) {
        match msg {
            StoreMsg::Tick(store_tick) => self.on_tick(store_tick),
            StoreMsg::Start { store } => self.start(store),
            StoreMsg::StoreUnreachable { store_id } => self.on_store_unreachable(store_id),
            StoreMsg::RaftMessage(raft_msg) => self.on_raft_message(raft_msg),
            StoreMsg::GenerateEngineChangeSet(_) => self.on_generate_engine_meta_change(msg),
        }
    }

    pub(crate) fn on_tick(&self, tick: StoreTick) {
        match tick {
            STORE_TICK_PD_HEARTBEAT => {
                self.on_pd_heartbeat_tick();
            }
            STORE_TICK_CONSISTENCY_CHECK => {
                self.on_consistency_check_tick();
            }
            _ => {}
        }
    }

    pub(crate) fn on_pd_heartbeat_tick(&self) {
        todo!()
    }

    pub(crate) fn on_consistency_check_tick(&self) {
        todo!()
    }

    pub(crate) fn start(&self, store: metapb::Store) {
        todo!()
    }

    pub(crate) fn on_store_unreachable(&self, store_id: u64) {
        todo!()
    }

    pub(crate) fn on_raft_message(&self, msg: Box<RaftMessage>) {
        todo!()
    }

    pub(crate) fn on_generate_engine_meta_change(&self, msg: StoreMsg) {
        // GenerateEngineMetaChange message is first sent to store handler to find a region id for this change,
        // Once we got the region ID, we send it to the router to create a raft log then propose this log, replicate to
        // followers.
        todo!()
    }
}
