// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use causal_ts::CausalTsProviderImpl;
use collections::HashSet;
use concurrency_manager::ConcurrencyManager;
use crossbeam::channel::{self, Receiver, Sender, TrySendError};
use engine_test::{
    ctor::{CfOptions, DbOptions},
    kv::{KvTestEngine, KvTestSnapshot, TestTabletFactory},
    raft::RaftTestEngine,
};
use engine_traits::{TabletContext, TabletRegistry, DATA_CFS};
use futures::executor::block_on;
use kvproto::{
    metapb::{self, RegionEpoch, Store},
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request},
    raft_serverpb::RaftMessage,
};
use pd_client::RpcClient;
use raft::eraftpb::MessageType;
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{
        region_meta::{RegionLocalState, RegionMeta},
        Config, RegionSnapshot, TabletSnapKey, TabletSnapManager, Transport, RAFT_INIT_LOG_INDEX,
    },
};
use raftstore_v2::{
    create_store_batch_system,
    router::{DebugInfoChannel, FlushChannel, PeerMsg, QueryResult, RaftRouter},
    Bootstrap, SimpleWriteEncoder, StateStorage, StoreSystem,
};
use slog::{debug, o, Logger};
use tempfile::TempDir;
use test_pd::mocker::Service;
use tikv_util::{
    config::{ReadableDuration, VersionTrack},
    store::new_peer,
    worker::{LazyWorker, Worker},
};
use txn_types::WriteBatchFlags;

pub fn check_skip_wal(path: &str) {
    let mut found = false;
    for f in std::fs::read_dir(path).unwrap() {
        let e = f.unwrap();
        if e.path().extension().map_or(false, |ext| ext == "log") {
            found = true;
            assert_eq!(e.metadata().unwrap().len(), 0, "{}", e.path().display());
        }
    }
    assert!(found, "no WAL found in {}", path);
}

pub struct TestRouter(RaftRouter<KvTestEngine, RaftTestEngine>);

impl Deref for TestRouter {
    type Target = RaftRouter<KvTestEngine, RaftTestEngine>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TestRouter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TestRouter {
    pub fn query(&self, region_id: u64, req: RaftCmdRequest) -> Option<QueryResult> {
        let (msg, sub) = PeerMsg::raft_query(req);
        self.send(region_id, msg).unwrap();
        block_on(sub.result())
    }

    pub fn must_query_debug_info(&self, region_id: u64, timeout: Duration) -> Option<RegionMeta> {
        let timer = Instant::now();
        while timer.elapsed() < timeout {
            let (ch, sub) = DebugInfoChannel::pair();
            let msg = PeerMsg::QueryDebugInfo(ch);
            let res = self.send(region_id, msg);
            if res.is_err() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            return block_on(sub.result());
        }
        None
    }

    pub fn simple_write(
        &self,
        region_id: u64,
        header: Box<RaftRequestHeader>,
        write: SimpleWriteEncoder,
    ) -> Option<RaftCmdResponse> {
        let (msg, sub) = PeerMsg::simple_write(header, write.encode());
        self.send(region_id, msg).unwrap();
        block_on(sub.result())
    }

    pub fn admin_command(&self, region_id: u64, req: RaftCmdRequest) -> Option<RaftCmdResponse> {
        let (msg, sub) = PeerMsg::admin_command(req);
        self.send(region_id, msg).unwrap();
        block_on(sub.result())
    }

    pub fn wait_flush(&self, region_id: u64, timeout: Duration) -> bool {
        let timer = Instant::now();
        while timer.elapsed() < timeout {
            let (ch, sub) = FlushChannel::pair();
            let res = self.send(region_id, PeerMsg::WaitFlush(ch));
            match res {
                Ok(_) => return block_on(sub.result()).is_some(),
                Err(TrySendError::Disconnected(_)) => return false,
                Err(TrySendError::Full(_)) => thread::sleep(Duration::from_millis(10)),
            }
        }
        panic!("unable to flush {}", region_id);
    }

    pub fn wait_applied_to_current_term(&self, region_id: u64, timeout: Duration) {
        let mut now = Instant::now();
        let deadline = now + timeout;
        let mut res = None;
        while now < deadline {
            res = self.must_query_debug_info(region_id, deadline - now);
            if let Some(info) = &res {
                // If term matches and apply to commit index, then it must apply to current
                // term.
                if info.raft_apply.applied_index == info.raft_apply.commit_index
                    && info.raft_apply.commit_term == info.raft_status.hard_state.term
                {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(10));
            now = Instant::now();
        }
        panic!(
            "region {} is not applied to current term, {:?}",
            region_id, res
        );
    }

    pub fn new_request_for(&self, region_id: u64) -> RaftCmdRequest {
        let meta = self
            .must_query_debug_info(region_id, Duration::from_secs(1))
            .unwrap();
        let mut req = RaftCmdRequest::default();
        req.mut_header().set_region_id(region_id);
        let epoch = req.mut_header().mut_region_epoch();
        let epoch_meta = &meta.region_state.epoch;
        epoch.set_version(epoch_meta.version);
        epoch.set_conf_ver(epoch_meta.conf_ver);
        let target_peer = *meta
            .region_state
            .peers
            .iter()
            .find(|p| p.id == meta.raft_status.id)
            .unwrap();
        let mut peer = new_peer(target_peer.store_id, target_peer.id);
        peer.role = target_peer.role.into();
        req.mut_header().set_peer(peer);
        req.mut_header().set_term(meta.raft_status.hard_state.term);
        req
    }

    pub fn stale_snapshot(&mut self, region_id: u64) -> RegionSnapshot<KvTestSnapshot> {
        let mut req = self.new_request_for(region_id);
        let header = req.mut_header();
        header.set_flags(WriteBatchFlags::STALE_READ.bits());
        header.set_flag_data(vec![0; 8]);
        let mut snap_req = Request::default();
        snap_req.set_cmd_type(CmdType::Snap);
        req.mut_requests().push(snap_req);
        block_on(self.snapshot(req)).unwrap()
    }

    pub fn region_detail(&self, region_id: u64) -> metapb::Region {
        let RegionLocalState {
            id,
            start_key,
            end_key,
            epoch,
            peers,
            ..
        } = self
            .must_query_debug_info(region_id, Duration::from_secs(1))
            .unwrap()
            .region_state;
        let mut region = metapb::Region::default();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        let mut region_epoch = RegionEpoch::default();
        region_epoch.set_conf_ver(epoch.conf_ver);
        region_epoch.set_version(epoch.version);
        region.set_region_epoch(region_epoch);
        for peer in peers {
            region.mut_peers().push(new_peer(peer.store_id, peer.id));
        }
        region
    }
}

pub struct RunningState {
    store_id: u64,
    pub raft_engine: RaftTestEngine,
    pub registry: TabletRegistry<KvTestEngine>,
    pub system: StoreSystem<KvTestEngine, RaftTestEngine>,
    pub cfg: Arc<VersionTrack<Config>>,
    pub transport: TestTransport,
    snap_mgr: TabletSnapManager,
    background: Worker,
}

impl RunningState {
    fn new(
        pd_client: &Arc<RpcClient>,
        path: &Path,
        cfg: Arc<VersionTrack<Config>>,
        transport: TestTransport,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,
        logger: &Logger,
    ) -> (TestRouter, Self) {
        let raft_engine =
            engine_test::raft::new_engine(&format!("{}", path.join("raft").display()), None)
                .unwrap();

        let mut bootstrap = Bootstrap::new(&raft_engine, 0, pd_client.as_ref(), logger.clone());
        let store_id = bootstrap.bootstrap_store().unwrap();
        let mut store = Store::default();
        store.set_id(store_id);

        let (router, mut system) = create_store_batch_system::<KvTestEngine, RaftTestEngine>(
            &cfg.value(),
            store_id,
            logger.clone(),
        );
        let cf_opts = DATA_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let mut db_opt = DbOptions::default();
        db_opt.set_state_storage(Arc::new(StateStorage::new(
            raft_engine.clone(),
            router.clone(),
        )));
        let factory = Box::new(TestTabletFactory::new(db_opt, cf_opts));
        let registry = TabletRegistry::new(factory, path.join("tablets")).unwrap();
        if let Some(region) = bootstrap.bootstrap_first_region(&store, store_id).unwrap() {
            let factory = registry.tablet_factory();
            let path = registry.tablet_path(region.get_id(), RAFT_INIT_LOG_INDEX);
            let ctx = TabletContext::new(&region, Some(RAFT_INIT_LOG_INDEX));
            if factory.exists(&path) {
                registry.remove(region.get_id());
                factory.destroy_tablet(ctx.clone(), &path).unwrap();
            }
            // Create the tablet without loading it in cache.
            factory.open_tablet(ctx, &path).unwrap();
        }

        let router = RaftRouter::new(store_id, router);
        let store_meta = router.store_meta().clone();
        let snap_mgr = TabletSnapManager::new(path.join("tablets_snap").to_str().unwrap()).unwrap();

        let coprocessor_host = CoprocessorHost::new(
            router.store_router().clone(),
            raftstore::coprocessor::Config::default(),
        );
        let background = Worker::new("background");
        let pd_worker = LazyWorker::new("pd-worker");
        system
            .start(
                store_id,
                cfg.clone(),
                raft_engine.clone(),
                registry.clone(),
                transport.clone(),
                pd_client.clone(),
                router.store_router(),
                store_meta,
                snap_mgr.clone(),
                concurrency_manager,
                causal_ts_provider,
                coprocessor_host,
                background.clone(),
                pd_worker,
            )
            .unwrap();

        let state = Self {
            store_id,
            raft_engine,
            registry,
            system,
            cfg,
            transport,
            snap_mgr,
            background,
        };
        (TestRouter(router), state)
    }
}

impl Drop for RunningState {
    fn drop(&mut self) {
        self.system.shutdown();
        self.background.stop();
    }
}

pub struct TestNode {
    pd_client: Arc<RpcClient>,
    path: TempDir,
    running_state: Option<RunningState>,
    logger: Logger,
}

impl TestNode {
    fn with_pd(pd_server: &test_pd::Server<Service>, logger: Logger) -> TestNode {
        let pd_client = Arc::new(test_pd::util::new_client(pd_server.bind_addrs(), None));
        let path = TempDir::new().unwrap();
        TestNode {
            pd_client,
            path,
            running_state: None,
            logger,
        }
    }

    fn start(&mut self, cfg: Arc<VersionTrack<Config>>, trans: TestTransport) -> TestRouter {
        let (router, state) = RunningState::new(
            &self.pd_client,
            self.path.path(),
            cfg,
            trans,
            ConcurrencyManager::new(1.into()),
            None,
            &self.logger,
        );
        self.running_state = Some(state);
        router
    }

    #[allow(dead_code)]
    pub fn tablet_registry(&self) -> &TabletRegistry<KvTestEngine> {
        &self.running_state().unwrap().registry
    }

    pub fn pd_client(&self) -> &Arc<RpcClient> {
        &self.pd_client
    }

    fn stop(&mut self) {
        self.running_state.take();
    }

    fn restart(&mut self) -> TestRouter {
        let state = self.running_state().unwrap();
        let prev_transport = state.transport.clone();
        let cfg = state.cfg.clone();
        self.stop();
        self.start(cfg, prev_transport)
    }

    pub fn running_state(&self) -> Option<&RunningState> {
        self.running_state.as_ref()
    }

    pub fn id(&self) -> u64 {
        self.running_state().unwrap().store_id
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone)]
pub struct TestTransport {
    tx: Sender<RaftMessage>,
    flush_cnt: Arc<AtomicUsize>,
}

pub fn new_test_transport() -> (TestTransport, Receiver<RaftMessage>) {
    let (tx, rx) = channel::unbounded();
    let flush_cnt = Default::default();
    (TestTransport { tx, flush_cnt }, rx)
}

impl Transport for TestTransport {
    fn send(&mut self, msg: RaftMessage) -> raftstore_v2::Result<()> {
        let _ = self.tx.send(msg);
        Ok(())
    }

    fn set_store_allowlist(&mut self, _stores: Vec<u64>) {}

    fn need_flush(&self) -> bool {
        !self.tx.is_empty()
    }

    fn flush(&mut self) {
        self.flush_cnt.fetch_add(1, Ordering::SeqCst);
    }
}

// TODO: remove following when we finally integrate it in tikv-server binary.
pub fn v2_default_config() -> Config {
    let mut config = Config::default();
    config.store_io_pool_size = 1;
    config
}

/// Disable all ticks, so test case can schedule manually.
pub fn disable_all_auto_ticks(cfg: &mut Config) {
    cfg.raft_base_tick_interval = ReadableDuration::ZERO;
    cfg.raft_log_gc_tick_interval = ReadableDuration::ZERO;
    cfg.raft_log_compact_sync_interval = ReadableDuration::ZERO;
    cfg.raft_engine_purge_interval = ReadableDuration::ZERO;
    cfg.split_region_check_tick_interval = ReadableDuration::ZERO;
    cfg.region_compact_check_interval = ReadableDuration::ZERO;
    cfg.pd_heartbeat_tick_interval = ReadableDuration::ZERO;
    cfg.pd_store_heartbeat_tick_interval = ReadableDuration::ZERO;
    cfg.snap_mgr_gc_tick_interval = ReadableDuration::ZERO;
    cfg.lock_cf_compact_interval = ReadableDuration::ZERO;
    cfg.peer_stale_state_check_interval = ReadableDuration::ZERO;
    cfg.consistency_check_interval = ReadableDuration::ZERO;
    cfg.report_region_flow_interval = ReadableDuration::ZERO;
    cfg.check_leader_lease_interval = ReadableDuration::ZERO;
    cfg.merge_check_tick_interval = ReadableDuration::ZERO;
    cfg.cleanup_import_sst_interval = ReadableDuration::ZERO;
    cfg.inspect_interval = ReadableDuration::ZERO;
    cfg.report_min_resolved_ts_interval = ReadableDuration::ZERO;
    cfg.reactive_memory_lock_tick_interval = ReadableDuration::ZERO;
    cfg.report_region_buckets_tick_interval = ReadableDuration::ZERO;
    cfg.check_long_uncommitted_interval = ReadableDuration::ZERO;
}

pub struct Cluster {
    pd_server: test_pd::Server<Service>,
    nodes: Vec<TestNode>,
    receivers: Vec<Receiver<RaftMessage>>,
    pub routers: Vec<TestRouter>,
    logger: Logger,
}

impl Default for Cluster {
    fn default() -> Cluster {
        Cluster::with_node_count(1, None)
    }
}

impl Cluster {
    pub fn with_config(config: Config) -> Cluster {
        Cluster::with_node_count(1, Some(config))
    }

    pub fn with_node_count(count: usize, config: Option<Config>) -> Self {
        let pd_server = test_pd::Server::new(1);
        let logger = slog_global::borrow_global().new(o!());
        let mut cluster = Cluster {
            pd_server,
            nodes: vec![],
            receivers: vec![],
            routers: vec![],
            logger,
        };
        let mut cfg = if let Some(config) = config {
            config
        } else {
            v2_default_config()
        };
        disable_all_auto_ticks(&mut cfg);
        for _ in 1..=count {
            let mut node = TestNode::with_pd(&cluster.pd_server, cluster.logger.clone());
            let (tx, rx) = new_test_transport();
            let router = node.start(Arc::new(VersionTrack::new(cfg.clone())), tx);
            cluster.nodes.push(node);
            cluster.receivers.push(rx);
            cluster.routers.push(router);
        }
        cluster
    }

    pub fn restart(&mut self, offset: usize) {
        self.routers.remove(offset);
        let router = self.nodes[offset].restart();
        self.routers.insert(offset, router);
    }

    pub fn node(&self, offset: usize) -> &TestNode {
        &self.nodes[offset]
    }

    /// Send messages and wait for side effects are all handled.
    #[allow(clippy::vec_box)]
    pub fn dispatch(&self, region_id: u64, mut msgs: Vec<Box<RaftMessage>>) {
        let mut regions = HashSet::default();
        regions.insert(region_id);
        loop {
            for msg in msgs.drain(..) {
                let offset = match self
                    .nodes
                    .iter()
                    .position(|n| n.id() == msg.get_to_peer().get_store_id())
                {
                    Some(offset) => offset,
                    None => {
                        debug!(self.logger, "failed to find node"; "message" => ?msg);
                        continue;
                    }
                };
                // Simulate already received the snapshot.
                if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
                    let from_offset = match self
                        .nodes
                        .iter()
                        .position(|n| n.id() == msg.get_from_peer().get_store_id())
                    {
                        Some(offset) => offset,
                        None => {
                            debug!(self.logger, "failed to find snapshot source node"; "message" => ?msg);
                            continue;
                        }
                    };
                    let key = TabletSnapKey::new(
                        region_id,
                        msg.get_to_peer().get_id(),
                        msg.get_message().get_snapshot().get_metadata().get_term(),
                        msg.get_message().get_snapshot().get_metadata().get_index(),
                    );
                    let from_snap_mgr = &self.node(from_offset).running_state().unwrap().snap_mgr;
                    let to_snap_mgr = &self.node(offset).running_state().unwrap().snap_mgr;
                    let gen_path = from_snap_mgr.tablet_gen_path(&key);
                    let recv_path = to_snap_mgr.final_recv_path(&key);
                    assert!(gen_path.exists());
                    std::fs::rename(gen_path, recv_path.clone()).unwrap();
                    assert!(recv_path.exists());
                }
                regions.insert(msg.get_region_id());
                if let Err(e) = self.routers[offset].send_raft_message(msg) {
                    debug!(self.logger, "failed to send raft message"; "err" => ?e);
                }
            }
            for (router, rx) in self.routers.iter().zip(&self.receivers) {
                for region_id in &regions {
                    router.wait_flush(*region_id, Duration::from_secs(3));
                }
                while let Ok(msg) = rx.try_recv() {
                    msgs.push(Box::new(msg));
                }
            }
            regions.clear();
            if msgs.is_empty() {
                return;
            }
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.routers.clear();
        for node in &mut self.nodes {
            node.stop();
        }
    }
}

pub mod split_helper {
    use std::{thread, time::Duration};

    use engine_traits::CF_DEFAULT;
    use futures::executor::block_on;
    use kvproto::{
        metapb, pdpb,
        raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest, RaftCmdResponse, SplitRequest},
    };
    use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};

    use super::TestRouter;

    pub fn new_batch_split_region_request(
        split_keys: Vec<Vec<u8>>,
        ids: Vec<pdpb::SplitId>,
        right_derive: bool,
    ) -> AdminRequest {
        let mut req = AdminRequest::default();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        req.mut_splits().set_right_derive(right_derive);
        let mut requests = Vec::with_capacity(ids.len());
        for (mut id, key) in ids.into_iter().zip(split_keys) {
            let mut split = SplitRequest::default();
            split.set_split_key(key);
            split.set_new_region_id(id.get_new_region_id());
            split.set_new_peer_ids(id.take_new_peer_ids());
            requests.push(split);
        }
        req.mut_splits().set_requests(requests.into());
        req
    }

    pub fn must_split(region_id: u64, req: RaftCmdRequest, router: &mut TestRouter) {
        let (msg, sub) = PeerMsg::admin_command(req);
        router.send(region_id, msg).unwrap();
        block_on(sub.result()).unwrap();

        // TODO: when persistent implementation is ready, we can use tablet index of
        // the parent to check whether the split is done. Now, just sleep a second.
        thread::sleep(Duration::from_secs(1));
    }

    pub fn put(router: &mut TestRouter, region_id: u64, key: &[u8]) -> RaftCmdResponse {
        let header = Box::new(router.new_request_for(region_id).take_header());
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(CF_DEFAULT, key, b"v1");
        router.simple_write(region_id, header, put).unwrap()
    }

    // Split the region according to the parameters
    // return the updated original region
    pub fn split_region<'a>(
        router: &'a mut TestRouter,
        region: metapb::Region,
        peer: metapb::Peer,
        split_region_id: u64,
        split_peer: metapb::Peer,
        left_key: Option<&'a [u8]>,
        right_key: Option<&'a [u8]>,
        propose_key: &[u8],
        split_key: &[u8],
        right_derive: bool,
    ) -> (metapb::Region, metapb::Region) {
        let region_id = region.id;
        let mut req = RaftCmdRequest::default();
        req.mut_header().set_region_id(region_id);
        req.mut_header()
            .set_region_epoch(region.get_region_epoch().clone());
        req.mut_header().set_peer(peer);

        let mut split_id = pdpb::SplitId::new();
        split_id.new_region_id = split_region_id;
        split_id.new_peer_ids = vec![split_peer.id];
        let admin_req = new_batch_split_region_request(
            vec![propose_key.to_vec()],
            vec![split_id],
            right_derive,
        );
        req.mut_requests().clear();
        req.set_admin_request(admin_req);

        must_split(region_id, req, router);

        let (left, right) = if !right_derive {
            (
                router.region_detail(region_id),
                router.region_detail(split_region_id),
            )
        } else {
            (
                router.region_detail(split_region_id),
                router.region_detail(region_id),
            )
        };

        if let Some(right_key) = right_key {
            let resp = put(router, left.id, right_key);
            assert!(resp.get_header().has_error(), "{:?}", resp);
            let resp = put(router, right.id, right_key);
            assert!(!resp.get_header().has_error(), "{:?}", resp);
        }
        if let Some(left_key) = left_key {
            let resp = put(router, left.id, left_key);
            assert!(!resp.get_header().has_error(), "{:?}", resp);
            let resp = put(router, right.id, left_key);
            assert!(resp.get_header().has_error(), "{:?}", resp);
        }

        assert_eq!(left.get_end_key(), split_key);
        assert_eq!(right.get_start_key(), split_key);
        assert_eq!(region.get_start_key(), left.get_start_key());
        assert_eq!(region.get_end_key(), right.get_end_key());

        (left, right)
    }
}
