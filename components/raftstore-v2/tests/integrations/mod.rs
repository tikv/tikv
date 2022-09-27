// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(assert_matches)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]
// TODO: remove following when tests can be run.
#![allow(dead_code)]
#![allow(unused_imports)]

use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    time::{Duration, Instant},
};

use collections::HashMap;
use crossbeam::channel::{self, Receiver, Sender};
use engine_test::{
    ctor::{CfOptions, DbOptions},
    kv::{KvTestEngine, TestTabletFactoryV2},
    raft::RaftTestEngine,
};
use engine_traits::{OpenOptions, RaftEngine, RaftLogBatch, TabletFactory, ALL_CFS};
use futures::executor::block_on;
use kvproto::{
    metapb::Store,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use pd_client::{PdClient, RpcClient};
use raft::eraftpb::MessageType;
use raftstore::store::{
    region_meta::RegionMeta, Config, Transport, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
    RAFT_INIT_LOG_INDEX,
};
use raftstore_v2::{
    create_store_batch_system,
    raft::write_initial_states,
    router::{DebugInfoChannel, PeerMsg, QueryResult},
    Bootstrap, StoreMeta, StoreRouter, StoreSystem,
};
use slog::{error, o, Logger};
use tempfile::{TempDir, TempPath};
use test_pd::mocker::Service;
use test_raftstore::{filter_send, Filter};
use tikv_util::{
    box_err,
    config::{ReadableDuration, VersionTrack},
    store::new_peer,
    HandyRwLock,
};

mod test_basic_write;
mod test_life;
mod test_read;
mod test_snap;
mod test_status;

#[derive(Clone)]
struct TestRouter {
    router: StoreRouter<KvTestEngine, RaftTestEngine>,
    recv_filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
}

impl Deref for TestRouter {
    type Target = StoreRouter<KvTestEngine, RaftTestEngine>;

    fn deref(&self) -> &Self::Target {
        &self.router
    }
}

impl DerefMut for TestRouter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.router
    }
}

impl TestRouter {
    pub fn new(router: StoreRouter<KvTestEngine, RaftTestEngine>) -> TestRouter {
        TestRouter {
            router,
            recv_filters: Arc::new(RwLock::new(vec![])),
        }
    }

    fn query(&self, region_id: u64, req: RaftCmdRequest) -> Option<QueryResult> {
        let (msg, sub) = PeerMsg::raft_query(req);
        self.send(region_id, msg).unwrap();
        block_on(sub.result())
    }

    fn must_query_debug_info(&self, region_id: u64, timeout: Duration) -> Option<RegionMeta> {
        let timer = Instant::now();
        while timer.elapsed() < timeout {
            let (ch, sub) = DebugInfoChannel::pair();
            let msg = PeerMsg::QueryDebugInfo(ch);
            if self.send(region_id, msg).is_err() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            return block_on(sub.result());
        }
        None
    }

    fn command(&self, region_id: u64, req: RaftCmdRequest) -> Option<RaftCmdResponse> {
        let (msg, sub) = PeerMsg::raft_command(req);
        self.send(region_id, msg).unwrap();
        block_on(sub.result())
    }

    #[allow(clippy::boxed_local)]
    fn send_raft_message(&self, msg: Box<RaftMessage>) -> raftstore_v2::Result<()> {
        filter_send(&self.recv_filters, *msg, |m| {
            self.router
                .send_raft_message(Box::new(m))
                .map_err(|e| box_err!(e))
        })
    }

    fn clear_recv_filters(&mut self) {
        self.recv_filters.wl().clear();
    }

    fn add_recv_filter(&mut self, filter: Box<dyn Filter>) {
        self.recv_filters.wl().push(filter);
    }
}

struct RunningState {
    raft_engine: RaftTestEngine,
    factory: Arc<TestTabletFactoryV2>,
    system: Option<StoreSystem<KvTestEngine, RaftTestEngine>>,
    cfg: Arc<VersionTrack<Config>>,
    transport: TestNodeTransport,
    logger: Logger,
}

struct ClusterBuilder {
    logger: Logger,
    pd_server: Arc<test_pd::Server<Service>>,
    raft_engines: Vec<RaftTestEngine>,
    store_ids: Vec<u64>,
    paths: Vec<Arc<TempDir>>,
    trans: ChannelTransport,
}

impl ClusterBuilder {
    fn new(n: usize) -> Self {
        let pd_server = test_pd::Server::new(1);
        let logger = slog_global::borrow_global().new(o!());
        let pd_client = test_pd::util::new_client(pd_server.bind_addrs(), None);
        let mut builder = ClusterBuilder {
            logger,
            pd_server: Arc::new(pd_server),
            raft_engines: vec![],
            store_ids: vec![],
            paths: vec![],
            trans: ChannelTransport::default(),
        };
        for _ in 1..=n {
            let logger = slog_global::borrow_global().new(o!());
            let path = Arc::new(TempDir::new().unwrap());
            let raft_engine = engine_test::raft::new_engine(
                &format!("{}", path.path().join("raft").display()),
                None,
            )
            .unwrap();
            let mut bootstrap = Bootstrap::new(&raft_engine, 0, &pd_client, logger);
            let store_id = bootstrap.bootstrap_store().unwrap();
            builder.paths.push(path);
            builder.raft_engines.push(raft_engine);
            builder.store_ids.push(store_id);
        }
        builder
    }

    fn build(&self, cfg: Arc<VersionTrack<Config>>) -> Cluster {
        let pd_client = test_pd::util::new_client(self.pd_server.bind_addrs(), None);
        {
            self.prepare_bootstrap_first_region(&pd_client);
        }
        let mut cluster = Cluster {
            pd_server: self.pd_server.clone(),
            nodes: vec![],
            receivers: vec![],
            routers: vec![],
            trans: self.trans.clone(),
        };

        for (i, store_id) in self.store_ids.iter().enumerate() {
            // init the config
            let logger = slog_global::borrow_global().new(o!());
            let path = self.paths[i].path();
            let node_trans = TestNodeTransport::new(self.trans.clone());
            let pd_client_inner = test_pd::util::new_client(self.pd_server.bind_addrs(), None);
            let raft_engine = self.raft_engines[i].clone();
            // create runing state
            let (router, state) = RunningState::new_with_engine(
                &pd_client_inner,
                path,
                raft_engine,
                cfg.clone(),
                node_trans,
                &logger,
            );
            let node = TestNode {
                pd_client: pd_client_inner,
                running_state: Some(state),
                logger,
                path: self.paths[i].clone(),
            };
            self.trans
                .core
                .lock()
                .unwrap()
                .insert(*store_id, router.clone());
            cluster.nodes.push(node);
            cluster.routers.push(router)
        }
        cluster
    }

    fn prepare_bootstrap_first_region(&self, pd_client: &RpcClient) {
        let region_id = pd_client.alloc_id().unwrap();
        let mut region = kvproto::metapb::Region::default();
        region.set_id(region_id);
        region.set_start_key(keys::EMPTY_KEY.to_vec());
        region.set_end_key(keys::EMPTY_KEY.to_vec());
        region.mut_region_epoch().set_version(INIT_EPOCH_VER);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
        for store_id in self.store_ids.iter() {
            let peer_id = pd_client.alloc_id().unwrap();
            let peer = new_peer(store_id.to_owned(), peer_id);
            region.mut_peers().push(peer.clone());
        }

        for raft_engine in self.raft_engines.iter() {
            let mut wb = raft_engine.log_batch(10);
            wb.put_prepare_bootstrap_region(&region).unwrap();
            write_initial_states(&mut wb, region.clone()).unwrap();
            raft_engine.consume(&mut wb, true).unwrap();
        }
    }
}

impl RunningState {
    fn new(
        pd_client: &RpcClient,
        path: &Path,
        cfg: Arc<VersionTrack<Config>>,
        transport: TestNodeTransport,
        logger: &Logger,
    ) -> (TestRouter, Self) {
        let cf_opts = ALL_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Arc::new(TestTabletFactoryV2::new(
            path,
            DbOptions::default(),
            cf_opts,
        ));
        let mut rengine: Option<RaftTestEngine> = None;
        for _ in 0..10 {
            match engine_test::raft::new_engine(&format!("{}", path.join("raft").display()), None) {
                Ok(engine) => {
                    rengine = Some(engine);
                    break;
                }
                Err(err) => {
                    error!(logger, "failed to create raft engine"; "err" => ?err);
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }

        let mut state = RunningState {
            raft_engine: rengine.unwrap(),
            factory,
            system: None,
            cfg,
            transport,
            logger: logger.clone(),
        };
        let router = state.start_system(pd_client);
        (router, state)
    }

    fn new_with_engine(
        pd_client: &RpcClient,
        path: &Path,
        raft_engine: RaftTestEngine,
        cfg: Arc<VersionTrack<Config>>,
        transport: TestNodeTransport,
        logger: &Logger,
    ) -> (TestRouter, Self) {
        let cf_opts = ALL_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();

        let factory = Arc::new(TestTabletFactoryV2::new(
            path,
            DbOptions::default(),
            cf_opts,
        ));
        let mut state = RunningState {
            raft_engine,
            factory,
            system: None,
            cfg,
            transport,
            logger: logger.clone(),
        };
        let router = state.start_system(pd_client);
        (router, state)
    }

    fn start_system(&mut self, pd_client: &RpcClient) -> TestRouter {
        let mut bootstrap = Bootstrap::new(&self.raft_engine, 0, pd_client, self.logger.clone());
        let store_id = bootstrap.bootstrap_store().unwrap();
        let mut store = Store::default();
        store.set_id(store_id);
        if let Some(region) = bootstrap.bootstrap_first_region(&store, store_id).unwrap() {
            if self.factory.exists(region.get_id(), RAFT_INIT_LOG_INDEX) {
                self.factory
                    .destroy_tablet(region.get_id(), RAFT_INIT_LOG_INDEX)
                    .unwrap();
            }
            let tablet = self
                .factory
                .open_tablet(
                    region.get_id(),
                    Some(RAFT_INIT_LOG_INDEX),
                    OpenOptions::default().set_create_new(true),
                )
                .unwrap();
            bootstrap.initial_first_tablet(&tablet, &region).unwrap();
        }

        let (router, mut system) = create_store_batch_system::<KvTestEngine, RaftTestEngine>(
            &self.cfg.value(),
            store_id,
            self.logger.clone(),
        );

        let store_meta = Arc::new(Mutex::new(StoreMeta::<KvTestEngine>::new()));
        system
            .start(
                store_id,
                self.cfg.clone(),
                self.raft_engine.clone(),
                self.factory.clone(),
                self.transport.clone(),
                &router,
                store_meta,
            )
            .unwrap();
        self.system = Some(system);
        TestRouter::new(router)
    }
}

impl Drop for RunningState {
    fn drop(&mut self) {
        self.system.as_mut().unwrap().shutdown();
    }
}

struct TestNode {
    pd_client: RpcClient,
    running_state: Option<RunningState>,
    logger: Logger,
    path: Arc<TempDir>,
}

impl TestNode {
    fn with_pd(pd_server: &test_pd::Server<Service>) -> TestNode {
        let logger = slog_global::borrow_global().new(o!());
        let pd_client = test_pd::util::new_client(pd_server.bind_addrs(), None);
        let path = Arc::new(TempDir::new().unwrap());

        TestNode {
            pd_client,
            running_state: None,
            logger,
            path,
        }
    }

    fn start(&mut self, cfg: Arc<VersionTrack<Config>>, trans: TestNodeTransport) -> TestRouter {
        let (router, state) =
            RunningState::new(&self.pd_client, self.path.path(), cfg, trans, &self.logger);
        self.running_state = Some(state);
        router
    }

    fn config(&self) -> &Arc<VersionTrack<Config>> {
        &self.running_state.as_ref().unwrap().cfg
    }

    fn stop(&mut self) {
        self.running_state.take();
    }

    fn restart(&mut self) -> TestRouter {
        let state = self.running_state.as_ref().unwrap();
        let prev_transport = state.transport.clone();
        let cfg = state.cfg.clone();
        self.stop();
        self.start(cfg, prev_transport)
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Default)]
pub struct ChannelTransport {
    core: Arc<Mutex<HashMap<u64, TestRouter>>>,
}

impl Transport for ChannelTransport {
    fn send(&mut self, msg: RaftMessage) -> raftstore_v2::Result<()> {
        let to_store = msg.get_to_peer().get_store_id();
        let core = self.core.lock().unwrap();
        // TODO: handle snapshot message
        match core.get(&to_store) {
            Some(h) => {
                h.send_raft_message(Box::new(msg))?;
                Ok(())
            }
            _ => Err(box_err!("missing sender for store {}", to_store)),
        }
    }

    fn set_store_allowlist(&mut self, _allowlist: Vec<u64>) {
        unimplemented!();
    }

    fn need_flush(&self) -> bool {
        false
    }

    fn flush(&mut self) {}
}

#[derive(Clone)]
pub struct TestNodeTransport {
    flush_cnt: Arc<AtomicUsize>,
    ch: ChannelTransport,
}

impl TestNodeTransport {
    pub fn new(ch: ChannelTransport) -> Self {
        Self {
            flush_cnt: Default::default(),
            ch,
        }
    }
}

impl Transport for TestNodeTransport {
    fn send(&mut self, msg: RaftMessage) -> raftstore_v2::Result<()> {
        let _ = self.ch.send(msg);
        Ok(())
    }

    fn set_store_allowlist(&mut self, _stores: Vec<u64>) {}

    fn need_flush(&self) -> bool {
        self.ch.need_flush()
    }

    fn flush(&mut self) {
        self.flush_cnt.fetch_add(1, Ordering::SeqCst);
        self.ch.flush();
    }
}

// TODO: remove following when we finally integrate it in tikv-server binary.
fn v2_default_config() -> Config {
    let mut config = Config::default();
    config.store_io_pool_size = 1;
    config
}

/// Disable all ticks, so test case can schedule manually.
fn disable_all_auto_ticks(cfg: &mut Config) {
    cfg.raft_base_tick_interval = ReadableDuration(Duration::from_millis(10));
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

struct Cluster {
    pd_server: Arc<test_pd::Server<Service>>,
    nodes: Vec<TestNode>,
    receivers: Vec<Receiver<RaftMessage>>,
    routers: Vec<TestRouter>,
    trans: ChannelTransport,
}

impl Default for Cluster {
    fn default() -> Cluster {
        Cluster::with_node_count(1)
    }
}

impl Cluster {
    fn with_node_count(count: usize) -> Self {
        let mut cfg = v2_default_config();
        disable_all_auto_ticks(&mut cfg);
        ClusterBuilder::new(count).build(Arc::new(VersionTrack::new(cfg.clone())))
    }

    fn restart(&mut self, offset: usize) {
        let router = self.nodes[offset].restart();
        self.routers[offset] = router;
    }

    fn node(&self, offset: usize) -> &TestNode {
        &self.nodes[offset]
    }

    fn router(&self, offset: usize) -> TestRouter {
        self.routers[offset].clone()
    }
}
