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
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use crossbeam::channel::{self, Receiver, Sender};
use engine_test::{
    ctor::{CfOptions, DbOptions},
    kv::{KvTestEngine, TestTabletFactoryV2},
    raft::RaftTestEngine,
};
use engine_traits::{OpenOptions, TabletFactory, ALL_CFS};
use futures::executor::block_on;
use kvproto::{
    metapb::Store,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use pd_client::RpcClient;
use raftstore::store::{region_meta::RegionMeta, Config, Transport, RAFT_INIT_LOG_INDEX};
use raftstore_v2::{
    create_store_batch_system,
    router::{DebugInfoChannel, PeerMsg, QueryResult},
    Bootstrap, StoreRouter, StoreSystem,
};
use slog::{o, Logger};
use tempfile::TempDir;
use test_pd::mocker::Service;
use tikv_util::config::{ReadableDuration, VersionTrack};

mod test_basic_write;
mod test_life;
mod test_status;

#[derive(Clone)]
struct TestRouter(StoreRouter<KvTestEngine, RaftTestEngine>);

impl Deref for TestRouter {
    type Target = StoreRouter<KvTestEngine, RaftTestEngine>;

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
}

struct RunningState {
    raft_engine: RaftTestEngine,
    factory: Arc<TestTabletFactoryV2>,
    system: StoreSystem<KvTestEngine, RaftTestEngine>,
    cfg: Arc<VersionTrack<Config>>,
    transport: TestTransport,
}

impl RunningState {
    fn new(
        pd_client: &RpcClient,
        path: &Path,
        cfg: Arc<VersionTrack<Config>>,
        transport: TestTransport,
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
        let raft_engine =
            engine_test::raft::new_engine(&format!("{}", path.join("raft").display()), None)
                .unwrap();
        let mut bootstrap = Bootstrap::new(&raft_engine, 0, pd_client, logger.clone());
        let store_id = bootstrap.bootstrap_store().unwrap();
        let mut store = Store::default();
        store.set_id(store_id);
        if let Some(region) = bootstrap.bootstrap_first_region(&store, store_id).unwrap() {
            if factory.exists(region.get_id(), RAFT_INIT_LOG_INDEX) {
                factory
                    .destroy_tablet(region.get_id(), RAFT_INIT_LOG_INDEX)
                    .unwrap();
            }
            factory
                .open_tablet(
                    region.get_id(),
                    Some(RAFT_INIT_LOG_INDEX),
                    OpenOptions::default().set_create_new(true),
                )
                .unwrap();
        }

        let (router, mut system) = create_store_batch_system::<KvTestEngine, RaftTestEngine>(
            &cfg.value(),
            store_id,
            logger.clone(),
        );
        system
            .start(
                store_id,
                cfg.clone(),
                raft_engine.clone(),
                factory.clone(),
                transport.clone(),
                &router,
            )
            .unwrap();

        let state = Self {
            raft_engine,
            factory,
            system,
            cfg,
            transport,
        };
        (TestRouter(router), state)
    }
}

impl Drop for RunningState {
    fn drop(&mut self) {
        self.system.shutdown();
    }
}

struct TestNode {
    pd_client: RpcClient,
    path: TempDir,
    running_state: Option<RunningState>,
    logger: Logger,
}

impl TestNode {
    fn with_pd(pd_server: &test_pd::Server<Service>) -> TestNode {
        let logger = slog_global::borrow_global().new(o!());
        let pd_client = test_pd::util::new_client(pd_server.bind_addrs(), None);
        let path = TempDir::new().unwrap();

        TestNode {
            pd_client,
            path,
            running_state: None,
            logger,
        }
    }

    fn start(&mut self, cfg: Arc<VersionTrack<Config>>, trans: TestTransport) -> TestRouter {
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

#[derive(Clone)]
pub struct TestTransport {
    tx: Sender<RaftMessage>,
    flush_cnt: Arc<AtomicUsize>,
}

fn new_test_transport() -> (TestTransport, Receiver<RaftMessage>) {
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
fn v2_default_config() -> Config {
    let mut config = Config::default();
    config.store_io_pool_size = 1;
    config
}

/// Disable all ticks, so test case can schedule manually.
fn disable_all_auto_ticks(cfg: &mut Config) {
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

struct Cluster {
    pd_server: test_pd::Server<Service>,
    nodes: Vec<TestNode>,
    receivers: Vec<Receiver<RaftMessage>>,
    routers: Vec<TestRouter>,
}

impl Default for Cluster {
    fn default() -> Cluster {
        Cluster::with_node_count(1)
    }
}

impl Cluster {
    fn with_node_count(count: usize) -> Self {
        let pd_server = test_pd::Server::new(1);
        let mut cluster = Cluster {
            pd_server,
            nodes: vec![],
            receivers: vec![],
            routers: vec![],
        };
        let mut cfg = v2_default_config();
        disable_all_auto_ticks(&mut cfg);
        for _ in 1..=count {
            let mut node = TestNode::with_pd(&cluster.pd_server);
            let (tx, rx) = new_test_transport();
            let router = node.start(Arc::new(VersionTrack::new(cfg.clone())), tx);
            cluster.nodes.push(node);
            cluster.receivers.push(rx);
            cluster.routers.push(router);
        }
        cluster
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
