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
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
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
use raftstore::store::{Config, Transport, RAFT_INIT_LOG_INDEX};
use raftstore_v2::{
    create_store_batch_system,
    router::{PeerMsg, QueryResult},
    Bootstrap, StoreRouter, StoreSystem,
};
use slog::{o, Logger};
use tempfile::TempDir;
use test_pd::mocker::Service;
use tikv_util::config::{ReadableDuration, VersionTrack};

mod test_status;

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

    fn command(&self, region_id: u64, req: RaftCmdRequest) -> Option<RaftCmdResponse> {
        let (msg, sub) = PeerMsg::raft_command(req);
        self.send(region_id, msg).unwrap();
        block_on(sub.result())
    }
}

struct TestNode {
    _pd_server: test_pd::Server<Service>,
    _pd_client: RpcClient,
    _path: TempDir,
    store: Store,
    raft_engine: Option<RaftTestEngine>,
    factory: Option<Arc<TestTabletFactoryV2>>,
    system: Option<StoreSystem<KvTestEngine, RaftTestEngine>>,
    cfg: Option<Arc<VersionTrack<Config>>>,
    logger: Logger,
}

impl TestNode {
    fn new() -> TestNode {
        let logger = slog_global::borrow_global().new(o!());
        let pd_server = test_pd::Server::new(1);
        let pd_client = test_pd::util::new_client(pd_server.bind_addrs(), None);
        let path = TempDir::new().unwrap();

        let cf_opts = ALL_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Arc::new(TestTabletFactoryV2::new(
            path.path(),
            DbOptions::default(),
            cf_opts,
        ));
        let raft_engine =
            engine_test::raft::new_engine(&format!("{}", path.path().join("raft").display()), None)
                .unwrap();
        let mut bootstrap = Bootstrap::new(&raft_engine, 0, &pd_client, logger.clone());
        let store_id = bootstrap.bootstrap_store().unwrap();
        let mut store = Store::default();
        store.set_id(store_id);
        let region = bootstrap
            .bootstrap_first_region(&store, store_id)
            .unwrap()
            .unwrap();
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

        TestNode {
            _pd_server: pd_server,
            _pd_client: pd_client,
            _path: path,
            store,
            raft_engine: Some(raft_engine),
            factory: Some(factory),
            system: None,
            cfg: None,
            logger,
        }
    }

    fn start(
        &mut self,
        cfg: Arc<VersionTrack<Config>>,
        trans: impl Transport + 'static,
    ) -> TestRouter {
        let (router, mut system) = create_store_batch_system::<KvTestEngine, RaftTestEngine>(
            &cfg.value(),
            self.store.clone(),
            self.logger.clone(),
        );
        system
            .start(
                self.store.clone(),
                cfg.clone(),
                self.raft_engine.clone().unwrap(),
                self.factory.clone().unwrap(),
                trans,
                &router,
            )
            .unwrap();
        self.cfg = Some(cfg);
        self.system = Some(system);
        TestRouter(router)
    }

    fn config(&self) -> &Arc<VersionTrack<Config>> {
        self.cfg.as_ref().unwrap()
    }

    fn stop(&mut self) {
        if let Some(mut system) = self.system.take() {
            system.shutdown();
        }
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.stop();
        self.raft_engine.take();
        self.factory.take();
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

fn setup_default_cluster() -> (TestNode, Receiver<RaftMessage>, TestRouter) {
    let mut node = TestNode::new();
    let mut cfg = v2_default_config();
    disable_all_auto_ticks(&mut cfg);
    let (tx, rx) = new_test_transport();
    let router = node.start(Arc::new(VersionTrack::new(cfg)), tx);
    (node, rx, router)
}
