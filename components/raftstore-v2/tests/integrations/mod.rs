// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]
#![feature(assert_matches)]
#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]
// TODO: remove following when tests can be run.
#![allow(dead_code)]
#![allow(unused_imports)]

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crossbeam::channel::{self, Receiver, Sender};
use engine_test::{
    ctor::{CfOptions, DbOptions},
    kv::{KvTestEngine, TestTabletFactory},
    raft::RaftTestEngine,
};
use engine_traits::{TabletFactory, ALL_CFS};
use kvproto::{metapb::Store, raft_serverpb::RaftMessage};
use pd_client::RpcClient;
use raftstore::store::{Config, Transport, RAFT_INIT_LOG_INDEX};
use raftstore_v2::{create_store_batch_system, Bootstrap, StoreRouter, StoreSystem};
use slog::{o, Logger};
use tempfile::TempDir;
use test_pd::mocker::Service;
use tikv_util::config::VersionTrack;

mod test_election;

type TestRouter = StoreRouter<KvTestEngine, RaftTestEngine>;

struct TestNode {
    _pd_server: test_pd::Server<Service>,
    _pd_client: RpcClient,
    _path: TempDir,
    store: Store,
    raft_engine: Option<RaftTestEngine>,
    factory: Option<Arc<TestTabletFactory>>,
    system: Option<StoreSystem<KvTestEngine, RaftTestEngine>>,
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
        let factory = Arc::new(TestTabletFactory::new(
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
        factory
            .create_tablet(region.get_id(), RAFT_INIT_LOG_INDEX)
            .unwrap();

        TestNode {
            _pd_server: pd_server,
            _pd_client: pd_client,
            _path: path,
            store,
            raft_engine: Some(raft_engine),
            factory: Some(factory),
            system: None,
            logger,
        }
    }

    fn start(
        &mut self,
        cfg: &Arc<VersionTrack<Config>>,
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
        self.system = Some(system);
        router
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

fn setup_default_cluster() -> (TestNode, Receiver<RaftMessage>, TestRouter) {
    let mut node = TestNode::new();
    let cfg = Default::default();
    let (tx, rx) = new_test_transport();
    let router = node.start(&cfg, tx);
    (node, rx, router)
}
