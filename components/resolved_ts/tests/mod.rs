// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::*, time::Duration};

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::{RocksEngine, RocksSnapshot};
use grpcio::{ChannelBuilder, ClientUnaryReceiver, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use online_config::ConfigValue;
use raftstore::coprocessor::CoprocessorHost;
use resolved_ts::{Observer, Task};
use test_raftstore::*;
use tikv::config::ResolvedTsConfig;
use tikv_util::{worker::LazyWorker, HandyRwLock};
use txn_types::TimeStamp;
static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}

pub struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub endpoints: HashMap<u64, LazyWorker<Task<RocksSnapshot>>>,
    pub obs: HashMap<u64, Observer<RocksEngine>>,
    tikv_cli: HashMap<u64, TikvClient>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,

    env: Arc<Environment>,
}

impl TestSuite {
    pub fn new(count: usize) -> Self {
        let mut cluster = new_server_cluster(1, count);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);
        Self::with_cluster(count, cluster)
    }

    pub fn with_cluster(count: usize, mut cluster: Cluster<ServerCluster>) -> Self {
        init();
        let pd_cli = cluster.pd_client.clone();
        let mut endpoints = HashMap::default();
        let mut obs = HashMap::default();
        let mut concurrency_managers = HashMap::default();
        // Hack! node id are generated from 1..count+1.
        for id in 1..=count as u64 {
            // Create and run cdc endpoints.
            let worker = LazyWorker::new(format!("cdc-{}", id));
            let mut sim = cluster.sim.wl();

            // Register cdc service to gRPC server.
            let scheduler = worker.scheduler();
            let rts_ob = resolved_ts::Observer::new(scheduler.clone());
            obs.insert(id, rts_ob.clone());
            sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
                move |host: &mut CoprocessorHost<_>| {
                    rts_ob.register_to(host);
                },
            ));
            endpoints.insert(id, worker);
        }

        cluster.run();
        for (id, worker) in &mut endpoints {
            let sim = cluster.sim.wl();
            let raft_router = sim.get_server_router(*id);
            let cm = sim.get_concurrency_manager(*id);
            let env = Arc::new(Environment::new(1));
            let cfg = ResolvedTsConfig {
                advance_ts_interval: tikv_util::config::ReadableDuration(Duration::from_millis(10)),
                ..Default::default()
            };
            let rts_endpoint = resolved_ts::Endpoint::new(
                &cfg,
                worker.scheduler(),
                raft_router,
                cluster.store_metas[id].clone(),
                pd_cli.clone(),
                cm.clone(),
                env,
                sim.security_mgr.clone(),
                resolved_ts::DummySinker::new(),
            );
            concurrency_managers.insert(*id, cm);
            worker.start(rts_endpoint);
        }

        TestSuite {
            cluster,
            endpoints,
            obs,
            concurrency_managers,
            env: Arc::new(Environment::new(1)),
            tikv_cli: HashMap::default(),
        }
    }

    pub fn stop(mut self) {
        for (_, mut worker) in self.endpoints {
            worker.stop();
        }
        self.cluster.shutdown();
    }

    pub fn must_change_advance_ts_interval(&self, store_id: u64, new_interval: Duration) {
        let change = {
            let mut c = std::collections::HashMap::default();
            c.insert(
                "advance_ts_interval".to_owned(),
                ConfigValue::Duration(new_interval.as_millis() as u64),
            );
            c
        };
        let scheduler = self.endpoints.get(&store_id).unwrap().scheduler();
        scheduler.schedule(Task::ChangeConfig { change }).unwrap();
    }

    pub fn must_kv_prewrite(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
    ) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(region_id));
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = self
            .get_tikv_client(region_id)
            .kv_prewrite(&prewrite_req)
            .unwrap();
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    pub fn must_kv_commit(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    ) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(region_id));
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let commit_resp = self
            .get_tikv_client(region_id)
            .kv_commit(&commit_req)
            .unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    pub fn must_kv_rollback(&mut self, region_id: u64, keys: Vec<Vec<u8>>, start_ts: TimeStamp) {
        let mut rollback_req = BatchRollbackRequest::default();
        rollback_req.set_context(self.get_context(region_id));
        rollback_req.start_version = start_ts.into_inner();
        rollback_req.set_keys(keys.into_iter().collect());
        let rollback_resp = self
            .get_tikv_client(region_id)
            .kv_batch_rollback(&rollback_req)
            .unwrap();
        assert!(
            !rollback_resp.has_region_error(),
            "{:?}",
            rollback_resp.get_region_error()
        );
        assert!(
            !rollback_resp.has_error(),
            "{:?}",
            rollback_resp.get_error()
        );
    }

    pub fn must_check_txn_status(
        &mut self,
        region_id: u64,
        primary_key: Vec<u8>,
        lock_ts: TimeStamp,
        caller_start_ts: TimeStamp,
        current_ts: TimeStamp,
        rollback_if_not_exist: bool,
    ) -> Action {
        let mut req = CheckTxnStatusRequest::default();
        req.set_context(self.get_context(region_id));
        req.set_primary_key(primary_key);
        req.set_lock_ts(lock_ts.into_inner());
        req.set_caller_start_ts(caller_start_ts.into_inner());
        req.set_current_ts(current_ts.into_inner());
        req.set_rollback_if_not_exist(rollback_if_not_exist);
        let resp = self
            .get_tikv_client(region_id)
            .kv_check_txn_status(&req)
            .unwrap();
        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert!(!resp.has_error(), "{:?}", resp.get_error());
        resp.get_action()
    }

    pub fn must_acquire_pessimistic_lock(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        start_ts: TimeStamp,
        for_update_ts: TimeStamp,
    ) {
        let mut lock_req = PessimisticLockRequest::default();
        lock_req.set_context(self.get_context(region_id));
        lock_req.set_mutations(muts.into_iter().collect());
        lock_req.start_version = start_ts.into_inner();
        lock_req.for_update_ts = for_update_ts.into_inner();
        lock_req.primary_lock = pk;
        let lock_resp = self
            .get_tikv_client(region_id)
            .kv_pessimistic_lock(&lock_req)
            .unwrap();
        assert!(
            !lock_resp.has_region_error(),
            "{:?}",
            lock_resp.get_region_error()
        );
        assert!(
            lock_resp.get_errors().is_empty(),
            "{:?}",
            lock_resp.get_errors()
        );
    }

    pub fn must_kv_pessimistic_prewrite(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
        for_update_ts: TimeStamp,
    ) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(region_id));
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        prewrite_req.for_update_ts = for_update_ts.into_inner();
        prewrite_req.mut_is_pessimistic_lock().push(true);
        let prewrite_resp = self
            .get_tikv_client(region_id)
            .kv_prewrite(&prewrite_req)
            .unwrap();
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    pub fn async_kv_commit(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    ) -> ClientUnaryReceiver<CommitResponse> {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(region_id));
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        self.get_tikv_client(region_id)
            .kv_commit_async(&commit_req)
            .unwrap()
    }

    pub fn get_context(&mut self, region_id: u64) -> Context {
        let epoch = self.cluster.get_region_epoch(region_id);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context
    }

    pub fn get_tikv_client(&mut self, region_id: u64) -> &TikvClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let store_id = leader.get_store_id();
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.tikv_cli
            .entry(leader.get_store_id())
            .or_insert_with(|| {
                let channel = ChannelBuilder::new(env).connect(&addr);
                TikvClient::new(channel)
            })
    }

    pub fn get_txn_concurrency_manager(&self, store_id: u64) -> Option<ConcurrencyManager> {
        self.concurrency_managers.get(&store_id).cloned()
    }

    pub fn set_tso(&self, ts: impl Into<TimeStamp>) {
        self.cluster.pd_client.set_tso(ts.into());
    }

    pub fn region_resolved_ts(&mut self, region_id: u64) -> Option<TimeStamp> {
        let leader = self.cluster.leader_of_region(region_id)?;
        let meta = self.cluster.store_metas[&leader.store_id].lock().unwrap();
        Some(
            meta.region_read_progress
                .get_safe_ts(&region_id)
                .unwrap()
                .into(),
        )
    }

    pub fn must_get_rts(&mut self, region_id: u64, rts: TimeStamp) {
        for _ in 0..50 {
            if let Some(ts) = self.region_resolved_ts(region_id) {
                if rts == ts {
                    return;
                }
            }
            sleep_ms(100)
        }
        panic!("fail to get same ts after 50 trys");
    }

    pub fn must_get_rts_ge(&mut self, region_id: u64, rts: TimeStamp) {
        for _ in 0..50 {
            if let Some(ts) = self.region_resolved_ts(region_id) {
                if rts < ts {
                    return;
                }
            }
            sleep_ms(100)
        }
        panic!("fail to get greater ts after 50 trys");
    }
}
