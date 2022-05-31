// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::*, time::Duration};

use cdc::{recv_timeout, CdcObserver, FeatureGate, MemoryQuota, Task};
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksEngine;
use grpcio::{
    ChannelBuilder, ClientDuplexReceiver, ClientDuplexSender, ClientUnaryReceiver, Environment,
};
use kvproto::{
    cdcpb::{create_change_data, ChangeDataClient, ChangeDataEvent, ChangeDataRequest},
    kvrpcpb::*,
    tikvpb::TikvClient,
};
use online_config::OnlineConfig;
use raftstore::coprocessor::CoprocessorHost;
use test_raftstore::*;
use tikv::{config::CdcConfig, server::DEFAULT_CLUSTER_ID};
use tikv_util::{
    config::ReadableDuration,
    worker::{LazyWorker, Runnable},
    HandyRwLock,
};
use txn_types::TimeStamp;
static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(test_util::setup_for_ci);
}

#[derive(Clone)]
pub struct ClientReceiver {
    receiver: Arc<Mutex<Option<ClientDuplexReceiver<ChangeDataEvent>>>>,
}

impl ClientReceiver {
    pub fn replace(
        &self,
        rx: Option<ClientDuplexReceiver<ChangeDataEvent>>,
    ) -> Option<ClientDuplexReceiver<ChangeDataEvent>> {
        std::mem::replace(&mut *self.receiver.lock().unwrap(), rx)
    }
}

#[allow(clippy::type_complexity)]
pub fn new_event_feed(
    client: &ChangeDataClient,
) -> (
    ClientDuplexSender<ChangeDataRequest>,
    ClientReceiver,
    Box<dyn Fn(bool) -> ChangeDataEvent + Send>,
) {
    let (req_tx, resp_rx) = client.event_feed().unwrap();
    let event_feed_wrap = Arc::new(Mutex::new(Some(resp_rx)));
    let event_feed_wrap_clone = event_feed_wrap.clone();

    let receive_event = move |keep_resolved_ts: bool| loop {
        let mut events;
        {
            let mut event_feed = event_feed_wrap_clone.lock().unwrap();
            events = event_feed.take();
        }
        let mut events_rx = if let Some(events_rx) = events.as_mut() {
            events_rx
        } else {
            return ChangeDataEvent::default();
        };
        let change_data =
            if let Some(event) = recv_timeout(&mut events_rx, Duration::from_secs(5)).unwrap() {
                event
            } else {
                return ChangeDataEvent::default();
            };
        {
            let mut event_feed = event_feed_wrap_clone.lock().unwrap();
            *event_feed = events;
        }
        let change_data_event = change_data.unwrap_or_default();
        if !keep_resolved_ts && change_data_event.has_resolved_ts() {
            continue;
        }
        tikv_util::info!("cdc receive event {:?}", change_data_event);
        break change_data_event;
    };
    (
        req_tx,
        ClientReceiver {
            receiver: event_feed_wrap,
        },
        Box::new(receive_event),
    )
}

pub struct TestSuiteBuilder {
    cluster: Option<Cluster<ServerCluster>>,
    memory_quota: Option<usize>,
}

impl TestSuiteBuilder {
    pub fn new() -> TestSuiteBuilder {
        TestSuiteBuilder {
            cluster: None,
            memory_quota: None,
        }
    }

    #[must_use]
    pub fn cluster(mut self, cluster: Cluster<ServerCluster>) -> TestSuiteBuilder {
        self.cluster = Some(cluster);
        self
    }

    #[must_use]
    pub fn memory_quota(mut self, memory_quota: usize) -> TestSuiteBuilder {
        self.memory_quota = Some(memory_quota);
        self
    }

    pub fn build(self) -> TestSuite {
        self.build_with_cluster_runner(|cluster| cluster.run())
    }

    pub fn build_with_cluster_runner<F>(self, mut runner: F) -> TestSuite
    where
        F: FnMut(&mut Cluster<ServerCluster>),
    {
        init();
        let memory_quota = self.memory_quota.unwrap_or(usize::MAX);
        let mut cluster = self.cluster.unwrap();
        let count = cluster.count;
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
            sim.pending_services
                .entry(id)
                .or_default()
                .push(Box::new(move || {
                    create_change_data(cdc::Service::new(
                        scheduler.clone(),
                        MemoryQuota::new(memory_quota),
                    ))
                }));
            sim.txn_extra_schedulers.insert(
                id,
                Arc::new(cdc::CdcTxnExtraScheduler::new(worker.scheduler().clone())),
            );
            let scheduler = worker.scheduler();
            let cdc_ob = cdc::CdcObserver::new(scheduler.clone());
            obs.insert(id, cdc_ob.clone());
            sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
                move |host: &mut CoprocessorHost<RocksEngine>| {
                    cdc_ob.register_to(host);
                },
            ));
            endpoints.insert(id, worker);
        }

        runner(&mut cluster);
        for (id, worker) in &mut endpoints {
            let sim = cluster.sim.wl();
            let raft_router = sim.get_server_router(*id);
            let cdc_ob = obs.get(id).unwrap().clone();
            let cm = sim.get_concurrency_manager(*id);
            let env = Arc::new(Environment::new(1));
            let cfg = CdcConfig::default();
            let mut cdc_endpoint = cdc::Endpoint::new(
                DEFAULT_CLUSTER_ID,
                &cfg,
                cluster.cfg.storage.api_version(),
                pd_cli.clone(),
                worker.scheduler(),
                raft_router,
                cluster.engines[id].kv.clone(),
                cdc_ob,
                cluster.store_metas[id].clone(),
                cm.clone(),
                env,
                sim.security_mgr.clone(),
                MemoryQuota::new(usize::MAX),
            );
            let mut updated_cfg = cfg.clone();
            updated_cfg.min_ts_interval = ReadableDuration::millis(100);
            cdc_endpoint.run(Task::ChangeConfig(cfg.diff(&updated_cfg)));
            cdc_endpoint.set_max_scan_batch_size(2);
            concurrency_managers.insert(*id, cm);
            worker.start(cdc_endpoint);
        }

        TestSuite {
            cluster,
            endpoints,
            obs,
            concurrency_managers,
            env: Arc::new(Environment::new(1)),
            tikv_cli: HashMap::default(),
            cdc_cli: HashMap::default(),
        }
    }
}

pub struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub endpoints: HashMap<u64, LazyWorker<Task>>,
    pub obs: HashMap<u64, CdcObserver>,
    tikv_cli: HashMap<u64, TikvClient>,
    cdc_cli: HashMap<u64, ChangeDataClient>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,

    env: Arc<Environment>,
}

impl Default for TestSuiteBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestSuite {
    pub fn new(count: usize, api_version: ApiVersion) -> TestSuite {
        let mut cluster = new_server_cluster_with_api_ver(1, count, api_version);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);
        // Disable background renew to make timestamp predictable.
        configure_for_causal_ts(&mut cluster, "0s", 1);

        let builder = TestSuiteBuilder::new();
        builder.cluster(cluster).build()
    }

    pub fn stop(mut self) {
        for (_, worker) in self.endpoints.drain() {
            worker.stop_worker();
        }
        self.cluster.shutdown();
    }

    pub fn new_changedata_request(&mut self, region_id: u64) -> ChangeDataRequest {
        let mut req = ChangeDataRequest {
            region_id,
            ..Default::default()
        };
        req.set_region_epoch(self.get_context(region_id).take_region_epoch());
        // Enable batch resolved ts feature.
        req.mut_header()
            .set_ticdc_version(FeatureGate::batch_resolved_ts().to_string());
        req
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

    pub fn must_kv_put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) {
        let mut rawkv_req = RawPutRequest::default();
        rawkv_req.set_context(self.get_context(region_id));
        rawkv_req.set_key(key);
        rawkv_req.set_value(value);
        rawkv_req.set_ttl(u64::MAX);

        let rawkv_resp = self.get_tikv_client(region_id).raw_put(&rawkv_req).unwrap();
        assert!(
            !rawkv_resp.has_region_error(),
            "{:?}",
            rawkv_resp.get_region_error()
        );
        assert!(rawkv_resp.error.is_empty(), "{:?}", rawkv_resp.get_error());
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
        let api_version = self.cluster.cfg.storage.api_version();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context.set_api_version(api_version);
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

    pub fn get_region_cdc_client(&mut self, region_id: u64) -> &ChangeDataClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let store_id = leader.get_store_id();
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.cdc_cli.entry(store_id).or_insert_with(|| {
            let channel = ChannelBuilder::new(env)
                .max_receive_message_len(i32::MAX)
                .connect(&addr);
            ChangeDataClient::new(channel)
        })
    }

    pub fn get_store_cdc_client(&mut self, store_id: u64) -> &ChangeDataClient {
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.cdc_cli.entry(store_id).or_insert_with(|| {
            let channel = ChannelBuilder::new(env).connect(&addr);
            ChangeDataClient::new(channel)
        })
    }

    pub fn get_txn_concurrency_manager(&self, store_id: u64) -> Option<ConcurrencyManager> {
        self.concurrency_managers.get(&store_id).cloned()
    }

    pub fn set_tso(&self, ts: impl Into<TimeStamp>) {
        self.cluster.pd_client.set_tso(ts.into());
    }

    pub fn flush_causal_timestamp_for_region(&mut self, region_id: u64) {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        self.cluster
            .sim
            .rl()
            .get_causal_ts_provider(leader.get_store_id())
            .unwrap()
            .flush()
            .unwrap();
    }
}
