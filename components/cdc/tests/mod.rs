// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::*;
use std::time::Duration;

use futures::{Future, Stream};
use grpcio::{ChannelBuilder, Environment};
use grpcio::{ClientDuplexReceiver, ClientDuplexSender, ClientUnaryReceiver};
use kvproto::cdcpb::{create_change_data, ChangeDataClient, ChangeDataEvent, ChangeDataRequest};
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use raftstore::coprocessor::CoprocessorHost;
use security::*;
use test_raftstore::*;
use tikv::config::CdcConfig;
use tikv_util::collections::HashMap;
use tikv_util::worker::Worker;
use tikv_util::HandyRwLock;
use txn_types::TimeStamp;

use cdc::{CdcObserver, MemoryQuota, Task};

#[allow(clippy::type_complexity)]
pub fn new_event_feed(
    client: &ChangeDataClient,
) -> (
    ClientDuplexSender<ChangeDataRequest>,
    Rc<Cell<Option<ClientDuplexReceiver<ChangeDataEvent>>>>,
    impl Fn(bool) -> ChangeDataEvent,
) {
    let (req_tx, resp_rx) = client.event_feed().unwrap();
    let event_feed_wrap = Rc::new(Cell::new(Some(resp_rx)));
    let event_feed_wrap_clone = event_feed_wrap.clone();

    let receive_event = move |keep_resolved_ts: bool| loop {
        let event_feed = event_feed_wrap_clone.as_ref();
        let (change_data, events) = match event_feed.replace(None).unwrap().into_future().wait() {
            Ok(res) => res,
            Err(e) => panic!("receive failed {:?}", e.0),
        };
        event_feed.set(Some(events));
        let change_data_event = change_data.unwrap();
        if !keep_resolved_ts && change_data_event.has_resolved_ts() {
            continue;
        }
        tikv_util::info!("cdc receive event {:?}", change_data_event);
        break change_data_event;
    };
    (req_tx, event_feed_wrap, receive_event)
}

pub struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub endpoints: HashMap<u64, Worker<Task>>,
    pub obs: HashMap<u64, CdcObserver>,
    tikv_cli: HashMap<u64, TikvClient>,
    cdc_cli: HashMap<u64, ChangeDataClient>,

    env: Arc<Environment>,
}

impl TestSuite {
    pub fn new(count: usize) -> TestSuite {
        let mut cluster = new_server_cluster(1, count);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);
        Self::with_cluster(count, cluster)
    }

    pub fn with_cluster(count: usize, mut cluster: Cluster<ServerCluster>) -> TestSuite {
        let pd_cli = cluster.pd_client.clone();
        let mut endpoints = HashMap::default();
        let mut obs = HashMap::default();
        // Hack! node id are generated from 1..count+1.
        for id in 1..=count as u64 {
            // Create and run cdc endpoints.
            let worker = Worker::new(format!("cdc-{}", id));
            let mut sim = cluster.sim.wl();

            // Register cdc service to gRPC server.
            let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
            let scheduler = worker.scheduler();
            sim.pending_services
                .entry(id)
                .or_default()
                .push(Box::new(move || {
                    let memory_quota = MemoryQuota::new(std::usize::MAX);
                    create_change_data(cdc::Service::new(
                        scheduler.clone(),
                        security_mgr.clone(),
                        memory_quota,
                    ))
                }));
            let scheduler = worker.scheduler();
            let cdc_ob = cdc::CdcObserver::new(scheduler.clone());
            obs.insert(id, cdc_ob.clone());
            sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
                move |host: &mut CoprocessorHost| {
                    cdc_ob.register_to(host);
                },
            ));
            endpoints.insert(id, worker);
        }

        cluster.run();
        for (id, worker) in &mut endpoints {
            let sim = cluster.sim.rl();
            let raft_router = sim.get_server_router(*id);
            let cdc_ob = obs.get(&id).unwrap().clone();
            let mut cdc_endpoint = cdc::Endpoint::new(
                &CdcConfig::default(),
                pd_cli.clone(),
                worker.scheduler(),
                raft_router,
                cdc_ob,
                cluster.store_metas[id].clone(),
                MemoryQuota::new(std::usize::MAX),
            );
            cdc_endpoint.set_min_ts_interval(Duration::from_millis(100));
            cdc_endpoint.set_max_scan_batch_size(2);
            worker.start(cdc_endpoint).unwrap();
        }

        TestSuite {
            cluster,
            endpoints,
            obs,
            env: Arc::new(Environment::new(1)),
            tikv_cli: HashMap::default(),
            cdc_cli: HashMap::default(),
        }
    }

    pub fn stop(mut self) {
        for (_, mut worker) in self.endpoints {
            worker.stop().unwrap().join().unwrap();
        }
        self.cluster.shutdown();
    }

    pub fn new_changedata_request(&mut self, region_id: u64) -> ChangeDataRequest {
        let mut req = ChangeDataRequest::default();
        req.region_id = region_id;
        req.set_region_epoch(self.get_context(region_id).take_region_epoch());
        // Batch resolved ts is supported by TiCDC in v4.0.8 release.
        req.mut_header().set_ticdc_version("4.0.8".into());
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
        let addr = self.cluster.sim.rl().get_addr(store_id).to_owned();
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
        let addr = self.cluster.sim.rl().get_addr(store_id).to_owned();
        let env = self.env.clone();
        self.cdc_cli.entry(store_id).or_insert_with(|| {
            let channel = ChannelBuilder::new(env)
                .max_receive_message_len(std::i32::MAX)
                .connect(&addr);
            ChangeDataClient::new(channel)
        })
    }

    pub fn get_store_cdc_client(&mut self, store_id: u64) -> &ChangeDataClient {
        let addr = self.cluster.sim.rl().get_addr(store_id).to_owned();
        let env = self.env.clone();
        self.cdc_cli.entry(store_id).or_insert_with(|| {
            let channel = ChannelBuilder::new(env).connect(&addr);
            ChangeDataClient::new(channel)
        })
    }
}
