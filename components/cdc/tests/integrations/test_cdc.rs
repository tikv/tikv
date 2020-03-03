// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::*;
use std::time::Duration;

use futures::{Future, Stream};
use grpcio::{ChannelBuilder, ClientSStreamReceiver, Environment};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    create_change_data,
    event::{row::OpType as EventRowOpType, Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataClient, ChangeDataEvent, ChangeDataRequest,
};
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use raftstore::coprocessor::CoprocessorHost;
use test_raftstore::*;
use tikv_util::collections::HashMap;
use tikv_util::worker::Worker;
use tikv_util::HandyRwLock;
use txn_types::TimeStamp;

use cdc::{CdcObserver, Task};

struct TestSuite {
    cluster: Cluster<ServerCluster>,
    endpoints: HashMap<u64, Worker<Task>>,
    obs: HashMap<u64, CdcObserver>,
    tikv_cli: TikvClient,
    cdc_cli: ChangeDataClient,

    _env: Arc<Environment>,
}

impl TestSuite {
    fn new(count: usize) -> TestSuite {
        super::init();
        let mut cluster = new_server_cluster(1, count);

        let pd_cli = cluster.pd_client.clone();
        let mut endpoints = HashMap::default();
        let mut obs = HashMap::default();
        // Hack! node id are generated from 1..count+1.
        for id in 1..=count as u64 {
            // Create and run cdc endpoints.
            let worker = Worker::new(format!("cdc-{}", id));
            let mut sim = cluster.sim.wl();

            // Register cdc service to gRPC server.
            let scheduler = worker.scheduler();
            sim.pending_services
                .entry(id)
                .or_default()
                .push(Box::new(move || {
                    create_change_data(cdc::Service::new(scheduler.clone()))
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
            let raft_router = (*sim).get_router(*id).unwrap();
            let cdc_ob = obs.get(&id).unwrap().clone();
            let mut cdc_endpoint =
                cdc::Endpoint::new(pd_cli.clone(), worker.scheduler(), raft_router, cdc_ob);
            cdc_endpoint.set_min_ts_interval(Duration::from_millis(100));
            cdc_endpoint.set_scan_batch_size(2);
            worker.start(cdc_endpoint).unwrap();
        }

        let region = cluster.get_region(&[]);
        let leader = cluster.leader_of_region(region.get_id()).unwrap();
        let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();
        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env.clone()).connect(&leader_addr);
        let tikv_cli = TikvClient::new(channel.clone());
        let cdc_cli = ChangeDataClient::new(channel);

        TestSuite {
            cluster,
            endpoints,
            obs,
            tikv_cli,
            cdc_cli,
            _env: env,
        }
    }

    fn stop(mut self) {
        for (_, mut worker) in self.endpoints {
            worker.stop().unwrap().join().unwrap();
        }
        self.cluster.shutdown();
    }

    fn must_kv_prewrite(&mut self, muts: Vec<Mutation>, pk: Vec<u8>, ts: TimeStamp) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(1));
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = self.tikv_cli.kv_prewrite(&prewrite_req).unwrap();
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

    fn must_kv_commit(&mut self, keys: Vec<Vec<u8>>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(1));
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let commit_resp = self.tikv_cli.kv_commit(&commit_req).unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    fn get_context(&mut self, region_id: u64) -> Context {
        let epoch = self.cluster.get_region_epoch(region_id);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context
    }
}

fn new_event_feed(
    client: &ChangeDataClient,
    req: &ChangeDataRequest,
) -> (
    Rc<Cell<Option<ClientSStreamReceiver<ChangeDataEvent>>>>,
    impl Fn(bool) -> Event_oneof_event,
) {
    let event_feed = client.event_feed(&req).unwrap();
    let event_feed_wrap = Rc::new(Cell::new(Some(event_feed)));
    let event_feed_wrap_clone = event_feed_wrap.clone();

    let receive_event = move |keep_resolved_ts: bool| loop {
        let event_feed = event_feed_wrap_clone.as_ref();
        let (change_data, events) = match event_feed.replace(None).unwrap().into_future().wait() {
            Ok(res) => res,
            Err(e) => panic!("receive failed {:?}", e.0),
        };
        event_feed.set(Some(events));
        let mut change_data = change_data.unwrap();
        assert_eq!(change_data.events.len(), 1);
        let change_data_event = &mut change_data.events[0];
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::ResolvedTs(_) if !keep_resolved_ts => continue,
            other => return other,
        }
    };
    (event_feed_wrap, receive_event)
}

#[test]
fn test_cdc_basic() {
    let mut suite = TestSuite::new(1);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);
    for _ in 0..2 {
        let event = receive_event(true);
        match event {
            // Even if there is no write,
            // resolved ts should be advanced regularly.
            Event_oneof_event::ResolvedTs(ts) => assert_ne!(0, ts),
            // Even if there is no write,
            // it should always outputs an Initialized event.
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            _ => panic!("unknown event"),
        }
    }

    // There must be a delegate.
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), start_ts);
    let event = receive_event(false);
    match event {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        _ => panic!("unknown event"),
    }

    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut counter = 0;
    loop {
        let event = receive_event(true);
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        if let Event_oneof_event::ResolvedTs(_) = event {
            counter += 1;
            if counter > 5 {
                break;
            }
        }
    }
    suite.must_kv_commit(vec![k.into_bytes()], start_ts, commit_ts);
    let event = receive_event(false);
    match event {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Commit);
        }
        _ => panic!("unknown event"),
    }

    // Split region 1
    let region1 = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region1, b"key2");
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    // The delegate must be removed.
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                assert!(delegate.is_none());
            }),
        ))
        .unwrap();

    // The second stream.
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let event_feed2 = suite.cdc_cli.event_feed(&req).unwrap();
    event_feed_wrap.as_ref().replace(Some(event_feed2));
    let event = receive_event(false);

    match event {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        _ => panic!("unknown event"),
    }
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    // Drop event_feed2 and cancel its server streaming.
    event_feed_wrap.as_ref().replace(None);
    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                assert!(delegate.is_none());
            }),
        ))
        .unwrap();

    // Stale region epoch.
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(Default::default()); // Zero region epoch.
    let event_feed3 = suite.cdc_cli.event_feed(&req).unwrap();
    event_feed_wrap.as_ref().replace(Some(event_feed3));
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Entries(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
    }

    suite.stop();
}

#[test]
fn test_cdc_not_leader() {
    let mut suite = TestSuite::new(3);

    let leader = suite.cluster.leader_of_region(1).unwrap();
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);

    // Make sure region 1 is registered.
    let event = receive_event(false);
    match event {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }

    // There must be a delegate.
    let scheduler = suite
        .endpoints
        .get(&leader.get_store_id())
        .unwrap()
        .scheduler();
    let (tx, rx) = mpsc::channel();
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(move |delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
                tx_.send(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();
    assert!(suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1));

    // Transfer leader.
    let peer = suite
        .cluster
        .get_region(&[])
        .take_peers()
        .into_iter()
        .find(|p| *p != leader)
        .unwrap();
    suite.cluster.must_transfer_leader(1, peer);
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1));

    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(move |delegate| {
                assert!(delegate.is_none());
                tx.send(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();

    let event_feed2 = suite.cdc_cli.event_feed(&req).unwrap();
    event_feed_wrap.as_ref().replace(Some(event_feed2));
    let event = receive_event(false);
    // Should failed with not leader error.
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1));

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_stale_epoch_after_region_ready() {
    let mut suite = TestSuite::new(3);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);
    // Make sure region 1 is registered.
    let event = receive_event(false);
    match event {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(Default::default()); // zero epoch is always stale.
    let event_feed = suite.cdc_cli.event_feed(&req).unwrap();
    let feed1_holder = event_feed_wrap.as_ref().replace(Some(event_feed));
    // Must receive epoch not match error.
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    // Must not receive any error on event feed 1.
    event_feed_wrap.as_ref().replace(feed1_holder);
    let event = receive_event(true);
    match event {
        Event_oneof_event::ResolvedTs(ts) => assert_ne!(0, ts),
        _ => panic!("unknown event"),
    }

    // Cancel event feed before finishing test.
    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_scan() {
    let mut suite = TestSuite::new(1);

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(vec![mutation], k.clone(), start_ts);
    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(vec![k.clone()], start_ts, commit_ts);

    // Prewrite again
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(vec![mutation], k.clone(), start_ts);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);

    let event = receive_event(false);
    match event {
        // Batch size is set to 2.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            assert_eq!(e.start_ts, 4, "{:?}", es);
            assert_eq!(e.commit_ts, 0, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.start_ts, 2, "{:?}", es);
            assert_eq!(e.commit_ts, 3, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
    }
    let event = receive_event(false);
    match event {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
    }

    // checkpoint_ts = 5;
    let checkpoint_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    // Commit = 6;
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(vec![k.clone()], start_ts, commit_ts);
    // Prewrite delete
    // Start = 7;
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Del);
    mutation.key = k.clone();
    suite.must_kv_prewrite(vec![mutation], k.clone(), start_ts);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.checkpoint_ts = checkpoint_ts.into_inner();
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let event_feed2 = suite.cdc_cli.event_feed(&req).unwrap();
    let event_feed1 = event_feed_wrap.as_ref().replace(Some(event_feed2));

    let event = receive_event(false);
    match event {
        // Batch size is set to 2.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            assert_eq!(e.get_op_type(), EventRowOpType::Delete, "{:?}", es);
            assert_eq!(e.start_ts, 7, "{:?}", es);
            assert_eq!(e.commit_ts, 0, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert!(e.value.is_empty(), "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.get_op_type(), EventRowOpType::Put, "{:?}", es);
            assert_eq!(e.start_ts, 4, "{:?}", es);
            assert_eq!(e.commit_ts, 6, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
    }
    let event = receive_event(false);
    match event {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
    }

    event_feed_wrap.as_ref().replace(None);
    drop(event_feed1);

    suite.stop();
}

#[test]
fn test_cdc_tso_failure() {
    let mut suite = TestSuite::new(3);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);

    // Make sure region 1 is registered.
    let event = receive_event(false);
    match event {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }

    suite.cluster.pd_client.trigger_tso_failure();

    // Make sure resolved ts can be advanced normally even with few tso failures.
    for _ in 0..10 {
        let event = receive_event(true);
        match event {
            Event_oneof_event::ResolvedTs(ts) => assert_ne!(0, ts),
            _ => panic!("unknown event"),
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_region_split() {
    let mut suite = TestSuite::new(3);

    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);

    // Make sure region 1 is registered.
    let event = receive_event(false);
    match event {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    // Split region.
    suite.cluster.must_split(&region, b"k0");
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    // Try to subscribe region again.
    let region = suite.cluster.get_region(b"k0");
    // Ensure it is old region.
    assert_eq!(req.get_region_id(), region.get_id());
    req.set_region_epoch(region.get_region_epoch().clone());
    let event_feed2 = suite.cdc_cli.event_feed(&req).unwrap();
    event_feed_wrap.as_ref().replace(Some(event_feed2));
    let event = receive_event(false);
    match event {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_failed_pending_batch() {
    let _guard = super::setup_fail();
    let mut suite = TestSuite::new(3);

    let incremental_scan_fp = "before_schedule_incremental_scan";
    fail::cfg(incremental_scan_fp, "pause").unwrap();

    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (event_feed_wrap, receive_event) = new_event_feed(&suite.cdc_cli, &req);

    // Split region.
    suite.cluster.must_split(&region, b"k0");
    // Wait for receiving split cmd.
    sleep_ms(200);
    fail::remove(incremental_scan_fp);

    let event = receive_event(false);
    match event {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    let event = receive_event(false);
    match event {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    // Try to subscribe region again.
    let region = suite.cluster.get_region(b"k0");
    // Ensure it is old region.
    assert_eq!(req.get_region_id(), region.get_id());
    req.set_region_epoch(region.get_region_epoch().clone());
    let event_feed2 = suite.cdc_cli.event_feed(&req).unwrap();
    event_feed_wrap.as_ref().replace(Some(event_feed2));
    let event = receive_event(false);
    match event {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
