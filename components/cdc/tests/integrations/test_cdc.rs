// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use std::time::Duration;

use crate::{new_event_feed, TestSuite};
use futures::sink::Sink;
use futures::Future;
use grpcio::WriteFlags;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::event::{
    row::OpType as EventRowOpType, Event as Event_oneof_event, LogType as EventLogType,
};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::sleep_ms;
use test_raftstore::*;

use cdc::{Task, Validate};

#[test]
fn test_cdc_basic() {
    let mut suite = TestSuite::new(1);

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let event = receive_event(false);
    event.events.into_iter().for_each(|e| {
        match e.event.unwrap() {
            // Even if there is no write,
            // it should always outputs an Initialized event.
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            other => panic!("unknown event {:?}", other),
        }
    });

    // Sleep a while to make sure the stream is registered.
    sleep_ms(1000);
    // There must be a delegate.
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        )))
        .unwrap();

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(1, vec![mutation], k.clone().into_bytes(), start_ts);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        other => panic!("unknown event {:?}", other),
    }

    let mut counter = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert_ne!(0, resolved_ts.ts);
            counter += 1;
        }
        if counter > 5 {
            break;
        }
    }
    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k.into_bytes()], start_ts, commit_ts);
    let mut event = receive_event(false);
    let mut events = event.take_events();
    assert_eq!(events.len(), 1, "{:?}", event);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Commit);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Split region 1
    let region1 = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region1, b"key2");
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    // The delegate must be removed.
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(|delegate| {
                assert!(delegate.is_none());
            }),
        )))
        .unwrap();

    // request again.
    let req = suite.new_changedata_request(1);
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Sleep a while to make sure the stream is registered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        )))
        .unwrap();

    // Drop stream and cancel its server streaming.
    event_feed_wrap.as_ref().replace(None);
    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(|delegate| {
                assert!(delegate.is_none());
            }),
        )))
        .unwrap();

    // Stale region epoch.
    let mut req = suite.new_changedata_request(1);
    req.set_region_epoch(Default::default()); // Zero region epoch.
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    suite.stop();
}

#[test]
fn test_cdc_not_leader() {
    let mut suite = TestSuite::new(3);

    let leader = suite.cluster.leader_of_region(1).unwrap();
    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Sleep a while to make sure the stream is registered.
    sleep_ms(1000);
    // There must be a delegate.
    let scheduler = suite
        .endpoints
        .get(&leader.get_store_id())
        .unwrap()
        .scheduler();
    let (tx, rx) = mpsc::channel();
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(move |delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
                tx_.send(()).unwrap();
            }),
        )))
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1)
        .is_some());

    // Transfer leader.
    let peer = suite
        .cluster
        .get_region(&[])
        .take_peers()
        .into_iter()
        .find(|p| *p != leader)
        .unwrap();
    suite.cluster.must_transfer_leader(1, peer);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1)
        .is_some());

    // Sleep a while to make sure the stream is deregistered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(move |delegate| {
                assert!(delegate.is_none());
                tx.send(()).unwrap();
            }),
        )))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();

    // Try to subscribe again.
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    // Should failed with not leader error.
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    assert!(!suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .is_subscribed(1)
        .is_some());

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_stale_epoch_after_region_ready() {
    let mut suite = TestSuite::new(3);

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    let mut req = suite.new_changedata_request(1);
    req.set_region_epoch(Default::default()); // zero epoch is always stale.
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    let _resp_rx = event_feed_wrap.as_ref().replace(Some(resp_rx));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Must receive epoch not match error.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Must receive epoch not match error.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
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
    let start_ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k.clone()], start_ts1, commit_ts1);

    // Prewrite again
    let start_ts2 = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts2);

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    if events.len() == 1 {
        events.extend(receive_event(false).events.into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        // Batch size is set to 2.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            assert_eq!(e.start_ts, start_ts2.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, 0, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.start_ts, start_ts1.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, commit_ts1.into_inner(), "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    // checkpoint_ts = 5;
    let checkpoint_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    // Commit = 6;
    let commit_ts2 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k.clone()], start_ts2, commit_ts2);
    // Prewrite delete
    let start_ts3 = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Del);
    mutation.key = k.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts3);

    let mut req = suite.new_changedata_request(1);
    req.checkpoint_ts = checkpoint_ts.into_inner();
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    if events.len() == 1 {
        events.extend(receive_event(false).events.to_vec());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        // Batch size is set to 2.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            assert_eq!(e.get_op_type(), EventRowOpType::Delete, "{:?}", es);
            assert_eq!(e.start_ts, start_ts3.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, 0, "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert!(e.value.is_empty(), "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.get_op_type(), EventRowOpType::Put, "{:?}", es);
            assert_eq!(e.start_ts, start_ts2.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, commit_ts2.into_inner(), "{:?}", es);
            assert_eq!(e.key, k, "{:?}", es);
            assert_eq!(e.value, v, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_tso_failure() {
    let mut suite = TestSuite::new(3);

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    suite.cluster.pd_client.trigger_tso_failure();

    // Make sure resolved ts can be advanced normally even with few tso failures.
    let mut counter = 0;
    let mut previous_ts = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert!(resolved_ts.ts >= previous_ts);
            assert_eq!(resolved_ts.regions, vec![1]);
            previous_ts = resolved_ts.ts;
            counter += 1;
        }
        if counter > 5 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_region_split() {
    let mut cluster = new_server_cluster(1, 1);
    configure_for_lease_read(&mut cluster, Some(100), None);
    cluster.pd_client.disable_default_operator();
    let mut suite = TestSuite::with_cluster(1, cluster);

    let region = suite.cluster.get_region(&[]);
    let mut req = suite.new_changedata_request(region.get_id());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Split region.
    suite.cluster.must_split(&region, b"k0");
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Try to subscribe region again.
    let region = suite.cluster.get_region(b"k0");
    // Ensure it is the previous region.
    assert_eq!(req.get_region_id(), region.get_id());
    req.set_region_epoch(region.get_region_epoch().clone());
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Subscribe tne new region.
    let region1 = suite.cluster.get_region(&[]);
    req.region_id = region1.get_id();
    req.set_region_epoch(region1.get_region_epoch().clone());
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Make sure resolved ts can be advanced normally.
    let mut counter = 0;
    let mut previous_ts = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert!(resolved_ts.ts >= previous_ts);
            assert!(
                resolved_ts.regions == vec![region.id, region1.id]
                    || resolved_ts.regions == vec![region1.id, region.id]
            );
            previous_ts = resolved_ts.ts;
            counter += 1;
        }
        if counter > 5 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_duplicate_subscribe() {
    let mut suite = TestSuite::new(1);

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Try to subscribe again.
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    // Should receive duplicate request error.
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_duplicate_request(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_batch_size_limit() {
    let mut suite = TestSuite::new(1);

    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Put);
    m1.key = k1.clone();
    m1.value = vec![0; 6 * 1024 * 1024];
    let mut m2 = Mutation::default();
    let k2 = b"k2".to_vec();
    m2.set_op(Op::Put);
    m2.key = k2.clone();
    m2.value = b"v2".to_vec();
    suite.must_kv_prewrite(1, vec![m1, m2], k1.clone(), start_ts);
    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1, k2], start_ts, commit_ts);

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events.len());
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", e.get_type());
            assert_eq!(e.key, b"k1", "{:?}", e.key);
        }
        other => panic!("unknown event {:?}", other),
    }
    // For the rest 2 events, Committed and Initialized.
    let mut entries = vec![];
    while entries.len() < 2 {
        match receive_event(false).events.remove(0).event.unwrap() {
            Event_oneof_event::Entries(es) => {
                entries.extend(es.entries.into_iter());
            }
            other => panic!("unknown event {:?}", other),
        }
    }
    assert_eq!(entries.len(), 2, "{:?}", entries);
    let e = &entries[0];
    assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", e.get_type());
    assert_eq!(e.key, b"k2", "{:?}", e.key);
    let e = &entries[1];
    assert_eq!(
        e.get_type(),
        EventLogType::Initialized,
        "{:?}",
        e.get_type()
    );

    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut m3 = Mutation::default();
    let k3 = b"k3".to_vec();
    m3.set_op(Op::Put);
    m3.key = k3.clone();
    m3.value = vec![0; 7 * 1024 * 1024];
    let mut m4 = Mutation::default();
    let k4 = b"k4".to_vec();
    m4.set_op(Op::Put);
    m4.key = k4;
    m4.value = b"v4".to_vec();
    suite.must_kv_prewrite(1, vec![m3, m4], k3, start_ts);

    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", e.get_type());
            assert_eq!(e.key, b"k4", "{:?}", e.key);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", e.get_type());
            assert_eq!(e.key, b"k3", "{:?}", e.key);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_old_value_basic() {
    let mut suite = TestSuite::new(1);
    let mut req = suite.new_changedata_request(1);
    req.set_extra_op(ExtraOp::ReadOldValue);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    sleep_ms(1000);

    // Insert value
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Put);
    m1.key = k1.clone();
    m1.value = b"v1".to_vec();
    let ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m1], k1.clone(), ts1);
    let ts2 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], ts1, ts2);
    // Rollback
    let mut m2 = Mutation::default();
    m2.set_op(Op::Put);
    m2.key = k1.clone();
    m2.value = b"v2".to_vec();
    let ts3 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m2], k1.clone(), ts3);
    suite.must_kv_rollback(1, vec![k1.clone()], ts3);
    // Update value
    let mut m3 = Mutation::default();
    m3.set_op(Op::Put);
    m3.key = k1.clone();
    m3.value = vec![b'3'; 5120];
    let ts4 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m3], k1.clone(), ts4);
    let ts5 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], ts4, ts5);
    // Lock
    let mut m4 = Mutation::default();
    m4.set_op(Op::Lock);
    m4.key = k1.clone();
    let ts6 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m4], k1.clone(), ts6);
    let ts7 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], ts6, ts7);
    // Delete value and rollback
    let mut m5 = Mutation::default();
    m5.set_op(Op::Del);
    m5.key = k1.clone();
    let ts8 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m5], k1.clone(), ts8);
    suite.must_kv_rollback(1, vec![k1.clone()], ts8);
    // Update value
    let mut m6 = Mutation::default();
    m6.set_op(Op::Put);
    m6.key = k1.clone();
    m6.value = b"v6".to_vec();
    let ts9 = suite.cluster.pd_client.get_tso().wait().unwrap();
    let ts10 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m6], k1.clone(), ts10);
    let ts11 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], ts10, ts11);
    // Delete value in pessimistic txn.
    // In pessimistic txn, CDC must use for_update_ts to read the old value.
    let mut m7 = Mutation::default();
    m7.set_op(Op::PessimisticLock);
    m7.key = k1.clone();
    let ts12 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_acquire_pessimistic_lock(1, vec![m7.clone()], k1.clone(), ts9, ts12);
    m7.set_op(Op::Del);
    suite.must_kv_pessimistic_prewrite(1, vec![m7], k1.clone(), ts9, ts12);
    let ts13 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], ts9, ts13);
    // Insert value again
    let mut m8 = Mutation::default();
    m8.set_op(Op::Insert);
    m8.key = k1.clone();
    m8.value = b"v1".to_vec();
    let ts14 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m8], k1, ts14);

    let mut event_count = 0;
    loop {
        let events = receive_event(false).events.to_vec();
        for event in events.into_iter() {
            match event.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for row in es.take_entries().to_vec() {
                        if row.get_type() == EventLogType::Prewrite {
                            if row.get_start_ts() == ts3.into_inner()
                                || row.get_start_ts() == ts4.into_inner()
                            {
                                assert_eq!(row.get_old_value(), b"v1");
                                event_count += 1;
                            } else if row.get_start_ts() == ts8.into_inner() {
                                assert_eq!(
                                    row.get_old_value(),
                                    vec![b'3'; 5120].as_slice(),
                                    "{:?}",
                                    row
                                );
                                event_count += 1;
                            } else if row.get_start_ts() == ts9.into_inner() {
                                assert_eq!(row.get_old_value(), b"v6");
                                event_count += 1;
                            }
                        }
                    }
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if event_count >= 4 {
            break;
        }
    }

    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut event_count = 0;
    loop {
        let event = receive_event(false);
        for e in event.events.into_iter() {
            match e.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for row in es.take_entries().to_vec() {
                        if row.get_type() == EventLogType::Committed
                            && row.get_start_ts() == ts1.into_inner()
                        {
                            assert_eq!(row.get_old_value(), b"");
                            event_count += 1;
                        } else if row.get_type() == EventLogType::Committed
                            && row.get_start_ts() == ts4.into_inner()
                        {
                            assert_eq!(row.get_old_value(), b"v1");
                            event_count += 1;
                        } else if row.get_type() == EventLogType::Prewrite
                            && row.get_start_ts() == ts14.into_inner()
                        {
                            assert_eq!(row.get_old_value(), b"");
                            event_count += 1;
                        }
                    }
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if event_count >= 3 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_old_value_multi_changefeeds() {
    let mut suite = TestSuite::new(1);
    let mut req = suite.new_changedata_request(1);
    req.set_extra_op(ExtraOp::ReadOldValue);
    let (req_tx_1, event_feed_wrap_1, receive_event_1) =
        new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx_1 = req_tx_1
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();

    req.set_extra_op(ExtraOp::Noop);
    let (req_tx_2, event_feed_wrap_2, receive_event_2) =
        new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx_2 = req_tx_2.send((req, WriteFlags::default())).wait().unwrap();

    sleep_ms(1000);
    // Insert value
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Put);
    m1.key = k1.clone();
    m1.value = b"v1".to_vec();
    let ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m1], k1.clone(), ts1);
    let ts2 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], ts1, ts2);
    // Update value
    let mut m2 = Mutation::default();
    m2.set_op(Op::Put);
    m2.key = k1.clone();
    m2.value = vec![b'3'; 5120];
    let ts3 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m2], k1.clone(), ts3);
    let ts4 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1], ts3, ts4);
    let mut event_count = 0;
    loop {
        let events = receive_event_1(false).events.to_vec();
        for event in events.into_iter() {
            match event.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for row in es.take_entries().to_vec() {
                        if row.get_type() == EventLogType::Prewrite {
                            if row.get_start_ts() == ts3.into_inner() {
                                assert_eq!(row.get_old_value(), b"v1");
                                event_count += 1;
                            } else {
                                assert_eq!(row.get_old_value(), b"");
                                event_count += 1;
                            }
                        }
                    }
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if event_count >= 2 {
            break;
        }
    }

    event_count = 0;
    loop {
        let events = receive_event_2(false).events.to_vec();
        for event in events.into_iter() {
            match event.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for row in es.take_entries().to_vec() {
                        if row.get_type() == EventLogType::Prewrite {
                            assert_eq!(row.get_old_value(), b"");
                            event_count += 1;
                        }
                    }
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if event_count >= 2 {
            break;
        }
    }

    event_feed_wrap_1.replace(None);
    event_feed_wrap_2.replace(None);
    suite.stop();
}

#[test]
fn test_old_value_cache() {
    let mut suite = TestSuite::new(1);
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    let mut req = suite.new_changedata_request(1);
    req.set_extra_op(ExtraOp::ReadOldValue);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_type(), EventLogType::Initialized);
        }
        other => panic!("unknown event {:?}", other),
    }
    let (tx, rx) = mpsc::channel();

    // Insert value, simulate INSERT INTO.
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Insert);
    m1.key = k1.clone();
    m1.value = b"v1".to_vec();
    suite.must_kv_prewrite(1, vec![m1], k1.clone(), 10.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_value(), b"v1");
            assert_eq!(row.get_old_value(), b"");
            assert_eq!(row.get_type(), EventLogType::Prewrite);
            assert_eq!(row.get_start_ts(), 10);
        }
        other => panic!("unknown event {:?}", other),
    }
    // k1 old value must be cached.
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(Validate::OldValueCache(Box::new(
            move |old_value_stats| {
                tx_.send((old_value_stats.access_count, old_value_stats.miss_count))
                    .unwrap();
            },
        ))))
        .unwrap();
    let (access_count, miss_count) = rx.recv().unwrap();
    assert_eq!(access_count, 1);
    assert_eq!(miss_count, 0);
    suite.must_kv_commit(1, vec![k1], 10.into(), 15.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_type(), EventLogType::Commit);
            assert_eq!(row.get_commit_ts(), 15);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Update a noexist value, simulate INSERT IGNORE INTO.
    let mut m2 = Mutation::default();
    let k2 = b"k2".to_vec();
    m2.set_op(Op::Put);
    m2.key = k2.clone();
    m2.value = b"v2".to_vec();
    suite.must_kv_prewrite(1, vec![m2], k2.clone(), 10.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_value(), b"v2");
            assert_eq!(row.get_old_value(), b"");
            assert_eq!(row.get_type(), EventLogType::Prewrite);
            assert_eq!(row.get_start_ts(), 10);
        }
        other => panic!("unknown event {:?}", other),
    }
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(Validate::OldValueCache(Box::new(
            move |old_value_stats| {
                tx_.send((old_value_stats.access_count, old_value_stats.miss_count))
                    .unwrap();
            },
        ))))
        .unwrap();
    let (access_count, miss_count) = rx.recv().unwrap();
    assert_eq!(access_count, 2);
    assert_eq!(miss_count, 0);
    suite.must_kv_commit(1, vec![k2], 10.into(), 15.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_type(), EventLogType::Commit);
            assert_eq!(row.get_commit_ts(), 15);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Update an exist value, simulate UPDATE.
    let mut m2 = Mutation::default();
    let k2 = b"k2".to_vec();
    m2.set_op(Op::Put);
    m2.key = k2.clone();
    m2.value = b"v3".to_vec();
    suite.must_kv_prewrite(1, vec![m2], k2.clone(), 20.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_value(), b"v3");
            assert_eq!(row.get_old_value(), b"v2");
            assert_eq!(row.get_type(), EventLogType::Prewrite);
            assert_eq!(row.get_start_ts(), 20);
        }
        other => panic!("unknown event {:?}", other),
    }
    // k2 old value must be cached.
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(Validate::OldValueCache(Box::new(
            move |old_value_stats| {
                tx_.send((old_value_stats.access_count, old_value_stats.miss_count))
                    .unwrap();
            },
        ))))
        .unwrap();
    let (access_count, miss_count) = rx.recv().unwrap();
    assert_eq!(access_count, 3);
    assert_eq!(miss_count, 0);
    suite.must_kv_commit(1, vec![k2], 20.into(), 25.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_type(), EventLogType::Commit);
            assert_eq!(row.get_commit_ts(), 25);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Update an exist value with pessimistic lock, simulate UPDATE.
    let mut m3 = Mutation::default();
    let k3 = b"k3".to_vec();
    m3.set_op(Op::PessimisticLock);
    m3.key = k3.clone();
    m3.value = b"v4".to_vec();
    suite.must_acquire_pessimistic_lock(1, vec![m3.clone()], k3.clone(), 30.into(), 32.into());
    m3.set_op(Op::Insert);
    suite.must_kv_pessimistic_prewrite(1, vec![m3], k3, 30.into(), 32.into());
    let mut events = receive_event(false).events.to_vec();
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(mut es) => {
            let row = &es.take_entries().to_vec()[0];
            assert_eq!(row.get_value(), b"v4");
            assert_eq!(row.get_old_value(), b"");
            assert_eq!(row.get_type(), EventLogType::Prewrite);
            assert_eq!(row.get_start_ts(), 30);
        }
        other => panic!("unknown event {:?}", other),
    }
    // k3 mutation type must be cached.
    let tx_ = tx.clone();
    scheduler
        .schedule(Task::Validate(Validate::OldValueCache(Box::new(
            move |old_value_stats| {
                tx_.send((old_value_stats.access_count, old_value_stats.miss_count))
                    .unwrap();
            },
        ))))
        .unwrap();
    let (access_count, miss_count) = rx.recv().unwrap();
    assert_eq!(access_count, 4);
    assert_eq!(miss_count, 0);

    event_feed_wrap.replace(None);
    suite.stop();
}
