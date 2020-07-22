// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;
use std::ops::Sub;
use std::sync::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{new_event_feed, TestSuite};
use futures::sink::Sink;
use futures::Future;
use grpcio::WriteFlags;
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    change_data_request::{
        NotifyTxnStatus as ChangeDataRequestNotifyTxnStatus,
        Request as ChangeDataRequest_oneof_Request,
    },
    event::{row::OpType as EventRowOpType, Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataRequest, TxnStatus,
};
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::sleep_ms;
use txn_types::TimeStamp;

use cdc::Task;

fn format_event_enum(e: &Event_oneof_event) -> String {
    match e {
        Event_oneof_event::ResolvedTs(e) => format!("ResolvedTs({})", e),
        Event_oneof_event::Entries(e) => format!("Entries({:?})", e),
        Event_oneof_event::Admin(e) => format!("Admin({:?})", e),
        Event_oneof_event::Error(e) => format!("Error({:?})", e),
        Event_oneof_event::LongTxn(e) => format!("LongTxn({:?})", e),
    }
}

#[test]
fn test_cdc_basic() {
    let mut suite = TestSuite::new(1);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    receive_event(true).into_iter().for_each(|e| {
        match e.event.unwrap() {
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
    });

    // Sleep a while to make sure the stream is registered.
    sleep_ms(200);
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
    suite.must_kv_prewrite(1, vec![mutation], k.clone().into_bytes(), start_ts);
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        _ => panic!("unknown event"),
    }

    let mut counter = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        for e in receive_event(true) {
            if let Event_oneof_event::ResolvedTs(_) = e.event.unwrap() {
                counter += 1;
            }
        }
        if counter > 5 {
            break;
        }
    }
    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k.into_bytes()], start_ts, commit_ts);
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Commit);
        }
        _ => panic!("unknown event"),
    }

    // Split region 1
    let region1 = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region1, b"key2");
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
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

    // request again.
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        _ => panic!("unknown event"),
    }
    // Sleep a while to make sure the stream is registered.
    sleep_ms(200);
    scheduler
        .schedule(Task::Validate(
            1,
            Box::new(|delegate| {
                let d = delegate.unwrap();
                assert_eq!(d.downstreams.len(), 1);
            }),
        ))
        .unwrap();

    // Drop stream and cancel its server streaming.
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
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Entries(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    // Sleep a while to make sure the stream is registered.
    sleep_ms(200);
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
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        _ => panic!("unknown event"),
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
        .schedule(Task::Validate(
            1,
            Box::new(move |delegate| {
                assert!(delegate.is_none());
                tx.send(()).unwrap();
            }),
        ))
        .unwrap();
    rx.recv_timeout(Duration::from_millis(200)).unwrap();

    // Try to subscribe again.
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    // Should failed with not leader error.
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_not_leader(), "{:?}", err);
        }
        _ => panic!("unknown event"),
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

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
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
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    let _resp_rx = event_feed_wrap.as_ref().replace(Some(resp_rx));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Must receive epoch not match error.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Must receive epoch not match error.
    let mut events = receive_event(false);
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
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts);
    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k.clone()], start_ts, commit_ts);

    // Prewrite again
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    if events.len() == 1 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
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
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }

    // checkpoint_ts = 5;
    let checkpoint_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    // Commit = 6;
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k.clone()], start_ts, commit_ts);
    // Prewrite delete
    // Start = 7;
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Del);
    mutation.key = k.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.checkpoint_ts = checkpoint_ts.into_inner();
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    if events.len() == 1 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
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
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_tso_failure() {
    let mut suite = TestSuite::new(3);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
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
    let mut counter = 0;
    let mut previous_ts = 0;
    loop {
        // Even if there is no write,
        // resolved ts should be advanced regularly.
        for e in receive_event(true) {
            match e.event.unwrap() {
                Event_oneof_event::ResolvedTs(ts) => {
                    assert!(ts >= previous_ts);
                    previous_ts = ts;
                    counter += 1;
                }
                _ => panic!("unknown event"),
            }
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
    let mut suite = TestSuite::new(3);

    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
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
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    // Try to subscribe region again.
    let region = suite.cluster.get_region(b"k0");
    // Ensure it is the previous region.
    assert_eq!(req.get_region_id(), region.get_id());
    req.set_region_epoch(region.get_region_epoch().clone());
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
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

#[cfg(feature = "failpoints")]
#[test]
fn test_duplicate_subscribe() {
    let mut suite = TestSuite::new(3);

    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    // Try to subscribe again.
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    // Should receive duplicate request error.
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_duplicate_request(), "{:?}", err);
        }
        _ => panic!("unknown event"),
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

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1, "{:?}", events.len());
    while events.len() < 3 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 3, "{:?}", events.len());
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", e.get_type());
            assert_eq!(e.key, b"k1", "{:?}", e.key);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", e.get_type());
            assert_eq!(e.key, b"k2", "{:?}", e.key);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }
    match events.pop().unwrap().event.unwrap() {
        // Then it outputs Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1);
            let e = &es.entries[0];
            assert_eq!(
                e.get_type(),
                EventLogType::Initialized,
                "{:?}",
                e.get_type()
            );
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }

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

    let mut events = receive_event(false);
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
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
        Event_oneof_event::Admin(e) => panic!("{:?}", e),
        Event_oneof_event::LongTxn(e) => panic!("{:?}", e),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_cdc_long_txn() {
    let mut suite = TestSuite::new(1);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, _event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Make sure region 1 is registered.
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        // Even if there is no write,
        // it should always outputs an Initialized event.
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now_ts = TimeStamp::compose(now.as_millis().try_into().unwrap(), 0);
    let start_ts = TimeStamp::compose(
        now.sub(Duration::from_secs(120))
            .as_millis()
            .try_into()
            .unwrap(),
        0,
    );
    let min_commit_ts1 = TimeStamp::compose(
        now.sub(Duration::from_secs(80))
            .as_millis()
            .try_into()
            .unwrap(),
        0,
    );
    let min_commit_ts2 = TimeStamp::compose(
        now.sub(Duration::from_secs(40))
            .as_millis()
            .try_into()
            .unwrap(),
        0,
    );

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite_large_txn(1, vec![mutation], k.clone(), start_ts, start_ts.next());
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        _ => panic!("unknown event"),
    }
    // Wait for resolved ts to be pushed to `start_ts`
    suite.set_tso(now_ts);
    let wait_for_resolved_ts_and_report_long_txn =
        |ts_pred: Box<dyn Fn(u64) -> bool>, expect_long_txn: bool| {
            let mut report_long_txn_received = false;
            let mut resolved_ts_reached = false;
            for _retry in 0..30 {
                let events = receive_event(true);
                for event in events {
                    match event.event.unwrap() {
                        Event_oneof_event::ResolvedTs(rts) => {
                            if resolved_ts_reached {
                                assert!(ts_pred(rts));
                            }
                            if ts_pred(rts) {
                                resolved_ts_reached = true;
                            }
                        }
                        Event_oneof_event::LongTxn(txn) => {
                            assert_eq!(txn.get_txn_info()[0].get_start_ts(), start_ts.into_inner());
                            report_long_txn_received = true;
                        }
                        e => panic!("unexpected event {}", format_event_enum(&e)),
                    }
                    if (report_long_txn_received || !expect_long_txn) && resolved_ts_reached {
                        return;
                    }
                }
                sleep_ms(50);
            }
            panic!(
                "can't receive expected events. got resolved ts: {}; got long txn: {}",
                resolved_ts_reached, report_long_txn_received
            );
        };

    wait_for_resolved_ts_and_report_long_txn(Box::new(|ts| ts == start_ts.into_inner()), true);

    // Push min_commit_ts by updating primary key.
    // `kv_check_txn_status` uses `max(caller_start_ts + 1, current_ts)` as the mew `min_commit_ts`,
    // so we pass it `min_commit_ts - 1`.
    let (_, _, action) = suite.must_kv_check_txn_status(
        1,
        k.clone(),
        start_ts,
        min_commit_ts1.prev(),
        min_commit_ts1.prev(),
    );
    assert_eq!(action, Action::MinCommitTsPushed);
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        _ => panic!("unknown event"),
    }

    // Then the resolved ts can be pushed to `min_commit_ts - 1`.
    wait_for_resolved_ts_and_report_long_txn(
        Box::new(|ts| ts == min_commit_ts1.prev().into_inner()),
        true,
    );

    // Push min_commit_ts by notifying txn status from CDC client.
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let mut txn_status = TxnStatus::default();
    txn_status.set_start_ts(start_ts.into_inner());
    txn_status.set_min_commit_ts(min_commit_ts2.into_inner());
    let mut notify = ChangeDataRequestNotifyTxnStatus::default();
    notify.set_txn_status(vec![txn_status].into());
    req.request = Some(ChangeDataRequest_oneof_Request::NotifyTxnStatus(notify));

    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();

    wait_for_resolved_ts_and_report_long_txn(
        Box::new(|ts| ts == min_commit_ts2.prev().into_inner()),
        true,
    );

    // Commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k], start_ts, commit_ts);
    let mut events = receive_event(false);
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Commit);
        }
        _ => panic!("unknown event"),
    }
    wait_for_resolved_ts_and_report_long_txn(Box::new(|ts| ts > now_ts.into_inner()), false);

    suite.stop();
}
