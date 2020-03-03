// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::Cell;
use std::rc::Rc;
use std::sync::*;
use std::time::Duration;

use futures::sink::Sink;
use futures::{Future, Stream};
use grpcio::{ClientDuplexReceiver, ClientDuplexSender, WriteFlags};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    create_change_data,
    event::{row::OpType as EventRowOpType, Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataClient, ChangeDataEvent, ChangeDataRequest,
};
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::sleep_ms;

use cdc::Task;

use super::TestSuite;

#[allow(clippy::type_complexity)]
fn new_event_feed(
    client: &ChangeDataClient,
) -> (
    ClientDuplexSender<ChangeDataRequest>,
    Rc<Cell<Option<ClientDuplexReceiver<ChangeDataEvent>>>>,
    impl Fn(bool) -> Event_oneof_event,
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
        let mut change_data = change_data.unwrap();
        assert_eq!(change_data.events.len(), 1);
        let change_data_event = &mut change_data.events[0];
        let event = change_data_event.event.take().unwrap();
        match event {
            Event_oneof_event::ResolvedTs(_) if !keep_resolved_ts => continue,
            other => return other,
        }
    };
    (req_tx, event_feed_wrap, receive_event)
}

#[test]
fn test_cdc_basic() {
    let mut suite = TestSuite::new(1);

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
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
    suite.must_kv_prewrite(1, vec![mutation], k.clone().into_bytes(), start_ts);
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
    suite.must_kv_commit(1, vec![k.into_bytes()], start_ts, commit_ts);
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
    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.replace(Some(resp_rx));
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
    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
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

    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
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

    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let feed1_holder = event_feed_wrap.replace(Some(resp_rx));
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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
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
    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let event_feed1 = event_feed_wrap.replace(Some(resp_rx));

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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();

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

    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.replace(Some(resp_rx));
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
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
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
    let (req_tx, resp_rx) = suite.get_cdc_client(1).event_feed().unwrap();
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.replace(Some(resp_rx));
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
