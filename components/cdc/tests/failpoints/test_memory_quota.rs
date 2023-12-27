// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::*, time::Duration};

use cdc::{Task, Validate};
use futures::{executor::block_on, SinkExt};
use grpcio::WriteFlags;
use kvproto::{cdcpb::*, kvrpcpb::*};
use pd_client::PdClient;
use test_raftstore::*;

use crate::{new_event_feed, TestSuiteBuilder};

#[test]
fn test_resolver_track_lock_memory_quota_exceeded() {
    let mut cluster = new_server_cluster(1, 1);
    // Increase the Raft tick interval to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(100), None);
    let memory_quota = 1024; // 1KB
    let mut suite = TestSuiteBuilder::new()
        .cluster(cluster)
        .memory_quota(memory_quota)
        .build();

    // Let CdcEvent size be 0 to effectively disable memory quota for CdcEvent.
    fail::cfg("cdc_event_size", "return(0)").unwrap();

    let req = suite.new_changedata_request(1);
    let (mut req_tx, _event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
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

    // Client must receive messages when there is no congest error.
    let key_size = memory_quota / 2;
    let (k, v) = (vec![1; key_size], vec![5]);
    // Prewrite
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k, start_ts);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Trigger congest error.
    let key_size = memory_quota * 2;
    let (k, v) = (vec![2; key_size], vec![5]);
    // Prewrite
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k, start_ts);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(e) => {
            // Unknown errors are translated into region_not_found.
            assert!(e.has_region_not_found(), "{:?}", e);
        }
        other => panic!("unknown event {:?}", other),
    }

    // The delegate must be removed.
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    let (tx, rx) = mpsc::channel();
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(move |delegate| {
                tx.send(delegate.is_none()).unwrap();
            }),
        )))
        .unwrap();

    assert!(
        rx.recv_timeout(Duration::from_millis(1000)).unwrap(),
        "find unexpected delegate"
    );

    suite.stop();
}

#[test]
fn test_pending_on_region_ready_memory_quota_exceeded() {
    let mut cluster = new_server_cluster(1, 1);
    // Increase the Raft tick interval to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(100), None);
    let memory_quota = 1024; // 1KB
    let mut suite = TestSuiteBuilder::new()
        .cluster(cluster)
        .memory_quota(memory_quota)
        .build();

    // Let CdcEvent size be 0 to effectively disable memory quota for CdcEvent.
    fail::cfg("cdc_event_size", "return(0)").unwrap();

    // Trigger memory quota exceeded error.
    fail::cfg("cdc_pending_on_region_ready", "return").unwrap();
    let req = suite.new_changedata_request(1);
    let (mut req_tx, _event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
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
    // MemoryQuotaExceeded error is triggered on_region_ready.
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(e) => {
            // Unknown errors are translated into region_not_found.
            assert!(e.has_region_not_found(), "{:?}", e);
        }
        other => panic!("unknown event {:?}", other),
    }

    // The delegate must be removed.
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    let (tx, rx) = mpsc::channel();
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(move |delegate| {
                tx.send(delegate.is_none()).unwrap();
            }),
        )))
        .unwrap();

    assert!(
        rx.recv_timeout(Duration::from_millis(1000)).unwrap(),
        "find unexpected delegate"
    );

    fail::remove("cdc_incremental_scan_start");
    suite.stop();
}

#[test]
fn test_pending_push_lock_memory_quota_exceeded() {
    let mut cluster = new_server_cluster(1, 1);
    // Increase the Raft tick interval to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(100), None);
    let memory_quota = 1024; // 1KB
    let mut suite = TestSuiteBuilder::new()
        .cluster(cluster)
        .memory_quota(memory_quota)
        .build();

    // Let CdcEvent size be 0 to effectively disable memory quota for CdcEvent.
    fail::cfg("cdc_event_size", "return(0)").unwrap();

    // Pause scan so that no region can be initialized, and all locks will be
    // put in pending locks.
    fail::cfg("cdc_incremental_scan_start", "pause").unwrap();

    let req = suite.new_changedata_request(1);
    let (mut req_tx, _event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();

    // Trigger congest error.
    let key_size = memory_quota * 2;
    let (k, v) = (vec![1; key_size], vec![5]);
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k, start_ts);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(e) => {
            // Unknown errors are translated into region_not_found.
            assert!(e.has_region_not_found(), "{:?}", e);
        }
        other => panic!("unknown event {:?}", other),
    }

    // The delegate must be removed.
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    let (tx, rx) = mpsc::channel();
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(move |delegate| {
                tx.send(delegate.is_none()).unwrap();
            }),
        )))
        .unwrap();

    assert!(
        rx.recv_timeout(Duration::from_millis(1000)).unwrap(),
        "find unexpected delegate"
    );

    fail::remove("cdc_incremental_scan_start");
    suite.stop();
}

#[test]
fn test_scan_lock_memory_quota_exceeded() {
    let mut cluster = new_server_cluster(1, 1);
    // Increase the Raft tick interval to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(100), None);
    let memory_quota = 1024; // 1KB
    let mut suite = TestSuiteBuilder::new()
        .cluster(cluster)
        .memory_quota(memory_quota)
        .build();

    // Let CdcEvent size be 0 to effectively disable memory quota for CdcEvent.
    fail::cfg("cdc_event_size", "return(0)").unwrap();

    // Put a lock that exceeds memory quota.
    let key_size = memory_quota * 2;
    let (k, v) = (vec![1; key_size], vec![5]);
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k, start_ts);

    // No region can be initialized.
    let req = suite.new_changedata_request(1);
    let (mut req_tx, _event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(e) => {
            // Unknown errors are translated into region_not_found.
            assert!(e.has_region_not_found(), "{:?}", e);
        }
        other => panic!("unknown event {:?}", other),
    }
    let scheduler = suite.endpoints.values().next().unwrap().scheduler();
    let (tx, rx) = mpsc::channel();
    scheduler
        .schedule(Task::Validate(Validate::Region(
            1,
            Box::new(move |delegate| {
                tx.send(delegate.is_none()).unwrap();
            }),
        )))
        .unwrap();

    assert!(
        rx.recv_timeout(Duration::from_millis(1000)).unwrap(),
        "find unexpected delegate"
    );

    suite.stop();
}
