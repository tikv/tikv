// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::time::Duration;

use futures::sink::Sink;
use futures::Future;
use grpcio::WriteFlags;
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    create_change_data,
    event::{row::OpType as EventRowOpType, Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataClient, ChangeDataEvent, ChangeDataRequest,
};
use raft::StateRole;
use raftstore::coprocessor::{ObserverContext, RoleObserver};
use test_raftstore::sleep_ms;
use util::*;

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

#[test]
fn test_region_ready_after_deregister() {
    let mut suite = TestSuite::new(1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_cdc_client(1));
    let _ = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Sleep for a while to make sure the region has been subscribed
    std::thread::sleep(Duration::from_millis(300));

    // Simulate a role change event
    let region = suite.cluster.get_region(&[]);
    let leader = suite.cluster.leader_of_region(region.get_id()).unwrap();
    let mut context = ObserverContext::new(&region);
    suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .on_role_change(&mut context, StateRole::Follower);

    // Then CDC should not panic
    fail::remove(fp);
    receive_event(false);

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
