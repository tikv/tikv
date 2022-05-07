// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::{thread, time::Duration};

use futures::{executor::block_on, sink::SinkExt};
use grpcio::WriteFlags;
use kvproto::{cdcpb::*, kvrpcpb::*, metapb::RegionEpoch};
use pd_client::PdClient;
use raft::StateRole;
use raftstore::coprocessor::{ObserverContext, RoleChange, RoleObserver};
use test_raftstore::sleep_ms;

use crate::{new_event_feed, TestSuite};

#[test]
fn test_failed_pending_batch() {
    // For test that a pending cmd batch contains a error like epoch not match.
    let mut suite = TestSuite::new(3);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let region = suite.cluster.get_region(&[]);
    let mut req = suite.new_changedata_request(region.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    // Split region.
    suite.cluster.must_split(&region, b"k0");
    // Wait for receiving split cmd.
    sleep_ms(200);
    fail::remove(fp);

    loop {
        let mut events = receive_event(false).events.to_vec();
        match events.pop().unwrap().event.unwrap() {
            Event_oneof_event::Error(err) => {
                assert!(err.has_epoch_not_match(), "{:?}", err);
                break;
            }
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            other => panic!("unknown event {:?}", other),
        }
    }
    // Try to subscribe region again.
    let region = suite.cluster.get_region(b"k0");
    // Ensure it is the previous region.
    assert_eq!(req.get_region_id(), region.get_id());
    req.set_region_epoch(region.get_region_epoch().clone());
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.replace(None);
    suite.stop();
}

#[test]
fn test_region_ready_after_deregister() {
    let mut suite = TestSuite::new(1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    // Sleep for a while to make sure the region has been subscribed
    sleep_ms(200);

    // Simulate a role change event
    let region = suite.cluster.get_region(&[]);
    let leader = suite.cluster.leader_of_region(region.get_id()).unwrap();
    let mut context = ObserverContext::new(&region);
    suite
        .obs
        .get(&leader.get_store_id())
        .unwrap()
        .on_role_change(&mut context, &RoleChange::new(StateRole::Follower));

    // Then CDC should not panic
    fail::remove(fp);
    receive_event(false);

    event_feed_wrap.replace(None);
    suite.stop();
}

#[test]
fn test_connections_register() {
    let mut suite = TestSuite::new(1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Region info
    let region = suite.cluster.get_region(&[]);
    // Prewrite
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(region.get_id(), vec![mutation], k.into_bytes(), start_ts);

    let mut req = suite.new_changedata_request(region.get_id());
    req.set_region_epoch(RegionEpoch::default());

    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    // Conn 1
    req.set_region_epoch(region.get_region_epoch().clone());
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    thread::sleep(Duration::from_secs(1));
    // Close conn 1
    event_feed_wrap.replace(None);
    // Conn 2
    let (mut req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    event_feed_wrap.replace(Some(resp_rx));
    // Split region.
    suite.cluster.must_split(&region, b"k0");
    fail::remove(fp);
    // Receive events from conn 2
    // As split happens before remove the pause fail point, so it must receive
    // an epoch not match error.
    let mut events = receive_event(false).events.to_vec();
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    event_feed_wrap.replace(None);
    suite.stop();
}

#[test]
fn test_merge() {
    let mut suite = TestSuite::new(1);
    // Split region
    let region = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region, b"k1");
    // Subscribe source region
    let source = suite.cluster.get_region(b"k0");
    let mut req = suite.new_changedata_request(region.get_id());
    req.region_id = source.get_id();
    req.set_region_epoch(source.get_region_epoch().clone());
    let (mut source_tx, source_wrap, source_event) =
        new_event_feed(suite.get_region_cdc_client(source.get_id()));
    block_on(source_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    // Subscribe target region
    let target = suite.cluster.get_region(b"k2");
    req.region_id = target.get_id();
    req.set_region_epoch(target.get_region_epoch().clone());
    let (mut target_tx, target_wrap, target_event) =
        new_event_feed(suite.get_region_cdc_client(target.get_id()));
    block_on(target_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    sleep_ms(200);
    // Pause before completing commit merge
    let commit_merge_fp = "before_handle_catch_up_logs_for_merge";
    fail::cfg(commit_merge_fp, "pause").unwrap();
    // The call is finished when prepare_merge is applied.
    suite.cluster.try_merge(source.get_id(), target.get_id());
    // Epoch not match after prepare_merge
    let mut events = source_event(false).events.to_vec();
    if events.len() == 1 {
        events.extend(source_event(false).events.into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    let mut events = target_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unknown event {:?}", other),
    }
    // Continue to commit merge
    let destroy_peer_fp = "destroy_peer";
    fail::cfg(destroy_peer_fp, "pause").unwrap();
    fail::remove(commit_merge_fp);
    // Wait until raftstore receives MergeResult
    sleep_ms(100);
    // Retry to subscribe source region
    let mut source_epoch = source.get_region_epoch().clone();
    source_epoch.set_version(source_epoch.get_version() + 1);
    source_epoch.set_conf_ver(source_epoch.get_conf_ver() + 1);
    req.region_id = source.get_id();
    req.set_region_epoch(source_epoch);
    block_on(source_tx.send((req, WriteFlags::default()))).unwrap();
    // Wait until raftstore receives ChangeCmd
    sleep_ms(100);
    fail::remove(destroy_peer_fp);
    loop {
        let mut events = source_event(false).events.to_vec();
        assert_eq!(events.len(), 1, "{:?}", events);
        match events.pop().unwrap().event.unwrap() {
            Event_oneof_event::Error(err) => {
                assert!(err.has_region_not_found(), "{:?}", err);
                break;
            }
            Event_oneof_event::Entries(es) => {
                assert!(es.entries.len() == 1, "{:?}", es);
                let e = &es.entries[0];
                assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
            }
            other => panic!("unknown event {:?}", other),
        }
    }
    let mut events = target_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    source_wrap.replace(None);
    target_wrap.replace(None);
    suite.stop();
}

#[test]
fn test_deregister_pending_downstream() {
    let mut suite = TestSuite::new(1);

    let build_resolver_fp = "before_schedule_resolver_ready";
    fail::cfg(build_resolver_fp, "pause").unwrap();
    let mut req = suite.new_changedata_request(1);
    let (mut req_tx1, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx1.send((req.clone(), WriteFlags::default()))).unwrap();
    // Sleep for a while to make sure the region has been subscribed
    sleep_ms(200);

    let raft_capture_fp = "raft_on_capture_change";
    fail::cfg(raft_capture_fp, "pause").unwrap();

    // Conn 2
    let (mut req_tx2, resp_rx2) = suite.get_region_cdc_client(1).event_feed().unwrap();
    req.set_region_epoch(RegionEpoch::default());
    block_on(req_tx2.send((req.clone(), WriteFlags::default()))).unwrap();
    let _resp_rx1 = event_feed_wrap.replace(Some(resp_rx2));
    // Sleep for a while to make sure the region has been subscribed
    sleep_ms(200);
    fail::remove(build_resolver_fp);
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }

    block_on(req_tx2.send((req, WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unknown event {:?}", other),
    }
    fail::remove(raft_capture_fp);

    event_feed_wrap.replace(None);
    suite.stop();
}
