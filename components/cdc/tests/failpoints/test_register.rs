// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::thread;
use std::time::Duration;

use crate::{new_event_feed, TestSuite};
use futures::sink::Sink;
use futures::Future;
use grpcio::WriteFlags;
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    event::{Event as Event_oneof_event, LogType as EventLogType},
    ChangeDataRequest,
};
use kvproto::kvrpcpb::*;
use kvproto::metapb::RegionEpoch;
use pd_client::PdClient;
use raft::StateRole;
use raftstore::coprocessor::{ObserverContext, RoleObserver};
use test_raftstore::sleep_ms;

#[test]
fn test_failed_pending_batch() {
    // For test that a pending cmd batch contains a error like epoch not match.
    let mut suite = TestSuite::new(3);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Split region.
    suite.cluster.must_split(&region, b"k0");
    // Wait for receiving split cmd.
    sleep_ms(200);
    fail::remove(fp);

    let mut events = receive_event(false);
    if events.len() == 1 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
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
    assert_eq!(events.len(), 1, "{:?}", events);
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

#[test]
fn test_region_ready_after_deregister() {
    let mut suite = TestSuite::new(1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
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
        .on_role_change(&mut context, StateRole::Follower);

    // Then CDC should not panic
    fail::remove(fp);
    receive_event(false);

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_connections_register() {
    let mut suite = TestSuite::new(3);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Region info
    let region = suite.cluster.get_region(&[]);
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(region.get_id(), vec![mutation], k.into_bytes(), start_ts);

    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(RegionEpoch::default());

    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    let mut events = receive_event(false);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    // Conn 1
    req.set_region_epoch(region.get_region_epoch().clone());
    let _req_tx1 = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    // Close conn 1
    event_feed_wrap.as_ref().replace(None);
    // Conn 2
    let (req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    let _req_tx1 = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    // Split region.
    suite.cluster.must_split(&region, b"k0");
    fail::remove(fp);
    // Receive events from conn 2
    let mut events = receive_event(false);
    while events.len() < 2 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events.len());
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        _ => panic!("unknown event"),
    }
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    event_feed_wrap.as_ref().replace(None);
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
    let mut req = ChangeDataRequest::default();
    req.region_id = source.get_id();
    req.set_region_epoch(source.get_region_epoch().clone());
    let (source_tx, source_wrap, source_event) =
        new_event_feed(suite.get_region_cdc_client(source.get_id()));
    let source_tx = source_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Subscribe target region
    let target = suite.cluster.get_region(b"k2");
    req.region_id = target.get_id();
    req.set_region_epoch(target.get_region_epoch().clone());
    let (target_tx, target_wrap, target_event) =
        new_event_feed(suite.get_region_cdc_client(target.get_id()));
    let _target_tx = target_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    sleep_ms(200);
    // Pause before completing commit merge
    let commit_merge_fp = "before_handle_catch_up_logs_for_merge";
    fail::cfg(commit_merge_fp, "pause").unwrap();
    // The call is finished when prepare_merge is applied.
    suite.cluster.try_merge(source.get_id(), target.get_id());
    // Epoch not match after prepare_merge
    let mut events = source_event(false);
    if events.len() == 1 {
        events.extend(source_event(false).into_iter());
    }
    assert_eq!(events.len(), 2, "{:?}", events);
    match events.remove(0).event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
    }
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }
    let mut events = target_event(false);
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        _ => panic!("unknown event"),
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
    let _source_tx = source_tx.send((req, WriteFlags::default())).wait().unwrap();
    // Wait until raftstore receives ChangeCmd
    sleep_ms(100);
    fail::remove(destroy_peer_fp);
    loop {
        let mut events = source_event(false);
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
            _ => panic!("unknown event"),
        }
    }
    let mut events = target_event(false);
    assert_eq!(events.len(), 1, "{:?}", events);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        _ => panic!("unknown event"),
    }

    source_wrap.as_ref().replace(None);
    target_wrap.as_ref().replace(None);
    suite.stop();
}
