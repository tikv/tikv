// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

use api_version::{test_kv_format_impl, KvFormat};
use futures::{executor::block_on, sink::SinkExt};
use grpcio::WriteFlags;
use kvproto::{cdcpb::*, kvrpcpb::*, raft_serverpb::RaftMessage};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, HandyRwLock};

use crate::{new_event_feed, TestSuite, TestSuiteBuilder};

#[test]
fn test_observe_duplicate_cmd() {
    test_kv_format_impl!(test_observe_duplicate_cmd_impl<ApiV1 ApiV2>);
}

fn test_observe_duplicate_cmd_impl<F: KvFormat>() {
    let mut suite = TestSuite::new(3, F::TAG);

    let region = suite.cluster.get_region(&[]);
    let req = suite.new_changedata_request(region.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.get_id()));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        other => panic!("unknown event {:?}", other),
    }

    // If tikv enable ApiV2, txn key needs to start with 'x';
    let (k, v) = ("xkey1".to_owned(), "value".to_owned());
    // Prewrite
    let start_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone().into_bytes();
    mutation.value = v.into_bytes();
    suite.must_kv_prewrite(
        region.get_id(),
        vec![mutation],
        k.clone().into_bytes(),
        start_ts,
    );
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(entries) => {
            assert_eq!(entries.entries.len(), 1);
            assert_eq!(entries.entries[0].get_type(), EventLogType::Prewrite);
        }
        other => panic!("unknown event {:?}", other),
    }
    let fp = "before_cdc_flush_apply";
    fail::cfg(fp, "pause").unwrap();

    // Async commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let commit_resp =
        suite.async_kv_commit(region.get_id(), vec![k.into_bytes()], start_ts, commit_ts);
    sleep_ms(200);
    // Close previous connection and open a new one twice time
    let (mut req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.replace(Some(resp_rx));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    let (mut req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.replace(Some(resp_rx));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    fail::remove(fp);
    // Receive Commit response
    block_on(commit_resp).unwrap();
    let mut events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1);
    match events.pop().unwrap().event.unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        other => panic!("unknown event {:?}", other),
    }

    // Make sure resolved ts can be advanced normally even with few tso failures.
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

    event_feed_wrap.replace(None);
    suite.stop();
}

// TODO: Change cmd is not used currently, so the test is unneeded,
// uncomment it after change cmd is used again
// #[test]
#[allow(dead_code)]
fn test_delayed_change_cmd() {
    let mut cluster = new_server_cluster(1, 3);
    configure_for_lease_read(&mut cluster, Some(50), Some(20));
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(100);
    cluster.pd_client.disable_default_operator();
    let mut suite = TestSuiteBuilder::new().cluster(cluster).build();
    suite.cluster.must_put(b"k1", b"v1");
    let region = suite.cluster.pd_client.get_region(&[]).unwrap();
    let leader = new_peer(1, 1);
    suite
        .cluster
        .must_transfer_leader(region.get_id(), leader.clone());
    // Wait util lease expired
    sleep_ms(300);

    let (sx, rx) = mpsc::sync_channel::<RaftMessage>(1);
    let send_flag = Arc::new(AtomicBool::new(true));
    let send_read_index_filter = RegionPacketFilter::new(region.get_id(), leader.get_store_id())
        .direction(Direction::Send)
        .msg_type(MessageType::MsgHeartbeat)
        .set_msg_callback(Arc::new(move |msg: &RaftMessage| {
            if send_flag
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                sx.send(msg.clone()).unwrap();
            }
        }));
    suite
        .cluster
        .add_send_filter(CloneFilterFactory(send_read_index_filter));

    let req = suite.new_changedata_request(region.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.get_id()));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();

    suite.cluster.must_put(b"k2", b"v2");

    let (mut req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.replace(Some(resp_rx));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    sleep_ms(200);

    suite
        .cluster
        .sim
        .wl()
        .clear_send_filters(leader.get_store_id());
    rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let mut counter = 0;
    loop {
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            assert_ne!(0, resolved_ts.ts);
            counter += 1;
        }
        for e in event.events.into_iter() {
            match e.event.unwrap() {
                Event_oneof_event::Entries(es) => {
                    assert!(es.entries.len() == 1, "{:?}", es);
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
                }
                other => panic!("unknown event {:?}", other),
            }
        }
        if counter > 3 {
            break;
        }
    }

    event_feed_wrap.replace(None);
    suite.stop();
}
