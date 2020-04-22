// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
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
use pd_client::PdClient;
use test_raftstore::sleep_ms;

#[test]
fn test_stale_resolver() {
    let mut suite = TestSuite::new(3);

    let fp = "before_schedule_resolver_ready";
    fail::cfg(fp, "pause").unwrap();

    // Close previous connection and open a new one twice time
    let region = suite.cluster.get_region(&[]);
    let mut req = ChangeDataRequest::default();
    req.region_id = region.get_id();
    req.set_region_epoch(region.get_region_epoch().clone());
    let (req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.get_id()));
    let _req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Sleep for a while to wait the scan is done
    sleep_ms(200);

    let (k, v) = ("key1".to_owned(), "value".to_owned());
    // Prewrite
    let start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
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

    // Block next scan
    let fp1 = "cdc_incremental_scan_start";
    fail::cfg(fp1, "pause").unwrap();
    // Close previous connection and open two new connections
    let (req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    let (req_tx1, resp_rx1) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    let _req_tx1 = req_tx1.send((req, WriteFlags::default())).wait().unwrap();
    // Unblock the first scan
    fail::remove(fp);
    // Sleep for a while to wait the wrong resolver init
    sleep_ms(100);
    // Async commit
    let commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    let commit_resp =
        suite.async_kv_commit(region.get_id(), vec![k.into_bytes()], start_ts, commit_ts);
    // Receive Commit response
    commit_resp.wait().unwrap();
    // Unblock all scans
    fail::remove(fp1);
    // Receive events
    let mut events = receive_event(false);
    while events.len() < 2 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 2);
    for event in events {
        match event.event.unwrap() {
            Event_oneof_event::Entries(es) => match es.entries.len() {
                1 => {
                    assert_eq!(es.entries[0].get_type(), EventLogType::Commit, "{:?}", es);
                }
                2 => {
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
                    let e = &es.entries[1];
                    assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
                }
                _ => panic!("{:?}", es),
            },
            Event_oneof_event::Error(e) => panic!("{:?}", e),
            _ => panic!("unknown event"),
        }
    }

    event_feed_wrap.as_ref().replace(Some(resp_rx1));
    // Receive events
    let mut events = receive_event(false);
    while events.len() < 2 {
        events.extend(receive_event(false).into_iter());
    }
    assert_eq!(events.len(), 2);
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
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 1, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Commit, "{:?}", es);
        }
        Event_oneof_event::Error(e) => panic!("{:?}", e),
        _ => panic!("unknown event"),
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
