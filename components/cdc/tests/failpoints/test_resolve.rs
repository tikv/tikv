// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{new_event_feed, TestSuite};
use futures::sink::Sink;
use futures::Future;
use grpcio::WriteFlags;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::event::{Event as Event_oneof_event, LogType as EventLogType};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::config::*;

#[test]
fn test_stale_resolver() {
    let mut suite = TestSuite::new(3);

    let fp = "before_schedule_resolver_ready";
    fail::cfg(fp, "pause").unwrap();

    // Close previous connection and open a new one twice time
    let region = suite.cluster.get_region(&[]);
    let req = suite.new_changedata_request(region.get_id());
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
    let mut events = receive_event(false).events.to_vec();
    while events.len() < 2 {
        events.extend(receive_event(false).events.into_iter());
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
            other => panic!("unknown event {:?}", other),
        }
    }

    event_feed_wrap.as_ref().replace(Some(resp_rx1));
    // Receive events
    for _ in 0..2 {
        let mut events = receive_event(false).events.to_vec();
        match events.pop().unwrap().event.unwrap() {
            Event_oneof_event::Entries(es) => match es.entries.len() {
                1 => {
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Commit, "{:?}", es);
                }
                2 => {
                    let e = &es.entries[0];
                    assert_eq!(e.get_type(), EventLogType::Prewrite, "{:?}", es);
                    let e = &es.entries[1];
                    assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
                }
                _ => {
                    panic!("unexepected event length {:?}", es);
                }
            },
            Event_oneof_event::Error(e) => panic!("{:?}", e),
            other => panic!("unknown event {:?}", other),
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}

#[test]
fn test_region_error() {
    let mut cluster = new_server_cluster(1, 1);
    cluster.cfg.cdc.min_ts_interval = ReadableDuration::millis(100);
    let mut suite = TestSuite::with_cluster(1, cluster);

    let multi_batch_fp = "cdc_before_handle_multi_batch";
    fail::cfg(multi_batch_fp, "return").unwrap();
    let deregister_fp = "cdc_before_handle_deregister";
    fail::cfg(deregister_fp, "return").unwrap();

    // Split region
    let region = suite.cluster.get_region(&[]);
    suite.cluster.must_split(&region, b"k1");
    // Subscribe source region
    let source = suite.cluster.get_region(b"k0");
    let mut req = suite.new_changedata_request(region.get_id());
    req.region_id = source.get_id();
    req.set_region_epoch(source.get_region_epoch().clone());
    let (source_tx, source_wrap, source_event) =
        new_event_feed(suite.get_region_cdc_client(source.get_id()));
    let _source_tx = source_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    // Subscribe target region
    let target = suite.cluster.get_region(b"k2");
    req.region_id = target.get_id();
    req.set_region_epoch(target.get_region_epoch().clone());
    let (target_tx, target_wrap, _target_event) =
        new_event_feed(suite.get_region_cdc_client(target.get_id()));
    let _target_tx = target_tx.send((req, WriteFlags::default())).wait().unwrap();
    sleep_ms(200);

    suite
        .cluster
        .pd_client
        .must_merge(source.get_id(), target.get_id());
    sleep_ms(1000);

    let mut last_resolved_ts = 0;
    for _ in 0..5 {
        let event = source_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            let ts = resolved_ts.ts;
            assert!(ts > last_resolved_ts);
            last_resolved_ts = ts;
        }
    }
    fail::remove(multi_batch_fp);
    fail::remove(deregister_fp);

    source_wrap.as_ref().replace(None);
    target_wrap.as_ref().replace(None);
    suite.stop();
}
