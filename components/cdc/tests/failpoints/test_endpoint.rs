// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::thread;
use std::time::Duration;

use cdc::recv_timeout;
use futures::executor::block_on;
use futures::sink::SinkExt;
use grpcio::WriteFlags;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::debug;

use kvproto::cdcpb::*;

use crate::{new_event_feed, TestSuite, TestSuiteBuilder};

#[test]
fn test_cdc_double_scan_deregister() {
    let mut suite = TestSuite::new(1);

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let start_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k], start_ts1, commit_ts1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let mut req = suite.new_changedata_request(1);
    req.mut_header().set_ticdc_version("5.0.0".into());
    let (mut req_tx, event_feed_wrap, _receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let mut req = suite.new_changedata_request(1);
    req.mut_header().set_ticdc_version("5.0.0".into());
    let (mut req_tx_1, event_feed_wrap_1, receive_event_1) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx_1.send((req, WriteFlags::default()))).unwrap();

    // close connection
    block_on(req_tx.close()).unwrap();
    event_feed_wrap.replace(None);

    // wait for the connection to close
    sleep_ms(300);

    fail::cfg(fp, "off").unwrap();

    'outer: loop {
        let event = receive_event_1(false);
        let mut event_vec = event.events.to_vec();
        while !event_vec.is_empty() {
            if let Event_oneof_event::Error(err) = event_vec.pop().unwrap().event.unwrap() {
                assert!(err.has_region_not_found(), "{:?}", err);
                break 'outer;
            }
        }
    }

    event_feed_wrap_1.replace(None);
    suite.stop();
}

#[test]
fn test_cdc_double_scan_io_error() {
    let mut suite = TestSuite::new(1);

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let start_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k], start_ts1, commit_ts1);

    fail::cfg("cdc_incremental_scan_start", "pause").unwrap();
    fail::cfg("cdc_scan_batch_fail", "1*return").unwrap();

    let mut req = suite.new_changedata_request(1);
    req.mut_header().set_ticdc_version("5.0.0".into());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let mut req = suite.new_changedata_request(1);
    req.mut_header().set_ticdc_version("5.0.0".into());
    let (mut req_tx_1, event_feed_wrap_1, receive_event_1) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx_1.send((req, WriteFlags::default()))).unwrap();

    // wait for the second connection to start incremental scan
    sleep_ms(1000);

    // unpause
    fail::cfg("cdc_incremental_scan_start", "off").unwrap();

    'outer: loop {
        let event = receive_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            // resolved ts has been pushed, so this conn is good.
            if resolved_ts.regions.len() == 1 {
                break 'outer;
            }
        }
        let mut event_vec = event.events.to_vec();
        while !event_vec.is_empty() {
            match event_vec.pop().unwrap().event.unwrap() {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_region_not_found(), "{:?}", err);
                    break 'outer;
                }
                e => {
                    debug!("cdc receive_event {:?}", e);
                }
            }
        }
    }

    'outer1: loop {
        let event = receive_event_1(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            // resolved ts has been pushed, so this conn is good.
            if resolved_ts.regions.len() == 1 {
                break 'outer1;
            }
        }
        let mut event_vec = event.events.to_vec();
        while !event_vec.is_empty() {
            match event_vec.pop().unwrap().event.unwrap() {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_region_not_found(), "{:?}", err);
                    break 'outer1;
                }
                e => {
                    debug!("cdc receive_event {:?}", e);
                }
            }
        }
    }

    event_feed_wrap.replace(None);
    event_feed_wrap_1.replace(None);
    suite.stop();
}

#[test]
#[ignore = "TODO: support continue scan after region split"]
fn test_cdc_scan_continues_after_region_split() {
    fail::cfg("cdc_after_incremental_scan_blocks_regional_errors", "pause").unwrap();

    let mut suite = TestSuite::new(1);

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let start_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k], start_ts1, commit_ts1);

    let mut req = suite.new_changedata_request(1);
    req.mut_header().set_ticdc_version("5.0.0".into());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let region = suite.cluster.get_region(b"key1");
    suite.cluster.must_split(&region, b"key2");

    // wait for region split to be processed
    sleep_ms(1000);

    fail::cfg("cdc_after_incremental_scan_blocks_regional_errors", "off").unwrap();

    let events = receive_event(false).events.to_vec();
    assert_eq!(events.len(), 1, "{:?}", events);
    match events[0].event.as_ref().unwrap() {
        Event_oneof_event::Entries(es) => {
            assert!(es.entries.len() == 2, "{:?}", es);
            let e = &es.entries[0];
            assert_eq!(e.get_type(), EventLogType::Committed, "{:?}", es);
            assert_eq!(e.start_ts, start_ts1.into_inner(), "{:?}", es);
            assert_eq!(e.commit_ts, commit_ts1.into_inner(), "{:?}", es);
            assert_eq!(e.key, b"key1", "{:?}", es);
            assert_eq!(e.value, b"value", "{:?}", es);

            let e = &es.entries[1];
            assert_eq!(e.get_type(), EventLogType::Initialized, "{:?}", es);
        }
        other => panic!("unexpected event {:?}", other),
    }

    let event = receive_event(true);
    if let Some(resolved_event) = event.resolved_ts.as_ref() {
        assert_eq!(resolved_event.regions, &[1], "{:?}", resolved_event);
        assert!(
            resolved_event.ts >= commit_ts1.into_inner(),
            "resolved_event: {:?}, commit_ts1: {:?}",
            resolved_event,
            commit_ts1
        );
    } else {
        panic!("unexpected event {:?}", event);
    }

    let events = receive_event(false).events.to_vec();
    match events[0].event.as_ref().unwrap() {
        Event_oneof_event::Error(err) => {
            assert!(err.has_epoch_not_match(), "{:?}", err);
        }
        other => panic!("unexpected event {:?}", other),
    }

    event_feed_wrap.replace(None);
    suite.stop();
}

// Test the `ResolvedTs` sequence shouldn't be pushed to a region downstream
// if the downstream hasn't been initialized.
#[test]
fn test_no_resolved_ts_before_downstream_initialized() {
    let cluster = new_server_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    let mut suite = TestSuiteBuilder::new().cluster(cluster).build();
    let region = suite.cluster.get_region(b"");
    let lead_client = PeerClient::new(&suite.cluster, region.id, new_peer(1, 1));

    // Create 2 changefeeds and the second will be blocked in initialization.
    let mut req_txs = Vec::with_capacity(2);
    let mut event_feeds = Vec::with_capacity(2);
    let mut receive_events = Vec::with_capacity(2);
    for i in 0..2 {
        if i == 1 {
            fail::cfg("cdc_before_initialize", "pause").unwrap();
        }
        let (mut req_tx, event_feed, receive_event) =
            new_event_feed(suite.get_region_cdc_client(region.id));
        let req = suite.new_changedata_request(region.id);
        block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
        req_txs.push(req_tx);
        event_feeds.push(event_feed);
        receive_events.push(receive_event);
        // Sleep a while to wait the capture has been initialized.
        thread::sleep(Duration::from_secs(1));
    }

    for version in 0..10 {
        let value = format!("value-{:0>6}", version);
        let start_ts = get_tso(&suite.cluster.pd_client);
        lead_client.must_kv_prewrite(
            vec![new_mutation(Op::Put, b"key", value.as_bytes())],
            b"key".to_vec(),
            start_ts,
        );
        let commit_ts = get_tso(&suite.cluster.pd_client);
        lead_client.must_kv_commit(vec![b"key".to_vec()], start_ts, commit_ts);
    }

    let th = thread::spawn(move || {
        // The first downstream can receive all real-time changes,
        // but the second can't receive nothing.
        for _ in 0..10 {
            let _ = receive_events[0](false);
            let mut rx = event_feeds[1].replace(None).unwrap();
            assert!(recv_timeout(&mut rx, Duration::from_secs(1)).is_err());
            event_feeds[1].replace(Some(rx));
        }
    });

    th.join().unwrap();
    fail::cfg("cdc_before_initialize", "off").unwrap();
    suite.stop();
}

// When a new CDC downstream is installed, delta changes for other downstreams on the same
// region should be flushed so that the new downstream can gets a fresh snapshot to performs
// a incremental scan. CDC can ensure that those delta changes are sent to CDC's `Endpoint`
// before the incremental scan, but `Sink` may break this rule. This case tests it won't
// happen any more.
#[test]
fn test_cdc_observed_before_incremental_scan_snapshot() {
    let cluster = new_server_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    let mut suite = TestSuiteBuilder::new().cluster(cluster).build();
    let region = suite.cluster.get_region(b"");
    let lead_client = PeerClient::new(&suite.cluster, region.id, new_peer(1, 1));

    // So that the second changefeed can get some delta changes elder than its snapshot.
    let (mut req_tx_0, event_feed_0, _) = new_event_feed(suite.get_region_cdc_client(region.id));
    let req_0 = suite.new_changedata_request(region.id);
    block_on(req_tx_0.send((req_0, WriteFlags::default()))).unwrap();

    fail::cfg("cdc_before_handle_multi_batch", "pause").unwrap();
    fail::cfg("cdc_sleep_before_drain_change_event", "return").unwrap();
    let (mut req_tx, event_feed, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.id));
    let req = suite.new_changedata_request(region.id);
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    thread::sleep(Duration::from_secs(1));

    for version in 0..10 {
        let key = format!("key-{:0>6}", version);
        let start_ts = get_tso(&suite.cluster.pd_client);
        lead_client.must_kv_prewrite(
            vec![new_mutation(Op::Put, key.as_bytes(), b"value")],
            key.as_bytes().to_owned(),
            start_ts,
        );
        let commit_ts = get_tso(&suite.cluster.pd_client);
        lead_client.must_kv_commit(vec![key.into_bytes()], start_ts, commit_ts);
    }

    fail::cfg("cdc_before_handle_multi_batch", "off").unwrap();
    fail::cfg("cdc_before_drain_change_event", "off").unwrap();
    // Wait the client wake up from `cdc_sleep_before_drain_change_event`.
    thread::sleep(Duration::from_secs(5));

    // `Initialized` should be the last event.
    let (mut initialized_pos, mut row_count) = (0, 0);
    while initialized_pos == 0 {
        for event in receive_event(false).get_events() {
            if let Some(Event_oneof_event::Entries(ref entries)) = event.event {
                for row in entries.get_entries() {
                    row_count += 1;
                    if row.r_type == EventLogType::Initialized {
                        initialized_pos = row_count;
                    }
                }
            }
        }
    }
    assert!(initialized_pos > 0);
    assert_eq!(initialized_pos, row_count);
    let mut rx = event_feed.replace(None).unwrap();
    if let Ok(Some(Ok(event))) = recv_timeout(&mut rx, Duration::from_secs(1)) {
        assert!(event.get_events().is_empty());
    }

    drop(event_feed_0);
    drop(event_feed);
    suite.stop();
}
