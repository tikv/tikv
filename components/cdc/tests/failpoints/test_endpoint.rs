// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{new_event_feed, TestSuite};
use futures::{Future, Sink};
use grpcio::WriteFlags;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::event::{Event as Event_oneof_event, LogType as EventLogType};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use test_raftstore::*;

#[test]
fn test_cdc_double_scan_deregister() {
    let mut suite = TestSuite::new(1);

    let (k, v) = (b"key1".to_vec(), b"value".to_vec());
    // Prewrite
    let start_ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k], start_ts1, commit_ts1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, _receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let req = suite.new_changedata_request(1);
    let (req_tx_1, event_feed_wrap_1, receive_event_1) =
        new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx_1 = req_tx_1.send((req, WriteFlags::default())).wait().unwrap();

    // close connection
    drop(req_tx);
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
    let start_ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v;
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k], start_ts1, commit_ts1);

    fail::cfg("cdc_incremental_scan_start", "pause").unwrap();
    fail::cfg("cdc_scan_batch_fail", "1*return").unwrap();

    let req = suite.new_changedata_request(1);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let req = suite.new_changedata_request(1);
    let (req_tx_1, event_feed_wrap_1, receive_event_1) =
        new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx_1 = req_tx_1.send((req, WriteFlags::default())).wait().unwrap();

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
            if let Event_oneof_event::Error(err) = event_vec.pop().unwrap().event.unwrap() {
                assert!(err.has_region_not_found(), "{:?}", err);
                break 'outer;
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
                    tikv_util::debug!("cdc receive event {:?}", e);
                }
            }
        }
    }

    event_feed_wrap.replace(None);
    event_feed_wrap_1.replace(None);
    suite.stop();
}
