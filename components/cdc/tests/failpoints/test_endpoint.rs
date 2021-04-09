// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{new_event_feed, TestSuite};
use futures::executor::block_on;
use futures::sink::SinkExt;
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
    let start_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k.clone();
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k.clone()], start_ts1, commit_ts1);

    let fp = "cdc_incremental_scan_start";
    fail::cfg(fp, "pause").unwrap();

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let req = suite.new_changedata_request(1);
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
        println!("cdc receive_event {:?}", event_vec);
        while event_vec.len() > 0 {
            match event_vec.pop().unwrap().event.unwrap() {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_region_not_found(), "{:?}", err);
                    break 'outer;
                }
                _ => {}
            }
        }
    }

    event_feed_wrap_1.replace(None);
    receive_event.replace(None);
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
    mutation.value = v.clone();
    suite.must_kv_prewrite(1, vec![mutation], k.clone(), start_ts1);
    // Commit
    let commit_ts1 = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    suite.must_kv_commit(1, vec![k.clone()], start_ts1, commit_ts1);

    fail::cfg("cdc_incremental_scan_start", "pause").unwrap();
    fail::cfg("cdc_scan_batch_fail", "1*return").unwrap();

    let req = suite.new_changedata_request(1);
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(1));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();

    // wait for the first connection to start incremental scan
    sleep_ms(1000);

    let req = suite.new_changedata_request(1);
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
        while event_vec.len() > 0 {
            match event_vec.pop().unwrap().event.unwrap() {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_region_not_found(), "{:?}", err);
                    break 'outer;
                }
                e => {
                    println!("cdc receive_event {:?}", e);
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
        println!("cdc receive_event {:?}", event_vec);
        while event_vec.len() > 0 {
            match event_vec.pop().unwrap().event.unwrap() {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_region_not_found(), "{:?}", err);
                    break 'outer1;
                }
                e => {
                    println!("cdc receive_event {:?}", e);
                }
            }
        }
    }

    event_feed_wrap.replace(None);
    event_feed_wrap_1.replace(None);
    suite.stop();
}
