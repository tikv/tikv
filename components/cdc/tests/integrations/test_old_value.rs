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
fn test_old_value_basic() {
    let mut suite = TestSuite::new(1);
    let mut req = ChangeDataRequest::default();
    req.region_id = 1;
    req.set_region_epoch(suite.get_context(1).take_region_epoch());
    req.set_extra_read(ExtraRead::Updated);
    let (req_tx, event_feed_wrap, receive_event) = new_event_feed(suite.get_region_cdc_client(1));
    let _req_tx = req_tx
        .send((req.clone(), WriteFlags::default()))
        .wait()
        .unwrap();
    sleep_ms(1000);

    // Insert value
    let mut m1 = Mutation::default();
    let k1 = b"k1".to_vec();
    m1.set_op(Op::Put);
    m1.key = k1.clone();
    m1.value = b"v1".to_vec();
    let m1_start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m1], k1.clone(), m1_start_ts);
    let m1_commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], m1_start_ts, m1_commit_ts);
    // Rollback
    let mut m2 = Mutation::default();
    m2.set_op(Op::Put);
    m2.key = k1.clone();
    m2.value = b"v2".to_vec();
    let m2_start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m2], k1.clone(), m2_start_ts);
    suite.must_kv_rollback(1, vec![k1.clone()], m2_start_ts);
    // Update value
    let mut m3 = Mutation::default();
    m3.set_op(Op::Put);
    m3.key = k1.clone();
    m3.value = b"v3".to_vec();
    let m3_start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m3], k1.clone(), m3_start_ts);
    let m3_commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], m3_start_ts, m3_commit_ts);
    // Lock
    let mut m4 = Mutation::default();
    m4.set_op(Op::Lock);
    m4.key = k1.clone();
    let m4_start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m4], k1.clone(), m4_start_ts);
    let m4_commit_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_commit(1, vec![k1.clone()], m4_start_ts, m4_commit_ts);
    // Delete value
    let mut m5 = Mutation::default();
    m5.set_op(Op::Del);
    m5.key = k1.clone();
    let m5_start_ts = suite.cluster.pd_client.get_tso().wait().unwrap();
    suite.must_kv_prewrite(1, vec![m5], k1.clone(), m5_start_ts);

    let mut event_count = 0;
    loop {
        let events = receive_event(false);
        for event in events {
            match event.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for row in es.take_entries() {
                        if row.get_type() == EventLogType::Prewrite {
                            if row.get_start_ts() == m2_start_ts.into_inner() {
                                assert_eq!(row.get_previous_value(), b"v1");
                                event_count += 1;
                            } else if row.get_start_ts() == m3_start_ts.into_inner() {
                                assert_eq!(row.get_previous_value(), b"v1");
                                event_count += 1;
                            } else if row.get_start_ts() == m5_start_ts.into_inner() {
                                assert_eq!(row.get_previous_value(), b"v3");
                                event_count += 1;
                            }
                        }
                    }
                }
                Event_oneof_event::Error(e) => panic!("{:?}", e),
                Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
                Event_oneof_event::Admin(e) => panic!("{:?}", e),
            }
        }
        if event_count >= 3 {
            break;
        }
    }

    let (req_tx, resp_rx) = suite.get_region_cdc_client(1).event_feed().unwrap();
    event_feed_wrap.as_ref().replace(Some(resp_rx));
    let _req_tx = req_tx.send((req, WriteFlags::default())).wait().unwrap();
    let mut event_count = 0;
    loop {
        let events = receive_event(false);
        for event in events {
            match event.event.unwrap() {
                Event_oneof_event::Entries(mut es) => {
                    for row in es.take_entries() {
                        if row.get_type() == EventLogType::Committed
                            && row.get_start_ts() == m1_start_ts.into_inner()
                        {
                            assert_eq!(row.get_previous_value(), b"");
                            event_count += 1;
                        } else if row.get_type() == EventLogType::Committed
                            && row.get_start_ts() == m3_start_ts.into_inner()
                        {
                            assert_eq!(row.get_previous_value(), b"v1");
                            event_count += 1;
                        } else if row.get_type() == EventLogType::Prewrite
                            && row.get_start_ts() == m5_start_ts.into_inner()
                        {
                            assert_eq!(row.get_previous_value(), b"v3");
                            event_count += 1;
                        }
                    }
                }
                Event_oneof_event::Error(e) => panic!("{:?}", e),
                Event_oneof_event::ResolvedTs(e) => panic!("{:?}", e),
                Event_oneof_event::Admin(e) => panic!("{:?}", e),
            }
        }
        if event_count >= 3 {
            break;
        }
    }

    event_feed_wrap.as_ref().replace(None);
    suite.stop();
}
