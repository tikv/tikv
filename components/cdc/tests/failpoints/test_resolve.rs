// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::time::Duration;

use futures::{executor::block_on, sink::SinkExt};
use grpcio::WriteFlags;
use kvproto::{cdcpb::*, kvrpcpb::*};
use pd_client::PdClient;
use raft::eraftpb::ConfChangeType;
use test_raftstore::*;
use tikv_util::config::*;

use crate::{new_event_feed, TestSuite, TestSuiteBuilder};

#[test]
fn test_stale_resolver() {
    let mut suite = TestSuite::new(3);

    let fp = "before_schedule_resolver_ready";
    fail::cfg(fp, "pause").unwrap();

    // Close previous connection and open a new one twice time
    let region = suite.cluster.get_region(&[]);
    let req = suite.new_changedata_request(region.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.get_id()));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    // Sleep for a while to wait the scan is done
    sleep_ms(200);

    let (k, v) = ("key1".to_owned(), "value".to_owned());
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

    // Block next scan
    let fp1 = "cdc_incremental_scan_start";
    fail::cfg(fp1, "pause").unwrap();
    // Close previous connection and open two new connections
    let (mut req_tx, resp_rx) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    event_feed_wrap.replace(Some(resp_rx));
    block_on(req_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    let (mut req_tx1, resp_rx1) = suite
        .get_region_cdc_client(region.get_id())
        .event_feed()
        .unwrap();
    block_on(req_tx1.send((req, WriteFlags::default()))).unwrap();
    // Unblock the first scan
    fail::remove(fp);
    // Sleep for a while to wait the wrong resolver init
    sleep_ms(100);
    // Async commit
    let commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let commit_resp =
        suite.async_kv_commit(region.get_id(), vec![k.into_bytes()], start_ts, commit_ts);
    // Receive Commit response
    block_on(commit_resp).unwrap();
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

    event_feed_wrap.replace(Some(resp_rx1));
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
                    panic!("unexpected event length {:?}", es);
                }
            },
            Event_oneof_event::Error(e) => panic!("{:?}", e),
            other => panic!("unknown event {:?}", other),
        }
    }

    event_feed_wrap.replace(None);
    suite.stop();
}

// Resolved ts can still advance even if some regions are merged (it drops
// callback that is used to advance resolved ts).
#[test]
fn test_region_error() {
    let mut cluster = new_server_cluster(1, 1);
    cluster.cfg.cdc.min_ts_interval = ReadableDuration::millis(100);
    let mut suite = TestSuiteBuilder::new().cluster(cluster).build();

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
    let (mut source_tx, source_wrap, _source_event) =
        new_event_feed(suite.get_region_cdc_client(source.get_id()));
    block_on(source_tx.send((req.clone(), WriteFlags::default()))).unwrap();
    // Subscribe target region
    let target = suite.cluster.get_region(b"k2");
    req.region_id = target.get_id();
    req.set_region_epoch(target.get_region_epoch().clone());
    let (mut target_tx, target_wrap, target_event) =
        new_event_feed(suite.get_region_cdc_client(target.get_id()));
    block_on(target_tx.send((req, WriteFlags::default()))).unwrap();
    sleep_ms(200);

    suite
        .cluster
        .must_try_merge(source.get_id(), target.get_id());
    sleep_ms(200);

    let mut last_resolved_ts = 0;
    for _ in 0..5 {
        let event = target_event(true);
        if let Some(resolved_ts) = event.resolved_ts.as_ref() {
            let ts = resolved_ts.ts;
            assert!(ts > last_resolved_ts);
            last_resolved_ts = ts;
        }
    }
    fail::remove(multi_batch_fp);
    fail::remove(deregister_fp);

    source_wrap.replace(None);
    target_wrap.replace(None);
    suite.stop();
}

#[test]
fn test_joint_confchange() {
    let mut cluster = new_server_cluster(1, 3);
    cluster.cfg.cdc.min_ts_interval = ReadableDuration::millis(100);
    cluster.cfg.cdc.hibernate_regions_compatible = true;
    let mut suite = TestSuiteBuilder::new().cluster(cluster).build();

    let receive_resolved_ts = |receive_event: &(dyn Fn(bool) -> ChangeDataEvent + Send)| {
        let mut last_resolved_ts = 0;
        let mut i = 0;
        loop {
            let event = receive_event(true);
            if let Some(resolved_ts) = event.resolved_ts.as_ref() {
                let ts = resolved_ts.ts;
                assert!(ts >= last_resolved_ts);
                last_resolved_ts = ts;
                i += 1;
            }
            if i > 10 {
                break;
            }
        }
    };

    let deregister_fp = "cdc_before_handle_deregister";
    fail::cfg(deregister_fp, "return").unwrap();

    suite.cluster.must_put(b"k1", b"v1");
    (1..=3).for_each(|i| must_get_equal(&suite.cluster.get_engine(i), b"k1", b"v1"));

    let region = suite.cluster.get_region(b"k1");
    let peers = region.get_peers();
    assert_eq!(peers.len(), 3);
    suite
        .cluster
        .must_transfer_leader(region.get_id(), peers[0].clone());

    let req = suite.new_changedata_request(region.get_id());
    let (mut req_tx, event_feed_wrap, receive_event) =
        new_event_feed(suite.get_region_cdc_client(region.get_id()));
    block_on(req_tx.send((req, WriteFlags::default()))).unwrap();
    receive_resolved_ts(&receive_event);

    suite.cluster.stop_node(peers[1].get_store_id());
    receive_resolved_ts(&receive_event);
    suite.cluster.run_node(peers[1].get_store_id()).unwrap();

    let confchanges = vec![(
        ConfChangeType::AddLearnerNode,
        new_learner_peer(peers[2].store_id, peers[2].id),
    )];
    suite
        .cluster
        .pd_client
        .must_joint_confchange(region.get_id(), confchanges);
    receive_resolved_ts(&receive_event);

    suite.cluster.stop_node(peers[1].get_store_id());
    let update_region_fp = "change_peer_after_update_region";
    fail::cfg(update_region_fp, "pause").unwrap();
    let confchanges = vec![
        (
            ConfChangeType::AddLearnerNode,
            new_learner_peer(peers[1].store_id, peers[1].id),
        ),
        (ConfChangeType::AddNode, peers[2].clone()),
    ];
    suite
        .cluster
        .pd_client
        .joint_confchange(region.get_id(), confchanges);
    sleep_ms(500);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        receive_resolved_ts(&receive_event);
        tx.send(()).unwrap();
    });
    assert!(rx.recv_timeout(Duration::from_secs(2)).is_err());

    fail::remove(update_region_fp);
    fail::remove(deregister_fp);

    event_feed_wrap.replace(None);
    suite.stop();
}
