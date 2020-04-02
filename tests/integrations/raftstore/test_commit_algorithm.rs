// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use raft::eraftpb::ConfChangeType;
use std::sync::mpsc;
use test_raftstore::*;
use tikv_util::HandyRwLock;

fn prepare_cluster() -> Cluster<ServerCluster> {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.pd_client.configure_dr_auto_sync("zone");
    cluster.create_engines();
    cluster.bootstrap_region().unwrap();
    cluster
        .cfg
        .server
        .labels
        .insert("zone".to_owned(), "ES".to_owned());
    cluster.run_node(1).unwrap();
    cluster.run_node(2).unwrap();
    cluster
        .cfg
        .server
        .labels
        .insert("zone".to_owned(), "WN".to_owned());
    cluster.run_node(3).unwrap();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v0");
    cluster
}

/// After enabling `IntegrityOverFirstLabel`, data should be replicated to different labels
/// before committed.
#[test]
fn test_integrity_over_first_label() {
    let mut cluster = prepare_cluster();
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    let region = cluster.get_region(b"k1");
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", b"k1", b"v1")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    rx.recv_timeout(Duration::from_millis(100)).unwrap();
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    cluster.clear_send_filters();
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", b"k2", b"v2")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    must_get_none(&cluster.get_engine(1), b"k2");
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

/// Conf change should consider labels when IntegrityOverFirstLabel is chosen.
#[test]
fn test_check_conf_change() {
    let mut cluster = prepare_cluster();
    cluster.pd_client.must_remove_peer(1, new_peer(2, 2));
    must_get_none(&cluster.get_engine(2), b"k1");
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    cluster.pd_client.must_add_peer(1, new_learner_peer(2, 4));
    let region = cluster.get_region(b"k1");
    // Peer 2 can be promoted as there will be enough quorum alive.
    let cc = new_change_peer_request(ConfChangeType::AddNode, new_peer(2, 4));
    let req = new_admin_request(region.get_id(), region.get_region_epoch(), cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(!res.get_header().has_error(), "{:?}", res);
    must_get_none(&cluster.get_engine(2), b"k1");
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(2), b"k1", b"v0");

    cluster.pd_client.must_remove_peer(1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    cluster.pd_client.must_add_peer(1, new_learner_peer(3, 5));
    let region = cluster.get_region(b"k1");
    // Peer 2 can be promoted as there will be enough quorum alive.
    let cc = new_change_peer_request(ConfChangeType::AddNode, new_peer(3, 5));
    let req = new_admin_request(region.get_id(), region.get_region_epoch(), cc);
    let res = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(
        res.get_header()
            .get_error()
            .get_message()
            .contains("unsafe to perform conf change peer"),
        "{:?}",
        res
    );
}
