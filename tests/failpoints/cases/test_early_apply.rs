// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::*;

use fail;
use futures::Future;

use pd_client::PdClient;
use test_raftstore::*;

/// Test whether system can recover from mismatched raft state and apply state.
///
/// If TiKV is not shutdown gracefully, apply state may go ahead of raft
/// state. TiKV should be able to recognize the situation and start normally.
#[test]
fn test_leader_early_apply() {
    let _guard = crate::setup();

    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    fail::cfg("handle_raft_ready_node_1_skip_write", "return()").unwrap();

    let epoch = cluster
        .pd_client
        .get_region_by_id(1)
        .wait()
        .unwrap()
        .unwrap()
        .take_region_epoch();

    let put = new_put_cmd(b"k2", b"v2");
    let mut req = new_request(1, epoch, vec![put], true);
    req.mut_header().set_peer(new_peer(1, 1));
    // Ignore error, we just want to send this command to peer (1, 1),
    let _ = cluster.call_command(req.clone(), Duration::from_millis(1));
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    cluster.stop_node(1);

    fail::remove("handle_raft_ready_node_1_skip_write");
    cluster.run_node(1).unwrap();
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}

#[test]
fn test_follower_early_apply() {
    let _guard = crate::setup();

    let mut cluster = new_node_cluster(0, 3);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    fail::cfg("handle_raft_ready_node_1_skip_write", "return()").unwrap();

    let epoch = cluster
        .pd_client
        .get_region_by_id(1)
        .wait()
        .unwrap()
        .unwrap()
        .take_region_epoch();

    let put = new_put_cmd(b"k2", b"v2");
    let mut req = new_request(1, epoch, vec![put], true);
    req.mut_header().set_peer(new_peer(3, 3));
    // Ignore error, we just want to send this command to peer (1, 1),
    let _ = cluster.call_command(req.clone(), Duration::from_millis(1));
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    cluster.stop_node(1);

    fail::remove("handle_raft_ready_node_1_skip_write");
    cluster.run_node(1).unwrap();
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}
