// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use test_raftstore::{
    configure_for_lease_read, new_node_cluster, new_request, new_snap_cmd, Simulator,
};
use tikv_util::HandyRwLock;

#[test]
fn test_lease_expire_before_get_snapshot() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);

    let _ = configure_for_lease_read(&mut cluster, None, None);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    let key = b"k0";
    let value = b"v0";
    cluster.must_put(b"k0", b"v0");
    fail::cfg("localreader_before_redirect", "panic").unwrap();

    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // fail::cfg("before_execute_get_snapshot", "pause").unwrap();
    let cluster_clone = cluster.clone();

    let region = cluster.get_region(key);
    let region_id = region.id;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let mut req = new_request(
        region_id,
        region.get_region_epoch(),
        vec![new_snap_cmd()],
        false,
    );
    req.mut_header().set_peer(leader);

    let (tx, rx) = mpsc::channel();
    cluster.sim.wl().async_read(node_id, None, req, tx);

    let resp = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    // let handle = thread::spawn(move || {

    // })
}
