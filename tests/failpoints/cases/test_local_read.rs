// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use test_raftstore::{
    configure_for_lease_read, make_cb, new_node_cluster, new_request, new_snap_cmd, Simulator,
};
use tikv_util::HandyRwLock;

// The test mock the situation that just before acquiring the snapshot, the
// lease expires.
#[test]
fn test_lease_expire_before_get_snapshot() {
    let mut cluster = new_node_cluster(0, 1);

    let _ = configure_for_lease_read(&mut cluster, None, None);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let key = b"k0";
    let value = b"v0";
    cluster.must_put(key, value);
    fail::cfg("localreader_before_redirect", "panic").unwrap();

    // Lease read works correctly
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    fail::cfg("localreader_before_redirect", "return").unwrap();
    fail::cfg("before_execute_get_snapshot", "pause").unwrap();

    let region = cluster.get_region(key);
    let region_id = region.id;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let node_id = leader.get_store_id();
    let mut req = new_request(
        region_id,
        region.get_region_epoch().clone(),
        vec![new_snap_cmd()],
        false,
    );
    req.mut_header().set_peer(leader);

    let (cb, rx) = make_cb(&req);
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        // This is equivalent to expiring the lease
        fail::cfg("local_reader_fail_lease_check", "return").unwrap();
        fail::cfg("before_execute_get_snapshot", "off").unwrap();
    });
    cluster.sim.wl().async_read(node_id, None, req, cb);

    // Request should be redirected to the raftstore, but the failpoint above makes
    // it just return. So cb will not be called.
    let _ = rx.recv_timeout(Duration::from_secs(5)).unwrap_err();

    handle.join().unwrap();
}
