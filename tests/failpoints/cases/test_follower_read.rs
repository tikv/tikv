use std::sync::Arc;

use fail;

use test_raftstore::*;

#[test]
fn test_wait_for_apply_index() {
    let mut cluster = new_node_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k0", b"v0");
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    let r1 = cluster.get_region(b"k1");
    // Ensure peer 3 is not leader to use fail point 'on_raft_ready'
    if cluster.leader_of_region(r1.get_id()) == Some(p3) {
        cluster.must_transfer_leader(r1.get_id(), p2);
    }

    fail::cfg("on_raft_ready", "pause").unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");
    cluster.must_put(b"k4", b"v4");
    cluster.must_put(b"k5", b"v5");
    fail::cfg("on_raft_ready", "off").unwrap();
    // Peer 3 does not apply the value of 'k5' right now
    must_get_none(&cluster.get_engine(3), b"k5");
    assert_eq!(cluster.must_get(b"k5").unwrap(), b"v5");
}
