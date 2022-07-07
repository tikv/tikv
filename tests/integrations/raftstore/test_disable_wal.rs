use test_raftstore::*;

#[test]
fn test_disable_wal() {
    let mut cluster = new_node_cluster(0, 1);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.start().unwrap();
    cluster.set_bootstrapped(1, 0);
    cluster.must_put(b"k0", b"v0");
    cluster.must_put(b"k1", b"v1");
    let _region = cluster.get_region(b"k0");
    // Make sure leader writes the data.
    must_get_equal(&cluster.get_engine(1), b"k0", b"v0");
    cluster.get_engine(1).flush(true).unwrap();
    cluster.must_put(b"k2", b"v2");
}
