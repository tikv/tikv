#[test]
fn test_partition_stale_read() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.put("k1", "v1");
    cluster.partition();
    cluster.get("k1");
}

#[test]
fn test_partition_write() {
    let mut cluster = new_node_cluster(0, 5);
    cluster.put("k1", "v1");
    cluster.partition();
    cluster.get("k1");
}
