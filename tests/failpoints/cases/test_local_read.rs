// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread};

use test_raftstore::{configure_for_lease_read, new_node_cluster};

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

    fail::cfg("before_execute_get_snapshot", "pause").unwrap();
    let cluster_clone = cluster.clone();
    let handle = thread::spawn(move || {

    })
}
