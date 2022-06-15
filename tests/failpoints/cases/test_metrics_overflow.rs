// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use test_raftstore::*;

#[test]
fn test_memory_metrics_overflow() {
    let mut cluster = new_server_cluster(0, 3);
    // To ensure the thread has full store disk usage infomation.
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let fp_send = "memtrace_raft_messages_overflow_check_send";
    fail::cfg(fp_send, "sleep(1)").unwrap(); // 1s seems enough
    let fp_peer_recv = "memtrace_raft_messages_overflow_check_peer_recv";
    fail::cfg(fp_peer_recv, "panic").unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_get(b"k1");
    fail::remove(fp_send);
    fail::remove(fp_peer_recv);
}
