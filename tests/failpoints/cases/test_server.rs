// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use test_raftstore::*;

#[test]
fn test_send_raft_channel_full() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    for id in 1..=3 {
        must_get_equal(&cluster.get_engine(id), b"k1", b"v1");
    }

    let send_raft_message_full_fp = "send_raft_message_full";
    let on_batch_raft_stream_drop_by_err_fp = "on_batch_raft_stream_drop_by_err";
    fail::cfg(send_raft_message_full_fp, "return").unwrap();
    fail::cfg(on_batch_raft_stream_drop_by_err_fp, "panic").unwrap();

    // send request while channel full should not cause the connection drop
    cluster.async_put(b"k2", b"v2").unwrap();

    fail::remove(send_raft_message_full_fp);
    cluster.must_put(b"k3", b"v3");
    for id in 1..=3 {
        must_get_equal(&cluster.get_engine(id), b"k3", b"v3");
    }
    fail::remove(on_batch_raft_stream_drop_by_err_fp);
}
