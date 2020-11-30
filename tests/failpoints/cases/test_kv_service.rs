// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::*;
use test_raftstore::must_new_cluster_and_kv_client;

#[test]
fn test_batch_get_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = BatchGetRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![b"a".to_vec(), b"b".to_vec()].into());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_batch_get(&req).unwrap();
    // the injected error should be returned at both places for backward compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}

#[test]
fn test_kv_scan_memory_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = ScanRequest::default();
    req.set_context(ctx);
    req.set_start_key(b"a".to_vec());
    req.version = 50;

    fail::cfg("raftkv_async_snapshot_err", "return").unwrap();
    let resp = client.kv_scan(&req).unwrap();
    // the injected error should be returned at both places for backward compatibility.
    assert!(!resp.pairs[0].get_error().get_abort().is_empty());
    assert!(!resp.get_error().get_abort().is_empty());
    fail::remove("raftkv_async_snapshot_err");
}
