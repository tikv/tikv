// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::*;
use test_raftstore::*;
use tikv_util::config::*;

#[test]
fn test_bucket_stats() {
    let (mut cluster, client, ctx) = must_new_and_configure_cluster_and_kv_client(|cluster| {
        cluster.cfg.coprocessor.enable_region_bucket = true;
        cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::days(1);
        cluster.cfg.raft_store.report_region_buckets_tick_interval = ReadableDuration::millis(100);
    });

    let fp = "mock_tick_interval";
    fail::cfg(fp, "return(0)").unwrap();

    sleep_ms(200);
    let mut keys = Vec::with_capacity(50);
    for i in 0..50u8 {
        let key = vec![b'k', i];
        cluster.must_put(&key, &[b' '; 4]);
        cluster.must_get(&[b'k', i]);
        keys.push(key);
    }
    let mut req = RawBatchGetRequest::default();
    req.set_context(ctx);
    req.set_keys(protobuf::RepeatedField::from(keys));
    client.raw_batch_get(&req).unwrap();
    sleep_ms(600);
    let buckets = cluster.must_get_buckets(1);
    assert_eq!(buckets.meta.keys.len(), 2);
    assert_eq!(buckets.stats.get_write_keys(), [50]);
    assert_eq!(buckets.stats.get_write_bytes(), [50 * (4 + 2)]);
    assert_eq!(buckets.stats.get_read_keys(), [50]);
    assert_eq!(buckets.stats.get_read_bytes(), [50 * (4 + 2)]);
    fail::remove(fp);
}
