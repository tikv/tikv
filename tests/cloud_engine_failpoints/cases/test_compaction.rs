// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use test_cloud_server::{must_wait, ServerCluster};
use tikv_util::config::ReadableSize;

use super::{i_to_key, i_to_val};

#[test]
fn test_retry_failed_flush() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(vec![1], |_, cfg| {
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
    });

    let flush_fp = "kvengine_flush_normal";
    let mut client = cluster.new_client();
    let region_id = client.get_region_id(&[]);
    fail::cfg(flush_fp, "return").unwrap();
    client.put_kv(0..100, i_to_key, i_to_val);
    client.put_kv(100..200, i_to_key, i_to_val);
    thread::sleep(Duration::from_secs(1));
    assert_eq!(
        cluster
            .get_kvengine(1)
            .get_shard_stat(region_id)
            .l0_table_count,
        0
    );
    fail::remove(flush_fp);
    must_wait(
        || {
            cluster
                .get_kvengine(1)
                .get_shard_stat(region_id)
                .l0_table_count
                > 0
        },
        3,
        "failed to flush memtables in time",
    );
    cluster.stop();
}
