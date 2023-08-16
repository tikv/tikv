// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableSize;

#[test]
fn test_write_buffer_manager() {
    use test_raftstore_v2::*;
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.rocksdb.lock_write_buffer_limit = Some(ReadableSize::kb(1));
    cluster.cfg.rocksdb.write_buffer_limit = Some(ReadableSize::kb(1));
}
