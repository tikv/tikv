// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_LOCK, CF_WRITE};
use tikv_util::config::ReadableSize;

fn dummy_string(len: usize) -> String {
    String::from_utf8(vec![0; len]).unwrap()
}

#[test]
fn test_write_buffer_manager() {
    use test_raftstore_v2::*;
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.rocksdb.lock_write_buffer_limit = Some(ReadableSize::kb(10));
    cluster.cfg.rocksdb.write_buffer_limit = Some(ReadableSize::kb(20));

    // Let write buffer size small to make memtable request fewer memories.
    // Otherwise, one single memory request can exceeds the write buffer limit set
    // above.
    cluster.cfg.rocksdb.lockcf.write_buffer_size = Some(ReadableSize::kb(64));
    cluster.cfg.rocksdb.writecf.write_buffer_size = Some(ReadableSize::kb(64));
    cluster.cfg.rocksdb.defaultcf.write_buffer_size = Some(ReadableSize::kb(64));
    cluster.run();

    let dummy = dummy_string(500);
    let fp = "on_memtable_sealed";
    fail::cfg(fp, "return(lock)").unwrap();

    for i in 0..10 {
        let key = format!("key-{:03}", i);
        for cf in &[CF_WRITE, CF_LOCK] {
            cluster.must_put_cf(cf, key.as_bytes(), dummy.as_bytes());
        }
    }

    fail::cfg(fp, "return(write)").unwrap();
    let dummy = dummy_string(1000);
    for i in 0..10 {
        let key = format!("key-{:03}", i);
        cluster.must_put_cf(CF_WRITE, key.as_bytes(), dummy.as_bytes());
    }
}
