// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::channel, Mutex},
    time::Duration,
};

use engine_traits::{MiscExt, CF_DEFAULT, CF_LOCK, CF_WRITE};
use tikv_util::config::ReadableSize;

fn dummy_string(len: usize) -> String {
    String::from_utf8(vec![0; len]).unwrap()
}

#[test]
fn test_write_buffer_manager() {
    use test_raftstore_v2::*;
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.rocksdb.lockcf.write_buffer_limit = Some(ReadableSize::kb(10));
    cluster.cfg.rocksdb.defaultcf.write_buffer_limit = Some(ReadableSize::kb(10));
    cluster.cfg.rocksdb.write_buffer_limit = Some(ReadableSize::kb(30));

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

    fail::cfg(fp, "return(default)").unwrap();

    for i in 0..10 {
        let key = format!("key-{:03}", i);
        for cf in &[CF_WRITE, CF_DEFAULT] {
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

#[test]
fn test_rocksdb_listener() {
    use test_raftstore_v2::*;
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.rocksdb.max_background_flushes = 1;
    cluster.run();

    let r = cluster.get_region(b"k10");
    cluster.must_split(&r, b"k10");

    for i in 0..20 {
        let k = format!("k{:02}", i);
        cluster.must_put(k.as_bytes(), b"val");
    }

    let r1 = cluster.get_region(b"k00").get_id();
    let r2 = cluster.get_region(b"k15").get_id();

    let engine = cluster.get_engine(1);
    let tablet1 = engine.get_tablet_by_id(r1).unwrap();
    let tablet2 = engine.get_tablet_by_id(r2).unwrap();

    fail::cfg("on_flush_begin", "1*pause").unwrap();
    tablet1.flush_cf("default", false).unwrap();
    std::thread::sleep(Duration::from_secs(1));

    tablet2.flush_cf("default", false).unwrap();
    for i in 20..30 {
        let k = format!("k{:02}", i);
        cluster.must_put(k.as_bytes(), b"val");
    }
    fail::cfg("on_memtable_sealed", "pause").unwrap();

    let h = std::thread::spawn(move || {
        tablet2.flush_cf("default", true).unwrap();
    });

    let (tx, rx) = channel();
    let tx = Mutex::new(tx);
    fail::cfg_callback("on_flush_completed", move || {
        let _ = tx.lock().unwrap().send(true);
    })
    .unwrap();
    fail::remove("on_flush_begin");

    let _ = rx.recv();
    let _ = rx.recv();
    fail::remove("on_memtable_sealed");

    h.join().unwrap();
}
