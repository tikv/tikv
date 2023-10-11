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

// The test mocks the senario before https://github.com/tikv/rocksdb/pull/347:
// note: before rocksdb/pull/347, lock is called before on_memtable_sealed.
// Case:
// Assume FlushMemtable cf1 (schedule flush task) and BackgroundCallFlush cf1
// (execute flush task) are performed concurrently.
// t        FlushMemtable cf1                   BackgroundCallFlush cf1
// 1.       lock
// 2.       convert memtable t2(seqno. 10-20)
//        to immemtable
// 3.       unlock
// 4.                                           lock
// 5.                                           pick memtables to flush:
//                                            t1(0-10), t2(10-20)
//                                            flush job(0-20)
// 6.                                           finish flush
// 7.                                           unlock
// 8.                                           on_flush_completed:
//                                            update last_flushed to 20
// 9.       on_memtable_sealed
//        10 > 20 *panic*
#[test]
fn test_rocksdb_listener() {
    use test_raftstore_v2::*;
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    // make flush thread num 1 to be easy to construct the case
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
    tablet1.flush_cf("default", false).unwrap(); // call flush 1
    std::thread::sleep(Duration::from_secs(1));

    tablet2.flush_cf("default", false).unwrap(); // call flush 2
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
        let _ = tx.lock().unwrap().send(true); // call flush 3
    })
    .unwrap();
    fail::remove("on_flush_begin");

    let _ = rx.recv(); // flush 1 done
    // Now, flush 1 has done, flush 3 is blocked at on_memtable_sealed.
    // Before https://github.com/tikv/rocksdb/pull/347, unlock will be called
    // before calling on_memtable_sealed, so flush 2 can pick the memtable sealed by
    // flush 3 and thus make the order chaos.
    // Now, unlock will not be called, so we have to remove failpoint to avoid
    // deadlock. 2 seconds is long enough to make the test failed before
    // rocksdb/pull/347.
    std::thread::sleep(Duration::from_secs(2));
    fail::remove("on_memtable_sealed");

    h.join().unwrap();
}
