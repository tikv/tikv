// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::{raw::DBStatisticsTickerType, RocksEngine};
use engine_traits::{MiscExt, CF_LOCK};
use test_raftstore::*;
use tikv_util::config::*;

fn flush<T: Simulator<RocksEngine>>(cluster: &mut Cluster<RocksEngine, T>) {
    for engines in cluster.engines.values() {
        engines.kv.flush_cf(CF_LOCK, true).unwrap();
    }
}

fn flush_then_check<T: Simulator<RocksEngine>>(
    cluster: &mut Cluster<RocksEngine, T>,
    interval: u64,
    written: bool,
) {
    flush(cluster);
    // Wait for compaction.
    sleep_ms(interval * 2);
    for statistics in &cluster.kv_statistics {
        let compact_write_bytes =
            statistics.get_ticker_count(DBStatisticsTickerType::CompactWriteBytes);
        if written {
            assert!(compact_write_bytes > 0);
        } else {
            assert_eq!(compact_write_bytes, 0);
        }
    }
}

fn test_compact_lock_cf<T: Simulator<RocksEngine>>(cluster: &mut Cluster<RocksEngine, T>) {
    let interval = 500;
    // Set lock_cf_compact_interval.
    cluster.cfg.raft_store.lock_cf_compact_interval = ReadableDuration::millis(interval);
    // Set lock_cf_compact_bytes_threshold.
    cluster.cfg.raft_store.lock_cf_compact_bytes_threshold = ReadableSize(100);
    cluster.cfg.rocksdb.lockcf.disable_auto_compactions = true;
    cluster.run();

    // Write 40 bytes, not reach lock_cf_compact_bytes_threshold, so there is no
    // compaction.
    for i in 0..5 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_cf(CF_LOCK, k.as_bytes(), v.as_bytes());
    }
    // Generate one sst, if there are datas only in one memtable, no compactions
    // will be triggered.
    flush(cluster);

    // Write more 40 bytes, still not reach lock_cf_compact_bytes_threshold,
    // so there is no compaction.
    for i in 5..10 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_cf(CF_LOCK, k.as_bytes(), v.as_bytes());
    }
    // Generate another sst.
    flush_then_check(cluster, interval, false);

    // Write more 50 bytes, reach lock_cf_compact_bytes_threshold.
    for i in 10..15 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_cf(CF_LOCK, k.as_bytes(), v.as_bytes());
    }
    flush_then_check(cluster, interval, true);
}

#[test]
fn test_server_compact_lock_cf() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_compact_lock_cf(&mut cluster);
}
