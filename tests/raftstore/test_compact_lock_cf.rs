// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use tikv::storage::CF_LOCK;
use rocksdb::DBStatisticsTickerType;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::server::new_server_cluster;

fn flush_then_check<T: Simulator>(cluster: &mut Cluster<T>, interval: u64, written: bool) {
    for engine in cluster.engines.values() {
        let lock_handle = engine.cf_handle(CF_LOCK).unwrap();
        engine.flush_cf(lock_handle, true).unwrap();
    }
    // Wait for compaction.
    sleep_ms(interval * 2);
    for engine in cluster.engines.values() {
        let compact_write_bytes =
            engine.get_statistics_ticker_count(DBStatisticsTickerType::CompactWriteBytes);
        if written {
            assert!(compact_write_bytes > 0);
        } else {
            assert_eq!(compact_write_bytes, 0);
        }
    }
}

fn test_compact_lock_cf<T: Simulator>(cluster: &mut Cluster<T>) {
    let interval = 500;
    // set lock_cf_compact_interval.
    cluster.cfg.raft_store.lock_cf_compact_interval = interval;
    // set lock_cf_compact_threshold.
    cluster.cfg.raft_store.lock_cf_compact_threshold = 100;
    cluster.run();

    // not reach lock_cf_compact_threshold, so there is no compaction.
    for i in 0..50 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_cf(CF_LOCK, k.as_bytes(), v.as_bytes());
    }
    flush_then_check(cluster, interval, false);

    // not reach lock_cf_compact_threshold, so there is no compaction.
    for i in 0..50 {
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
