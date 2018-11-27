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

use rocksdb::Range;

use test_raftstore::*;
use tikv::raftstore::store::keys::{data_key, DATA_MAX_KEY};
use tikv::storage::mvcc::{Write, WriteType};
use tikv::storage::types::Key as MvccKey;
use tikv::storage::CF_WRITE;
use tikv::util::config::*;
use tikv::util::rocksdb::get_cf_handle;

fn gen_mvcc_put_kv(k: &[u8], v: &[u8], start_ts: u64, commit_ts: u64) -> (Vec<u8>, Vec<u8>) {
    let k = MvccKey::from_encoded(data_key(k));
    let k = k.append_ts(commit_ts);
    let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
    (k.as_encoded().clone(), w.to_bytes())
}

fn gen_delete_k(k: &[u8], commit_ts: u64) -> Vec<u8> {
    let k = MvccKey::from_encoded(data_key(k));
    let k = k.append_ts(commit_ts);
    k.as_encoded().clone()
}

fn test_compact_after_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.region_compact_check_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_compact_min_tombstones = 500;
    cluster.cfg.raft_store.region_compact_tombstones_percent = 50;
    cluster.cfg.raft_store.region_compact_check_step = 1;
    cluster.run();

    for i in 0..1000 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        let (k, v) = gen_mvcc_put_kv(k.as_bytes(), v.as_bytes(), 1, 2);
        cluster.must_put_cf(CF_WRITE, &k, &v);
    }
    for engines in cluster.engines.values() {
        let cf = get_cf_handle(&engines.kv, CF_WRITE).unwrap();
        engines.kv.flush_cf(cf, true).unwrap();
    }

    for i in 0..1000 {
        let k = format!("k{}", i);
        let k = gen_delete_k(k.as_bytes(), 2);
        cluster.must_delete_cf(CF_WRITE, &k);
    }
    for engines in cluster.engines.values() {
        let cf = get_cf_handle(&engines.kv, CF_WRITE).unwrap();
        engines.kv.flush_cf(cf, true).unwrap();
    }

    // wait for compaction.
    sleep_ms(300);

    for engines in cluster.engines.values() {
        let cf_handle = get_cf_handle(&engines.kv, CF_WRITE).unwrap();
        let approximate_size = engines
            .kv
            .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
        assert_eq!(approximate_size, 0);
    }
}

#[test]
fn test_node_compact_after_delete() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_compact_after_delete(&mut cluster);
}
