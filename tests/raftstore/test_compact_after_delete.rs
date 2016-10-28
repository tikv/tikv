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


use tikv::storage::{CF_DEFAULT, CF_WRITE};
use tikv::util::rocksdb::get_cf_handle;
use rocksdb::Range;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;

fn test_compact_after_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.region_compact_check_tick_interval = 500;
    cluster.cfg.raft_store.region_compact_delete_keys_count = 5;
    cluster.run();

    for i in 1..9 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_cf(CF_DEFAULT, k.as_bytes(), v.as_bytes());
        cluster.must_put_cf(CF_WRITE, k.as_bytes(), v.as_bytes());
        cluster.must_delete_cf(CF_DEFAULT, k.as_bytes());
        cluster.must_delete_cf(CF_WRITE, k.as_bytes());
    }

    // wait for compaction.
    sleep_ms(1000);

    for engine in cluster.engines.values() {
        let cf_handle = get_cf_handle(engine, CF_DEFAULT).unwrap();
        let approximate_size =
            engine.get_approximate_sizes_cf(cf_handle, &[Range::new(b"", b"k9")])[0];
        assert_eq!(approximate_size, 0);

        let cf_handle = get_cf_handle(engine, CF_WRITE).unwrap();
        let approximate_size =
            engine.get_approximate_sizes_cf(cf_handle, &[Range::new(b"", b"k9")])[0];
        assert_eq!(approximate_size, 0);
    }
}

#[test]
fn test_server_compact_after_delete() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_compact_after_delete(&mut cluster);
}

#[test]
fn test_node_compact_after_delete() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_compact_after_delete(&mut cluster);
}
