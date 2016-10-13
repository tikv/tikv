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
use tikv::util::rocksdb::get_cf_handle;
use rocksdb::Range;

use super::util::*;
use super::cluster::{Cluster, Simulator};
use super::server::new_server_cluster;

fn test_compact_lock_cf<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.lock_cf_compact_interval_secs = 1;
    cluster.run();

    for i in 1..9 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        cluster.must_put_cf(CF_LOCK, k.as_bytes(), v.as_bytes());
    }
    for i in 1..9 {
        let k = format!("k{}", i);
        cluster.must_delete_cf(CF_LOCK, k.as_bytes());
    }

    // wait for compacting cf-lock
    sleep_ms(2000);

    for engine in cluster.engines.values() {
        let cf_handle = get_cf_handle(engine, CF_LOCK).unwrap();
        let approximate_size =
            engine.get_approximate_sizes_cf(cf_handle, &[Range::new(b"", b"k9")])[0];
        assert_eq!(approximate_size, 0);
    }
}

#[test]
fn test_server_compact_lock_cf() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_compact_lock_cf(&mut cluster);
}
