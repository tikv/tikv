// Copyright 2017 PingCAP, Inc.
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


use std::thread::*;
use std::time::*;

use fail;
use tikv::util::config::*;

use raftstore::node::new_node_cluster;
use raftstore::util::*;


#[test]
fn test_overlap_cleanup() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);

    let check_snapshot_fp = "tikv::raftstore::store::store::check_snapshot";

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");

    fail::cfg(check_snapshot_fp, "return(overlap cleanup)").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k2");

    // Stale snapshot should be deleted. And gc is disabled (60 secs), so
    // there should be at most two snapshots, one is not gc yet, the other is
    // a rescheduled snapshot.
    let snap_dir = cluster.get_snap_dir(3);
    for _ in 0..10 {
        let snapfiles: Vec<_> = fs::read_dir(snap_dir)
            .unwrap()
            .map(|p| p.unwrap().path())
            .collect();
        assert!(snapfiles.len() <= 3, "too many snaps: {:?}", snapfiles);
        thread::sleep(Duration::from_millis(100));
    }
}
