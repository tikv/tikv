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

use std::*;
use std::sync::Arc;
use std::time::*;

use fail;
use tikv::util::config::*;

use raftstore::node::new_node_cluster;
use raftstore::util::*;

#[test]
fn test_overlap_cleanup() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);

    let gen_snapshot_fp = "region_gen_snap";

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    let region_id = cluster.run_conf_change();
    pd_client.must_add_peer(region_id, new_peer(2, 2));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // This will only pause the bootstrapped region, so the split region
    // can still work as expected.
    fail::cfg(gen_snapshot_fp, "pause").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.must_put(b"k3", b"v3");
    let region1 = cluster.get_region(b"k1");
    cluster.must_split(&region1, b"k2");
    // Wait till the snapshot of split region is applied, whose range is ["", "k2").
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    // Resume the fail point and pause it again. So only the paused snapshot is generated.
    // And the paused snapshot's range is ["", ""), hence overlap.
    fail::cfg(gen_snapshot_fp, "pause").unwrap();
    // Wait a little bit for the message being sent out.
    thread::sleep(Duration::from_secs(1));
    // Overlap snapshot should be deleted.
    let snap_dir = cluster.get_snap_dir(3);
    for p in fs::read_dir(&snap_dir).unwrap() {
        let name = p.unwrap().file_name().into_string().unwrap();
        let mut parts = name.split('_');
        parts.next();
        if parts.next().unwrap() == "1" {
            panic!("snapshot of region 1 should be deleted.");
        }
    }
    fail::remove(gen_snapshot_fp);
}

#[test]
fn test_snapshot_between_save() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_rule();

    let region_id = cluster.run_conf_change();

    pd_client.must_add_peer(region_id, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Add peer on store 3 so that it will be shutdown by fail point.
    fail::cfg("raft_snapshot_between_save", "return").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    cluster.stop_node(3);

    // Reset fail point and restart store 3.
    fail::cfg("raft_snapshot_between_save", "off").unwrap();
    cluster.run_node(3);

    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    fail::remove("raft_snapshot_between_save");
}

#[test]
fn test_snapshot_apply_validate_fail() {
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_rule();

    let region_id = cluster.run_conf_change();

    pd_client.must_add_peer(region_id, new_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    fail::cfg("raft_snapshot_validate", "return").unwrap();
    pd_client.must_add_peer(region_id, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");

    fail::cfg("raft_snapshot_validate", "off").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    fail::remove("raft_snapshot_validate");
}
