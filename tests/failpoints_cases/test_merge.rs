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


use std::time::*;
use std::thread;

use fail;
use kvproto::metapb::MergeDirection;
use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use tikv::util::config::*;
use tikv::pd::PdClient;
use tikv::raftstore::store::keys;
use tikv::storage::CF_RAFT;
use tikv::raftstore::store::Peekable;

use raftstore::node::new_node_cluster;
use raftstore::util;
use raftstore::util::*;

#[test]
fn test_node_merge_rollback() {
    ::util::init_log();
    let _guard = ::setup();
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.merge_check_tick_interval = ReadableDuration::millis(100);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_rule();

    cluster.run_conf_change();

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    pd_client.must_add_peer(left.get_id(), new_peer(2, 2));
    pd_client.must_add_peer(right.get_id(), new_peer(2, 4));

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    let epoch = region.get_region_epoch();

    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();

    let pre_merge = util::new_pre_merge(MergeDirection::Forward);
    let req = util::new_admin_request(region.get_id(), &epoch, pre_merge);
    // The callback will be called when pre-merge is applied.
    let res = cluster.call_command_on_leader(req, Duration::from_secs(3));
    assert!(res.is_ok(), "{:?}", res);
    
    // Add a peer to trigger rollback.
    pd_client.must_add_peer(right.get_id(), new_peer(3, 5));
    cluster.must_put(b"k4", b"v4");
    util::must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
    
    let region = pd_client.get_region(b"k1").unwrap();
    // After split and pre-merge, version becomes 1 + 2 = 3;
    assert_eq!(region.get_region_epoch().get_version(), 3);
    // After ConfChange and pre-merge, conf version becomes 1 + 2 = 3;
    assert_eq!(region.get_region_epoch().get_conf_ver(), 3);
    fail::remove(schedule_merge_fp);
    // Wait till rollback.
    let timer = Instant::now();
    loop {
        let resp = cluster.request(
            b"k11",
            vec![new_put_cmd(b"k11", b"v11")],
            false,
            Duration::from_secs(5),
        );
        if !resp.get_header().has_error() {
            break;
        }
        if !resp.get_header().get_error().get_message().contains("read only") {
            panic!("response {:?} has error", resp);
        }
        if timer.elapsed() > Duration::from_secs(5) {
            panic!("region still in read only mode after 5 secs");
        }
        thread::sleep(Duration::from_millis(100));
    }

    for i in 1..3 {
        let state_key = keys::region_state_key(region.get_id());
        let state: RegionLocalState = cluster
                .get_engine(i)
                .get_msg_cf(CF_RAFT, &state_key)
                .unwrap()
                .unwrap();
        assert_eq!(state.get_state(), PeerState::Normal);
        assert_eq!(*state.get_region(), region);
    }
}
