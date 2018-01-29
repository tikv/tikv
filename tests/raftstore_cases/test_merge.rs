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

use std::time::Duration;
use std::sync::Arc;
use std::thread;

use kvproto::metapb;
use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use tikv::pd::PdClient;
use tikv::raftstore::store::keys;
use tikv::storage::CF_RAFT;
use tikv::raftstore::store::Peekable;

use super::node::new_node_cluster;
use super::util;
use raftstore::util::*;

#[test]
fn test_node_base_merge() {
    let mut cluster = new_node_cluster(0, 3);

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(left.get_end_key(), right.get_start_key());
    assert_eq!(right.get_start_key(), b"k2");
    let get = util::new_request(
        right.get_id(),
        right.get_region_epoch().clone(),
        vec![util::new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header().get_error().has_key_not_in_region(),
        "{:?}",
        resp
    );

    pd_client.must_merge(left.get_id(), metapb::MergeDirection::Forward);

    let region = pd_client.get_region(b"k1").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    let orgin_epoch = left.get_region_epoch();
    let new_epoch = region.get_region_epoch();
    // PreMerge + Merge, so it should be 2.
    assert_eq!(new_epoch.get_version(), orgin_epoch.get_version() + 2);
    assert_eq!(new_epoch.get_conf_ver(), orgin_epoch.get_conf_ver());
    let get = util::new_request(
        region.get_id(),
        new_epoch.to_owned(),
        vec![util::new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");

    let version = left.get_region_epoch().get_version();
    let conf_ver = left.get_region_epoch().get_conf_ver();
    'outer: for i in 1..4 {
        let state_key = keys::region_state_key(left.get_id());
        let mut state = RegionLocalState::default();
        for _ in 0..3 {
            state = cluster
                .get_engine(i)
                .get_msg_cf(CF_RAFT, &state_key)
                .unwrap()
                .unwrap();
            if state.get_state() == PeerState::Tombstone {
                let region = state.get_region();
                if region.get_region_epoch().get_version() == version + 1 {
                    assert_eq!(region.get_region_epoch().get_conf_ver(), conf_ver + 1);
                }
                continue 'outer;
            }
            thread::sleep(Duration::from_millis(500));
        }
        panic!("store {} is still not merged: {:?}", i, state);
    }

    cluster.must_put(b"k4", b"v4");
}

#[test]
fn test_node_merge_with_admin_entries() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_rule();

    cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    pd_client.must_add_peer(left.get_id(), new_peer(2, 2));
    pd_client.must_add_peer(right.get_id(), new_peer(2, 4));

    pd_client.must_merge(left.get_id(), metapb::MergeDirection::Forward);

    let region = pd_client.get_region(b"k1").unwrap();
    assert_eq!(region.get_id(), right.get_id());
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    let new_epoch = region.get_region_epoch();
    let get = util::new_request(
        region.get_id(),
        new_epoch.to_owned(),
        vec![util::new_get_cmd(b"k1")],
        false,
    );
    debug!("requesting {:?}", get);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v1");
}
