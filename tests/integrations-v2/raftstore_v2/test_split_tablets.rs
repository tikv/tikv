// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    sync::{mpsc::channel, Arc},
    thread,
    time::Duration,
};

use engine_traits::{Peekable, CF_DEFAULT, CF_WRITE};
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{AdminCmdType, AdminRequest, BatchSplitRequest, SplitRequest},
    raft_serverpb::RaftMessage,
};
use pd_client::PdClient;
use raft::prelude::MessageType;
use raftstore::{
    store::{Bucket, BucketRange, Callback, WriteResponse},
    Result,
};
use test_raftstore::{
    find_peer, must_get_equal, must_get_none, new_admin_request, new_get_cmd, new_peer,
    new_request, sleep_ms, CloneFilterFactory, Filter, RegionPacketFilter,
};
use test_raftstore_v2::{new_node_cluster, put_cf_till_size, put_till_size, Cluster, Simulator};
use tikv::storage::{kv::SnapshotExt, Snapshot};
use tikv_util::config::{ReadableDuration, ReadableSize};
use txn_types::{Key, PessimisticLock};

pub const REGION_MAX_SIZE: u64 = 50000;
pub const REGION_SPLIT_SIZE: u64 = 30000;

#[test]
fn test_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

fn test_auto_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.coprocessor.region_max_size = Some(ReadableSize(REGION_MAX_SIZE));
    cluster.cfg.coprocessor.region_split_size = ReadableSize(REGION_SPLIT_SIZE);

    let check_size_diff = cluster.cfg.raft_store.region_split_check_diff().0;
    let mut range = 1..;

    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);

    let region = pd_client.get_region(b"").unwrap();

    let last_key = put_till_size(cluster, REGION_SPLIT_SIZE, &mut range);

    // it should be finished in millis if split.
    thread::sleep(Duration::from_millis(300));

    let target = pd_client.get_region(&last_key).unwrap();

    assert_eq!(region, target);

    let max_key = put_cf_till_size(
        cluster,
        CF_WRITE,
        REGION_MAX_SIZE - REGION_SPLIT_SIZE + check_size_diff,
        &mut range,
    );

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    if left == right {
        cluster.wait_region_split(&region);
    }

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();

    assert_ne!(left, right);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(right.get_start_key(), left.get_end_key());
    assert_eq!(region.get_end_key(), right.get_end_key());
    assert_eq!(pd_client.get_region(&max_key).unwrap(), right);
    assert_eq!(pd_client.get_region(left.get_end_key()).unwrap(), right);

    let middle_key = left.get_end_key();
    let leader = cluster.leader_of_region(left.get_id()).unwrap();
    let store_id = leader.get_store_id();
    let mut size = 0;

    let region_ids = cluster.region_ids(store_id);
    for id in region_ids {
        cluster
            .scan(store_id, id, CF_DEFAULT, b"", middle_key, false, |k, v| {
                size += k.len() as u64;
                size += v.len() as u64;
                Ok(true)
            })
            .expect("");
    }

    assert!(size <= REGION_SPLIT_SIZE);
    // although size may be smaller than REGION_SPLIT_SIZE, but the diff should
    // be small.
    // assert!(size > REGION_SPLIT_SIZE - 1000);

    let epoch = left.get_region_epoch().clone();
    let get = new_request(left.get_id(), epoch, vec![new_get_cmd(&max_key)], false);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_quick_election_after_split() {
    let mut cluster = new_node_cluster(0, 3);
    test_quick_election_after_split(&mut cluster);
}

// For the peer which is the leader of the region before split, it should
// campaigns immediately. and then this peer may take the leadership
// earlier. `test_quick_election_after_split` is a helper function for testing
// this feature.
fn test_quick_election_after_split<T: Simulator>(cluster: &mut Cluster<T>) {
    // Calculate the reserved time before a new campaign after split.
    let reserved_time =
        Duration::from_millis(cluster.cfg.raft_store.raft_base_tick_interval.as_millis() * 2);

    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let region = cluster.get_region(b"k1");
    let old_leader = cluster.leader_of_region(region.get_id()).unwrap();

    cluster.must_split(&region, b"k2");

    // Wait for the peer of new region to start campaign.
    thread::sleep(reserved_time);

    // The campaign should always succeeds in the ideal test environment.
    let new_region = cluster.get_region(b"k3");
    // Ensure the new leader is established for the newly split region, and it
    // shares the same store with the leader of old region.
    let new_leader = cluster.query_leader(
        old_leader.get_store_id(),
        new_region.get_id(),
        Duration::from_secs(5),
    );
    assert!(new_leader.is_some());
}
