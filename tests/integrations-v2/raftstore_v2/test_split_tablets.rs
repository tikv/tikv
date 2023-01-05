// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use engine_traits::{CF_DEFAULT, CF_WRITE};
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{AdminCmdType, AdminRequest, BatchSplitRequest, SplitRequest},
};
use pd_client::PdClient;
use test_raftstore::{must_get_equal, new_admin_request, new_get_cmd, new_peer, new_request};
use test_raftstore_v2::{
    configure_for_lease_read, new_node_cluster, new_server_cluster, put_cf_till_size,
    put_till_size, Cluster, Simulator,
};
use tikv::storage::{kv::SnapshotExt, Snapshot};
use tikv_util::config::{ReadableDuration, ReadableSize};
use txn_types::{Key, PessimisticLock};

pub const REGION_MAX_SIZE: u64 = 50000;
pub const REGION_SPLIT_SIZE: u64 = 30000;

#[test]
fn test_server_base_split_region_left_derive() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_base_split_region(&mut cluster, Cluster::must_split, false);
}

#[test]
fn test_server_base_split_region_right_derive() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_base_split_region(&mut cluster, Cluster::must_split, true);
}

fn test_base_split_region<T, F>(cluster: &mut Cluster<T>, split: F, right_derive: bool)
where
    T: Simulator,
    F: Fn(&mut Cluster<T>, &metapb::Region, &[u8]),
{
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);

    let tbls = vec![
        (b"k22", b"k11", b"k33"),
        (b"k11", b"k00", b"k11"),
        (b"k33", b"k22", b"k33"),
    ];

    for (split_key, left_key, right_key) in tbls {
        println!(
            "split key {:?}, left key {:?}, right key {:?}",
            split_key, left_key, right_key
        );
        cluster.must_put(left_key, b"v1");
        cluster.must_put(right_key, b"v3");

        // Left and right key must be in same region before split.
        let region = pd_client.get_region(left_key).unwrap();
        let region2 = pd_client.get_region(right_key).unwrap();
        assert_eq!(region.get_id(), region2.get_id());

        // Split with split_key, so left_key must in left, and right_key in right.
        split(cluster, &region, split_key);

        let left = pd_client.get_region(left_key).unwrap();
        let right = pd_client.get_region(right_key).unwrap();

        assert_eq!(
            region.get_id(),
            if right_derive {
                right.get_id()
            } else {
                left.get_id()
            }
        );
        assert_eq!(region.get_start_key(), left.get_start_key());
        assert_eq!(left.get_end_key(), right.get_start_key());
        assert_eq!(region.get_end_key(), right.get_end_key());

        cluster.must_put(left_key, b"vv1");
        assert_eq!(cluster.get(left_key).unwrap(), b"vv1".to_vec());

        cluster.must_put(right_key, b"vv3");
        assert_eq!(cluster.get(right_key).unwrap(), b"vv3".to_vec());

        let epoch = left.get_region_epoch().clone();
        let get = new_request(left.get_id(), epoch, vec![new_get_cmd(right_key)], false);
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
    }
}

#[test]
fn test_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

#[test]
fn test_server_auto_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
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
    assert!(size > REGION_SPLIT_SIZE - 1000);

    let epoch = left.get_region_epoch().clone();
    let get = new_request(left.get_id(), epoch, vec![new_get_cmd(&max_key)], false);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(500))
        .unwrap();

    println!("resp {:?}", resp);
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_quick_election_after_split() {
    let mut cluster = new_node_cluster(0, 3);
    test_quick_election_after_split(&mut cluster);
}

#[test]
fn test_server_quick_election_after_split() {
    let mut cluster = new_server_cluster(0, 3);
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

#[test]
fn test_node_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_split_region(&mut cluster);
}

#[test]
fn test_server_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_split_region(&mut cluster);
}

fn test_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    // length of each key+value
    let item_len = 74;
    // make bucket's size to item_len, which means one row one bucket
    cluster.cfg.coprocessor.region_max_size = Some(ReadableSize(item_len) * 1024);
    let mut range = 1..;
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();
    let mid_key = put_till_size(cluster, 11 * item_len, &mut range);
    let max_key = put_till_size(cluster, 9 * item_len, &mut range);
    let target = pd_client.get_region(&max_key).unwrap();
    assert_eq!(region, target);
    pd_client.must_split_region(target, pdpb::CheckPolicy::Scan, vec![]);

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(mid_key.as_slice(), right.get_start_key());
    assert_eq!(right.get_start_key(), left.get_end_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    let region = pd_client.get_region(b"x").unwrap();
    pd_client.must_split_region(
        region,
        pdpb::CheckPolicy::Usekey,
        vec![b"x1".to_vec(), b"y2".to_vec()],
    );
    let x1 = pd_client.get_region(b"x1").unwrap();
    assert_eq!(x1.get_start_key(), b"x1");
    assert_eq!(x1.get_end_key(), b"y2");
    let y2 = pd_client.get_region(b"y2").unwrap();
    assert_eq!(y2.get_start_key(), b"y2");
    assert_eq!(y2.get_end_key(), b"");
}

#[test]
fn test_node_split_update_region_right_derive() {
    let mut cluster = new_node_cluster(0, 3);
    // Election timeout and max leader lease is 1s.
    configure_for_lease_read(&mut cluster, Some(100), Some(10));

    cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    let right = pd_client.get_region(b"k2").unwrap();

    let origin_leader = cluster.leader_of_region(right.get_id()).unwrap();
    let new_leader = right
        .get_peers()
        .iter()
        .cloned()
        .find(|p| p.get_id() != origin_leader.get_id())
        .unwrap();

    // Make sure split is done in the new_leader.
    // "k4" belongs to the right.
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(new_leader.get_store_id()), b"k4", b"v4");

    // Transfer leadership to another peer.
    cluster.must_transfer_leader(right.get_id(), new_leader);

    // Make sure the new_leader is in lease.
    cluster.must_put(b"k4", b"v5");

    // "k1" is not in the range of right.
    let get = new_request(
        right.get_id(),
        right.get_region_epoch().clone(),
        vec![new_get_cmd(b"k1")],
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
}

#[test]
fn test_split_with_epoch_not_match() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Remove a peer to make conf version become 2.
    pd_client.must_remove_peer(1, new_peer(2, 2));
    let region = cluster.get_region(b"");

    let mut admin_req = AdminRequest::default();
    admin_req.set_cmd_type(AdminCmdType::BatchSplit);

    let mut batch_split_req = BatchSplitRequest::default();
    batch_split_req.mut_requests().push(SplitRequest::default());
    batch_split_req.mut_requests()[0].set_split_key(b"s".to_vec());
    batch_split_req.mut_requests()[0].set_new_region_id(1000);
    batch_split_req.mut_requests()[0].set_new_peer_ids(vec![1001, 1002]);
    batch_split_req.mut_requests()[0].set_right_derive(true);
    admin_req.set_splits(batch_split_req);

    let mut epoch = region.get_region_epoch().clone();
    epoch.conf_ver -= 1;
    let req = new_admin_request(1, &epoch, admin_req);
    let resp = cluster
        .call_command_on_leader(req, Duration::from_secs(3))
        .unwrap();
    assert!(resp.get_header().get_error().has_epoch_not_match());
}

#[test]
fn test_split_with_in_memory_pessimistic_locks() {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Set two pessimistic locks in the original region.
    let txn_ext = cluster
        .must_get_snapshot_of_region(1)
        .ext()
        .get_txn_ext()
        .unwrap()
        .clone();
    let lock_a = PessimisticLock {
        primary: b"a".to_vec().into_boxed_slice(),
        start_ts: 10.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
        last_change_ts: 5.into(),
        versions_to_last_change: 3,
    };
    let lock_c = PessimisticLock {
        primary: b"c".to_vec().into_boxed_slice(),
        start_ts: 20.into(),
        ttl: 3000,
        for_update_ts: 20.into(),
        min_commit_ts: 30.into(),
        last_change_ts: 5.into(),
        versions_to_last_change: 3,
    };
    {
        let mut locks = txn_ext.pessimistic_locks.write();
        locks
            .insert(vec![
                (Key::from_raw(b"a"), lock_a.clone()),
                (Key::from_raw(b"c"), lock_c.clone()),
            ])
            .unwrap();
    }

    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"b");

    // After splitting, each new region should contain one lock.

    let region = cluster.get_region(b"a");
    let txn_ext = cluster
        .must_get_snapshot_of_region(region.id)
        .ext()
        .get_txn_ext()
        .unwrap()
        .clone();
    assert_eq!(
        txn_ext.pessimistic_locks.read().get(&Key::from_raw(b"a")),
        Some(&(lock_a, false))
    );

    let region = cluster.get_region(b"c");
    let txn_ext = cluster
        .must_get_snapshot_of_region(region.id)
        .ext()
        .get_txn_ext()
        .unwrap()
        .clone();
    assert_eq!(
        txn_ext.pessimistic_locks.read().get(&Key::from_raw(b"c")),
        Some(&(lock_c, false))
    );
}
