// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::channel;
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use kvproto::metapb;
use kvproto::pdpb;
use kvproto::raft_cmdpb::*;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;

use engine::Iterable;
use engine::CF_WRITE;
use keys::data_key;
use pd_client::PdClient;
use test_raftstore::*;
use tikv::raftstore::store::{Callback, WriteResponse};
use tikv::raftstore::Result;
use tikv_util::config::*;

pub const REGION_MAX_SIZE: u64 = 50000;
pub const REGION_SPLIT_SIZE: u64 = 30000;

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

#[test]
fn test_server_split_region_twice() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);

    let (split_key, left_key, right_key) = (b"k22", b"k11", b"k33");
    cluster.must_put(left_key, b"v1");
    cluster.must_put(right_key, b"v3");

    // Left and right key must be in same region before split.
    let region = pd_client.get_region(left_key).unwrap();
    let region2 = pd_client.get_region(right_key).unwrap();
    assert_eq!(region.get_id(), region2.get_id());

    let (tx, rx) = channel();
    let key = split_key.to_vec();
    let c = Box::new(move |write_resp: WriteResponse| {
        let mut resp = write_resp.response;
        let admin_resp = resp.mut_admin_response();
        let split_resp = admin_resp.mut_splits();
        let mut regions: Vec<_> = split_resp.take_regions().into();
        let mut d = regions.drain(..);
        let (left, right) = (d.next().unwrap(), d.next().unwrap());
        assert_eq!(left.get_end_key(), key.as_slice());
        assert_eq!(region2.get_start_key(), left.get_start_key());
        assert_eq!(left.get_end_key(), right.get_start_key());
        assert_eq!(region2.get_end_key(), right.get_end_key());
        tx.send(right).unwrap();
    });
    cluster.split_region(&region, split_key, Callback::Write(c));
    let region3 = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    cluster.must_put(split_key, b"v2");

    let (tx1, rx1) = channel();
    let c = Box::new(move |write_resp: WriteResponse| {
        assert!(write_resp.response.has_header());
        assert!(write_resp.response.get_header().has_error());
        assert!(!write_resp.response.has_admin_response());
        tx1.send(()).unwrap();
    });
    cluster.split_region(&region3, split_key, Callback::Write(c));
    rx1.recv_timeout(Duration::from_secs(5)).unwrap();
}

fn test_auto_split_region<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.coprocessor.region_max_size = ReadableSize(REGION_MAX_SIZE);
    cluster.cfg.coprocessor.region_split_size = ReadableSize(REGION_SPLIT_SIZE);

    let check_size_diff = cluster.cfg.raft_store.region_split_check_diff.0;
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
    cluster.engines[&store_id]
        .kv
        .scan(&data_key(b""), &data_key(middle_key), false, |k, v| {
            size += k.len() as u64;
            size += v.len() as u64;
            Ok(true)
        })
        .expect("");
    assert!(size <= REGION_SPLIT_SIZE);
    // although size may be smaller than REGION_SPLIT_SIZE, but the diff should
    // be small.
    assert!(size > REGION_SPLIT_SIZE - 1000);

    let epoch = left.get_region_epoch().clone();
    let get = new_request(left.get_id(), epoch, vec![new_get_cmd(&max_key)], false);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

#[test]
fn test_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

#[test]
fn test_incompatible_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_incompatible_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

#[test]
fn test_server_auto_split_region() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

#[test]
fn test_incompatible_server_auto_split_region() {
    let count = 5;
    let mut cluster = new_incompatible_server_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

// A filter that disable commitment by heartbeat.
#[derive(Clone)]
struct EraseHeartbeatCommit;

impl Filter for EraseHeartbeatCommit {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for msg in msgs {
            if msg.get_message().get_msg_type() == MessageType::MsgHeartbeat {
                msg.mut_message().set_commit(0);
            }
        }
        Ok(())
    }
}

fn check_cluster(cluster: &mut Cluster<impl Simulator>, k: &[u8], v: &[u8], all_committed: bool) {
    let region = cluster.pd_client.get_region(k).unwrap();
    let mut tried_cnt = 0;
    let leader = loop {
        match cluster.leader_of_region(region.get_id()) {
            None => {
                tried_cnt += 1;
                if tried_cnt >= 3 {
                    panic!("leader should be elected");
                }
                continue;
            }
            Some(l) => break l,
        }
    };
    for i in 1..=region.get_peers().len() as u64 {
        let engine = cluster.get_engine(i);
        if all_committed || i == leader.get_store_id() {
            must_get_equal(&engine, k, v);
        } else {
            must_get_none(&engine, k);
        }
    }
}

/// TiKV enables lazy broadcast commit optimization, which can delay split
/// on follower node. So election of new region will delay. We need to make
/// sure broadcast commit is disabled when split.
#[test]
fn test_delay_split_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 500;
    cluster.cfg.raft_store.merge_max_log_gap = 100;
    cluster.cfg.raft_store.raft_log_gc_threshold = 500;
    // To stable the test, we use a large hearbeat timeout 200ms(100ms * 2).
    // And to elect leader quickly, set election timeout to 1s(100ms * 10).
    configure_for_lease_read(&mut cluster, Some(100), Some(10));

    // We use three nodes for this test.
    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);

    let region = pd_client.get_region(b"").unwrap();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    // Although skip bcast is enabled, but heartbeat will commit the log in period.
    check_cluster(&mut cluster, b"k1", b"v1", true);
    check_cluster(&mut cluster, b"k3", b"v3", true);
    cluster.must_transfer_leader(region.get_id(), new_peer(1, 1));

    cluster.add_send_filter(CloneFilterFactory(EraseHeartbeatCommit));

    cluster.must_put(b"k4", b"v4");
    sleep_ms(100);
    // skip bcast is enabled by default, so all followers should not commit
    // the log.
    check_cluster(&mut cluster, b"k4", b"v4", false);

    cluster.must_transfer_leader(region.get_id(), new_peer(3, 3));
    // New leader should flush old committed entries eagerly.
    check_cluster(&mut cluster, b"k4", b"v4", true);
    cluster.must_put(b"k5", b"v5");
    // New committed entries should be broadcast lazily.
    check_cluster(&mut cluster, b"k5", b"v5", false);
    cluster.add_send_filter(CloneFilterFactory(EraseHeartbeatCommit));

    let k2 = b"k2";
    // Split should be bcast eagerly, otherwise following must_put will fail
    // as no leader is available.
    cluster.must_split(&region, k2);
    cluster.must_put(b"k6", b"v6");

    sleep_ms(100);
    // After split, skip bcast is enabled again, so all followers should not
    // commit the log.
    check_cluster(&mut cluster, b"k6", b"v6", false);
}

fn test_split_overlap_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // We use three nodes([1, 2, 3]) for this test.
    cluster.run();

    // guarantee node 1 is leader
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k0", b"v0");
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));

    let pd_client = Arc::clone(&cluster.pd_client);

    // isolate node 3 for region 1.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 3)));
    cluster.must_put(b"k1", b"v1");

    let region = pd_client.get_region(b"").unwrap();

    // split (-inf, +inf) -> (-inf, k2), [k2, +inf]
    cluster.must_split(&region, b"k2");

    cluster.must_put(b"k2", b"v2");

    // node 1 and node 2 must have k2, but node 3 must not.
    for i in 1..3 {
        let engine = cluster.get_engine(i);
        must_get_equal(&engine, b"k2", b"v2");
    }

    let engine3 = cluster.get_engine(3);
    must_get_none(&engine3, b"k2");

    thread::sleep(Duration::from_secs(1));
    let snap_dir = cluster.get_snap_dir(3);
    // no snaps should be sent.
    let snapfiles: Vec<_> = fs::read_dir(snap_dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    assert!(snapfiles.is_empty());

    cluster.clear_send_filters();
    cluster.must_put(b"k3", b"v3");

    sleep_ms(3000);
    // node 3 must have k3.
    must_get_equal(&engine3, b"k3", b"v3");
}

#[test]
fn test_node_split_overlap_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_split_overlap_snapshot(&mut cluster);
}

#[test]
fn test_server_split_overlap_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_split_overlap_snapshot(&mut cluster);
}

fn test_apply_new_version_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    // truncate the log quickly so that we can force sending snapshot.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 5;
    cluster.cfg.raft_store.merge_max_log_gap = 1;
    cluster.cfg.raft_store.raft_log_gc_threshold = 5;

    // We use three nodes([1, 2, 3]) for this test.
    cluster.run();

    // guarantee node 1 is leader
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k0", b"v0");
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));

    let pd_client = Arc::clone(&cluster.pd_client);

    // isolate node 3 for region 1.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, 3)));
    cluster.must_put(b"k1", b"v1");

    let region = pd_client.get_region(b"").unwrap();

    // split (-inf, +inf) -> (-inf, k2), [k2, +inf]
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k2", b"v2");

    // node 1 and node 2 must have k2, but node 3 must not.
    for i in 1..3 {
        let engine = cluster.get_engine(i);
        must_get_equal(&engine, b"k2", b"v2");
    }

    let engine3 = cluster.get_engine(3);
    must_get_none(&engine3, b"k2");

    // transfer leader to ease the preasure of store 1.
    cluster.must_transfer_leader(1, new_peer(2, 2));

    for _ in 0..100 {
        // write many logs to force log GC for region 1 and region 2.
        cluster.must_put(b"k1", b"v1");
        cluster.must_put(b"k2", b"v2");
    }

    cluster.clear_send_filters();

    sleep_ms(3000);
    // node 3 must have k1, k2.
    must_get_equal(&engine3, b"k1", b"v1");
    must_get_equal(&engine3, b"k2", b"v2");
}

#[test]
fn test_node_apply_new_version_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_apply_new_version_snapshot(&mut cluster);
}

#[test]
fn test_server_apply_new_version_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_apply_new_version_snapshot(&mut cluster);
}

fn test_split_with_stale_peer<T: Simulator>(cluster: &mut Cluster<T>) {
    // disable raft log gc.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(500);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    // add peer (3,3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    cluster.must_put(b"k0", b"v0");
    // check node 3 has k0.
    let engine3 = cluster.get_engine(3);
    must_get_equal(&engine3, b"k0", b"v0");

    // guarantee node 1 is leader.
    cluster.must_transfer_leader(r1, new_peer(1, 1));

    // isolate node 3 for region 1.
    // only filter MsgAppend to avoid election when recover.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3).msg_type(MessageType::MsgAppend),
    ));

    let region = pd_client.get_region(b"").unwrap();

    // split (-inf, +inf) -> (-inf, k2), [k2, +inf]
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k2", b"v2");

    let region2 = pd_client.get_region(b"k2").unwrap();

    // remove peer3 in region 2.
    let peer3 = find_peer(&region2, 3).unwrap();
    pd_client.must_remove_peer(region2.get_id(), peer3.clone());

    // clear isolation so node 3 can split region 1.
    // now node 3 has a stale peer for region 2, but
    // it will be removed soon.
    cluster.clear_send_filters();
    cluster.must_put(b"k1", b"v1");

    // check node 3 has k1
    must_get_equal(&engine3, b"k1", b"v1");

    // split [k2, +inf) -> [k2, k3), [k3, +inf]
    cluster.must_split(&region2, b"k3");
    let region3 = pd_client.get_region(b"k3").unwrap();
    // region 3 can't contain node 3.
    assert_eq!(region3.get_peers().len(), 2);
    assert!(find_peer(&region3, 3).is_none());

    let new_peer_id = pd_client.alloc_id().unwrap();
    // add peer (3, new_peer_id) to region 3
    pd_client.must_add_peer(region3.get_id(), new_peer(3, new_peer_id));

    cluster.must_put(b"k3", b"v3");
    // node 3 must have k3.
    must_get_equal(&engine3, b"k3", b"v3");
}

#[test]
fn test_node_split_with_stale_peer() {
    let mut cluster = new_node_cluster(0, 3);
    test_split_with_stale_peer(&mut cluster);
}

#[test]
fn test_server_split_with_stale_peer() {
    let mut cluster = new_server_cluster(0, 3);
    test_split_with_stale_peer(&mut cluster);
}

fn test_split_region_diff_check<T: Simulator>(cluster: &mut Cluster<T>) {
    let region_max_size = 2000;
    let region_split_size = 1000;
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_split_check_diff = ReadableSize(10);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(20);
    cluster.cfg.coprocessor.region_max_size = ReadableSize(region_max_size);
    cluster.cfg.coprocessor.region_split_size = ReadableSize(region_split_size);

    let mut range = 1..;

    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);

    // The default size index distance is too large for small data,
    // we flush multiple times to generate more size index handles.
    for _ in 0..10 {
        put_till_size(cluster, region_max_size, &mut range);
    }

    // Peer will split when size of region meet region_max_size,
    // so assume the last region_max_size of data is not involved in split,
    // there will be at least (region_max_size * 10 - region_max_size) / region_split_size regions.
    // But region_max_size of data should be split too, so there will be at least 2 more regions.
    let min_region_cnt = (region_max_size * 10 - region_max_size) / region_split_size + 2;

    let mut try_cnt = 0;
    loop {
        sleep_ms(20);
        let region_cnt = pd_client.get_split_count() + 1;
        if region_cnt >= min_region_cnt as usize {
            return;
        }
        try_cnt += 1;
        if try_cnt == 500 {
            panic!(
                "expect split cnt {}, but got {}",
                min_region_cnt, region_cnt
            );
        }
    }
}

#[test]
fn test_server_split_region_diff_check() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_split_region_diff_check(&mut cluster);
}

#[test]
fn test_node_split_region_diff_check() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_split_region_diff_check(&mut cluster);
}

fn test_split_epoch_not_match<T: Simulator>(cluster: &mut Cluster<T>, right_derive: bool) {
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    let old = pd_client.get_region(b"k1").unwrap();
    // Construct a get command using old region meta.
    let get_old = new_request(
        old.get_id(),
        old.get_region_epoch().clone(),
        vec![new_get_cmd(b"k1")],
        false,
    );
    cluster.must_split(&old, b"k2");
    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k3").unwrap();

    let new = if right_derive {
        right.clone()
    } else {
        left.clone()
    };

    // Newer epoch also triggers the EpochNotMatch error.
    let mut latest_epoch = new.get_region_epoch().clone();
    let latest_version = latest_epoch.get_version() + 1;
    latest_epoch.set_version(latest_version);

    let get_new = new_request(new.get_id(), latest_epoch, vec![new_get_cmd(b"k1")], false);
    for get in &[get_old, get_new] {
        let resp = cluster
            .call_command_on_leader(get.clone(), Duration::from_secs(5))
            .unwrap();
        assert!(resp.get_header().has_error(), "{:?}", get);
        assert!(
            resp.get_header().get_error().has_epoch_not_match(),
            "{:?}",
            get
        );
        if right_derive {
            assert_eq!(
                resp.get_header()
                    .get_error()
                    .get_epoch_not_match()
                    .get_current_regions(),
                &[right.clone(), left.clone()]
            );
        } else {
            assert_eq!(
                resp.get_header()
                    .get_error()
                    .get_epoch_not_match()
                    .get_current_regions(),
                &[left.clone(), right.clone()]
            );
        }
    }
}

#[test]
fn test_server_split_epoch_not_match_left_derive() {
    let mut cluster = new_server_cluster(0, 3);
    test_split_epoch_not_match(&mut cluster, false);
}

#[test]
fn test_server_split_epoch_not_match_right_derive() {
    let mut cluster = new_server_cluster(0, 3);
    test_split_epoch_not_match(&mut cluster, true);
}

#[test]
fn test_node_split_epoch_not_match_left_derive() {
    let mut cluster = new_node_cluster(0, 3);
    test_split_epoch_not_match(&mut cluster, false);
}

#[test]
fn test_node_split_epoch_not_match_right_derive() {
    let mut cluster = new_node_cluster(0, 3);
    test_split_epoch_not_match(&mut cluster, true);
}

// For the peer which is the leader of the region before split,
// it should campaigns immediately. and then this peer may take the leadership earlier.
// `test_quick_election_after_split` is a helper function for testing this feature.
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
    // Ensure the new leader is established for the newly split region, and it shares the
    // same store with the leader of old region.
    let new_leader = cluster.query_leader(
        old_leader.get_store_id(),
        new_region.get_id(),
        Duration::from_secs(5),
    );
    assert!(new_leader.is_some());
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
    cluster.cfg.coprocessor.region_max_size = ReadableSize(item_len) * 1024;
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
    cluster.must_transfer_leader(right.get_id(), new_leader.clone());

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
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

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
