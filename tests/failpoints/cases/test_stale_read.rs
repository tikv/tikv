// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::*, mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use kvproto::metapb::{Peer, Region};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::store::Callback;
use test_raftstore::*;
use tikv_util::{config::*, HandyRwLock};

fn stale_read_during_splitting(right_derive: bool) {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    cluster.cfg.raft_store.right_derive_when_split = right_derive;
    let election_timeout = configure_for_lease_read(&mut cluster, None, None);
    cluster.run();

    // Write the initial values.
    let key1 = b"k1";
    let v1 = b"v1";
    cluster.must_put(key1, v1);
    let key2 = b"k2";
    let v2 = b"v2";
    cluster.must_put(key2, v2);

    // Get the first region.
    let region_left = cluster.get_region(key1);
    let region_right = cluster.get_region(key2);
    assert_eq!(region_left, region_right);
    let region1 = region_left;
    assert_eq!(region1.get_id(), 1);
    let peer3 = region1
        .get_peers()
        .iter()
        .find(|p| p.get_id() == 3)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region1.get_id(), peer3.clone());

    // Get the current leader.
    let leader1 = peer3;

    // Pause the apply worker of peer 3.
    let apply_split = "apply_before_split_1_3";
    fail::cfg(apply_split, "pause").unwrap();

    // Split the first region.
    cluster.split_region(&region1, key2, Callback::write(Box::new(move |_| {})));

    // Sleep for a while.
    // The TiKVs that have followers of the old region will elected a leader
    // of the new region.
    //           TiKV A  TiKV B  TiKV C
    // Region 1    L       F       F
    // Region 2    X       L       F
    // Note: A has the peer 3,
    //       L: leader, F: follower, X: peer is not ready.
    thread::sleep(election_timeout);

    // A key that is covered by the old region and the new region.
    let stale_key = if right_derive { key1 } else { key2 };
    // Get the new region.
    let region2 = cluster.get_region_with(stale_key, |region| region != &region1);

    // Get the leader of the new region.
    let leader2 = cluster.leader_of_region(region2.get_id()).unwrap();
    assert_ne!(leader1.get_store_id(), leader2.get_store_id());

    must_not_stale_read(
        &mut cluster,
        stale_key,
        &region1,
        &leader1,
        &region2,
        &leader2,
        apply_split,
    );
}

fn must_not_stale_read(
    cluster: &mut Cluster<NodeCluster>,
    stale_key: &[u8],
    old_region: &Region,
    old_leader: &Peer,
    new_region: &Region,
    new_leader: &Peer,
    fp: &str,
) {
    // A new value for stale_key.
    let v3 = b"v3";
    let mut request = new_request(
        new_region.get_id(),
        new_region.get_region_epoch().clone(),
        vec![new_put_cf_cmd("default", stale_key, v3)],
        false,
    );
    request.mut_header().set_peer(new_leader.clone());
    cluster
        .call_command_on_node(new_leader.get_store_id(), request, Duration::from_secs(5))
        .unwrap();

    // LocalRead.
    let read_quorum = false;
    must_not_eq_on_key(
        cluster,
        stale_key,
        v3,
        read_quorum,
        old_region,
        old_leader,
        new_region,
        new_leader,
    );

    // ReadIndex.
    let read_quorum = true;
    must_not_eq_on_key(
        cluster,
        stale_key,
        v3,
        read_quorum,
        old_region,
        old_leader,
        new_region,
        new_leader,
    );

    // Leaders can always propose read index despite split/merge.
    let propose_readindex = "before_propose_readindex";
    fail::cfg(propose_readindex, "return(true)").unwrap();

    // Can not execute reads that are queued.
    let value1 = read_on_peer(
        cluster,
        old_leader.clone(),
        old_region.clone(),
        stale_key,
        read_quorum,
        Duration::from_secs(1),
    );
    debug!("stale_key: {:?}, {:?}", stale_key, value1);
    value1.unwrap_err(); // Error::Timeout

    // Remove the fp.
    fail::remove(fp);

    // It should read an error instead of timeout.
    let value1 = read_on_peer(
        cluster,
        old_leader.clone(),
        old_region.clone(),
        stale_key,
        read_quorum,
        Duration::from_secs(5),
    );
    debug!("stale_key: {:?}, {:?}", stale_key, value1);
    assert!(value1.unwrap().get_header().has_error());

    // Clean up.
    fail::remove(propose_readindex);
}

fn must_not_eq_on_key(
    cluster: &mut Cluster<NodeCluster>,
    key: &[u8],
    value: &[u8],
    read_quorum: bool,
    old_region: &Region,
    old_leader: &Peer,
    new_region: &Region,
    new_leader: &Peer,
) {
    let value1 = read_on_peer(
        cluster,
        old_leader.clone(),
        old_region.clone(),
        key,
        read_quorum,
        Duration::from_secs(1),
    );
    let value2 = read_on_peer(
        cluster,
        new_leader.clone(),
        new_region.clone(),
        key,
        read_quorum,
        Duration::from_secs(1),
    );
    debug!("stale_key: {:?}, {:?} vs {:?}", key, value1, value2);
    assert_eq!(must_get_value(value2.as_ref().unwrap()).as_slice(), value);
    // The old leader should return an error.
    assert!(
        value1.as_ref().unwrap().get_header().has_error(),
        "{:?}",
        value1
    );
}

#[test]
fn test_node_stale_read_during_splitting_left_derive() {
    stale_read_during_splitting(false);
}

#[test]
fn test_node_stale_read_during_splitting_right_derive() {
    stale_read_during_splitting(true);
}

#[test]
fn test_stale_read_during_merging() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    configure_for_merge(&mut cluster);
    let election_timeout = configure_for_lease_read(&mut cluster, None, None);
    cluster.cfg.raft_store.right_derive_when_split = false;
    cluster.cfg.raft_store.pd_heartbeat_tick_interval =
        cluster.cfg.raft_store.raft_base_tick_interval;
    debug!("max leader lease: {:?}", election_timeout);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run_conf_change();

    // Write the initial values.
    let key1 = b"k1";
    let v1 = b"v1";
    cluster.must_put(key1, v1);
    let key2 = b"k2";
    let v2 = b"v2";
    cluster.must_put(key2, v2);
    let region = pd_client.get_region(b"k1").unwrap();
    pd_client.must_add_peer(region.get_id(), new_peer(2, 4));
    pd_client.must_add_peer(region.get_id(), new_peer(3, 5));

    cluster.must_split(&region, b"k2");

    let mut region1 = cluster.get_region(key1);
    let mut region1000 = cluster.get_region(key2);
    assert_ne!(region1, region1000);
    assert_eq!(region1.get_id(), 1); // requires disable right_derive.
    let leader1 = region1
        .get_peers()
        .iter()
        .find(|p| p.get_id() == 4)
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region1.get_id(), leader1.clone());

    let leader1000 = region1000
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() != leader1.get_store_id())
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region1000.get_id(), leader1000.clone());
    assert_ne!(leader1.get_store_id(), leader1000.get_store_id());

    // Sleeps an election timeout. The new leader needs enough time to gather
    // all followers progress, in cause the merge request is reject by the
    // log gap too large (min_progress == 0).
    thread::sleep(election_timeout);

    //             merge into
    // region1000 ------------> region1
    cluster.must_try_merge(region1000.get_id(), region1.get_id());

    // Pause the apply workers except for the peer 4.
    let apply_commit_merge = "apply_before_commit_merge_except_1_4";
    fail::cfg(apply_commit_merge, "pause").unwrap();

    // Wait for commit merge.
    // The TiKVs that have followers of the old region will elected a leader
    // of the new region.
    //             TiKV A  TiKV B  TiKV C
    // Region    1   L       F       F
    // Region 1000   F       L       F
    //           after wait
    // Region    1   L       F       F
    // Region 1000   X       L       F
    // Note: L: leader, F: follower, X: peer is not exist.
    // TODO: what if cluster runs slow and lease is expired.
    // Epoch changed by prepare merge.
    // We can not use `get_region_with` to get the latest info of reigon 1000,
    // because leader1 is not paused, it executes commit merge very fast
    // and reports pd, its range covers region1000.
    //
    // region1000 does prepare merge, it increases ver and conf_ver by 1.
    debug!("before merge: {:?} | {:?}", region1000, region1);
    let region1000_version = region1000.get_region_epoch().get_version() + 1;
    region1000
        .mut_region_epoch()
        .set_version(region1000_version);
    let region1000_conf_version = region1000.get_region_epoch().get_conf_ver() + 1;
    region1000
        .mut_region_epoch()
        .set_conf_ver(region1000_conf_version);

    // Epoch changed by commit merge.
    region1 = cluster.get_region_with(key1, |region| region != &region1);
    debug!("after merge: {:?} | {:?}", region1000, region1);

    // A key that is covered by region 1000 and region 1.
    let stale_key = key2;

    must_not_stale_read(
        &mut cluster,
        stale_key,
        &region1000,
        &leader1000,
        &region1,
        &leader1,
        apply_commit_merge,
    );
}

#[test]
fn test_read_index_when_transfer_leader_2() {
    let mut cluster = new_node_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    // Stop log compaction to transfer leader with filter easier.
    configure_for_request_snapshot(&mut cluster);
    let max_lease = Duration::from_secs(2);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration(max_lease);

    // Add peer 2 and 3 and wait them to apply it.
    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k0", b"v0");
    cluster.pd_client.must_add_peer(r1, new_peer(2, 2));
    cluster.pd_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(2), b"k0", b"v0");
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    // Put and test again to ensure that peer 3 get the latest writes by message append
    // instead of snapshot, so that transfer leader to peer 3 can 100% success.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    let r1 = cluster.get_region(b"k1");
    let old_leader = cluster.leader_of_region(r1.get_id()).unwrap();

    // Use a macro instead of a closure to avoid any capture of local variables.
    macro_rules! read_on_old_leader {
        () => {{
            let (tx, rx) = mpsc::sync_channel(1);
            let mut read_request = new_request(
                r1.get_id(),
                r1.get_region_epoch().clone(),
                vec![new_get_cmd(b"k1")],
                true, // read quorum
            );
            read_request.mut_header().set_peer(new_peer(1, 1));
            let sim = cluster.sim.wl();
            sim.async_command_on_node(
                old_leader.get_id(),
                read_request,
                Callback::Read(Box::new(move |resp| tx.send(resp.response).unwrap())),
            )
            .unwrap();
            rx
        }};
    }

    // Delay all raft messages to peer 1.
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let filter = Box::new(
        RegionPacketFilter::new(r1.get_id(), old_leader.get_store_id())
            .direction(Direction::Recv)
            .skip(MessageType::MsgTransferLeader)
            .when(Arc::new(AtomicBool::new(true)))
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster
        .sim
        .wl()
        .add_recv_filter(old_leader.get_id(), filter);

    let resp1 = read_on_old_leader!();

    cluster.must_transfer_leader(r1.get_id(), new_peer(3, 3));

    let resp2 = read_on_old_leader!();

    // Unpark all pending messages and clear all filters.
    let router = cluster.sim.wl().get_router(old_leader.get_id()).unwrap();
    let mut reserved_msgs = Vec::new();
    'LOOP: loop {
        for raft_msg in std::mem::take(&mut *dropped_msgs.lock().unwrap()) {
            let msg_type = raft_msg.get_message().get_msg_type();
            if msg_type == MessageType::MsgHeartbeatResponse || msg_type == MessageType::MsgAppend {
                reserved_msgs.push(raft_msg);
                if msg_type == MessageType::MsgAppend {
                    break 'LOOP;
                }
            }
        }
    }

    // Resume reserved messages in one batch to make sure the old leader can get read and role
    // change in one `Ready`.
    fail::cfg("pause_on_peer_collect_message", "pause").unwrap();
    for raft_msg in reserved_msgs {
        router.send_raft_message(raft_msg).unwrap();
    }
    fail::cfg("pause_on_peer_collect_message", "off").unwrap();
    cluster.sim.wl().clear_recv_filters(old_leader.get_id());

    let resp1 = resp1.recv().unwrap();
    assert!(resp1.get_header().get_error().has_stale_command());

    // Response 2 should contains an error.
    let resp2 = resp2.recv().unwrap();
    assert!(resp2.get_header().get_error().has_stale_command());
    drop(cluster);
    fail::remove("pause_on_peer_collect_message");
}

#[test]
fn test_read_after_peer_destroyed() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    // Add 2 peers.
    for i in 2..4 {
        pd_client.must_add_peer(r1, new_peer(i, i));
    }

    // Make sure peer 1 leads the region.
    cluster.must_transfer_leader(r1, new_peer(1, 1));
    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let destroy_peer_fp = "destroy_peer";
    fail::cfg(destroy_peer_fp, "pause").unwrap();
    pd_client.must_remove_peer(r1, new_peer(1, 1));
    sleep_ms(300);

    // Try writing k2 to peer3
    let mut request = new_request(
        r1,
        cluster.pd_client.get_region_epoch(r1),
        vec![new_get_cmd(b"k1")],
        false,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    // Wait for raftstore receives the read request.
    sleep_ms(200);
    fail::remove(destroy_peer_fp);

    let resp = rx.recv_timeout(Duration::from_millis(200)).unwrap();
    assert!(
        resp.get_header().get_error().has_region_not_found(),
        "{:?}",
        resp
    );
}

/// In previous implementation, we suspect the leader lease at the position of `leader_commit_prepare_merge`
/// failpoint when `PrepareMerge` log is committed, which is too late to prevent stale read.
#[test]
fn test_stale_read_during_merging_2() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    configure_for_merge(&mut cluster);
    configure_for_lease_read(&mut cluster, Some(50), Some(20));

    cluster.run();

    for i in 0..10 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }

    let region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let left = pd_client.get_region(b"k1").unwrap();
    let right = pd_client.get_region(b"k2").unwrap();

    let left_peer_1 = find_peer(&left, 1).unwrap().to_owned();
    cluster.must_transfer_leader(left.get_id(), left_peer_1.clone());
    let right_peer_3 = find_peer(&right, 3).unwrap().to_owned();
    cluster.must_transfer_leader(right.get_id(), right_peer_3);

    let leader_commit_prepare_merge_fp = "leader_commit_prepare_merge";
    fail::cfg(leader_commit_prepare_merge_fp, "pause").unwrap();

    pd_client.must_merge(left.get_id(), right.get_id());

    cluster.must_put(b"k1", b"v1");

    let value = read_on_peer(
        &mut cluster,
        left_peer_1,
        left,
        b"k1",
        false,
        Duration::from_millis(200),
    );
    // The leader lease must be suspected so the local read is forbidden.
    // The result should be Error::Timeout because the leader is paused at
    // the position of `leader_commit_prepare_merge` failpoint.
    // In previous implementation, the result is ok and the value is "v"
    // but the right answer is "v1".
    value.unwrap_err();

    fail::remove(leader_commit_prepare_merge_fp);
}
