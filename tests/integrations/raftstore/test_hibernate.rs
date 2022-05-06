// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::*, *},
    thread,
    time::Duration,
};

use futures::executor::block_on;
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use test_raftstore::*;
use tikv_util::{time::Instant, HandyRwLock};

#[test]
fn test_proposal_prevent_sleep() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );

    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1).direction(Direction::Send),
    ));
    let region = block_on(cluster.pd_client.get_region_by_id(1))
        .unwrap()
        .unwrap();

    let put = new_put_cmd(b"k2", b"v2");
    let mut req = new_request(1, region.get_region_epoch().clone(), vec![put], true);
    req.mut_header().set_peer(new_peer(1, 1));
    // ignore error, we just want to send this command to peer (1, 1),
    // and the command can't be executed because we have only one peer,
    // so here will return timeout error, we should ignore it.
    let _ = cluster.call_command(req, Duration::from_millis(10));
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));

    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1).direction(Direction::Send),
    ));
    let mut request = new_request(
        region.get_id(),
        region.get_region_epoch().clone(),
        vec![new_read_index_cmd()],
        true,
    );
    request.mut_header().set_peer(new_peer(1, 1));
    let (cb, rx) = make_cb(&request);
    // send to peer 2
    cluster
        .sim
        .rl()
        .async_command_on_node(1, request, cb)
        .unwrap();
    thread::sleep(Duration::from_millis(10));
    cluster.clear_send_filters();
    let resp = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        !resp.get_header().has_error(),
        "{:?}",
        resp.get_header().get_error()
    );

    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1).direction(Direction::Send),
    ));
    let conf_change = new_change_peer_request(ConfChangeType::RemoveNode, new_peer(3, 3));
    let mut admin_req = new_admin_request(1, region.get_region_epoch(), conf_change);
    admin_req.mut_header().set_peer(new_peer(1, 1));
    let (cb, _rx) = make_cb(&admin_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, admin_req, cb)
        .unwrap();
    thread::sleep(Duration::from_millis(10));
    cluster.clear_send_filters();
    cluster.pd_client.must_none_peer(1, new_peer(3, 3));
}

/// Tests whether single voter still replicates log to learner after restart.
///
/// A voter will become leader in a single tick. The case check if the role
/// change is detected correctly.
#[test]
fn test_single_voter_restart() {
    let mut cluster = new_server_cluster(0, 2);
    configure_for_hibernate(&mut cluster);
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();
    cluster.pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.stop_node(2);
    cluster.must_put(b"k2", b"v2");
    cluster.stop_node(1);
    // Restart learner first to avoid network influence.
    cluster.run_node(2).unwrap();
    cluster.run_node(1).unwrap();
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
}

/// Tests whether an isolated learner can be prompted to voter.
#[test]
fn test_prompt_learner() {
    let mut cluster = new_server_cluster(0, 4);
    configure_for_hibernate(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 20;
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();
    cluster.pd_client.must_add_peer(1, new_peer(2, 2));
    cluster.pd_client.must_add_peer(1, new_peer(3, 3));

    cluster.pd_client.must_add_peer(1, new_learner_peer(4, 4));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");

    // Suppose there is only one way partition.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3).direction(Direction::Send),
    ));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 4).direction(Direction::Send),
    ));
    let idx = cluster.truncated_state(1, 1).get_index();
    // Trigger a log compaction.
    for i in 0..cluster.cfg.raft_store.raft_log_gc_count_limit * 2 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    cluster.wait_log_truncated(1, 1, idx + 1);
    // Wait till leader peer goes to sleep again.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    cluster.clear_send_filters();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3).direction(Direction::Send),
    ));
    cluster.pd_client.must_add_peer(1, new_peer(4, 4));
}

/// Tests whether leader resumes correctly when pre-transfer
/// leader response is delayed more than an election timeout.
#[test]
fn test_transfer_leader_delay() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let messages = Arc::new(Mutex::new(vec![]));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3)
            .direction(Direction::Send)
            .msg_type(MessageType::MsgTransferLeader)
            .reserve_dropped(messages.clone()),
    ));

    cluster.transfer_leader(1, new_peer(3, 3));
    let timer = Instant::now();
    while timer.saturating_elapsed() < Duration::from_secs(3) && messages.lock().unwrap().is_empty()
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(messages.lock().unwrap().len(), 1);

    // Wait till leader peer goes to sleep again.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );

    cluster.clear_send_filters();
    cluster.add_send_filter(CloneFilterFactory(DropMessageFilter::new(
        MessageType::MsgTimeoutNow,
    )));
    let router = cluster.sim.wl().get_router(1).unwrap();
    router
        .send_raft_message(messages.lock().unwrap().pop().unwrap())
        .unwrap();

    let timer = Instant::now();
    while timer.saturating_elapsed() < Duration::from_secs(3) {
        let resp = cluster.request(
            b"k2",
            vec![new_put_cmd(b"k2", b"v2")],
            false,
            Duration::from_secs(5),
        );
        let header = resp.get_header();
        if !header.has_error() {
            return;
        }
        if !header
            .get_error()
            .get_message()
            .contains("proposal dropped")
        {
            panic!("response {:?} has error", resp);
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("failed to request after 3 seconds");
}

/// If a learner is isolated before split and then catch up logs by snapshot, then the
/// range for split learner will be missing on the node until leader is waken.
#[test]
fn test_split_delay() {
    let mut cluster = new_server_cluster(0, 4);
    configure_for_hibernate(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 20;
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();
    cluster.pd_client.must_add_peer(1, new_peer(2, 2));
    cluster.pd_client.must_add_peer(1, new_peer(3, 3));

    cluster.pd_client.must_add_peer(1, new_learner_peer(4, 4));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");

    // Suppose there is only one way partition.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 4).direction(Direction::Recv),
    ));
    let idx = cluster.truncated_state(1, 1).get_index();
    // Trigger a log compaction.
    for i in 0..cluster.cfg.raft_store.raft_log_gc_count_limit * 2 {
        cluster.must_put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
    }
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k3");
    cluster.wait_log_truncated(1, 1, idx + 1);
    // Wait till leader peer goes to sleep again.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 2
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(4), b"k2", b"v2");
}

/// During rolling update, hibernate configuration may vary in different nodes.
/// If leader sleep unconditionally, follower may start new election and cause
/// jitters in service. This case tests leader will go to sleep only when every
/// followers are agreed to.
#[test]
fn test_inconsistent_configuration() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );

    // Ensure leader can sleep if all nodes enable hibernate region.
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(true));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(!awakened.load(Ordering::SeqCst));

    // Simulate rolling disable hibernate region in followers
    filter.store(false, Ordering::SeqCst);
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.stop_node(3);
    cluster.run_node(3).unwrap();
    cluster.must_put(b"k2", b"v2");
    // In case leader changes.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    awakened.store(false, Ordering::SeqCst);
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader should keep awake as peer 3 won't agree to sleep.
    assert!(awakened.load(Ordering::SeqCst));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));

    // Simulate rolling disable hibernate region in leader
    cluster.clear_send_filters();
    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    awakened.store(false, Ordering::SeqCst);
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 3)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            })),
    ));
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader should keep awake as hibernate region is disabled.
    assert!(awakened.load(Ordering::SeqCst));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(3, 3)));
}

/// Negotiating hibernation is implemented after 5.0.0, for older version binaries,
/// negotiating can cause connection reset due to new enum type. The test ensures
/// negotiation won't happen until cluster is upgraded.
#[test]
fn test_hibernate_feature_gate() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.reset_version("4.0.0");
    configure_for_hibernate(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Wait for hibernation check.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );

    // Ensure leader won't sleep if cluster version is small.
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(true));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(awakened.load(Ordering::SeqCst));

    // Simulating all binaries are upgraded to 5.0.0.
    cluster.pd_client.reset_version("5.0.0");
    filter.store(false, Ordering::SeqCst);
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    awakened.store(false, Ordering::SeqCst);
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader can go to sleep as version requirement is met.
    assert!(!awakened.load(Ordering::SeqCst));
}

/// Tests when leader is demoted in a hibernated region, the region can recover automatically.
#[test]
fn test_leader_demoted_when_hibernated() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_hibernate(&mut cluster);
    cluster.pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    cluster.pd_client.must_add_peer(r, new_peer(2, 2));
    cluster.pd_client.must_add_peer(r, new_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(r, new_peer(1, 1));
    // Demote a follower to learner.
    cluster.pd_client.must_joint_confchange(
        r,
        vec![
            (ConfChangeType::AddLearnerNode, new_learner_peer(3, 3)),
            (ConfChangeType::AddLearnerNode, new_learner_peer(4, 4)),
        ],
    );
    // So old leader will not commit the demote request.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r, 1)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
    ));
    // So new leader will not commit the demote request.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r, 3)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
    ));

    // So only peer 3 can become leader.
    for id in 1..=2 {
        cluster.add_send_filter(CloneFilterFactory(
            RegionPacketFilter::new(r, id)
                .msg_type(MessageType::MsgRequestPreVote)
                .direction(Direction::Send),
        ));
    }
    // Leave joint.
    cluster.async_exit_joint(r).unwrap();
    // Ensure peer 3 can campaign.
    cluster.wait_last_index(r, 3, 11, Duration::from_secs(5));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r, 1)
            .msg_type(MessageType::MsgHeartbeat)
            .direction(Direction::Send),
    ));
    // Wait some time to ensure the request has been delivered.
    thread::sleep(Duration::from_millis(100));
    // Peer 3 should start campaign.
    let timer = Instant::now();
    loop {
        cluster.reset_leader_of_region(r);
        let cur_leader = cluster.leader_of_region(r);
        if let Some(ref cur_leader) = cur_leader {
            if cur_leader.get_id() == 3 && cur_leader.get_store_id() == 3 {
                break;
            }
        }
        if timer.saturating_elapsed() > Duration::from_secs(5) {
            panic!("peer 3 is still not leader after 5 seconds.");
        }
        let region = cluster.get_region(b"k1");
        let mut request = new_request(
            region.get_id(),
            region.get_region_epoch().clone(),
            vec![new_put_cf_cmd("default", b"k1", b"v1")],
            false,
        );
        request.mut_header().set_peer(new_peer(3, 3));
        // In case peer 3 is hibernated.
        let (cb, _rx) = make_cb(&request);
        cluster
            .sim
            .rl()
            .async_command_on_node(3, request, cb)
            .unwrap();
    }

    cluster.clear_send_filters();
    // If there is no leader in the region, the cluster can't write two kvs successfully.
    // The first one is possible to succeed if it's committed with the conf change at the
    // same time, but the second one can't be committed or accepted because conf change
    // should be applied and the leader should be demoted as learner.
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
}
