// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::*, *},
    thread,
    time::Duration,
};

use engine_rocks::RocksSnapshot;
use futures::executor::block_on;
use kvproto::raft_cmdpb::RaftCmdRequest;
use pd_client::PdClient;
use raft::eraftpb::{ConfChangeType, MessageType};
use raftstore::store::msg::*;
use test_raftstore::*;
use tikv_util::{HandyRwLock, config::ReadableDuration, time::Instant};

#[test]
fn test_proposal_prevent_sleep() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster.cfg);
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
    let (cb, mut rx) = make_cb_rocks(&request);
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
    let (cb, _rx) = make_cb_rocks(&admin_req);
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
    configure_for_hibernate(&mut cluster.cfg);
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
    configure_for_hibernate(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(20);
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
    for i in 0..cluster.cfg.raft_store.raft_log_gc_count_limit() * 2 {
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
    configure_for_hibernate(&mut cluster.cfg);
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
    cluster.add_send_filter(CloneFilterFactory(DropMessageFilter::new(Arc::new(|m| {
        m.get_message().get_msg_type() != MessageType::MsgTimeoutNow
    }))));
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

/// If a learner is isolated before split and then catch up logs by snapshot,
/// then the range for split learner will be missing on the node until leader is
/// waken.
#[test]
fn test_split_delay() {
    let mut cluster = new_server_cluster(0, 4);
    configure_for_hibernate(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(20);
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
    for i in 0..cluster.cfg.raft_store.raft_log_gc_count_limit() * 2 {
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
    configure_for_hibernate(&mut cluster.cfg);
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

/// Negotiating hibernation is implemented after 5.0.0, for older version
/// binaries, negotiating can cause connection reset due to new enum type. The
/// test ensures negotiation won't happen until cluster is upgraded.
#[test]
fn test_hibernate_feature_gate() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.reset_version("4.0.0");
    configure_for_hibernate(&mut cluster.cfg);
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

/// Tests when leader is demoted in a hibernated region, the region can recover
/// automatically.
#[test]
fn test_leader_demoted_when_hibernated() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_hibernate(&mut cluster.cfg);
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
    let _ = cluster.async_exit_joint(r).unwrap();
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
        let (cb, _rx) = make_cb_rocks(&request);
        cluster
            .sim
            .rl()
            .async_command_on_node(3, request, cb)
            .unwrap();
    }

    cluster.clear_send_filters();
    // If there is no leader in the region, the cluster can't write two kvs
    // successfully. The first one is possible to succeed if it's committed with
    // the conf change at the same time, but the second one can't be committed
    // or accepted because conf change should be applied and the leader should
    // be demoted as learner.
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
}

/// Tests whether leader can hibernate when some voters are down.
#[test]
fn test_hibernate_quorum_feature_basic() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster.cfg);
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
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(false));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(!awakened.load(Ordering::SeqCst));

    // Put some data to make leader exit hibernation.
    cluster.clear_send_filters(); // clear filters before putting data
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    // Must wait here for replication to finish before stopping node.
    thread::sleep(Duration::from_millis(1000));

    // Simulate one voter down.
    cluster.stop_node(3);
    thread::sleep(cluster.cfg.raft_store.max_peer_down_duration.0 * 2);
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));

    filter.store(false, Ordering::SeqCst);
    awakened.store(false, Ordering::SeqCst);
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader can go to sleep as voters can reach majority.
    assert!(!awakened.load(Ordering::SeqCst));

    // Put some data to make leader exit hibernation when one peer is down.
    cluster.clear_send_filters(); // clear filters before putting data
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(2), b"k3", b"v3");
    thread::sleep(cluster.cfg.raft_store.max_peer_down_duration.0 * 2);
    // Check whether leader can go to sleep again.
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));
    filter.store(false, Ordering::SeqCst);
    awakened.store(false, Ordering::SeqCst);
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader can go to sleep as voters can reach majority.
    assert!(!awakened.load(Ordering::SeqCst));

    // Start voter again.
    cluster.clear_send_filters();
    cluster.run_node(3).unwrap();
    thread::sleep(cluster.cfg.raft_store.max_peer_down_duration.0 * 2);
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));
    // add send filter again
    filter.store(false, Ordering::SeqCst);
    awakened.store(false, Ordering::SeqCst);
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader can go to sleep after down voter recover.
    assert!(!awakened.load(Ordering::SeqCst));

    cluster.clear_send_filters(); // clear filters before putting data
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
}

/// Tests whether leader can hibernate when one learner is down.
#[test]
fn test_hibernate_quorum_feature_down_learner() {
    let mut cluster = new_server_cluster(0, 4);
    configure_for_hibernate(&mut cluster.cfg);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(20);
    cluster.pd_client.disable_default_operator();
    cluster.run_conf_change();
    cluster.pd_client.must_add_peer(1, new_peer(2, 2));
    cluster.pd_client.must_add_peer(1, new_peer(3, 3));

    cluster.pd_client.must_add_peer(1, new_learner_peer(4, 4));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(4), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(false));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(!awakened.load(Ordering::SeqCst));

    // Simulate one learner down.
    cluster.stop_node(4);
    thread::sleep(cluster.cfg.raft_store.max_peer_down_duration.0 * 2);
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));

    filter.store(false, Ordering::SeqCst);
    awakened.store(false, Ordering::SeqCst);
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader can go to sleep as voters can reach majority.
    assert!(!awakened.load(Ordering::SeqCst));

    // Start learner again.
    cluster.clear_send_filters();
    cluster.run_node(4).unwrap();
    thread::sleep(cluster.cfg.raft_store.max_peer_down_duration.0 * 2);
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));
    // add send filter again
    filter.store(false, Ordering::SeqCst);
    awakened.store(false, Ordering::SeqCst);
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader can go to sleep after down learner recover.
    assert!(!awakened.load(Ordering::SeqCst));

    cluster.clear_send_filters(); // clear filters before putting data
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(4), b"k3", b"v3");
}

/// Test senario: keep max_peer_down_duration unchanged and down one voter to
/// check whether leader cannot hibernate in short time(before
/// max_peer_down_duration elapsed).
#[test]
fn test_hibernate_quorum_feature_voter_down_shortly() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster.cfg);
    cluster.cfg.raft_store.max_peer_down_duration = ReadableDuration::secs(600); // keep it unchanged
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
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(false));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(!awakened.load(Ordering::SeqCst));

    cluster.clear_send_filters(); // clear filters before putting data
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    // Must wait for replication to finish before stopping node.
    thread::sleep(Duration::from_millis(1000));

    // Simulate one voter down.
    cluster.stop_node(3);
    // Ensure leader is node 1.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(1, 1)));
    // change leader to node 2 to make sure leader knows node-3 is down
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.reset_leader_of_region(1);
    assert_eq!(cluster.leader_of_region(1), Some(new_peer(2, 2)));
    // Besides changing leader, we can also put some data to make leader know voter
    // is down. cluster.must_put(b"k3", b"v3");
    // must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
    // add send filter again
    filter.store(false, Ordering::SeqCst);
    awakened.store(false, Ordering::SeqCst);
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    // Wait till leader peer goes to sleep.
    thread::sleep(
        cluster.cfg.raft_store.raft_base_tick_interval.0
            * 3
            * cluster.cfg.raft_store.raft_election_timeout_ticks as u32,
    );
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    // Leader is awake because down voter is not detected (need 10min).
    assert!(awakened.load(Ordering::SeqCst));

    cluster.clear_send_filters();
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}

fn make_cb(cmd: &RaftCmdRequest) -> (Callback<RocksSnapshot>, CbReceivers) {
    let (proposed_tx, proposed_rx) = mpsc::channel();
    let (cb, _applied_rx) = make_cb_ext(
        cmd,
        Some(Box::new(move || proposed_tx.send(()).unwrap())),
        None,
    );
    (
        cb,
        CbReceivers {
            proposed: proposed_rx,
        },
    )
}

fn make_write_req(cluster: &mut Cluster<NodeCluster>, k: &[u8], v: &[u8]) -> RaftCmdRequest {
    let r = cluster.get_region(k);
    let mut req = new_request(
        r.get_id(),
        r.get_region_epoch().clone(),
        vec![new_put_cmd(k, v)],
        false,
    );
    let leader = cluster.leader_of_region(r.get_id()).unwrap();
    req.mut_header().set_peer(leader);
    req
}

struct CbReceivers {
    proposed: mpsc::Receiver<()>,
}

impl CbReceivers {
    fn assert_proposed_ok(&self) {
        self.proposed.recv_timeout(Duration::from_secs(1)).unwrap();
    }
}

/// Tests that leader cannot hibernate when matched peers don't form a quorum.
/// This covers the scenario of quorum check failure in peer.rs
/// check_before_tick().
#[test]
fn test_hibernate_not_enough_matched_peers() {
    let mut cluster = new_node_cluster(0, 3);
    configure_for_hibernate(&mut cluster.cfg);
    // Make sure max_peer_down_duration < election_timeout so that leader can detect
    // down peers before stepping down.
    // base_tick = 50ms
    // election_timeout = 20 * 50ms = 1000ms
    // max_peer_down_duration = 200ms
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(50);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 20;
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::millis(2500); // must larger than 2*election_timeout
    cluster.cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100); // call collect_down_peers()
    cluster.cfg.raft_store.max_peer_down_duration = ReadableDuration::millis(200);
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
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(false));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(!awakened.load(Ordering::SeqCst)); // leader hibernated successfully.

    cluster.clear_send_filters(); // clear filters before putting data

    // Add monitor filter again to check if leader keeps sending messages
    let awakened = Arc::new(AtomicBool::new(false));
    let filter = Arc::new(AtomicBool::new(false));
    let a = awakened.clone();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Send)
            .set_msg_callback(Arc::new(move |_| {
                a.store(true, Ordering::SeqCst);
            }))
            .when(filter.clone()),
    ));

    // Filter both AppendResponse (to keep matched index low) and HeartbeatResponse
    // (to make peers down)
    let recv_filter = Box::new(
        RegionPacketFilter::new(1, 1)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse)
            .msg_type(MessageType::MsgHeartbeatResponse),
    );
    cluster.add_recv_filter_on_node(1, recv_filter);

    let write_req = make_write_req(&mut cluster, b"k2", b"v2");
    let (cb, cb_receivers) = make_cb(&write_req);
    cluster
        .sim
        .rl()
        .async_command_on_node(1, write_req, cb)
        .unwrap();
    cb_receivers.assert_proposed_ok();

    // Wait for leader to detect down peers (max_peer_down_duration = 200ms)
    // But NOT wait too long to cause Leader Step Down (election_timeout = 1000ms)
    thread::sleep(Duration::from_millis(500));

    // Verify leader cannot hibernate because matched peers don't form quorum
    // 1. Peers 2 & 3 are detected as Down (no heartbeat response for > 200ms).
    // 2. check_before_tick skips "replication" check because peers are Down.
    // 3. matched_peer_ids only contains Leader (1), because peers 2 & 3 matched
    //    index is old (no AppendResponse).
    // 4. has_quorum({1}) returns false.
    // 5. check_before_tick returns "not enough matched peers".
    // 6. Leader stays awake.
    filter.store(true, Ordering::SeqCst);
    thread::sleep(cluster.cfg.raft_store.raft_heartbeat_interval() * 2);
    assert!(awakened.load(Ordering::SeqCst));

    cluster.clear_send_filters();
    cluster.clear_recv_filter_on_node(1);
    thread::sleep(Duration::from_secs(1));
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
}
