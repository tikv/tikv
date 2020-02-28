// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use std::thread;
use std::time::*;

use raft::eraftpb::MessageType;

use test_raftstore::*;
use tikv_util::HandyRwLock;

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
    let timer = Instant::now();
    loop {
        if cluster.truncated_state(1, 1).get_index() > idx {
            break;
        }
        thread::sleep(Duration::from_millis(10));
        if timer.elapsed() > Duration::from_secs(3) {
            panic!("log is not compact after 3 seconds");
        }
    }
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
    while timer.elapsed() < Duration::from_secs(3) && messages.lock().unwrap().is_empty() {
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
    while timer.elapsed() < Duration::from_secs(3) {
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
