// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use std::thread;
use std::time::*;

use futures::Future;
use raft::eraftpb::{ConfChangeType, MessageType};
use test_raftstore::*;
use tikv::pd::PdClient;
use tikv_util::HandyRwLock;

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
    let region = cluster
        .pd_client
        .get_region_by_id(1)
        .wait()
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
    let mut admin_req = new_admin_request(1, &region.get_region_epoch(), conf_change);
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
