// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::*;
use std::sync::*;
use std::thread;
use std::time::Duration;

use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;
use tikv_util::HandyRwLock;

#[test]
fn test_break_leadership_on_restart() {
    let mut cluster = new_node_cluster(0, 3);
    let base_tick_ms = 50;
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(base_tick_ms);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 2;
    cluster.cfg.raft_store.raft_election_timeout_ticks = 10;
    // So the random election timeout will always be 10, which makes the case more stable.
    cluster.cfg.raft_store.raft_min_election_timeout_ticks = 10;
    cluster.cfg.raft_store.raft_max_election_timeout_ticks = 11;
    // Disable peer stale state check.
    cluster.cfg.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(1000);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration::secs(2000);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(500);
    cluster.pd_client.disable_default_operator();
    let r = cluster.run_conf_change();
    cluster.pd_client.must_add_peer(r, new_peer(2, 2));
    cluster.pd_client.must_add_peer(r, new_peer(3, 3));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    // Wait until all peers of region 1 hibernate and then stop peer 2.
    thread::sleep(Duration::from_millis(base_tick_ms * 30));
    cluster.stop_node(2);

    // Peer 3 will:
    // 1. steps a heartbeat message from its leader and then ticks 1 time.
    // 2. ticks a peer_stale_state_check, which will change state from Idle to PreChaos.
    // 3. continues to tick until it hibernates totally.
    fail::cfg("on_raft_base_tick_idle_with_missing_ticks", "return").unwrap();
    let router = cluster.sim.rl().get_router(3).unwrap();
    let mut raft_msg = RaftMessage::default();
    raft_msg.region_id = 1;
    raft_msg.set_from_peer(new_peer(1, 1));
    raft_msg.set_to_peer(new_peer(3, 3));
    raft_msg.mut_region_epoch().version = 1;
    raft_msg.mut_region_epoch().conf_ver = 3;
    raft_msg.mut_message().msg_type = MessageType::MsgHeartbeat;
    raft_msg.mut_message().from = 1;
    raft_msg.mut_message().to = 3;
    raft_msg.mut_message().term = 6;
    router.send_raft_message(raft_msg).unwrap();

    // Wait until the peer 3 hibernates again.
    // Until here, peer 3 will be like `election_elapsed=3 && missing_ticks=6`.
    thread::sleep(Duration::from_millis(base_tick_ms * 10));
    fail::remove("on_raft_base_tick_idle_with_missing_ticks");

    // Restart the peer 2 and it will broadcast `MsgRequestPreVote` later, which will wake up
    // peer 1 and 3. Peer 3 will starts a new election in only 1 tick because its election_elapsed
    // will be `3 + 6 + 1`.
    let (tx, rx) = mpsc::sync_channel(128);
    let filter = RegionPacketFilter::new(1, 3)
        .direction(Direction::Send)
        .msg_type(MessageType::MsgRequestVote)
        .when(Arc::new(AtomicBool::new(false)))
        .set_msg_callback(Arc::new(move |m| drop(tx.send(m.clone()))));
    cluster.add_send_filter(CloneFilterFactory(filter));
    cluster.run_node(2).unwrap();

    // If peer 3 starts a new election, it can be rejected by peer 1 and 2 because both of them
    // are in lease. Then peer 1 will step down because heartbeat responses from peer 3 can
    // carry a higher term. So there will be at least 1 election timeout that the region loses
    // its leader.
    assert!(rx.recv_timeout(Duration::from_secs(2)).is_err());
}
