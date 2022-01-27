// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam::channel;
use engine_rocks::raw::Writable;
use engine_rocks::Compat;
use engine_traits::{Iterable, Peekable, RaftEngineReadOnly};
use engine_traits::{SyncMutable, CF_RAFT};
use kvproto::raft_serverpb::{PeerState, RaftMessage, RegionLocalState, StoreIdent};
use protobuf::Message;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::config::*;
use tikv_util::time::Instant;

fn test_tombstone<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k1", b"v1");

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    // Remove peer (2, 2) from region 1.
    pd_client.must_remove_peer(r1, new_peer(2, 2));

    // After new leader is elected, the change peer must be finished.
    cluster.leader_of_region(r1).unwrap();
    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k3");
    let mut existing_kvs = vec![];
    for cf in engine_2.cf_names() {
        engine_2
            .c()
            .scan_cf(cf, b"", &[0xFF], false, |k, v| {
                existing_kvs.push((k.to_vec(), v.to_vec()));
                Ok(true)
            })
            .unwrap();
    }
    // only tombstone key and store ident key exist.
    assert_eq!(existing_kvs.len(), 2);
    existing_kvs.sort();
    assert_eq!(existing_kvs[0].0.as_slice(), keys::STORE_IDENT_KEY);
    assert_eq!(existing_kvs[1].0, keys::region_state_key(r1));

    let mut ident = StoreIdent::default();
    ident.merge_from_bytes(&existing_kvs[0].1).unwrap();
    assert_eq!(ident.get_store_id(), 2);
    assert_eq!(ident.get_cluster_id(), cluster.id());

    let mut state = RegionLocalState::default();
    state.merge_from_bytes(&existing_kvs[1].1).unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // The peer 2 may be destroyed by:
    // 1. Apply the ConfChange RemovePeer command, the tombstone ConfVer is 4
    // 2. Receive a GC command before applying 1, the tombstone ConfVer is 3
    let conf_ver = state.get_region().get_region_epoch().get_conf_ver();
    assert!(conf_ver == 4 || conf_ver == 3);

    // Send a stale raft message to peer (2, 2)
    let mut raft_msg = RaftMessage::default();

    raft_msg.set_region_id(r1);
    // Use an invalid from peer to ignore gc peer message.
    raft_msg.set_from_peer(new_peer(0, 0));
    raft_msg.set_to_peer(new_peer(2, 2));
    raft_msg.mut_region_epoch().set_conf_ver(0);
    raft_msg.mut_region_epoch().set_version(0);

    cluster.send_raft_msg(raft_msg).unwrap();

    // We must get RegionNotFound error.
    let region_status = new_status_request(r1, new_peer(2, 2), new_region_leader_cmd());
    let resp = cluster
        .call_command(region_status, Duration::from_secs(5))
        .unwrap();
    assert!(
        resp.get_header().get_error().has_region_not_found(),
        "region must not found, but got {:?}",
        resp
    );
}

#[test]
fn test_node_tombstone() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_tombstone(&mut cluster);
}

#[test]
fn test_server_tombstone() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_tombstone(&mut cluster);
}

fn test_fast_destroy<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);

    // Disable default max peer number check.
    pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");
    // remove peer (3, 3)
    pd_client.must_remove_peer(1, new_peer(3, 3));

    must_get_none(&engine_3, b"k1");

    cluster.stop_node(3);

    let key = keys::region_state_key(1);
    let state: RegionLocalState = engine_3.c().get_msg_cf(CF_RAFT, &key).unwrap().unwrap();
    assert_eq!(state.get_state(), PeerState::Tombstone);

    // Force add some dirty data.
    engine_3.put(&keys::data_key(b"k0"), b"v0").unwrap();

    cluster.must_put(b"k2", b"v2");

    // start node again.
    cluster.run_node(3).unwrap();

    // add new peer in node 3
    pd_client.must_add_peer(1, new_peer(3, 4));

    must_get_equal(&engine_3, b"k2", b"v2");
    // the dirty data must be cleared up.
    must_get_none(&engine_3, b"k0");
}

#[test]
fn test_server_fast_destroy() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_fast_destroy(&mut cluster);
}

fn test_readd_peer<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();

    // add peer (2,2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster.get_engine(2);
    must_get_equal(&engine_2, b"k1", b"v1");

    // add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let engine_3 = cluster.get_engine(3);
    must_get_equal(&engine_3, b"k1", b"v1");

    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Remove peer (2, 2) from region 1.
    pd_client.must_remove_peer(r1, new_peer(2, 2));

    // After new leader is elected, the change peer must be finished.
    cluster.leader_of_region(r1).unwrap();
    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    pd_client.must_add_peer(r1, new_peer(2, 4));

    cluster.clear_send_filters();
    cluster.must_put(b"k4", b"v4");
    let engine = cluster.get_engine(2);
    must_get_equal(&engine, b"k4", b"v4");

    // Stale gc message should be ignored.
    let epoch = pd_client.get_region_epoch(r1);
    let mut gc_msg = RaftMessage::default();
    gc_msg.set_region_id(r1);
    gc_msg.set_from_peer(new_peer(1, 1));
    gc_msg.set_to_peer(new_peer(2, 2));
    gc_msg.set_region_epoch(epoch);
    gc_msg.set_is_tombstone(true);
    cluster.send_raft_msg(gc_msg).unwrap();
    // Fixme: find a better way to check if the message is ignored.
    thread::sleep(Duration::from_secs(1));
    must_get_equal(&engine, b"k4", b"v4");
}

#[test]
fn test_node_readd_peer() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_readd_peer(&mut cluster);
}

#[test]
fn test_server_readd_peer() {
    let count = 5;
    let mut cluster = new_server_cluster(0, count);
    test_readd_peer(&mut cluster);
}

// Simulate a case that tikv exit before a removed peer clean up its stale meta.
#[test]
fn test_server_stale_meta() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    cluster.run();
    cluster.add_send_filter(IsolationFilterFactory::new(3));
    pd_client.must_remove_peer(1, new_peer(3, 3));
    pd_client.must_add_peer(1, new_peer(3, 4));
    cluster.shutdown();

    let engine_3 = cluster.get_engine(3);
    let mut state: RegionLocalState = engine_3
        .c()
        .get_msg_cf(CF_RAFT, &keys::region_state_key(1))
        .unwrap()
        .unwrap();
    state.set_state(PeerState::Tombstone);

    engine_3
        .c()
        .put_msg_cf(CF_RAFT, &keys::region_state_key(1), &state)
        .unwrap();
    cluster.clear_send_filters();

    // avoid TIMEWAIT
    sleep_ms(500);
    cluster.start().unwrap();

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&engine_3, b"k1", b"v1");
}

/// Tests a tombstone peer won't trigger wrong gc message.
///
/// An uninitialized peer's peer list is empty. If a message from a healthy peer passes
/// all the other checks accidentally, it may trigger a tombstone message which will
/// make the healthy peer destroy all its data.
#[test]
fn test_safe_tombstone_gc() {
    let mut cluster = new_node_cluster(0, 5);

    let tick = cluster.cfg.raft_store.raft_election_timeout_ticks;
    let base_tick_interval = cluster.cfg.raft_store.raft_base_tick_interval.0;
    let check_interval = base_tick_interval * (tick as u32 * 2 + 1);
    cluster.cfg.raft_store.peer_stale_state_check_interval = ReadableDuration(check_interval);
    cluster.cfg.raft_store.abnormal_leader_missing_duration = ReadableDuration(check_interval * 2);
    cluster.cfg.raft_store.max_leader_missing_duration = ReadableDuration(check_interval * 2);

    let pd_client = Arc::clone(&cluster.pd_client);

    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(2, 2));
    pd_client.must_add_peer(r, new_peer(3, 3));

    cluster.add_send_filter(IsolationFilterFactory::new(4));

    pd_client.must_add_peer(r, new_peer(4, 4));
    pd_client.must_add_peer(r, new_peer(5, 5));
    cluster.must_transfer_leader(r, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(5), b"k1", b"v1");

    let (tx, rx) = channel::unbounded();
    cluster.clear_send_filters();
    cluster.add_send_filter(IsolationFilterFactory::new(5));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r, 4)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .set_msg_callback(Arc::new(move |msg| {
                let _ = tx.send(msg.clone());
            })),
    ));

    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    pd_client.must_remove_peer(r, new_peer(4, 4));
    let key = keys::region_state_key(r);
    let mut state: Option<RegionLocalState> = None;
    let timer = Instant::now();
    while timer.saturating_elapsed() < Duration::from_secs(5) {
        state = cluster.get_engine(4).c().get_msg_cf(CF_RAFT, &key).unwrap();
        if state.is_some() {
            break;
        }
        thread::sleep(Duration::from_millis(30));
    }
    if state.is_none() {
        panic!("region on store 4 has not been tombstone after 5 seconds.");
    }
    cluster.clear_send_filters();
    cluster.add_send_filter(PartitionFilterFactory::new(vec![1, 2, 3], vec![4, 5]));

    thread::sleep(base_tick_interval * tick as u32 * 3);
    must_get_equal(&cluster.get_engine(5), b"k1", b"v1");
}

/// Logs scan are now moved to raftlog gc threads. The case is to test if logs
/// are cleaned up no mater whether log gc task has been executed.
#[test]
fn test_destroy_clean_up_logs_with_log_gc() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 50;
    cluster.cfg.raft_store.raft_log_gc_threshold = 50;
    let pd_client = cluster.pd_client.clone();

    // Disable default max peer number check.
    pd_client.disable_default_operator();
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    let raft_engine = cluster.engines[&3].raft.clone();
    let mut dest = vec![];
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    assert!(!dest.is_empty());

    pd_client.must_remove_peer(1, new_peer(3, 3));
    must_get_none(&cluster.get_engine(3), b"k1");
    dest.clear();
    // Normally destroy peer should cleanup all logs.
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    assert!(dest.is_empty(), "{:?}", dest);

    pd_client.must_add_peer(1, new_peer(3, 4));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
    dest.clear();
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    assert!(!dest.is_empty());

    pd_client.must_remove_peer(1, new_peer(3, 4));
    must_get_none(&cluster.get_engine(3), b"k1");
    dest.clear();
    // Peer created by snapshot should also cleanup all logs.
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    assert!(dest.is_empty(), "{:?}", dest);

    pd_client.must_add_peer(1, new_peer(3, 5));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(3), b"k4", b"v4");
    dest.clear();
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    assert!(!dest.is_empty());

    let state = cluster.truncated_state(1, 3);
    for _ in 0..50 {
        cluster.must_put(b"k5", b"v5");
    }
    cluster.wait_log_truncated(1, 3, state.get_index() + 1);

    pd_client.must_remove_peer(1, new_peer(3, 5));
    must_get_none(&cluster.get_engine(3), b"k1");
    dest.clear();
    // Peer destroy after log gc should also cleanup all logs.
    raft_engine.get_all_entries_to(1, &mut dest).unwrap();
    assert!(dest.is_empty(), "{:?}", dest);
}
