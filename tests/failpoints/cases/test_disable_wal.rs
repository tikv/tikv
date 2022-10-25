// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{MiscExt, Peekable, RaftEngineReadOnly, CF_RAFT};
use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use raft::prelude::MessageType;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, HandyRwLock};

#[test]
fn test_disable_wal_recovery_add_peer_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.cfg.raft_store.region_worker_tick_interval = ReadableDuration::millis(50);
    let region = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.pd_client.must_add_peer(region, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.must_transfer_leader(region, new_peer(1, 1));
    // Skip applying snapshot into RocksDB to keep peer status in Applying.
    let apply_snapshot_fp = "apply_pending_snapshot";
    fail::cfg(apply_snapshot_fp, "return").unwrap();
    cluster.pd_client.add_peer(region, new_learner_peer(3, 3));
    loop {
        let region_state: Option<RegionLocalState> = cluster
            .get_engine(3)
            .get_msg_cf(CF_RAFT, &keys::region_state_key(region))
            .unwrap();
        if let Some(region_state) = region_state {
            if region_state.get_state() == PeerState::Applying {
                break;
            }
        }
    }
    cluster.stop_node(3);
    cluster.run_node(3).unwrap();
    fail::remove(apply_snapshot_fp);
    // Check if region range exists
    cluster.must_split(&cluster.get_region(b""), b"k2");
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
    let region_state: RegionLocalState = cluster
        .get_engine(3)
        .get_msg_cf(CF_RAFT, &keys::region_state_key(region))
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_state(), PeerState::Normal);
    cluster.stop_node(3);
    cluster.run_node(3).unwrap();
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");
}

#[test]
fn test_disable_wal_recovery_catchup_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_snapshot(&mut cluster);
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(10);
    cluster.cfg.raft_store.disable_kv_wal = true;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    let fp = "gc_seqno_relations";
    fail::cfg(fp, "return").unwrap();

    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let recv_filter = Box::new(
        RegionPacketFilter::new(1, 3)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    );
    cluster.sim.wl().add_recv_filter(3, recv_filter);

    for i in 0..20 {
        cluster.must_put(format!("k{}", i).as_bytes(), b"v");
    }
    // Wait util raft logs GC-ed.
    sleep_ms(100);
    let apply_state = cluster
        .get_raft_engine(3)
        .get_apply_state(1)
        .unwrap()
        .unwrap();
    cluster.sim.wl().clear_recv_filters(3);
    must_get_equal(&cluster.get_engine(3), b"k2", b"v");
    sleep_ms(100);
    cluster.get_engine(3).flush_cfs(true).unwrap();
    cluster.stop_node(3);
    fail::remove(fp);
    cluster.run_node(3).unwrap();
    let restarted_state = cluster
        .get_raft_engine(3)
        .get_apply_state(1)
        .unwrap()
        .unwrap();
    assert!(apply_state.get_applied_index() < restarted_state.get_applied_index());
    must_get_equal(&cluster.get_engine(3), b"k2", b"v");
}
