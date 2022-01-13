// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::TikvClient;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;
use tikv_util::HandyRwLock;

/// When a follower applies log slowly, leader should not transfer leader
/// to it. Otherwise, new leader may wait a long time to serve read/write
/// requests.
#[test]
fn test_transfer_leader_slow_apply() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 1002));
    pd_client.must_add_peer(r1, new_peer(3, 1003));

    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    let fp = "on_handle_apply_1003";
    fail::cfg(fp, "pause").unwrap();
    for i in 0..=cluster.cfg.raft_store.leader_transfer_max_log_lag {
        let bytes = format!("k{:03}", i).into_bytes();
        cluster.must_put(&bytes, &bytes);
    }
    cluster.transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    assert_ne!(cluster.leader_of_region(r1).unwrap(), new_peer(3, 1003));
    fail::remove(fp);
    cluster.must_transfer_leader(r1, new_peer(3, 1003));
    cluster.must_put(b"k3", b"v3");
    must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
}

#[test]
fn test_transfer_leader_other_region_apply_log_lag() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(2, 2));
    pd_client.must_add_peer(r, new_peer(3, 3));

    let r1 = cluster.get_region(b"");
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.must_put(b"k9", b"v9");
    cluster.must_split(&r1, b"k6");
    let r1 = cluster.get_region(b"");
    let r2 = cluster.get_region(b"k6");
    cluster.must_transfer_leader(r2.get_id(), new_peer(1, 1));

    // r1 on store 3 has apply log lag, r2 reject transfer leader to the peer on store 3
    let fp = "on_handle_apply_1003";
    fail::cfg(fp, "pause").unwrap();
    for i in 0..=cluster.cfg.raft_store.leader_transfer_max_log_lag {
        let bytes = format!("k{:03}", i).into_bytes();
        cluster.must_put(&bytes, &bytes);
    }
    cluster.transfer_leader(r2.get_id(), new_peer(3, 3));
    cluster.must_put(b"k6", b"v6");
    must_get_equal(&cluster.get_engine(1), b"k6", b"v6");
    assert_ne!(
        cluster.leader_of_region(r2.get_id()).unwrap(),
        new_peer(3, 3)
    );
    fail::remove(fp);
    cluster.must_transfer_leader(r2.get_id(), new_peer(3, 3));
    cluster.must_put(b"k7", b"v7");
    must_get_equal(&cluster.get_engine(3), b"k7", b"v7");
}

#[test]
fn test_transfer_leader_other_region_replicate_log_lag() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(10);

    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r = cluster.run_conf_change();
    pd_client.must_add_peer(r, new_peer(2, 2));
    pd_client.must_add_peer(r, new_peer(3, 3));

    let r1 = cluster.get_region(b"");
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    cluster.must_put(b"k9", b"v9");
    cluster.must_split(&r1, b"k6");
    let r1 = cluster.get_region(b"");
    let r2 = cluster.get_region(b"k6");
    println!("here {:?}", r2);
    cluster.must_transfer_leader(r2.get_id(), new_peer(1, 1));

    // r1 on store 3 has replicate log lag, r2 reject transfer leader to the peer on store 3
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(r1.get_id(), 3)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));

    for i in 0..=cluster.cfg.raft_store.leader_transfer_max_log_lag {
        let bytes = format!("k{:03}", i).into_bytes();
        cluster.must_put(&bytes, &bytes);
    }
    thread::sleep(Duration::from_millis(10));
    cluster.transfer_leader(r2.get_id(), new_peer(3, 3));
    cluster.must_put(b"k6", b"v6");
    must_get_equal(&cluster.get_engine(1), b"k6", b"v6");
    assert_ne!(
        cluster.leader_of_region(r2.get_id()).unwrap(),
        new_peer(3, 3)
    );
    cluster.clear_send_filters();
    thread::sleep(Duration::from_millis(10));
    cluster.must_transfer_leader(r2.get_id(), new_peer(3, 3));
    cluster.must_put(b"k7", b"v7");
    must_get_equal(&cluster.get_engine(3), b"k7", b"v7");
}

#[test]
fn test_prewrite_before_max_ts_is_synced() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    cluster.run();

    let addr = cluster.sim.rl().get_addr(1);
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let do_prewrite = |cluster: &mut Cluster<ServerCluster>| {
        let region_id = 1;
        let leader = cluster.leader_of_region(region_id).unwrap();
        let epoch = cluster.get_region_epoch(region_id);
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(leader);
        ctx.set_region_epoch(epoch);

        let mut req = PrewriteRequest::default();
        req.set_context(ctx);
        req.set_primary_lock(b"key".to_vec());
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(b"key".to_vec());
        mutation.set_value(b"value".to_vec());
        req.mut_mutations().push(mutation);
        req.set_start_version(100);
        req.set_lock_ttl(20000);
        req.set_use_async_commit(true);
        client.kv_prewrite(&req).unwrap()
    };

    cluster.must_transfer_leader(1, new_peer(2, 2));
    fail::cfg("test_raftstore_get_tso", "return(50)").unwrap();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let resp = do_prewrite(&mut cluster);
    assert!(resp.get_region_error().has_max_timestamp_not_synced());
    fail::remove("test_raftstore_get_tso");
    thread::sleep(Duration::from_millis(200));
    let resp = do_prewrite(&mut cluster);
    assert!(!resp.get_region_error().has_max_timestamp_not_synced());
}
