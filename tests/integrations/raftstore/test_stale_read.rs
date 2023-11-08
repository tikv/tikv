// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, sync::Arc, time::Duration};

use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, Op},
    metapb::{Peer, Region},
    tikvpb_grpc::TikvClient,
};
use test_raftstore::{must_get_equal, new_mutation, new_peer};
use test_raftstore_macro::test_case;
use tikv_util::{config::ReadableDuration, time::Instant};

use crate::tikv_util::HandyRwLock;

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_stale_read_with_ts0() {
    let mut cluster = new_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.cfg.resolved_ts.enable = true;
    cluster.cfg.resolved_ts.advance_ts_interval = ReadableDuration::millis(200);
    cluster.run();

    let region_id = 1;
    let env = Arc::new(Environment::new(1));
    let new_client = |peer: Peer| {
        let cli = TikvClient::new(
            ChannelBuilder::new(env.clone())
                .connect(&cluster.sim.rl().get_addr(peer.get_store_id())),
        );
        let epoch = cluster.get_region_epoch(region_id);
        let mut ctx = Context::default();
        ctx.set_region_id(region_id);
        ctx.set_peer(peer);
        ctx.set_region_epoch(epoch);
        PeerClient { cli, ctx }
    };
    let leader = new_peer(1, 1);
    let mut leader_client = new_client(leader.clone());
    let follower = new_peer(2, 2);
    let mut follower_client2 = new_client(follower);

    cluster.must_transfer_leader(1, leader);

    // Set the `stale_read` flag
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    assert!(
        follower_client2
            .kv_read(b"key1".to_vec(), 0)
            .region_error
            .into_option()
            .unwrap()
            .not_leader
            .is_some()
    );
    assert!(leader_client.kv_read(b"key1".to_vec(), 0).not_found);
}

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_stale_read_resolved_ts_advance() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.resolved_ts.enable = true;
    cluster.cfg.resolved_ts.advance_ts_interval = ReadableDuration::millis(200);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    cluster.run_conf_change();
    let cluster = RefCell::new(cluster);

    let must_resolved_ts_advance = |region: &Region| {
        let cluster = cluster.borrow_mut();
        let ts = cluster.store_metas[&region.get_peers()[0].get_store_id()]
            .lock()
            .unwrap()
            .region_read_progress
            .get_resolved_ts(&region.get_id())
            .unwrap();
        let now = Instant::now();
        for peer in region.get_peers() {
            loop {
                let new_ts = cluster.store_metas[&peer.get_store_id()]
                    .lock()
                    .unwrap()
                    .region_read_progress
                    .get_resolved_ts(&region.get_id())
                    .unwrap();
                if new_ts <= ts {
                    if now.saturating_elapsed() > Duration::from_secs(5) {
                        panic!("timeout");
                    }
                    continue;
                }
                break;
            }
        }
    };
    // Now region 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.borrow_mut().must_put(key, value);
    assert_eq!(cluster.borrow_mut().get(key), Some(value.to_vec()));

    // Make sure resolved ts advances.
    let region = cluster.borrow().get_region(&[]);
    must_resolved_ts_advance(&region);

    // Add peer (2, 2) to region 1.
    pd_client.must_add_peer(region.id, new_peer(2, 2));
    must_get_equal(&cluster.borrow().get_engine(2), key, value);

    // Test conf change.
    let region = cluster.borrow().get_region(&[]);
    must_resolved_ts_advance(&region);

    // Test transfer leader.
    let region = cluster.borrow().get_region(&[]);
    cluster
        .borrow_mut()
        .must_transfer_leader(region.get_id(), region.get_peers()[1].clone());
    must_resolved_ts_advance(&region);

    // Test split.
    let split_key = b"k1";
    cluster.borrow_mut().must_split(&region, split_key);
    let left = cluster.borrow().get_region(&[]);
    let right = cluster.borrow().get_region(split_key);
    must_resolved_ts_advance(&left);
    must_resolved_ts_advance(&right);
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_resolved_ts_after_destroy_peer() {
    let mut cluster = new_cluster(0, 3);
    cluster.cfg.resolved_ts.enable = true;
    cluster.cfg.resolved_ts.advance_ts_interval = ReadableDuration::millis(200);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    // Now region 1 only has peer (1, 1);
    let (key, value) = (b"k1", b"v1");

    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    // Add peer (2, 2) to region 1.
    pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), key, value);

    // Add peer (3, 3) to region 1.
    pd_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), key, value);

    // Transfer leader to peer (2, 2).
    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Remove peer (1, 1) from region 1.
    pd_client.must_remove_peer(r1, new_peer(1, 1));

    // Make sure region 1 is removed from store 1.
    cluster.wait_destroy_and_clean(r1, new_peer(1, 1));

    // Must not get destory peer's read progress
    let meta = cluster.store_metas[&r1].lock().unwrap();
    assert_eq!(None, meta.region_read_progress.get_resolved_ts(&r1))
}
